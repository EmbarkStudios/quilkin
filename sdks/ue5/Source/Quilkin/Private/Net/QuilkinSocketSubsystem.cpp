/*
 * Copyright 2023 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "QuilkinSocketSubsystem.h"
#include "QuilkinControlMessageProtocol.h"
#include "Containers/QuilkinCircularBuffer.h"
#include "QuilkinSocket.h"
#include "QuilkinLog.h"
#include "QuilkinSettings.h"
#include "QuilkinTelemetry.h"
#include "Async/Async.h"
#include "Async/AsyncWork.h"
#include "Async/ParallelFor.h"
#include "IPAddress.h"
#include "Engine/GameInstance.h"
#include "GenericPlatform/GenericPlatformMath.h"
#include "Serialization/JsonSerializer.h"
#include "CoreGlobals.h"
#include "Quilkin.h"

FQuilkinSocketSubsystem::FQuilkinSocketSubsystem(ISocketSubsystem* WrappedSocketSubsystem) 
	: SocketSubsystem{WrappedSocketSubsystem}
{}

FQuilkinSocketSubsystem::~FQuilkinSocketSubsystem()
{
}

bool FQuilkinSocketSubsystem::Init(FString& Error)
{
	UE_LOG(LogQuilkin, Display, TEXT("Initialising Socket Subsystem"));
	return true;
}

void FQuilkinSocketSubsystem::Shutdown()
{
}

bool FQuilkinSocketSubsystem::Tick(float DeltaTime) {

	if (!UQuilkinConfigSubsystem::Get()->GetMeasureEndpoints())
	{
		return true;
	}

	if (!UQuilkinConfigSubsystem::Get()->GetEnabled())
	{
		return true;
	}

	TickElapsed += DeltaTime;

	if (TickElapsed >= 60)
	{
		TickElapsed = 0;

		// If there's no internet
		if (FGenericPlatformMisc::GetNetworkConnectionType() == ENetworkConnectionType::None)
		{
			UE_LOG(LogQuilkin, Warning, TEXT("no internet connection available"));
			return true;
		}

		UpdateEndpoints(Map.GetEndpoints(), Map.GetTopologies());
	}

	return true;
}

FUniqueSocket FQuilkinSocketSubsystem::SetupNonblockingUDPSocket(const FString& Name) const
{
	auto Socket = SocketSubsystem->CreateUniqueSocket(NAME_DGram, *Name, FNetworkProtocolTypes::IPv4);
	if (!Socket) 
	{
		UE_LOG(LogQuilkin, Error, TEXT("Couldn't allocate a new socket"));
		return nullptr;
	}
		
	if (!Socket->SetNonBlocking(true))
	{
		UE_LOG(LogQuilkin, Error, TEXT("Couldn't set the socket to non-blocking"));
		return nullptr;
	}

	return Socket;
}

void FQuilkinSocketSubsystem::UpdateEndpoints(TArray<FQuilkinEndpoint>&& Endpoints, TArray<FQuilkinTopology>&& Topologies)
{
	check(IsInGameThread());
	check(Endpoints.Num() == Topologies.Num());

	if (PingingInAction)
	{
		UE_LOG(LogQuilkin, Display, TEXT("Skipping PingEndpoints task, already in progress"));
		return;
	}

	PingingInAction = true;
	UE_LOG(LogQuilkin, Display, TEXT("PingEndpoints task has started"));

	if (Endpoints.IsEmpty())
	{
		UE_LOG(LogQuilkin, Warning, TEXT("No endpoints to measure"));
		PingingInAction = false;
		return;
	}

	auto Socket = SetupNonblockingUDPSocket("QuilkinPing");
	if (!Socket)
	{
		PingingInAction = false;
		return;
	}

	AsyncTask(ENamedThreads::AnyBackgroundThreadNormalTask, [This = AsShared(), Socket=MoveTemp(Socket), Endpoints=MoveTemp(Endpoints), Topologies=MoveTemp(Topologies)]() mutable
	{
		auto PingResults = This->Ping(Socket, Endpoints);

		AsyncTask(ENamedThreads::GameThread, [This, PingResults=MoveTemp(PingResults), Topologies=MoveTemp(Topologies), Endpoints=MoveTemp(Endpoints)]() mutable
		{
			// Collect measurements & sent telemetry
			TArray<CircularBuffer<int64>> Measurements;
			Measurements.Reserve(PingResults.Num());
			
			for (int32 i = 0; i < PingResults.Num(); i++)
			{
				auto& PingResult = PingResults[i];
				auto& Region = Topologies[i].Region;

				int32 PingsSent = PingResult.Sent;
				int32 PingsReceived = PingResult.Latencies.Num();

				for (int32 j = PingsReceived; j < PingsSent; j++)
				{
					FQuilkinTelemetry::QuilkinPingError(Endpoints[i].Host, Endpoints[i].QcmpPort, Region, QuilkinPingTimeout, Endpoints[i].ToString());
				}

				for (auto& Error : PingResult.Errors)
				{
					FQuilkinTelemetry::QuilkinPingError(Endpoints[i].Host, Endpoints[i].QcmpPort, Region, Error, Endpoints[i].ToString());
				}

				if (PingsReceived == 0)
				{
					FQuilkinTelemetry::QuilkinPingError(Endpoints[i].Host, Endpoints[i].QcmpPort, Region, QuilkinPingNoValidSamples, Endpoints[i].ToString());
				}

				Measurements.Emplace(MoveTemp(PingResult.Latencies));
			}

			This->Map = FQuilkinEndpointMap(MoveTemp(Endpoints), MoveTemp(Topologies), MoveTemp(Measurements));

			UQuilkin::OnMeasurementCompleted.Broadcast();
			This->PingingInAction = false;
		});
	});
}

TArray<FPingEndpointResult> FQuilkinSocketSubsystem::Ping(const FUniqueSocket& Socket, const TArray<FQuilkinEndpoint>& Endpoints, const PingSettings& Settings)
{
	struct NonceData
	{
		int32 EndpointIndex;
		TArray<uint8> Nonces;
	};
	TMap<FString, NonceData> AddressToNonceMap;

	// Prepare result array — one entry per input endpoint, same order.
	TArray<FPingEndpointResult> Results;
	Results.SetNum(Endpoints.Num());

	/**
	 * Step 1. Send few pings to every endpoint
	 */
	uint32 PingsSent = 0;
	for (int32 i = 0; i < Endpoints.Num(); i++)
	{
		auto Res = Endpoints[i].ToQcmpInternetAddr(this);
		if (Res.IsError())
		{
			UE_LOG(LogQuilkin, Warning, TEXT("Couldn't resolve %s to an IP address"), *Endpoints[i].Host);
			continue;
		}
		auto& EndpointAddress = Res.GetValue();

		auto& [EndpointIndex, Nonces] = AddressToNonceMap.FindOrAdd(
			EndpointAddress->ToString(true),
			NonceData { .EndpointIndex = i });

		uint8 CurrentNonce = FMath::RandRange(0, UINT8_MAX);
		for (uint32 j = 0; j < Settings.Count; j++)
		{
			FPing Ping;
			Ping.Nonce = CurrentNonce++; // Overflow to 0 is expected
			auto Buffer = Ping.Encode();
			auto BytesSent = 0;
			if (!Socket->SendTo(Buffer.GetData(), Buffer.Num(), BytesSent, *EndpointAddress))
			{
				UE_LOG(LogQuilkin, Warning, TEXT("Failed to send ping to %s: %s"),
					*EndpointAddress->ToString(true), GetSocketError(GetLastErrorCode()));
				continue;
			}

			Nonces.Add(Ping.GetNonce());
			Results[i].Sent++;
			PingsSent++;
		}
	}

	/**
	 * Step 2. Await for the pings to come back and measure RTT
	 */
	uint32 ResponsesReceived = 0;

	TArray<uint8> Buffer;
	Buffer.SetNumUninitialized(1024);
	auto SenderAddr = CreateInternetAddr();

	const double StartTime = FPlatformTime::Seconds();
	while (ResponsesReceived < PingsSent && FPlatformTime::Seconds() - StartTime < Settings.TimeoutSec && !IsEngineExitRequested())
	{
		int32 BytesReceived;
		if (!Socket->RecvFrom(Buffer.GetData(), Buffer.Num(), BytesReceived, *SenderAddr))
		{
			ESocketErrors Error = GetLastErrorCode();
			if (Error == SE_EWOULDBLOCK || Error == SE_TRY_AGAIN)
			{
				// Normal flow - non-blocking socket did not block
				FPlatformProcess::SleepNoStats(Settings.SleepAfterRecvSec);
				continue;
			}

			UE_LOG(LogQuilkin, Error, TEXT("Failed to read from socket: %s"), GetSocketError(Error));
			break;
		}

		if (BytesReceived == 0)
		{
			// Ignore empty packets
			continue;
		}

		int64 PingReceivedTimeNanos = GetUnixTimestampInNanos();
		FString SenderAddressStr = SenderAddr->ToString(true);

		auto EndpointFound = AddressToNonceMap.Find(SenderAddressStr);
		if (!EndpointFound)
		{
			UE_LOG(LogQuilkin, Warning, TEXT("Received packet from unexpected address %s"), *SenderAddressStr);
			continue;
		}
		auto& [EndpointIndex, ExpectedNonces] = *EndpointFound;

		auto Result = FPingReply::Decode(Buffer);
		if (Result.IsError())
		{
			UE_LOG(LogQuilkin, Warning, TEXT("Failed to decode ping reply: %s from %s"), *Result.GetError(), *SenderAddressStr);
			Results[EndpointIndex].Errors.Add(QuilkinPingDecodeError);
			continue;
		}

		auto Packet = Result.GetValue()->AsVariant<FPingReply>();
		if (!Packet.IsSet())
		{
			UE_LOG(LogQuilkin, Warning, TEXT("Expected ping reply, found packet code %d from %s"), Result.GetValue()->GetCode(), *SenderAddressStr);
			Results[EndpointIndex].Errors.Add(QuilkinPingInvalidPacket);
			continue;
		}

		auto Reply = Packet.GetValue();
		if (!ExpectedNonces.Contains(Reply->GetNonce()))
		{
			UE_LOG(LogQuilkin, Warning, TEXT("Received nonce %d from %s didn't match any sent nonce"), Reply->GetNonce(), *SenderAddressStr);
			Results[EndpointIndex].Errors.Add(QuilkinPingNonceMismatch);
			continue;
		}

		int64 RTT = Reply->RoundTimeDelay(PingReceivedTimeNanos);
		Results[EndpointIndex].Latencies.Add(RTT);
		ResponsesReceived++;

		UE_LOG(LogQuilkin, Verbose, TEXT("[%d/%d] Received nonce %d from %s. RTT: %lld"), ResponsesReceived, PingsSent, Reply->GetNonce(), *SenderAddressStr, NanosToMillis(RTT));
	}

	return Results;
}

// MARK: ISocketSubsystem Interface

FSocket* FQuilkinSocketSubsystem::CreateSocket(const FName& SocketType, const FString& SocketDescription, bool bForceUDP) 
{ 
	return SocketSubsystem->CreateSocket(SocketType, SocketDescription, bForceUDP); 
}

FSocket* FQuilkinSocketSubsystem::CreateSocket(const FName& SocketType, const FString& SocketDescription, const FName& ProtocolName)
{
	FUniqueSocket Socket = SocketSubsystem->CreateUniqueSocket(SocketType, SocketDescription, ProtocolName);
	ESocketType InSocketType = Socket->GetSocketType();
	
	auto QuilkinSocket = new FQuilkinSocket(MoveTemp(Socket), InSocketType, SocketDescription, ProtocolName);
	QuilkinSocket->Subsystem = AsWeak();
	return QuilkinSocket;
}

void FQuilkinSocketSubsystem::DestroySocket(FSocket* Socket)
{
	delete Socket;
}

FResolveInfoCached* FQuilkinSocketSubsystem::CreateResolveInfoCached(TSharedPtr<FInternetAddr> Addr) const 
{
	return SocketSubsystem->CreateResolveInfoCached(Addr);
}

FAddressInfoResult FQuilkinSocketSubsystem::GetAddressInfo(const TCHAR* HostName, const TCHAR* ServiceName, EAddressInfoFlags QueryFlags, const FName ProtocolTypeName, ESocketType SocketType)
{
	auto Result = SocketSubsystem->GetAddressInfo(HostName, ServiceName, QueryFlags, ProtocolTypeName, SocketType);

#if UE_SERVER
	auto Cfg = UQuilkinConfigSubsystem::Get();
	bool IPv6Prioritised = Cfg->GetIPv6Prioritised();
	UE_LOG(LogQuilkin, Verbose, TEXT("Calling Quilkin GAI, IPv6 priority: %s"), IPv6Prioritised ? TEXT("true"): TEXT("false"));

	if (IPv6Prioritised) {
		TDeque<FAddressInfoResultData> IPv6Queue;
		for (auto Entry : Result.Results) {
			if (Entry.AddressProtocol == ESocketProtocolFamily::IPv6) {
				IPv6Queue.PushFirst(Entry);
			}
			else {
				IPv6Queue.PushLast(Entry);
			}
		}

		if (UE_LOG_ACTIVE(LogQuilkin, Verbose)) {
			for (auto Entry : IPv6Queue) {
				UE_LOG(LogQuilkin, Verbose, TEXT("GAI: Found IPv6 Address: %s"), *Entry.Address->ToString(true));
			}
		}

		TArray<FAddressInfoResultData> IPv6Array;
		for (auto Entry : IPv6Queue) {
			IPv6Array.Add(Entry);
		}

		Result.Results = IPv6Array;
	}

#endif

	return Result;
}

void FQuilkinSocketSubsystem::GetAddressInfoAsync(FAsyncGetAddressInfoCallback Callback, const TCHAR* HostName, const TCHAR* ServiceName, EAddressInfoFlags QueryFlags, const FName ProtocolTypeName, ESocketType SocketType) 
{
	SocketSubsystem->GetAddressInfoAsync(Callback, HostName, ServiceName, QueryFlags, ProtocolTypeName, SocketType);
}

TSharedPtr<FInternetAddr> FQuilkinSocketSubsystem::GetAddressFromString(const FString& InAddress)
{
	return SocketSubsystem->GetAddressFromString(InAddress);
}

FResolveInfo* FQuilkinSocketSubsystem::GetHostByName(const ANSICHAR* HostName)
{
	return SocketSubsystem->GetHostByName(HostName);
}

bool FQuilkinSocketSubsystem::RequiresChatDataBeSeparate()
{
	return SocketSubsystem->RequiresChatDataBeSeparate();
}

bool FQuilkinSocketSubsystem::RequiresEncryptedPackets()
{
	return SocketSubsystem->RequiresEncryptedPackets();
}

bool FQuilkinSocketSubsystem::GetHostName(FString& HostName)
{
	return SocketSubsystem->GetHostName(HostName);
}

TSharedRef<FInternetAddr> FQuilkinSocketSubsystem::CreateInternetAddr()
{
	return SocketSubsystem->CreateInternetAddr();
}

TSharedRef<FInternetAddr> FQuilkinSocketSubsystem::CreateInternetAddr(const FName ProtocolType)
{
	return SocketSubsystem->CreateInternetAddr(ProtocolType);
}

TSharedRef<FInternetAddr> FQuilkinSocketSubsystem::GetLocalBindAddr(FOutputDevice& Out)
{
	return SocketSubsystem->GetLocalBindAddr(Out);
}

TArray<TSharedRef<FInternetAddr>> FQuilkinSocketSubsystem::GetLocalBindAddresses()
{
	auto Addresses = SocketSubsystem->GetLocalBindAddresses();

#if UE_SERVER
	bool IPv6Prioritised = UQuilkinConfigSubsystem::Get()->GetIPv6Prioritised();
	UE_LOG(LogQuilkin, Verbose, TEXT("Calling Quilkin GetLocalBindAddresses, IPv6 only: %s"), IPv6Prioritised ? TEXT("true"): TEXT("false"));

	if (IPv6Prioritised) {
		TDeque<TSharedRef<FInternetAddr>> IPv6Queue;
		for (auto Entry : Addresses) {
			if (Entry->GetProtocolType() == FNetworkProtocolTypes::IPv6) {
				IPv6Queue.PushFirst(Entry);
			}
			else {
				IPv6Queue.PushLast(Entry);
			}
		}

		if (UE_LOG_ACTIVE(LogQuilkin, Verbose)) {
			for (auto Entry : IPv6Queue) {
				UE_LOG(LogQuilkin, Verbose, TEXT("Found Address: %s"), *Entry->ToString(true));
			}
		}

		TArray<TSharedRef<FInternetAddr>> IPv6Array;
		for (auto Entry : IPv6Queue) {
			IPv6Array.Add(Entry);
		}

		return IPv6Array;
	}
#endif

	return Addresses;
}

bool FQuilkinSocketSubsystem::GetLocalAdapterAddresses(TArray<TSharedPtr<FInternetAddr>>& OutAddresses)
{
	return SocketSubsystem->GetLocalAdapterAddresses(OutAddresses);
}

TUniquePtr<FRecvMulti> FQuilkinSocketSubsystem::CreateRecvMulti(int32 MaxNumPackets, int32 MaxPacketSize, ERecvMultiFlags Flags)
{
	return SocketSubsystem->CreateRecvMulti(MaxNumPackets, MaxPacketSize, Flags);
}

TSharedRef<FInternetAddr> FQuilkinSocketSubsystem::GetLocalHostAddr(FOutputDevice& Out, bool& bCanBindAll)
{
	return SocketSubsystem->GetLocalHostAddr(Out, bCanBindAll);
}

bool FQuilkinSocketSubsystem::GetMultihomeAddress(TSharedRef<FInternetAddr>& Addr)
{
	return SocketSubsystem->GetMultihomeAddress(Addr);
}

bool FQuilkinSocketSubsystem::HasNetworkDevice()
{
	return SocketSubsystem->HasNetworkDevice();
}

const TCHAR* FQuilkinSocketSubsystem::GetSocketAPIName() const
{
	return SocketSubsystem->GetSocketAPIName();
}

ESocketErrors FQuilkinSocketSubsystem::GetLastErrorCode()
{
	return SocketSubsystem->GetLastErrorCode();
}

ESocketErrors FQuilkinSocketSubsystem::TranslateErrorCode(int32 Code)
{
	return SocketSubsystem->TranslateErrorCode(Code);
}

bool FQuilkinSocketSubsystem::IsSocketRecvMultiSupported() const
{
	return SocketSubsystem->IsSocketRecvMultiSupported();
}

bool FQuilkinSocketSubsystem::IsSocketWaitSupported() const
{
	return SocketSubsystem->IsSocketWaitSupported();
}

double FQuilkinSocketSubsystem::TranslatePacketTimestamp(const FPacketTimestamp& Timestamp, ETimestampTranslation Translation)
{
	return SocketSubsystem->TranslatePacketTimestamp(Timestamp, Translation);
}

bool FQuilkinSocketSubsystem::IsRecvFromWithPktInfoSupported() const
{
	return SocketSubsystem->IsRecvFromWithPktInfoSupported();
}
