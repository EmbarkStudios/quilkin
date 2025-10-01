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
#include "QuilkinCircularBuffer.h"
#include "QuilkinDelegates.h"
#include "QuilkinSocket.h"
#include "QuilkinLog.h"
#include "QuilkinSettings.h"
#include "Async/Async.h"
#include "Async/AsyncWork.h"
#include "Async/ParallelFor.h"
#include "IPAddress.h"
#include "Engine/GameInstance.h"
#include "Runtime/Online/HTTP/Public/Http.h"
#include "GenericPlatform/GenericPlatformMath.h"
#include "Serialization/JsonSerializer.h"
#include "Logging/StructuredLog.h"

static int64 NowSeconds() {
    return FDateTime::UtcNow().ToUnixTimestamp();
}

FQuilkinSocketSubsystem::FQuilkinSocketSubsystem(ISocketSubsystem* WrappedSocketSubsystem) 
    : PingSocket{nullptr}
    , SocketSubsystem{WrappedSocketSubsystem}
{
    FQueuedThreadPool* ThreadPool = FQueuedThreadPool::Allocate();
    int32 NumThreads = FGenericPlatformMath::Max(FPlatformMisc::NumberOfCores() / 2, 2);
    if (!ThreadPool->Create(NumThreads, 8 * 1024)) {
        UE_LOG(LogQuilkin, Error, TEXT("Couldn't allocate thread pool"));
    }
    this->PingThreadPool = ThreadPool;
}

FQuilkinSocketSubsystem::~FQuilkinSocketSubsystem()
{
}

bool FQuilkinSocketSubsystem::Init(FString& Error)
{
    UE_LOG(LogQuilkin, Display, TEXT("Initialising Socket Subsystem"));
    PingSocket = CreateIPv4Socket(TEXT("QuilkinPingSocket"));
    FQuilkinDelegates::GetQuilkinEndpointMeasurements.BindRaw(&Map, &FQuilkinEndpointMap::GetEndpointMeasurements);
    FQuilkinDelegates::GetLowestLatencyEndpoint.BindRaw(&Map, &FQuilkinEndpointMap::GetLowestLatencyEndpoint);
    FQuilkinDelegates::GetLowestLatencyEndpointInRegion.BindRaw(&Map, &FQuilkinEndpointMap::GetLowestLatencyEndpointInRegion);
    FQuilkinDelegates::GetLowestLatencyEndpointInEachRegion.BindRaw(&Map, &FQuilkinEndpointMap::GetLowestLatencyEndpointInEachRegion);
    FQuilkinDelegates::GetLowestLatencyToDatacenters.BindRaw(&Map, &FQuilkinEndpointMap::GetLowestLatencyToDatacenters);

    return true;
}

void FQuilkinSocketSubsystem::Shutdown()
{
    if (PingSocket != nullptr)
    {
        PingSocket = nullptr;
    }
}

bool FQuilkinSocketSubsystem::Tick(float DeltaTime) {
    // We bind here as a "late initialisation" step, as the SocketSubsystem will be initialised before GEngine.
    if (UQuilkinConfigSubsystem::IsAvailable() && !UpdateEndpointHandle.IsValid()) {
        UpdateEndpointHandle = UQuilkinConfigSubsystem::Get()->OnEndpointsChanged.AddRaw(this, &FQuilkinSocketSubsystem::UpdateEndpoints);
    }

    if (!UQuilkinConfigSubsystem::Get()->GetMeasureEndpoints())
    {
        return true;
    }

    if (!UQuilkinConfigSubsystem::Get()->GetEnabled())
    {
        return true;
    }

    TickElapsed += DeltaTime;

    if (TickElapsed >= 60) {
        TickElapsed = 0;

        // If there's no internet
        if (FGenericPlatformMisc::GetNetworkConnectionType() == ENetworkConnectionType::None) {
            UE_LOG(LogQuilkin, Warning, TEXT("no internet connection available"));
            return true;
        }

        PingEndpoints();
    }

    return true;
}

void FQuilkinSocketSubsystem::AllocatePingSocketsForEndpoints() {
    SocketAllocationLock.Lock();

    auto EndpointLength = Map.Endpoints.Num();
    auto PingSocketLength = PingSockets.Num();
    if (PingSocketLength < EndpointLength) {
        for (int32 i = 0; i < EndpointLength - PingSocketLength; i++) {
            auto SocketResult = CreateRandomUdpSocket();

            if (SocketResult.IsError()) {
                UE_LOG(LogQuilkin, Error, TEXT("Couldn't allocate socket, message: %s"), *SocketResult.GetError());
                SocketAllocationLock.Unlock();
                return;
            }

            PingSockets.Add(SocketResult.GetValue());
        }
    }
    else if (PingSocketLength > EndpointLength) {
        for (int32 i = 0; i < PingSocketLength - EndpointLength; i++) {
            if (!PingSockets.Pop()->Close()) {
                auto ErrorCode = GetLastErrorCode();
                FString ErrorDescription = GetSocketError(ErrorCode);
                UE_LOG(LogQuilkin, Error, TEXT("failed to close socket, code: %d, message: %s"), ErrorCode, *ErrorDescription);
            }
        }
    }

    SocketAllocationLock.Unlock();
}

void FQuilkinSocketSubsystem::UpdateEndpoints(TArray<FQuilkinEndpoint> NewEndpoints) {
    Map.Endpoints.ResetWithKeys(NewEndpoints, [](auto Endpoint) -> CircularBuffer<int64> {
        return CircularBuffer<int64>(50);
    });

    PingEndpoints();
}

TResult<FSocket*, FString> FQuilkinSocketSubsystem::CreateRandomUdpSocket()
{
    FSocket* Socket = CreateIPv4Socket(TEXT("QuilkinPingSocket"));

    if (!Socket)
    {
        return TResult<FSocket*, FString>(TEXT("couldn't create ping socket"));
    }

    return TResult<FSocket*, FString>(Socket);
}

void FQuilkinSocketSubsystem::PingEndpoints() {
    static std::atomic<bool> IS_ACTIVE(false);
    if (Map.Endpoints.IsEmpty()) {
        UE_LOG(LogQuilkin, Verbose, TEXT("no endpoints to measure"));
        return;
    }

    if (IS_ACTIVE.load()) {
        UE_LOG(LogQuilkin, Verbose, TEXT("ping task already executing"));
        return;
    }

    IS_ACTIVE.store(true);
    AsyncTask(ENamedThreads::AnyBackgroundThreadNormalTask, [this]() {
        auto Keys = Map.Endpoints.GetKeys();

        AllocatePingSocketsForEndpoints();

        if (PingSockets.Num() != Map.Endpoints.Num()) {
            UE_LOG(LogQuilkin, Error, TEXT("Couldn't allocate enough sockets to measure latency"));
            IS_ACTIVE.store(false);
            return;
        }

        TArray<FAsyncTask<FPingTask>*> Tasks;
        for (int32 Index = 0; Index < Map.Endpoints.Num(); ++Index)
        {
            auto Task = new FAsyncTask<FPingTask>(this, Keys, Index);
            Task->StartBackgroundTask(PingThreadPool);
            Tasks.Add(Task);
        }

        for (auto Task : Tasks)
        {
            Task->EnsureCompletion();
        }
        
        if (UE_LOG_ACTIVE(LogQuilkin, Verbose)) {
            auto Result = this->Map.GetLowestLatencyEndpoint(FString());

            if (!Result.IsSet()) {
                IS_ACTIVE.store(false);
                return;
            }

            auto Pair = Result.GetValue();
            FQuilkinEndpoint Endpoint = Pair.template Get<0>();
            UE_LOG(LogQuilkin, Verbose, TEXT("Lowest latency endpoint is %s (%dms)"), *Endpoint.ToString(), NanosToMillis(Pair.template Get<1>()));
        }

        GetDatacenterLatencies([this](TMap<FString, int64> NewMeasurements) {
            AsyncTask(ENamedThreads::GameThread, [this, NewMeasurements]() {
                if (UQuilkinConfigSubsystem::IsAvailable()) {
                    auto Config = UQuilkinConfigSubsystem::Get();
                    if (Config->MeasurementCompleted.IsBound()) {
                        Config->MeasurementCompleted.Broadcast(NewMeasurements);
                    }
                }
                IS_ACTIVE.store(false);
            });
        });
    });
}

TResult<int64, FString> FQuilkinSocketSubsystem::SendPing(FSocket* Socket, FInternetAddr& AddrRef)
{
    uint32 PingCount = 5;
    TArray<uint8> Nonces;
    auto Addr = AddrRef.Clone();

    UE_LOG(LogQuilkin, Verbose, TEXT("measuring latency to %s"), *Addr->ToString(true));

    for (uint32 i = 0; i < PingCount; i++) {
        auto Result = SendPacket(Socket, *Addr);
        if (Result.IsError()) {
            UE_LOG(LogQuilkin, Warning, TEXT("failed to send ping to %s: %s"), *Addr->ToString(true), *Result.GetError());
        }
        else {
            Nonces.Push(Result.GetValue());
        }
    }

    if (Nonces.IsEmpty()) {
        return TResult<int64, FString>(TEXT("all pings to failed"));
    }

    auto NewLatency = WaitForResponses(Socket, *Addr, PingCount, Nonces);

    if (NewLatency.IsSet()) {
        auto latency = NewLatency.GetValue();
        UE_LOG(LogQuilkin, Verbose, TEXT("new measured latency for %s: %dms"), *Addr->ToString(true), NanosToMillis(latency));
        return TResult<int64, FString>(latency);
    }
    else {
        return TResult<int64, FString>(TEXT("received no responses"));
    }

}

TResult<uint8, FString> FQuilkinSocketSubsystem::SendPacket(FSocket* Socket, FInternetAddr& Addr)
{
    auto Ping = FPing();
    auto Buffer = Ping.Encode();
    auto BytesSent = 0;
    UE_LOG(LogQuilkin, Verbose, TEXT("sending ping to %s, Nonce: %d"), *Addr.ToString(true), Ping.GetNonce());
    if (!Socket->SendTo(Buffer.GetData(), Buffer.Num(), BytesSent, Addr)) {
        auto ErrorCode = GetLastErrorCode();
        FString ErrorDescription = GetSocketError(ErrorCode);
        return TResult<uint8, FString>(ErrorDescription);
    }
    else {
        return TResult<uint8, FString>(Ping.GetNonce());
    }
}

TOptional<int64> FQuilkinSocketSubsystem::WaitForResponses(FSocket* Socket, FInternetAddr& Addr, uint32 PingCount, TArray<uint8> Nonces)
{
    const double Timeout = 5.0;
    const double StartTime = NowSeconds();
    uint32 ExpectedResponses = Nonces.Num();
    int32 ExceededTimeouts = 0;
    CircularBuffer<int64> SuccessfulResponses = CircularBuffer<int64>(PingCount);

    while ((SuccessfulResponses.Num() < Nonces.Num()) && (ExceededTimeouts <= Nonces.Num()) && (NowSeconds() - StartTime < Timeout))
    {
        if (IsEngineExitRequested()) {
            return TOptional<int64>();
        }

        TArray<uint8> Buffer;
        Buffer.SetNumUninitialized(1024);
        int32 BytesReceived = 0;
        UE_LOG(LogQuilkin, Verbose, TEXT("waiting on ping response from %s"), *Addr.ToString(true));
        if (Socket->Wait(ESocketWaitConditions::WaitForRead, FTimespan::FromMilliseconds(500)))
        {
            if (Socket->RecvFrom(Buffer.GetData(), Buffer.Num(), BytesReceived, Addr))
            {
                UE_LOG(LogQuilkin, Verbose, TEXT("received response from %s"), *Addr.ToString(true));
                auto Result = FPingReply::Decode(Buffer);

                if (Result.IsError()) {
                    UE_LOG(LogQuilkin, Warning, TEXT("failed to decode ping reply: %s"), *Result.GetError());
                    continue;
                }

                auto Packet = Result.GetValue()->AsVariant<FPingReply>();

                if (!Packet.IsSet()) {
                    UE_LOG(LogQuilkin, Warning, TEXT("expected ping reply, found: %d"), Result.GetValue()->GetCode());
                    continue;
                }

                auto Reply = Packet.GetValue();

                if (!Nonces.Contains(Reply->GetNonce())) {
                    UE_LOG(LogQuilkin, Warning, TEXT("received nonce (%d) didn't match any sent nonce"), Reply->GetNonce());
                    continue;
                }
                else {
                    UE_LOG(LogQuilkin, Verbose, TEXT("received nonce (%d)"), Reply->GetNonce());
                }

                SuccessfulResponses.Add(Reply->RoundTimeDelay());
            }
        }
        else
        {
            ExceededTimeouts += 1;
            continue;
        }
    }

    if (ExceededTimeouts > 0) {
        UE_LOG(LogQuilkin, Display, TEXT("%s exceeded WaitForRead timeout %d times"), *Addr.ToString(true), ExceededTimeouts);
    }

    if ((PingCount - ExceededTimeouts) <= 0) {
        return TOptional<int64>();
    }

    if (UQuilkinConfigSubsystem::IsAvailable() && UQuilkinConfigSubsystem::Get()->GetMeasurementImprovement()) {
        return TOptional<int64>(SuccessfulResponses.Median());
    }

    // If we sent less pings or we received less pings due to system errors or packet loss,
    //  then we penalise the endpoint's latency measurement for being inconsistent.
    uint32 PenaltyFactor = (PingCount - ExpectedResponses) + (ExpectedResponses - SuccessfulResponses.Num()) + ExceededTimeouts;
    for (uint32 i = 0; i < PenaltyFactor; i++) {
        SuccessfulResponses.Add(DefaultPenaltyLatency);
    }

    return TOptional<int64>(SuccessfulResponses.Median());
}

template <typename Fn> void FQuilkinSocketSubsystem::GetDatacenterLatencies(Fn OnCompletion) {
    if (!UQuilkinConfigSubsystem::IsAvailable()) {
        UE_LOG(LogQuilkin, Display, TEXT("config subsystem unavailable, terminating GetDatacenterLatencies"));
        return;
    }

    auto Config = UQuilkinConfigSubsystem::Get();

    TSharedRef<int64, ESPMode::ThreadSafe> RemainingRequests = MakeShareable(new int64);
    *RemainingRequests = Map.Endpoints.Num();

    Map.Endpoints.ForEach([this, &Config, RemainingRequests, OnCompletion](FQuilkinEndpoint Endpoint, CircularBuffer<int64> Buffer) {
        auto LatencyInMillis = uint64(NanosToMillis(Buffer.Median()));
        if (LatencyInMillis >= Config->GetPingThresholdMillis()) {
            UE_LOG(LogQuilkin, Verbose, TEXT("Skipping %s, measured latency (%dms) > ping threshold (%dms"), *Endpoint.ToString(), LatencyInMillis, Config->GetPingThresholdMillis());
            *RemainingRequests -= 1;
            if (*RemainingRequests == 0) {
                OnCompletion(this->Map.GetLowestLatencyToDatacenters());
            }
            return;
        }

        auto HttpRequest = FHttpModule::Get().CreateRequest();
        HttpRequest->SetURL(FString::Printf(TEXT("http://%s"), *Endpoint.ToQcmpInternetAddr(this).GetValue()->ToString(true)));
        HttpRequest->SetVerb("GET");
        HttpRequest->SetTimeout(0.5);

        HttpRequest->OnProcessRequestComplete().BindLambda([this, Endpoint, OnCompletion, RemainingRequests](FHttpRequestPtr Request, FHttpResponsePtr Response, bool bWasSuccessful) {
            if (!bWasSuccessful || !Response.IsValid())
            {
                UE_LOG(LogQuilkin, Verbose, TEXT("GetDatacenters failed for %s: (was successful: %s), (response is valid: %s)"), *Endpoint.ToQcmpInternetAddr(this).GetValue()->ToString(true), bWasSuccessful ? TEXT("true"): TEXT("false"), Response.IsValid() ? TEXT("true"): TEXT("false"));
                *RemainingRequests -= 1;
                if (*RemainingRequests == 0) {
                    OnCompletion(this->Map.GetLowestLatencyToDatacenters());
                }
                return;
            }

            FString JsonResponse = Response->GetContentAsString();

            TSharedPtr<FJsonObject> JsonObject;
            TSharedRef<TJsonReader<>> Reader = TJsonReaderFactory<>::Create(JsonResponse);
            if (FJsonSerializer::Deserialize(Reader, JsonObject) && JsonObject.IsValid())
            {
                TArray<FQuilkinDatacenter> DatacenterArray;

                for (auto& Elem : JsonObject->Values)
                {
                    double Distance;
                    FString IcaoCode = Elem.Key;
                    if (Elem.Value->TryGetNumber(Distance)) {
                        UE_LOG(LogQuilkin, Verbose, TEXT("%s has %dms latency to %s"), *Endpoint.ToString(), NanosToMillis(std::llround(Distance)), *IcaoCode);
                        DatacenterArray.Add(FQuilkinDatacenter {
                            IcaoCode,
                            Distance,
                        });
                    }
                }

                this->Map.Datacenters.Add(Endpoint, DatacenterArray);
            }

            *RemainingRequests -= 1;
            if (*RemainingRequests == 0) {
                OnCompletion(this->Map.GetLowestLatencyToDatacenters());
            }
        });

        HttpRequest->ProcessRequest();
    });
}

// MARK: ISocketSubsystem Interface

FSocket* FQuilkinSocketSubsystem::CreateSocket(const FName& SocketType, const FString& SocketDescription, bool bForceUDP) 
{ 
    return SocketSubsystem->CreateSocket(SocketType, SocketDescription, bForceUDP); 
}

FSocket* FQuilkinSocketSubsystem::CreateSocket(const FName& SocketType, const FString& SocketDescription, const FName& ProtocolName)
{
    FSocket* WrappedSocket = SocketSubsystem->CreateSocket(SocketType, SocketDescription, ProtocolName);
    if (WrappedSocket == nullptr)
    {
        UE_LOG(LogQuilkin, Warning, TEXT("CreateSocket returned nullptr"));
        return nullptr;
    }

    ESocketType InSocketType = WrappedSocket->GetSocketType();
    auto Socket = new FQuilkinSocket(FUniqueSocket(WrappedSocket), InSocketType, SocketDescription, ProtocolName);

    Socket->Subsystem = AsWeak();
    return Socket;
}

FSocket* FQuilkinSocketSubsystem::CreateIPv4Socket(const FString& SocketDescription) {
    FName SocketType = NAME_DGram;
    FSocket* WrappedSocket = SocketSubsystem->CreateSocket(SocketType, SocketDescription, FNetworkProtocolTypes::IPv4);
    if (WrappedSocket == nullptr)
    {
        UE_LOG(LogQuilkin, Warning, TEXT("CreateSocket returned nullptr"));
        return nullptr;
    }

    ESocketType InSocketType = WrappedSocket->GetSocketType();
    return WrappedSocket;
};

void FQuilkinSocketSubsystem::DestroySocket(FSocket* Socket)
{
    SocketSubsystem->DestroySocket(Socket);
}

FResolveInfoCached* FQuilkinSocketSubsystem::CreateResolveInfoCached(TSharedPtr<FInternetAddr> Addr) const 
{
    return SocketSubsystem->CreateResolveInfoCached(Addr);
}

void FQuilkinSocketSubsystem::MapAddressInfoResultToQuilkinAddrs(FAddressInfoResult& Result)
{
    for (auto& Entry : Result.Results) {
        Entry.Address = MakeShareable(new FQuilkinInternetAddrs(AsWeak(), Entry.Address));
    }
}

FAddressInfoResult FQuilkinSocketSubsystem::GetAddressInfo(const TCHAR* HostName, const TCHAR* ServiceName, EAddressInfoFlags QueryFlags, const FName ProtocolTypeName, ESocketType SocketType)
{
    auto Result = SocketSubsystem->GetAddressInfo(HostName, ServiceName, QueryFlags, ProtocolTypeName, SocketType);
    auto Cfg = UQuilkinConfigSubsystem::Get();

#if !WITH_SERVER_CODE
    if (Cfg->GetProxyFailover()) {
        MapAddressInfoResultToQuilkinAddrs(Result);
    }
#endif // !WITH_SERVER_CODE

#if WITH_SERVER_CODE
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
    SocketSubsystem->GetAddressInfoAsync([this, Callback](FAddressInfoResult Result) {
#if !WITH_SERVER_CODE
        if (UQuilkinConfigSubsystem::Get()->GetProxyFailover()) {
            MapAddressInfoResultToQuilkinAddrs(Result);
        }
#endif
        Callback(Result);
    }, HostName, ServiceName, QueryFlags, ProtocolTypeName, SocketType);
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

#if WITH_SERVER_CODE
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

