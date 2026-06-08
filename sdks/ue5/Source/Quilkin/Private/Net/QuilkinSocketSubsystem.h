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

#pragma once

#include <cmath>

#include "CoreMinimal.h"
#include "Async/Async.h"
#include "Async/AsyncWork.h"
#include "Misc/QueuedThreadPool.h"
#include "SocketSubsystem.h"
#include "Containers/Ticker.h"

#include "QuilkinEndpoint.h"
#include "../Containers/QuilkinEndpointMap.h"

struct FQuilkinEndpoint;
struct FQuilkinTopology;
class FQuilkinSocket;

struct FPingEndpointResult
{
	int32 Sent = 0;
	CircularBuffer<int64> Latencies;
	TArray<FString> Errors;
};

#define QUILKIN_SOCKETSUBSYSTEM_NAME TEXT("Quilkin")

class FQuilkinSocketSubsystem : public ISocketSubsystem, 
	public TSharedFromThis<FQuilkinSocketSubsystem>,
    public FTSTickerObjectBase
{
public:
	FQuilkinSocketSubsystem(ISocketSubsystem* WrappedSocketSubsystem);
	virtual ~FQuilkinSocketSubsystem();

	bool Tick(float DeltaTime) override;
	static QUILKIN_API FQuilkinSocketSubsystem* Instance() {
		return static_cast<FQuilkinSocketSubsystem*>(Get(QUILKIN_SOCKETSUBSYSTEM_NAME));
	}

	//~ Begin ISocketSubsystem Interface
	virtual bool Init(FString& Error) override;
	virtual void Shutdown() override;
	virtual FSocket* CreateSocket(const FName& SocketType, const FString& SocketDescription, bool bForceUDP) override;
	virtual FSocket* CreateSocket(const FName& SocketType, const FString& SocketDescription, const FName& ProtocolName) override;
	virtual void DestroySocket(FSocket* Socket) override;
	virtual class FResolveInfoCached* CreateResolveInfoCached(TSharedPtr<FInternetAddr> Addr) const override;
	virtual FAddressInfoResult GetAddressInfo(const TCHAR* HostName, const TCHAR* ServiceName = nullptr, EAddressInfoFlags QueryFlags = EAddressInfoFlags::Default, const FName ProtocolTypeName = NAME_None, ESocketType SocketType = ESocketType::SOCKTYPE_Unknown) override;
	virtual void GetAddressInfoAsync(FAsyncGetAddressInfoCallback Callback, const TCHAR* HostName,
		const TCHAR* ServiceName, EAddressInfoFlags QueryFlags,
		const FName ProtocolTypeName,
		ESocketType SocketType) override;
	virtual TSharedPtr<FInternetAddr> GetAddressFromString(const FString& InAddress) override;
	virtual class FResolveInfo* GetHostByName(const ANSICHAR* HostName);
	virtual bool RequiresChatDataBeSeparate() override;
	virtual bool RequiresEncryptedPackets() override;
	virtual bool GetHostName(FString& HostName) override;
	virtual TSharedRef<FInternetAddr> CreateInternetAddr() override;
	virtual TSharedRef<FInternetAddr> CreateInternetAddr(const FName ProtocolType) override;
	virtual TSharedRef<FInternetAddr> GetLocalBindAddr(FOutputDevice& Out) override;
	virtual TArray<TSharedRef<FInternetAddr>> GetLocalBindAddresses() override;
	virtual bool GetLocalAdapterAddresses(TArray<TSharedPtr<FInternetAddr>>& OutAddresses) override;
	virtual TUniquePtr<FRecvMulti> CreateRecvMulti(int32 MaxNumPackets, int32 MaxPacketSize,
		ERecvMultiFlags Flags = ERecvMultiFlags::None) override;
	virtual TSharedRef<FInternetAddr> GetLocalHostAddr(FOutputDevice& Out, bool& bCanBindAll) override;
	virtual bool GetMultihomeAddress(TSharedRef<FInternetAddr>& Addr) override;
	virtual bool HasNetworkDevice() override;
	virtual const TCHAR* GetSocketAPIName() const override;
	virtual ESocketErrors GetLastErrorCode() override;
	virtual ESocketErrors TranslateErrorCode(int32 Code) override;
	virtual bool IsSocketRecvMultiSupported() const override;
	virtual bool IsSocketWaitSupported() const override;
	virtual double TranslatePacketTimestamp(const FPacketTimestamp& Timestamp,
		ETimestampTranslation Translation) override;
	virtual bool IsRecvFromWithPktInfoSupported() const override;
	//~ End ISocketSubsystem Interface

	FUniqueSocket SetupNonblockingUDPSocket(const FString& Name) const;
	void UpdateEndpoints(TArray<FQuilkinEndpoint>&& Endpoints, TArray<FQuilkinTopology>&& Topologies);

	struct PingSettings
	{
		uint32 Count;
		double TimeoutSec;
		float SleepAfterRecvSec;
	};
	TArray<FPingEndpointResult> Ping(
		const FUniqueSocket& Socket,
		const TArray<FQuilkinEndpoint>& Endpoints,
		const PingSettings& Settings = { .Count = 5, .TimeoutSec = 1.0, .SleepAfterRecvSec = 1.0 / 1'000 });

	FQuilkinEndpointMap Map;
protected:
	ISocketSubsystem* SocketSubsystem;
	float TickElapsed = 0;
	
private:
	bool PingingInAction = false;
};