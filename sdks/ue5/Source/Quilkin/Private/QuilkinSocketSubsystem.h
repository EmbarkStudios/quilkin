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

#include "QuilkinCircularBuffer.h"
#include "QuilkinConcurrentMap.h"
#include "QuilkinConstants.h"
#include "QuilkinDatacenter.h"
#include "QuilkinEndpoint.h"
#include "QuilkinEndpointMap.h"
#include "QuilkinInternetAddrs.h"
#include "QuilkinSocket.h"

struct FQuilkinEndpoint;

#define QUILKIN_SOCKETSUBSYSTEM_NAME TEXT("Quilkin")

class FQuilkinSocketSubsystem : public ISocketSubsystem, 
    public TSharedFromThis<FQuilkinSocketSubsystem>,
    public FTSTickerObjectBase
{
public:
    FSocket* PingSocket;
    FDelegateHandle UpdateEndpointHandle;
    FQueuedThreadPool* PingThreadPool;

    FQuilkinSocketSubsystem(ISocketSubsystem* WrappedSocketSubsystem);
    virtual ~FQuilkinSocketSubsystem();

    FSocket* CreateIPv4Socket(const FString& SocketDescription);
    bool Tick(float DeltaTime) override;


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

    TResult<int64, FString> SendPing(FSocket* Socket, FInternetAddr& Endpoint);
    TResult<uint8, FString> SendPacket(FSocket* Socket, FInternetAddr& Endpoint);

    TArray<FSocket*> PingSockets;
    FQuilkinEndpointMap Map;
protected:
    ISocketSubsystem* SocketSubsystem;
    FCriticalSection SocketAllocationLock;
    float TickElapsed = 0;

    template <typename Fn> void GetDatacenterLatencies(Fn OnCompletion);
    void AllocatePingSocketsForEndpoints();
    TResult<FSocket*, FString> CreateRandomUdpSocket();
    static TOptional<int64> WaitForResponses(FSocket* Socket, FInternetAddr& Endpoint, uint32 PingCount, TArray<uint8> Nonces);
    void PingEndpoints();
    void UpdateEndpoints(TArray<FQuilkinEndpoint>);
    void MapAddressInfoResultToQuilkinAddrs(FAddressInfoResult& Result);
};

class FPingTask : public FNonAbandonableTask
{
    friend class FAsyncTask<FPingTask>;
public:
    FPingTask(FQuilkinSocketSubsystem* InSubsystem, TArray<FQuilkinEndpoint>& InKeys, int32 TaskIndex)
        : Subsystem(InSubsystem)
        , Keys(InKeys)
        , Index(TaskIndex)
    {}

    void DoWork()
    {
        if (Index >= Keys.Num() || Index >= Subsystem->PingSockets.Num()) {
            UE_LOG(LogQuilkin, Warning, TEXT("Cancelling task as index greater than available sockets"));
            return;
        }

        auto Endpoint = Keys[Index];
        auto Socket = Subsystem->PingSockets[Index];
        auto EndpointResult = Endpoint.ToQcmpInternetAddr(Subsystem);

        if (EndpointResult.IsError()) {
            UE_LOG(LogQuilkin, Warning, TEXT("Couldn't resolve %s to an IP address"), *Endpoint.Host);
            return;
        }
        TSharedRef<FInternetAddr> Addr = EndpointResult.GetValue();
        FInternetAddr& Ptr = Addr.Get();
        auto PingResult = Subsystem->SendPing(Socket, Ptr);
        if (PingResult.IsError()) {
            UE_LOG(LogQuilkin, Warning, TEXT("ping for %s failed: %s"), *Endpoint.Host, *PingResult.GetError());
            return;
        }

        auto Latency = PingResult.GetValue();
        Subsystem->Map.Endpoints.FindOrDefaultToAdd(Endpoint, Latency);
    }

    FORCEINLINE TStatId GetStatId() const
    {
        RETURN_QUICK_DECLARE_CYCLE_STAT(FMyTask, STATGROUP_ThreadPoolAsyncTasks);
    }

private:
    FQuilkinSocketSubsystem* Subsystem;
    TArray<FQuilkinEndpoint>& Keys;
    int32 Index;
};

