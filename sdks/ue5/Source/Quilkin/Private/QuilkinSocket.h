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

#include <atomic>

#include "CoreMinimal.h"
#include "Sockets.h"
#include "SocketSubsystem.h"

#include "QuilkinSettings.h"
#include "QuilkinConstants.h"
#include "QuilkinDelegates.h"
#include "QuilkinCircularBuffer.h"
#include "QuilkinEndpoint.h"
#include "QuilkinPacketHandler.h"
#include "Containers/Ticker.h"

class FQuilkinSocketSubsystem;

class FQuilkinSocket: 
    public FSocket,
    public FTSTickerObjectBase
{
public:
    FQuilkinSocket(FUniqueSocket WrappedSocket, ESocketType InSocketType, const FString& InSocketDescription, const FName& InSocketProtocol);
    virtual ~FQuilkinSocket();

    static bool NextProxyExceedsThreshold(int64 EndpointLatency, float Threshold);

    //~ Begin FSocket Interface
    virtual bool Shutdown(ESocketShutdownMode Mode) override;
    virtual bool Close() override;
    virtual bool Bind(const FInternetAddr& Addr) override;
    virtual bool Connect(const FInternetAddr& Addr) override;
    virtual bool Listen(int32 MaxBacklog) override;
    virtual bool WaitForPendingConnection(bool& bHasPendingConnection, const FTimespan& WaitTime) override;
    virtual bool HasPendingData(uint32& PendingDataSize) override;
    virtual FSocket* Accept(const FString& InSocketDescription) override;
    virtual FSocket* Accept(FInternetAddr& OutAddr, const FString& InSocketDescription) override;
    virtual bool SendTo(const uint8* Data, int32 Count, int32& BytesSent, const FInternetAddr& Destination) override;
    virtual bool Send(const uint8* Data, int32 Count, int32& BytesSent) override;
    virtual bool RecvFrom(uint8* Data, int32 BufferSize, int32& BytesRead, FInternetAddr& Source, ESocketReceiveFlags::Type Flags = ESocketReceiveFlags::None) override;
    virtual bool Recv(uint8* Data, int32 BufferSize, int32& BytesRead, ESocketReceiveFlags::Type Flags = ESocketReceiveFlags::None) override;
    virtual bool Wait(ESocketWaitConditions::Type Condition, FTimespan WaitTime) override;
    virtual ESocketConnectionState GetConnectionState() override;
    virtual void GetAddress(FInternetAddr& OutAddr) override;
    virtual bool GetPeerAddress(FInternetAddr& OutAddr) override;
    virtual bool SetNonBlocking(bool bIsNonBlocking = true) override;
    virtual bool SetBroadcast(bool bAllowBroadcast = true) override;
    virtual bool SetNoDelay(bool bIsNoDelay = true) override;
    virtual bool JoinMulticastGroup(const FInternetAddr& GroupAddress) override;
    virtual bool JoinMulticastGroup(const FInternetAddr& GroupAddress, const FInternetAddr& InterfaceAddress) override;
    virtual bool LeaveMulticastGroup(const FInternetAddr& GroupAddress) override;
    virtual bool LeaveMulticastGroup(const FInternetAddr& GroupAddress, const FInternetAddr& InterfaceAddress) override;
    virtual bool SetMulticastLoopback(bool bLoopback) override;
    virtual bool SetMulticastTtl(uint8 TimeToLive) override;
    virtual bool SetMulticastInterface(const FInternetAddr& InterfaceAddress) override;
    virtual bool SetReuseAddr(bool bAllowReuse = true) override;
    virtual bool SetLinger(bool bShouldLinger = true, int32 Timeout = 0) override;
    virtual bool SetRecvErr(bool bUseErrorQueue = true) override;
    virtual bool SetSendBufferSize(int32 Size, int32& NewSize) override;
    virtual bool SetReceiveBufferSize(int32 Size, int32& NewSize) override;
    virtual int32 GetPortNo() override;
    virtual bool RecvMulti(FRecvMulti& MultiData, ESocketReceiveFlags::Type Flags=ESocketReceiveFlags::None) override;
    virtual bool SetRetrieveTimestamp(bool bRetrieveTimestamp=true) override;
    virtual bool SetIpPktInfo(bool bEnable) override;
    virtual bool RecvFromWithPktInfo(uint8* Data, int32 BufferSize, int32& BytesRead, FInternetAddr& Source, FInternetAddr& Destination, ESocketReceiveFlags::Type Flags = ESocketReceiveFlags::None) override;
    //~ End FSocket Interface

    //~ Begin FTSTickerObjectBase Interface
    bool Tick(float DeltaTime) override;
    //~ End FTSTickerObjectBase Interface

    TWeakPtr<FQuilkinSocketSubsystem> Subsystem;
protected:
    FUniqueSocket Socket;
    FQuilkinPacketHandler Handler;

    struct {
        TSharedPtr<FInternetAddr> Addr;
        int64 Latency;
    } CurrentProxy;
    FString IcaoCode;
    TArray<TSharedPtr<FInternetAddr>> FailedProxyAddrs;
    float SecondsSinceLastPacket = 0;
    TOptional<float> CurrentCooldown = TOptional<float>();

    void IncrementTimeSinceLastPacket(float DeltaTime);
    void ResetTimeSinceLastPacket();
    void StartCooldown(float Cooldown);
    bool ShouldSwitchProxies(UQuilkinConfigSubsystem* Cfg);
    void SwitchProxies(UQuilkinConfigSubsystem* Cfg, FQuilkinSocketSubsystem* SubSys);

    void ReplaceProxyAddr(TSharedPtr<FInternetAddr> NewProxyAddr, int64 NewLatency) {
        FailedProxyAddrs.Add(CurrentProxy.Addr);
        auto OldProxy = CurrentProxy;
        CurrentProxy = {NewProxyAddr, NewLatency}; 

        if (FQuilkinDelegates::FailoverTriggered.IsBound()) {
            FQuilkinDelegates::FailoverTriggered.Broadcast(
                TPair<TSharedPtr<FInternetAddr>, int64>(OldProxy.Addr, OldProxy.Latency),
                TPair<TSharedPtr<FInternetAddr>, int64>(CurrentProxy.Addr, CurrentProxy.Latency)
            );
        }
    }

    bool OnCooldown() {
        return CurrentCooldown.IsSet() && CurrentCooldown.GetValue() > 0;
    }
};