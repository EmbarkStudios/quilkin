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

#include "QuilkinSocket.h"

#include "Containers/Ticker.h"
#include "Async/Async.h"
#include "Logging/StructuredLog.h"
#include "Templates/Casts.h"

#include "QuilkinConcurrentMap.h"
#include "QuilkinConstants.h"
#include "QuilkinSettings.h"
#include "QuilkinSocketSubsystem.h"

FQuilkinSocket::FQuilkinSocket(FUniqueSocket WrappedSocket, ESocketType InSocketType, const FString& InSocketDescription, const FName& InSocketProtocol)
    : FSocket(InSocketType, InSocketDescription, InSocketProtocol)
    , Socket{MoveTemp(WrappedSocket)}
    , Handler{}
{
}

FQuilkinSocket::~FQuilkinSocket()
{
}

bool FQuilkinSocket::Shutdown(ESocketShutdownMode Mode)
{
    return Socket.Get()->Shutdown(Mode);
}

bool FQuilkinSocket::Close()
{
    return Socket.Get()->Close();
}

bool FQuilkinSocket::Bind(const FInternetAddr& Addr)
{
    return Socket.Get()->Bind(Addr);
}

bool FQuilkinSocket::Connect(const FInternetAddr& Addr)
{
    return Socket.Get()->Connect(Addr);
}

bool FQuilkinSocket::Listen(int32 MaxBacklog)
{
    return Socket.Get()->Listen(MaxBacklog);
}

bool FQuilkinSocket::WaitForPendingConnection(bool& bHasPendingConnection, const FTimespan& WaitTime)
{
    return Socket.Get()->WaitForPendingConnection(bHasPendingConnection, WaitTime);
}

bool FQuilkinSocket::HasPendingData(uint32& PendingDataSize)
{
    return Socket.Get()->HasPendingData(PendingDataSize);
}

FSocket* FQuilkinSocket::Accept(const FString& InSocketDescription)
{
    return Socket.Get()->Accept(InSocketDescription);
}

FSocket* FQuilkinSocket::Accept(FInternetAddr& OutAddr, const FString& InSocketDescription)
{
    return Socket.Get()->Accept(OutAddr, InSocketDescription);
}

bool FQuilkinSocket::SendTo(const uint8* Data, int32 Count, int32& BytesSent, const FInternetAddr& Destination)
{
    return Handler.Write(Data, Count, BytesSent, [this, &Destination](const uint8* Data, int32 Count, int32& BytesSent) {
        auto Cfg = UQuilkinConfigSubsystem::Get();
        if (Cfg->GetEnabled() && Cfg->GetProxyFailover() && CurrentProxy.Addr.IsValid()) {
            UE_LOG(LogQuilkin, VeryVerbose, TEXT("proxy sendto: %s"), *CurrentProxy.Addr->ToString(true));
            const FInternetAddr& ProxyRef = *CurrentProxy.Addr;
            return Socket.Get()->SendTo(Data, Count, BytesSent, ProxyRef);
        }
        else {
            UE_LOG(LogQuilkin, VeryVerbose, TEXT("sendto: %s"), *Destination.ToString(true));

            // On the server we currently don't use our internet addr type, so we can skip checking it for now.
#if WITH_SERVER_CODE
            return Socket.Get()->SendTo(Data, Count, BytesSent, Destination);
#endif

            if (Cfg->GetProxyFailover()) {
                auto QuilkinAddr = static_cast<const FQuilkinInternetAddrs&>(Destination);
                const FInternetAddr& ProxyRef = *QuilkinAddr.BaseAddr;
                return Socket.Get()->SendTo(Data, Count, BytesSent, ProxyRef);
            }
            else {
                return Socket.Get()->SendTo(Data, Count, BytesSent, Destination);
            }
        }
    });
}

bool FQuilkinSocket::Send(const uint8* Data, int32 Count, int32& BytesSent)
{
    return Handler.Write(Data, Count, BytesSent, [this](const uint8* Data, int32 Count, int32& BytesSent) {
        return Socket.Get()->Send(Data, Count, BytesSent);
    });
}

bool FQuilkinSocket::RecvFrom(uint8* Data, int32 BufferSize, int32& BytesRead, FInternetAddr& Source, ESocketReceiveFlags::Type Flags)
{
    bool Success = Socket.Get()->RecvFrom(Data, BufferSize, BytesRead, Source, Flags);

    if (Success) {
        ResetTimeSinceLastPacket();
    }

    return Success;
}

bool FQuilkinSocket::Recv(uint8* Data, int32 BufferSize, int32& BytesRead, ESocketReceiveFlags::Type Flags)
{
    bool Success = Socket.Get()->Recv(Data, BufferSize, BytesRead, Flags);

    if (Success) {
        ResetTimeSinceLastPacket();
    }

    return Success;
}

bool FQuilkinSocket::Wait(ESocketWaitConditions::Type Condition, FTimespan WaitTime)
{
    return Socket.Get()->Wait(Condition, WaitTime);
}

ESocketConnectionState FQuilkinSocket::GetConnectionState()
{
    return Socket.Get()->GetConnectionState();

}

void FQuilkinSocket::GetAddress(FInternetAddr& OutAddr)
{
    return Socket.Get()->GetAddress(OutAddr);
}

bool FQuilkinSocket::GetPeerAddress(FInternetAddr& OutAddr)
{
    return Socket.Get()->GetPeerAddress(OutAddr);
}

bool FQuilkinSocket::SetNonBlocking(bool bIsNonBlocking)
{
    return Socket.Get()->SetNonBlocking(bIsNonBlocking);

}

bool FQuilkinSocket::SetBroadcast(bool bAllowBroadcast)
{
    return Socket.Get()->SetBroadcast(bAllowBroadcast);
}

bool FQuilkinSocket::SetNoDelay(bool bIsNoDelay)
{
    return Socket.Get()->SetNoDelay(bIsNoDelay);
}

bool FQuilkinSocket::JoinMulticastGroup(const FInternetAddr& GroupAddress)
{
    return Socket.Get()->JoinMulticastGroup(GroupAddress);
}

bool FQuilkinSocket::JoinMulticastGroup(const FInternetAddr& GroupAddress, const FInternetAddr& InterfaceAddress)
{
    return Socket.Get()->JoinMulticastGroup(GroupAddress, InterfaceAddress);
}

bool FQuilkinSocket::LeaveMulticastGroup(const FInternetAddr& GroupAddress)
{
    return Socket.Get()->LeaveMulticastGroup(GroupAddress);
}

bool FQuilkinSocket::LeaveMulticastGroup(const FInternetAddr& GroupAddress, const FInternetAddr& InterfaceAddress)
{
    return Socket.Get()->LeaveMulticastGroup(GroupAddress, InterfaceAddress);
}

bool FQuilkinSocket::SetMulticastLoopback(bool bLoopback)
{
    return Socket.Get()->SetMulticastLoopback(bLoopback);
}

bool FQuilkinSocket::SetMulticastTtl(uint8 TimeToLive)
{
    return Socket.Get()->SetMulticastTtl(TimeToLive);
}

bool FQuilkinSocket::SetMulticastInterface(const FInternetAddr& InterfaceAddress)
{
    return Socket.Get()->SetMulticastInterface(InterfaceAddress);
}

bool FQuilkinSocket::SetReuseAddr(bool bAllowReuse)
{
    return Socket.Get()->SetReuseAddr(bAllowReuse);
}

bool FQuilkinSocket::SetLinger(bool bShouldLinger, int32 Timeout)
{
    return Socket.Get()->SetLinger(bShouldLinger, Timeout);
}

bool FQuilkinSocket::SetRecvErr(bool bUseErrorQueue)
{
    return Socket.Get()->SetRecvErr(bUseErrorQueue);
}

bool FQuilkinSocket::SetSendBufferSize(int32 Size, int32& NewSize)
{
    return Socket.Get()->SetSendBufferSize(Size, NewSize);
}

bool FQuilkinSocket::SetReceiveBufferSize(int32 Size, int32& NewSize)
{
    return Socket.Get()->SetReceiveBufferSize(Size, NewSize);
}

int32 FQuilkinSocket::GetPortNo()
{
    return Socket.Get()->GetPortNo();
}

bool FQuilkinSocket::RecvMulti(FRecvMulti& MultiData, ESocketReceiveFlags::Type Flags)
{
    return Socket.Get()->RecvMulti(MultiData, Flags);
}

bool FQuilkinSocket::SetRetrieveTimestamp(bool bRetrieveTimestamp)
{
    return Socket.Get()->SetRetrieveTimestamp(bRetrieveTimestamp);
}

bool FQuilkinSocket::SetIpPktInfo(bool bEnable)
{
    return Socket.Get()->SetIpPktInfo(bEnable);
}

bool FQuilkinSocket::RecvFromWithPktInfo(uint8* Data, int32 BufferSize, int32& BytesRead, FInternetAddr& Source, FInternetAddr& Destination, ESocketReceiveFlags::Type Flags)
{
    return Socket.Get()->RecvFromWithPktInfo(Data, BufferSize, BytesRead, Source, Destination, Flags);
}

/** Tracks the last time a packet was received by the client, and if it exceeds JitterThreshold,
  * finds the next lowest latency proxy to use.
  */
bool FQuilkinSocket::Tick(float DeltaTime)
{
    // On server just terminate the tick task as it's not used.
#if WITH_SERVER_CODE
    UE_LOG(LogQuilkin, Verbose, TEXT("disabling socket tick"));
    return false;
#endif // !WITH_SERVER_CODE

    auto Cfg = UQuilkinConfigSubsystem::Get();
    if (!Cfg->GetProxyFailover() || !Cfg->GetEnabled()) {
        return true;
    }

    IncrementTimeSinceLastPacket(DeltaTime);

    if (OnCooldown()) {
        return true;
    }

    auto SubSys = Subsystem.Pin();
    if (!CurrentProxy.Addr.IsValid() && !SubSys->Map.Endpoints.IsEmpty()) {
        StartCooldown(Cfg->GetCooldown());
        auto Found = SubSys->Map.GetLowestLatencyEndpoint(IcaoCode);

        if (!Found.IsSet()) {
            UE_LOG(LogQuilkin, Verbose, TEXT("no measured endpoints available to initialise failover"));
            return true;
        }

        auto Pair = Found.GetValue();
        auto Result = Pair.Key.ToInternetAddr(SubSys.Get());
        if (Result.IsSuccess()) {
            CurrentProxy.Addr = Result.GetValue();
            CurrentProxy.Latency = Pair.Value;
        }
        else {
            UE_LOGFMT(
                LogQuilkin,
                Warning,
                "unable to resolve endpoint to address: {0}",
                Pair.Key.ToString()
            );
        }
    } else if (ShouldSwitchProxies(Cfg)) {
        UE_LOGFMT(
            LogQuilkin,
            Warning,
            "{0} seconds since last packet, exceeded threshold ({1}s), switching proxies",
            SecondsSinceLastPacket,
            Cfg->GetJitterThreshold()
        );

        SwitchProxies(Cfg, SubSys.Get());
    }

    return true;
}

void FQuilkinSocket::IncrementTimeSinceLastPacket(float DeltaTime) {
    if (Handler.IsEnabled()) {
        SecondsSinceLastPacket += DeltaTime;
    }
    else {
        SecondsSinceLastPacket = 0;
    }

    if (CurrentCooldown.IsSet()) {
        float NewCooldown = CurrentCooldown.GetValue() - DeltaTime;

        if (NewCooldown <= 0) {
            CurrentCooldown.Reset();
        }
        else {
            CurrentCooldown.Emplace(NewCooldown);
        }
    }
}

void FQuilkinSocket::ResetTimeSinceLastPacket() { SecondsSinceLastPacket = 0; }

void FQuilkinSocket::StartCooldown(float Cooldown) {
    CurrentCooldown = TOptional<float>(Cooldown);
}

bool FQuilkinSocket::ShouldSwitchProxies(UQuilkinConfigSubsystem* Cfg) {
    bool NotOnCooldown = !OnCooldown();
    bool HasExceededJitterThreshold = SecondsSinceLastPacket >= Cfg->GetJitterThreshold();
    return (NotOnCooldown && HasExceededJitterThreshold);
}

void FQuilkinSocket::SwitchProxies(UQuilkinConfigSubsystem* Cfg, FQuilkinSocketSubsystem* SubSys) {
    StartCooldown(Cfg->GetCooldown());
    ResetTimeSinceLastPacket();

    // If there's only one endpoint available, there's nothing to switch to.
    if (SubSys->Map.Endpoints.Num() <= 1) {
        UE_LOG(LogQuilkin, Display, TEXT("only one endpoint available, not switching"));
        return;
    }

    // If there's only one endpoint left, clear the other attempts.
    if (FailedProxyAddrs.Num() >= SubSys->Map.Endpoints.Num() - 1) {
        UE_LOG(LogQuilkin, Warning, TEXT("Tried all available proxies, resetting list of proxies available"));
        FailedProxyAddrs.Empty();
    }

    TOptional<EndpointPair> Result = SubSys->Map.FindNextLowestLatencyEndpoint(SubSys, FailedProxyAddrs);

    if (Result.IsSet()) {
        EndpointPair Pair = Result.GetValue();
        float EndpointLatency = Pair.Value;
        if (FQuilkinSocket::NextProxyExceedsThreshold(EndpointLatency, Cfg->GetJitterThreshold())) {
            UE_LOGFMT(
                LogQuilkin,
                Display,
                "next available endpoint exceeds threshold ({0}s > {1}s), attempting previous {2} proxies",
                EndpointLatency,
                Cfg->GetJitterThreshold(),
                FailedProxyAddrs.Num()
            );
            FailedProxyAddrs.Empty();
            Result = SubSys->Map.FindNextLowestLatencyEndpoint(SubSys, FailedProxyAddrs);

            if (Result.IsSet()) {
                Pair = Result.GetValue();
                ReplaceProxyAddr(Pair.Key.ToInternetAddr(SubSys).GetValue(), Pair.Value);
                return;
            }
        }
        else {
            ReplaceProxyAddr(Pair.Key.ToInternetAddr(SubSys).GetValue(), Pair.Value);
            return;
        }
    }

    UE_LOG(
        LogQuilkin,
        Warning,
        TEXT("Attempted to connect to different proxy address, but none were available")
    );
}

bool FQuilkinSocket::NextProxyExceedsThreshold(int64 EndpointLatency, float Threshold) {
    EndpointLatency = NanosToSeconds(EndpointLatency);
    return (EndpointLatency > Threshold);
}