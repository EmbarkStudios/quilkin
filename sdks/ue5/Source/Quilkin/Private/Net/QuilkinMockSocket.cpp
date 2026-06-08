#include "QuilkinMockSocket.h"
#include "QuilkinSocketSubsystem.h"
#include "Math/UnrealMathUtility.h"
#include "Misc/AutomationTest.h"

FMockSocket::FMockSocket(const FString& SocketDescription)
    : Description(SocketDescription)
{
    BoundAddress = ISocketSubsystem::Get(PLATFORM_SOCKETSUBSYSTEM)->CreateInternetAddr();
    PeerAddress = ISocketSubsystem::Get(PLATFORM_SOCKETSUBSYSTEM)->CreateInternetAddr();
}

void FMockSocket::SetReceiveData(const TArray<TArray<uint8>>& DataPackets)
{
    ReceivePackets.Empty();
    for (const auto& Packet : DataPackets)
    {
		PendingData += Packet.Num();
        ReceivePackets.Enqueue(Packet);
    }
}

void FMockSocket::SetBlocking(bool bInShouldBlock)
{
    bShouldBlock = bInShouldBlock;
}

void FMockSocket::SetDelayEnabled(bool bInEnableDelay)
{
    bDelayEnabled = bInEnableDelay;
}

void FMockSocket::ClearSentData()
{
    SentPackets.Empty();
    AllSentData.Empty();
}

bool FMockSocket::HasPendingData(uint32& PendingDataSize)
{
	if (ReceivePackets.IsEmpty()) {
		PendingDataSize = 0;
		return false;
	}
	else {
		PendingDataSize = PendingData;
		return true;
	}
}

bool FMockSocket::Close()
{
    return true;
}

bool FMockSocket::Bind(const FInternetAddr& Addr)
{
    BoundAddress->SetRawIp(Addr.GetRawIp());
    BoundAddress->SetPort(Addr.GetPort());
    return true;
}

bool FMockSocket::Connect(const FInternetAddr& Addr)
{
    return false;
}

bool FMockSocket::Shutdown(ESocketShutdownMode Mode)
{
    return true;
}

bool FMockSocket::Listen(int32 MaxBacklog)
{
    return false;
}

FSocket* FMockSocket::Accept(const FString& InSocketDescription)
{
    // Return a new mock socket for accepted connection
    return new FMockSocket(InSocketDescription);
}

FSocket* FMockSocket::Accept(FInternetAddr& OutAddr, const FString& InSocketDescription)
{
    OutAddr.SetRawIp(PeerAddress->GetRawIp());
    OutAddr.SetPort(PeerAddress->GetPort());
    return new FMockSocket(InSocketDescription);
}

bool FMockSocket::SendTo(const uint8* Data, int32 Count, int32& BytesSent, const FInternetAddr& Destination)
{
    TArray<uint8> Packet(Data, Count);
    SentPackets.Add(Packet);
    AllSentData.Append(Data, Count);
    BytesSent = Count;
    return true;
}

bool FMockSocket::Send(const uint8* Data, int32 Count, int32& BytesSent)
{
    return SendTo(Data, Count, BytesSent, *PeerAddress);
}

bool FMockSocket::RecvFrom(uint8* Data, int32 BufferSize, int32& BytesRead, FInternetAddr& OutAddr, ESocketReceiveFlags::Type Flags)
{
    OutAddr.SetRawIp(PeerAddress->GetRawIp());
    OutAddr.SetPort(PeerAddress->GetPort());
    return ConsumeReceiveData(Data, BufferSize, BytesRead);
}

bool FMockSocket::Recv(uint8* Data, int32 BufferSize, int32& BytesRead, ESocketReceiveFlags::Type Flags)
{
    return ConsumeReceiveData(Data, BufferSize, BytesRead);
}

bool FMockSocket::ConsumeReceiveData(uint8* Data, int32 BufferSize, int32& BytesRead)
{
    BytesRead = 0;
    TArray<uint8> Packet;
	if (ReceivePackets.Dequeue(Packet))
	{
		const int32 CopyAmount = FMath::Min(BufferSize, Packet.Num());
		FMemory::Memcpy(Data, Packet.GetData(), CopyAmount);
		BytesRead = CopyAmount;
		PendingData -= BytesRead;
	}
	return true;
}

ESocketConnectionState FMockSocket::GetConnectionState()
{
    return ESocketConnectionState::SCS_NotConnected;
}

void FMockSocket::GetAddress(FInternetAddr& OutAddr)
{
    OutAddr.SetRawIp(BoundAddress->GetRawIp());
    OutAddr.SetPort(BoundAddress->GetPort());
}

bool FMockSocket::GetPeerAddress(FInternetAddr& OutAddr)
{
	return false;
}

bool FMockSocket::SetNonBlocking(bool bIsNonBlocking)
{
    bShouldBlock = !bIsNonBlocking;
    return true;
}

int32 FMockSocket::GetPortNo()
{
    return BoundAddress->GetPort();
}

bool FMockSocket::SetBroadcast(bool bAllowBroadcast) { return true; }
bool FMockSocket::SetReuseAddr(bool bAllowReuse) { return true; }
bool FMockSocket::SetLinger(bool bShouldLinger, int32 Timeout) { return true; }
bool FMockSocket::SetRecvErr(bool bUseErrorQueue) { return true; }
bool FMockSocket::WaitForPendingConnection(bool& bHasPendingConnection, const FTimespan& WaitTime) { return false; }
bool FMockSocket::SetSendBufferSize(int32 Size, int32& NewSize) { NewSize = Size; return true; }
bool FMockSocket::SetReceiveBufferSize(int32 Size, int32& NewSize) { NewSize = Size; return true; }

bool FMockSocket::Wait(ESocketWaitConditions::Type Condition, FTimespan WaitTime)
{
    switch (Condition)
    {
    case ESocketWaitConditions::WaitForRead:
		uint32 _Size;
        return HasPendingData(_Size) || !bShouldBlock;
    case ESocketWaitConditions::WaitForWrite:
        return true; // Always ready to write in mock
    case ESocketWaitConditions::WaitForReadOrWrite:
        return true; // Always ready for both in mock
    default:
        return false;
    }
}

bool FMockSocket::SetNoDelay(bool bNoDelay)
{
    return true;
}

bool FMockSocket::JoinMulticastGroup(const FInternetAddr& GroupAddress)
{
    return true;
}

bool FMockSocket::JoinMulticastGroup(const FInternetAddr& GroupAddress, const FInternetAddr& InterfaceAddress)
{
    return true;
}

bool FMockSocket::LeaveMulticastGroup(const FInternetAddr& GroupAddress)
{
    return true;
}

bool FMockSocket::LeaveMulticastGroup(const FInternetAddr& GroupAddress, const FInternetAddr& InterfaceAddress)
{
    return true;
}

bool FMockSocket::SetMulticastLoopback(bool bLoopback)
{
    return true;
}

bool FMockSocket::SetMulticastTtl(uint8 TimeToLive)
{
    return true;
}

bool FMockSocket::SetMulticastInterface(const FInternetAddr& InterfaceAddress)
{
    return true;
}

bool FMockSocket::RecvMulti(FRecvMulti& RecvMulti, ESocketReceiveFlags::Type Flags)
{
    return false;
}

bool FMockSocket::SetRetrieveTimestamp(bool bRetrieveTimestamp)
{
    return true;
}

bool FMockSocket::SetIpPktInfo(bool bIpPktInfo)
{
    return true;
}

bool FMockSocket::RecvFromWithPktInfo(uint8* Data, int32 BufferSize, int32& BytesRead, FInternetAddr& OutAddr, FInternetAddr& OutPktInfo, ESocketReceiveFlags::Type Flags)
{
    OutPktInfo.SetRawIp(PeerAddress->GetRawIp());
    OutPktInfo.SetPort(PeerAddress->GetPort());
    return RecvFrom(Data, BufferSize, BytesRead, OutAddr, Flags);
}
