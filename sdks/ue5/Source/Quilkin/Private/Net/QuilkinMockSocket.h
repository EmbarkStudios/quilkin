#pragma once

#include "Containers/Queue.h"
#include "Sockets.h"
#include "IPAddress.h"
#include "../Containers/QuilkinCircularBuffer.h"

class QUILKIN_API FMockSocket final : public FSocket
{
private:
    bool ConsumeReceiveData(uint8* Data, int32 BufferSize, int32& BytesRead);
    
    FString Description;
    ESocketConnectionState ConnectionState;
    
    bool bShouldBlock = true;
    bool bDelayEnabled = false;
	uint32 PendingData;
    
	// packet capture
    TQueue<TArray<uint8>> ReceivePackets;
    TArray<TArray<uint8>> SentPackets;
    TArray<uint8> AllSentData;
    
    // mock connection info
    TSharedPtr<FInternetAddr> BoundAddress;
    TSharedPtr<FInternetAddr> PeerAddress;
public:
    explicit FMockSocket(const FString& SocketDescription = TEXT("Mock Socket"));
    virtual ~FMockSocket() = default;

    // Configuration API for tests
    void SetReceiveData(const TArray<TArray<uint8>>& DataPackets);
    void SetBlocking(bool bInShouldBlock);
    void SetDelayEnabled(bool bInEnableDelay);
    void ClearSentData();
    const TArray<TArray<uint8>>& GetSentPackets() const { return SentPackets; }
    const TArray<uint8>& GetFlattenedSentData() const { return AllSentData; }

    // FSocket interface
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
};