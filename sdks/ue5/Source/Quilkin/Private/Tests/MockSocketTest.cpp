#include "CoreMinimal.h"
#include "Misc/AutomationTest.h"
#include "Tests/AutomationCommon.h"
#include "../Net/QuilkinSocketSubsystem.h"
#include "../Net/QuilkinMockSocket.h"

#if WITH_DEV_AUTOMATION_TESTS

IMPLEMENT_SIMPLE_AUTOMATION_TEST(FMockSocketBasicTest, "Quilkin.Net.MockSocket.Basic", EAutomationTestFlags::ApplicationContextMask | EAutomationTestFlags::SmokeFilter)

bool FMockSocketBasicTest::RunTest(const FString& Parameters)
{
    // FSocket()
	FMockSocket Socket;
    TestTrue(TEXT("Initial state should be not connected"), Socket.GetConnectionState() == ESocketConnectionState::SCS_NotConnected);
	uint32 PendingSize;
	Socket.HasPendingData(PendingSize);
    TestTrue(TEXT("Initial state should not have pending data"), PendingSize == 0);
    
    // FSocket::Bind
    TSharedRef<FInternetAddr> Addr = ISocketSubsystem::Get(PLATFORM_SOCKETSUBSYSTEM)->CreateInternetAddr();
    Addr->SetIp(0x7F000001); // 127.0.0.1
    Addr->SetPort(8080);
    
    TestTrue(TEXT("bind should succeed"), Socket.Bind(*Addr));
    TSharedRef<FInternetAddr> OutAddr = ISocketSubsystem::Get(PLATFORM_SOCKETSUBSYSTEM)->CreateInternetAddr();
	Socket.GetAddress(*OutAddr);
    TestTrue(TEXT("ip addr matches"), OutAddr->GetRawIp() == Addr->GetRawIp());
    TestTrue(TEXT("port matches"), OutAddr->GetPort() == Addr->GetPort());
    
    return true;
}

IMPLEMENT_SIMPLE_AUTOMATION_TEST(FMockSocketSendRecvTest, "Quilkin.Net.MockSocket.SendRecv", EAutomationTestFlags::ApplicationContextMask | EAutomationTestFlags::SmokeFilter)

bool FMockSocketSendRecvTest::RunTest(const FString& Parameters)
{
    FMockSocket Socket;
    TArray<uint8> TestData = {0x01, 0x02, 0x03, 0x04, 0x05};
    TArray<uint8> SendData = {0x0A, 0x0B, 0x0C};
    int32 BytesSent = 0;

	Socket.SetReceiveData({ TestData });
    
	// FSocket::Send
    TestTrue(TEXT("Send should succeed"), Socket.Send(SendData.GetData(), SendData.Num(), BytesSent));
    TestEqual(TEXT("BytesSent should match sent data"), BytesSent, SendData.Num());
    
    const TArray<TArray<uint8>>& SentPackets = Socket.GetSentPackets();
    TestEqual(TEXT("Should have one sent packet"), SentPackets.Num(), 1);
    TestEqual(TEXT("Sent packet should match original data"), SentPackets[0], SendData);
    
    int32 BytesRead = 0;
    TArray<uint8> RecvBuffer;
    RecvBuffer.SetNumZeroed(TestData.Num());

	// FSocket::Recv
    TestTrue(TEXT("Recv should succeed"), Socket.Recv(RecvBuffer.GetData(), RecvBuffer.Num(), BytesRead));
    TestEqual(TEXT("BytesRead should match test data"), BytesRead, TestData.Num());
    TestEqual(TEXT("Received data should match test data"), RecvBuffer, TestData);
    
    Socket.ClearSentData();
    TestEqual(TEXT("ClearSentData should clear history"), Socket.GetSentPackets().Num(), 0);
    
    return true;
}

#endif // WITH_DEV_AUTOMATION_TESTS
