#if WITH_DEV_AUTOMATION_TESTS

#include "CoreMinimal.h"
#include "Misc/AutomationTest.h"

#include "../Net/QuilkinPacketHandler.h"
#include "QuilkinSettings.h"

namespace
{
	TArray<uint8> MakeToken(uint8 Fill)
	{
		TArray<uint8> Token;
		Token.Init(Fill, 16);
		return Token;
	}

	TArray<uint8> CaptureWrite(FQuilkinPacketHandler& Handler, const TArray<uint8>& Payload)
	{
		int32 BytesSent = 0;
		TArray<uint8> Captured;
		Handler.Write(Payload.GetData(), Payload.Num(), BytesSent,
			[&](const uint8* Data, int32 Count, int32& OutBytesSent) -> bool
			{
				Captured = TArray<uint8>(Data, Count);
				OutBytesSent = Count;
				return true;
			});
		return Captured;
	}

	struct FSavedSubsystemState
	{
		bool bEnabled;
		bool bPacketHandling;
		TArray<uint8> RoutingToken;
	};

	FSavedSubsystemState ConfigureSubsystem(const TArray<uint8>& Token, bool bEnabled)
	{
		UQuilkinConfigSubsystem* Config = UQuilkinConfigSubsystem::Get();
		FSavedSubsystemState Saved{ Config->GetEnabled(), Config->PacketHandling, Config->GetRoutingToken() };
		Config->SetEnabled(bEnabled);
		Config->PacketHandling = bEnabled;
		Config->SetRoutingToken(Token);
		return Saved;
	}

	void RestoreSubsystem(const FSavedSubsystemState& Saved)
	{
		UQuilkinConfigSubsystem* Config = UQuilkinConfigSubsystem::Get();
		Config->SetEnabled(Saved.bEnabled);
		Config->PacketHandling = Saved.bPacketHandling;
		Config->SetRoutingToken(Saved.RoutingToken);
	}
}

// When disabled the handler must forward the original bytes to the socket unchanged.
IMPLEMENT_SIMPLE_AUTOMATION_TEST(FQuilkinPacketHandlerDisabledPassthroughTest,
	"Quilkin.PacketHandler.DisabledPassthrough",
	EAutomationTestFlags::EditorContext | EAutomationTestFlags::EngineFilter)

bool FQuilkinPacketHandlerDisabledPassthroughTest::RunTest(const FString& Parameters)
{
	const FSavedSubsystemState Saved = ConfigureSubsystem(MakeToken(0xAB), /*bEnabled=*/false);

	FQuilkinPacketHandler Handler;

	const TArray<uint8> Payload = { 0x01, 0x02, 0x03, 0x04, 0x05 };
	const TArray<uint8> Captured = CaptureWrite(Handler, Payload);

	TestEqual(TEXT("Disabled: byte count unchanged"), Captured.Num(), Payload.Num());
	TestEqual(TEXT("Disabled: bytes unchanged"), Captured, Payload);

	RestoreSubsystem(Saved);
	return true;
}

// When enabled the handler must produce [original data][16-byte routing token][version byte 0].
IMPLEMENT_SIMPLE_AUTOMATION_TEST(FQuilkinPacketHandlerEnabledPacketFormatTest,
	"Quilkin.PacketHandler.EnabledPacketFormat",
	EAutomationTestFlags::EditorContext | EAutomationTestFlags::EngineFilter)

bool FQuilkinPacketHandlerEnabledPacketFormatTest::RunTest(const FString& Parameters)
{
	const TArray<uint8> Token = MakeToken(0xCC);
	const FSavedSubsystemState Saved = ConfigureSubsystem(Token, /*bEnabled=*/true);

	FQuilkinPacketHandler Handler;

	const TArray<uint8> Payload = { 0xAA, 0xBB, 0xCC, 0xDD };
	const TArray<uint8> Captured = CaptureWrite(Handler, Payload);

	const int32 ExpectedSize = Payload.Num() + Token.Num() + 1 /* version */;
	TestEqual(TEXT("Enabled: total size = payload + token + version"), Captured.Num(), ExpectedSize);

	for (int32 i = 0; i < Payload.Num(); ++i)
	{
		TestEqual(FString::Printf(TEXT("Enabled: payload byte %d intact"), i), Captured[i], Payload[i]);
	}

	for (int32 i = 0; i < Token.Num(); ++i)
	{
		TestEqual(FString::Printf(TEXT("Enabled: token byte %d at offset %d"), i, Payload.Num() + i),
			Captured[Payload.Num() + i], Token[i]);
	}

	TestEqual(TEXT("Enabled: version byte is 0"), Captured.Last(), static_cast<uint8>(0));

	RestoreSubsystem(Saved);
	return true;
}

// Different routing tokens must produce different packets, embedded at the correct offset.
IMPLEMENT_SIMPLE_AUTOMATION_TEST(FQuilkinPacketHandlerTokenEmbeddedTest,
	"Quilkin.PacketHandler.TokenEmbedded",
	EAutomationTestFlags::EditorContext | EAutomationTestFlags::EngineFilter)

bool FQuilkinPacketHandlerTokenEmbeddedTest::RunTest(const FString& Parameters)
{
	const TArray<uint8> Payload = { 0x01, 0x02, 0x03 };

	const FSavedSubsystemState SavedA = ConfigureSubsystem(MakeToken(0x11), /*bEnabled=*/true);
	FQuilkinPacketHandler HandlerA;
	const TArray<uint8> OutputA = CaptureWrite(HandlerA, Payload);
	RestoreSubsystem(SavedA);

	const FSavedSubsystemState SavedB = ConfigureSubsystem(MakeToken(0x22), /*bEnabled=*/true);
	FQuilkinPacketHandler HandlerB;
	const TArray<uint8> OutputB = CaptureWrite(HandlerB, Payload);
	RestoreSubsystem(SavedB);

	TestNotEqual(TEXT("Different tokens produce different packets"), OutputA, OutputB);

	const int32 TokenOffset = Payload.Num();
	for (int32 i = 0; i < 16; ++i)
	{
		TestEqual(FString::Printf(TEXT("Token A byte %d"), i), OutputA[TokenOffset + i], static_cast<uint8>(0x11));
		TestEqual(FString::Printf(TEXT("Token B byte %d"), i), OutputB[TokenOffset + i], static_cast<uint8>(0x22));
	}

	return true;
}

// The return value from the socket callback must be propagated back to the caller.
IMPLEMENT_SIMPLE_AUTOMATION_TEST(FQuilkinPacketHandlerWriteReturnValueTest,
	"Quilkin.PacketHandler.WriteReturnValuePropagated",
	EAutomationTestFlags::EditorContext | EAutomationTestFlags::EngineFilter)

bool FQuilkinPacketHandlerWriteReturnValueTest::RunTest(const FString& Parameters)
{
	const TArray<uint8> Payload = { 0x01 };
	int32 BytesSent = 0;

	const FSavedSubsystemState Saved = ConfigureSubsystem(MakeToken(0xAB), /*bEnabled=*/true);
	FQuilkinPacketHandler Handler;

	const bool bSuccess = Handler.Write(Payload.GetData(), Payload.Num(), BytesSent,
		[](const uint8*, int32 Count, int32& Out) -> bool { Out = Count; return true; });
	const bool bFailure = Handler.Write(Payload.GetData(), Payload.Num(), BytesSent,
		[](const uint8*, int32, int32&) -> bool { return false; });

	TestTrue(TEXT("Propagates true from socket callback"), bSuccess);
	TestFalse(TEXT("Propagates false from socket callback"), bFailure);

	RestoreSubsystem(Saved);
	return true;
}

// An empty payload when enabled must produce exactly token + version bytes with the correct structure.
IMPLEMENT_SIMPLE_AUTOMATION_TEST(FQuilkinPacketHandlerEmptyPayloadTest,
	"Quilkin.PacketHandler.EnabledEmptyPayload",
	EAutomationTestFlags::EditorContext | EAutomationTestFlags::EngineFilter)

bool FQuilkinPacketHandlerEmptyPayloadTest::RunTest(const FString& Parameters)
{
	const FSavedSubsystemState Saved = ConfigureSubsystem(MakeToken(0xAB), /*bEnabled=*/true);
	FQuilkinPacketHandler Handler;

	const TArray<uint8> EmptyPayload;
	const TArray<uint8> Captured = CaptureWrite(Handler, EmptyPayload);

	TestEqual(TEXT("Empty payload: output size is token + version"), Captured.Num(), 16 + 1);
	TestEqual(TEXT("Empty payload: version byte is 0"), Captured.Last(), static_cast<uint8>(0));

	RestoreSubsystem(Saved);
	return true;
}

#endif // WITH_DEV_AUTOMATION_TESTS
