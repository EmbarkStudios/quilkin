#if WITH_DEV_AUTOMATION_TESTS

#include "CoreMinimal.h"
#include "Misc/AutomationTest.h"

#include "../Net/QuilkinGameMessageProtocol.h"

// Encoding RoutingToken={0xDE,0xAD,0xBE,0xEF}, Latency=[] must produce the exact byte sequence below.
//
// Wire format:  [CBOR map][u16 length, big-endian]["QLKN"][version=1]
//
// CBOR map (10 bytes):
//   0xA2                       — map, 2 entries
//   0x00                       — key 0 (RoutingToken)
//   0x44 0xDE 0xAD 0xBE 0xEF  — byte string, 4 bytes
//   0x01                       — key 1 (Latency)
//   0x9F 0xFF                  — indefinite array begin + break (empty)
//
// Trailer (7 bytes):
//   0x00, 0x0A  — CBOR length = 10
//   'Q','L','K','N'
//   0x01        — version
IMPLEMENT_SIMPLE_AUTOMATION_TEST(FQuilkinGameMessageEncoderKnownBytesTest,
	"Quilkin.GameMessageProtocol.Encoder.KnownBytes",
	EAutomationTestFlags::ApplicationContextMask | EAutomationTestFlags::SmokeFilter)

bool FQuilkinGameMessageEncoderKnownBytesTest::RunTest(const FString& Parameters)
{
	const TArray<uint8> Expected = {
		// CBOR map, 2 entries
		0xA2,
		// key 0: RoutingToken — byte string, 4 bytes
		0x00, 0x44, 0xDE, 0xAD, 0xBE, 0xEF,
		// key 1: Latency — indefinite array, empty
		0x01, 0x9F, 0xFF,
		// Trailer
		0x00, 0x0A, // CBOR length = 10
		'Q', 'L', 'K', 'N',
		0x01,       // version = 1
	};

	FQuilkinGameMessage Message;
	Message.RoutingToken = { 0xDE, 0xAD, 0xBE, 0xEF };

	TArray<uint8> Encoded;
	Message.Encode(Encoded);
	TestEqual(TEXT("Encoded bytes match expected"), Encoded, Expected);
	return true;
}

// Decoding the known byte sequence must recover all fields.
IMPLEMENT_SIMPLE_AUTOMATION_TEST(FQuilkinGameMessageDecoderKnownBytesTest,
	"Quilkin.GameMessageProtocol.Decoder.KnownBytes",
	EAutomationTestFlags::ApplicationContextMask | EAutomationTestFlags::SmokeFilter)

bool FQuilkinGameMessageDecoderKnownBytesTest::RunTest(const FString& Parameters)
{
	const TArray<uint8> Input = {
		0xA2,
		0x00, 0x44, 0xDE, 0xAD, 0xBE, 0xEF,
		0x01, 0x9F, 0xFF,
		0x00, 0x0A,
		'Q', 'L', 'K', 'N',
		0x01,
	};

	const auto Result = FQuilkinGameMessage::Decode(Input);
	TestTrue(TEXT("Decode succeeds"), Result.HasValue());

	const TArray<uint8> ExpectedToken = { 0xDE, 0xAD, 0xBE, 0xEF };
	TestEqual(TEXT("RoutingToken matches"), Result.GetValue().RoutingToken, ExpectedToken);
	TestEqual(TEXT("Latency is empty"), Result.GetValue().Latency.Num(), 0);
	return true;
}

// Encode then decode must recover all fields.
IMPLEMENT_SIMPLE_AUTOMATION_TEST(FQuilkinGameMessageRoundTripTest,
	"Quilkin.GameMessageProtocol.RoundTrip",
	EAutomationTestFlags::ApplicationContextMask | EAutomationTestFlags::SmokeFilter)

bool FQuilkinGameMessageRoundTripTest::RunTest(const FString& Parameters)
{
	FQuilkinGameMessage Original;
	Original.RoutingToken.Init(0xAB, 16);

	TArray<uint8> Encoded;
	Original.Encode(Encoded);

	const auto Result = FQuilkinGameMessage::Decode(Encoded);
	TestTrue(TEXT("Round-trip: decode succeeds"), Result.HasValue());
	TestEqual(TEXT("Round-trip: RoutingToken"), Result.GetValue().RoutingToken, Original.RoutingToken);
	TestEqual(TEXT("Round-trip: Latency empty"), Result.GetValue().Latency.Num(), 0);
	return true;
}

// A message with three hops must round-trip all hop fields and produce correct latency values.
//
// Hop layout (all timestamps in nanoseconds):
//   [0] client:        Start=1000, Transmit=1000  (no processing time at client)
//   [1] proxy ingress: Start=1100, Transmit=1120  (20 ns processing)
//   [2] gameserver:    Start=1200, Transmit=1250  (50 ns processing)
//   [3] proxy egress:  Start=1350, Transmit=1360  (10 ns processing)
//
// GetHopLatency(1) = 1100 - 1000 = 100   (client → proxy ingress)
// GetHopLatency(2) = 1200 - 1120 = 80    (proxy ingress → gameserver)
// GetHopLatency(3) = 1350 - 1250 = 100   (gameserver → proxy egress)
//
// GetTotalRoundTripTime(ClientReceiveTimestamp=1500):
//   total elapsed   = 1500 - 1000 = 500
//   total processing = (1000-1000) + (1120-1100) + (1250-1200) + (1360-1350) = 0+20+50+10 = 80
//   RTT             = 500 - 80 = 420
IMPLEMENT_SIMPLE_AUTOMATION_TEST(FQuilkinGameMessageHopsRoundTripTest,
	"Quilkin.GameMessageProtocol.RoundTrip.WithHops",
	EAutomationTestFlags::ApplicationContextMask | EAutomationTestFlags::SmokeFilter)

bool FQuilkinGameMessageHopsRoundTripTest::RunTest(const FString& Parameters)
{
	FQuilkinGameMessage Original;
	Original.RoutingToken = { 0x01, 0x02, 0x03 };

	FQuilkinHopTimestamp Client;
	Client.IcaoCode          = FName(TEXT("EGLL"));
	Client.StartTimestamp    = 1000;
	Client.TransmitTimestamp = 1000;

	FQuilkinHopTimestamp ProxyIngress;
	ProxyIngress.IcaoCode          = FName(TEXT("EGLL"));
	ProxyIngress.StartTimestamp    = 1100;
	ProxyIngress.TransmitTimestamp = 1120;

	FQuilkinHopTimestamp GameServer;
	GameServer.IcaoCode          = FName(TEXT("EGLL"));
	GameServer.StartTimestamp    = 1200;
	GameServer.TransmitTimestamp = 1250;

	FQuilkinHopTimestamp ProxyEgress;
	ProxyEgress.IcaoCode          = FName(TEXT("EHAM"));
	ProxyEgress.StartTimestamp    = 1350;
	ProxyEgress.TransmitTimestamp = 1360;

	Original.Latency.Add(Client);
	Original.Latency.Add(ProxyIngress);
	Original.Latency.Add(GameServer);
	Original.Latency.Add(ProxyEgress);

	TArray<uint8> Encoded;
	Original.Encode(Encoded);

	const auto DecodeResult = FQuilkinGameMessage::Decode(Encoded);
	TestTrue(TEXT("Decode succeeds"), DecodeResult.HasValue());

	const FQuilkinGameMessage& Decoded = DecodeResult.GetValue();
	TestEqual(TEXT("RoutingToken"), Decoded.RoutingToken, Original.RoutingToken);
	TestEqual(TEXT("Hop count"), Decoded.Latency.Num(), 4);

	TestEqual(TEXT("Hop[0] IcaoCode"),         Decoded.Latency[0].IcaoCode,          Client.IcaoCode);
	TestEqual(TEXT("Hop[0] StartTimestamp"),    Decoded.Latency[0].StartTimestamp,    Client.StartTimestamp);
	TestEqual(TEXT("Hop[0] TransmitTimestamp"), Decoded.Latency[0].TransmitTimestamp, Client.TransmitTimestamp);

	TestEqual(TEXT("Hop[1] IcaoCode"),         Decoded.Latency[1].IcaoCode,          ProxyIngress.IcaoCode);
	TestEqual(TEXT("Hop[1] StartTimestamp"),    Decoded.Latency[1].StartTimestamp,    ProxyIngress.StartTimestamp);
	TestEqual(TEXT("Hop[1] TransmitTimestamp"), Decoded.Latency[1].TransmitTimestamp, ProxyIngress.TransmitTimestamp);

	TestEqual(TEXT("Hop[2] IcaoCode"),         Decoded.Latency[2].IcaoCode,          GameServer.IcaoCode);
	TestEqual(TEXT("Hop[2] StartTimestamp"),    Decoded.Latency[2].StartTimestamp,    GameServer.StartTimestamp);
	TestEqual(TEXT("Hop[2] TransmitTimestamp"), Decoded.Latency[2].TransmitTimestamp, GameServer.TransmitTimestamp);

	TestEqual(TEXT("Hop[3] IcaoCode"),         Decoded.Latency[3].IcaoCode,          ProxyEgress.IcaoCode);
	TestEqual(TEXT("Hop[3] StartTimestamp"),    Decoded.Latency[3].StartTimestamp,    ProxyEgress.StartTimestamp);
	TestEqual(TEXT("Hop[3] TransmitTimestamp"), Decoded.Latency[3].TransmitTimestamp, ProxyEgress.TransmitTimestamp);

	const TOptional<int64> HopLatency1 = Decoded.GetHopLatency(1);
	TestTrue(TEXT("GetHopLatency(1) has value"), HopLatency1.IsSet());
	TestEqual(TEXT("GetHopLatency(1): client → proxy ingress"), HopLatency1.GetValue(), static_cast<int64>(100));

	const TOptional<int64> HopLatency2 = Decoded.GetHopLatency(2);
	TestTrue(TEXT("GetHopLatency(2) has value"), HopLatency2.IsSet());
	TestEqual(TEXT("GetHopLatency(2): proxy ingress → gameserver"), HopLatency2.GetValue(), static_cast<int64>(80));

	const TOptional<int64> HopLatency3 = Decoded.GetHopLatency(3);
	TestTrue(TEXT("GetHopLatency(3) has value"), HopLatency3.IsSet());
	TestEqual(TEXT("GetHopLatency(3): gameserver → proxy egress"), HopLatency3.GetValue(), static_cast<int64>(100));

	const TOptional<int64> TotalRTT = Decoded.GetTotalRoundTripTime(1500);
	TestTrue(TEXT("GetTotalRoundTripTime has value"), TotalRTT.IsSet());
	TestEqual(TEXT("GetTotalRoundTripTime"), TotalRTT.GetValue(), static_cast<int64>(420));

	return true;
}

// A buffer shorter than the 7-byte trailer must produce FBufferTooShort.
IMPLEMENT_SIMPLE_AUTOMATION_TEST(FQuilkinGameMessageDecoderTooShortTest,
	"Quilkin.GameMessageProtocol.Decoder.RejectTooShort",
	EAutomationTestFlags::ApplicationContextMask | EAutomationTestFlags::SmokeFilter)

bool FQuilkinGameMessageDecoderTooShortTest::RunTest(const FString& Parameters)
{
	const TArray<uint8> TooShort = { 0x01, 0x02, 0x03 };
	const auto Result = FQuilkinGameMessage::Decode(TooShort);

	TestTrue(TEXT("Too short: returns error"), Result.HasError());
	TestTrue(TEXT("Too short: correct error type"),
		Result.GetError().Variant.IsType<FQuilkinDecodeError::FBufferTooShort>());
	TestEqual(TEXT("Too short: ActualBytes"),
		Result.GetError().Variant.Get<FQuilkinDecodeError::FBufferTooShort>().ActualBytes, 3);

	return true;
}

// A packet with the wrong version byte (last byte) must produce FUnknownVersion.
IMPLEMENT_SIMPLE_AUTOMATION_TEST(FQuilkinGameMessageDecoderUnknownVersionTest,
	"Quilkin.GameMessageProtocol.Decoder.RejectUnknownVersion",
	EAutomationTestFlags::ApplicationContextMask | EAutomationTestFlags::SmokeFilter)

bool FQuilkinGameMessageDecoderUnknownVersionTest::RunTest(const FString& Parameters)
{
	const TArray<uint8> BadVersion = {
		0xA0,                // CBOR: empty map (1 byte)
		0x00, 0x01,          // CBOR length = 1
		'Q', 'L', 'K', 'N',
		0xFF,                // wrong version
	};

	const auto Result = FQuilkinGameMessage::Decode(BadVersion);

	TestTrue(TEXT("Wrong version: returns error"), Result.HasError());
	TestTrue(TEXT("Wrong version: correct error type"),
		Result.GetError().Variant.IsType<FQuilkinDecodeError::FUnknownVersion>());
	TestEqual(TEXT("Wrong version: FoundVersion"),
		Result.GetError().Variant.Get<FQuilkinDecodeError::FUnknownVersion>().FoundVersion, static_cast<uint8>(0xFF));

	return true;
}

// A packet with the wrong magic tag must produce FInvalidTag.
IMPLEMENT_SIMPLE_AUTOMATION_TEST(FQuilkinGameMessageDecoderBadTagTest,
	"Quilkin.GameMessageProtocol.Decoder.RejectBadTag",
	EAutomationTestFlags::ApplicationContextMask | EAutomationTestFlags::SmokeFilter)

bool FQuilkinGameMessageDecoderBadTagTest::RunTest(const FString& Parameters)
{
	const TArray<uint8> BadTag = {
		0xA0,                // CBOR: empty map (1 byte)
		0x00, 0x01,          // CBOR length = 1
		'X', 'X', 'X', 'X', // wrong tag
		0x01,
	};

	const auto Result = FQuilkinGameMessage::Decode(BadTag);

	TestTrue(TEXT("Bad tag: returns error"), Result.HasError());
	TestTrue(TEXT("Bad tag: correct error type"),
		Result.GetError().Variant.IsType<FQuilkinDecodeError::FInvalidTag>());

	return true;
}

// A packet where the declared CBOR length doesn't match the actual body size must produce FLengthMismatch.
IMPLEMENT_SIMPLE_AUTOMATION_TEST(FQuilkinGameMessageDecoderLengthMismatchTest,
	"Quilkin.GameMessageProtocol.Decoder.RejectLengthMismatch",
	EAutomationTestFlags::ApplicationContextMask | EAutomationTestFlags::SmokeFilter)

bool FQuilkinGameMessageDecoderLengthMismatchTest::RunTest(const FString& Parameters)
{
	const TArray<uint8> LengthMismatch = {
		0xA1,                // 1 byte of CBOR
		0x00, 0xFF,          // declares CborLen = 255 (body is only 1 byte)
		'Q', 'L', 'K', 'N',
		0x01,
	};

	const auto Result = FQuilkinGameMessage::Decode(LengthMismatch);

	TestTrue(TEXT("Length mismatch: returns error"), Result.HasError());
	TestTrue(TEXT("Length mismatch: correct error type"),
		Result.GetError().Variant.IsType<FQuilkinDecodeError::FLengthMismatch>());

	const auto& Err = Result.GetError().Variant.Get<FQuilkinDecodeError::FLengthMismatch>();
	TestEqual(TEXT("Length mismatch: Declared"), Err.Declared, 255);
	TestEqual(TEXT("Length mismatch: Expected"), Err.Expected, 1);

	return true;
}

#endif // WITH_DEV_AUTOMATION_TESTS
