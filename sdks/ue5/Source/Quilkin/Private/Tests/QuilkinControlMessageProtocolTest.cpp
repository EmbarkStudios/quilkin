#if WITH_DEV_AUTOMATION_TESTS

#include "../QuilkinControlMessageProtocol.h"
#include "../QuilkinLog.h"

#include "CoreMinimal.h"
#include "Misc/AutomationTest.h"

template <typename T>
static bool ArrayIsEqual(const TArray<T>& Array1, const TArray<T>& Array2)
{
    if (Array1.Num() != Array2.Num())
    {
        return false;
    }

    for (int32 i = 0; i < Array1.Num(); ++i)
    {
        if (!(Array1[i] == Array2[i]))
        {
            return false;
        }
    }

    return true;
}

IMPLEMENT_SIMPLE_AUTOMATION_TEST(FTestPing, "Quilkin.Protocol.Ping", EAutomationTestFlags::EditorContext | EAutomationTestFlags::EngineFilter)

bool FTestPing::RunTest(const FString& Parameters)
{
   const TArray<uint8> Input = {
      // Magic
      'Q', 'L', 'K', 'N',
      // Version
      0,
      // Code
      0,
      // Length
      0, 9,
      // Nonce
      0xBF,
      // Payload
      0, 0, 0, 0, 0x63, 0xb6, 0xe9, 0x57
   };

   auto Result = FProtocolVariant::Decode(Input);
   TestTrue("Decode Ping", Result.IsSuccess());
   TSharedPtr<FProtocolVariant> Variant = Result.GetValue();
   TestTrue("Nonce Matches", Variant->GetNonce() == 0xBF);
   TestTrue("Ping Variant", Variant->GetCode() == 0);
   // FPing* Ping = Cast<FPing>(*Variant.Get());
   // TestTrue("Ping Timestamp", Ping->GetTimestamp() == 0x63B6E957);

   // Encode
   auto Archive = Result.GetValue()->Encode();
   TArray<uint8> Buffer = static_cast<TArray<uint8>&>(Archive);
   TestTrue("Encoded Ping Equals Input", ArrayIsEqual(Buffer, Input));
   return true;
}

IMPLEMENT_SIMPLE_AUTOMATION_TEST(FTestPingReply, "Quilkin.Protocol.PingReply", EAutomationTestFlags::EditorContext | EAutomationTestFlags::EngineFilter)

bool FTestPingReply::RunTest(const FString& Parameters)
{
   const TArray<uint8> Input = {
      // Magic
      'Q', 'L', 'K', 'N',
      // Version
      0,
      // Code
      1,
      // Length
      0, 25,
      // Nonce
      0xBF,
      // Payload
      0, 0, 0, 0, 0x63, 0xb6, 0xe9, 0x57,
      0, 0, 0, 0, 0x63, 0xb6, 0xe9, 0x57,
      0, 0, 0, 0, 0x63, 0xb6, 0xe9, 0x57
   };

   auto Result = FProtocolVariant::Decode(Input);
   TestTrue("Decode PingReply", Result.IsSuccess());
   TSharedPtr<FProtocolVariant> Variant = Result.GetValue();
   TestTrue("Nonce Matches", Variant->GetNonce() == 0xBF);
   TestTrue("PingReply Variant", Variant->GetCode() == 1);
   auto Archive = Variant->Encode();
   TArray<uint8> Buffer = static_cast<TArray<uint8>&>(Archive);
   TestTrue("Encoded PingReply Equals Input", ArrayIsEqual(Buffer, Input));
   return true;
}

IMPLEMENT_SIMPLE_AUTOMATION_TEST(FTestRejectMalformedPacket, "Quilkin.Protocol.RejectMalformedPacket", EAutomationTestFlags::EditorContext | EAutomationTestFlags::EngineFilter)

bool FTestRejectMalformedPacket::RunTest(const FString& Parameters)
{
   const TArray<uint8> Input = {
      // Magic
      'Q', 'L', 'K', 'N',
      // Version
      0,
      // Code (intentionally Ping)
      0,
      // Length
      0, 25,
      // Nonce
      0xBF,
      // Payload
      0, 0, 0, 0, 0x63, 0xb6, 0xe9, 0x57,
      0, 0, 0, 0, 0x63, 0xb6, 0xe9, 0x57,
      0, 0, 0, 0, 0x63, 0xb6, 0xe9, 0x57
   };

   auto Result = FProtocolVariant::Decode(Input);
   TestTrue("Reject Malformed Packet", Result.IsError());
   return true;
}

IMPLEMENT_SIMPLE_AUTOMATION_TEST(FTestRejectUnknownPacket, "Quilkin.Protocol.RejectUnknownPacket", EAutomationTestFlags::EditorContext | EAutomationTestFlags::EngineFilter)

bool FTestRejectUnknownPacket::RunTest(const FString& Parameters)
{
   const TArray<uint8> Input = {
      // Magic
      'Q', 'L', 'K', 'N',
      // Version
      0,
      // Code
      0xff
   };

   auto Result = FProtocolVariant::Decode(Input);
   TestTrue("Reject Unknown Packet", Result.IsError());
   return true;
}

IMPLEMENT_SIMPLE_AUTOMATION_TEST(FTestRejectUnknownVersion, "Quilkin.Protocol.RejectUnknownVersion", EAutomationTestFlags::EditorContext | EAutomationTestFlags::EngineFilter)

bool FTestRejectUnknownVersion::RunTest(const FString& Parameters)
{
   const TArray<uint8> Input = {
      // Magic
      'Q', 'L', 'K', 'N',
      // Version
      0xff
   };

   auto Result = FProtocolVariant::Decode(Input);
   TestTrue("Reject Unknown Version", Result.IsError());
   return true;
}

IMPLEMENT_SIMPLE_AUTOMATION_TEST(FTestRejectNoMagicHeader, "Quilkin.Protocol.RejectNoMagicHeader", EAutomationTestFlags::EditorContext | EAutomationTestFlags::EngineFilter)

bool FTestRejectNoMagicHeader::RunTest(const FString& Parameters)
{
   const TArray<uint8> Input = {
      0xff, 0xff, 0, 0, 0, 0, 0x63, 0xb6, 0xe9, 0x57
   };

   auto Result = FProtocolVariant::Decode(Input);
   TestTrue("Reject No Magic Header", Result.IsError());
   return true;
}

#endif
