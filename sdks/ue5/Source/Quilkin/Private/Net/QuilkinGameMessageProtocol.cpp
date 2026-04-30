#include "QuilkinGameMessageProtocol.h"

TValueOrError<FQuilkinPacketParseResult, FQuilkinDecodeError>
	QuilkinDecodePacket(const uint8* Data, int32 Size)
{
	if (Size < QuilkinCbor::TrailerSize)
	{
		return MakeError(FQuilkinDecodeError::MakeBufferTooShort(Size, QuilkinCbor::TrailerSize));
	}

	const uint8 PacketVersion = Data[Size - 1];
	if (PacketVersion != QuilkinCbor::Version)
	{
		return MakeError(FQuilkinDecodeError::MakeUnknownVersion(PacketVersion));
	}

	const int32 TagOffset = Size - 1 - QuilkinCbor::TagLen;
	for (int32 i = 0; i < QuilkinCbor::TagLen; ++i)
	{
		if (Data[TagOffset + i] != QuilkinCbor::Tag[i])
		{
			return MakeError(FQuilkinDecodeError::MakeInvalidTag());
		}
	}

	const int32 LenOffset = Size - 1 - QuilkinCbor::TagLen - 2;
	const uint16 CborLen  = (static_cast<uint16>(Data[LenOffset]) << 8) | Data[LenOffset + 1];
	const int32 CborStart = Size - QuilkinCbor::TrailerSize - static_cast<int32>(CborLen);
	if (CborStart < 0)
	{
		return MakeError(FQuilkinDecodeError::MakeLengthMismatch(static_cast<int32>(CborLen), Size - QuilkinCbor::TrailerSize));
	}

	// Copy only the CBOR body bytes to avoid reading beyond the game data region.
	TArray<uint8> CborBytes(Data + CborStart, static_cast<int32>(CborLen));
	FMemoryReader MemReader(CborBytes);
	FCborReader CborReader(&MemReader, ECborEndianness::StandardCompliant);

	FCborContext MapContext;
	if (!CborReader.ReadNext(MapContext) || MapContext.MajorType() != ECborCode::Map)
	{
		return MakeError(FQuilkinDecodeError::MakeCborMapError());
	}

	const int64 NumPairs = static_cast<int64>(MapContext.AsLength()) / 2;
	FQuilkinGameMessage Message;

	const TValueOrError<void, FQuilkinDecodeError> PairsResult =
		FQuilkinGameMessage::DecodePairs(CborReader, NumPairs, Message);
	if (PairsResult.HasError())
	{
		return MakeError(PairsResult.GetError());
	}

	return MakeValue(FQuilkinPacketParseResult{ CborStart, MoveTemp(Message) });
}
