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

#include "CoreMinimal.h"
#include "Algo/Accumulate.h"
#include "CborReader.h"
#include "CborWriter.h"
#include "Misc/TVariant.h"
#include "Serialization/MemoryReader.h"
#include "Serialization/MemoryWriter.h"
#include "Templates/Tuple.h"
#include "Templates/ValueOrError.h"

#include <type_traits>

#include "QuilkinGameMessageProtocol.generated.h"

// All reasons a decode can fail. Each variant carries only the data relevant to that failure.
struct FQuilkinDecodeError
{
	struct FBufferTooShort  { int32 ActualBytes; int32 MinBytes; };
	struct FUnknownVersion  { uint8 FoundVersion; };
	struct FInvalidTag      {};
	struct FLengthMismatch  { int32 Declared; int32 Expected; };
	struct FCborMapError    {};
	struct FCborKeyError    { int64 EntryIndex; };
	struct FCborValueError  { int64 EntryIndex; };
	struct FTypeMismatch    { uint64 Key; };
	struct FUnknownKey      { uint64 Key; };

	using FVariant = TVariant<
		FBufferTooShort,
		FUnknownVersion,
		FInvalidTag,
		FLengthMismatch,
		FCborMapError,
		FCborKeyError,
		FCborValueError,
		FTypeMismatch,
		FUnknownKey
	>;

	FVariant Variant;

	template<typename T>
	explicit FQuilkinDecodeError(T InVariant)
		: Variant(TInPlaceType<T>(), MoveTemp(InVariant))
	{}

	static FQuilkinDecodeError MakeBufferTooShort(int32 ActualBytes, int32 MinBytes)
	{
		return FQuilkinDecodeError(FBufferTooShort{ ActualBytes, MinBytes });
	}

	static FQuilkinDecodeError MakeUnknownVersion(uint8 FoundVersion)
	{
		return FQuilkinDecodeError(FUnknownVersion{ FoundVersion });
	}

	static FQuilkinDecodeError MakeInvalidTag()
	{
		return FQuilkinDecodeError(FInvalidTag{});
	}

	static FQuilkinDecodeError MakeLengthMismatch(int32 Declared, int32 Expected)
	{
		return FQuilkinDecodeError(FLengthMismatch{ Declared, Expected });
	}

	static FQuilkinDecodeError MakeCborMapError()
	{
		return FQuilkinDecodeError(FCborMapError{});
	}

	static FQuilkinDecodeError MakeCborKeyError(int64 EntryIndex)
	{
		return FQuilkinDecodeError(FCborKeyError{ EntryIndex });
	}

	static FQuilkinDecodeError MakeCborValueError(int64 EntryIndex)
	{
		return FQuilkinDecodeError(FCborValueError{ EntryIndex });
	}

	static FQuilkinDecodeError MakeTypeMismatch(uint64 Key)
	{
		return FQuilkinDecodeError(FTypeMismatch{ Key });
	}

	static FQuilkinDecodeError MakeUnknownKey(uint64 Key)
	{
		return FQuilkinDecodeError(FUnknownKey{ Key });
	}
};

// Associates a typed CBOR key enum with a member pointer.
// Each struct uses its own TKey enum, keeping the key type scoped to that struct.
template<typename TMsg, typename TKey, typename TField>
struct TQuilkinField
{
	TKey Key;
	TField TMsg::* MemberPtr;
};

template<typename TMsg, typename TKey, typename TField>
TQuilkinField<TMsg, TKey, TField> MakeQuilkinField(TKey Key, TField TMsg::* MemberPtr)
{
	return { Key, MemberPtr };
}

namespace QuilkinCbor
{
	// Detects any TArray<E, A> — routes to major type 4 (array).
	// TArray<uint8> is matched by a prior is_same_v check (byte string) and never reaches here.
	template<typename T>
	constexpr bool TIsArray = false;

	template<typename E, typename A>
	constexpr bool TIsArray<TArray<E, A>> = true;

	// Detects any struct that exposes a static Fields() — routes to nested map serialization.
	template<typename T, typename = void>
	constexpr bool THasFields = false;

	template<typename T>
	constexpr bool THasFields<T, std::void_t<decltype(T::Fields())>> = true;

	// Type-dependent false so static_assert in an else branch only fires when instantiated.
	template<typename T>
	constexpr bool TAlwaysFalse = false;

	static constexpr uint8  Version     = 1;
	static constexpr uint8  Tag[4]      = { 'Q', 'L', 'K', 'N' };
	static constexpr int32  TagLen      = 4;
	// version(1) + tag(4) + cbor_length(2)
	static constexpr int32  TrailerSize = 7;
}

// ---------------------------------------------------------------------------
// WriteCborField<T> — single template, if constexpr dispatch per CBOR major type.
//
// Supported types:
//   uint64               → major type 0 (unsigned integer)
//   int64                → major type 1 (negative) / 0 (non-negative)
//   TArray<uint8>        → major type 2 (byte string)
//   FString              → major type 3 (text string)
//   FName                → major type 3 (text string, serialized via ToString())
//   TArray<T>            → major type 4 (array, recursive per element)
//   struct with Fields() → major type 5 (map, keys cast to uint64)
//   bool                 → major type 7 / prim
//   float                → major type 7 / prim (4-byte IEEE 754)
//   double               → major type 7 / prim (8-byte IEEE 754)
// ---------------------------------------------------------------------------
template<typename T>
void WriteCborField(FCborWriter& Writer, const T& Value)
{
	if constexpr (std::is_same_v<T, uint64>)
	{
		Writer.WriteValue(Value);
	}
	else if constexpr (std::is_same_v<T, int64>)
	{
		Writer.WriteValue(Value);
	}
	else if constexpr (std::is_same_v<T, TArray<uint8>>)
	{
		Writer.WriteValue(Value.GetData(), static_cast<uint64>(Value.Num()));
	}
	else if constexpr (std::is_same_v<T, FString>)
	{
		Writer.WriteValue(Value);
	}
	else if constexpr (std::is_same_v<T, FName>)
	{
		Writer.WriteValue(Value.ToString());
	}
	else if constexpr (QuilkinCbor::TIsArray<T>)
	{
		Writer.WriteContainerStart(ECborCode::Array, -1);
		for (const typename T::ElementType& Element : Value)
		{
			WriteCborField(Writer, Element);
		}
		Writer.WriteContainerEnd();
	}
	else if constexpr (QuilkinCbor::THasFields<T>)
	{
		auto FieldDescriptors = T::Fields();
		const int32 NumFields = TTupleArity<decltype(FieldDescriptors)>::Value;
		Writer.WriteContainerStart(ECborCode::Map, NumFields);
		VisitTupleElements([&](const auto& Field)
		{
			Writer.WriteValue(static_cast<uint64>(Field.Key));
			WriteCborField(Writer, Value.*Field.MemberPtr);
		}, FieldDescriptors);
	}
	else if constexpr (std::is_same_v<T, bool>)
	{
		Writer.WriteValue(Value);
	}
	else if constexpr (std::is_same_v<T, float>)
	{
		Writer.WriteValue(Value);
	}
	else if constexpr (std::is_same_v<T, double>)
	{
		Writer.WriteValue(Value);
	}
	else
	{
		static_assert(QuilkinCbor::TAlwaysFalse<T>, "WriteCborField: no CBOR encoding for this type");
	}
}

// ---------------------------------------------------------------------------
// ReadCborField<T> — single template, if constexpr dispatch per CBOR major type.
// FCborReader& is threaded through so array and struct branches can call ReadNext.
// ---------------------------------------------------------------------------
template<typename T>
bool ReadCborField(FCborReader& Reader, const FCborContext& Context, T& OutValue)
{
	if constexpr (std::is_same_v<T, uint64>)
	{
		if (Context.MajorType() != ECborCode::Uint)
		{
			return false;
		}
		OutValue = Context.AsUInt();
	}
	else if constexpr (std::is_same_v<T, int64>)
	{
		if (Context.MajorType() != ECborCode::Int && Context.MajorType() != ECborCode::Uint)
		{
			return false;
		}
		OutValue = Context.AsInt();
	}
	else if constexpr (std::is_same_v<T, TArray<uint8>>)
	{
		if (Context.MajorType() != ECborCode::ByteString)
		{
			return false;
		}
		const TArrayView<const uint8> View = Context.AsByteArray();
		OutValue = TArray<uint8>(View.GetData(), View.Num());
	}
	else if constexpr (std::is_same_v<T, FString>)
	{
		if (Context.MajorType() != ECborCode::TextString)
		{
			return false;
		}
		OutValue = Context.AsString();
	}
	else if constexpr (std::is_same_v<T, FName>)
	{
		if (Context.MajorType() != ECborCode::TextString)
		{
			return false;
		}
		OutValue = FName(*Context.AsString());
	}
	else if constexpr (QuilkinCbor::TIsArray<T>)
	{
		if (Context.MajorType() != ECborCode::Array)
		{
			return false;
		}
		if (Context.IsIndefiniteContainer())
		{
			// Read elements until the break code that closes the indefinite array.
			while (true)
			{
				FCborContext ElemContext;
				if (!Reader.ReadNext(ElemContext))
				{
					return false;
				}
				if (ElemContext.IsBreak())
				{
					break;
				}
				typename T::ElementType Element{};
				if (!ReadCborField(Reader, ElemContext, Element))
				{
					return false;
				}
				OutValue.Add(MoveTemp(Element));
			}
		}
		else
		{
			const int64 Num = static_cast<int64>(Context.AsLength());
			OutValue.Reserve(static_cast<int32>(Num));
			for (int64 i = 0; i < Num; ++i)
			{
				FCborContext ElemContext;
				if (!Reader.ReadNext(ElemContext))
				{
					return false;
				}
				typename T::ElementType Element{};
				if (!ReadCborField(Reader, ElemContext, Element))
				{
					return false;
				}
				OutValue.Add(MoveTemp(Element));
			}
		}
	}
	else if constexpr (QuilkinCbor::THasFields<T>)
	{
		if (Context.MajorType() != ECborCode::Map)
		{
			return false;
		}
		const int64 NumPairs = static_cast<int64>(Context.AsLength()) / 2;
		auto FieldDescriptors = T::Fields();
		for (int64 i = 0; i < NumPairs; ++i)
		{
			FCborContext KeyContext;
			if (!Reader.ReadNext(KeyContext) || KeyContext.MajorType() != ECborCode::Uint)
			{
				return false;
			}
			const uint64 Key = KeyContext.AsUInt();

			FCborContext ValueContext;
			if (!Reader.ReadNext(ValueContext))
			{
				return false;
			}

			bool bFound = false;
			bool bError = false;
			VisitTupleElements([&](auto& Field)
			{
				if (!bFound && static_cast<uint64>(Field.Key) == Key)
				{
					bFound = true;
					if (!ReadCborField(Reader, ValueContext, OutValue.*Field.MemberPtr))
					{
						bError = true;
					}
				}
			}, FieldDescriptors);

			if (bError || !bFound)
			{
				return false;
			}
		}

		// For a finite map, the reader leaves the exhausted context on the stack until the next
		// ReadNext call. Consuming it here ensures the parent context (e.g. an indefinite array)
		// is back on top before the next element is read.
		if (!Context.IsIndefiniteContainer())
		{
			FCborContext BreakContext;
			Reader.ReadNext(BreakContext);
		}
	}
	else if constexpr (std::is_same_v<T, bool>)
	{
		if (Context.MajorType() != ECborCode::Prim
			|| (Context.AdditionalValue() != ECborCode::False && Context.AdditionalValue() != ECborCode::True))
		{
			return false;
		}
		OutValue = Context.AsBool();
	}
	else if constexpr (std::is_same_v<T, float>)
	{
		if (Context.RawCode() != (ECborCode::Prim | ECborCode::Value_4Bytes))
		{
			return false;
		}
		OutValue = Context.AsFloat();
	}
	else if constexpr (std::is_same_v<T, double>)
	{
		if (Context.RawCode() != (ECborCode::Prim | ECborCode::Value_8Bytes))
		{
			return false;
		}
		OutValue = Context.AsDouble();
	}
	else
	{
		static_assert(QuilkinCbor::TAlwaysFalse<T>, "ReadCborField: no CBOR decoding for this type");
	}
	return true;
}

// ---------------------------------------------------------------------------
// Latency types
// ---------------------------------------------------------------------------

// CBOR integer keys for FQuilkinHopTimestamp fields.
UENUM()
enum class EQuilkinHopKey : uint8
{
	IcaoCode          = 0,
	StartTimestamp    = 1,
	TransmitTimestamp = 2,
};

// Timestamps recorded at a single hop (proxy ingress, gameserver, proxy egress, etc.).
//
// IcaoCode identifies the datacenter location (e.g. "EGLL", "EHAM").
// StartTimestamp    — nanoseconds since epoch when this hop received the packet.
// TransmitTimestamp — nanoseconds since epoch when this hop forwarded the packet onward.
//
// When the client creates the initial entry it sets StartTimestamp = TransmitTimestamp = send time,
// since it is both the originator and the first transmitter.
USTRUCT()
struct FQuilkinHopTimestamp
{
	GENERATED_BODY()

	UPROPERTY()
	FName IcaoCode;

	UPROPERTY()
	int64 StartTimestamp = 0;

	UPROPERTY()
	int64 TransmitTimestamp = 0;

	static auto Fields()
	{
		return MakeTuple(
			MakeQuilkinField(EQuilkinHopKey::IcaoCode,          &FQuilkinHopTimestamp::IcaoCode),
			MakeQuilkinField(EQuilkinHopKey::StartTimestamp,     &FQuilkinHopTimestamp::StartTimestamp),
			MakeQuilkinField(EQuilkinHopKey::TransmitTimestamp,  &FQuilkinHopTimestamp::TransmitTimestamp)
		);
	}
};

// ---------------------------------------------------------------------------
// Game message
// ---------------------------------------------------------------------------

// CBOR integer keys for FQuilkinGameMessage fields.
UENUM()
enum class EQuilkinGameMessageKey : uint8
{
	RoutingToken = 0,
	Latency      = 1,
};

// Wire format (body first, trailer after):
//   [CBOR map][u16 CBOR length, big-endian]["QLKN"][version byte = 1]
//
// To add a new field:
//   1. Add an EQuilkinGameMessageKey entry.
//   2. Add a member below (any CBOR-supported type, or a struct with Fields()).
//   3. Add a MakeQuilkinField entry to Fields().
//
// Latency layout:
//   Client send:   Latency contains one entry — the client hop, StartTimestamp = TransmitTimestamp = send time.
//   Return path:   Latency contains one entry per hop in traversal order:
//                    [0] client, [1] proxy ingress, [2] gameserver, [3] proxy egress, ...
USTRUCT()
struct FQuilkinGameMessage
{
	GENERATED_BODY()

	UPROPERTY()
	TArray<uint8> RoutingToken;

	UPROPERTY()
	TArray<FQuilkinHopTimestamp> Latency;

	static auto Fields()
	{
		return MakeTuple(
			MakeQuilkinField(EQuilkinGameMessageKey::RoutingToken, &FQuilkinGameMessage::RoutingToken),
			MakeQuilkinField(EQuilkinGameMessageKey::Latency,      &FQuilkinGameMessage::Latency)
		);
	}

	// Returns the one-way transit time in nanoseconds for the packet to travel from the
	// previous hop to HopIndex, or an unset optional if HopIndex is out of range.
	TOptional<int64> GetHopLatency(int32 HopIndex) const
	{
		if (HopIndex < 1 || HopIndex >= Latency.Num())
		{
			return TOptional<int64>();
		}
		return Latency[HopIndex].StartTimestamp - Latency[HopIndex - 1].TransmitTimestamp;
	}

	// Returns the total round-trip time in nanoseconds, excluding processing time at each hop,
	// giving the pure network transit time. Returns an unset optional if Latency is empty.
	TOptional<int64> GetTotalRoundTripTime(int64 ClientReceiveTimestamp) const
	{
		if (Latency.IsEmpty())
		{
			return TOptional<int64>();
		}
		const int64 TotalProcessingTime = Algo::Accumulate(Latency, int64(0),
			[](const int64 Acc, const FQuilkinHopTimestamp& Hop)
			{
				return Acc + (Hop.TransmitTimestamp - Hop.StartTimestamp);
			});
		return (ClientReceiveTimestamp - Latency[0].StartTimestamp) - TotalProcessingTime;
	}

	// Appends the encoded message (CBOR body + trailer) directly to OutBuffer.
	// OutBuffer may already contain data; the message is appended at the end.
	void Encode(TArray<uint8>& OutBuffer) const
	{
		auto FieldDescriptors = FQuilkinGameMessage::Fields();
		const int32 NumFields = TTupleArity<decltype(FieldDescriptors)>::Value;

		const int32 CborStart = OutBuffer.Num();
		{
			FMemoryWriter MemWriter(OutBuffer, /*bIsPersistent=*/false, /*bSetOffset=*/true);
			FCborWriter CborWriter(&MemWriter, ECborEndianness::StandardCompliant);
			CborWriter.WriteContainerStart(ECborCode::Map, NumFields);
			VisitTupleElements([&](const auto& Field)
			{
				CborWriter.WriteValue(static_cast<uint64>(Field.Key));
				WriteCborField(CborWriter, this->*Field.MemberPtr);
			}, FieldDescriptors);
		}

		const uint16 CborLen = static_cast<uint16>(OutBuffer.Num() - CborStart);
		OutBuffer.Add(static_cast<uint8>((CborLen >> 8) & 0xFF));
		OutBuffer.Add(static_cast<uint8>(CborLen & 0xFF));
		OutBuffer.Append(QuilkinCbor::Tag, QuilkinCbor::TagLen);
		OutBuffer.Add(QuilkinCbor::Version);
	}

	UE_NODISCARD static TValueOrError<FQuilkinGameMessage, FQuilkinDecodeError> Decode(const TArray<uint8>& Buffer)
	{
		const TValueOrError<void, FQuilkinDecodeError> TrailerResult = ValidateTrailer(Buffer);
		if (TrailerResult.HasError())
		{
			return MakeError(TrailerResult.GetError());
		}

		FMemoryReader MemReader(Buffer);
		FCborReader CborReader(&MemReader, ECborEndianness::StandardCompliant);

		FCborContext MapContext;
		if (!CborReader.ReadNext(MapContext) || MapContext.MajorType() != ECborCode::Map)
		{
			return MakeError(FQuilkinDecodeError::MakeCborMapError());
		}

		const int64 NumPairs = static_cast<int64>(MapContext.AsLength()) / 2;
		FQuilkinGameMessage Message;

		const TValueOrError<void, FQuilkinDecodeError> PairsResult = DecodePairs(CborReader, NumPairs, Message);
		if (PairsResult.HasError())
		{
			return MakeError(PairsResult.GetError());
		}

		return MakeValue(MoveTemp(Message));
	}

private:
	static TValueOrError<void, FQuilkinDecodeError> ValidateTrailer(const TArray<uint8>& Buffer)
	{
		if (Buffer.Num() < QuilkinCbor::TrailerSize)
		{
			return MakeError(FQuilkinDecodeError::MakeBufferTooShort(Buffer.Num(), QuilkinCbor::TrailerSize));
		}

		const uint8 PacketVersion = Buffer.Last();
		if (PacketVersion != QuilkinCbor::Version)
		{
			return MakeError(FQuilkinDecodeError::MakeUnknownVersion(PacketVersion));
		}

		const int32 TagOffset = Buffer.Num() - 1 - QuilkinCbor::TagLen;
		for (int32 i = 0; i < QuilkinCbor::TagLen; ++i)
		{
			if (Buffer[TagOffset + i] != QuilkinCbor::Tag[i])
			{
				return MakeError(FQuilkinDecodeError::MakeInvalidTag());
			}
		}

		const int32 LenOffset = Buffer.Num() - 1 - QuilkinCbor::TagLen - 2;
		const uint16 CborLen  = (static_cast<uint16>(Buffer[LenOffset]) << 8) | Buffer[LenOffset + 1];

		const int32 ExpectedCborSize = Buffer.Num() - QuilkinCbor::TrailerSize;
		if (static_cast<int32>(CborLen) != ExpectedCborSize)
		{
			return MakeError(FQuilkinDecodeError::MakeLengthMismatch(static_cast<int32>(CborLen), ExpectedCborSize));
		}

		return MakeValue();
	}

public:
	static TValueOrError<void, FQuilkinDecodeError> DecodePairs(FCborReader& CborReader, int64 NumPairs, FQuilkinGameMessage& Message)
	{
		for (int64 i = 0; i < NumPairs; ++i)
		{
			const TValueOrError<void, FQuilkinDecodeError> EntryResult = DecodeEntry(CborReader, i, Message);
			if (EntryResult.HasError())
			{
				return MakeError(EntryResult.GetError());
			}
		}
		return MakeValue();
	}

private:
	static TValueOrError<void, FQuilkinDecodeError> DecodeEntry(FCborReader& CborReader, int64 EntryIndex, FQuilkinGameMessage& Message)
	{
		FCborContext KeyContext;
		if (!CborReader.ReadNext(KeyContext) || KeyContext.MajorType() != ECborCode::Uint)
		{
			return MakeError(FQuilkinDecodeError::MakeCborKeyError(EntryIndex));
		}
		const uint64 Key = KeyContext.AsUInt();

		FCborContext ValueContext;
		if (!CborReader.ReadNext(ValueContext))
		{
			return MakeError(FQuilkinDecodeError::MakeCborValueError(EntryIndex));
		}

		bool bFound = false;
		bool bTypeMismatch = false;
		auto FieldDescriptors = FQuilkinGameMessage::Fields();
		VisitTupleElements([&](auto& Field)
		{
			if (!bFound && static_cast<uint64>(Field.Key) == Key)
			{
				bFound = true;
				if (!ReadCborField(CborReader, ValueContext, Message.*Field.MemberPtr))
				{
					bTypeMismatch = true;
				}
			}
		}, FieldDescriptors);

		if (bTypeMismatch)
		{
			return MakeError(FQuilkinDecodeError::MakeTypeMismatch(Key));
		}
		if (!bFound)
		{
			return MakeError(FQuilkinDecodeError::MakeUnknownKey(Key));
		}

		return MakeValue();
	}
};

// Metadata extracted from a received QGMP packet.
// GameDataSize is the byte count of the game payload that precedes the QGMP envelope.
// Message holds the parsed QGMP fields; future fields (latency, etc.) appear here as they are added.
struct FQuilkinPacketParseResult
{
	int32 GameDataSize;
	FQuilkinGameMessage Message;
};

// Parses a QGMP envelope from the tail of a received packet buffer (Data[0..Size)).
// On success: GameDataSize is the number of game data bytes at the front of the buffer.
// On failure: the buffer does not end with a valid QGMP trailer.
UE_NODISCARD TValueOrError<FQuilkinPacketParseResult, FQuilkinDecodeError>
	QuilkinDecodePacket(const uint8* Data, int32 Size);
