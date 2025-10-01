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

#include <chrono>
#include <type_traits>

#include "Containers/Union.h"
#include "Containers/UnrealString.h"
#include "CoreMinimal.h"
#include "Math/UnrealMathUtility.h"
#include "Misc/DateTime.h"
#include "Serialization/BufferArchive.h"
#include "Serialization/MemoryReader.h"

#include "QuilkinResult.h"

static inline int64 GetUnixTimestampInNanos()
{
    auto now = std::chrono::system_clock::now();
    auto now_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(now.time_since_epoch());
    return now_ns.count();
}

static constexpr uint32 ConvertMagicHeaderToUInt32(const uint8 Header[4]) {
    return (static_cast<uint32>(Header[0]) << 24) |
        (static_cast<uint32>(Header[1]) << 16) |
        (static_cast<uint32>(Header[2]) << 8) |
        (static_cast<uint32>(Header[3]));
}

class FProtocolVariant
{
public:
    virtual ~FProtocolVariant() = default;

    template <typename Variant>
    TOptional<Variant*> AsVariant();

    FBufferArchive Encode() const;
    static TResult<TSharedPtr<FProtocolVariant>, FString> Decode(const TArray<uint8>& Buffer);
    virtual FBufferArchive EncodeVariant() const = 0;
    virtual uint16 PayloadLength() const = 0;
    virtual uint8 GetCode() const = 0;
    virtual uint8 GetNonce() const = 0;

private:
    static constexpr uint8 MagicHeaderBytes[] = { 'Q' , 'L' , 'K' , 'N' };
    static constexpr uint32 MagicHeader = ConvertMagicHeaderToUInt32(MagicHeaderBytes);
    static constexpr uint8 ProtocolVersion = 0;
};

struct FPing : public FProtocolVariant
{
    uint8 Nonce;
    int64 ClientTimestamp;

public:
    explicit FPing()
    {
        Nonce = FMath::RandHelper(256);
        ClientTimestamp = GetUnixTimestampInNanos();
    }

    explicit FPing(uint8 InNonce, int64 InClientTimestamp)
        : Nonce(InNonce), ClientTimestamp(InClientTimestamp)
    {
    }

    static TResult<FPing, FString> DecodeVariant(FMemoryReader& Ar);
    static uint8 Discriminant() { return 0; }
    static uint8 StaticPayloadLength() { return sizeof(Nonce) + sizeof(ClientTimestamp); }
    virtual FBufferArchive EncodeVariant() const override;
    virtual uint16 PayloadLength() const override { return StaticPayloadLength(); }
    virtual uint8 GetCode() const override { return Discriminant(); }
    virtual uint8 GetNonce() const override { return Nonce; }
    uint8 GetTimestamp() const { return ClientTimestamp; }
};

struct FPingReply : public FProtocolVariant
{
    uint8 Nonce;
    int64 ClientTimestamp;
    int64 ServerStartTimestamp;
    int64 ServerTransmitTimestamp;

public:
    explicit FPingReply(uint8 InNonce, int64 InClientTimestamp, int64 InServerStartTimestamp, int64 InServerTransmitTimestamp)
        : Nonce(InNonce), ClientTimestamp(InClientTimestamp), ServerStartTimestamp(InServerStartTimestamp), ServerTransmitTimestamp(InServerTransmitTimestamp)
    {
    }

    int64 RoundTimeDelay(int64 ClientReceiveTimestamp) {
        return (ClientReceiveTimestamp - ClientTimestamp)
             - (ServerTransmitTimestamp - ServerStartTimestamp);
    }

    int64 RoundTimeDelay() {
        return RoundTimeDelay(GetUnixTimestampInNanos());
    }

    static TResult<FPingReply, FString> DecodeVariant(FMemoryReader& Ar);
    static uint8 Discriminant() { return 1; }
    static uint8 StaticPayloadLength() { return sizeof(Nonce) + sizeof(ClientTimestamp) + sizeof(ServerStartTimestamp) + sizeof(ServerTransmitTimestamp); }
    virtual FBufferArchive EncodeVariant() const override;
    virtual uint16 PayloadLength() const override { return StaticPayloadLength(); }
    virtual uint8 GetCode() const override { return Discriminant(); }
    virtual uint8 GetNonce() const override { return Nonce; }
};

class ArchiveExtensions
{
public:
    /* Encodes an integer format in big endian encoding, this should
       be handled by `FBufferArchive.ArForceByteSwapping` for us, but
       that doesn't seem to actually work as expected.
    */
    template <typename Int>
    static void EncodeBe(FBufferArchive& Ar, Int Value) {
        for (int i = sizeof(Int) - 1; i >= 0; --i) {
            uint8 Byte = (Value >> (8 * i)) & 0xFF;
            Ar.Serialize(&Byte, sizeof(Byte));
        }
    }

    /* Encodes a buffer archive into another in big endian format. */
    static void EncodeArchiveBe(FBufferArchive& Ar, FBufferArchive& Input) {
        for (int32 i = 0; i < Input.Num(); ++i)
        {
            uint8 Byte = Input[i];
            ArchiveExtensions::EncodeBe(Ar, Byte);
        }
    }
};
