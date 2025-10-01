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

#include "QuilkinControlMessageProtocol.h"

#include "Containers/UnrealString.h"
#include "QuilkinLog.h"

template <typename Variant>
TOptional<Variant*> FProtocolVariant::AsVariant() {
    if (std::is_same<Variant, FPing>() && (GetCode() == FPing::Discriminant())) {
        return TOptional(static_cast<Variant*>(this));
    }
    else if (std::is_same<Variant, FPingReply>() && (GetCode() == FPingReply::Discriminant())) {
        return TOptional(static_cast<Variant*>(this));
    }
    else {
        return TOptional<Variant*>();
    }
}

// We get linker errors without this specialisation, I don't know why.
template <>
TOptional<FPingReply*> FProtocolVariant::AsVariant() {
     if (GetCode() == FPingReply::Discriminant()) {
        return TOptional(static_cast<FPingReply*>(this));
    }
    else {
        return TOptional<FPingReply*>();
    }
}

FBufferArchive FProtocolVariant::Encode() const
{
    FBufferArchive VariantAr = EncodeVariant();
    checkSlow(VariantAr.Num() == PayloadLength());

    FBufferArchive Ar;
    ArchiveExtensions::EncodeBe(Ar, MagicHeader);
    ArchiveExtensions::EncodeBe(Ar, ProtocolVersion);
    ArchiveExtensions::EncodeBe(Ar, GetCode());
    ArchiveExtensions::EncodeBe(Ar, PayloadLength());
    ArchiveExtensions::EncodeArchiveBe(Ar, VariantAr);

    return Ar;
}

TResult<TSharedPtr<FProtocolVariant>, FString> FProtocolVariant::Decode(const TArray<uint8>& Buffer)
{
    FMemoryReader Ar(Buffer);
#if PLATFORM_LITTLE_ENDIAN
    Ar.ArForceByteSwapping = true;
#endif

    if (Ar.TotalSize() < sizeof(MagicHeader) + sizeof(ProtocolVersion) + sizeof(uint8))
    {
        return TResult<TSharedPtr<FProtocolVariant>, FString>("Buffer too small");
    }

    uint8 DecodedMagicHeader[4];
    uint8 DecodedProtocolVersion;
    uint8 Discriminant;

    Ar << DecodedMagicHeader[0];
    Ar << DecodedMagicHeader[1];
    Ar << DecodedMagicHeader[2];
    Ar << DecodedMagicHeader[3];

    Ar << DecodedProtocolVersion << Discriminant;

    bool headerMatches = true;
    for (size_t i = 0; i < 4; ++i)
    {
        uint8 Lhs = DecodedMagicHeader[i];
        uint8 Rhs = MagicHeaderBytes[i];

        if (Lhs != Rhs)
        {
            headerMatches = false;
        }
    }

    if (!headerMatches)
    {
        return TResult<TSharedPtr<FProtocolVariant>, FString>("Invalid magic header");
    }

    if (DecodedProtocolVersion != ProtocolVersion)
    {
        return TResult<TSharedPtr<FProtocolVariant>, FString>("unknown protocol version");
    }

    uint16 Length = 0;

    for (auto i = 0; i < 2; i++) {
        uint8 Byte;
        Length = Length << 8;
        Ar << Byte;
        Length = Length | Byte;
    }

    switch (Discriminant)
    {
    case 0:
    {
        if (Length != FPing::StaticPayloadLength()) {
            return TResult<TSharedPtr<FProtocolVariant>, FString>("Ping Length Mismatch");
        }

        auto DecodeResult = FPing::DecodeVariant(Ar);
        if (DecodeResult.IsError())
        {
            return TResult<TSharedPtr<FProtocolVariant>, FString>(DecodeResult.GetError());
        }
        return TResult<TSharedPtr<FProtocolVariant>, FString>(MakeShared<FPing>(DecodeResult.GetValue()));
    }
    case 1:
    {
        if (Length != FPingReply::StaticPayloadLength()) {
            return TResult<TSharedPtr<FProtocolVariant>, FString>("Ping Length Mismatch");
        }

        auto DecodeResult = FPingReply::DecodeVariant(Ar);
        if (DecodeResult.IsError())
        {
            return TResult<TSharedPtr<FProtocolVariant>, FString>(DecodeResult.GetError());
        }
        return TResult<TSharedPtr<FProtocolVariant>, FString>(MakeShared<FPingReply>(DecodeResult.GetValue()));
    }
    default:
        // Unknown packet type, return error
        return TResult<TSharedPtr<FProtocolVariant>, FString>("Unknown packet type");
    }
}

FBufferArchive FPing::EncodeVariant() const
{
    FBufferArchive Ar;
    ArchiveExtensions::EncodeBe(Ar, Nonce);
    ArchiveExtensions::EncodeBe(Ar, ClientTimestamp);
    return Ar;
}

TResult<FPing, FString> FPing::DecodeVariant(FMemoryReader& Ar)
{
    if (Ar.TotalSize() - Ar.Tell() < FPing::StaticPayloadLength())
    {
        return TResult<FPing, FString>("Insufficient data for FPing");
    }

    uint8 DecodedNonce;
    int64 DecodedClientTimestamp;

    Ar << DecodedNonce;
    Ar << DecodedClientTimestamp;

    return TResult<FPing, FString>(FPing(DecodedNonce, DecodedClientTimestamp));
}

FBufferArchive FPingReply::EncodeVariant() const
{
    FBufferArchive Ar;
    ArchiveExtensions::EncodeBe(Ar, Nonce);
    ArchiveExtensions::EncodeBe(Ar, ClientTimestamp);
    ArchiveExtensions::EncodeBe(Ar, ServerStartTimestamp);
    ArchiveExtensions::EncodeBe(Ar, ServerTransmitTimestamp);
    return Ar;
}

TResult<FPingReply, FString> FPingReply::DecodeVariant(FMemoryReader& Ar)
{
    if (Ar.TotalSize() - Ar.Tell() < FPingReply::StaticPayloadLength())
    {
        return TResult<FPingReply, FString>("Insufficient data for FPingReply");
    }

    uint8 DecodedNonce;
    int64 DecodedClientTimestamp, DecodedServerStartTimestamp, DecodedServerTransmitTimestamp;

    Ar << DecodedNonce << DecodedClientTimestamp << DecodedServerStartTimestamp << DecodedServerTransmitTimestamp;

    return TResult<FPingReply, FString>(FPingReply(DecodedNonce, DecodedClientTimestamp, DecodedServerStartTimestamp, DecodedServerTransmitTimestamp));
}