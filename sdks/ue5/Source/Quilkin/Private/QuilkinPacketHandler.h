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
#include "QuilkinLog.h"
#include "Serialization/BitWriter.h"
#include "Misc/Base64.h"

class FQuilkinPacketHandler
{
public:
    FQuilkinPacketHandler();
    bool IsEnabled();
    bool IsDisabled() {
        return !IsEnabled();
    };

    template<typename Fn> FORCEINLINE const bool Write(const uint8* Data, int32 CountBytes, int32& BytesSent, Fn WriteToSocket)
    {
        if (IsDisabled()) {
            return WriteToSocket(Data, CountBytes, BytesSent);
        }
        ensureMsgf(RoutingToken.Num() == 16, TEXT("Routing token must be 16 bytes, received %d, proxy connection will fail"), RoutingToken.Num());
        
        // Add the current packet version.
        uint8 PacketVersion = 0;
        int PacketVersionNumBytes = 1;
        // Reserve enough space for the token and packet version.
        FBitWriter Packet((CountBytes + RoutingToken.Num() + PacketVersionNumBytes) * 8, true);

        Packet.Serialize((void*)Data, CountBytes);
        Packet.Serialize(RoutingToken.GetData(), RoutingToken.Num());
        Packet.Serialize(&PacketVersion, PacketVersionNumBytes);
        if (UE_LOG_ACTIVE(LogQuilkin, VeryVerbose))
        {
            FString Base64Token = FBase64::Encode(RoutingToken);
            UE_LOG(LogQuilkin, VeryVerbose, TEXT("Wrapping packet in Quilkin PDU; version: %d, size: %d, token: %s"), PacketVersion, Packet.GetNumBytes(), *Base64Token);
        }
        return WriteToSocket(Packet.GetData(), Packet.GetNumBytes(), BytesSent);
    }

private:
    TArray<uint8> RoutingToken;
    bool Enabled = false;
};
