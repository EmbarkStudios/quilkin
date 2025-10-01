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

#include "QuilkinPacketHandler.h"
#include "Engine/GameInstance.h"
#include "QuilkinSettings.h"

FQuilkinPacketHandler::FQuilkinPacketHandler()
{
    auto Settings = UQuilkinConfigSubsystem::Get();
    RoutingToken = Settings->GetRoutingToken();
    Enabled = Settings->PacketHandling && Settings->GetEnabled();

    if (UE_LOG_ACTIVE(LogQuilkin, Display))
    {
        FString Base64Token = FBase64::Encode(RoutingToken);
        UE_LOG(LogQuilkin, Display, TEXT("Initialising PacketHandler: Packet Handling: %s, Routing Token: %s"), Enabled ? TEXT("Enabled") : TEXT("Disabled"), *Base64Token);
    }
}

bool FQuilkinPacketHandler::IsEnabled()
{
    // If it was disabled when it was initially enabled, keep it disabled,
    // If it was disabled later while it was enabled, disable it.
    return Enabled && UQuilkinConfigSubsystem::Get()->GetEnabled();
}
