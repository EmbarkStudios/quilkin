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
#include "IPAddress.h"

const static FString& QuilkinPingDecodeError = "decode_error";
const static FString& QuilkinPingInvalidPacket = "invalid_packet";
const static FString& QuilkinPingNonceMismatch = "nonce_mismatch";
const static FString& QuilkinPingTimeout = "timeout";
const static FString& QuilkinPingNoValidSamples = "no_valid_samples";

class FQuilkinTelemetry
{
public:
	static void QuilkinPingError(const FString& Host, const uint16 QcmpPort, const FString& Region, const FString& Error, const FString& Endpoint);
};
