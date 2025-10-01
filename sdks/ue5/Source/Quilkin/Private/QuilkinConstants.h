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

inline constexpr uint64_t MillisToNanos(uint64_t milliseconds) {
    return milliseconds * 1000000;
}

constexpr int64 DefaultLatencyThreshold = MillisToNanos(150);
constexpr int64 DefaultPenaltyLatency = MillisToNanos(200);

inline int64 NanosToMillis(int64 Nanoseconds)
{
    return Nanoseconds / 1'000'000;
}

inline float NanosToSeconds(int64 Nanoseconds)
{
    float Nanos = Nanoseconds;
    // nanos -> millis -> seconds
    return (Nanos / 1'000'000) / 1000;
}
