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
#include "UObject/ObjectMacros.h"
#include "UObject/UObjectGlobals.h"
#include "UObject/Object.h"
#include "Templates/Tuple.h"
#include "QuilkinEndpoint.h"

using EndpointMap = TMap <FString, EndpointPair>;
using DatacenterMap = TMap <FString, int64>;
using AddrLatency = TPair <TSharedPtr<FInternetAddr>, int64>;
class QUILKIN_API FQuilkinDelegates
{
public:
    /**
     * Delegate used to get a copy of the proxy endpoints with their latest median latency.
     */
    DECLARE_DELEGATE_RetVal(TArray<EndpointPair>, FGetQuilkinEndpointMeasurements);
    static FGetQuilkinEndpointMeasurements GetQuilkinEndpointMeasurements;

    /**
     * Delegate used to get the endpoint with the lowest median latency, if the parameter is
     * an empty string, then it returns the lowest to the client, if a datacenter code is provided
     * then it returns the endpoint creates the shortest path to the datacenter. Returns `None` if
     * there are no endpoints, the datacenter parameter is non-empty and not found, or
     * `MeasureEndpoints` is `false`.
     */
    DECLARE_DELEGATE_RetVal_OneParam(TOptional<EndpointPair>, FGetLowestLatencyEndpoint, FString);
    static FGetLowestLatencyEndpoint GetLowestLatencyEndpoint;

    /**
     * Delegate used to get the endpoint that matches the `Region` paramaeter with the
     * lowest median latency. Returns `None` if there are no endpoints matching that region,
     * or `MeasureEndpoints` is `false`.
     */
    DECLARE_DELEGATE_RetVal_OneParam(TOptional<EndpointPair>, FGetLowestLatencyEndpointInRegion, FString);
    static FGetLowestLatencyEndpointInRegion GetLowestLatencyEndpointInRegion;

    /**
     * Delegate used to get the endpoint that matches the `Region` paramaeter with the
     * lowest median latency. Returns `None` if there are no endpoints matching that region,
     * or `MeasureEndpoints` is `false`.
     */
    DECLARE_DELEGATE_RetVal(EndpointMap, FGetLowestLatencyEndpointInEachRegion);
    static FGetLowestLatencyEndpointInEachRegion GetLowestLatencyEndpointInEachRegion;

    /**
     * Delegate used to get the lowest latency measurement to each datacenter.
     */
    DECLARE_DELEGATE_RetVal(DatacenterMap, FGetLowestLatencyToDatacenters);
    static FGetLowestLatencyToDatacenters GetLowestLatencyToDatacenters;

    /**
     * Fired whenever a socket's connection to a proxy has changed with (from, to) as parameters.
     *
     * from: the old address and latency as a TPair.
     * to: new address and latency as a TPair.
     */
    DECLARE_MULTICAST_DELEGATE_TwoParams(FFailoverTriggered, AddrLatency, AddrLatency);
    static FFailoverTriggered FailoverTriggered;
};
