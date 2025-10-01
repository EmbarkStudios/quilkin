#pragma once

#include <cmath>

#include "Async/Async.h"
#include "Async/AsyncWork.h"
#include "CoreMinimal.h"
#include "Logging/StructuredLog.h"
#include "Misc/QueuedThreadPool.h"
#include "SocketSubsystem.h"

#include "QuilkinCircularBuffer.h"
#include "QuilkinConcurrentMap.h"
#include "QuilkinConstants.h"
#include "QuilkinDatacenter.h"
#include "QuilkinEndpoint.h"
#include "QuilkinSocket.h"

class FQuilkinSocketSubsystem;

struct FQuilkinEndpointMap {
    TSConcurrentMap<FQuilkinEndpoint, TArray<FQuilkinDatacenter>> Datacenters;
    TSConcurrentMap<FQuilkinEndpoint, CircularBuffer<int64>> Endpoints;

    /* Returns a tuple of the latency with the lowest median latency endpoint, along with
       the median latency of that endpoint. Returns `None` if no endpoints available.
    */
    TOptional<EndpointPair> GetLowestLatencyEndpoint(FString IcaoCode);

    /* Given OldAddr, will return the lowest latency endpoint that does not match any of OldAddrs.
    *  If OldAddr doesn't match any endpoint this is equivalvent of GetLowestLatencyEndpoint.
    *  Returns None if no endpoints available.
    */
    TOptional<EndpointPair> FindNextLowestLatencyEndpoint(FQuilkinSocketSubsystem* SubSys, const TArray<TSharedPtr<FInternetAddr>>& OldAddrs);

    /* Returns an array of the lowest latency endpoint in each region. */
    TMap<FString, EndpointPair> GetLowestLatencyEndpointInEachRegion();

    TOptional<FQuilkinEndpoint> GetLowestLatencyProxyToDatacenter(FString Datacenter) const;
    TMap<FString, int64> GetLowestLatencyToDatacenters() const;
    TOptional<TTuple<FQuilkinEndpoint, int64>> GetLowestLatencyEndpointInRegion(FString Region);
    /* Shared implementation between `GetLowestLatencyEndpoint` and `GetLowestLatencyEndpointInRegion`. */
    template <typename Fn> TOptional<TTuple<FQuilkinEndpoint, int64>> GetLowestLatencyEndpointImplementation(FString IcaoCode, Fn Filter);
    TArray<TTuple<FQuilkinEndpoint, int64>> GetEndpointMeasurements();
};
