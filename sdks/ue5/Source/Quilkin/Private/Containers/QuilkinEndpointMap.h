#pragma once

#include <cmath>

#include "CoreMinimal.h"

#include "QuilkinCircularBuffer.h"
#include "QuilkinEndpoint.h"

class FQuilkinSocketSubsystem;

struct FProxyLatencyLookupResult
{
	const FQuilkinEndpoint& Endpoint;
	const FString& Region;
	const int64 LatencyNanos;
};

struct FDatacenterLatencyLookupResult
{
	const FQuilkinEndpoint& Endpoint;
	const FString IcaoCode;
	const int64 LatencyNanos;
};

class FQuilkinEndpointMap
{
public:
	FQuilkinEndpointMap() = default;
	FQuilkinEndpointMap(TArray<FQuilkinEndpoint>&& Endpoints,
						TArray<FQuilkinTopology>&& Topologies,
						TArray<CircularBuffer<int64>>&& Measurements);

	/* Returns measured distances to proxies from shortest to longest */
	TArray<FProxyLatencyLookupResult> GetProxyLatenciesSorted() const;

	/* Returns measured distances to the chosen datacenter through proxies in the chosen region */ 
	TArray<FProxyLatencyLookupResult> GetDatacenterLatenciesSorted(const FString& IcaoCode, const FString& ProxyRegion) const;
	
	/* Returns the length of the shortest path to each known datacenter */
	TArray<FDatacenterLatencyLookupResult> GetLowestLatencyToDatacenters() const;
	
	TArray<FQuilkinEndpoint> GetEndpoints() const { return Endpoints; }
	TArray<FQuilkinTopology> GetTopologies() const { return Topologies; }
	
private:
	FString ToLog() const;
	
	// Static data
	TArray<FQuilkinEndpoint>       Endpoints;
	TArray<FQuilkinTopology>       Topologies;

	// Index sorted by measured proxy latency
	struct ProxyMeasurement
	{
		int64 LatencyNanos;
		int32 Index;
	};
	TArray<ProxyMeasurement> MeasuredProxies;
	void CreateMeasuredProxyIndex(const TArray<CircularBuffer<int64>>& Measurements);

	// Cache
	TArray<FProxyLatencyLookupResult> ProxyLatencyCache;
	void CacheProxyLatencyLookupResults();
	
	TArray<FDatacenterLatencyLookupResult> DatacenterLatencyCache;
	void CacheDatacenterLatencyLookupResults();
};
