#include "QuilkinEndpointMap.h"

#include "../QuilkinConstants.h"
#include "QuilkinLog.h"

FQuilkinEndpointMap::FQuilkinEndpointMap(TArray<FQuilkinEndpoint>&& NewEndpoints, TArray<FQuilkinTopology>&& NewTopologies, TArray<CircularBuffer<int64>>&& Measurements)
	: Endpoints(MoveTemp(NewEndpoints)), Topologies(MoveTemp(NewTopologies))
{
	check(Endpoints.Num() == Topologies.Num() && Endpoints.Num() == Measurements.Num());

	CreateMeasuredProxyIndex(Measurements);
	CacheProxyLatencyLookupResults();
	CacheDatacenterLatencyLookupResults();
	
	UE_LOG(LogQuilkin, Display, TEXT("%s"), *ToLog());
}

void FQuilkinEndpointMap::CreateMeasuredProxyIndex(const TArray<CircularBuffer<int64>>& Measurements)
{
	for (int32 i = 0; i < Measurements.Num(); ++i)
	{
		if (!Measurements[i].IsEmpty())
		{
			MeasuredProxies.Emplace( Measurements[i].Median(), i);
		}
	}
	
	MeasuredProxies.Sort([](const auto& A, const auto& B) { return A.LatencyNanos < B.LatencyNanos; });
}

void FQuilkinEndpointMap::CacheDatacenterLatencyLookupResults()
{
	TMap</* Icao */ FString, TPair</* Latency */ int64, /* Index */ int32>> Lookups;
	
	for (auto& [ProxyLatency, Index] : MeasuredProxies)
	{
		for (const auto& [Icao, DcLatency] : Topologies[Index].DatacenterLatencies)
		{
			const int64 Total = ProxyLatency + DcLatency;
			auto& [FoundEntry, FoundIndex] = Lookups.FindOrAdd(Icao, { Total, Index });
			if (Total < FoundEntry)
			{
				FoundEntry = Total;
				FoundIndex = Index;
			}
		}
	}

	DatacenterLatencyCache.Reserve(Lookups.Num());
	for (auto& [Icao, LatencyIndex] : Lookups)
	{
		DatacenterLatencyCache.Emplace(Endpoints[LatencyIndex.Value], Icao, LatencyIndex.Key);
	}

	// Sorting so that the results look good in the log
	DatacenterLatencyCache.Sort([](const auto& A, const auto& B) { return A.LatencyNanos < B.LatencyNanos; });
}

void FQuilkinEndpointMap::CacheProxyLatencyLookupResults()
{
	ProxyLatencyCache.Reserve(MeasuredProxies.Num());
	for (auto& [ProxyLatency, Index] : MeasuredProxies)
	{
		ProxyLatencyCache.Add({ Endpoints[Index], Topologies[Index].Region, ProxyLatency });
	}
}

TArray<FProxyLatencyLookupResult> FQuilkinEndpointMap::GetDatacenterLatenciesSorted(const FString& IcaoCode, const FString& ProxyRegion) const
{
	TArray<FProxyLatencyLookupResult> Results;
	for (auto& [ProxyLatency, Index] : MeasuredProxies)
	{
		if (ProxyRegion != Topologies[Index].Region)
		{
			continue;
		}

		const int64* DcLatency = Topologies[Index].DatacenterLatencies.Find(IcaoCode);
		if (!DcLatency)
		{
			continue;
		}

		Results.Add({ Endpoints[Index], Topologies[Index].Region, ProxyLatency + *DcLatency });
	}

	Results.Sort([](const auto& A, const auto& B) { return A.LatencyNanos < B.LatencyNanos; });
	return Results;
}

TArray<FProxyLatencyLookupResult> FQuilkinEndpointMap::GetProxyLatenciesSorted() const
{
	return ProxyLatencyCache;
}

TArray<FDatacenterLatencyLookupResult> FQuilkinEndpointMap::GetLowestLatencyToDatacenters() const
{
	return DatacenterLatencyCache;
}

FString FQuilkinEndpointMap::ToLog() const
{
	FString Output;
	
	Output += "\nClient latency to proxies: ";
	for (auto& [ProxyLatency, Index] : MeasuredProxies)
	{
		Output += FString::Printf(TEXT("(%s: %lldms) "), *Endpoints[Index].Host, NanosToMillis(ProxyLatency));
	}
	
	Output += "\nClient latency to datacenters: ";
	for (auto& [Endpoint, Icao, LatencyIndex] : DatacenterLatencyCache)
	{
		Output += FString::Printf(TEXT("(%s %s: %lldms) "), *Icao, *Endpoint.Host, NanosToMillis(LatencyIndex));
	}

	return Output;
}
