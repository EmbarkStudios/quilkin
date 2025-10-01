#include "QuilkinEndpointMap.h"

TOptional<EndpointPair> FQuilkinEndpointMap::GetLowestLatencyEndpoint(FString IcaoCode)
{
    return GetLowestLatencyEndpointImplementation(IcaoCode, [](FQuilkinEndpoint _e, int64 _m) { return true; });
}

TOptional<TTuple<FQuilkinEndpoint, int64>> FQuilkinEndpointMap::GetLowestLatencyEndpointInRegion(FString Region)
{
    return GetLowestLatencyEndpointImplementation(FString(), [Region](FQuilkinEndpoint Endpoint, int64 Median) {
        return Region == Endpoint.Region;
    });

}

TOptional<EndpointPair> FQuilkinEndpointMap::FindNextLowestLatencyEndpoint(FQuilkinSocketSubsystem* SubSys, const TArray<TSharedPtr<FInternetAddr>>& OldAddrs)
{
    return GetLowestLatencyEndpointImplementation(FString(), [this, SubSys, OldAddrs](FQuilkinEndpoint Endpoint, int64 Median) {
        // If ExcludeAddr is set, exclude it from the matches.
        if (!OldAddrs.IsEmpty()) {
            UE_LOG(LogQuilkin, Verbose, TEXT("skipping excluded addresses"));
            for (const auto& Addr : OldAddrs) {
                UE_LOGFMT(LogQuilkin, Verbose, "skipping {0}", Addr->ToString(true));
                if (*Addr == Endpoint.ToInternetAddr(SubSys).GetValue().Get()) {
                    return false;
                }
            }
        }

        return true;
    });
}

EndpointMap FQuilkinEndpointMap::GetLowestLatencyEndpointInEachRegion()
{
    auto FoundEndpoints = TMap<FString, EndpointPair>();
    if (Endpoints.IsEmpty()) {
        UE_LOG(LogQuilkin, Verbose, TEXT("no endpoints available"));
        return FoundEndpoints;
    }

    Endpoints.ForEach([&FoundEndpoints](FQuilkinEndpoint Endpoint, CircularBuffer<int64> Buffer) {
        int64 Median = Buffer.Median();

        if (Buffer.IsEmpty() || Median == 0) {
            UE_LOGFMT(LogQuilkin, Verbose, "{0} hasn't been measured", Endpoint.ToString());
        } else if (!FoundEndpoints.Contains(Endpoint.Region) || FoundEndpoints.Find(Endpoint.Region)->Value > Median) {
            FoundEndpoints.Add(Endpoint.Region, EndpointPair(Endpoint, Median));
        }
    });

    return FoundEndpoints;
}

template <typename Fn> TOptional<EndpointPair> FQuilkinEndpointMap::GetLowestLatencyEndpointImplementation(FString IcaoCode, Fn Filter)
{
    if (Endpoints.IsEmpty()) {
        UE_LOG(LogQuilkin, Verbose, TEXT("no endpoints available"));
        return TOptional<EndpointPair>();
    }

    auto Result = TOptional<EndpointPair>();
    Endpoints.ForEach([this, &Result, &Filter, IcaoCode](FQuilkinEndpoint Endpoint, CircularBuffer<int64> Buffer) {
        int64 Median = Buffer.Median();
        auto FoundDatacenters = Datacenters.Find(Endpoint);

        if (Buffer.IsEmpty() || Median == 0) {
            UE_LOGFMT(LogQuilkin, Verbose, "{0} hasn't been measured", Endpoint.ToString());
        } else if (!Filter(Endpoint, Median)) {
            UE_LOGFMT(LogQuilkin, Verbose, "{0} failed filter", Endpoint.ToString());
        } else if (!Result.IsSet()) {
            int64 Latency;

            if (!IcaoCode.IsEmpty() && FoundDatacenters) {
                for (const auto& Datacenter : *FoundDatacenters) {
                    if (Datacenter.IcaoCode == IcaoCode) {
                        Latency = Datacenter.TotalDistance(Median);
                    }
                }
            }
            else {
                Latency = Median;
            }

            Result.Emplace(EndpointPair(Endpoint, Latency));
        } else if (IcaoCode.IsEmpty() && Result.IsSet() && Result.GetValue().template Get<1>() > Median) {
            Result.Emplace(EndpointPair(Endpoint, Median));
        } else if (FoundDatacenters) {
            for (const auto& Datacenter : *FoundDatacenters) {
                auto Latency = Datacenter.TotalDistance(Median);
                if (IcaoCode == Datacenter.IcaoCode && Result.GetValue().template Get<1>() > Latency) {
                    Result.Emplace(EndpointPair(Endpoint, Latency));
                }
            }
        }
    });

    if (Result.IsSet()) {
        auto Pair = Result.GetValue();
        UE_LOGFMT(LogQuilkin, Verbose, "found endpoint {0}, latency: {1}", Pair.template Get<0>().ToString(), Pair.template Get<1>());
    }
    else {
        UE_LOGFMT(LogQuilkin, Verbose, "no endpoints available");
    }

    return Result;
}

TArray<TTuple<FQuilkinEndpoint, int64>> FQuilkinEndpointMap::GetEndpointMeasurements() {
    return Endpoints.FilterMapToArray<TTuple<FQuilkinEndpoint, int64>>([](FQuilkinEndpoint Endpoint, CircularBuffer<int64> Buffer) {
        auto Median = Buffer.Median();
        if (Median == 0) {
            return TOptional<TTuple<FQuilkinEndpoint, int64>>();
        }
        else {
            return TOptional<TTuple<FQuilkinEndpoint, int64>>(TTuple<FQuilkinEndpoint, int64>(Endpoint, Median));
        }
    });
}

TOptional<FQuilkinEndpoint> FQuilkinEndpointMap::GetLowestLatencyProxyToDatacenter(FString IcaoCode) const {
    TOptional<FQuilkinEndpoint> FoundEndpoint;
    int64 LowestLatency = INT64_MAX;

    Endpoints.ForEach([this, &FoundEndpoint, LowestLatency, IcaoCode](FQuilkinEndpoint Endpoint, CircularBuffer<int64> Buffer) {
        auto ProxyLatency = Buffer.Median();

        const TArray<FQuilkinDatacenter>* FoundEntries = Datacenters.Find(Endpoint);

        if (FoundEntries == nullptr) {
            UE_LOG(LogQuilkin, Verbose, TEXT("no measured datacenters for %s"), *Endpoint.ToString());
            return;
        }

        auto FoundDatacenter = FoundEntries->FindByPredicate([IcaoCode](auto Datacenter) {
            return Datacenter.IcaoCode == IcaoCode;
        });

        if (FoundDatacenter == nullptr) {
            UE_LOG(LogQuilkin, Verbose, TEXT("haven't measured %s for %s"), *IcaoCode, *Endpoint.ToString());
            return;
        }

        if (FoundEndpoint.IsSet()) {
            if (FoundDatacenter->TotalDistance(ProxyLatency) < LowestLatency) {
                FoundEndpoint = Endpoint;
            }
        }
        else {
            FoundEndpoint = Endpoint;
        }
    });

    return FoundEndpoint;
}

TMap<FString, int64> FQuilkinEndpointMap::GetLowestLatencyToDatacenters() const
{
    TMap<FString, int64> Map;

    Endpoints.ForEach([this, &Map](FQuilkinEndpoint Endpoint, CircularBuffer<int64> Buffer) {
        auto ProxyLatency = Buffer.Median();
        auto FoundDatacenters = Datacenters.Find(Endpoint);

        if (FoundDatacenters == nullptr) {
            UE_LOG(LogQuilkin, Verbose, TEXT("no measured datacenters for %s"), *Endpoint.ToString());
            return;
        }

        for (auto& Datacenter : *FoundDatacenters) {
            auto FoundEntry = Map.Find(Datacenter.IcaoCode);

            if (FoundEntry == nullptr) {
                Map.Add(Datacenter.IcaoCode, Datacenter.TotalDistance(ProxyLatency));
            }
            else {
                auto TotalDistance = Datacenter.TotalDistance(ProxyLatency);
                if (*FoundEntry > TotalDistance) {
                    Map.Add(Datacenter.IcaoCode, TotalDistance);
                }
            }
        }
    });

    return Map;
}
