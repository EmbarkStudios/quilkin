#pragma once

#include "CoreMinimal.h"

struct FQuilkinDatacenter {
    FString IcaoCode;
    double Distance;

    int64 TotalDistance(int64 ProxyLatency) const {
        int64 RoundedLatency = std::llround(Distance);
        return ProxyLatency + RoundedLatency;
    }
};
