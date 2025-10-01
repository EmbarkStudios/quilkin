#if WITH_DEV_AUTOMATION_TESTS

#include "../QuilkinEndpointMap.h"

#include "CoreMinimal.h"
#include "Misc/AutomationTest.h"

IMPLEMENT_SIMPLE_AUTOMATION_TEST(FGetLowestLatencyEndpoint, "Quilkin.EndpointMap.GetLowestLatencyEndpoint", EAutomationTestFlags::EditorContext | EAutomationTestFlags::EngineFilter)

bool FGetLowestLatencyEndpoint::RunTest(const FString& Parameters)
{
    FQuilkinEndpoint Eu;
    FQuilkinEndpoint Us;
    FQuilkinEndpointMap Map;

    Eu.Host = TEXT("192.0.0.1");
    Us.Host = TEXT("192.0.0.2");

    CircularBuffer<int64> EuLatency;
    TArray<FQuilkinDatacenter> EuDatacenters;
    CircularBuffer<int64> UsLatency;
    TArray<FQuilkinDatacenter> UsDatacenters;

    EuLatency.Add(25);
    EuDatacenters.Add({ TEXT("KLAX"), 45.f });
    UsLatency.Add(50);
    UsDatacenters.Add({ TEXT("KLAX"), 10.f });

    Map.Endpoints.Add(Eu, EuLatency);
    Map.Datacenters.Add(Eu, EuDatacenters);
    Map.Endpoints.Add(Us, UsLatency);
    Map.Datacenters.Add(Us, UsDatacenters);

    TestTrue("empty icao returns eu", Map.GetLowestLatencyEndpoint(FString()).GetValue().template Get<0>() == Eu);
    TestTrue("klax icao returns us", Map.GetLowestLatencyEndpoint(TEXT("KLAX")).GetValue().template Get<0>() == Us);
    return true;
}

#endif // WITH_DEV_AUTOMATION_TESTS
