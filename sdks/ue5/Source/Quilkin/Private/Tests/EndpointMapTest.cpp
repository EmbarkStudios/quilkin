#if WITH_DEV_AUTOMATION_TESTS

#include "Containers/QuilkinEndpointMap.h"

#include "CoreMinimal.h"
#include "Misc/AutomationTest.h"

IMPLEMENT_SIMPLE_AUTOMATION_TEST(FGetProxyLatenciesSorted, "Quilkin.EndpointMap.GetProxyLatenciesSorted", EAutomationTestFlags::EditorContext | EAutomationTestFlags::EngineFilter)
bool FGetProxyLatenciesSorted::RunTest(const FString& Parameters)
{
	// Empty map returns empty result
	{
		FQuilkinEndpointMap EmptyMap({}, {}, {});
		auto EmptyResult = EmptyMap.GetProxyLatenciesSorted();
		TestEqual("empty map returns empty result", EmptyResult.Num(), 0);
	}

	// Eu1: proxy latency 25
	FQuilkinEndpoint Eu1;
	Eu1.Host = "192.0.0.1";

	FQuilkinTopology Eu1Topo;
	Eu1Topo.Region = "EU";

	CircularBuffer<int64> Eu1Measurements;
	Eu1Measurements.Add(25);

	// Eu2: proxy latency 50
	FQuilkinEndpoint Eu2;
	Eu2.Host = "192.0.0.2";

	FQuilkinTopology Eu2Topo;
	Eu2Topo.Region = "EU";

	CircularBuffer<int64> Eu2Measurements;
	Eu2Measurements.Add(50);

	// Us: proxy latency 30
	FQuilkinEndpoint Us;
	Us.Host = "192.0.0.3";

	FQuilkinTopology UsTopo;
	UsTopo.Region = "US";

	CircularBuffer<int64> UsMeasurements;
	UsMeasurements.Add(30);

	FQuilkinEndpointMap Map(
		{ Eu1, Eu2, Us },
		{ Eu1Topo, Eu2Topo, UsTopo },
		{ Eu1Measurements, Eu2Measurements, UsMeasurements }
	);

	auto Result = Map.GetProxyLatenciesSorted();

	// Sorted by proxy latency: Eu1(25), Us(30), Eu2(50)
	TestEqual("result has 3 endpoints", Result.Num(), 3);
	TestEqual("first is Eu1 (lowest latency)", Result[0].Endpoint.Host, FString("192.0.0.1"));
	TestEqual("first latency is 25", Result[0].LatencyNanos, 25);
	TestEqual("first region is EU", Result[0].Region, FString("EU"));
	TestEqual("second is Us", Result[1].Endpoint.Host, FString("192.0.0.3"));
	TestEqual("second latency is 30", Result[1].LatencyNanos, 30);
	TestEqual("second region is US", Result[1].Region, FString("US"));
	TestEqual("third is Eu2", Result[2].Endpoint.Host, FString("192.0.0.2"));
	TestEqual("third latency is 50", Result[2].LatencyNanos, 50);
	return true;
}

IMPLEMENT_SIMPLE_AUTOMATION_TEST(FGetDatacenterLatenciesSorted, "Quilkin.EndpointMap.GetDatacenterLatenciesSorted", EAutomationTestFlags::EditorContext | EAutomationTestFlags::EngineFilter)
bool FGetDatacenterLatenciesSorted::RunTest(const FString& Parameters)
{
	// Eu1: proxy latency 25, KLAX 45 (total 70), KJFK 100 (total 125)
	FQuilkinEndpoint Eu1;
	Eu1.Host = "192.0.0.1";

	FQuilkinTopology Eu1Topo;
	Eu1Topo.Region = "EU";
	Eu1Topo.DatacenterLatencies.Add("KLAX", 45);
	Eu1Topo.DatacenterLatencies.Add("KJFK", 100);

	CircularBuffer<int64> Eu1Measurements;
	Eu1Measurements.Add(25);

	// Eu2: proxy latency 40, KLAX 10 (total 50), KJFK 80 (total 120)
	FQuilkinEndpoint Eu2;
	Eu2.Host = "192.0.0.2";

	FQuilkinTopology Eu2Topo;
	Eu2Topo.Region = "EU";
	Eu2Topo.DatacenterLatencies.Add("KLAX", 10);
	Eu2Topo.DatacenterLatencies.Add("KJFK", 80);

	CircularBuffer<int64> Eu2Measurements;
	Eu2Measurements.Add(40);

	// Us: proxy latency 50, KLAX 10 (total 60), KJFK 30 (total 80)
	FQuilkinEndpoint Us;
	Us.Host = "192.0.0.3";

	FQuilkinTopology UsTopo;
	UsTopo.Region = "US";
	UsTopo.DatacenterLatencies.Add("KLAX", 10);
	UsTopo.DatacenterLatencies.Add("KJFK", 30);

	CircularBuffer<int64> UsMeasurements;
	UsMeasurements.Add(50);

	FQuilkinEndpointMap Map(
		{ Eu1, Eu2, Us },
		{ Eu1Topo, Eu2Topo, UsTopo },
		{ Eu1Measurements, Eu2Measurements, UsMeasurements }
	);

	// KLAX+EU: Eu1 total 70, Eu2 total 50 -> picks Eu2
	{
		auto Result = Map.GetDatacenterLatenciesSorted("KLAX", "EU");
		TestTrue("KLAX+EU returns results", Result.Num() > 0);
		TestEqual("KLAX+EU picks Eu2 (lowest total)", Result[0].Endpoint.Host, FString("192.0.0.2"));
		TestEqual("KLAX+EU total distance", Result[0].LatencyNanos, 40 + 10);
	}

	// KJFK+EU: Eu1 total 125, Eu2 total 120 -> picks Eu2
	{
		auto Result = Map.GetDatacenterLatenciesSorted("KJFK", "EU");
		TestTrue("KJFK+EU returns results", Result.Num() > 0);
		TestEqual("KJFK+EU picks Eu2 (lowest total)", Result[0].Endpoint.Host, FString("192.0.0.2"));
		TestEqual("KJFK+EU total distance", Result[0].LatencyNanos, 40 + 80);
	}

	// KLAX+US: only Us, total 60
	{
		auto Result = Map.GetDatacenterLatenciesSorted("KLAX", "US");
		TestTrue("KLAX+US returns results", Result.Num() > 0);
		TestEqual("KLAX+US returns US endpoint", Result[0].Endpoint.Host, FString("192.0.0.3"));
		TestEqual("KLAX+US total distance", Result[0].LatencyNanos, 50 + 10);
	}

	// KJFK+US: only Us, total 80
	{
		auto Result = Map.GetDatacenterLatenciesSorted("KJFK", "US");
		TestTrue("KJFK+US returns results", Result.Num() > 0);
		TestEqual("KJFK+US returns US endpoint", Result[0].Endpoint.Host, FString("192.0.0.3"));
		TestEqual("KJFK+US total distance", Result[0].LatencyNanos, 50 + 30);
	}

	{
		auto NoResult = Map.GetDatacenterLatenciesSorted("KLAX", "UNKNOWN");
		TestEqual("nonexistent region returns empty", NoResult.Num(), 0);
	}

	{
		auto NoResult = Map.GetDatacenterLatenciesSorted("UNKNOWN", "US");
		TestEqual("nonexistent datacenter returns empty", NoResult.Num(), 0);
	}
	return true;
}

IMPLEMENT_SIMPLE_AUTOMATION_TEST(FGetLowestLatencyToDatacenters, "Quilkin.EndpointMap.GetLowestLatencyToDatacenters", EAutomationTestFlags::EditorContext | EAutomationTestFlags::EngineFilter)
bool FGetLowestLatencyToDatacenters::RunTest(const FString& Parameters)
{
	// Empty map returns empty result
	{
		FQuilkinEndpointMap EmptyMap({}, {}, {});
		auto EmptyResult = EmptyMap.GetLowestLatencyToDatacenters();
		TestEqual("empty map returns empty result", EmptyResult.Num(), 0);
	}

	// Proxy1: latency 25, KLAX distance 45 (total 70), KJFK distance 100 (total 125)
	FQuilkinEndpoint Proxy1;
	Proxy1.Host = "192.0.0.1";

	FQuilkinTopology Proxy1Topo;
	Proxy1Topo.DatacenterLatencies.Add("KLAX", 45);
	Proxy1Topo.DatacenterLatencies.Add("KJFK", 100);

	CircularBuffer<int64> Proxy1Measurements;
	Proxy1Measurements.Add(25);

	// Proxy2: latency 50, KLAX distance 10 (total 60), KJFK distance 30 (total 80)
	FQuilkinEndpoint Proxy2;
	Proxy2.Host = "192.0.0.2";

	FQuilkinTopology Proxy2Topo;
	Proxy2Topo.DatacenterLatencies.Add("KLAX", 10);
	Proxy2Topo.DatacenterLatencies.Add("KJFK", 30);

	CircularBuffer<int64> Proxy2Measurements;
	Proxy2Measurements.Add(50);

	FQuilkinEndpointMap Map(
		{ Proxy1, Proxy2 },
		{ Proxy1Topo, Proxy2Topo },
		{ Proxy1Measurements, Proxy2Measurements }
	);

	auto Result = Map.GetLowestLatencyToDatacenters();
	
	// Find results by ICAO code
	const auto* KlaxResult = Result.FindByPredicate([](const auto& R) { return R.IcaoCode == "KLAX"; });
	const auto* KjfkResult = Result.FindByPredicate([](const auto& R) { return R.IcaoCode == "KJFK"; });

	TestEqual("result has 2 datacenters", Result.Num(), 2);
	
	if (TestTrue("KLAX entry exists", KlaxResult != nullptr))
	{
		// KLAX: min(25+45=70, 50+10=60) = 60
		TestEqual("KLAX has lowest total distance", KlaxResult->LatencyNanos, 60);
	}

	if (TestTrue("KJFK entry exists", KjfkResult != nullptr))
	{
		// KJFK: min(25+100=125, 50+30=80) = 80
		TestEqual("KJFK has lowest total distance", KjfkResult->LatencyNanos, 80);
	}
	return true;
}

#endif // WITH_DEV_AUTOMATION_TESTS
