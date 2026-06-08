#include "Quilkin.h"
#include "QuilkinLog.h"

UQuilkin::FOnMeasurementCompleted UQuilkin::OnMeasurementCompleted;

UQuilkin* UQuilkin::Get()
{
	checkf(GEngine != nullptr, TEXT("UQuilkinConfigSubsystem can only be called inside an Engine context"));
	UQuilkin* Subsystem = GEngine->GetEngineSubsystem<UQuilkin>();
	checkf(Subsystem != nullptr, TEXT("UQuilkinConfigSubsystem hasn't been initialised"));
	return Subsystem;
}

void UQuilkin::Initialize(FSubsystemCollectionBase& Collection)
{
	UE_LOG(LogQuilkin, Display, TEXT("Initialising Quilkin"));
	SubSys = static_cast<FQuilkinSocketSubsystem*>(FQuilkinSocketSubsystem::Get(QUILKIN_SOCKETSUBSYSTEM_NAME));
}

void UQuilkin::Deinitialize()
{
	UE_LOG(LogQuilkin, Display, TEXT("Tearing down Quilkin API"));

	if (OnMeasurementCompleted.IsBound())
	{
		OnMeasurementCompleted.Clear();
	}
}

UQuilkinConfigSubsystem* UQuilkin::Config()
{
	return UQuilkinConfigSubsystem::Get();
}

TArray<FProxyLatencyLookupResult> UQuilkin::GetProxyLatenciesSorted() const 
{
	return SubSys->Map.GetProxyLatenciesSorted();
}

TArray<FProxyLatencyLookupResult> UQuilkin::GetDatacenterLatenciesSorted(const FString& IcaoCode, const FString& ProxyRegion) const
{
	return SubSys->Map.GetDatacenterLatenciesSorted(IcaoCode, ProxyRegion);
}

TArray<FDatacenterLatencyLookupResult> UQuilkin::GetLowestLatencyToDatacenters() const
{
	return SubSys->Map.GetLowestLatencyToDatacenters();
}

TPair<FString, int64> UQuilkin::ConnectionStatus() const
{
	auto Pair = QuilkinReachability::Unreal(&*SubSys).ConnectionStatus();
	return { EQuilkinReachabilityToString(Pair.Key), Pair.Value };
}

TPair<FString, int64> UQuilkin::ConnectionStatusToProxy(const FQuilkinEndpoint& Endpoint) const
{
	auto Pair = QuilkinReachability::Unreal(&*SubSys).ConnectionStatusToProxy(*Endpoint.ToQcmpInternetAddr(&*SubSys).GetValue());
	return { EQuilkinReachabilityToString(Pair.Key), Pair.Value };
}

void UQuilkin::UpdateEndpoints(TArray<FQuilkinEndpoint>&& Endpoints, TArray<FQuilkinTopology>&& Topologies) const
{
	return SubSys->UpdateEndpoints(MoveTemp(Endpoints), MoveTemp(Topologies));
}
