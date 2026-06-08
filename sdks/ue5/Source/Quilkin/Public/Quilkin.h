#pragma once

#include "CoreMinimal.h"
#include "Containers/UnrealString.h"
#include "Logging/StructuredLog.h"
#include "UObject/ObjectMacros.h"
#include "UObject/UObjectGlobals.h"
#include "UObject/Object.h"

#include "QuilkinReachability.h"
#include "../Private/QuilkinConstants.h"
#include "QuilkinSettings.h"
#include "../Private/Net/QuilkinSocketSubsystem.h"

#include "Quilkin.generated.h"

UCLASS()
class QUILKIN_API UQuilkin: public UEngineSubsystem {
	GENERATED_BODY()

	FQuilkinSocketSubsystem* SubSys;
	virtual void Initialize(FSubsystemCollectionBase& Collection) override;
	virtual void Deinitialize() override;
public:
	/** Returns an instance of the Quilkin API subsystem. */
	static UQuilkin* Get();

	/** Checks if the API subsystem has been loaded into the engine. */
	static bool IsAvailable()
	{
		return GEngine != nullptr && GEngine->GetEngineSubsystem<UQuilkin>() != nullptr;
	}

	/** Returns an instance of Quilkin's configuration subsystem. */
	static UQuilkinConfigSubsystem* Config();

	
	/** Returns measured latencies to proxies from shortest to longest */
	TArray<FProxyLatencyLookupResult> GetProxyLatenciesSorted() const;

	/** Returns measured latencies to the chosen datacenter through proxies of the chosen region */ 
	TArray<FProxyLatencyLookupResult> GetDatacenterLatenciesSorted(const FString& IcaoCode, const FString& ProxyRegion) const;
	
	/** Returns the shortest path to each known datacenter */
	TArray<FDatacenterLatencyLookupResult> GetLowestLatencyToDatacenters() const;

	/** Update data based on which Quilkin operates. This includes the list of proxy addresses as well as network distances
	 * between said proxies and gameserver-containing datacenters.
	 */
	void UpdateEndpoints(TArray<FQuilkinEndpoint>&& Endpoints, TArray<FQuilkinTopology>&& Topologies) const;

	/** Fired whenever Quilkin has completed network measurements */
	DECLARE_MULTICAST_DELEGATE(FOnMeasurementCompleted);
	static FOnMeasurementCompleted OnMeasurementCompleted;
	
	
	/** Returns whether the client can reach the public internet, returning whether it can,
	  * and how long it took to execute.
	  */
	ReachPair ConnectionStatus() const;

	/** When executed returns whether the client can reach the provided endpoint over the public internet,
	  * returning whether it can and how long it took to execute.
	  */
	ReachPair ConnectionStatusToProxy(const FQuilkinEndpoint& Endpoint) const;
};
