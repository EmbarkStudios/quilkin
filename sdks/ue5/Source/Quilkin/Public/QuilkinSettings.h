#pragma once

#include "CoreMinimal.h"
#include "Engine/Engine.h"
#include "Engine/DeveloperSettings.h"
#include "Subsystems/EngineSubsystem.h"
#include "QuilkinEndpoint.h"

#include "QuilkinSettings.generated.h"

using DatacenterMap = TMap<FString, int64>;

UCLASS(config = Engine, defaultconfig, meta = (DisplayName="Quilkin Settings"))
class QUILKIN_API UQuilkinDeveloperSettings : public UDeveloperSettings
{
    GENERATED_BODY()

protected:
    FName CategoryName = FName("Plugins");
    FName SectionName = FName("Quilkin");

public:
    // UDeveloperSettings overrides
    UQuilkinDeveloperSettings(const FObjectInitializer& ObjectInitializer) {};

    bool IsEnabled() const {
        return Enabled && IsEnabledInEditor();
    }

    bool IsEnabledInEditor() const {
#if WITH_EDITOR
        return EnabledInPie;
#else
        return true;
#endif
    }

    /** The token used to route traffic from the proxy to the appropiate gameserver */
    UPROPERTY(config, EditAnywhere, Category = Settings)
    TArray<uint8> RoutingToken;

    /** Whether to use Quilkin proxy routing in-game */
    UPROPERTY(config, EditAnywhere, Category = Settings)
    bool Enabled = false;

    /** Whether to use Quilkin proxy routing in-editor */
    UPROPERTY(config, EditAnywhere, Category = Settings)
    bool EnabledInPie = false;

    /** Whether to regularly measure each endpoint in `Endpoints`'s latency. */
    UPROPERTY(config, EditAnywhere, Category = Settings)
    bool MeasureEndpoints = false;

    /** The amount of time (in milliseconds) that Quilkin will consider a proxy too
      * far to be worth measuring the full datacenter latency.
      */
    UPROPERTY(config, EditAnywhere, Category = Settings)
    uint64 PingThresholdMillis = 185;

    /** The amount of time (in seconds) that Quilkin should wait before switching
      * to the next available proxy.
      */
    UPROPERTY(config, EditAnywhere, Category = Settings)
    float JitterThreshold = 0.5;

    /** List of endpoints to Quilkin proxies */
    UPROPERTY(config, EditAnywhere, Category = Settings)
    TArray<FQuilkinEndpoint> Endpoints;

    /** Ensures available addresses prioritises IPv6 addresses. */
    UPROPERTY(config, EditAnywhere, Category = Settings)
    bool IPv6Prioritised = false;

    /** Enables the proxy to dynamically switch between proxies during gameplay . */
    UPROPERTY(config, EditAnywhere, Category = Settings)
    bool ProxyFailover = false;

    /** The amount of time (in seconds) that proxy switching should be disabled for
      * after making a switch. 
      */
    UPROPERTY(config, EditAnywhere, Category = Settings)
    float Cooldown = 20.0;
};

/** Defines a property and a delegate for that property, and provides a getter
  * which will call the delegate if bound, otherwise will call the primitive property.
  */
#define DECLARE_PROPERTY_AND_DELEGATE(Type, PropName) \
    private: \
        UPROPERTY(EditAnywhere, Category = "Quilkin") \
        Type PropName; \
    public: \
        DECLARE_DELEGATE_RetVal(Type, F##PropName##BindingDelegate); \
        F##PropName##BindingDelegate PropName##Binding; \
        DECLARE_MULTICAST_DELEGATE_OneParam(F##PropName##ChangedDelegate, Type); \
        F##PropName##ChangedDelegate On##PropName##Changed; \
    public: \
        Type Get##PropName() \
        { \
            if (PropName##Binding.IsBound()) \
            { \
                return PropName##Binding.Execute(); \
            } \
            else \
            { \
                return PropName; \
            } \
        } \
        void Set##PropName(Type Value) \
        { \
            checkf(! PropName##Binding.IsBound(), TEXT("Cannot call Set##PropName with PropName##Binding set.")); \
            PropName = Value; \
            if (On##PropName##Changed.IsBound()) { \
                On##PropName##Changed.Broadcast(PropName); \
            } \
        }


UCLASS()
class QUILKIN_API UQuilkinConfigSubsystem : public UEngineSubsystem
{
    GENERATED_BODY()

    UQuilkinConfigSubsystem();
    virtual void Deinitialize() override;

public:
    static UQuilkinConfigSubsystem* Get();
    static bool IsAvailable() {
        return GEngine != nullptr && GEngine->GetEngineSubsystem<UQuilkinConfigSubsystem>() != nullptr;
    }

    /** Whether sockets should add routing tokens to packets */
    UPROPERTY(EditAnywhere, Category = "Quilkin")
    bool PacketHandling;

    DECLARE_PROPERTY_AND_DELEGATE(bool, Enabled);
    DECLARE_PROPERTY_AND_DELEGATE(bool, MeasureEndpoints);
    DECLARE_PROPERTY_AND_DELEGATE(uint64, PingThresholdMillis);
    DECLARE_PROPERTY_AND_DELEGATE(TArray<uint8>, RoutingToken);
    DECLARE_PROPERTY_AND_DELEGATE(float, JitterThreshold);
    DECLARE_PROPERTY_AND_DELEGATE(TArray<FQuilkinEndpoint>, Endpoints);
    DECLARE_PROPERTY_AND_DELEGATE(bool, IPv6Prioritised);
    DECLARE_PROPERTY_AND_DELEGATE(bool, ProxyFailover);
    DECLARE_PROPERTY_AND_DELEGATE(float, Cooldown);
    DECLARE_PROPERTY_AND_DELEGATE(bool, MeasurementImprovement);

    DECLARE_MULTICAST_DELEGATE_OneParam(FMeasurementCompletedDelegate, DatacenterMap);
    FMeasurementCompletedDelegate MeasurementCompleted;
};
