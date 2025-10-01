#include "QuilkinSettings.h"

UQuilkinConfigSubsystem::UQuilkinConfigSubsystem() {
    UE_LOG(LogQuilkin, Display, TEXT("Initialising UQuilkinConfigSubsystem"));
    const UQuilkinDeveloperSettings* DefaultSettings = GetDefault<UQuilkinDeveloperSettings>();
    Enabled = DefaultSettings->IsEnabled();
    RoutingToken = DefaultSettings->RoutingToken;
    MeasureEndpoints = DefaultSettings->MeasureEndpoints;
    PingThresholdMillis = DefaultSettings->PingThresholdMillis;
    JitterThreshold = DefaultSettings->JitterThreshold;
    Endpoints = DefaultSettings->Endpoints;
    IPv6Prioritised = DefaultSettings->IPv6Prioritised;
    ProxyFailover = DefaultSettings->ProxyFailover;
    Cooldown = DefaultSettings->Cooldown;
}

void UQuilkinConfigSubsystem::Deinitialize() {
    UE_LOG(LogQuilkin, Display, TEXT("Tearing down UQuilkinConfigSubsystem"));
}

UQuilkinConfigSubsystem* UQuilkinConfigSubsystem::Get() {
    checkf(GEngine != nullptr, TEXT("UQuilkinConfigSubsystem can only be called inside an Engine context"));
    UQuilkinConfigSubsystem* Subsystem = GEngine->GetEngineSubsystem<UQuilkinConfigSubsystem>();
    checkf(Subsystem != nullptr, TEXT("UQuilkinConfigSubsystem hasn't been initialised"));
    return Subsystem;
}
