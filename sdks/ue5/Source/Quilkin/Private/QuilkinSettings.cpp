#include "QuilkinSettings.h"
#include "QuilkinLog.h"

UQuilkinConfigSubsystem::UQuilkinConfigSubsystem() {
	UE_LOG(LogQuilkin, Display, TEXT("Initialising UQuilkinConfigSubsystem"));
	const UQuilkinDeveloperSettings* DefaultSettings = GetDefault<UQuilkinDeveloperSettings>();
	Enabled = DefaultSettings->IsEnabled();
	RoutingToken = DefaultSettings->RoutingToken;
	MeasureEndpoints = DefaultSettings->MeasureEndpoints;
	IPv6Prioritised = DefaultSettings->IPv6Prioritised;
	ProxyFailover = DefaultSettings->ProxyFailover;
	FailoverCooldownDurationSec = DefaultSettings->FailoverCooldownDurationSec;
	InitialCooldownDurationSec = DefaultSettings->InitialCooldownDurationSec;
	PacketLossThresholdPercent = DefaultSettings->PacketLossThresholdPercent;
	PacketLossThresholdDurationSec = DefaultSettings->PacketLossThresholdDurationSec;
	PacketJitterThresholdSec = DefaultSettings->PacketJitterThresholdSec;
	UseQGMP = DefaultSettings->UseQGMP;
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
