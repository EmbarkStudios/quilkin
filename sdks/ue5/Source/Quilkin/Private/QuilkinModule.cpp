/*
 * Copyright 2023 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "QuilkinModule.h"
#include "QuilkinLog.h"

#include "Modules/ModuleManager.h"
#include "SocketSubsystemModule.h"
#include "UObject/NameTypes.h"
#include "Misc/ConfigCacheIni.h"

#define FIVE_MINUTES 5 * 60

IMPLEMENT_MODULE(FQuilkinModule, Quilkin);

void FQuilkinModule::StartupModule()
{
    UE_LOG(LogQuilkin, Log, TEXT("FQuilkinModule::StartupModule()"));

    bool bEnabled = true;
    if (GConfig)
    {
        GConfig->GetBool(TEXT("Quilkin.SocketSubsystem"), TEXT("Enabled"), bEnabled, GEngineIni);
    }
    UE_LOG(LogQuilkin, Log, TEXT("QuilkinSocketSubsystem is %s"), bEnabled ? TEXT("enabled") : TEXT("disabled"));

    if (!bEnabled)
    {
        return;
    }

    FSocketSubsystemModule& SocketSubsystemModule = FModuleManager::LoadModuleChecked<FSocketSubsystemModule>("Sockets");

    ISocketSubsystem* DefaultSocketSubsystem = SocketSubsystemModule.GetSocketSubsystem();
    if (DefaultSocketSubsystem == nullptr)
    {
        UE_LOG(LogQuilkin, Log, TEXT("No default SocketSubsystem was set. Will not use Quilkin SocketSubsystem"));
        return;
    }
    UE_LOG(LogQuilkin, Log, TEXT("Overriding default SocketSubsystem with QuilkinSocketSubsystem"));

    QuilkinSocketSubsystem = MakeShared<FQuilkinSocketSubsystem>(DefaultSocketSubsystem);
    FString Unused;
    QuilkinSocketSubsystem->Init(Unused);
    SocketSubsystemModule.RegisterSocketSubsystem(QUILKIN_SOCKETSUBSYSTEM_NAME, QuilkinSocketSubsystem.Get(), true);
}

void FQuilkinModule::ShutdownModule()
{
    UE_LOG(LogQuilkin, Log, TEXT("FQuilkinModule::ShutdownModule()"));

    if (!QuilkinSocketSubsystem.IsValid())
    {
        return;
    }

    FSocketSubsystemModule& SocketSubsystemModule = FModuleManager::LoadModuleChecked<FSocketSubsystemModule>("Sockets");
    SocketSubsystemModule.UnregisterSocketSubsystem(QUILKIN_SOCKETSUBSYSTEM_NAME);
    QuilkinSocketSubsystem.Reset();
}

bool FQuilkinModule::SupportsDynamicReloading()
{
    return false;
}

bool FQuilkinModule::SupportsAutomaticShutdown()
{
    // Shutdown gets called by the SocketSubsystem, if we were registered (and we don't do anything if we weren't)
    return false;
}