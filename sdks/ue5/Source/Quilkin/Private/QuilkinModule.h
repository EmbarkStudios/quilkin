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

#pragma once

#include "CoreMinimal.h"
#include "Modules/ModuleInterface.h"
#include "QuilkinSocketSubsystem.h"
#include "QuilkinEndpoint.h"
#include "QuilkinConcurrentMap.h"

class FQuilkinModule : public IModuleInterface
{
public:
    //~ Begin IModuleInterface interface
    virtual void StartupModule() override;
    virtual void ShutdownModule() override;
    virtual bool SupportsDynamicReloading() override;
    virtual bool SupportsAutomaticShutdown() override;
    //~ End IModuleInterface Interface

private:
    TSharedPtr<FQuilkinSocketSubsystem> QuilkinSocketSubsystem;
};
