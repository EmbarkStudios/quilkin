#if WITH_DEV_AUTOMATION_TESTS

#include "../QuilkinSocket.h"
#include "../QuilkinLog.h"

#include "CoreMinimal.h"
#include "Misc/AutomationTest.h"

IMPLEMENT_SIMPLE_AUTOMATION_TEST(FNanosToMillis, "Quilkin.UDP.NanosToMillis", EAutomationTestFlags::EditorContext | EAutomationTestFlags::EngineFilter)

bool FNanosToMillis::RunTest(const FString& Parameters)
{
    TestFalse("150ms < 0.5s", FQuilkinSocket::NextProxyExceedsThreshold(MillisToNanos(150), 0.5));
    TestTrue("750ms > 0.5s", FQuilkinSocket::NextProxyExceedsThreshold(MillisToNanos(750), 0.5));
    return true;
}

#endif // WITH_DEV_AUTOMATION_TESTS
