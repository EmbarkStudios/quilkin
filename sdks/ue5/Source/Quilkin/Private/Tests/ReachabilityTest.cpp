#if WITH_DEV_AUTOMATION_TESTS

#include "CoreMinimal.h"
#include "Misc/AutomationTest.h"
#include "Tests/AutomationCommon.h"

#include "../Net/QuilkinSocketSubsystem.h"
#include "QuilkinReachability.h"

IMPLEMENT_SIMPLE_AUTOMATION_TEST(FQuilkinReachabilityTest, "Quilkin.Reachability", EAutomationTestFlags::ApplicationContextMask | EAutomationTestFlags::SmokeFilter)

bool FQuilkinReachabilityTest::RunTest(const FString& Parameters)
{
	TSharedRef<FInternetAddr> ProxyAddress = FQuilkinSocketSubsystem::Get()->CreateInternetAddr();

    // No network device
    {
        QuilkinReachabilityMock Checker(false, false, false, TOptional<int64>());
        auto Status = Checker.ConnectionStatusToProxy(*ProxyAddress);
        TestEqual(TEXT("No network device"), Status.Key, EQuilkinReachability::NoNetworkDevice);
    }

    // Connected to network but no internet
    {
        QuilkinReachabilityMock Checker(true, false, false, TOptional<int64>());
        auto Status = Checker.ConnectionStatusToProxy(*ProxyAddress);
        TestEqual(TEXT("Connected to network but no internet"), Status.Key, EQuilkinReachability::ConnectedToNetworkNoInternet);
    }

    // Connected to internet but cannot reach address
    {
        QuilkinReachabilityMock Checker(true, true, false, TOptional<int64>());
        auto Status = Checker.ConnectionStatusToProxy(*ProxyAddress);
        TestEqual(TEXT("Connected to internet but cannot reach address"), Status.Key, EQuilkinReachability::ConnectedToInternetCannotReachAddress);
    }

    // Can reach address
    {
        QuilkinReachabilityMock Checker(true, true, true, TOptional<int64>());
        auto Status = Checker.ConnectionStatusToProxy(*ProxyAddress);
        TestEqual(
			FString::Printf(
				TEXT("cannot reach address: %s != %s"),
				*EQuilkinReachabilityToString(Status.Key),
				*EQuilkinReachabilityToString(EQuilkinReachability::ConnectedToInternetCannotReachProxy)
			),
			Status.Key,
			EQuilkinReachability::ConnectedToInternetCannotReachProxy
		);
    }

    // Can reach proxy
    {
        QuilkinReachabilityMock Checker(true, true, true, TOptional<int64>(50));
        auto Status = Checker.ConnectionStatusToProxy(*ProxyAddress);
        TestEqual(TEXT("cannot reach proxy"), Status.Key, EQuilkinReachability::CanReachAddress);
    }

    return true;
}
#endif // WITH_DEV_AUTOMATION_TESTS
