#pragma once

#include "CoreMinimal.h"
#include "Containers/UnrealString.h"
#include "Logging/StructuredLog.h"

#include "../Private/Net/QuilkinControlMessageProtocol.h"
#include "../Private/Net/QuilkinSocketSubsystem.h"

UENUM(BlueprintType)
enum class EQuilkinReachability : uint8
{
	NoNetworkDevice,
	ConnectedToNetworkNoInternet,
	ConnectedToInternetCannotReachAddress,
	ConnectedToInternetCannotReachProxy,
	CanReachAddress
};

inline FString EQuilkinReachabilityToString(EQuilkinReachability Status, const FString& TestAddress = TEXT("1.1.1.1")) {
	switch (Status)
	{
	case EQuilkinReachability::NoNetworkDevice:
		return TEXT("no network device found");
	case EQuilkinReachability::ConnectedToNetworkNoInternet:
		return TEXT("no access to public internet");
	case EQuilkinReachability::ConnectedToInternetCannotReachAddress:
		return FString::Format(TEXT("cannot reach <{0}>"), { TestAddress, });
	case EQuilkinReachability::ConnectedToInternetCannotReachProxy:
		return TEXT("cannot reach proxy");
	case EQuilkinReachability::CanReachAddress:
		return TEXT("able to reach proxy");
	}

	return FString();
}

class QuilkinReachabilityUnreal;
class QuilkinReachabilityMock;

class QUILKIN_API QuilkinReachability
{
public:
	using Unreal = QuilkinReachabilityUnreal;
	using Mock = QuilkinReachabilityMock;
	using ReachPair = TPair<EQuilkinReachability, int64>;

	template<typename T, typename F> TPair<T, int64> Measure(F Closure) {
		auto Before = GetUnixTimestampInNanos();
		auto Result = Closure();
		auto After = GetUnixTimestampInNanos();
		return TPair<T, int64>(Result, After - Before);
	}

    ReachPair ConnectionStatus(const FString& TestAddress = TEXT("1.1.1.1"))
    {
		auto Before = GetUnixTimestampInNanos();
		TPair<bool, int64> AdapterResult = Measure<bool>([&]() { return HasNetworkAdapter(); });
        if (!AdapterResult.Key)
        {
            return ReachPair(EQuilkinReachability::NoNetworkDevice, AdapterResult.Value);
        }

		TPair<bool, int64> InternetResult = Measure<bool>([&]() { return HasInternetConnectivity(); });
        if (!InternetResult.Key)
        {
            return ReachPair(EQuilkinReachability::ConnectedToNetworkNoInternet, InternetResult.Value);
        }

		TPair<bool, int64> AddressResult = Measure<bool>([&]() { return CanReachAddress(TestAddress); });
        if (!AddressResult.Key)
        {
            return ReachPair(EQuilkinReachability::ConnectedToInternetCannotReachAddress, AddressResult.Value);
        }

		auto After = GetUnixTimestampInNanos();
        return ReachPair(EQuilkinReachability::CanReachAddress, After - Before);
    }

    ReachPair ConnectionStatusToProxy(const FInternetAddr& ProxyAddress, const FString& TestAddress = TEXT("1.1.1.1"))
    {
		auto Result = ConnectionStatus(TestAddress);

        if (Result.Key != EQuilkinReachability::CanReachAddress)
        {
			return Result;
        }

		TPair<TOptional<int64>, int64> QuilkinResult = Measure<TOptional<int64>>([&]() { return CanReachProxy(ProxyAddress); });

        if (QuilkinResult.Key.IsSet())
        {
			return ReachPair(EQuilkinReachability::CanReachAddress, QuilkinResult.Key.GetValue());
        } else
        {
            return ReachPair(EQuilkinReachability::ConnectedToInternetCannotReachProxy, QuilkinResult.Value);
        }

    }

protected:
	virtual ~QuilkinReachability() {}
    virtual bool HasNetworkAdapter() const = 0;
    virtual bool HasInternetConnectivity() const = 0;
    virtual bool CanReachAddress(const FString& Address) const = 0;
    virtual TOptional<int64> CanReachProxy(const FInternetAddr& Address) const = 0;
};


class QuilkinReachabilityMock : public QuilkinReachability
{
public:
    QuilkinReachabilityMock(bool bHasNetworkAdapter, bool bHasInternet, bool bCanReachAddress, TOptional<int64> ProxyPingResult)
        : bHasNetworkAdapter(bHasNetworkAdapter)
        , bHasInternet(bHasInternet)
        , bCanReachAddress(bCanReachAddress)
        , ProxyPingResult(ProxyPingResult)
    {}

    bool bHasNetworkAdapter;
    bool bHasInternet;
    bool bCanReachAddress;
    TOptional<int64> ProxyPingResult;
	TArray<TSharedPtr<FInternetAddr>> BlockedEndpoints;
protected:
    bool HasNetworkAdapter() const override { return bHasNetworkAdapter; }
    bool HasInternetConnectivity() const override { return bHasInternet; }
    bool CanReachAddress(const FString& Address) const override { return bCanReachAddress; }
    TOptional<int64> CanReachProxy(const FInternetAddr& Proxy) const override {
		for (const auto& Blocked : BlockedEndpoints) {
			if (*Blocked == Proxy) {
				return TOptional<int64>();
			}
		}

		return ProxyPingResult;
	}
};


class QUILKIN_API QuilkinReachabilityUnreal : public QuilkinReachability
{
public:
	QuilkinReachabilityUnreal(FQuilkinSocketSubsystem* InSubSys): SubSys(InSubSys) {}

protected:
	FQuilkinSocketSubsystem* SubSys;

	bool ValidSubSys() const { return SubSys != nullptr; }
	virtual bool HasNetworkAdapter() const override;
	virtual bool HasInternetConnectivity() const override;
	virtual bool CanReachAddress(const FString& Domain) const override;
	virtual TOptional<int64> CanReachProxy(const FInternetAddr& Addr) const override;
    
	bool ReachableViaTcp(const FInternetAddr& Domain) const;
	TOptional<int64> ReachableViaQcmp(const FInternetAddr& Addr) const;
};
