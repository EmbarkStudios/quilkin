#include "QuilkinReachability.h"
#include "QuilkinLog.h"
#include "Sockets.h"

bool QuilkinReachabilityUnreal::HasNetworkAdapter() const
{
    return ValidSubSys() && SubSys->HasNetworkDevice();
}

bool QuilkinReachabilityUnreal::HasInternetConnectivity() const
{
    return FPlatformMisc::GetNetworkConnectionStatus() == ENetworkConnectionStatus::Connected;
}

bool QuilkinReachabilityUnreal::CanReachAddress(const FString& Domain) const
{
    if (!ValidSubSys()) return false;

    TSharedPtr<FInternetAddr> TargetAddr = SubSys->CreateInternetAddr();
    bool bIsValid = false;
    TargetAddr->SetIp(*Domain, bIsValid);

    if (!bIsValid)
    {
        FAddressInfoResult ResolvedAddress = SubSys->GetAddressInfo(
            *Domain,
            nullptr,
            EAddressInfoFlags::Default,
            NAME_None
        );

        if (ResolvedAddress.ReturnCode != ESocketErrors::SE_NO_ERROR || ResolvedAddress.Results.Num() == 0)
        {
            return false;
        }

        TargetAddr = ResolvedAddress.Results[0].Address->Clone();
    }

    TargetAddr->SetPort(80);

    return ReachableViaTcp(*TargetAddr);
}

TOptional<int64> QuilkinReachabilityUnreal::CanReachProxy(const FInternetAddr& Addr) const
{
    if (!ValidSubSys()) return false;

    return ReachableViaQcmp(Addr);
}

bool QuilkinReachabilityUnreal::ReachableViaTcp(const FInternetAddr& Domain) const
{
    if (!ValidSubSys()) return false;

    FUniqueSocket TestSocket = SubSys->CreateUniqueSocket(NAME_Stream, TEXT("ReachabilityTest"), FNetworkProtocolTypes::IPv4);
    if (!TestSocket)
    {
	    return false;
    }

    TestSocket->SetNonBlocking(true);
    bool bConnected = TestSocket->Connect(Domain);
    double TimeoutTime = FPlatformTime::Seconds() + 2.0;

    while (!bConnected && FPlatformTime::Seconds() < TimeoutTime)
    {
		FPlatformProcess::Sleep(0.1);
        bConnected = TestSocket->Wait(ESocketWaitConditions::WaitForWrite, FTimespan::FromSeconds(1));
    }

    TestSocket->Close();

    return bConnected;
}

TOptional<int64> QuilkinReachabilityUnreal::ReachableViaQcmp(const FInternetAddr& Addr) const
{
    if (!ValidSubSys())
    {
	    return false;
    }
	
	auto Socket = SubSys->SetupNonblockingUDPSocket("QuilkinReachabilityPing");
	if (!Socket) 
	{
		UE_LOG(LogQuilkin, Error, TEXT("Couldn't allocate reachable ping socket"));
		return NullOpt;
	}

	UE_LOGFMT(LogQuilkin, Verbose, "Pinging {0}", Addr.ToString(true));

	FQuilkinEndpoint Endpoint;
	Endpoint.Host = Addr.ToString(false);

	// TODO @mwiniarski: Make this non-blocking!!! 
	auto PingResult = SubSys->Ping(Socket, {Endpoint});
	if (!PingResult.Num())
	{
		UE_LOGFMT(LogQuilkin, Error, "Qcmp ping to {0} failed", Addr.ToString(true));
		return NullOpt;
	}
	
	return TOptional(PingResult[0].Latencies.Median());
}