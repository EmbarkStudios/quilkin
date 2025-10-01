#include "QuilkinInternetAddrs.h"
#include "QuilkinLog.h"
#include "QuilkinSettings.h"
#include "QuilkinSocketSubsystem.h"

bool FQuilkinInternetAddrs::CompareEndpoints(const FInternetAddr& InAddr) const
{
    return *this == InAddr;
}

void FQuilkinInternetAddrs::SetIp(uint32 InAddr)
{
    return BaseAddr->SetIp(InAddr);
}

void FQuilkinInternetAddrs::SetIp(const TCHAR* InAddr, bool& bIsValid)
{
    return BaseAddr->SetIp(InAddr, bIsValid);
}

void FQuilkinInternetAddrs::GetIp(uint32& OutAddr) const
{
    return BaseAddr->GetIp(OutAddr);
}

void FQuilkinInternetAddrs::SetPort(int32 InPort)
{
    return BaseAddr->SetPort(InPort);
}

void FQuilkinInternetAddrs::GetPort(int32& OutPort) const
{
    return BaseAddr->GetPort(OutPort);
}

int32 FQuilkinInternetAddrs::GetPort() const
{
    return BaseAddr->GetPort();
}

void FQuilkinInternetAddrs::SetPlatformPort(int32 InPort)
{
    return BaseAddr->SetPlatformPort(InPort);
}

int32 FQuilkinInternetAddrs::GetPlatformPort() const
{
    return BaseAddr->GetPlatformPort();
}

void FQuilkinInternetAddrs::SetRawIp(const TArray<uint8>& RawAddr)
{
    return BaseAddr->SetRawIp(RawAddr);
}

TArray<uint8> FQuilkinInternetAddrs::GetRawIp() const
{
    return BaseAddr->GetRawIp();
}

void FQuilkinInternetAddrs::SetAnyAddress()
{
    return BaseAddr->SetAnyAddress();
}

void FQuilkinInternetAddrs::SetBroadcastAddress()
{
    return BaseAddr->SetBroadcastAddress();
}

void FQuilkinInternetAddrs::SetLoopbackAddress()
{
    return BaseAddr->SetLoopbackAddress();
}

FString FQuilkinInternetAddrs::ToString(bool bAppendPort) const
{
    return BaseAddr->ToString(bAppendPort);
}

bool FQuilkinInternetAddrs::operator==(const FInternetAddr& Other) const
{
    return *BaseAddr == Other || UQuilkinConfigSubsystem::Get()->GetEnabled();
}

uint32 FQuilkinInternetAddrs::GetTypeHash() const
{
    return BaseAddr->GetTypeHash();
}

bool FQuilkinInternetAddrs::IsValid() const
{
    return BaseAddr->IsValid();
}

TSharedRef<FInternetAddr> FQuilkinInternetAddrs::Clone() const
{
    return MakeShareable(new FQuilkinInternetAddrs(Subsystem.Pin()->AsWeak(), BaseAddr->Clone()));
}

FName FQuilkinInternetAddrs::GetProtocolType() const
{
    return BaseAddr->GetProtocolType();
}

void FQuilkinInternetAddrs::DumpAddrData() const
{
    return BaseAddr->DumpAddrData();
}
