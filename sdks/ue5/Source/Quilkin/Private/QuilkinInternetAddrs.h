#pragma once

#include "CoreMinimal.h"
#include "QuilkinCircularBuffer.h"
#include "../Public/QuilkinEndpoint.h"
#include "IPAddress.h"

class FQuilkinSocketSubsystem;

class QUILKIN_API FQuilkinInternetAddrs : public FInternetAddr {
public:
    FQuilkinInternetAddrs(TWeakPtr<FQuilkinSocketSubsystem> InSubSystem, TSharedRef<FInternetAddr> InBaseAddr) : Subsystem(InSubSystem), BaseAddr(InBaseAddr) {};
    ~FQuilkinInternetAddrs() {};

    //~ Start FInternetAddr overrides
    virtual bool CompareEndpoints(const FInternetAddr& InAddr) const override;
    virtual void SetIp(uint32 InAddr) override;
    virtual void SetIp(const TCHAR* InAddr, bool& bIsValid) override;
    virtual void GetIp(uint32& OutAddr) const override;
    virtual void SetPort(int32 InPort) override;
    virtual void GetPort(int32& OutPort) const override;
    virtual int32 GetPort() const override;
    virtual void SetPlatformPort(int32 InPort) override;
    virtual int32 GetPlatformPort() const override;
    virtual void SetRawIp(const TArray<uint8>& RawAddr) override;
    virtual TArray<uint8> GetRawIp() const override;
    virtual void SetAnyAddress() override;
    virtual void SetBroadcastAddress() override;
    virtual void SetLoopbackAddress() override;
    virtual FString ToString(bool bAppendPort) const override;
    virtual bool operator==(const FInternetAddr& Other) const override;
    virtual uint32 GetTypeHash() const override;
    virtual bool IsValid() const override;
    virtual TSharedRef<FInternetAddr> Clone() const override;
    virtual FName GetProtocolType() const override;
    virtual void DumpAddrData() const override;
    //~ End FInternetAddr overrides

private:
    TWeakPtr<FQuilkinSocketSubsystem> Subsystem;
public:
    TSharedRef<FInternetAddr> BaseAddr;
};
