This is an unreal engine 5 plugin for Quilkin, a UDP proxy for gameservers. The plugin provides several features that you can use with Quilkin deployments, such as proxying game traffic, and latency measurement.

You can also find guide level documentation on how the proxy works in the [Quilkin Book](https://embarkstudios.github.io/quilkin/main/book/).

### Installation

Copy this plugin to your `Plugins` folder in your `Engine` directory.

### Configuration

Static configuration is available in the editor through `UQuilkinDeveloperSettings` in "Project Settings".

Dynamic configuration is available through `UQuilkinConfigSubsystem`, it is initialised from the settings provided in `UQuilkinDeveloperSettings`, but can also be updated in code, and users can bind individual properties to delegates allowing them to dynamically set based on custom logic.

- `bool Enabled` Whether the plugin will attach a versioned routing token to UDP packets to allow load balancers forward traffic to the correct gameserver. This also requires the address the clients connect to be a Quilkin load balancer, if connected directly to a gameserver the client will be rejected.
- `bool EnabledInPie` By default `Enabled` is disabled in editor to prevent interfering with local clients and gameservers, you can override this behaviour by also enabling `EnabledInPie`.
- `TArray<uint8> RoutingToken` The routing token representing the gameserver a client wants to reach, the token **must** be 16 bytes exactly. Currently the plugin only supports using `Enabled` with a routing token to create the following layout. It is assumed that the routing token would come from an external service, such as a matchmaking system.

```
<packet> | token    | version
 X bytes | 16 bytes | 1 byte
```

- `TArray<FQuilkinEndpoint> Endpoints` A set of Quilkin load balancer endpoints that can be used for the following features.
- `bool MeasureEndpoints` When enabled, the plugin will start a new `Tick` task that executes at a fixed interval (currently 30 seconds), where it will spawn a new background task that will ping each endpoint in `Endpoints`, and track its measurement in a fixed size circular buffer.
   Pings are handled through Quilkin Control Message Protocol, this is a bespoke protocol for UDP to be able to support situations where for example using ICMP is not possible, see the [Quilkin Book](https://embarkstudios.github.io/quilkin/main/book/services/proxy/qcmp.html) for more details on the protocol data unit.
   **Note** `MeasureEndpoints` is orthogonal to `Enabled` and `UseEndpoints` meaning that you can use `MeasureEndpoints` for latency measurements without being required to also use Quilkin for game traffic.
- `bool UseEndpoints` Whether to use `Endpoints` for game traffic. When enabled, instead of using the provided `FInternetAddr`, the plugin will choose the lowest latency endpoint available and send traffic through that endpoint to connect to the gameserver, and if the latency should exceed `JitterThreshold` then the plugin will attempt to redirect traffic to the next available endpoint with the lowest latency.

### Delegates

Quilkin exposes a number of delegates to be able to access certain information, they can be accessed through the `FQuilkinDelegates` class.

- `GetQuilkinEndpointMeasurements` returns `TArray<EndpointPair>` representing each endpoint set in `Endpoints` with their median latency. The array will be empty if no endpoints have been set and `MeasureEndpoints` is not enabled.

- `GetLowestLatencyEndpoint` returns `TOptional<EndpointPair>` is a specialisation of `GetQuilkinEndpointMeasurements` returning the lowest latency endpoint and its median latency. The delegate will return `None` if the array is empty and `MeasureEndpoints` is not enabled.
