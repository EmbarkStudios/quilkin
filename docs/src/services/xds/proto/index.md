# Protocol Documentation
<a name="top"></a>

## Table of Contents

- [quilkin/relay/v1alpha1/relay.proto](#quilkin_relay_v1alpha1_relay-proto)
    - [AggregatedControlPlaneDiscoveryService](#quilkin-relay-v1alpha1-AggregatedControlPlaneDiscoveryService)
  
- [quilkin/config/v1alpha1/config.proto](#quilkin_config_v1alpha1_config-proto)
    - [Cluster](#quilkin-config-v1alpha1-Cluster)
    - [ClusterMap](#quilkin-config-v1alpha1-ClusterMap)
    - [Datacenter](#quilkin-config-v1alpha1-Datacenter)
    - [Endpoint](#quilkin-config-v1alpha1-Endpoint)
    - [Filter](#quilkin-config-v1alpha1-Filter)
    - [FilterChain](#quilkin-config-v1alpha1-FilterChain)
    - [Host](#quilkin-config-v1alpha1-Host)
    - [Ipv6](#quilkin-config-v1alpha1-Ipv6)
    - [Locality](#quilkin-config-v1alpha1-Locality)
  
- [quilkin/filters/capture/v1alpha1/capture.proto](#quilkin_filters_capture_v1alpha1_capture-proto)
    - [Capture](#quilkin-filters-capture-v1alpha1-Capture)
    - [Capture.Prefix](#quilkin-filters-capture-v1alpha1-Capture-Prefix)
    - [Capture.Regex](#quilkin-filters-capture-v1alpha1-Capture-Regex)
    - [Capture.Suffix](#quilkin-filters-capture-v1alpha1-Capture-Suffix)
  
- [quilkin/filters/concatenate/v1alpha1/concatenate.proto](#quilkin_filters_concatenate_v1alpha1_concatenate-proto)
    - [Concatenate](#quilkin-filters-concatenate-v1alpha1-Concatenate)
    - [Concatenate.StrategyValue](#quilkin-filters-concatenate-v1alpha1-Concatenate-StrategyValue)
  
    - [Concatenate.Strategy](#quilkin-filters-concatenate-v1alpha1-Concatenate-Strategy)
  
- [quilkin/filters/debug/v1alpha1/debug.proto](#quilkin_filters_debug_v1alpha1_debug-proto)
    - [Debug](#quilkin-filters-debug-v1alpha1-Debug)
  
- [quilkin/filters/drop/v1alpha1/drop.proto](#quilkin_filters_drop_v1alpha1_drop-proto)
    - [Drop](#quilkin-filters-drop-v1alpha1-Drop)
  
- [quilkin/filters/firewall/v1alpha1/firewall.proto](#quilkin_filters_firewall_v1alpha1_firewall-proto)
    - [Firewall](#quilkin-filters-firewall-v1alpha1-Firewall)
    - [Firewall.PortRange](#quilkin-filters-firewall-v1alpha1-Firewall-PortRange)
    - [Firewall.Rule](#quilkin-filters-firewall-v1alpha1-Firewall-Rule)
  
    - [Firewall.Action](#quilkin-filters-firewall-v1alpha1-Firewall-Action)
  
- [quilkin/filters/load_balancer/v1alpha1/load_balancer.proto](#quilkin_filters_load_balancer_v1alpha1_load_balancer-proto)
    - [LoadBalancer](#quilkin-filters-load_balancer-v1alpha1-LoadBalancer)
    - [LoadBalancer.PolicyValue](#quilkin-filters-load_balancer-v1alpha1-LoadBalancer-PolicyValue)
  
    - [LoadBalancer.Policy](#quilkin-filters-load_balancer-v1alpha1-LoadBalancer-Policy)
  
- [quilkin/filters/local_rate_limit/v1alpha1/local_rate_limit.proto](#quilkin_filters_local_rate_limit_v1alpha1_local_rate_limit-proto)
    - [LocalRateLimit](#quilkin-filters-local_rate_limit-v1alpha1-LocalRateLimit)
  
- [quilkin/filters/match/v1alpha1/match.proto](#quilkin_filters_match_v1alpha1_match-proto)
    - [Match](#quilkin-filters-matches-v1alpha1-Match)
    - [Match.Branch](#quilkin-filters-matches-v1alpha1-Match-Branch)
    - [Match.Config](#quilkin-filters-matches-v1alpha1-Match-Config)
  
- [quilkin/filters/pass/v1alpha1/pass.proto](#quilkin_filters_pass_v1alpha1_pass-proto)
    - [Pass](#quilkin-filters-pass-v1alpha1-Pass)
  
- [quilkin/filters/token_router/v1alpha1/token_router.proto](#quilkin_filters_token_router_v1alpha1_token_router-proto)
    - [TokenRouter](#quilkin-filters-token_router-v1alpha1-TokenRouter)
  
- [quilkin/filters/timestamp/v1alpha1/timestamp.proto](#quilkin_filters_timestamp_v1alpha1_timestamp-proto)
    - [Timestamp](#quilkin-filters-timestamp-v1alpha1-Timestamp)
  
- [quilkin/pprof.proto](#quilkin_pprof-proto)
    - [Function](#perftools-profiles-Function)
    - [Label](#perftools-profiles-Label)
    - [Line](#perftools-profiles-Line)
    - [Location](#perftools-profiles-Location)
    - [Mapping](#perftools-profiles-Mapping)
    - [Profile](#perftools-profiles-Profile)
    - [Sample](#perftools-profiles-Sample)
    - [ValueType](#perftools-profiles-ValueType)
  
- [Scalar Value Types](#scalar-value-types)



<a name="quilkin_relay_v1alpha1_relay-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## quilkin/relay/v1alpha1/relay.proto


 

 

 


<a name="quilkin-relay-v1alpha1-AggregatedControlPlaneDiscoveryService"></a>

### AggregatedControlPlaneDiscoveryService
The Manager Discovery Service provides an RPC for a management
service to upstream its configuration to a relay service.
This RPC works essentially the same as xDS, except instead of the
client connecting to the server to receive configuration, the
client is connecting to the server send its configuration.

This service enables the relay to merge the configuration of all
currently live management servers as a single aggregated
xDS server without the relay needing to maintain a list
of xDS servers to connect to in the relay itself.

| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| StreamAggregatedResources | [.envoy.service.discovery.v3.DiscoveryResponse](#envoy-service-discovery-v3-DiscoveryResponse) stream | [.envoy.service.discovery.v3.DiscoveryRequest](#envoy-service-discovery-v3-DiscoveryRequest) stream | This RPC is not supported but remains here as part of the xDS standard, as Quilkin only uses a delta configuration transport for performance reasons. |
| DeltaAggregatedResources | [.envoy.service.discovery.v3.DeltaDiscoveryResponse](#envoy-service-discovery-v3-DeltaDiscoveryResponse) stream | [.envoy.service.discovery.v3.DeltaDiscoveryRequest](#envoy-service-discovery-v3-DeltaDiscoveryRequest) stream | Delta (Incremental) xDS implementation, used by agents and/or management servers to update the relay with their configuration |
| SubscribeDeltaResources | [.envoy.service.discovery.v3.DeltaDiscoveryRequest](#envoy-service-discovery-v3-DeltaDiscoveryRequest) stream | [.envoy.service.discovery.v3.DeltaDiscoveryResponse](#envoy-service-discovery-v3-DeltaDiscoveryResponse) stream | Used by proxies to subscribe to changes from the relay |

 



<a name="quilkin_config_v1alpha1_config-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## quilkin/config/v1alpha1/config.proto



<a name="quilkin-config-v1alpha1-Cluster"></a>

### Cluster



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| locality | [Locality](#quilkin-config-v1alpha1-Locality) |  |  |
| endpoints | [Endpoint](#quilkin-config-v1alpha1-Endpoint) | repeated |  |






<a name="quilkin-config-v1alpha1-ClusterMap"></a>

### ClusterMap



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| clusters | [Cluster](#quilkin-config-v1alpha1-Cluster) | repeated |  |






<a name="quilkin-config-v1alpha1-Datacenter"></a>

### Datacenter



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| host | [string](#string) |  |  |
| qcmp_port | [uint32](#uint32) |  |  |
| icao_code | [string](#string) |  |  |






<a name="quilkin-config-v1alpha1-Endpoint"></a>

### Endpoint



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| host | [string](#string) |  |  |
| port | [uint32](#uint32) |  |  |
| metadata | [google.protobuf.Struct](#google-protobuf-Struct) |  |  |
| host2 | [Host](#quilkin-config-v1alpha1-Host) |  |  |






<a name="quilkin-config-v1alpha1-Filter"></a>

### Filter



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| name | [string](#string) |  |  |
| label | [string](#string) | optional |  |
| config | [string](#string) | optional |  |






<a name="quilkin-config-v1alpha1-FilterChain"></a>

### FilterChain



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| filters | [Filter](#quilkin-config-v1alpha1-Filter) | repeated |  |






<a name="quilkin-config-v1alpha1-Host"></a>

### Host



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| name | [string](#string) |  |  |
| ipv4 | [fixed32](#fixed32) |  |  |
| ipv6 | [Ipv6](#quilkin-config-v1alpha1-Ipv6) |  |  |






<a name="quilkin-config-v1alpha1-Ipv6"></a>

### Ipv6



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| first | [fixed64](#fixed64) |  |  |
| second | [fixed64](#fixed64) |  |  |






<a name="quilkin-config-v1alpha1-Locality"></a>

### Locality



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| region | [string](#string) |  |  |
| zone | [string](#string) |  |  |
| sub_zone | [string](#string) |  |  |





 

 

 

 



<a name="quilkin_filters_capture_v1alpha1_capture-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## quilkin/filters/capture/v1alpha1/capture.proto



<a name="quilkin-filters-capture-v1alpha1-Capture"></a>

### Capture



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| metadata_key | [google.protobuf.StringValue](#google-protobuf-StringValue) |  |  |
| prefix | [Capture.Prefix](#quilkin-filters-capture-v1alpha1-Capture-Prefix) |  |  |
| suffix | [Capture.Suffix](#quilkin-filters-capture-v1alpha1-Capture-Suffix) |  |  |
| regex | [Capture.Regex](#quilkin-filters-capture-v1alpha1-Capture-Regex) |  |  |






<a name="quilkin-filters-capture-v1alpha1-Capture-Prefix"></a>

### Capture.Prefix



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| size | [uint32](#uint32) |  |  |
| remove | [google.protobuf.BoolValue](#google-protobuf-BoolValue) |  |  |






<a name="quilkin-filters-capture-v1alpha1-Capture-Regex"></a>

### Capture.Regex



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| regex | [google.protobuf.StringValue](#google-protobuf-StringValue) |  |  |






<a name="quilkin-filters-capture-v1alpha1-Capture-Suffix"></a>

### Capture.Suffix



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| size | [uint32](#uint32) |  |  |
| remove | [google.protobuf.BoolValue](#google-protobuf-BoolValue) |  |  |





 

 

 

 



<a name="quilkin_filters_concatenate_v1alpha1_concatenate-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## quilkin/filters/concatenate/v1alpha1/concatenate.proto



<a name="quilkin-filters-concatenate-v1alpha1-Concatenate"></a>

### Concatenate



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| on_write | [Concatenate.StrategyValue](#quilkin-filters-concatenate-v1alpha1-Concatenate-StrategyValue) |  |  |
| on_read | [Concatenate.StrategyValue](#quilkin-filters-concatenate-v1alpha1-Concatenate-StrategyValue) |  |  |
| bytes | [bytes](#bytes) |  |  |






<a name="quilkin-filters-concatenate-v1alpha1-Concatenate-StrategyValue"></a>

### Concatenate.StrategyValue



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| value | [Concatenate.Strategy](#quilkin-filters-concatenate-v1alpha1-Concatenate-Strategy) |  |  |





 


<a name="quilkin-filters-concatenate-v1alpha1-Concatenate-Strategy"></a>

### Concatenate.Strategy


| Name | Number | Description |
| ---- | ------ | ----------- |
| DoNothing | 0 |  |
| Append | 1 |  |
| Prepend | 2 |  |


 

 

 



<a name="quilkin_filters_debug_v1alpha1_debug-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## quilkin/filters/debug/v1alpha1/debug.proto



<a name="quilkin-filters-debug-v1alpha1-Debug"></a>

### Debug



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [google.protobuf.StringValue](#google-protobuf-StringValue) |  |  |





 

 

 

 



<a name="quilkin_filters_drop_v1alpha1_drop-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## quilkin/filters/drop/v1alpha1/drop.proto



<a name="quilkin-filters-drop-v1alpha1-Drop"></a>

### Drop






 

 

 

 



<a name="quilkin_filters_firewall_v1alpha1_firewall-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## quilkin/filters/firewall/v1alpha1/firewall.proto



<a name="quilkin-filters-firewall-v1alpha1-Firewall"></a>

### Firewall



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| on_read | [Firewall.Rule](#quilkin-filters-firewall-v1alpha1-Firewall-Rule) | repeated |  |
| on_write | [Firewall.Rule](#quilkin-filters-firewall-v1alpha1-Firewall-Rule) | repeated |  |






<a name="quilkin-filters-firewall-v1alpha1-Firewall-PortRange"></a>

### Firewall.PortRange



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| min | [uint32](#uint32) |  |  |
| max | [uint32](#uint32) |  |  |






<a name="quilkin-filters-firewall-v1alpha1-Firewall-Rule"></a>

### Firewall.Rule



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| action | [Firewall.Action](#quilkin-filters-firewall-v1alpha1-Firewall-Action) |  |  |
| sources | [string](#string) | repeated |  |
| ports | [Firewall.PortRange](#quilkin-filters-firewall-v1alpha1-Firewall-PortRange) | repeated |  |





 


<a name="quilkin-filters-firewall-v1alpha1-Firewall-Action"></a>

### Firewall.Action


| Name | Number | Description |
| ---- | ------ | ----------- |
| Allow | 0 |  |
| Deny | 1 |  |


 

 

 



<a name="quilkin_filters_load_balancer_v1alpha1_load_balancer-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## quilkin/filters/load_balancer/v1alpha1/load_balancer.proto



<a name="quilkin-filters-load_balancer-v1alpha1-LoadBalancer"></a>

### LoadBalancer



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| policy | [LoadBalancer.PolicyValue](#quilkin-filters-load_balancer-v1alpha1-LoadBalancer-PolicyValue) |  |  |






<a name="quilkin-filters-load_balancer-v1alpha1-LoadBalancer-PolicyValue"></a>

### LoadBalancer.PolicyValue



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| value | [LoadBalancer.Policy](#quilkin-filters-load_balancer-v1alpha1-LoadBalancer-Policy) |  |  |





 


<a name="quilkin-filters-load_balancer-v1alpha1-LoadBalancer-Policy"></a>

### LoadBalancer.Policy


| Name | Number | Description |
| ---- | ------ | ----------- |
| RoundRobin | 0 |  |
| Random | 1 |  |
| Hash | 2 |  |


 

 

 



<a name="quilkin_filters_local_rate_limit_v1alpha1_local_rate_limit-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## quilkin/filters/local_rate_limit/v1alpha1/local_rate_limit.proto



<a name="quilkin-filters-local_rate_limit-v1alpha1-LocalRateLimit"></a>

### LocalRateLimit



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| max_packets | [uint64](#uint64) |  |  |
| period | [google.protobuf.UInt32Value](#google-protobuf-UInt32Value) |  |  |





 

 

 

 



<a name="quilkin_filters_match_v1alpha1_match-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## quilkin/filters/match/v1alpha1/match.proto



<a name="quilkin-filters-matches-v1alpha1-Match"></a>

### Match



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| on_read | [Match.Config](#quilkin-filters-matches-v1alpha1-Match-Config) | optional |  |
| on_write | [Match.Config](#quilkin-filters-matches-v1alpha1-Match-Config) | optional |  |






<a name="quilkin-filters-matches-v1alpha1-Match-Branch"></a>

### Match.Branch



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| value | [google.protobuf.Value](#google-protobuf-Value) |  |  |
| filter | [envoy.config.listener.v3.Filter](#envoy-config-listener-v3-Filter) |  |  |






<a name="quilkin-filters-matches-v1alpha1-Match-Config"></a>

### Match.Config



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| metadata_key | [google.protobuf.StringValue](#google-protobuf-StringValue) |  |  |
| branches | [Match.Branch](#quilkin-filters-matches-v1alpha1-Match-Branch) | repeated |  |
| fallthrough | [envoy.config.listener.v3.Filter](#envoy-config-listener-v3-Filter) |  |  |





 

 

 

 



<a name="quilkin_filters_pass_v1alpha1_pass-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## quilkin/filters/pass/v1alpha1/pass.proto



<a name="quilkin-filters-pass-v1alpha1-Pass"></a>

### Pass






 

 

 

 



<a name="quilkin_filters_token_router_v1alpha1_token_router-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## quilkin/filters/token_router/v1alpha1/token_router.proto



<a name="quilkin-filters-token_router-v1alpha1-TokenRouter"></a>

### TokenRouter



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| metadata_key | [google.protobuf.StringValue](#google-protobuf-StringValue) |  |  |





 

 

 

 



<a name="quilkin_filters_timestamp_v1alpha1_timestamp-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## quilkin/filters/timestamp/v1alpha1/timestamp.proto



<a name="quilkin-filters-timestamp-v1alpha1-Timestamp"></a>

### Timestamp



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| metadata_key | [google.protobuf.StringValue](#google-protobuf-StringValue) |  |  |





 

 

 

 



<a name="quilkin_pprof-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## quilkin/pprof.proto



<a name="perftools-profiles-Function"></a>

### Function



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [uint64](#uint64) |  | Unique nonzero id for the function. |
| name | [int64](#int64) |  | Name of the function, in human-readable form if available.

Index into string table |
| system_name | [int64](#int64) |  | Name of the function, as identified by the system. For instance, it can be a C&#43;&#43; mangled name.

Index into string table |
| filename | [int64](#int64) |  | Source file containing the function.

Index into string table |
| start_line | [int64](#int64) |  | Line number in source file. |






<a name="perftools-profiles-Label"></a>

### Label



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [int64](#int64) |  | Index into string table |
| str | [int64](#int64) |  | At most one of the following must be present

Index into string table |
| num | [int64](#int64) |  |  |
| num_unit | [int64](#int64) |  | Should only be present when num is present. Specifies the units of num. Use arbitrary string (for example, &#34;requests&#34;) as a custom count unit. If no unit is specified, consumer may apply heuristic to deduce the unit. Consumers may also interpret units like &#34;bytes&#34; and &#34;kilobytes&#34; as memory units and units like &#34;seconds&#34; and &#34;nanoseconds&#34; as time units, and apply appropriate unit conversions to these.

Index into string table |






<a name="perftools-profiles-Line"></a>

### Line



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| function_id | [uint64](#uint64) |  | The id of the corresponding profile.Function for this line. |
| line | [int64](#int64) |  | Line number in source code. |






<a name="perftools-profiles-Location"></a>

### Location
Describes function and line table debug information.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [uint64](#uint64) |  | Unique nonzero id for the location. A profile could use instruction addresses or any integer sequence as ids. |
| mapping_id | [uint64](#uint64) |  | The id of the corresponding profile.Mapping for this location. It can be unset if the mapping is unknown or not applicable for this profile type. |
| address | [uint64](#uint64) |  | The instruction address for this location, if available. It should be within [Mapping.memory_start...Mapping.memory_limit] for the corresponding mapping. A non-leaf address may be in the middle of a call instruction. It is up to display tools to find the beginning of the instruction if necessary. |
| line | [Line](#perftools-profiles-Line) | repeated | Multiple line indicates this location has inlined functions, where the last entry represents the caller into which the preceding entries were inlined.

E.g., if memcpy() is inlined into printf: line[0].function_name == &#34;memcpy&#34; line[1].function_name == &#34;printf&#34; |
| is_folded | [bool](#bool) |  | Provides an indication that multiple symbols map to this location&#39;s address, for example due to identical code folding by the linker. In that case the line information above represents one of the multiple symbols. This field must be recomputed when the symbolization state of the profile changes. |






<a name="perftools-profiles-Mapping"></a>

### Mapping



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [uint64](#uint64) |  | Unique nonzero id for the mapping. |
| memory_start | [uint64](#uint64) |  | Address at which the binary (or DLL) is loaded into memory. |
| memory_limit | [uint64](#uint64) |  | The limit of the address range occupied by this mapping. |
| file_offset | [uint64](#uint64) |  | Offset in the binary that corresponds to the first mapped address. |
| filename | [int64](#int64) |  | The object this entry is loaded from. This can be a filename on disk for the main binary and shared libraries, or virtual abstractions like &#34;[vdso]&#34;.

Index into string table |
| build_id | [int64](#int64) |  | A string that uniquely identifies a particular program version with high probability. E.g., for binaries generated by GNU tools, it could be the contents of the .note.gnu.build-id field.

Index into string table |
| has_functions | [bool](#bool) |  | The following fields indicate the resolution of symbolic info. |
| has_filenames | [bool](#bool) |  |  |
| has_line_numbers | [bool](#bool) |  |  |
| has_inline_frames | [bool](#bool) |  |  |






<a name="perftools-profiles-Profile"></a>

### Profile



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| sample_type | [ValueType](#perftools-profiles-ValueType) | repeated | A description of the samples associated with each Sample.value. For a cpu profile this might be: [[&#34;cpu&#34;,&#34;nanoseconds&#34;]] or [[&#34;wall&#34;,&#34;seconds&#34;]] or [[&#34;syscall&#34;,&#34;count&#34;]] For a heap profile, this might be: [[&#34;allocations&#34;,&#34;count&#34;], [&#34;space&#34;,&#34;bytes&#34;]], If one of the values represents the number of events represented by the sample, by convention it should be at index 0 and use sample_type.unit == &#34;count&#34;. |
| sample | [Sample](#perftools-profiles-Sample) | repeated | The set of samples recorded in this profile. |
| mapping | [Mapping](#perftools-profiles-Mapping) | repeated | Mapping from address ranges to the image/binary/library mapped into that address range. mapping[0] will be the main binary. |
| location | [Location](#perftools-profiles-Location) | repeated | Useful program location |
| function | [Function](#perftools-profiles-Function) | repeated | Functions referenced by locations |
| string_table | [string](#string) | repeated | A common table for strings referenced by various messages. string_table[0] must always be &#34;&#34;. |
| drop_frames | [int64](#int64) |  | frames with Function.function_name fully matching the following regexp will be dropped from the samples, along with their successors.

Index into string table. |
| keep_frames | [int64](#int64) |  | frames with Function.function_name fully matching the following regexp will be kept, even if it matches drop_functions.

Index into string table. |
| time_nanos | [int64](#int64) |  | Time of collection (UTC) represented as nanoseconds past the epoch. |
| duration_nanos | [int64](#int64) |  | Duration of the profile, if a duration makes sense. |
| period_type | [ValueType](#perftools-profiles-ValueType) |  | The kind of events between sampled ocurrences. e.g [ &#34;cpu&#34;,&#34;cycles&#34; ] or [ &#34;heap&#34;,&#34;bytes&#34; ] |
| period | [int64](#int64) |  | The number of events between sampled occurrences. |
| comment | [int64](#int64) | repeated | Freeform text associated to the profile.

Indices into string table. |
| default_sample_type | [int64](#int64) |  | Index into the string table of the type of the preferred sample value. If unset, clients should default to the last sample value. |






<a name="perftools-profiles-Sample"></a>

### Sample
Each Sample records values encountered in some program
context. The program context is typically a stack trace, perhaps
augmented with auxiliary information like the thread-id, some
indicator of a higher level request being handled etc.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| location_id | [uint64](#uint64) | repeated | The ids recorded here correspond to a Profile.location.id. The leaf is at location_id[0]. |
| value | [int64](#int64) | repeated | The type and unit of each value is defined by the corresponding entry in Profile.sample_type. All samples must have the same number of values, the same as the length of Profile.sample_type. When aggregating multiple samples into a single sample, the result has a list of values that is the elemntwise sum of the lists of the originals. |
| label | [Label](#perftools-profiles-Label) | repeated | label includes additional context for this sample. It can include things like a thread id, allocation size, etc |






<a name="perftools-profiles-ValueType"></a>

### ValueType
ValueType describes the semantics and measurement units of a value.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| ty | [int64](#int64) |  | Rename it from type to ty to avoid using keyword in Rust.

Index into string table. |
| unit | [int64](#int64) |  | Index into string table. |





 

 

 

 



## Scalar Value Types

| .proto Type | Notes | C++ | Java | Python | Go | C# | PHP | Ruby |
| ----------- | ----- | --- | ---- | ------ | -- | -- | --- | ---- |
| <a name="double" /> double |  | double | double | float | float64 | double | float | Float |
| <a name="float" /> float |  | float | float | float | float32 | float | float | Float |
| <a name="int32" /> int32 | Uses variable-length encoding. Inefficient for encoding negative numbers – if your field is likely to have negative values, use sint32 instead. | int32 | int | int | int32 | int | integer | Bignum or Fixnum (as required) |
| <a name="int64" /> int64 | Uses variable-length encoding. Inefficient for encoding negative numbers – if your field is likely to have negative values, use sint64 instead. | int64 | long | int/long | int64 | long | integer/string | Bignum |
| <a name="uint32" /> uint32 | Uses variable-length encoding. | uint32 | int | int/long | uint32 | uint | integer | Bignum or Fixnum (as required) |
| <a name="uint64" /> uint64 | Uses variable-length encoding. | uint64 | long | int/long | uint64 | ulong | integer/string | Bignum or Fixnum (as required) |
| <a name="sint32" /> sint32 | Uses variable-length encoding. Signed int value. These more efficiently encode negative numbers than regular int32s. | int32 | int | int | int32 | int | integer | Bignum or Fixnum (as required) |
| <a name="sint64" /> sint64 | Uses variable-length encoding. Signed int value. These more efficiently encode negative numbers than regular int64s. | int64 | long | int/long | int64 | long | integer/string | Bignum |
| <a name="fixed32" /> fixed32 | Always four bytes. More efficient than uint32 if values are often greater than 2^28. | uint32 | int | int | uint32 | uint | integer | Bignum or Fixnum (as required) |
| <a name="fixed64" /> fixed64 | Always eight bytes. More efficient than uint64 if values are often greater than 2^56. | uint64 | long | int/long | uint64 | ulong | integer/string | Bignum |
| <a name="sfixed32" /> sfixed32 | Always four bytes. | int32 | int | int | int32 | int | integer | Bignum or Fixnum (as required) |
| <a name="sfixed64" /> sfixed64 | Always eight bytes. | int64 | long | int/long | int64 | long | integer/string | Bignum |
| <a name="bool" /> bool |  | bool | boolean | boolean | bool | bool | boolean | TrueClass/FalseClass |
| <a name="string" /> string | A string must always contain UTF-8 encoded or 7-bit ASCII text. | string | String | str/unicode | string | string | string | String (UTF-8) |
| <a name="bytes" /> bytes | May contain any arbitrary sequence of bytes. | string | ByteString | str | []byte | ByteString | string | String (ASCII-8BIT) |

