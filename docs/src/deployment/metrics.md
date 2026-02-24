# Proxy Metrics

The following are metrics that Quilkin provides while in Proxy Mode.

# ASN Maxmind Information

If Quilkin is provided a a MaxmindDB GeoIP database, Quilkin will log the
following information in the `maxmind information` log, as well as populate
the following fields in any metrics with matching labels.

| Field           | Description                                   |
|-----------------|-----------------------------------------------|
| `asn`           | ASN Number                                    |
| `organization`  | The organisation responsible for the ASN      |
| `country_code`  | The corresponding country code                |
| `ip_prefix`     | The IP prefix CIDR address                    |
| `prefix_entity` | The name of the entity for the prefix address |
| `prefix_name`   | The name of the prefix address                |

> Maxmind databases often require a licence and/or fee, so they aren't included
> by default with Quilkin.

## General Metrics

The proxy exposes the following general metrics:

* `quilkin_packets_processing_duration_seconds{event, asn, ip_prefix}` (Histogram)

  The total duration of time in seconds that it took to process a packet.
    * The `event` label is either:
        * `read`: when the proxy receives data from a downstream connection on the listening port.
        * `write`: when the proxy sends data to a downstream connection via the listening port.

* `quilkin_packets_dropped_total{reason, asn, ip_prefix}` (Counter)

  The total number of packets (not associated with any session) that were dropped by proxy.
  Not that packets reflected by this metric were dropped at an earlier stage before they were associated with any session. For session based metrics, see the list of [session metrics][session-metrics] instead.
    * `reason = NoConfiguredEndpoints`
        * `NoConfiguredEndpoints`: No upstream endpoints were available to send the packet to. This can occur e.g if the endpoints cluster was scaled down to zero and the proxy is configured via a control plane.

* `quilkin_cluster_active`

  The number of currently active clusters.

* `quilkin_cluster_active_endpoints`

  The number of currently active upstream endpoints. Note that this tracks the number of endpoints that the proxy
  knows of rather than those that it is connected to (see [Session Metrics][session-metrics] instead for those)

* `quilkin_bytes_total{event, asn, ip_prefix}`

   The total number of bytes sent or received
  * The `event` label is either:
    * `read`: when the proxy receives data from a downstream connection on the listening port.
    * `write`: when the proxy sends data to a downstream connection via the listening port.

* `quilkin_packets_total{event, asn, ip_prefix}`

  The total number of packets sent or recieved.
  * The `event` label is either:
    * `read`: when the proxy receives data from a downstream connection on the listening port.
    * `write`: when the proxy sends data to a downstream connection via the listening port.

* `quilkin_packet_jitter{event, asn, ip_prefix}`

  The time between receiving new packets (in nanoseconds).

* `quilkin_errors_total{event, asn, ip_prefix}`

  The total number of errors encountered while reading a packet from the upstream endpoint.

* `quilkin_game_traffic_tasks`

  The amount of game traffic tasks that have spawned

* `quilkin_game_traffic_task_closed`
   
  The amount of game traffic tasks that have shutdown

## Session Metrics

The proxy exposes the following metrics around sessions:

* `quilkin_session_active{asn, organization, country_code, ip_prefix, prefix_entity, prefix_name}`

  The number of currently active sessions. If a maxmind database has been
  provided, the labels are populated:
  * The `asn` label is the [ASN](https://en.wikipedia.org/wiki/Autonomous_system_(Internet)) number of the connecting
    client.
  * The `ip_prefix`label is the IP prefix of the connecting client.

* `quilkin_session_duration_secs` (Histogram)

  A histogram over how long sessions lasted before they were torn down. Note that, by definition, active sessions are not included in this metric.

* `quilkin_session_total` (Counter)

  The total number of sessions that have been created.

## Filter Metrics
Quilkin's filters use a set of generic metric keys, to make it easier to build visualisations that can account for
a dynamic set of filters that can be added, removed, or updated at runtime with different configurations. All of
these metrics share a common set of labels.

| Label | Description |
|-------|-------------|
| `id` | The ID of the filter that used the metric. |
| `label` | The name of the metric being measured. |
| `help` | The description of the filter metric. |
| `direction` | The direction of packet flow (e.g. read/write). |

* `quilkin_filter_int_counter{id, label, help, direction}`
  Generic filter counter, see help label for more specific info.

* `quilkin_filter_histogram{id, label, help, direction, shared_metadata_1}`
  generic filter histogram, see help label for more specific info.

* `quilkin_filter_read_duration_seconds{filter}`

  The duration it took for a `filter`'s `read` implementation to execute.
  * The`filter` label is the name of the filter being executed.

* `quilkin_filter_write_duration_seconds{filter}`

  The duration it took for a `filter`'s `write` implementation to execute.
  * The `filter` label is the name of the filter being executed.

[session-metrics]: #session-metrics
