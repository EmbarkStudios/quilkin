# Proxy Metrics

The following are metrics that Quilkin provides while in Proxy Mode.

# ASN Maxmind Information

If Quilkin is provided a a MaxmindDB GeoIP database, Quilkin will log the
following information in the `maxmind information` log. Only `country_code`, on
`quilkin_session_active`, and `asn`, on the connection quality metrics below, are
exported as labels; the rest are too high in cardinality to be.

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

* `quilkin_packets_dropped_total{event, reason, filter, destination}` (Counter)

  The total number of packets that were dropped by the proxy.
    * The `reason` label is a closed set, so a breakdown built on it survives a
      filter being renamed or an `errno` producing different text:
        * `no_endpoint_match`: no upstream endpoint was available, or none matched the packet's routing token.
        * `filter_drop`: a filter chose to drop the packet, ie the chain worked as configured.
        * `filter_error`: a filter failed to process the packet.
        * `socket_error`: the socket refused the packet, or the packet couldn't be built for it.
        * `queue_full`: a send or receive queue was full.
        * `invalid_packet`: the packet couldn't be parsed as a datagram Quilkin handles. The specific parse failure is logged rather than labelled.
        * `session_limit`: the session limit was reached, so no session could be established.
        * `internal`: Quilkin lost track of state it needed to forward the packet.
    * The `filter` label is the filter responsible, and is empty when the drop
      wasn't a filter's decision.
    * The `destination` label is described under
      [`quilkin_bytes_total`](#general-metrics), and is empty for packets dropped
      before they were routed.

* `quilkin_cluster_active`

  The number of currently active clusters.

* `quilkin_cluster_active_endpoints`

  The number of currently active upstream endpoints. Note that this tracks the number of endpoints that the proxy
  knows of rather than those that it is connected to (see [Session Metrics][session-metrics] instead for those)

* `quilkin_bytes_total{event, destination}`

   The total number of bytes sent or received
  * The `event` label is either:
    * `read`: when the proxy receives data from a downstream connection on the listening port.
    * `write`: when the proxy sends data to a downstream connection via the listening port.
  * The `destination` label is the cluster the packet was routed to, so traffic
    can be attributed to a gameserver fleet rather than only counted in aggregate.
    It carries the same values as `quilkin_active_endpoints`, so the two can be
    joined.

    The cluster comes from the routing decision itself, not from looking the
    address up afterwards, so it is the cluster the packet actually went through
    even when the same endpoint is configured in more than one. It is empty when
    the destination didn't come from a cluster with a locality, which includes
    destinations a filter decoded from the packet rather than selecting from the
    cluster map. Note that `event=write` traffic carries the cluster of the
    gameserver that sent it, not of the client it is going to.

* `quilkin_packets_total{event, destination}`

  The total number of packets sent or recieved.
  * The labels are the same as [`quilkin_bytes_total`](#general-metrics).

* `quilkin_packet_jitter{event}`

  The time between packets arriving at an I/O loop (in nanoseconds). This covers
  every session the loop serves, so it is a whole-proxy figure; for the
  distribution across players use `quilkin_session_jitter_seconds`.

  The series stops being exported when no packet arrived during an aggregation
  interval, rather than continuing to publish the last value the proxy saw.

* `quilkin_errors_total{event, reason}`

  The total number of errors encountered while reading a packet from the upstream endpoint.
  * The `reason` label is a closed set, replacing the previous free-text `display`
    label. For I/O errors it names the `errno`, eg `invalid_input`,
    `no_buffer_space` or `message_too_long`, rather than interpolating the
    platform's error string.

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

* `quilkin_sessions_closed_total{reason}` (Counter)

  The total number of sessions that have ended, by why they ended. A fall in
  session count otherwise looks the same whether players left or their endpoints
  vanished.
  * `idle_timeout`: no traffic within the session TTL. UDP has no close, so this
    is what a player leaving normally looks like.
  * `endpoint_gone`: the endpoint the session was routed to is no longer in the
    cluster map. Only reported for destinations that were in the cluster map when
    the session was created, so an endpoint configured by name is never
    misattributed here.
  * `shutdown`: the proxy is shutting down.

## Connection Quality Metrics

A player's jitter and their ISP are per-player facts, so neither can be a metric
label: concurrent sessions and the thousands of ASNs seen in a day of traffic both
blow up cardinality. Instead the proxy tracks quality per session internally and
periodically exports a projection whose series count is bounded by configuration
rather than by traffic. See the `--service.udp.metrics.*` options for the
tunables.

Note the division of labour. Whether an individual *session* is having a bad time
is a judgement the proxy makes, because it is the only thing holding that
session's packet timing. Whether an *ISP* is having a bad time is left to the
consumer of these metrics: a proxy carries on the order of a hundred concurrent
sessions spread over thousands of ASNs, so no single one sees enough of any ASN to
threshold on. The proxy exports the numerator and the denominator per ASN and
expects them to be summed across the fleet before any conclusion is drawn.

Interarrival jitter is measured against a monotonic clock, and gaps longer than a
second are treated as the stream restarting rather than as jitter, so a player
pausing doesn't register as a player with a bad connection.

Sessions the proxy saw no traffic for during an interval are counted, but
contribute no quality judgement.

* `quilkin_session_jitter_seconds` (Histogram)

  The distribution of per-session interarrival jitter of downstream packets, as
  the RFC 3550 estimator. A histogram rather than a mean, because a mean of 0.5 ms
  is compatible with a few percent of players at 80 ms, and those are the players
  worth knowing about — `histogram_quantile` over this answers "what does the 99th
  percentile player at this proxy see".

  Only jitter is exported per session. Loss and round-trip time on the client's
  leg need either packet sequence numbers or a client-side timestamp, and the
  proxy has neither.

* `quilkin_sessions_active_by_asn{asn}` (Gauge)

  Active sessions by the client's ASN, for the largest
  `--service.udp.metrics.top-asns` ASNs at this proxy, and the denominator for the
  metric below.

  Sessions belonging to any other ASN are counted under `asn="other"`, so the
  breakdown always sums to the session total. Sessions whose client IP resolved to
  no ASN are counted under `asn="unknown"`. Setting `--top-asns` to 0 disables
  per-ASN reporting entirely.

* `quilkin_client_sessions_degraded{asn, reason}` (Gauge)

  The number of sessions breaching a quality threshold, by the client's ASN. Only
  ASNs currently breaching are exported, so a healthy proxy publishes nothing
  here and the series count follows the number of ISPs actually in trouble.
  * `reason = jitter`: sessions at or above `--service.udp.metrics.jitter-threshold-ms`.

  This is a count, not a verdict. For the affected share of an ISP, divide by
  `quilkin_sessions_active_by_asn` — summing both across proxies first, since one
  proxy's view of a single ASN is too small a sample to threshold on:

  ```promql
  sum by (asn) (quilkin_client_sessions_degraded{reason="jitter"})
    / sum by (asn) (quilkin_sessions_active_by_asn)
  ```

* `quilkin_client_sessions_degraded_total{reason}` (Counter)

  The total number of times a session was observed breaching a threshold, for
  alerting on a rate without depending on a share the proxy would have to pick a
  cut-off for.

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
