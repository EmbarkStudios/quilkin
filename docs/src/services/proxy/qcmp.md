# Quilkin Control Message Protocol (QCMP)

| services | ports | Protocol |
|----------|-------|-----------|
| QCMP | 7600 | UDP AND TCP (IPv4 OR IPv6) |

In addition to the TCP based administration API, Quilkin provides a meta API
over UDP and TCP. The purpose of this API is to provide meta operations that can
be used by untrusted clients. Currently the API is focuses on providing pings
for latency measurement but that may change overtime as new features are added.

## Ping
The main functionality currently in QCMP is pinging, measuring the latency from
one service to another over UDP. You can also measure the distance locally using
the `quilkin qcmp ping` command.

```
quilkin qcmp ping 127.0.0.1:7600
```

## Protocol Data Unit
The following is a [Kaitai Struct](https://kaitai.io/) configuration of the protocol data unit
accepted by Quilkin proxies.

```yaml
meta:
  id: quilkin_packet
  endian: be
seq:
  - id: magic_header
    contents: "QLKN"
  - id: protocol_version
    type: u1
  - id: packet_type
    type: u1
  - id: packet_length
    type: u2
  - id: packet
    size: packet_length
    type:
      switch-on: packet_type
      cases:
        0: ping_packet
        1: ping_reply_packet

types:
  ping_packet:
    doc: |
        Sent by a client to a proxy containing the time when the client sent the
        packet along with a unique nonce. The timestamp should be a UTC unix
        timestamp in **nanoseconds**.
    seq:
      - id: nonce
        type: u1
      - id: client_sent_timestamp
        type: u8

  ping_reply_packet:
    doc: |
        A reply from a proxy containing the ping's nonce, the client sent time,
        the server's receive time, and the server's sent time. The timestamps
        should be UTC unix timestamps in **nanoseconds**.
    seq:
      - id: nonce
        type: u1
      - id: client_sent_timestamp
        type: u8
      - id: server_receive_timestamp
        type: u8
      - id: server_sent_timestamp
        type: u8
```

## Datacenter Latency

In addition to being able to ping Quilkin to get the latency between the client
and proxy. In order to allow clients to send information to services like a
matchmaker about which datacentre they are closest to, Quilkin also includes
the ability to get a proxy's latency to each of its connected datacentres.

> Note: This requires a multi-cluster relay setup, as when you set up proxies
  in the same cluster as gameservers, this measurement is redundant.

All that is required to set this up is to provide an ICAO code to the agent in
the gameserver cluster. (E.g. through the environment variable `ICAO_CODE`).
No further setup is required. **You can use duplicate ICAO codes**, Quilkin will
choose the best result amongst the duplicates to return. Quilkin assumes that
multiple of the same ICAO code refer to the same phyiscal datacentre, so latency
between them should negible.

> Why ICAO? ICAO is an international standard for airport codes, airport codes
  are an easy human readable code that makes it easy to use geo-visualisations
  in tools like Grafana, and easily allows grouping. IATA codes only cover
  major airports, ICAO codes cover practically every airport making them easy to
  more accurately represent the location of any datacentre.


### API And Schema

Currently the datacentre latency can be retrieved by sending a `GET /` HTTP
request to the QCMP port.

The returned data is a JSON object with each key being the ICAO code for the
datacentre, and the value being the latency in nanoseconds.

## Metrics

* `quilkin_phoenix_requests`

  The amount of phoenix (latency) requests

* `quilkin_phoenix_task_closed`

  Whether the phoenix latency measurement task has shutdown
  
* `quilkin_phoenix_server_errors`

  The amount of errors attempting to spawn the phoenix HTTP server

* `quilkin_service_qcmp_active`

  Whether the QCMP service is currently running, either 1 for running or 0 for not.

* `quilkin_service_qcmp_bytes_total{kind, asn, ip_prefix}`

  Total number of bytes processed through QCMP. `kind` is either `valid`, `invalid`, or `unsupported`.

* `quilkin_service_qcmp_errors_total{asn, ip_prefix}`

  Total number of errors QCMP has encountered.

* `quilkin_service_qcmp_packets_total{kind, asn, ip_prefix}`

  Total number of packets processed through QCMP. `kind` is either `valid`, `invalid`, or `unsupported`.

* `quilkin_service_qcmp_ingress_latency_seconds{asn, ip_prefix}`

  The time from when the client created the packet, to when QCMP received it.
