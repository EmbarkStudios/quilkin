# Quickstart: Quilkin with netcat

## Requirements

* A \*nix terminal
* A binary release of Quilkin from the [Github releases page](https://github.com/EmbarkStudios/quilkin/releases) or by running `cargo install quilkin`
* [ncat](https://www.google.com/search?q=ncat)
* [netcat](http://netcat.sourceforge.net/)

## 1. Start an udp echo service

So that we have a target for sending UDP packets to, let's use `ncat` to create a simple UDP echo process.

To do this run:

```shell
ncat -e $(which cat) -k -u -l 8080
```

This routes all UDP packets that `ncat` receives to the local `cat` process, which echoes it back.

## 2. Start Quilkin

Next let's configure Quilkin in proxy mode, with a static configuration that points at the
UDP echo service we just started.

```shell
quilkin proxy --to 127.0.0.1:8080
```

This configuration will start Quilkin on the [default proxy port](../../services/proxy.md), and it will
redirect all incoming UDP traffic to a single endpoint of 127.0.0.1, port 8080.

You should see an output like the following:

```shell
{"timestamp":"2023-01-19T10:16:23.399277Z","level":"INFO","fields":{"message":"Starting Quilkin","version":"0.6
.0-dev","commit":"72176a191ffc3a597e3834ee1d0090b30caf81d4"},"target":"quilkin::cli","filename":"src/cli.rs"}
{"timestamp":"2023-01-19T10:16:23.399771Z","level":"INFO","fields":{"message":"Starting admin endpoint","addres
s":"0.0.0.0:8000"},"target":"quilkin::admin","filename":"src/admin.rs"}
{"timestamp":"2023-01-19T10:16:23.400544Z","level":"INFO","fields":{"message":"Starting","port":7777,"proxy_id"
:"7e9fc464-6ccc-41fe-afc4-6c97089de9b8"},"target":"quilkin::proxy","filename":"src/proxy.rs"}
{"timestamp":"2023-01-19T10:16:23.401192Z","level":"INFO","fields":{"message":"Quilkin is ready"},"target":"qui
```

## 3. Send a packet

In (yet 😃) another shell, let's use netcat to send an udp packet.

Run the following to connect netcat to Quilkin's receiving port of 7000 via UDP (`-u`):

```shell
nc -u 127.0.0.1 7777
```

Type the word "test" and hit enter, you should see it echoed back to you like so:

```shell
nc -u 127.0.0.1 7777
test
test
```

Feel free to send even more packets, as many as you would like 👍.

Congratulations! You have successfully routed a UDP packet and back again with Quilkin!

What's next?

* Run through the [Quilkin with Agones quickstart](agones-xonotic-sidecar.md).
* Have a look at some of [the examples](https://github.com/EmbarkStudios/quilkin/blob/{{GITHUB_REF_NAME}}/examples) we have.
