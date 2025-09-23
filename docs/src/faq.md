# FAQ

## Just how fast is Quilkin? What sort of performance can I expect?

Our current testing shows that on Quilkin shows that it process packets _quite fast_!

We won't be publishing performance benchmarks, as performance will always
change depending on the underlying hardware, number of filters, configurations and more.

We highly recommend you run your own load tests on your platform and configuration, matching your production
workload and configuration as close as possible.

Our [iperf3](https://iperf.fr/) based performance test in the
[examples' folder](https://github.com/EmbarkStudios/quilkin/tree/{{GITHUB_REF_NAME}}/examples/iperf3) is a good starting point.

Since this is still an alpha project, we have plans on investigating further performance improvements in upcoming
releases, both from an optimisation and observability perspective as well.

## Can I integrate Quilkin with C++ code?

Quilkin is also released as a <a href="https://crates.io/crates/quilkin" data-proofer-ignore>library</a> so it can be
integrated with an external codebase as necessary.

Using Rust code inside a C or C++ project mostly consists of two parts.

* Creating a C-friendly API in Rust
* Embedding your Rust project into an external build system

See [A little Rust with your C](https://docs.rust-embedded.org/book/interoperability/rust-with-c.html) for more
information.

Over time, we will be expanding documentation on how to integrate with specific engines if running Quilkin as a
separate binary is not an option.

## I would like to run Quilkin as a client side proxy on a console? Can I do that?

This is an ongoing discussion, and since console development is protected by non-disclosure agreements, we can't
comment on this directly.

That being said, we have an [Unreal Engine](./sdks/unreal-engine.md) for games
in circumstances where compiling Rust or providing a separate Quilkin binary as
an executable is not an option.

## Any reason you didn't contribute this into/extend Envoy?

This is an excellent question! [Envoy](https://www.envoyproxy.io/) is an amazing project, and has set many of the
standards for how [proxies are written and orchestrated](./services/xds.md), and was an inspiration for many of
the decisions made on Quilkin.

However, we decided to build this project separately:

* Envoy seems primarily focused on web/mobile network workloads (which makes total sense), whereas we wanted
  something specialised on gaming UDP communication, so having a leaner, more focused codebase would allow us to move
  faster.
* We found the Rust and Cargo ecosystem easier to work with than Bazel and C++, and figured our users would as well.
