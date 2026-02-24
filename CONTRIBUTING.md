# Contributing to Quilkin
Thank you for your interest in contributing to Quilkin.

The easiest way to get started if you want to contribute code is to look for [unassigned "help wanted" issues.]. If you have any questions along the way please ask for help on [GitHub Discussions].

If you have a feature you'd like to request that doesn't have an existing issue, please feel free to create one with the ["Feature Request"] issue template.

Participation in this project comes under the [Contributor Covenant Code of Conduct](code-of-conduct.md)

[unassigned "help wanted" issues.]: https://github.com/EmbarkStudios/quilkin/issues?q=is%3Aissue%20state%3Aopen%20label%3A%22help%20wanted%22%20no%3Aassignee
[github discussions]: https://github.com/EmbarkStudios/quilkin/discussions
["Feature Request"]: https://github.com/EmbarkStudios/quilkin/issues/new?template=feature_request.md

## Repository Layout

Quilkin is a Cargo workspace, containing not just the main binary but a a lot of companion crates for helping with interation testing, functionality, etc. The main Quilkin crate is located in the top level directory.

- [`agones`](./crates/agones) Agones Integration Test Runner
- [`ebpf`](./crates/ebpf) The eBPF module that is loaded at runtime when using the XDP I/O backend on Linux.
- [`macros`](./crates/macros) Helper procedural macros.
- [`nmap-service-probes`](./crates/nmap-service-probes) A sans-io parser for reading nmap service probe files for parsing well known UDP packets.
- [`proto-gen`](./crates/proto-gen) A wrapper tool for generating and validating protobuf.
- [`quilkin-proto`](./crates/quilkin-proto) The generated protobuf Rust code.
- [`test`](./crates/test) A set of integration tests with utilities designed for testing.
- [`xdp`](./crates/xdp) Library for loading a eBPF XDP program.
- [`xds`](./crates/xds) Library for managing gRPC xDS protocol.

## Technical Architecture

To enable Quilkin to work in a wide variety of environments and at different levels of scale, Quilkin has a large surface area of integrations with different APIs. The technical architecture can be broadly broken up into three distinct areas.

- **Configuration** Information available to Quilkin such as gameservers and network filters.
- **Services** Network services for users (both client & backend) that pull from the configuration (E.g. UDP, HTTP, gRPC).
- **Providers** Sources of information that Quilkin can use to update its configuration. (E.g. gRPC, K8S, Agones).

When writing new or adding new functionality to services or providers, Quilkin prioritises convention over configuration, meaning that when possible a Quilkin integration should feel intuitive for people familiar with that ecosystem.

When designing the integration, you should ask questions like "Are there well known ports, paths, names that users would expect?", "What are good defaults that meet the principle of least surprise?", "Are there ways to automate retrieving that information from the environment?"

To add a service you need to add it to the [`src/service.rs`](./src/service.rs), and then add the configuration to the `Service` struct. Providers are similarly located in [`src/providers`](./src/providers).

## Submitting code via Pull Requests
See our [Quilkin Development and Building Guide](./build/README.md) for developing, testing and building Quilkin.

- We follow the [GitHub Pull Request Model](https://help.github.com/articles/about-pull-requests/) for
  all contributions.
- For large bodies of work, we recommend creating an issue and labelling it
  "[kind/design](https://github.com/EmbarkStudios/quilkin/issues?q=is%3Aissue+is%3Aopen+label%3Akind%2Fdesign)"
  outlining the feature that you wish to build, and describing how it will be implemented. This gives a chance
  for review to happen early, and ensures no wasted effort occurs.
- For new features, documentation *must* be included. Documentation can currently be found under 
  the [docs](./docs) folder.
- All submissions, including submissions by project members, will require review before being merged.
- Finally - *Thanks* for considering submitting code to Quilkin!

## Coding standards
When submitting pull requests, make sure to do the following:

- Format all Rust code with [rustfmt](https://github.com/rust-lang/rustfmt).
- Ensure all Rust code passes the Rust [clippy](https://github.com/rust-lang/rust-clippy) linter.
- Remove trailing whitespace. Many editors will do this automatically.
- Ensure any new files have [a trailing newline](https://stackoverflow.com/questions/5813311/no-newline-at-end-of-file)

## Continuous Integration
Continuous integration is provided by [Google Cloud Build](https://cloud.google.com/cloud-build),
through the [cloudbuild.yaml](./cloudbuild.yaml) file found at the root of the directory, and integrated with the
Github repository via the 
[Cloud Build Github app](https://cloud.google.com/cloud-build/docs/automating-builds/run-builds-on-github).

Build success or failure are displayed on each pull request with relevant details.

To gain access to the details of a specific Cloud Build, join the 
[quilkin-discuss](https://groups.google.com/forum/#!forum/quilkin-discuss) google group.

See the [Google Cloud Build documentation](https://cloud.google.com/cloud-build/docs/) for more details on
how to edit and expand the build process.

## Contributor License Agreement

Contributions to this project must be accompanied by a Contributor License
Agreement. You (or your employer) retain the copyright to your contribution,
this simply gives us permission to use and redistribute your contributions as
part of the project. Head over to <https://cla.developers.google.com/> to see
your current agreements on file or to sign a new one.

You generally only need to submit a CLA once, so if you've already submitted one
(even if it was for a different project), you probably don't need to do it
again.

### Additional Resources

#### Coding and Development

- [How to write a good Git Commit message](https://chris.beams.io/posts/git-commit/) -
  Great way to make sure your Pull Requests get accepted.
