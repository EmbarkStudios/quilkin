# Quilkin Development

We welcome development from the community on Quilkin!

## Cloning the repository

We use several submodules, so make sure you have them downloaded and updated.

```shell script
git clone https://github.com/EmbarkStudios/quilkin.git
cd quilkin
git submodule update --init --recursive
```

You will likely want to replace `https://github.com/EmbarkStudios/quilkin.git` with your own fork of the repository
for your development.

### Developing with Rust tooling

Standard development with [cargo](https://doc.rust-lang.org/cargo/) is fully supported.

#### Rust version

We target a specific version of Rust for each build, so we can be deliberate when changing versions to
track if there are any performance changes between Rust releases.

Run `rustup show` in the root of the project directory to install the rust toolchain that is standard for Quilkin
development.

#### Building

To build a debug release, run:

`cargo build`

To build a production release, run:

`cargo build --release`

##### Generating Protobuf Files

If you are adding or editing the `.proto` files in this repository, they will need to be regenerated along with the 
reference documentation. To do this,
you will need [protoc](https://grpc.io/docs/protoc-installation/) and 
[protoc-gen-doc](https://github.com/pseudomuto/protoc-gen-doc) installed locally.

Then run:

`cargo run -p proto-gen -- generate`

Which will identify changes in protobuf definitions and regenerate code and documentation appropriately. 

#### Testing

To run the unit, integration and docs tests:

`cargo test`

To run our benchmarks:

`cargo bench`

We use [criterion](https://github.com/bheisler/criterion.rs) for benchmarking. You can find visual reports under `./target/criterion`.

To test dependency licences and security advisories:

`cargo deny check`

See the [agones](../crates/agones) folder for the [Agones](https://agones.dev) integration testing tooling.

### Developing with Make + Docker 

There are a few reasons you may want to use the [Make](https://www.gnu.org/software/make/)
and [Docker](https://docs.docker.com/) setup in conjunction with the Rust tooling setup:

* Run all the tests that are run in our continuous integration suite.
* Build binaries for your development version of Quilkin for all operating systems supported.
* Build a container image for your development version of Quilkin.

To use the tooling for Make + Docker testing and development, you will need:

* A *nix shell/environment, such as found on Linux, macOS or WSL on Windows.
* Make installed
* [Docker installed](https://docs.docker.com/get-docker/)

#### Run tests

`make test` will run all tests for this project, except the [Agones](https:/agones.dev) integration tests.

This can be useful if continuous integration is failing, but you are unable to reproduce the issue using 
your local Rust tooling

See the [agones](../agones) folder for the `make` targets to build, push and run the [Agones](https://agones.dev) 
integration testing tooling.

#### Build everything

To build all operating system binaries, as well as the container image for your current development version of 
Quilkin, run `make build`.

#### Escape with BUILD_LOCAL

Depending on your OS and setup, running everything through a Docker container may have a performance impact (Hi WSL! 👋).

If you have `cargo` installed locally, you can use the `BUILD_LOCAL` env variable to force the `Make` system use 
your local `cargo` configuration rather than build in our Docker image, which can result in a faster iterative 
development loop for some platforms.

For example:

```shell
# Builds the quilkin container image through our build image
$ make build-image 
```
```shell
# Builds the quilkin binary locally first, before building the Quilkin container image. 
$ BUILD_LOCAL=1 make build-image
```

See `make help` for all the targets this applies to.

#### Show all commands

There are more targets available than just the above.

To see all commands that are available, run `make` or `make help` from this directory.

This will display an output similar to this one with all make targets with descriptions of what they do:

```shell
$ cd build
$ make

Usage: make <command> [ARGUMENT1=value1] [ARGUMENT2=value2]

Commands:

 test                 # Run all tests

 build                # Build all binaries and images

 build-linux-binary   # Build release and debug binaries for x86_64-pc-windows-gnu

 build-windows-binary # Build release and debug binaries for x86_64-unknown-linux-gnu

 build-image          # Build release and debug container images.
                      # Use IMAGE_NAME argument to specify the container registry and image name. Defaults to 'quilkin'.
```
