# Changelog

# v0.9.0 (2024-08-19)

## What's Changed
### Breaking changes
* Remove idle request interval for agent by @XAMPPRocky in https://github.com/googleforgames/quilkin/pull/946
### Implemented enhancements
* Implement remaining proxy tests by @Jake-Shadle in https://github.com/googleforgames/quilkin/pull/916
* Add node address selection via type and ip kind by @Jake-Shadle in https://github.com/googleforgames/quilkin/pull/934
* Add PortPolicy::None by @XAMPPRocky in https://github.com/googleforgames/quilkin/pull/940
* Add some performance optimizations by @Jake-Shadle in https://github.com/googleforgames/quilkin/pull/944
* Upgrade Agones to 1.40.0 + CRD changes by @markmandel in https://github.com/googleforgames/quilkin/pull/945
* Add an optimized token router filter by @Jake-Shadle in https://github.com/googleforgames/quilkin/pull/948
* Build single token -> address map by @Jake-Shadle in https://github.com/googleforgames/quilkin/pull/978
* Replace `tokio-uring` with `io-uring` by @Jake-Shadle in https://github.com/googleforgames/quilkin/pull/993
* Update project to beta status by @XAMPPRocky in https://github.com/googleforgames/quilkin/pull/997
* Add basic heap stats by @Jake-Shadle in https://github.com/googleforgames/quilkin/pull/998
### Fixed bugs
* Fix timestamp/duration unit confusion by @Jake-Shadle in https://github.com/googleforgames/quilkin/pull/921
* Fix safety issue by @Jake-Shadle in https://github.com/googleforgames/quilkin/pull/922
* Fix phoenix http by @Jake-Shadle in https://github.com/googleforgames/quilkin/pull/926
* Don't run `add_host_to_datacenter` by @Jake-Shadle in https://github.com/googleforgames/quilkin/pull/931
* Fix infinite loop by @Jake-Shadle in https://github.com/googleforgames/quilkin/pull/956
* Ignore config maps for agents by @Jake-Shadle in https://github.com/googleforgames/quilkin/pull/957
* Fix relay listener updates by @Jake-Shadle in https://github.com/googleforgames/quilkin/pull/966
* Increase downstream->upstream buffer by @XAMPPRocky in https://github.com/googleforgames/quilkin/pull/969
* Cap error heap usage by @Jake-Shadle in https://github.com/googleforgames/quilkin/pull/987
* Unstick release 0.9.0 by @markmandel in https://github.com/googleforgames/quilkin/pull/1002
### Security fixes
* cargo update + flake fixes by @markmandel in https://github.com/googleforgames/quilkin/pull/930
### Other
* Bump google.golang.org/protobuf from 1.32.0 to 1.33.0 in /build/ci/github-bot by @dependabot in https://github.com/googleforgames/quilkin/pull/907
* Prep for 0.9.0-dev by @markmandel in https://github.com/googleforgames/quilkin/pull/908
* Test refactor proposal by @Jake-Shadle in https://github.com/googleforgames/quilkin/pull/909
* Add more logs to phoenix service by @XAMPPRocky in https://github.com/googleforgames/quilkin/pull/913
* Reenable test by @Jake-Shadle in https://github.com/googleforgames/quilkin/pull/914
* Update to Rust 1.77.0 by @XAMPPRocky in https://github.com/googleforgames/quilkin/pull/912
* Make idle request logs debug by @XAMPPRocky in https://github.com/googleforgames/quilkin/pull/915
* Add change detection log by @XAMPPRocky in https://github.com/googleforgames/quilkin/pull/917
* Add debugging to phoenix http service by @Jake-Shadle in https://github.com/googleforgames/quilkin/pull/919
* Make unknown gameserver log debug by @XAMPPRocky in https://github.com/googleforgames/quilkin/pull/923
* Slight test improvements by @Jake-Shadle in https://github.com/googleforgames/quilkin/pull/925
* Bump golang.org/x/net from 0.19.0 to 0.23.0 in /build/ci/github-bot by @dependabot in https://github.com/googleforgames/quilkin/pull/928
* Upgrade Agones CRD by @XAMPPRocky in https://github.com/googleforgames/quilkin/pull/937
* Update kube to 0.91 by @XAMPPRocky in https://github.com/googleforgames/quilkin/pull/941
* Add kube@0.91 to skip-tree by @XAMPPRocky in https://github.com/googleforgames/quilkin/pull/943
* Remove manual build of rust-linux-darwin-builder by @markmandel in https://github.com/googleforgames/quilkin/pull/947
* Add test for hashed token router by @XAMPPRocky in https://github.com/googleforgames/quilkin/pull/950
* Move related crates into crates directory by @XAMPPRocky in https://github.com/googleforgames/quilkin/pull/961
* Always build token maps by @Jake-Shadle in https://github.com/googleforgames/quilkin/pull/962
* Move xDS and protobuf definitions into separate crates. by @XAMPPRocky in https://github.com/googleforgames/quilkin/pull/963
* Nuke non-delta streams by @Jake-Shadle in https://github.com/googleforgames/quilkin/pull/965
* Move MaxmindDb::lookup to session creation by @XAMPPRocky in https://github.com/googleforgames/quilkin/pull/968
* Guide: Show command options for `manage providers` by @markmandel in https://github.com/googleforgames/quilkin/pull/971
* Github: PR Labeler action by @markmandel in https://github.com/googleforgames/quilkin/pull/972
* Build tooling updates and fixes. by @markmandel in https://github.com/googleforgames/quilkin/pull/974
* Update agent documentation by @XAMPPRocky in https://github.com/googleforgames/quilkin/pull/976
* Make xDS library generic over any resource type. by @XAMPPRocky in https://github.com/googleforgames/quilkin/pull/967
* Update crates by @Jake-Shadle in https://github.com/googleforgames/quilkin/pull/977
* Cleanup by @Jake-Shadle in https://github.com/googleforgames/quilkin/pull/979
* Rename HashedTokenRouter -> TokenRouter by @Jake-Shadle in https://github.com/googleforgames/quilkin/pull/980
* Reduce ASN related allocations by @Jake-Shadle in https://github.com/googleforgames/quilkin/pull/991
* Generate reference docs from proto files by @markmandel in https://github.com/googleforgames/quilkin/pull/982
* Update crates/cargo-deny by @Jake-Shadle in https://github.com/googleforgames/quilkin/pull/992
* Add Apache Headers to those that are missing by @markmandel in https://github.com/googleforgames/quilkin/pull/999
* Docs for Heap Allocation Metrics by @markmandel in https://github.com/googleforgames/quilkin/pull/1000
* Build: More explicit container caching by @markmandel in https://github.com/googleforgames/quilkin/pull/1001
* Release 0.9.0 by @markmandel in https://github.com/googleforgames/quilkin/pull/996

**Full Changelog**: https://github.com/googleforgames/quilkin/compare/v0.8.0...v0.9.0

# v0.8.0 (2024-03-13)

## What's Changed
### Implemented enhancements
* Move admin server to separate OS thread by @XAMPPRocky in https://github.com/googleforgames/quilkin/pull/841
* Use ClusterMap for Filter::read by @XAMPPRocky in https://github.com/googleforgames/quilkin/pull/855
* Move game traffic sockets to io-uring by @XAMPPRocky in https://github.com/googleforgames/quilkin/pull/850
* Add lz4 support by @Jake-Shadle in https://github.com/googleforgames/quilkin/pull/868
* Add Configurable gRPC message size environment variable by @XAMPPRocky in https://github.com/googleforgames/quilkin/pull/870
* Move xDS proxy task to its own thread+runtime by @XAMPPRocky in https://github.com/googleforgames/quilkin/pull/871
* Change debug symbols profile release -> bench by @Jake-Shadle in https://github.com/googleforgames/quilkin/pull/879
* Add initial support for delta xDS by @Jake-Shadle in https://github.com/googleforgames/quilkin/pull/882
* Add pprof endpoint by @XAMPPRocky in https://github.com/googleforgames/quilkin/pull/875
* ClusterMap benchmarks by @Jake-Shadle in https://github.com/googleforgames/quilkin/pull/856
* Update Unreal Engine Plugin by @XAMPPRocky in https://github.com/googleforgames/quilkin/pull/778
* Implement Phoenix Network Coordinates by @XAMPPRocky in https://github.com/googleforgames/quilkin/pull/854
* Improve compile times by @Jake-Shadle in https://github.com/googleforgames/quilkin/pull/896
* Move non-linux builds to github by @Jake-Shadle in https://github.com/googleforgames/quilkin/pull/899
### Fixed bugs
* Update max_delay to 2 sec by @zezhehh in https://github.com/googleforgames/quilkin/pull/840
* Move pipeline errors from metrics to a fixed interval report by @XAMPPRocky in https://github.com/googleforgames/quilkin/pull/843
* Remove unwraps from proxy::sessions by @XAMPPRocky in https://github.com/googleforgames/quilkin/pull/849
* Commit Cargo.lock by @Jake-Shadle in https://github.com/googleforgames/quilkin/pull/867
* Fix PoolBuffer::split by @Jake-Shadle in https://github.com/googleforgames/quilkin/pull/869
* Fix: warning: failed to get HEAD path by @markmandel in https://github.com/googleforgames/quilkin/pull/878
* Send back an empty delta response for initial `ignore-me` by @Jake-Shadle in https://github.com/googleforgames/quilkin/pull/892
* Make the ready check has endpoints OR has xDS connection by @XAMPPRocky in https://github.com/googleforgames/quilkin/pull/872
* Fix health_server test by @Jake-Shadle in https://github.com/googleforgames/quilkin/pull/897
* Build: Https on packages.cloud.google.com by @markmandel in https://github.com/googleforgames/quilkin/pull/903
### Other
* Prep for 0.8.0 by @markmandel in https://github.com/googleforgames/quilkin/pull/831
* Remove spawn task for each packet. by @XAMPPRocky in https://github.com/googleforgames/quilkin/pull/842
* Move cluster into net by @XAMPPRocky in https://github.com/googleforgames/quilkin/pull/845
* Update kubernetes watch configuration to prioritise performance by @XAMPPRocky in https://github.com/googleforgames/quilkin/pull/844
* Minor cleanup by @Jake-Shadle in https://github.com/googleforgames/quilkin/pull/846
* Update xds client by @XAMPPRocky in https://github.com/googleforgames/quilkin/pull/848
* Remove built dependency by @Jake-Shadle in https://github.com/googleforgames/quilkin/pull/847
* Refactor benchmarks by @Jake-Shadle in https://github.com/googleforgames/quilkin/pull/853
* Update to Rust 1.74.0 by @XAMPPRocky in https://github.com/googleforgames/quilkin/pull/859
* Remove Watch requirements on Clone/PartialEq by @Jake-Shadle in https://github.com/googleforgames/quilkin/pull/876
* Split out changes by @Jake-Shadle in https://github.com/googleforgames/quilkin/pull/883
* Bump zerocopy from 0.7.26 to 0.7.31 by @dependabot in https://github.com/googleforgames/quilkin/pull/886
* Bump unsafe-libyaml from 0.2.9 to 0.2.10 by @dependabot in https://github.com/googleforgames/quilkin/pull/888
* Update dependencies on GitHub Bot by @markmandel in https://github.com/googleforgames/quilkin/pull/889
* Bump h2 from 0.3.22 to 0.3.24 by @dependabot in https://github.com/googleforgames/quilkin/pull/891
* Remove agent note by @XAMPPRocky in https://github.com/googleforgames/quilkin/pull/894
* Update CODEOWNERS by @XAMPPRocky in https://github.com/googleforgames/quilkin/pull/895
* Remove protobuf_src from example by @Jake-Shadle in https://github.com/googleforgames/quilkin/pull/898
* Bump mio from 0.8.10 to 0.8.11 by @dependabot in https://github.com/googleforgames/quilkin/pull/900
* Use newly published deny by @Jake-Shadle in https://github.com/googleforgames/quilkin/pull/901
* Add licence.html to release quilkin.zip by @markmandel in https://github.com/googleforgames/quilkin/pull/905

## New Contributors
* @zezhehh made their first contribution in https://github.com/googleforgames/quilkin/pull/840
* @Jake-Shadle made their first contribution in https://github.com/googleforgames/quilkin/pull/846

**Full Changelog**: https://github.com/googleforgames/quilkin/compare/v0.7.0...v0.8.0

# v0.7.0 (2023-10-18)

## What's Changed
### Breaking changes
* Move QCMP to a seperate port by @XAMPPRocky in https://github.com/googleforgames/quilkin/pull/741
* Rename: ConcatenateBytes ➡️ Concatenate by @markmandel in https://github.com/googleforgames/quilkin/pull/813
### Implemented enhancements
* Add ASN cardinality to packet metrics by @XAMPPRocky in https://github.com/googleforgames/quilkin/pull/746
* Unit and integration test debugging enhancements by @markmandel in https://github.com/googleforgames/quilkin/pull/762
* Add logging for what endpoints are added and removed by @XAMPPRocky in https://github.com/googleforgames/quilkin/pull/774
* Add `qcmp ping` command by @XAMPPRocky in https://github.com/googleforgames/quilkin/pull/770
* add cli option for log formatting (Closes #531) by @Baschtie in https://github.com/googleforgames/quilkin/pull/784
* Locally listen on IPv4 and IPv6 addresses by @markmandel in https://github.com/googleforgames/quilkin/pull/788
* Add --idle-request-interval-secs by @XAMPPRocky in https://github.com/googleforgames/quilkin/pull/790
* Examples for relay configuration by @markmandel in https://github.com/googleforgames/quilkin/pull/807
* Add quickstart for the relay setup by @XAMPPRocky in https://github.com/googleforgames/quilkin/pull/772
* Integration tests for Agones Relay and Agent by @markmandel in https://github.com/googleforgames/quilkin/pull/811
* Add health checks for each service by @XAMPPRocky in https://github.com/googleforgames/quilkin/pull/802
* Refactor sessions to use socket pool by @XAMPPRocky in https://github.com/googleforgames/quilkin/pull/815
* Release aarch64-apple-darwin binary by @markmandel in https://github.com/googleforgames/quilkin/pull/829
### Fixed bugs
* Fix: mdbook-variables failing on doc building. by @markmandel in https://github.com/googleforgames/quilkin/pull/764
* Fix watch on clusters, by removing inner Arc by @XAMPPRocky in https://github.com/googleforgames/quilkin/pull/773
* Late initialise upstream socket to prevent session map lock by @XAMPPRocky in https://github.com/googleforgames/quilkin/pull/781
* Fix version issue when publishing release docs by @markmandel in https://github.com/googleforgames/quilkin/pull/793
* Fix dead lock in SessionPool by @XAMPPRocky in https://github.com/googleforgames/quilkin/pull/826
### Security fixes
* Update Agones to 1.33.0 by @markmandel in https://github.com/googleforgames/quilkin/pull/760
* Agones: Update simple-game-server image to latest by @markmandel in https://github.com/googleforgames/quilkin/pull/763
* Update to Rust 1.71.1 (cargo CVE-2023-38497) by @markmandel in https://github.com/googleforgames/quilkin/pull/767
* Security: Update trust-dns-resolver by @markmandel in https://github.com/googleforgames/quilkin/pull/779
* Update to Distroless based on Debian 12 by @markmandel in https://github.com/googleforgames/quilkin/pull/805
### Other
* Prep: 0.7.0-dev by @markmandel in https://github.com/googleforgames/quilkin/pull/759
* Add test for Cluster::merge by @XAMPPRocky in https://github.com/googleforgames/quilkin/pull/765
* Automatically update approved PRs with automerge by @markmandel in https://github.com/googleforgames/quilkin/pull/766
* Small doc improvements by @markmandel in https://github.com/googleforgames/quilkin/pull/768
* Updated protoc-gen-validate to v1.0.2 by @markmandel in https://github.com/googleforgames/quilkin/pull/769
* Update Tokio: 1.32.0 by @markmandel in https://github.com/googleforgames/quilkin/pull/776
* Update dependencies and Rust version by @XAMPPRocky in https://github.com/googleforgames/quilkin/pull/782
* README description update by @markmandel in https://github.com/googleforgames/quilkin/pull/783
* fix timestamp warnings by @XAMPPRocky in https://github.com/googleforgames/quilkin/pull/791
* Move more logs to debug by @XAMPPRocky in https://github.com/googleforgames/quilkin/pull/796
* Refactor ClusterMap by @XAMPPRocky in https://github.com/googleforgames/quilkin/pull/785
* Move maxmind information log to debug and per-session by @XAMPPRocky in https://github.com/googleforgames/quilkin/pull/797
* Update to the latest Xonotic release. by @markmandel in https://github.com/googleforgames/quilkin/pull/799
* Fix broken Envoy link. by @markmandel in https://github.com/googleforgames/quilkin/pull/801
* Update CODEOWNERS by @XAMPPRocky in https://github.com/googleforgames/quilkin/pull/803
* Change quickstart example README.md to point to docs. by @markmandel in https://github.com/googleforgames/quilkin/pull/800
* Update Existing Examples and Quickstarts by @markmandel in https://github.com/googleforgames/quilkin/pull/808
* Update to Rust 1.73.0 by @XAMPPRocky in https://github.com/googleforgames/quilkin/pull/809
* Update dependencies by @XAMPPRocky in https://github.com/googleforgames/quilkin/pull/810
* Docs: Connect Agent and Relay to Providers by @markmandel in https://github.com/googleforgames/quilkin/pull/812
* Cloud Build: Put logs back in public bucket by @markmandel in https://github.com/googleforgames/quilkin/pull/817
* Bump golang.org/x/net from 0.7.0 to 0.17.0 in /build/ci/github-bot by @dependabot in https://github.com/googleforgames/quilkin/pull/819
* Examples: Update readiness and liveness checks. by @markmandel in https://github.com/googleforgames/quilkin/pull/821
* Make noisy relay logs debug by @XAMPPRocky in https://github.com/googleforgames/quilkin/pull/823
* Reorganise modules by @XAMPPRocky in https://github.com/googleforgames/quilkin/pull/824
* Loadtesting fixes by @XAMPPRocky in https://github.com/googleforgames/quilkin/pull/827

## New Contributors
* @Baschtie made their first contribution in https://github.com/googleforgames/quilkin/pull/784

**Full Changelog**: https://github.com/googleforgames/quilkin/compare/v0.6.0...v0.7.0

## v0.6.0 (2023-07-7)

## What's Changed
### Breaking changes
* Refactor documentation layout and define default ports by @XAMPPRocky in https://github.com/googleforgames/quilkin/pull/688
* Refactor filter metrics into a single vector of metrics with labels by @XAMPPRocky in https://github.com/googleforgames/quilkin/pull/736
### Implemented enhancements
* Add region parameters for control planes by @XAMPPRocky in https://github.com/googleforgames/quilkin/pull/696
* Add initial implementation of relay service by @XAMPPRocky in https://github.com/googleforgames/quilkin/pull/698
* Add environment variables to config providers by @XAMPPRocky in https://github.com/googleforgames/quilkin/pull/707
* Add test for relay proxy routing (and support for file providers) by @XAMPPRocky in https://github.com/googleforgames/quilkin/pull/710
* Resolve DNS asynchronously by @XAMPPRocky in https://github.com/googleforgames/quilkin/pull/721
* Update termination code to allow proxies to wait until all sessions expire by @XAMPPRocky in https://github.com/googleforgames/quilkin/pull/740
* Implement Agent service by @XAMPPRocky in https://github.com/googleforgames/quilkin/pull/744
* Remove shutdown_rx from downstream loop by @XAMPPRocky in https://github.com/googleforgames/quilkin/pull/742
### Fixed bugs
* Add hotfix around xDS stream sometimes not responding with changes by @XAMPPRocky in https://github.com/googleforgames/quilkin/pull/683
* Refactor provider task retrying and move providers to their own module by @XAMPPRocky in https://github.com/googleforgames/quilkin/pull/702
* Workaround for mdbook-variables by @markmandel in https://github.com/googleforgames/quilkin/pull/705
* Don't drop the stream client until the function completes by @XAMPPRocky in https://github.com/googleforgames/quilkin/pull/709
* Recover when stream channel breaks by @XAMPPRocky in https://github.com/googleforgames/quilkin/pull/713
* Fix proxy not retrying from received xDS error by @XAMPPRocky in https://github.com/googleforgames/quilkin/pull/727
* Find the endpoint to delete using partial information from the server in case some information is missing by @XAMPPRocky in https://github.com/googleforgames/quilkin/pull/729
* Clamp provider retry delay to five minutes by @XAMPPRocky in https://github.com/googleforgames/quilkin/pull/733
* Improve server delete logic by @XAMPPRocky in https://github.com/googleforgames/quilkin/pull/738
### Security fixes
* Migrate image to supported distroless tag by @markmandel in https://github.com/googleforgames/quilkin/pull/693
### Other
* Updates for 0.6.0-dev by @markmandel in https://github.com/googleforgames/quilkin/pull/681
* Add warning when watcher fails to send an update by @XAMPPRocky in https://github.com/googleforgames/quilkin/pull/684
* Remove unneeded pin requirement by @XAMPPRocky in https://github.com/googleforgames/quilkin/pull/692
* Strip Config of all service specific configuration for now by @XAMPPRocky in https://github.com/googleforgames/quilkin/pull/694
* Add debug line for cli parameters by @XAMPPRocky in https://github.com/googleforgames/quilkin/pull/706
* Add filter chain discovery to the relay by @XAMPPRocky in https://github.com/googleforgames/quilkin/pull/703
* Bump golang.org/x/net from 0.4.0 to 0.7.0 in /build/ci/github-bot by @dependabot in https://github.com/googleforgames/quilkin/pull/708
* Bump golang.org/x/crypto from 0.0.0-20210921155107-089bfa567519 to 0.1.0 in /build/ci/github-bot by @dependabot in https://github.com/googleforgames/quilkin/pull/715
* `continue` rather than `return` upon receiving invalid gameserver by @XAMPPRocky in https://github.com/googleforgames/quilkin/pull/724
* Refactor drop into a single metric, change filters to return a result by @XAMPPRocky in https://github.com/googleforgames/quilkin/pull/722
* Include debug info on unknown server in warning by @XAMPPRocky in https://github.com/googleforgames/quilkin/pull/732
* Add experimental notification in the docs by @XAMPPRocky in https://github.com/googleforgames/quilkin/pull/730
* Use GitHub's Changelog Generation by @markmandel in https://github.com/googleforgames/quilkin/pull/735
* Add initial implementation of QCMP by @XAMPPRocky in https://github.com/googleforgames/quilkin/pull/676
* Update Rust to 1.69.0 and update dependencies by @XAMPPRocky in https://github.com/googleforgames/quilkin/pull/745
* Fix probuf link in docs by @markmandel in https://github.com/googleforgames/quilkin/pull/753
* Move session expiry to debug logging. by @markmandel in https://github.com/googleforgames/quilkin/pull/754
* Update examples to work with upcoming 0.6.0 by @markmandel in https://github.com/googleforgames/quilkin/pull/755
* Update Docs for 0.6.0 Release by @markmandel in https://github.com/googleforgames/quilkin/pull/756
* Downgrade trust-dns-resolver by @XAMPPRocky in https://github.com/googleforgames/quilkin/pull/748
* Improve missing gameserver warning by @XAMPPRocky in https://github.com/googleforgames/quilkin/pull/743
* Add GDC presentation to documentation. by @markmandel in https://github.com/googleforgames/quilkin/pull/757

## New Contributors
* @dependabot made their first contribution in https://github.com/googleforgames/quilkin/pull/708

**Full Changelog**: https://github.com/googleforgames/quilkin/compare/v0.5.0...v0.6.0

## [v0.5.0](https://github.com/googleforgames/quilkin/tree/v0.5.0) (2023-01-11)

[Full Changelog](https://github.com/googleforgames/quilkin/compare/v0.4.0...v0.5.0)

**Breaking changes:**

- Rename run command to proxy [\#661](https://github.com/googleforgames/quilkin/pull/661) ([XAMPPRocky](https://github.com/XAMPPRocky))

**Implemented enhancements:**

- \[Docs\] Agones xDS Provider Quickstart [\#644](https://github.com/googleforgames/quilkin/issues/644)
- --version flag for binary [\#668](https://github.com/googleforgames/quilkin/pull/668) ([markmandel](https://github.com/markmandel))
- Agones xDS Provider Quickstart [\#667](https://github.com/googleforgames/quilkin/pull/667) ([markmandel](https://github.com/markmandel))

**Fixed bugs:**

- xDS dynamic routing stops working after 4-5 days [\#660](https://github.com/googleforgames/quilkin/issues/660)
- Docs: `quilkin run` vs `proxy` bug in preprocessor [\#678](https://github.com/googleforgames/quilkin/pull/678) ([markmandel](https://github.com/markmandel))
- Timeout for xDS gRPPC Client connection [\#664](https://github.com/googleforgames/quilkin/pull/664) ([markmandel](https://github.com/markmandel))
- Add retry for provider task [\#659](https://github.com/googleforgames/quilkin/pull/659) ([XAMPPRocky](https://github.com/XAMPPRocky))
- docs/src/proxy/filters/writing\_custom\_filters: fix port typo [\#652](https://github.com/googleforgames/quilkin/pull/652) ([markus-wa](https://github.com/markus-wa))

**Closed issues:**

- Release 0.4.0 [\#647](https://github.com/googleforgames/quilkin/issues/647)

**Merged pull requests:**

- Make: package\_version less brittle [\#675](https://github.com/googleforgames/quilkin/pull/675) ([markmandel](https://github.com/markmandel))
- Replace SessionManager with TtlMap, refactor downstream packet processing to track more errors in metrics [\#674](https://github.com/googleforgames/quilkin/pull/674) ([XAMPPRocky](https://github.com/XAMPPRocky))
- Update dependencies [\#670](https://github.com/googleforgames/quilkin/pull/670) ([XAMPPRocky](https://github.com/XAMPPRocky))
- Error for invalid endpoint [\#665](https://github.com/googleforgames/quilkin/pull/665) ([markmandel](https://github.com/markmandel))
- Notification bot: Update dependencies [\#663](https://github.com/googleforgames/quilkin/pull/663) ([markmandel](https://github.com/markmandel))
- Move instrument level to trace [\#658](https://github.com/googleforgames/quilkin/pull/658) ([XAMPPRocky](https://github.com/XAMPPRocky))
- Make: Move cargo registry to /target/build-image/ [\#654](https://github.com/googleforgames/quilkin/pull/654) ([markmandel](https://github.com/markmandel))
- Update dependencies [\#651](https://github.com/googleforgames/quilkin/pull/651) ([XAMPPRocky](https://github.com/XAMPPRocky))
- Remove map\_proto\_enum and simplify proto enum conversions [\#650](https://github.com/googleforgames/quilkin/pull/650) ([XAMPPRocky](https://github.com/XAMPPRocky))
- Bump version to 0.5.0-dev [\#649](https://github.com/googleforgames/quilkin/pull/649) ([markmandel](https://github.com/markmandel))

## [v0.4.0](https://github.com/googleforgames/quilkin/tree/v0.4.0) (2022-11-15)

[Full Changelog](https://github.com/googleforgames/quilkin/compare/v0.3.0...v0.4.0)

**Breaking changes:**

- Refactor configuration and builder pattern [\#525](https://github.com/googleforgames/quilkin/pull/525) ([XAMPPRocky](https://github.com/XAMPPRocky))
- Add `StaticFilter` trait [\#515](https://github.com/googleforgames/quilkin/pull/515) ([XAMPPRocky](https://github.com/XAMPPRocky))

**Implemented enhancements:**

- Variable substitution with mdbook: point release docs at a release branch [\#609](https://github.com/googleforgames/quilkin/issues/609)
- Include image built in PR on CI Bot Result [\#593](https://github.com/googleforgames/quilkin/issues/593)
- Readiness probe based on number of endpoints [\#590](https://github.com/googleforgames/quilkin/issues/590)
- Docker image entrypoint should just be "/quilkin" [\#583](https://github.com/googleforgames/quilkin/issues/583)
- Configure basic configuration values with command flags [\#572](https://github.com/googleforgames/quilkin/issues/572)
- Be able to configure tracing/log levels [\#541](https://github.com/googleforgames/quilkin/issues/541)
- Use reflectors for Agones provider API [\#532](https://github.com/googleforgames/quilkin/issues/532)
- Add Metrics and logging to the xDS server [\#522](https://github.com/googleforgames/quilkin/issues/522)
- Make `config::Config` compatible with xDS manager [\#520](https://github.com/googleforgames/quilkin/issues/520)
- Add admin server for xDS management server [\#519](https://github.com/googleforgames/quilkin/issues/519)
- e2e tests for Agones integration [\#510](https://github.com/googleforgames/quilkin/issues/510)
- Switch to using `serde\_json::Value` internally over `serde\_yaml::Value` [\#507](https://github.com/googleforgames/quilkin/issues/507)
- impl `prost::Message` for filter configuration directly. [\#505](https://github.com/googleforgames/quilkin/issues/505)
- xDS: Support any filter [\#486](https://github.com/googleforgames/quilkin/issues/486)
- More in-depth network metrics about clients \(IPv4 and IPv6\) [\#450](https://github.com/googleforgames/quilkin/issues/450)
- Replace listen distributor task with multithreaded `SO\_REUSEPORT` task. [\#410](https://github.com/googleforgames/quilkin/issues/410)
- xDS Example [\#233](https://github.com/googleforgames/quilkin/issues/233)
- Total review of guide [\#645](https://github.com/googleforgames/quilkin/pull/645) ([markmandel](https://github.com/markmandel))
- Build macos binaries on ARM [\#636](https://github.com/googleforgames/quilkin/pull/636) ([markmandel](https://github.com/markmandel))
-  mdbook-variables for Documentation [\#629](https://github.com/googleforgames/quilkin/pull/629) ([markmandel](https://github.com/markmandel))
- Add timestamp filter [\#627](https://github.com/googleforgames/quilkin/pull/627) ([XAMPPRocky](https://github.com/XAMPPRocky))
- Added ASN and Prefix labels to active sessions [\#621](https://github.com/googleforgames/quilkin/pull/621) ([XAMPPRocky](https://github.com/XAMPPRocky))
- Agones xDS Provider example [\#618](https://github.com/googleforgames/quilkin/pull/618) ([markmandel](https://github.com/markmandel))
- \(Mostly\) Build tools working on arm64 [\#612](https://github.com/googleforgames/quilkin/pull/612) ([markmandel](https://github.com/markmandel))
- Docker image entrypoint is now `/quilkin` [\#607](https://github.com/googleforgames/quilkin/pull/607) ([markmandel](https://github.com/markmandel))
- Add ASN maxmind database integration [\#604](https://github.com/googleforgames/quilkin/pull/604) ([XAMPPRocky](https://github.com/XAMPPRocky))
- Add Build Images to CI results [\#599](https://github.com/googleforgames/quilkin/pull/599) ([markmandel](https://github.com/markmandel))
- Add ready probe endpoint [\#591](https://github.com/googleforgames/quilkin/pull/591) ([XAMPPRocky](https://github.com/XAMPPRocky))
- Agones + Quilkin xDS integration test. [\#587](https://github.com/googleforgames/quilkin/pull/587) ([markmandel](https://github.com/markmandel))
- Agones GameServer + Quilkin sidecar test [\#582](https://github.com/googleforgames/quilkin/pull/582) ([markmandel](https://github.com/markmandel))
- Basic Agones GameServer integration test [\#580](https://github.com/googleforgames/quilkin/pull/580) ([markmandel](https://github.com/markmandel))
- Add arguments to `quilkin run` [\#574](https://github.com/googleforgames/quilkin/pull/574) ([XAMPPRocky](https://github.com/XAMPPRocky))
- Add BUILD\_LOCAL to build tools [\#565](https://github.com/googleforgames/quilkin/pull/565) ([markmandel](https://github.com/markmandel))
- Clarify packet direction in traces, use base64 for bytes\_to\_string [\#563](https://github.com/googleforgames/quilkin/pull/563) ([XAMPPRocky](https://github.com/XAMPPRocky))
- Improve build-image incremental workflow [\#561](https://github.com/googleforgames/quilkin/pull/561) ([markmandel](https://github.com/markmandel))
- Implement xDS in Quilkin [\#552](https://github.com/googleforgames/quilkin/pull/552) ([XAMPPRocky](https://github.com/XAMPPRocky))
- Run Agones tests in CI [\#551](https://github.com/googleforgames/quilkin/pull/551) ([markmandel](https://github.com/markmandel))
- Dev: Build, Push and Test Agones Integrations [\#549](https://github.com/googleforgames/quilkin/pull/549) ([markmandel](https://github.com/markmandel))
- Tools for Agones e2e testing [\#545](https://github.com/googleforgames/quilkin/pull/545) ([markmandel](https://github.com/markmandel))
- Adding Erin to Cargo Authors. [\#544](https://github.com/googleforgames/quilkin/pull/544) ([markmandel](https://github.com/markmandel))
- Implemented Port reuse for downstream connection. [\#543](https://github.com/googleforgames/quilkin/pull/543) ([markmandel](https://github.com/markmandel))
- Test Utility to enable tracing logging. [\#537](https://github.com/googleforgames/quilkin/pull/537) ([markmandel](https://github.com/markmandel))
- Update xDS doc  [\#528](https://github.com/googleforgames/quilkin/pull/528) ([rezvaneh](https://github.com/rezvaneh))
- Metrics for Match Filter [\#511](https://github.com/googleforgames/quilkin/pull/511) ([markmandel](https://github.com/markmandel))
- Add basic alpha version of the UE4 plugin [\#485](https://github.com/googleforgames/quilkin/pull/485) ([XAMPPRocky](https://github.com/XAMPPRocky))

**Fixed bugs:**

- Fatal error from too old resource version [\#626](https://github.com/googleforgames/quilkin/issues/626)
- Get build tooling working on ARM/M1 Mac [\#608](https://github.com/googleforgames/quilkin/issues/608)
- `quilkin\_session\_rx\_bytes\_total` metric seems to not be working [\#605](https://github.com/googleforgames/quilkin/issues/605)
- \[Agones test\] create\_quilkin\_pod should test for more than running [\#597](https://github.com/googleforgames/quilkin/issues/597)
- \[Agones\] Removing/updating token annotation on a GameServer deoesn't update the Endpoint [\#589](https://github.com/googleforgames/quilkin/issues/589)
- Metrics having no data after refactors. [\#588](https://github.com/googleforgames/quilkin/issues/588)
- xDS: Locality leak on GameServer event handling [\#585](https://github.com/googleforgames/quilkin/issues/585)
- Running with Management Server doesn't exit on SIGTERM [\#575](https://github.com/googleforgames/quilkin/issues/575)
- Examples need updating to new config format [\#559](https://github.com/googleforgames/quilkin/issues/559)
- Cant build with `cross` [\#530](https://github.com/googleforgames/quilkin/issues/530)
- mdbook removing space indentation on code examples [\#503](https://github.com/googleforgames/quilkin/issues/503)
- Try to recover when the main process fails [\#634](https://github.com/googleforgames/quilkin/pull/634) ([XAMPPRocky](https://github.com/XAMPPRocky))
- Fixes for example Grafana dashboard [\#625](https://github.com/googleforgames/quilkin/pull/625) ([markmandel](https://github.com/markmandel))
- Fix not receiving stream pushes [\#624](https://github.com/googleforgames/quilkin/pull/624) ([XAMPPRocky](https://github.com/XAMPPRocky))
- Change admin flag to `no-admin`, respect shutdown signal [\#622](https://github.com/googleforgames/quilkin/pull/622) ([XAMPPRocky](https://github.com/XAMPPRocky))
- Docs, tests and bug fixes for /ready [\#602](https://github.com/googleforgames/quilkin/pull/602) ([markmandel](https://github.com/markmandel))
- Allow gameservers to not have port [\#598](https://github.com/googleforgames/quilkin/pull/598) ([XAMPPRocky](https://github.com/XAMPPRocky))
- Fix metrics namespace not always being present [\#596](https://github.com/googleforgames/quilkin/pull/596) ([XAMPPRocky](https://github.com/XAMPPRocky))
- Fix metric names in dashboard [\#595](https://github.com/googleforgames/quilkin/pull/595) ([XAMPPRocky](https://github.com/XAMPPRocky))
- Add LocalitySet type to ensure there aren't duplicate locality entries [\#592](https://github.com/googleforgames/quilkin/pull/592) ([XAMPPRocky](https://github.com/XAMPPRocky))
- Resubscribe to resources after disconnect, fix test flakiness [\#571](https://github.com/googleforgames/quilkin/pull/571) ([XAMPPRocky](https://github.com/XAMPPRocky))
- Catch tonic errors in xDS client loop [\#567](https://github.com/googleforgames/quilkin/pull/567) ([XAMPPRocky](https://github.com/XAMPPRocky))
- Recover xDS connection if stream terminates [\#566](https://github.com/googleforgames/quilkin/pull/566) ([XAMPPRocky](https://github.com/XAMPPRocky))
- Use status.ports rather spec.ports with Agones [\#562](https://github.com/googleforgames/quilkin/pull/562) ([XAMPPRocky](https://github.com/XAMPPRocky))
- Only search for config file when required [\#560](https://github.com/googleforgames/quilkin/pull/560) ([markmandel](https://github.com/markmandel))
- Remove cross from build tools [\#540](https://github.com/googleforgames/quilkin/pull/540) ([markmandel](https://github.com/markmandel))
- Update htmltest for test-docs [\#512](https://github.com/googleforgames/quilkin/pull/512) ([markmandel](https://github.com/markmandel))
- Multiple documentation fixes [\#504](https://github.com/googleforgames/quilkin/pull/504) ([markmandel](https://github.com/markmandel))

**Security fixes:**

- Fix security advisory in chrono [\#557](https://github.com/googleforgames/quilkin/pull/557) ([markmandel](https://github.com/markmandel))

**Closed issues:**

- Release 0.4.0 Requirements [\#546](https://github.com/googleforgames/quilkin/issues/546)
- Add guide documentation for UE4 Plugin [\#524](https://github.com/googleforgames/quilkin/issues/524)
- Add guide documentation for xDS server [\#523](https://github.com/googleforgames/quilkin/issues/523)
- Release 0.3.0 [\#499](https://github.com/googleforgames/quilkin/issues/499)
- Add metrics to `Match` filter. [\#453](https://github.com/googleforgames/quilkin/issues/453)
- Proposal: Cut 0.3.0 Release. [\#444](https://github.com/googleforgames/quilkin/issues/444)
- Message queues should drop when full [\#380](https://github.com/googleforgames/quilkin/issues/380)
- Regex Filter [\#316](https://github.com/googleforgames/quilkin/issues/316)
- Alternatives to proto submodules [\#169](https://github.com/googleforgames/quilkin/issues/169)
- Consider alternative formats for documenting config parameters  [\#149](https://github.com/googleforgames/quilkin/issues/149)

**Merged pull requests:**

- Release 0.4.0 [\#648](https://github.com/googleforgames/quilkin/pull/648) ([markmandel](https://github.com/markmandel))
- Update release instructions to remove `-debug` [\#646](https://github.com/googleforgames/quilkin/pull/646) ([markmandel](https://github.com/markmandel))
- Consistency of "packets\_dropped\_total" metric [\#641](https://github.com/googleforgames/quilkin/pull/641) ([markmandel](https://github.com/markmandel))
- Update to Rust 1.65.0 [\#640](https://github.com/googleforgames/quilkin/pull/640) ([XAMPPRocky](https://github.com/XAMPPRocky))
- Implement string interning and symbol resolution for metadata keys [\#638](https://github.com/googleforgames/quilkin/pull/638) ([XAMPPRocky](https://github.com/XAMPPRocky))
- Update yaml configuration documentation [\#635](https://github.com/googleforgames/quilkin/pull/635) ([markmandel](https://github.com/markmandel))
- Refactor metrics statics into free fns [\#633](https://github.com/googleforgames/quilkin/pull/633) ([XAMPPRocky](https://github.com/XAMPPRocky))
- Agones: Add token removal test. [\#631](https://github.com/googleforgames/quilkin/pull/631) ([markmandel](https://github.com/markmandel))
- Refactor filter model from Context-\>Response into ref mut Context [\#630](https://github.com/googleforgames/quilkin/pull/630) ([XAMPPRocky](https://github.com/XAMPPRocky))
- Connect Timestamp Filter to rest of documentation [\#628](https://github.com/googleforgames/quilkin/pull/628) ([markmandel](https://github.com/markmandel))
- Flatten proxy config section into root [\#623](https://github.com/googleforgames/quilkin/pull/623) ([XAMPPRocky](https://github.com/XAMPPRocky))
- Update dependencies [\#620](https://github.com/googleforgames/quilkin/pull/620) ([XAMPPRocky](https://github.com/XAMPPRocky))
- Remove duplicate metric [\#619](https://github.com/googleforgames/quilkin/pull/619) ([XAMPPRocky](https://github.com/XAMPPRocky))
- Sidecar Example README: Switch to relative paths [\#617](https://github.com/googleforgames/quilkin/pull/617) ([markmandel](https://github.com/markmandel))
- Use `TimedSizedCache` [\#616](https://github.com/googleforgames/quilkin/pull/616) ([XAMPPRocky](https://github.com/XAMPPRocky))
- Update CODEOWNERS [\#615](https://github.com/googleforgames/quilkin/pull/615) ([XAMPPRocky](https://github.com/XAMPPRocky))
- Update to Rust 1.64.0 [\#614](https://github.com/googleforgames/quilkin/pull/614) ([XAMPPRocky](https://github.com/XAMPPRocky))
- Refactor session metrics and errors [\#613](https://github.com/googleforgames/quilkin/pull/613) ([XAMPPRocky](https://github.com/XAMPPRocky))
- remove unwrap [\#606](https://github.com/googleforgames/quilkin/pull/606) ([XAMPPRocky](https://github.com/XAMPPRocky))
- Add short poll to xDS clients as workaround, improve xDS tracing [\#603](https://github.com/googleforgames/quilkin/pull/603) ([XAMPPRocky](https://github.com/XAMPPRocky))
- Remove allocated check for deleted gameserver events [\#594](https://github.com/googleforgames/quilkin/pull/594) ([XAMPPRocky](https://github.com/XAMPPRocky))
- Relax trait bounds for Default, Deserialize, and Serialize for Slot\<T\> [\#581](https://github.com/googleforgames/quilkin/pull/581) ([XAMPPRocky](https://github.com/XAMPPRocky))
- Move proxy::Server to Proxy and sessions::session to sessions [\#578](https://github.com/googleforgames/quilkin/pull/578) ([XAMPPRocky](https://github.com/XAMPPRocky))
- Update example configurations to new format [\#577](https://github.com/googleforgames/quilkin/pull/577) ([markmandel](https://github.com/markmandel))
- Update configuration reference docs [\#576](https://github.com/googleforgames/quilkin/pull/576) ([markmandel](https://github.com/markmandel))
- Refactor CLI organisation, and improve termination [\#570](https://github.com/googleforgames/quilkin/pull/570) ([XAMPPRocky](https://github.com/XAMPPRocky))
- Extend `fs::write` test delay, and remove admin server from test to prevent flakiness [\#569](https://github.com/googleforgames/quilkin/pull/569) ([XAMPPRocky](https://github.com/XAMPPRocky))
- Update to Rust 1.63.0 [\#568](https://github.com/googleforgames/quilkin/pull/568) ([XAMPPRocky](https://github.com/XAMPPRocky))
- Use base64 when logging token, add fmt::Display impl for metadata::Value [\#554](https://github.com/googleforgames/quilkin/pull/554) ([XAMPPRocky](https://github.com/XAMPPRocky))
- Cleanup of licences and advisories [\#550](https://github.com/googleforgames/quilkin/pull/550) ([markmandel](https://github.com/markmandel))
- Delete Go based xDS [\#548](https://github.com/googleforgames/quilkin/pull/548) ([markmandel](https://github.com/markmandel))
- build/Makefile - convert spaces to tabs [\#547](https://github.com/googleforgames/quilkin/pull/547) ([markmandel](https://github.com/markmandel))
- General testing cleanup [\#539](https://github.com/googleforgames/quilkin/pull/539) ([markmandel](https://github.com/markmandel))
- `connect\(\)` Sessions socket to Endpoint address [\#538](https://github.com/googleforgames/quilkin/pull/538) ([markmandel](https://github.com/markmandel))
- Upgrade Tokio to 1.19.2 [\#536](https://github.com/googleforgames/quilkin/pull/536) ([markmandel](https://github.com/markmandel))
- Update to Rust 1.61.0 [\#535](https://github.com/googleforgames/quilkin/pull/535) ([XAMPPRocky](https://github.com/XAMPPRocky))
- Update link to DiscoveryRequest [\#533](https://github.com/googleforgames/quilkin/pull/533) ([XAMPPRocky](https://github.com/XAMPPRocky))
- Update to Rust 1.60 [\#526](https://github.com/googleforgames/quilkin/pull/526) ([XAMPPRocky](https://github.com/XAMPPRocky))
- Replace serde\_yaml with serde\_json internally [\#518](https://github.com/googleforgames/quilkin/pull/518) ([rezvaneh](https://github.com/rezvaneh))
- Use htmltest binary not build from source [\#516](https://github.com/googleforgames/quilkin/pull/516) ([markmandel](https://github.com/markmandel))
- Add conversion to protobuf for most core types [\#514](https://github.com/googleforgames/quilkin/pull/514) ([XAMPPRocky](https://github.com/XAMPPRocky))
- Update xDS APIs [\#502](https://github.com/googleforgames/quilkin/pull/502) ([XAMPPRocky](https://github.com/XAMPPRocky))
- Preparation for 0.4.0 [\#501](https://github.com/googleforgames/quilkin/pull/501) ([markmandel](https://github.com/markmandel))
- Refactor FilterManager into SharedFilterChain [\#491](https://github.com/googleforgames/quilkin/pull/491) ([XAMPPRocky](https://github.com/XAMPPRocky))
- Refactor Metrics Registry into public static [\#490](https://github.com/googleforgames/quilkin/pull/490) ([XAMPPRocky](https://github.com/XAMPPRocky))
- Refactor FilterRegistry into private static [\#489](https://github.com/googleforgames/quilkin/pull/489) ([XAMPPRocky](https://github.com/XAMPPRocky))
- Add `Debug` for `FilterChain` and `FilterManager::with\_dynamic` [\#487](https://github.com/googleforgames/quilkin/pull/487) ([XAMPPRocky](https://github.com/XAMPPRocky))

## [v0.3.0](https://github.com/googleforgames/quilkin/tree/v0.3.0) (2022-03-02)

[Full Changelog](https://github.com/googleforgames/quilkin/compare/v0.2.0...v0.3.0)

**Breaking changes:**

- Remove `extensions` from filter identifiers [\#484](https://github.com/googleforgames/quilkin/pull/484) ([XAMPPRocky](https://github.com/XAMPPRocky))
- Add auto-generation of JSON schema. [\#478](https://github.com/googleforgames/quilkin/pull/478) ([XAMPPRocky](https://github.com/XAMPPRocky))
- Add regex capture and rename capture\_bytes to capture [\#458](https://github.com/googleforgames/quilkin/pull/458) ([XAMPPRocky](https://github.com/XAMPPRocky))
- Add matches filter [\#447](https://github.com/googleforgames/quilkin/pull/447) ([XAMPPRocky](https://github.com/XAMPPRocky))
- Add `metadata::Value` [\#436](https://github.com/googleforgames/quilkin/pull/436) ([XAMPPRocky](https://github.com/XAMPPRocky))
- Add a ttl map and support ratelimiting per IP address [\#406](https://github.com/googleforgames/quilkin/pull/406) ([iffyio](https://github.com/iffyio))

**Implemented enhancements:**

- Upgrade to Tokio 1.16.0 [\#475](https://github.com/googleforgames/quilkin/issues/475)
- Changing dynamic metadata from `Any`. [\#433](https://github.com/googleforgames/quilkin/issues/433)
- Provide stack traces for runtime errors. [\#418](https://github.com/googleforgames/quilkin/issues/418)
- Accept Docker service hostnames in addition to IP addresses in configuration. [\#415](https://github.com/googleforgames/quilkin/issues/415)
- Limit LocalRateLimit per IP [\#405](https://github.com/googleforgames/quilkin/issues/405)
- Add support for version based packet processing [\#401](https://github.com/googleforgames/quilkin/issues/401)
- Add abstraction for filter state lookup [\#375](https://github.com/googleforgames/quilkin/issues/375)
- CI: link checking on documentation [\#367](https://github.com/googleforgames/quilkin/issues/367)
- Allowlist filter [\#343](https://github.com/googleforgames/quilkin/issues/343)
- Metric: Total Packet Processing time [\#292](https://github.com/googleforgames/quilkin/issues/292)
- Add an ID field to filters [\#174](https://github.com/googleforgames/quilkin/issues/174)
- Blocklist Filter [\#158](https://github.com/googleforgames/quilkin/issues/158)
- Implement a control plane [\#131](https://github.com/googleforgames/quilkin/issues/131)
- Filter Idea: Rate limiting [\#5](https://github.com/googleforgames/quilkin/issues/5)
- Benchmark comparing read and write throughput [\#479](https://github.com/googleforgames/quilkin/pull/479) ([markmandel](https://github.com/markmandel))
- Update Rust to 1.58.1 [\#473](https://github.com/googleforgames/quilkin/pull/473) ([XAMPPRocky](https://github.com/XAMPPRocky))
- Add Pass & Drop filters, Refactor `Match` config [\#472](https://github.com/googleforgames/quilkin/pull/472) ([XAMPPRocky](https://github.com/XAMPPRocky))
- management-server: update metrics and doc [\#469](https://github.com/googleforgames/quilkin/pull/469) ([iffyio](https://github.com/iffyio))
- Example Grafana Graph for Packet Processing Times [\#452](https://github.com/googleforgames/quilkin/pull/452) ([markmandel](https://github.com/markmandel))
- CI: Documentation test gen, links, images, etc. [\#449](https://github.com/googleforgames/quilkin/pull/449) ([markmandel](https://github.com/markmandel))
- Metric: Total Packet processing time [\#441](https://github.com/googleforgames/quilkin/pull/441) ([markmandel](https://github.com/markmandel))
- xds-server updates [\#437](https://github.com/googleforgames/quilkin/pull/437) ([iffyio](https://github.com/iffyio))
- Add "Rust Doc" link to each Filter [\#434](https://github.com/googleforgames/quilkin/pull/434) ([markmandel](https://github.com/markmandel))
- Documentation for Firewall filter [\#432](https://github.com/googleforgames/quilkin/pull/432) ([markmandel](https://github.com/markmandel))
- Code: Firewall filter [\#416](https://github.com/googleforgames/quilkin/pull/416) ([markmandel](https://github.com/markmandel))
- Support clear bot history on new build [\#390](https://github.com/googleforgames/quilkin/pull/390) ([fredagsfys](https://github.com/fredagsfys))
- replace slog with tracing in Filter [\#385](https://github.com/googleforgames/quilkin/pull/385) ([rezvaneh](https://github.com/rezvaneh))
- XDS Management Server [\#360](https://github.com/googleforgames/quilkin/pull/360) ([iffyio](https://github.com/iffyio))

**Fixed bugs:**

- XDS backoff is broken [\#461](https://github.com/googleforgames/quilkin/issues/461)
- 404 on FAQ link in main branch docs. [\#443](https://github.com/googleforgames/quilkin/issues/443)
- Fix exponential backoff on xds client retry [\#465](https://github.com/googleforgames/quilkin/pull/465) ([markmandel](https://github.com/markmandel))
- archive\_dependencies.sh: Handle no dependencies [\#463](https://github.com/googleforgames/quilkin/pull/463) ([markmandel](https://github.com/markmandel))
- xds: delete snapshot for disconnected proxies [\#462](https://github.com/googleforgames/quilkin/pull/462) ([iffyio](https://github.com/iffyio))
- Docs: Fix broken links [\#445](https://github.com/googleforgames/quilkin/pull/445) ([markmandel](https://github.com/markmandel))
- Fix bug archiving dependencies [\#442](https://github.com/googleforgames/quilkin/pull/442) ([markmandel](https://github.com/markmandel))
- Save iperf3 metrics.json in /quilkin [\#438](https://github.com/googleforgames/quilkin/pull/438) ([markmandel](https://github.com/markmandel))
- Fixes for filter documentation [\#431](https://github.com/googleforgames/quilkin/pull/431) ([markmandel](https://github.com/markmandel))

**Security fixes:**

- Upgrade Tokio for RUSTSEC-2021-0124 [\#439](https://github.com/googleforgames/quilkin/pull/439) ([markmandel](https://github.com/markmandel))

**Closed issues:**

- Rename `Matches` to `Match`. [\#454](https://github.com/googleforgames/quilkin/issues/454)
- Refactor to source/destination rather than to/from [\#448](https://github.com/googleforgames/quilkin/issues/448)
- lack of example of Client Proxy to Separate Server Proxies Pools [\#403](https://github.com/googleforgames/quilkin/issues/403)
- Release 0.2.0 [\#398](https://github.com/googleforgames/quilkin/issues/398)
- Move from `slog` to `tracing` [\#317](https://github.com/googleforgames/quilkin/issues/317)
- Filter Naming Scheme [\#291](https://github.com/googleforgames/quilkin/issues/291)
- \[bot\] Hide Old Comment On Push [\#266](https://github.com/googleforgames/quilkin/issues/266)
- Performance Testing [\#14](https://github.com/googleforgames/quilkin/issues/14)

**Merged pull requests:**

- Release 0.3.0 [\#500](https://github.com/googleforgames/quilkin/pull/500) ([markmandel](https://github.com/markmandel))
- Update CODEOWNERS [\#498](https://github.com/googleforgames/quilkin/pull/498) ([XAMPPRocky](https://github.com/XAMPPRocky))
- Update dependencies [\#497](https://github.com/googleforgames/quilkin/pull/497) ([XAMPPRocky](https://github.com/XAMPPRocky))
- Update to Rust 1.59 [\#496](https://github.com/googleforgames/quilkin/pull/496) ([XAMPPRocky](https://github.com/XAMPPRocky))
- Update xds.md [\#495](https://github.com/googleforgames/quilkin/pull/495) ([XAMPPRocky](https://github.com/XAMPPRocky))
- Refactor ClusterManager into SharedCluster [\#493](https://github.com/googleforgames/quilkin/pull/493) ([XAMPPRocky](https://github.com/XAMPPRocky))
- Increase Cloud Build timeout to 2h [\#492](https://github.com/googleforgames/quilkin/pull/492) ([markmandel](https://github.com/markmandel))
- Update clap to 3.1 [\#488](https://github.com/googleforgames/quilkin/pull/488) ([XAMPPRocky](https://github.com/XAMPPRocky))
- Update CODEOWNERS [\#477](https://github.com/googleforgames/quilkin/pull/477) ([iffyio](https://github.com/iffyio))
- Update dependencies [\#476](https://github.com/googleforgames/quilkin/pull/476) ([XAMPPRocky](https://github.com/XAMPPRocky))
- Fix 404 in docs [\#471](https://github.com/googleforgames/quilkin/pull/471) ([markmandel](https://github.com/markmandel))
- Add link to xds sample from xDS guide [\#470](https://github.com/googleforgames/quilkin/pull/470) ([markmandel](https://github.com/markmandel))
- Consistent implementation of source/dest [\#468](https://github.com/googleforgames/quilkin/pull/468) ([markmandel](https://github.com/markmandel))
- Remove src/filters/extensions [\#467](https://github.com/googleforgames/quilkin/pull/467) ([markmandel](https://github.com/markmandel))
- Remove `slog-json` from archive\_dependencies.sh [\#466](https://github.com/googleforgames/quilkin/pull/466) ([markmandel](https://github.com/markmandel))
- Document Filter naming conventions [\#464](https://github.com/googleforgames/quilkin/pull/464) ([markmandel](https://github.com/markmandel))
- Completely remove `slog` and replace with `tracing` [\#457](https://github.com/googleforgames/quilkin/pull/457) ([XAMPPRocky](https://github.com/XAMPPRocky))
- Update to Rust 1.57 [\#455](https://github.com/googleforgames/quilkin/pull/455) ([XAMPPRocky](https://github.com/XAMPPRocky))
- Fix licence field in macros Cargo.toml [\#451](https://github.com/googleforgames/quilkin/pull/451) ([markmandel](https://github.com/markmandel))
- xds-server: Create informers before starting informer factory [\#446](https://github.com/googleforgames/quilkin/pull/446) ([cheahjs](https://github.com/cheahjs))
- Align Filter Chain metrics with conventions [\#440](https://github.com/googleforgames/quilkin/pull/440) ([markmandel](https://github.com/markmandel))
- Move xds-management-server to the top-level [\#435](https://github.com/googleforgames/quilkin/pull/435) ([XAMPPRocky](https://github.com/XAMPPRocky))
- Refactor AdsClient and switch backoff with tryhard [\#430](https://github.com/googleforgames/quilkin/pull/430) ([XAMPPRocky](https://github.com/XAMPPRocky))
- Remove reqwest from dev-dependencies [\#429](https://github.com/googleforgames/quilkin/pull/429) ([XAMPPRocky](https://github.com/XAMPPRocky))
- Update to Rust 1.56 and 2021 edition [\#428](https://github.com/googleforgames/quilkin/pull/428) ([XAMPPRocky](https://github.com/XAMPPRocky))
- Add RUSTSEC-2020-0159 to ignore list [\#425](https://github.com/googleforgames/quilkin/pull/425) ([XAMPPRocky](https://github.com/XAMPPRocky))
- Update dependencies [\#424](https://github.com/googleforgames/quilkin/pull/424) ([XAMPPRocky](https://github.com/XAMPPRocky))
- Add eyre for error reporting [\#423](https://github.com/googleforgames/quilkin/pull/423) ([XAMPPRocky](https://github.com/XAMPPRocky))
- Move iPerf benchmark to use Docker [\#421](https://github.com/googleforgames/quilkin/pull/421) ([XAMPPRocky](https://github.com/XAMPPRocky))
- Replace `SocketAddr` with `EndpointAddress` [\#419](https://github.com/googleforgames/quilkin/pull/419) ([XAMPPRocky](https://github.com/XAMPPRocky))
- Update prometheus to 0.13 [\#414](https://github.com/googleforgames/quilkin/pull/414) ([XAMPPRocky](https://github.com/XAMPPRocky))
- Update to Rust 1.55.0 [\#413](https://github.com/googleforgames/quilkin/pull/413) ([XAMPPRocky](https://github.com/XAMPPRocky))
- Preparation for 0.3.0 [\#402](https://github.com/googleforgames/quilkin/pull/402) ([markmandel](https://github.com/markmandel))
- GitHub: Issue and PR templates [\#400](https://github.com/googleforgames/quilkin/pull/400) ([markmandel](https://github.com/markmandel))

## [v0.2.0](https://github.com/googleforgames/quilkin/tree/v0.2.0) (2021-09-22)

[Full Changelog](https://github.com/googleforgames/quilkin/compare/v0.1.0...v0.2.0)

**Breaking changes:**

- Add a /config endpoint [\#396](https://github.com/googleforgames/quilkin/pull/396) ([iffyio](https://github.com/iffyio))
- Use hostname as default proxy id [\#391](https://github.com/googleforgames/quilkin/pull/391) ([iffyio](https://github.com/iffyio))
- Move to run and test subcommand setup [\#369](https://github.com/googleforgames/quilkin/pull/369) ([XAMPPRocky](https://github.com/XAMPPRocky))
- Add a type for session keys [\#364](https://github.com/googleforgames/quilkin/pull/364) ([iffyio](https://github.com/iffyio))
- Refactor Endpoint and Metadata from cluster and config into one type. [\#358](https://github.com/googleforgames/quilkin/pull/358) ([XAMPPRocky](https://github.com/XAMPPRocky))
- Move I/O and configuration out of runner::run [\#350](https://github.com/googleforgames/quilkin/pull/350) ([XAMPPRocky](https://github.com/XAMPPRocky))
- Filter Extension Re-organisation [\#293](https://github.com/googleforgames/quilkin/pull/293) ([XAMPPRocky](https://github.com/XAMPPRocky))

**Implemented enhancements:**

- Make a quilkin crate [\#232](https://github.com/googleforgames/quilkin/issues/232)
- Always backoff and retry management server [\#392](https://github.com/googleforgames/quilkin/pull/392) ([iffyio](https://github.com/iffyio))
- Hash load balancer [\#381](https://github.com/googleforgames/quilkin/pull/381) ([gilesheron](https://github.com/gilesheron))
- CI: Add cross compile and basic image test [\#370](https://github.com/googleforgames/quilkin/pull/370) ([markmandel](https://github.com/markmandel))
- `make docs` to preview documentation locally [\#366](https://github.com/googleforgames/quilkin/pull/366) ([markmandel](https://github.com/markmandel))
- Add "examples" page to the documentation. [\#362](https://github.com/googleforgames/quilkin/pull/362) ([markmandel](https://github.com/markmandel))
- GH Action: Sizing labels on PRs [\#353](https://github.com/googleforgames/quilkin/pull/353) ([markmandel](https://github.com/markmandel))
- Add `cargo-deny` to CI by adding to `make test` [\#340](https://github.com/googleforgames/quilkin/pull/340) ([markmandel](https://github.com/markmandel))
- Add a companies using Quilkin section [\#335](https://github.com/googleforgames/quilkin/pull/335) ([luna-duclos](https://github.com/luna-duclos))
- Add naive benchmark [\#321](https://github.com/googleforgames/quilkin/pull/321) ([XAMPPRocky](https://github.com/XAMPPRocky))

**Fixed bugs:**

- Fix duplicate metric registration for filters [\#397](https://github.com/googleforgames/quilkin/pull/397) ([iffyio](https://github.com/iffyio))
- Include endpoint token and metadata from cluster update [\#359](https://github.com/googleforgames/quilkin/pull/359) ([iffyio](https://github.com/iffyio))
- Fix for change: --filename to --config [\#355](https://github.com/googleforgames/quilkin/pull/355) ([markmandel](https://github.com/markmandel))

**Security fixes:**

- Security: Updated tokio + prost-types dependencies [\#341](https://github.com/googleforgames/quilkin/pull/341) ([markmandel](https://github.com/markmandel))

**Closed issues:**

- Add admin endpoints to introspect proxy config [\#394](https://github.com/googleforgames/quilkin/issues/394)
- Review Docs: Writing Custom Filters for API changes [\#373](https://github.com/googleforgames/quilkin/issues/373)
- Create monthly community meeting [\#372](https://github.com/googleforgames/quilkin/issues/372)
- Build: `make docs` to preview documentation locally [\#365](https://github.com/googleforgames/quilkin/issues/365)
- Add Cargo Deny to `make test` [\#327](https://github.com/googleforgames/quilkin/issues/327)
- Release 0.1.0 [\#325](https://github.com/googleforgames/quilkin/issues/325)
- 0.1.0 Release Schedule [\#313](https://github.com/googleforgames/quilkin/issues/313)
- Refactor Filter Module Structure [\#280](https://github.com/googleforgames/quilkin/issues/280)
- review metrics to prevent high-cardinality data [\#263](https://github.com/googleforgames/quilkin/issues/263)

**Merged pull requests:**

- Release 0.2.0 [\#399](https://github.com/googleforgames/quilkin/pull/399) ([markmandel](https://github.com/markmandel))
- Docs: Updated Custom Filters [\#395](https://github.com/googleforgames/quilkin/pull/395) ([markmandel](https://github.com/markmandel))
- Exit on SIGTERM [\#393](https://github.com/googleforgames/quilkin/pull/393) ([iffyio](https://github.com/iffyio))
- Update custom filter example and add CI [\#389](https://github.com/googleforgames/quilkin/pull/389) ([markmandel](https://github.com/markmandel))
- Add github CODEOWNERS file [\#388](https://github.com/googleforgames/quilkin/pull/388) ([iffyio](https://github.com/iffyio))
- Remove test subcommand from clap for 0.2 [\#386](https://github.com/googleforgames/quilkin/pull/386) ([XAMPPRocky](https://github.com/XAMPPRocky))
- README: Add community meetings [\#377](https://github.com/googleforgames/quilkin/pull/377) ([markmandel](https://github.com/markmandel))
- Fix 404: Writing Custom Filters [\#376](https://github.com/googleforgames/quilkin/pull/376) ([markmandel](https://github.com/markmandel))
- Delete filter\_registry.rs [\#368](https://github.com/googleforgames/quilkin/pull/368) ([XAMPPRocky](https://github.com/XAMPPRocky))
- Update doc links [\#363](https://github.com/googleforgames/quilkin/pull/363) ([iffyio](https://github.com/iffyio))
- Better language on using.md [\#361](https://github.com/googleforgames/quilkin/pull/361) ([markmandel](https://github.com/markmandel))
- Remove redundant cfg\(test\) [\#357](https://github.com/googleforgames/quilkin/pull/357) ([XAMPPRocky](https://github.com/XAMPPRocky))
- Fix README.md development documentation links [\#356](https://github.com/googleforgames/quilkin/pull/356) ([markmandel](https://github.com/markmandel))
- Remove initial xds update delay [\#354](https://github.com/googleforgames/quilkin/pull/354) ([iffyio](https://github.com/iffyio))
- Remove need for nightly in CI and developer guide [\#352](https://github.com/googleforgames/quilkin/pull/352) ([markmandel](https://github.com/markmandel))
- Update to Rust 1.54.0 [\#351](https://github.com/googleforgames/quilkin/pull/351) ([XAMPPRocky](https://github.com/XAMPPRocky))
- Documentation snapshot/release README.md section [\#348](https://github.com/googleforgames/quilkin/pull/348) ([markmandel](https://github.com/markmandel))
- Oops, most of the badges didn't have links [\#342](https://github.com/googleforgames/quilkin/pull/342) ([markmandel](https://github.com/markmandel))
- Make licence explicit in Cargo.toml [\#338](https://github.com/googleforgames/quilkin/pull/338) ([markmandel](https://github.com/markmandel))
- Fixed README.md typo: where -\> were [\#337](https://github.com/googleforgames/quilkin/pull/337) ([moppius](https://github.com/moppius))
- Add announcements to the README [\#336](https://github.com/googleforgames/quilkin/pull/336) ([markmandel](https://github.com/markmandel))
- Move packet buffer to heap [\#334](https://github.com/googleforgames/quilkin/pull/334) ([iffyio](https://github.com/iffyio))
- Update BRANDING.md [\#333](https://github.com/googleforgames/quilkin/pull/333) ([thisisnotapril](https://github.com/thisisnotapril))
- Create BRANDING.md [\#332](https://github.com/googleforgames/quilkin/pull/332) ([thisisnotapril](https://github.com/thisisnotapril))
- Fix link to twitter page [\#331](https://github.com/googleforgames/quilkin/pull/331) ([markmandel](https://github.com/markmandel))
- Preparation for 0.2.0 [\#329](https://github.com/googleforgames/quilkin/pull/329) ([markmandel](https://github.com/markmandel))
- Add mdbook and GitHub pages deployment [\#319](https://github.com/googleforgames/quilkin/pull/319) ([XAMPPRocky](https://github.com/XAMPPRocky))
- Cleanup other parts of the public API. [\#308](https://github.com/googleforgames/quilkin/pull/308) ([XAMPPRocky](https://github.com/XAMPPRocky))

## [v0.1.0](https://github.com/googleforgames/quilkin/tree/v0.1.0) (2021-07-09)

[Full Changelog](https://github.com/googleforgames/quilkin/compare/d60c41ce257c77a4daafb5ab645536f8e1f2aa14...v0.1.0)

**Implemented enhancements:**

- Regular updates to Nightly for CI build step [\#287](https://github.com/googleforgames/quilkin/issues/287)
- Convention: quilkin.yaml file [\#270](https://github.com/googleforgames/quilkin/issues/270)
- Write a github PR comment notifier for cloudbuilds [\#259](https://github.com/googleforgames/quilkin/issues/259)
- Idea: Proc macro for creating filters/filter factories [\#248](https://github.com/googleforgames/quilkin/issues/248)
- Add cargo-deny config [\#239](https://github.com/googleforgames/quilkin/issues/239)
- Example: Grafana dashboard\(s\) [\#234](https://github.com/googleforgames/quilkin/issues/234)
- Examples for Alpha Launch [\#228](https://github.com/googleforgames/quilkin/issues/228)
- Create release process + checklist [\#213](https://github.com/googleforgames/quilkin/issues/213)
- Create linux docker images on release [\#201](https://github.com/googleforgames/quilkin/issues/201)
- Metrics: Packet processing time [\#167](https://github.com/googleforgames/quilkin/issues/167)
- Make metric port configurable [\#101](https://github.com/googleforgames/quilkin/issues/101)
- Add support to invoke filter chain in reverse order [\#92](https://github.com/googleforgames/quilkin/issues/92)
- Version configuration [\#77](https://github.com/googleforgames/quilkin/issues/77)
- FilterChain for each Session [\#75](https://github.com/googleforgames/quilkin/issues/75)
- \[Alpha Release\] Let's write some documentation [\#62](https://github.com/googleforgames/quilkin/issues/62)
- Cross Compilation [\#19](https://github.com/googleforgames/quilkin/issues/19)
- Expose Metrics [\#11](https://github.com/googleforgames/quilkin/issues/11)
- gRPC configuration management control plane API [\#10](https://github.com/googleforgames/quilkin/issues/10)
- Filter idea: Simple routing with every packet having the connection id appended [\#8](https://github.com/googleforgames/quilkin/issues/8)
- Basic non-transparent UDP proxying from Receiver to all endpoints [\#7](https://github.com/googleforgames/quilkin/issues/7)
- Basic non-transparent UDP proxying from Sender to Receiver [\#6](https://github.com/googleforgames/quilkin/issues/6)
- \[Client\] LB across multiple endpoints? [\#3](https://github.com/googleforgames/quilkin/issues/3)
- Design and Implementation of Filters [\#1](https://github.com/googleforgames/quilkin/issues/1)
- Make + Docker toolchain for development [\#303](https://github.com/googleforgames/quilkin/pull/303) ([markmandel](https://github.com/markmandel))
- Example Grafana dashboard for Quilkin Metrics [\#300](https://github.com/googleforgames/quilkin/pull/300) ([markmandel](https://github.com/markmandel))
- Quilkin logo and Quilly the mascot [\#289](https://github.com/googleforgames/quilkin/pull/289) ([markmandel](https://github.com/markmandel))
- Add rust-toolchain.toml to project [\#249](https://github.com/googleforgames/quilkin/pull/249) ([markmandel](https://github.com/markmandel))
- Quickstart for Agones + Quilkin [\#235](https://github.com/googleforgames/quilkin/pull/235) ([markmandel](https://github.com/markmandel))
- Quickstart using netcat [\#231](https://github.com/googleforgames/quilkin/pull/231) ([markmandel](https://github.com/markmandel))
- Usage documentation [\#230](https://github.com/googleforgames/quilkin/pull/230) ([markmandel](https://github.com/markmandel))
- Agones and Xonotic Examples [\#229](https://github.com/googleforgames/quilkin/pull/229) ([markmandel](https://github.com/markmandel))
- Release checklist [\#226](https://github.com/googleforgames/quilkin/pull/226) ([markmandel](https://github.com/markmandel))
- Create a CHANGELOG.md on release [\#225](https://github.com/googleforgames/quilkin/pull/225) ([markmandel](https://github.com/markmandel))
- Add /live health endpoint to admin server [\#221](https://github.com/googleforgames/quilkin/pull/221) ([markmandel](https://github.com/markmandel))
- Release and Debug image release pipeline [\#214](https://github.com/googleforgames/quilkin/pull/214) ([markmandel](https://github.com/markmandel))
- Build Windows & Linux Binaries in Cloud Build [\#209](https://github.com/googleforgames/quilkin/pull/209) ([markmandel](https://github.com/markmandel))
- Document xDS, filter/endpoint metadata [\#203](https://github.com/googleforgames/quilkin/pull/203) ([iffyio](https://github.com/iffyio))
- Implementation and test of Filter ordering [\#198](https://github.com/googleforgames/quilkin/pull/198) ([markmandel](https://github.com/markmandel))
- Add xDS metrics [\#195](https://github.com/googleforgames/quilkin/pull/195) ([iffyio](https://github.com/iffyio))
- on\_read & on-write config for ConcatenateBytes  [\#186](https://github.com/googleforgames/quilkin/pull/186) ([markmandel](https://github.com/markmandel))
- Documentation for the Compress filter [\#181](https://github.com/googleforgames/quilkin/pull/181) ([markmandel](https://github.com/markmandel))
- Add XDS Filter implementation [\#171](https://github.com/googleforgames/quilkin/pull/171) ([iffyio](https://github.com/iffyio))
- Implementation of Compression Filter. [\#162](https://github.com/googleforgames/quilkin/pull/162) ([markmandel](https://github.com/markmandel))
- xds fail fast on invalid URI [\#161](https://github.com/googleforgames/quilkin/pull/161) ([iffyio](https://github.com/iffyio))
- Endpoint metadata support from static config [\#160](https://github.com/googleforgames/quilkin/pull/160) ([iffyio](https://github.com/iffyio))
- Generate proto files for XDS filter configs [\#159](https://github.com/googleforgames/quilkin/pull/159) ([iffyio](https://github.com/iffyio))
- Include metadata for XDS endpoints [\#154](https://github.com/googleforgames/quilkin/pull/154) ([iffyio](https://github.com/iffyio))
- Add dynamic proxy configuration [\#150](https://github.com/googleforgames/quilkin/pull/150) ([iffyio](https://github.com/iffyio))
- Add session metrics to docs [\#143](https://github.com/googleforgames/quilkin/pull/143) ([iffyio](https://github.com/iffyio))
- Add example integration architecture documentation [\#139](https://github.com/googleforgames/quilkin/pull/139) ([markmandel](https://github.com/markmandel))
- Add an overview to the documentation [\#137](https://github.com/googleforgames/quilkin/pull/137) ([markmandel](https://github.com/markmandel))
- EndpointAuthentication filter [\#135](https://github.com/googleforgames/quilkin/pull/135) ([markmandel](https://github.com/markmandel))
- Add a ClusterManager abstraction [\#128](https://github.com/googleforgames/quilkin/pull/128) ([iffyio](https://github.com/iffyio))
- Authentication Token Capture Filter [\#118](https://github.com/googleforgames/quilkin/pull/118) ([markmandel](https://github.com/markmandel))
- Add XDS client [\#115](https://github.com/googleforgames/quilkin/pull/115) ([iffyio](https://github.com/iffyio))
- Add XDS logic for cluster and endpoints [\#112](https://github.com/googleforgames/quilkin/pull/112) ([iffyio](https://github.com/iffyio))
- Concat Byte Filter [\#111](https://github.com/googleforgames/quilkin/pull/111) ([markmandel](https://github.com/markmandel))
- Add xds proto code generation [\#108](https://github.com/googleforgames/quilkin/pull/108) ([iffyio](https://github.com/iffyio))
- Be able to pass data between Filters [\#107](https://github.com/googleforgames/quilkin/pull/107) ([markmandel](https://github.com/markmandel))
- Add test helper to manage test resources [\#106](https://github.com/googleforgames/quilkin/pull/106) ([iffyio](https://github.com/iffyio))
- Add GRPC proto submodules [\#105](https://github.com/googleforgames/quilkin/pull/105) ([iffyio](https://github.com/iffyio))
- Added ConnectionConfig to CreateFilterArgs [\#93](https://github.com/googleforgames/quilkin/pull/93) ([markmandel](https://github.com/markmandel))
- Add rate limiter metrics [\#87](https://github.com/googleforgames/quilkin/pull/87) ([iffyio](https://github.com/iffyio))
- Add rate limiter config and integration test [\#81](https://github.com/googleforgames/quilkin/pull/81) ([iffyio](https://github.com/iffyio))
- Convert connection\_id's to base64 byte arrays [\#79](https://github.com/googleforgames/quilkin/pull/79) ([markmandel](https://github.com/markmandel))
- Validate that addresses are unique [\#74](https://github.com/googleforgames/quilkin/pull/74) ([markmandel](https://github.com/markmandel))
- Lazy instantiation of Filters [\#71](https://github.com/googleforgames/quilkin/pull/71) ([markmandel](https://github.com/markmandel))
- Add local rate limiting [\#69](https://github.com/googleforgames/quilkin/pull/69) ([iffyio](https://github.com/iffyio))
- Add Validation to Config [\#68](https://github.com/googleforgames/quilkin/pull/68) ([markmandel](https://github.com/markmandel))
- Implementation of endpoint\_receive\_filter [\#59](https://github.com/googleforgames/quilkin/pull/59) ([markmandel](https://github.com/markmandel))
- Implementation of endpoint\_send\_filter [\#58](https://github.com/googleforgames/quilkin/pull/58) ([markmandel](https://github.com/markmandel))
- Session should track Endpoint as destination [\#57](https://github.com/googleforgames/quilkin/pull/57) ([markmandel](https://github.com/markmandel))
- Add client proxy load balancing support [\#56](https://github.com/googleforgames/quilkin/pull/56) ([iffyio](https://github.com/iffyio))
- Implementation of local\_send\_filter [\#55](https://github.com/googleforgames/quilkin/pull/55) ([markmandel](https://github.com/markmandel))
- Implementation of local\_receive\_filter [\#53](https://github.com/googleforgames/quilkin/pull/53) ([markmandel](https://github.com/markmandel))
- Add recv\_addr to endpoint\_receive\_filter [\#51](https://github.com/googleforgames/quilkin/pull/51) ([markmandel](https://github.com/markmandel))
- Implementation of FilterChain [\#45](https://github.com/googleforgames/quilkin/pull/45) ([markmandel](https://github.com/markmandel))
- Implementation of DebugFilter [\#44](https://github.com/googleforgames/quilkin/pull/44) ([markmandel](https://github.com/markmandel))
- Cleanup Server \> run - run\_recv\_from [\#43](https://github.com/googleforgames/quilkin/pull/43) ([markmandel](https://github.com/markmandel))
- Integration Test for client-\>server and back [\#38](https://github.com/googleforgames/quilkin/pull/38) ([markmandel](https://github.com/markmandel))
- Expand Filter Trait for sending and receiving [\#37](https://github.com/googleforgames/quilkin/pull/37) ([markmandel](https://github.com/markmandel))
- Ability to be a library and binary [\#36](https://github.com/googleforgames/quilkin/pull/36) ([markmandel](https://github.com/markmandel))
- Stub for default filters [\#35](https://github.com/googleforgames/quilkin/pull/35) ([markmandel](https://github.com/markmandel))
- Implementation of Filter API and FilterRegistry [\#33](https://github.com/googleforgames/quilkin/pull/33) ([markmandel](https://github.com/markmandel))
- Implementation of the Filter data structure. [\#31](https://github.com/googleforgames/quilkin/pull/31) ([markmandel](https://github.com/markmandel))
- Implementation of Session expiration. [\#27](https://github.com/googleforgames/quilkin/pull/27) ([markmandel](https://github.com/markmandel))
- Track expiration times for Sessions [\#26](https://github.com/googleforgames/quilkin/pull/26) ([markmandel](https://github.com/markmandel))
- Architecture diagram for README page. [\#15](https://github.com/googleforgames/quilkin/pull/15) ([markmandel](https://github.com/markmandel))

**Fixed bugs:**

- CI should use stable clippy [\#281](https://github.com/googleforgames/quilkin/issues/281)
- ICE when running `cargo +nightly test`. [\#276](https://github.com/googleforgames/quilkin/issues/276)
- Flaky Test: local\_rate\_limit tests [\#177](https://github.com/googleforgames/quilkin/issues/177)
- Bug: Changelog didn't have the version [\#324](https://github.com/googleforgames/quilkin/pull/324) ([markmandel](https://github.com/markmandel))
- Bugs/cleanup in netcat quickstart [\#322](https://github.com/googleforgames/quilkin/pull/322) ([markmandel](https://github.com/markmandel))
- Remove `name` from all examples and configurations [\#295](https://github.com/googleforgames/quilkin/pull/295) ([markmandel](https://github.com/markmandel))
- Fix release cloudbuild with rust-toolchain.yaml [\#255](https://github.com/googleforgames/quilkin/pull/255) ([markmandel](https://github.com/markmandel))
- Fix rate limiter flaky test [\#178](https://github.com/googleforgames/quilkin/pull/178) ([iffyio](https://github.com/iffyio))
- Don't panic on debug when packets aren't utf-8 [\#156](https://github.com/googleforgames/quilkin/pull/156) ([markmandel](https://github.com/markmandel))
- Fix bug in UpstreamEndpoints::retain [\#145](https://github.com/googleforgames/quilkin/pull/145) ([markmandel](https://github.com/markmandel))
- Run clippy first in CI [\#100](https://github.com/googleforgames/quilkin/pull/100) ([iffyio](https://github.com/iffyio))
- Filters shouldn't require configs [\#96](https://github.com/googleforgames/quilkin/pull/96) ([markmandel](https://github.com/markmandel))

**Closed issues:**

- Dev: make it easier to make build a container [\#296](https://github.com/googleforgames/quilkin/issues/296)
- Give Quilkin Bot Art [\#278](https://github.com/googleforgames/quilkin/issues/278)
- Add Error Handling Library [\#269](https://github.com/googleforgames/quilkin/issues/269)
- Design Question: `FilterFactory::name` returning `String` [\#251](https://github.com/googleforgames/quilkin/issues/251)
- Use `rust-toolchain` to manage Rust version. [\#241](https://github.com/googleforgames/quilkin/issues/241)
- Migrate CI build step to Artifact Registry [\#202](https://github.com/googleforgames/quilkin/issues/202)
- Refactor Compress Filter with on\_read & on\_write configuration [\#196](https://github.com/googleforgames/quilkin/issues/196)
- Refactor Filter to have `read` and `write` methods [\#192](https://github.com/googleforgames/quilkin/issues/192)
- Move deny warnings to CI [\#189](https://github.com/googleforgames/quilkin/issues/189)
- CI: Reuse downloaded / compiled libraries between steps [\#173](https://github.com/googleforgames/quilkin/issues/173)
- Mark a configuration as having passed validation before using it [\#172](https://github.com/googleforgames/quilkin/issues/172)
- Pass shutdown channel to run\_prune\_sessions [\#165](https://github.com/googleforgames/quilkin/issues/165)
- Implement session close inside drop [\#164](https://github.com/googleforgames/quilkin/issues/164)
- Is there a benefit to proxy\_mode? [\#163](https://github.com/googleforgames/quilkin/issues/163)
- Avoid need to clone metadata keys [\#155](https://github.com/googleforgames/quilkin/issues/155)
- Set log level [\#153](https://github.com/googleforgames/quilkin/issues/153)
- Remove name field from Endpoint objects [\#152](https://github.com/googleforgames/quilkin/issues/152)
- Do not parse packets as utf8 [\#151](https://github.com/googleforgames/quilkin/issues/151)
- Refactor UpstreamEndpoints retain to return enum [\#146](https://github.com/googleforgames/quilkin/issues/146)
- Avoid copying endpoints list for every packet [\#138](https://github.com/googleforgames/quilkin/issues/138)
- Proxy config file format [\#130](https://github.com/googleforgames/quilkin/issues/130)
- Add default method impl to Filter trait [\#124](https://github.com/googleforgames/quilkin/issues/124)
- Use custom struct implementation for filter values [\#122](https://github.com/googleforgames/quilkin/issues/122)
- Add documentation entry for filter values [\#121](https://github.com/googleforgames/quilkin/issues/121)
- Public chat room [\#119](https://github.com/googleforgames/quilkin/issues/119)
- Refactor Client.lb\_policy into it's own filter [\#103](https://github.com/googleforgames/quilkin/issues/103)
- FilterChain: Arguments should be a context object, and return a response object [\#94](https://github.com/googleforgames/quilkin/issues/94)
- Add helper to manage test resources [\#84](https://github.com/googleforgames/quilkin/issues/84)
- Add clippy to ci [\#82](https://github.com/googleforgames/quilkin/issues/82)
- Consolidate Filter Trait to two functions [\#80](https://github.com/googleforgames/quilkin/issues/80)
- Add health endpoint [\#73](https://github.com/googleforgames/quilkin/issues/73)
- Avoid unnecessary cloning in Filter trait [\#72](https://github.com/googleforgames/quilkin/issues/72)
- Use a consistent pattern for error handling [\#67](https://github.com/googleforgames/quilkin/issues/67)
- Filter Idea: Compression Filter [\#47](https://github.com/googleforgames/quilkin/issues/47)
- Should sender/receiver be client/server as concepts? [\#22](https://github.com/googleforgames/quilkin/issues/22)
- Integration Tests [\#4](https://github.com/googleforgames/quilkin/issues/4)
- Continuous Integration [\#2](https://github.com/googleforgames/quilkin/issues/2)

**Merged pull requests:**

- Release 0.1.0 [\#326](https://github.com/googleforgames/quilkin/pull/326) ([markmandel](https://github.com/markmandel))
- Ignore priority/wontfix for changelog [\#323](https://github.com/googleforgames/quilkin/pull/323) ([markmandel](https://github.com/markmandel))
- Tweaks to release checklist. [\#314](https://github.com/googleforgames/quilkin/pull/314) ([markmandel](https://github.com/markmandel))
- Move Cloud Build test and release to Makefile [\#312](https://github.com/googleforgames/quilkin/pull/312) ([markmandel](https://github.com/markmandel))
- Exclude items from crate [\#311](https://github.com/googleforgames/quilkin/pull/311) ([markmandel](https://github.com/markmandel))
- Remove redundant clones [\#307](https://github.com/googleforgames/quilkin/pull/307) ([XAMPPRocky](https://github.com/XAMPPRocky))
- Nightly builds of CI and Release Cloud Build Steps [\#306](https://github.com/googleforgames/quilkin/pull/306) ([markmandel](https://github.com/markmandel))
- Use `thiserror` for Error definitions [\#304](https://github.com/googleforgames/quilkin/pull/304) ([XAMPPRocky](https://github.com/XAMPPRocky))
- Fix for Apache Licence header [\#302](https://github.com/googleforgames/quilkin/pull/302) ([markmandel](https://github.com/markmandel))
- Fix metric typo in proxy documentation [\#299](https://github.com/googleforgames/quilkin/pull/299) ([markmandel](https://github.com/markmandel))
- Center image in README [\#298](https://github.com/googleforgames/quilkin/pull/298) ([XAMPPRocky](https://github.com/XAMPPRocky))
- Add Prometheus scraping to Xonotic example [\#297](https://github.com/googleforgames/quilkin/pull/297) ([markmandel](https://github.com/markmandel))
- Update release checklist to include quilkin-macros [\#294](https://github.com/googleforgames/quilkin/pull/294) ([markmandel](https://github.com/markmandel))
- Output the rust version for each build. [\#288](https://github.com/googleforgames/quilkin/pull/288) ([markmandel](https://github.com/markmandel))
- Refactor and break out top-level filter module. [\#286](https://github.com/googleforgames/quilkin/pull/286) ([XAMPPRocky](https://github.com/XAMPPRocky))
- Use `include\_str!` instead of `doc\(include\)` [\#284](https://github.com/googleforgames/quilkin/pull/284) ([XAMPPRocky](https://github.com/XAMPPRocky))
- Use channel size of 1 to send xds updates [\#283](https://github.com/googleforgames/quilkin/pull/283) ([iffyio](https://github.com/iffyio))
- Fixes from clippy [\#282](https://github.com/googleforgames/quilkin/pull/282) ([XAMPPRocky](https://github.com/XAMPPRocky))
- Add default configuration file conventions. [\#279](https://github.com/googleforgames/quilkin/pull/279) ([XAMPPRocky](https://github.com/XAMPPRocky))
- Use 2018 mod style [\#277](https://github.com/googleforgames/quilkin/pull/277) ([XAMPPRocky](https://github.com/XAMPPRocky))
- Use sampled logging for token router errors [\#275](https://github.com/googleforgames/quilkin/pull/275) ([iffyio](https://github.com/iffyio))
- Reject config if it contains unknown fields [\#274](https://github.com/googleforgames/quilkin/pull/274) ([iffyio](https://github.com/iffyio))
- Wait for execution result channel before recving [\#273](https://github.com/googleforgames/quilkin/pull/273) ([iffyio](https://github.com/iffyio))
- Token Router: Note about token authorship [\#271](https://github.com/googleforgames/quilkin/pull/271) ([markmandel](https://github.com/markmandel))
- Remove Session metric upstream & downstream labels [\#268](https://github.com/googleforgames/quilkin/pull/268) ([markmandel](https://github.com/markmandel))
- Implement Github notification bot. [\#264](https://github.com/googleforgames/quilkin/pull/264) ([markmandel](https://github.com/markmandel))
- Add execution measurement metrics to `FilterChain` [\#262](https://github.com/googleforgames/quilkin/pull/262) ([XAMPPRocky](https://github.com/XAMPPRocky))
- Update dependencies, add deny.toml [\#261](https://github.com/googleforgames/quilkin/pull/261) ([XAMPPRocky](https://github.com/XAMPPRocky))
- Fix links in rustdoc documentation [\#260](https://github.com/googleforgames/quilkin/pull/260) ([XAMPPRocky](https://github.com/XAMPPRocky))
- Use clap macros for about and name info [\#257](https://github.com/googleforgames/quilkin/pull/257) ([XAMPPRocky](https://github.com/XAMPPRocky))
- Improvements to Agones+Xonotic examples [\#254](https://github.com/googleforgames/quilkin/pull/254) ([markmandel](https://github.com/markmandel))
- Add shutdown-rx to SessionManager::run\_prune\_sessions [\#253](https://github.com/googleforgames/quilkin/pull/253) ([XAMPPRocky](https://github.com/XAMPPRocky))
- Add proc-macro for importing protobuf and defining filter IDs. [\#252](https://github.com/googleforgames/quilkin/pull/252) ([XAMPPRocky](https://github.com/XAMPPRocky))
- Roadmap Complete - Remove from README [\#250](https://github.com/googleforgames/quilkin/pull/250) ([markmandel](https://github.com/markmandel))
- Update Rust version to 1.51 [\#247](https://github.com/googleforgames/quilkin/pull/247) ([XAMPPRocky](https://github.com/XAMPPRocky))
- Clippy fixes  [\#246](https://github.com/googleforgames/quilkin/pull/246) ([XAMPPRocky](https://github.com/XAMPPRocky))
- Refactor UpstreamEndpoints::retain [\#245](https://github.com/googleforgames/quilkin/pull/245) ([XAMPPRocky](https://github.com/XAMPPRocky))
- Close Session on drop, refactor `SessionManager::prune\_sessions` [\#244](https://github.com/googleforgames/quilkin/pull/244) ([XAMPPRocky](https://github.com/XAMPPRocky))
- Add documentation for writing filters [\#243](https://github.com/googleforgames/quilkin/pull/243) ([iffyio](https://github.com/iffyio))
- Add missing licences. [\#242](https://github.com/googleforgames/quilkin/pull/242) ([markmandel](https://github.com/markmandel))
- Point COC contact to a Google Group. [\#240](https://github.com/googleforgames/quilkin/pull/240) ([markmandel](https://github.com/markmandel))
- It's a FAQ [\#238](https://github.com/googleforgames/quilkin/pull/238) ([markmandel](https://github.com/markmandel))
- Prepare to be released on crates.io [\#237](https://github.com/googleforgames/quilkin/pull/237) ([markmandel](https://github.com/markmandel))
- Updates to README.md [\#236](https://github.com/googleforgames/quilkin/pull/236) ([markmandel](https://github.com/markmandel))
- iperf3 performance testing example [\#227](https://github.com/googleforgames/quilkin/pull/227) ([markmandel](https://github.com/markmandel))
- Merge sessions map and pruning logic [\#224](https://github.com/googleforgames/quilkin/pull/224) ([iffyio](https://github.com/iffyio))
- Debug Filter: Capitalise log statement. [\#223](https://github.com/googleforgames/quilkin/pull/223) ([markmandel](https://github.com/markmandel))
- Tick off gRPC configuration management control plane API [\#222](https://github.com/googleforgames/quilkin/pull/222) ([markmandel](https://github.com/markmandel))
- Pull `Admin` module out of `Metrics` [\#220](https://github.com/googleforgames/quilkin/pull/220) ([markmandel](https://github.com/markmandel))
- Missing `source` in `ClusterManager` logger. [\#219](https://github.com/googleforgames/quilkin/pull/219) ([markmandel](https://github.com/markmandel))
- Fix `Admin` Config model and documentation [\#218](https://github.com/googleforgames/quilkin/pull/218) ([markmandel](https://github.com/markmandel))
- Update slog and update logging to be consistent [\#217](https://github.com/googleforgames/quilkin/pull/217) ([markmandel](https://github.com/markmandel))
- Fix old comment on Config. [\#216](https://github.com/googleforgames/quilkin/pull/216) ([markmandel](https://github.com/markmandel))
- Move setup logic to lib to enable reuse [\#215](https://github.com/googleforgames/quilkin/pull/215) ([iffyio](https://github.com/iffyio))
- Update proto submodules [\#212](https://github.com/googleforgames/quilkin/pull/212) ([iffyio](https://github.com/iffyio))
- Wrap metadata keys in Arc [\#211](https://github.com/googleforgames/quilkin/pull/211) ([iffyio](https://github.com/iffyio))
- Add xDS integration test [\#210](https://github.com/googleforgames/quilkin/pull/210) ([iffyio](https://github.com/iffyio))
- Expose binary version as debug/release [\#208](https://github.com/googleforgames/quilkin/pull/208) ([markmandel](https://github.com/markmandel))
- Add validated config type [\#207](https://github.com/googleforgames/quilkin/pull/207) ([iffyio](https://github.com/iffyio))
- Use the same metrics registry for filters [\#206](https://github.com/googleforgames/quilkin/pull/206) ([iffyio](https://github.com/iffyio))
- Move deny warnings to CI [\#205](https://github.com/googleforgames/quilkin/pull/205) ([markmandel](https://github.com/markmandel))
- Migrate Container Registry ➡ Artifact Registry [\#204](https://github.com/googleforgames/quilkin/pull/204) ([markmandel](https://github.com/markmandel))
- Mention filter reverse ordering in docs [\#200](https://github.com/googleforgames/quilkin/pull/200) ([iffyio](https://github.com/iffyio))
- Ticking off roadmap items [\#199](https://github.com/googleforgames/quilkin/pull/199) ([markmandel](https://github.com/markmandel))
- Refactor Compress with on\_read & on\_write config [\#197](https://github.com/googleforgames/quilkin/pull/197) ([markmandel](https://github.com/markmandel))
- Rename Filter to align with Envoy concepts [\#194](https://github.com/googleforgames/quilkin/pull/194) ([markmandel](https://github.com/markmandel))
- Add proto config for all filters [\#193](https://github.com/googleforgames/quilkin/pull/193) ([iffyio](https://github.com/iffyio))
- Use an async task pool to process packets [\#191](https://github.com/googleforgames/quilkin/pull/191) ([iffyio](https://github.com/iffyio))
- Enable XDS filter manager [\#190](https://github.com/googleforgames/quilkin/pull/190) ([iffyio](https://github.com/iffyio))
- Add Ifeanyi to the authors file [\#187](https://github.com/googleforgames/quilkin/pull/187) ([markmandel](https://github.com/markmandel))
- Replace warp with hyper for http server impl [\#184](https://github.com/googleforgames/quilkin/pull/184) ([iffyio](https://github.com/iffyio))
- Remove doctest workaround for proto generated comments [\#183](https://github.com/googleforgames/quilkin/pull/183) ([iffyio](https://github.com/iffyio))
- Reuse config deserialization logic across filters [\#182](https://github.com/googleforgames/quilkin/pull/182) ([iffyio](https://github.com/iffyio))
- Add proxy configuration reference to README.md [\#180](https://github.com/googleforgames/quilkin/pull/180) ([markmandel](https://github.com/markmandel))
- Integration test for Compress filter [\#179](https://github.com/googleforgames/quilkin/pull/179) ([markmandel](https://github.com/markmandel))
- Improve Cloud Build build times [\#176](https://github.com/googleforgames/quilkin/pull/176) ([markmandel](https://github.com/markmandel))
- Reuse static config deserialise logic [\#175](https://github.com/googleforgames/quilkin/pull/175) ([iffyio](https://github.com/iffyio))
- Upgrade to Tokio 1.0 [\#170](https://github.com/googleforgames/quilkin/pull/170) ([markmandel](https://github.com/markmandel))
- Removal of Proxy Mode [\#168](https://github.com/googleforgames/quilkin/pull/168) ([markmandel](https://github.com/markmandel))
- Perf improvements [\#166](https://github.com/googleforgames/quilkin/pull/166) ([iffyio](https://github.com/iffyio))
- Bunch of files without licences [\#157](https://github.com/googleforgames/quilkin/pull/157) ([markmandel](https://github.com/markmandel))
- Updated to the Roadmap [\#148](https://github.com/googleforgames/quilkin/pull/148) ([markmandel](https://github.com/markmandel))
- Update proxy configuration format [\#144](https://github.com/googleforgames/quilkin/pull/144) ([iffyio](https://github.com/iffyio))
- Fix for CI grabbing submodules [\#142](https://github.com/googleforgames/quilkin/pull/142) ([markmandel](https://github.com/markmandel))
- Default Filter trait implementation [\#141](https://github.com/googleforgames/quilkin/pull/141) ([markmandel](https://github.com/markmandel))
- Add wrapper over Endpoints [\#140](https://github.com/googleforgames/quilkin/pull/140) ([iffyio](https://github.com/iffyio))
- Remove remanining returns of Tokio Result [\#136](https://github.com/googleforgames/quilkin/pull/136) ([iffyio](https://github.com/iffyio))
- Fix unit test for CaptureBytes [\#134](https://github.com/googleforgames/quilkin/pull/134) ([markmandel](https://github.com/markmandel))
- Rename CaptureBytes context\_key ➡ metadata\_key [\#133](https://github.com/googleforgames/quilkin/pull/133) ([markmandel](https://github.com/markmandel))
- Refactor Filter Values to Dynamic Metadata [\#132](https://github.com/googleforgames/quilkin/pull/132) ([markmandel](https://github.com/markmandel))
- Replace oneshot shutdown signal with watch channel [\#129](https://github.com/googleforgames/quilkin/pull/129) ([iffyio](https://github.com/iffyio))
- Updated development and community documentation [\#127](https://github.com/googleforgames/quilkin/pull/127) ([markmandel](https://github.com/markmandel))
- Upgrade Rust toolchain [\#126](https://github.com/googleforgames/quilkin/pull/126) ([markmandel](https://github.com/markmandel))
- Make LoadBalancer a filter [\#125](https://github.com/googleforgames/quilkin/pull/125) ([iffyio](https://github.com/iffyio))
- Typo: ConcatBytes ➡ ConcatenateBytes [\#117](https://github.com/googleforgames/quilkin/pull/117) ([markmandel](https://github.com/markmandel))
- Docs: Config is an object not an any [\#116](https://github.com/googleforgames/quilkin/pull/116) ([markmandel](https://github.com/markmandel))
- Removal of client-\>connection\_id [\#114](https://github.com/googleforgames/quilkin/pull/114) ([markmandel](https://github.com/markmandel))
- Rename DebugFilter -\> Debug to avoid redundancy [\#113](https://github.com/googleforgames/quilkin/pull/113) ([markmandel](https://github.com/markmandel))
- Let's clippy our tests 👍 [\#110](https://github.com/googleforgames/quilkin/pull/110) ([markmandel](https://github.com/markmandel))
- Use builder to create Config [\#109](https://github.com/googleforgames/quilkin/pull/109) ([iffyio](https://github.com/iffyio))
- Pass context objects in filter APIs [\#102](https://github.com/googleforgames/quilkin/pull/102) ([iffyio](https://github.com/iffyio))
- Validate config from external docs [\#99](https://github.com/googleforgames/quilkin/pull/99) ([iffyio](https://github.com/iffyio))
- Refactor: debug\_filter -\> debug [\#95](https://github.com/googleforgames/quilkin/pull/95) ([markmandel](https://github.com/markmandel))
- Add missing licence [\#91](https://github.com/googleforgames/quilkin/pull/91) ([markmandel](https://github.com/markmandel))
- Test utility for filters with no change. [\#90](https://github.com/googleforgames/quilkin/pull/90) ([markmandel](https://github.com/markmandel))
- Add filters docs and external doc tests support [\#89](https://github.com/googleforgames/quilkin/pull/89) ([iffyio](https://github.com/iffyio))
- Documentation for LocalRateLimit Filter Metrics [\#88](https://github.com/googleforgames/quilkin/pull/88) ([markmandel](https://github.com/markmandel))
- Add clippy to ci and fix warnings [\#85](https://github.com/googleforgames/quilkin/pull/85) ([iffyio](https://github.com/iffyio))
- Refactor Filter to two functions [\#83](https://github.com/googleforgames/quilkin/pull/83) ([markmandel](https://github.com/markmandel))
- Integration test for DebugFilter [\#78](https://github.com/googleforgames/quilkin/pull/78) ([markmandel](https://github.com/markmandel))
- Add metrics integration test [\#76](https://github.com/googleforgames/quilkin/pull/76) ([iffyio](https://github.com/iffyio))
- Cloud Build timeout to 30m [\#66](https://github.com/googleforgames/quilkin/pull/66) ([markmandel](https://github.com/markmandel))
- Add metrics support [\#65](https://github.com/googleforgames/quilkin/pull/65) ([iffyio](https://github.com/iffyio))
- End to End test for the TestFilter [\#64](https://github.com/googleforgames/quilkin/pull/64) ([markmandel](https://github.com/markmandel))
- Integration tests reusable components [\#61](https://github.com/googleforgames/quilkin/pull/61) ([markmandel](https://github.com/markmandel))
- Unit test for endpoint send & local receive filter [\#60](https://github.com/googleforgames/quilkin/pull/60) ([markmandel](https://github.com/markmandel))
- Remove redundant async and fix comment typos [\#54](https://github.com/googleforgames/quilkin/pull/54) ([iffyio](https://github.com/iffyio))
- Move TestFilters into test\_utils [\#52](https://github.com/googleforgames/quilkin/pull/52) ([markmandel](https://github.com/markmandel))
- Refactor UDP Test Utils to be more flexible [\#50](https://github.com/googleforgames/quilkin/pull/50) ([markmandel](https://github.com/markmandel))
- Remove `async` from some Server functions [\#49](https://github.com/googleforgames/quilkin/pull/49) ([markmandel](https://github.com/markmandel))
- Update Readme Roadmap [\#48](https://github.com/googleforgames/quilkin/pull/48) ([markmandel](https://github.com/markmandel))
- Refactor Config.get\_endpoints\(\) [\#46](https://github.com/googleforgames/quilkin/pull/46) ([markmandel](https://github.com/markmandel))
- Cleanup Server \> run - run\_prune\_sessions [\#42](https://github.com/googleforgames/quilkin/pull/42) ([markmandel](https://github.com/markmandel))
- Server tests, drop 'server\_ prefix. [\#41](https://github.com/googleforgames/quilkin/pull/41) ([markmandel](https://github.com/markmandel))
- Cleanup Server \> run - run\_receive\_packet [\#40](https://github.com/googleforgames/quilkin/pull/40) ([markmandel](https://github.com/markmandel))
- Cleanup consistency with channel import [\#39](https://github.com/googleforgames/quilkin/pull/39) ([markmandel](https://github.com/markmandel))
- Upgrade CI to Rust 1.42.0 [\#34](https://github.com/googleforgames/quilkin/pull/34) ([markmandel](https://github.com/markmandel))
- Use Plain Terminal Logging for test output [\#32](https://github.com/googleforgames/quilkin/pull/32) ([markmandel](https://github.com/markmandel))
- Refactor Session and Server into separate modules [\#29](https://github.com/googleforgames/quilkin/pull/29) ([markmandel](https://github.com/markmandel))
- Cloud Build CI implementation [\#28](https://github.com/googleforgames/quilkin/pull/28) ([markmandel](https://github.com/markmandel))
- Upgrade dependencies. [\#25](https://github.com/googleforgames/quilkin/pull/25) ([markmandel](https://github.com/markmandel))
- Tests for Server::run [\#24](https://github.com/googleforgames/quilkin/pull/24) ([markmandel](https://github.com/markmandel))
- Rename sender/receiver to client/server [\#23](https://github.com/googleforgames/quilkin/pull/23) ([markmandel](https://github.com/markmandel))
- Tests for Session::new [\#21](https://github.com/googleforgames/quilkin/pull/21) ([markmandel](https://github.com/markmandel))
- Test for Server::process\_receive\_packet\_channel [\#20](https://github.com/googleforgames/quilkin/pull/20) ([markmandel](https://github.com/markmandel))
- Test for Server::process\_receive\_socket [\#18](https://github.com/googleforgames/quilkin/pull/18) ([markmandel](https://github.com/markmandel))
- Test for Server::ensure\_session [\#17](https://github.com/googleforgames/quilkin/pull/17) ([markmandel](https://github.com/markmandel))
- Test for Session::send\_to [\#16](https://github.com/googleforgames/quilkin/pull/16) ([markmandel](https://github.com/markmandel))



\* *This Changelog was automatically generated by [github_changelog_generator](https://github.com/github-changelog-generator/github-changelog-generator)*
