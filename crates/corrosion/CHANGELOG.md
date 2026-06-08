# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0](https://github.com/EmbarkStudios/quilkin/releases/tag/quilkin-corrosion-v0.1.0) - 2026-05-26

### Added

- Initial corrosion integration ([#1348](https://github.com/EmbarkStudios/quilkin/pull/1348))

### Fixed

- rustdoc errors & warnings

### Other

- set corrosion to publish = true
- remove .* from .gitignore, set version of corrosion dependency
- Remove sub DB when streams are terminated ([#1393](https://github.com/EmbarkStudios/quilkin/pull/1393))
- Foward xDS events to corrosion ([#1376](https://github.com/EmbarkStudios/quilkin/pull/1376))
- Bind corrosion client to unspecified address ([#1381](https://github.com/EmbarkStudios/quilkin/pull/1381))
- Fix FromSqlValue for DatacenterRow ([#1374](https://github.com/EmbarkStudios/quilkin/pull/1374))
- fixup Cargo.toml's in packages ([#1341](https://github.com/EmbarkStudios/quilkin/pull/1341))
- remove unused dependencies ([#1335](https://github.com/EmbarkStudios/quilkin/pull/1335))
- update dependencies ([#1321](https://github.com/EmbarkStudios/quilkin/pull/1321))
- Update crates and rust version ([#1313](https://github.com/EmbarkStudios/quilkin/pull/1313))
- Subscription I/O ([#1308](https://github.com/EmbarkStudios/quilkin/pull/1308))
- Add support for subscriptions ([#1296](https://github.com/EmbarkStudios/quilkin/pull/1296))
- Add version 1 handshake test ([#1273](https://github.com/EmbarkStudios/quilkin/pull/1273))
- Add persistent UDP stream ([#1271](https://github.com/EmbarkStudios/quilkin/pull/1271))
- Add corrosion ([#1261](https://github.com/EmbarkStudios/quilkin/pull/1261))
