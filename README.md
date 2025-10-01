<p  align="center">
    <img src="./docs/logos/logo-white.png" alt="Quilkin logo" height="220">
</p>

[![GitHub](https://img.shields.io/github/license/EmbarkStudios/quilkin)](./LICENSE)
[![Crates.io](https://img.shields.io/crates/v/quilkin)](https://crates.io/crates/quilkin)
[![Docs.rs](https://docs.rs/quilkin/badge.svg)](https://docs.rs/quilkin)
[![GitHub release (latest SemVer)](https://img.shields.io/github/v/release/EmbarkStudios/quilkin)](https://github.com/EmbarkStudios/quilkin/releases)
[![Discord](https://img.shields.io/discord/773975408265134100)](https://discord.gg/mfBNZjBDnc)
[![Twitter Follow](https://img.shields.io/twitter/follow/quilkindev?style=social)](https://twitter.com/quilkindev)

Quilkin is a non-transparent UDP proxy specifically designed for use with large scale multiplayer dedicated game
servers deployments that ensures security, access control, telemetry data, metrics and more without the end user
having to custom build and integrate this functionality into their game clients and servers directly.

## Announcements

* [Embark Studios: Say hi to Quilkin, an open-source UDP proxy](https://medium.com/embarkstudios/say-hi-to-quilkin-an-open-source-udp-proxy-88577c795204)
* [Google Cloud: Introducing Quilkin: open-source UDP proxies built for game server communication](https://cloud.google.com/blog/products/gaming/introducing-quilkin)

## Project State

Project is currently in *beta* status, meaning that it is being actively developed, and being used in production
systems, but that the API may break and functionality may be changed or removed between releases.

## Documentation

### Releases

* v0.9.0 ([guides](https://embarkstudios.github.io/quilkin/v0.9.0/book/),
  [api](https://embarkstudios.github.io/quilkin/v0.9.0/api/quilkin/),
  [macros](https://embarkstudios.github.io/quilkin/v0.9.0/api/quilkin_macros/))
* v0.8.0 ([guides](https://embarkstudios.github.io/quilkin/v0.8.0/book/),
  [api](https://embarkstudios.github.io/quilkin/v0.8.0/api/quilkin/),
  [macros](https://embarkstudios.github.io/quilkin/v0.8.0/api/quilkin_macros/))
* v0.7.0 ([guides](https://embarkstudios.github.io/quilkin/v0.7.0/book/),
  [api](https://embarkstudios.github.io/quilkin/v0.7.0/api/quilkin/),
  [macros](https://embarkstudios.github.io/quilkin/v0.7.0/api/quilkin_macros/))
* v0.6.0 ([guides](https://embarkstudios.github.io/quilkin/v0.6.0/book/),
  [api](https://embarkstudios.github.io/quilkin/v0.6.0/api/quilkin/),
  [macros](https://embarkstudios.github.io/quilkin/v0.6.0/api/quilkin_macros/))
* v0.5.0 ([guides](https://embarkstudios.github.io/quilkin/v0.5.0/book/),
  [api](https://embarkstudios.github.io/quilkin/v0.5.0/api/quilkin/),
  [macros](https://embarkstudios.github.io/quilkin/v0.5.0/api/quilkin_macros/))
* v0.4.0 ([guides](https://embarkstudios.github.io/quilkin/v0.4.0/book/),
  [api](https://embarkstudios.github.io/quilkin/v0.4.0/api/quilkin/),
  [macros](https://embarkstudios.github.io/quilkin/v0.4.0/api/quilkin_macros/))
* v0.3.0 ([guides](https://embarkstudios.github.io/quilkin/v0.3.0/book/),
  [api](https://embarkstudios.github.io/quilkin/v0.3.0/api/quilkin/),
  [macros](https://embarkstudios.github.io/quilkin/v0.3.0/api/quilkin_macros/))
* v0.2.0 ([guides](https://embarkstudios.github.io/quilkin/v0.2.0/book/),
  [api](https://embarkstudios.github.io/quilkin/v0.2.0/api/quilkin/),
  [macros](https://embarkstudios.github.io/quilkin/v0.2.0/api/quilkin_macros/))
* v0.1.0 ([guides](https://github.com/embarkstudios/quilkin/blob/v0.1.0/README.md#usage),
  [api](https://docs.rs/quilkin/0.1.0/quilkin/), [macros](https://docs.rs/quilkin-macros/0.1.0/quilkin_macros/)).

### Development

> This documentation is for the development version of Quilkin, currently active on the `main` branch. To view the
> documentation for a specific release, click on the appropriate release documentation link above.

* [Guide](https://embarkstudios.github.io/quilkin/main/book/)
* [Api](https://embarkstudios.github.io/quilkin/main/api/quilkin/)
* [Macros](https://embarkstudios.github.io/quilkin/main/api/quilkin_macros/)

## Code of Conduct

Participation in this project comes under the [Contributor Covenant Code of Conduct](code-of-conduct.md)

## Development and Contribution

Please read the [contributing](CONTRIBUTING.md) guide for directions on writing code and submitting Pull Requests.

Quilkin is in active development - we would love your help in shaping its future!

## Community

There are lots of ways to engage with the Quilkin community:

* Here on [Github](https://github.com/EmbarkStudios/quilkin) via
  [issues](https://github.com/EmbarkStudios/quilkin/issues) and
  [pull requests](https://github.com/EmbarkStudios/quilkin/pulls).
* Join our [mailing list](https://groups.google.com/forum/#!forum/quilkin-discuss), which also gives you access to
  our continuous integration builds.
* Join our [Discord chat server](https://discord.gg/mfBNZjBDnc).
* Follow up on [Twitter](https://twitter.com/quilkindev).
* Join our monthly community meetings ([webcal](https://calendar.google.com/calendar/embed?src=c_9mgnkieltlphoijg5uio0g6a14%40group.calendar.google.com&ctz=America%2FLos_Angeles),
  [ical](https://calendar.google.com/calendar/ical/c_9mgnkieltlphoijg5uio0g6a14%40group.calendar.google.com/public/basic.ics),
  [recordings](https://www.youtube.com/playlist?list=PLhkWKwFGACw1oG-qbKQZBlAKAfuG_XpTd)).

## Credits

Many concepts and architectural decisions were inspired by [Envoy Proxy](https://www.envoyproxy.io/).
Huge thanks to that team for the inspiration they provided with all their hard work.

## Companies using Quilkin

[<img src="./image/embark.png" alt="Embark Studios" height="100">](https://www.embark-studios.com/)

## Licence

Apache 2.0

<img src="./docs/logos/mascot.png" alt="Quilly, the Quilkin mascot" height="200" align="right">
