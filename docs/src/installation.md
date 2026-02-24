# Installation

There are variety of automated and manual methods for installing Quilkin onto
your system. For cloud deployments Quilkin provides a [container image](#oci-image)
to make it easily to immediately start using it. You can also install Quilkin on
your local machine through [Cargo](#cargo).

## Distributions

### [OCI Image](https://github.com/embarkstudios/quilkin/pkgs/container/quilkin)

<dl>
  <dt><strong>Source / Method</strong></dt>
  <dd>

```shell
docker pull ghcr.io/embarkstudios/quilkin:<version>-<short_commit>
```

  </dd>
  <dt><strong>Notes</strong></dt>
  <dd>Pre-built Quilkin binary with no preset arguments</dd>
</dl>

### <a href="https://lib.rs/crates/quilkin" data-proofer-ignore>Cargo</a>

<dl>
  <dt><strong>Source / Method</strong></dt>
  <dd>

```
cargo install quilkin
```

  </dd>
  <dt><strong>Notes</strong></dt>
  <dd>Compiled from source using cargo</dd>
</dl>

### [GitHub](https://github.com/EmbarkStudios/quilkin)

<dl>
  <dt><strong>Source / Method</strong></dt>
  <dd>

[GitHub Releases](https://github.com/EmbarkStudios/quilkin/releases)

  </dd>
  <dt><strong>Notes</strong></dt>
  <dd>Pre-built binaries for manual installation</dd>
</dl>
