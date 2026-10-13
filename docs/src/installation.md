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
# A release.
docker pull ghcr.io/embarkstudios/quilkin:<version>
# A specific commit built from `main`.
docker pull ghcr.io/embarkstudios/quilkin:<short_commit>
```

  </dd>
  <dt><strong>Notes</strong></dt>
  <dd>Pre-built Quilkin binary with no preset arguments. Releases are tagged
  <code>YYYY.M.PATCH</code> and <code>latest</code>; builds from <code>main</code> are tagged
  with the commit's short SHA, such as <code>41673c8</code>.</dd>
</dl>

### Cargo

<dl>
  <dt><strong>Source / Method</strong></dt>
  <dd>

```
cargo install --git https://github.com/EmbarkStudios/quilkin --tag v<version> quilkin
```

  </dd>
  <dt><strong>Notes</strong></dt>
  <dd>Compiled from source using cargo. Quilkin is not published to crates.io,
  so this installs from the Git tag.</dd>
</dl>
