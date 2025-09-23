# Release {version}

## Prerequisites

- [ ] Have at least `Editor` level access to `quilkin` Google Cloud project.
- [ ] Local gcloud configuration is pointing at the `quilkin` Google Cloud project.
- [ ] Local git remote `upstream` points at `git@github.com:EmbarkStudios/quilkin.git`.

## Steps

- [ ] Review that closed issues have appropriate tags for the changelog.
- [ ] Review that merged PRs have appropriate tags for the changelog.
- [ ] Run `git remote update && git checkout main && git reset --hard upstream/main` to ensure your code is in line
  with upstream.
- [ ] Update Cargo version for release
  - [ ] Edit the `version` field in `./Cargo.toml` and remove the `-dev` suffix.
  - [ ] Edit the `quilkin-macros` dependency in `./Cargo.toml` and remove the `-dev` suffix.
  - [ ] Edit the `quilkin-proto` dependency in `./Cargo.toml` and remove the `-dev` suffix.
  - [ ] Edit the `quilkin-xds` dependency in `./Cargo.toml` and remove the `-dev` suffix.
  - [ ] Edit the `version` field in `./crates/macros/Cargo.toml` and remove the `-dev` suffix.
  - [ ] Edit the `version` field in `./crates/quilkin-proto/Cargo.toml` and remove the `-dev` suffix.
  - [ ] Edit the `version` field in `./crates/xds/Cargo.toml` and remove the `-dev` suffix.
- [ ] cd to `./build/release` and run `make` to submit the cloud build
- [ ] Download all the artifacts from the cloud build.
- [ ] Review `license.html` to ensure that there aren't any new MPL, or CDDL dependencies from the last
  release. If there are:
  - [ ] Add the dependencies to [archive_dependencies.sh](https://github.com/EmbarkStudios/quilkin/blob/main/build/release/archive_dependencies.sh)
          so that the source is archived in the container image.
  - [ ] Reset checklist back to "run `make` to submit the cloud build", and start from there again.
- [ ] Run `cd crates/macros && cargo publish --dry-run --allow-dirty` and ensure there are no issues.
- [ ] Run `cd crates/quilkin-protos && cargo publish --dry-run --allow-dirty` and ensure there are no issues.
- [ ] Run `cd crates/xds && cargo publish --dry-run --allow-dirty` and ensure there are no issues.
- [ ] Run `cargo publish --dry-run --allow-dirty` and ensure there are no issues.
- [ ] Run `cargo clippy` in the root directory, and ensure there are no issues.
- [ ] Add a release item to README.md "Documentation" > "Releases" list with related links in reverse chronological
  order.
- [ ] Review any `data-proofer-ignore` attributes from links in the documentation in `./docs`, and remove any no
  longer needed.
- [ ] Update all yaml files in to `./examples` to the next release version.
- [ ] Create a draft [Github release](https://github.com/EmbarkStudios/quilkin/releases/new)
  - [ ] Populate the tag with `v{version}`
  - [ ] Click `Generate release notes` to generate the change log for this release.
  - [ ] Copy the release notes from the draft release and paste it at the top of CHANGELOG.md.
  - [ ] Using the
      [Github release template](https://github.com/EmbarkStudios/quilkin/blob/main/build/templates/github-release.md)
      update the generated release notes with a description, and relevant changelog sections.
  - [ ] Attach all the cloud build artifacts to the draft GitHub release.
- [ ] Submit these changes as a PR, and merge with approval.
- [ ] Run `git remote update && git checkout main && git reset --hard upstream/main` to ensure your code is in line
      with upstream.
- [ ] Run `git checkout -b release-{version} && git push upstream release-{version}` to create a release branch.
- [ ] Publish to [crates.io/crates/quilkin-macros](https://crates.io/crates/quilkin-macros): run `cd macros && cargo publish`
- [ ] Publish to [crates.io/crates/quilkin](https://crates.io/crates/quilkin): run `cargo publish`
- [ ] Submit the release.
- [ ] Post announcemnts
  - [ ] [mailing list](https://groups.google.com/g/quilkin-discuss).
  - [ ] [Discord #announcement](https://discord.com/channels/773975408265134100/879794098721140786)
  - [ ] [Twitter account](https://twitter.com/quilkindev).
- [ ] Update Cargo version for development
  - [ ] Edit `Cargo.toml` and increment the [minor version](https://semver.org/) and apply the `-dev` suffix to the
      `version`.
  - [ ] Edit the `quilkin-macros` dependency in `./Cargo.toml` and increment the [minor version](https://semver.org/)
      and apply the `-dev` suffix to the `version`.
  - [ ] Edit the `quilkin-protos` dependency in `./Cargo.toml` and increment the [minor version](https://semver.org/)
      and apply the `-dev` suffix to the `version`.
  - [ ] Edit the `quilkin-xds` dependency in `./Cargo.toml` and increment the [minor version](https://semver.org/)
    and apply the `-dev` suffix to the `version`.
  - [ ] Edit the `version` field in `./crates/macros/Cargo.toml`and increment the [minor version](https://semver.org/)
      and apply the `-dev` suffix to the `version`.
  - [ ] Edit the `version` field in `./crates/quilkin-protos/Cargo.toml`and increment the [minor version](https://semver.org/)
      and apply the `-dev` suffix to the `version`.
- [ ] Edit the `version` field in `./crates/xds/Cargo.toml`and increment the [minor version](https://semver.org/)
    and apply the `-dev` suffix to the `version`.
- [ ] Submit this change as a PR, and merge with approval.

Congratulation! 🎉 You have successfully released Quilkin!
