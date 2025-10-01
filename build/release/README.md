# Build Release

Run `make` to submit the `cloudbuild.yaml` to [Google Cloud Build](https://cloud.google.com/build) and build the 
release:

* amd64 Linux, Windows and macOS executables.
* amd64 Linux Docker image.

If you need to pass extra arguments to the `make` target, the target comes with an `$(ARGS)` parameter than can be
used.

The executables are stored under `gs://$PROJECT_ID-quilkin-releases` in a zip file named quilkin-${version}.zip, 
where the version is the version stored in Cargo.toml.

This `cloudbuild.yaml` assumes several things are set up in the project:

## Docker container repository

There needs to be a docker container repository called `release` already created in your 
project, and the release images will create the Docker images therein. The images will be tagged with ${version} 
where the version is the version stored in Cargo.toml.

To create this, run: `gcloud artifacts repositories create release --repository-format=docker --location=us`.
