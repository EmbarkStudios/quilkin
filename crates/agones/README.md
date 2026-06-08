# Agones Integration Tests

This folder contains the integration tests for the Quilkin and Agones integration.

## Requirements

* A Kubernetes cluster with [Agones](https://agones.dev) installed.
* Local authentication to the cluster via `kubectl`.

## Running with kind (recommended, matches CI)

The quickest way to run the tests — and what CI does — is against a local
[kind](https://kind.sigs.k8s.io/) cluster. [`setup-kind.sh`](./setup-kind.sh), shared by local
runs and [the `agones.yml` workflow](../../.github/workflows/agones.yml), creates the cluster,
installs Agones, builds and loads the Quilkin image, and runs the tests:

```shell
mise run //crates/agones:test
```

`kubectl`, `helm` and `docker` must be installed; the script installs `kind` if missing, and
exposes the individual steps (`create`, `agones`, `image`, `run`, `diagnostics`, `teardown`).
See the top of the script for the environment variables it honours.

### Rootless Docker/Podman

Under rootless Docker/Podman (common on WSL2), kind needs systemd cgroup delegation enabled once
on the host:

```shell
sudo mkdir -p /etc/systemd/system/user@.service.d
printf '[Service]\nDelegate=yes\n' | sudo tee /etc/systemd/system/user@.service.d/delegate.conf
sudo systemctl daemon-reload
```

Then restart the user session (on WSL2: `wsl --shutdown` from Windows, reopen, then
`systemctl --user restart docker`). See the [kind rootless docs](https://kind.sigs.k8s.io/docs/user/rootless/).

## Running against another cluster

If you already have a cluster (minikube, GKE, ...) with Agones installed and `kubectl`
authenticated against it, run the tests directly with `IMAGE_TAG` pointing at a Quilkin image
the cluster can pull:

```shell
IMAGE_TAG=us-docker.pkg.dev/my-project-name/dev/quilkin:0.4.0-auyz cargo nextest run -p agones
```

Each test runs in its own namespace, so they run in parallel.

### Creating an Agones Minikube Cluster

You can use [minikube](https://github.com/kubernetes/minikube) to create a cluster and install
Agones on it. Because of the virtualisation layers required by various minikube drivers, only
certain combinations of OS and driver provide direct UDP connectivity, so follow the
[Agones documentation on setting up minikube](https://agones.dev/site/docs/installation/creating-cluster/minikube/)
to choose a known-working combination. Then follow the
[Agones installation documentation](https://agones.dev/site/docs/installation/install-agones/).

### Creating an Agones GKE Cluster with Terraform

[main.tf](./main.tf) is a convenience tool for setting up a GKE cluster for end-to-end testing.
It requires the [Google Cloud CLI](https://cloud.google.com/sdk/gcloud),
[kubectl](https://kubernetes.io/docs/tasks/tools/#kubectl) and
[Terraform](https://www.terraform.io/downloads).

By default the cluster is created in zone `us-west1-c`; see [main.tf](./main.tf) to override
the variables.

```shell
terraform init
gcloud auth application-default login
terraform apply -var project="<YOUR_GCP_ProjectID>"
gcloud container clusters get-credentials --zone us-west1-c agones
```

You will also need a Docker repository the cluster can pull from (such as
[Artifact Registry](https://cloud.google.com/artifact-registry)) to host the `IMAGE_TAG` image.

## Troubleshooting

If you have authentication issues sending commands to the cluster from the tests, run a
`kubectl` command (e.g. `kubectl get pods`) against the cluster to refresh the authentication
token and try again.

## Licence

Apache 2.0
