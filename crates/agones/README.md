# Agones Integration Tests

This folder containers the integration tests for Quilkin and Agones integration.

## Requirements

* A Kubernetes cluster with [Agones](https://agones.dev) installed.
* Local authentication to the cluster via `kubectl`.

## Creating an Agones Minikube Cluster

If you want to test locally, you can use a tool such a [minikube](https://github.com/kubernetes/minikube) to create 
a cluster, and install Agones on it.

Because of the virtualisation layers that are required with various drivers of Minikube,  only certain combinations of 
OS's and drivers can provide direct UDP connectivity, therefore it's worth following the 
[Agones documentation on setting up Minikube](https://agones.dev/site/docs/installation/creating-cluster/minikube/) 
to set up a known working combination.

Then follow either the YAML or Helm install options in the 
[Agones installation documentation](https://agones.dev/site/docs/installation/install-agones/) depoending on your 
preference.

## Creating an Agones GKE Cluster with Terraform

The following is a convenience tool for setting up a cluster for end-to-end testing.

This requires:

* [Google Cloud CLI](https://cloud.google.com/sdk/gcloud)
* [kubectl](https://kubernetes.io/docs/tasks/tools/#kubectl)
* [Terraform](https://www.terraform.io/downloads)

You can also use `make shell` from the [build](../build) folder, which will give you a shell environment with all
the tools needed.

By default, the provided Terraform script creates a cluster in zone "us-west1-c", but this can be overwritten in the 
variables. See [main.tf](./main.tf) for details.

```
terraform init
gcloud auth application-default login
terraform apply -var project="<YOUR_GCP_ProjectID>"
gcloud container clusters get-credentials --zone us-west1-c agones
```

## Running Tests

To run the Agones integration tests with a Quilkin image, you will need to specify the image 
to be used along with the `cargo test` command and ensure that is available on the currently authenticated Kubernetes 
cluster.

This can be done through the `IMAGE_TAG` environment variable like so:

```shell
IMAGE_TAG=us-docker.pkg.dev/my-project-name/dev/quilkin:0.4.0-auyz cargo test
```

### Build, Push and Test in one Go 💪

The [`build/Makefile`](../build/Makefile) provides a targets that can be executed to build a development image, 
push it an appropriate location, and run the set of Agones integration tests, depending on where you Kubernetes 
cluster is set up. 

#### Minikube

This target assumes that you have a [working minikube cluster](#creating-an-agones-minikube-cluster),
under the profile `quilkin`, with Agones installed, and the local `.kube` configuration is currently
authenticated against it.

To build, push and run the tests:

```shell
make minikube-test-agones
```

To change from the default profile of `quilkin`, use the variable `MINIKUBE_PROFILE` to do so.

To pass extra arguments to `cargo test`, to run only a single test, for example, use the `ARGS` variable
to pass through those options.

#### Hosted Kubernetes Cluster

This target assumes that you have a
[working hosted Kubernetes cluster, such as GKE](#creating-an-agones-gke-cluster-with-terraform),
with Agones installed, the local `.kube` configuration is currently authenticated against it,
and a hosted [docker repository](https://docs.docker.com/docker-hub/repos/) such as
[Artifact Registry](https://cloud.google.com/artifact-registry) has been provisioned.

To build, push and run the tests:

```shell
REPOSITORY=us-docker.pkg.dev/my-project/repository-name/ make test-agones
```

Where `REPOSITORY` is the provisioned Docker repository to push the development image to, and utilise in the 
integration tests.

> Note: The REPOSITORY variable will need to end with a trailing slash: /.

To pass extra arguments to `cargo test`, to run only a single test, for example, use the `ARGS` variable
to pass through those options.

### Troubleshooting

If you ever have authentication issues sending commands to the cluster from the e2e test, run a `kubectl` 
command (e.g. `kubectl get pods`) against the designated cluster to refresh the authentication token and try again. 

## Licence

Apache 2.0
