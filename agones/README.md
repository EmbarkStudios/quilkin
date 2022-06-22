# Agones Integration Tests

This folder containers the integration tests for Quilkin and Agones integration.

## Requirements

* A Kubernetes cluster with [Agones](https://agones.dev) installed.
* Local authentication to the cluster via `kubectl`.

## Running Tests

To run the tests, run `cargo test` in this folder. This will run the e2e to tests with the default Quilkin image.

When writing new tests for new features, you will want to specify a development image hosted on a container 
registry to test against. This can be done through the `QUILKIN_IMAGE` environment variable like so:

```shell
QUILKIN_IMAGE=us-docker.pkg.dev/my-project-name/dev/quilkin:0.4.0-dev cargo test
```

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

## Licence

Apache 2.0
