# Copyright 2021 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#  __     __         _       _     _
#  \ \   / /_ _ _ __(_) __ _| |__ | | ___ ___
#   \ \ / / _` | '__| |/ _` | '_ \| |/ _ \ __|
#    \ V / (_| | |  | | (_| | |_) | |  __\__ \
#     \_/ \__,_|_|  |_|\__,_|_.__/|_|\___|___/
#

mkfile_path := $(abspath $(lastword $(MAKEFILE_LIST)))
build_path := $(dir $(mkfile_path))
project_path := $(realpath $(build_path)/..)

CARGO_HOME ?= $(build_path)/.cargo
BUILD_IMAGE_TAG ?= quilkin-build
rust_toolchain := $(shell grep channel $(project_path)/rust-toolchain.toml | awk '{ print $$3 }')

# if this is a release, don't put the sha, otherwise, leave it off.
ifdef QUILKIN_RELEASE
	package_version := $(shell grep -A1 -w "name = \"quilkin\"" $(project_path)/Cargo.toml | grep version -m 1 | awk '{print $$3}')
else
	git_sha := $(shell git rev-parse --short=7 HEAD)
	package_version := $(shell grep -A1 -w "name = \"quilkin\"" $(project_path)/Cargo.toml | grep version -m 1 | awk '{print $$3}')-${git_sha}
endif

# Set this value if you want to use an external registry
REPOSITORY ?= ""
IMAGE_TAG ?= ${REPOSITORY}quilkin:$(package_version)
PREV_IMAGE_TAG ?= us-docker.pkg.dev/quilkin/release/quilkin:0.8.0
MINIKUBE_PROFILE ?= quilkin
CARGO_TARGET_DIR ?= /workspace/target/build-image
common_rust_args := -v $(project_path):/workspace -w /workspace \
					-v $(CARGO_HOME)/registry:/usr/local/cargo/registry \
					-e "CARGO_TARGET_DIR=$(CARGO_TARGET_DIR)"

KUBECONFIG ?= ~/.kube/config
kubeconfig_path := $(dir $(KUBECONFIG))
helm_config := ~/.config/helm
helm_cache := ~/.cache/helm

kube_mount_args := -v $(kubeconfig_path):/root/.kube -v $(helm_config):/root/.config/helm -v $(helm_cache):/root/.cache/helm
minikube_args := --network=host -v ~/.minikube:$(HOME)/.minikube
gcloud_mount_args := -v $(build_path)/.config/gcloud:/root/.config/gcloud

cargo_build_x86_64_linux := build --profile=lto --target x86_64-unknown-linux-gnu
cargo_build_x86_64_apple := build --release --target x86_64-apple-darwin
cargo_build_aarch64-apple := build --release --target aarch64-apple-darwin
cargo_build_x86_64_windows := build --release --target x86_64-pc-windows-gnu

#   _____                    _
#  |_   _|_ _ _ __ __ _  ___| |_ ___
#    | |/ _` | '__/ _` |/ _ \ __/ __|
#    | | (_| | | | (_| |  __/ |_\__ \
#    |_|\__,_|_|  \__, |\___|\__|___/
#                 |___/

help: ensure-multi-arch
	@cat $(MAKEFILE_LIST) | docker run --rm -i xanders/make-help

ensure-multi-arch:
	@docker run --privileged --rm tonistiigi/binfmt --install linux/amd64,linux/arm64 > /dev/null 2>&1

# output the current build version
version:
	@echo $(package_version)

# Run all tests
test: ensure-build-image

# In CI with split jobs that both fetch they will fail if run in parallel since
# cargo will be fighting with itself for some the same host directory that is
# mapped into the container, so instead just split it out to its own job
fetch-quilkin: ensure-build-image
	docker run --rm $(common_rust_args)  \
			--network=host \
			-e RUST_BACKTRACE=1 --entrypoint=cargo $(BUILD_IMAGE_TAG) fetch

# Build all binaries, images and related artifacts
build: binary-archive build-image

# Build all binaries
build-all-binaries: ensure-build-image build-linux-binary build-macos-binary build-windows-binary

# Checks for changes to protobuf definitions and generates rust code and reference documentation if there is any.
gen-protobuf: ensure-build-image
	docker run --rm $(DOCKER_RUN_ARGS) $(common_rust_args) \
		--entrypoint=cargo $(BUILD_IMAGE_TAG) run -p proto-gen -- generate

# Build an archive all binaries
binary-archive: ensure-build-image build-licence-report build-all-binaries
	docker run --rm $(common_rust_args) -w $(CARGO_TARGET_DIR) \
			--entrypoint=bash $(BUILD_IMAGE_TAG) -c 'cp ../../license.html . && zip ../../quilkin-$(package_version).zip ./*/lto/quilkin ./*/lto/quilkin.exe ./license.html'

# Build binary for x86_64-unknown-linux-gnu.
# Use BUILD_LOCAL=1 to build through local cargo rather than through the build container.
build-linux-binary: ensure-build-image gen-protobuf
ifdef BUILD_LOCAL
	cargo $(cargo_build_x86_64_linux)
else
	docker run --rm $(common_rust_args) -e "CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER=/usr/bin/x86_64-linux-gnu-gcc" \
		--entrypoint=cargo $(BUILD_IMAGE_TAG) $(cargo_build_x86_64_linux)
endif

# Build binary for x86_64-pc-windows-gnu
# Use BUILD_LOCAL=1 to build through local cargo rather than through the build container.
build-windows-binary: ensure-build-image gen-protobuf
ifdef BUILD_LOCAL
	cargo $(cargo_build_x86_64_windows)
else
	docker run --rm $(common_rust_args) -e "CARGO_TARGET_X86_64_PC_WINDOWS_GNU_LINKER=/usr/bin/x86_64-w64-mingw32-gcc" \
		--entrypoint=cargo $(BUILD_IMAGE_TAG) $(cargo_build_x86_64_windows)
endif

# Build binary for x86_64-apple-darwin and aarch64-apple-darwin
# Use BUILD_LOCAL=1 to build through local cargo rather than through the build container.
build-macos-binary: gen-protobuf
ifdef BUILD_LOCAL
	cargo $(cargo_build_x86_64_apple)
	cargo $(cargo_build_aarch64-apple)
else
	docker run --rm -v $(project_path):/workspace -w /workspace \
		-v $(CARGO_HOME)/registry:/root/.cargo/registry \
		-e "CARGO_TARGET_DIR=$(CARGO_TARGET_DIR)" \
		-e "CC=o64-clang" -e "CXX=o64-clang++" \
		-e "PROTOC=/opt/protoc/bin/protoc" \
		joseluisq/rust-linux-darwin-builder:$(rust_toolchain) \
			sh -c "rustup target add x86_64-apple-darwin aarch64-apple-darwin && \
			cargo $(cargo_build_x86_64_apple) --no-default-features && \
			CC=oa64-clang CXX=oa64-clang++ LIBZ_SYS_STATIC=1 cargo $(cargo_build_aarch64-apple) --no-default-features"
endif

# Build container image.
# Use either `REPOSITORY` to specify a container repository (defaults to blank/none), or use `IMAGE_TAG` argument to specify
# the entire image name and tag. Defaults to `quilkin:${version}-${git-sha}`.
# Use BUILD_LOCAL=1 to build the binary through local cargo rather than through the build container.
build-image: ensure-build-image build-licence-report build-linux-binary
build-image:
	-mkdir -p "$(project_path)/target/image/"
ifdef BUILD_LOCAL
	cp "$(project_path)/target/x86_64-unknown-linux-gnu/lto/quilkin" "$(project_path)/target/image/"
else
	cp "$(project_path)/target/build-image/x86_64-unknown-linux-gnu/lto/quilkin" "$(project_path)/target/image/"
endif
	docker run --rm $(common_rust_args) \
		--entrypoint=bash $(BUILD_IMAGE_TAG) -c './image/archive_dependencies.sh'
	docker build --platform=linux/amd64 -t $(IMAGE_TAG) -f $(project_path)/image/Dockerfile $(project_path)

# Generates the HTML report of all open source licence dependencies
build-licence-report: ensure-build-image
build-licence-report:
	docker run --rm $(common_rust_args) \
		--entrypoint=bash $(BUILD_IMAGE_TAG) -c 'cargo about generate license.html.hbs > license.html'

# Builds Quilkin, pushes it to a repository (use REPOSITORY arg to set value)
# and then runs the Agones integration tests. See targets `build-images` and `push` for more options and details.
# Not part of `test` as it requires a Kubernetes cluster to be provisioned and running.
# To pass extra arguments to `cargo test`, to run only a single test, for example, use the `ARGS` variable
# to set those options.
# If a `kubectl` authentication failure occurs, run `kubectl get ns` to confirm access and refresh the Kubernetes
# authentication token, and try again if successful.
test-agones: push
test-agones:
	$(MAKE) run-test-agones

run-test-agones: ensure-kube-dirs
run-test-agones:
	docker image pull ${PREV_IMAGE_TAG}
	docker run --rm $(DOCKER_RUN_ARGS) $(common_rust_args) -w /workspace/crates/agones  \
			--entrypoint=cargo $(BUILD_IMAGE_TAG) clippy --tests -- -D warnings
	docker run --rm $(DOCKER_RUN_ARGS) $(common_rust_args) -w /workspace/crates/agones \
		--entrypoint=cargo $(BUILD_IMAGE_TAG) fmt -- --check
	# Confirm access to K8s, and force refresh of the auth token for the kube context
	docker run --rm $(DOCKER_RUN_ARGS) $(common_rust_args) $(kube_mount_args) -w /workspace/crates/agones \
		--entrypoint=kubectl $(BUILD_IMAGE_TAG) get ns
	docker run --rm $(DOCKER_RUN_ARGS) $(common_rust_args) $(kube_mount_args) -w /workspace/crates/agones \
			-e "RUST_BACKTRACE=1" -e "IMAGE_TAG=${IMAGE_TAG}" -e "PREV_IMAGE_TAG=${PREV_IMAGE_TAG}" --entrypoint=cargo $(BUILD_IMAGE_TAG) test $(ARGS)

# Convenience target to build and push quilkin images to a repository.
# Use `REPOSITORY` arg to specify the repository to push to.
# USe `SKIP_BUILD_IMAGE` if you want to skip building the image if it has been already built.
# See `build-image` for more details.
push:
ifndef SKIP_BUILD_IMAGE
push: build-image
endif
	docker push $(IMAGE_TAG)

# Convenience target to build and push quilkin images into a minikube instance
# Use `MINIKUBE_PROFILE` to specify the profile. Defaults to `quilkin`.
minikube-push: build-image
	minikube image load $(IMAGE_TAG) -p $(MINIKUBE_PROFILE)

# Builds Quilkin, pushes it to a minikube instance (use `MINIKUBE_PROFILE` to specify the profile. Defaults to `quilkin`)
# and then runs the Agones integration tests. See targets `build-images` and `minikube-push` for more options and details.
# Not part of `test` as it requires a Kubernetes cluster to be provisioned and running.
# To pass extra arguments to `cargo test`, to run only a single test, for example, use the `ARGS` variable
# to set those options.
minikube-test-agones: minikube-push
	$(MAKE) DOCKER_RUN_ARGS="$(minikube_args)" run-test-agones

# Start an interactive shell inside the build image
# Useful for testing, or adhoc cargo, gcloud, kubectl or other commands
shell: ensure-gcloud-dirs ensure-kube-dirs ensure-build-image
	# we --network=host because docker containers are not great at ipv6.
	docker run --rm -it $(DOCKER_RUN_ARGS) $(common_rust_args) \
		$(gcloud_mount_args) $(kube_mount_args) \
		 --network=host \
		 --entrypoint=bash $(BUILD_IMAGE_TAG)

ensure-build-image: ensure-cargo-registry
	docker buildx build $(BUILD_IMAGE_ARG) --cache-to type=inline --cache-from type=registry,ref=$(BUILD_IMAGE_TAG) --build-arg RUST_TOOLCHAIN=$(rust_toolchain) --tag=$(BUILD_IMAGE_TAG) $(build_path)/build-image/

ensure-cargo-registry:
	-mkdir -p $(CARGO_HOME)/registry

ensure-gcloud-dirs:
	-mkdir -p $(build_path)/.gcloud

ensure-kube-dirs:
	-mkdir -p ~/.config/helm
	-mkdir -p ~/.kube

ci-gcloud-auth-cluster:
	# Internal CI target. Used to authenticate against the integration test cluster.
	docker run --rm $(DOCKER_RUN_ARGS) $(common_rust_args) $(kube_mount_args) --network=cloudbuild \
		   -e "USE_GKE_GCLOUD_AUTH_PLUGIN=True" --entrypoint=gcloud $(BUILD_IMAGE_TAG) container clusters get-credentials agones --zone us-west1-c
