# Copyright 2021 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License") &&
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

FROM debian:bookworm

ARG RUST_TOOLCHAIN

ENV RUSTUP_HOME=/usr/local/rustup \
    CARGO_HOME=/usr/local/cargo \
    PATH=/usr/local/cargo/bin:$PATH \
    CARGO_TERM_COLOR=always \
    LC_ALL=C.UTF-8 \
    LANG=C.UTF-8

# Install packages
RUN set -eux && \
    apt-get update && \
    apt-get install -y lsb-release jq curl wget zip git build-essential software-properties-common protobuf-compiler \
        libssl-dev pkg-config bash-completion g++-x86-64-linux-gnu g++-mingw-w64-x86-64 && \
    echo "source /etc/bash_completion" >> /root/.bashrc

# install protoc-gen-doc
RUN cd /tmp && \
    wget --quiet -O protoc-gen-doc.tar.gz https://github.com/pseudomuto/protoc-gen-doc/releases/download/v1.5.1/protoc-gen-doc_1.5.1_linux_amd64.tar.gz && \
    tar -xf protoc-gen-doc.tar.gz && \
    mv ./protoc-gen-doc /usr/local/bin/ && \
    rm protoc-gen-doc.tar.gz

# install gcloud
# Credit: https://cloud.google.com/sdk/docs/install#deb
RUN echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list && curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key --keyring /usr/share/keyrings/cloud.google.gpg  add - && apt-get update -y && \
    apt-get install google-cloud-cli google-cloud-cli-gke-gcloud-auth-plugin kubectl -y && \
    echo "source /usr/share/google-cloud-sdk/completion.bash.inc" >> /root/.bashrc && \
    echo "source <(kubectl completion bash)" >> /root/.bashrc

# install terraform. There is no apt support for arm64, so direct downloading instead.
RUN wget --quiet -O terraform.zip "https://releases.hashicorp.com/terraform/1.5.4/terraform_1.5.4_linux_$(dpkg --print-architecture).zip" && \
    unzip terraform.zip && rm terraform.zip && mv terraform /usr/local/bin/

# install helm
# Credit: https://helm.sh/docs/intro/install/
RUN curl https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash && \
    echo "source <(helm completion bash)" >> /root/.bashrc

# Add /workspace as a git safe directory
RUN git config --global --add safe.directory /workspace

# Install Rust. Inspiration from: https://github.com/rust-lang/docker-rust/blob/master/1.64.0/bullseye/Dockerfile
RUN set -eux; \
    dpkgArch="$(dpkg --print-architecture)" && \
    case "${dpkgArch##*-}" in \
        amd64) rustArch='x86_64-unknown-linux-gnu';; \
        armhf) rustArch='armv7-unknown-linux-gnueabihf';; \
        arm64) rustArch='aarch64-unknown-linux-gnu';; \
        i386) rustArch='i686-unknown-linux-gnu';; \
        *) echo >&2 "unsupported architecture: ${dpkgArch}"; exit 1 ;; \
    esac && \
    wget --quiet "https://static.rust-lang.org/rustup/dist/${rustArch}/rustup-init" && \
    chmod +x rustup-init; \
    ./rustup-init -y --no-modify-path --profile minimal --default-toolchain $RUST_TOOLCHAIN --default-host ${rustArch}; \
    rm rustup-init && \
    chmod -R a+w $RUSTUP_HOME $CARGO_HOME && \
    rustup component add rustfmt clippy && \
    rustup target add x86_64-unknown-linux-gnu aarch64-unknown-linux-gnu x86_64-pc-windows-gnu && \
    cargo install --locked cargo-about proto-gen && \
    rustup --version && \
    cargo --version && \
    rustc --version
