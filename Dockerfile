# --- base image ---
FROM debian:bookworm AS chef
SHELL ["/bin/bash", "-o", "pipefail", "-c"]

RUN set -eux && \
    apt-get update && \
    apt-get install -y --no-install-recommends \
        build-essential \
        ca-certificates \
        curl \
        g++-x86-64-linux-gnu \
        git \
        libssl-dev \
        pkg-config \
        protobuf-compiler \
        wget \
        zip \
    && rm -rf /var/lib/apt/lists/*

ENV MISE_DATA_DIR="/mise"
ENV MISE_CONFIG_DIR="/mise"
ENV MISE_CACHE_DIR="/mise/cache"
ENV MISE_INSTALL_PATH="/usr/local/bin/mise"
ENV PATH="/mise/shims:$PATH"
RUN curl https://mise.run | sh
RUN --mount=type=secret,id=github_token \
    if [ -f /run/secrets/github_token ]; then \
      export GITHUB_API_TOKEN=$(cat /run/secrets/github_token); \
    fi && \
    mise use -g github:LukeMathWalker/cargo-chef rust github:EmbarkStudios/cargo-about github:EmbarkStudios/proto-gen

# --- Plan: extract dependency recipe ---
FROM chef AS planner
COPY . /workspace
WORKDIR /workspace
RUN cargo chef prepare --recipe-path recipe.json

# --- Cook: build dependencies (cached when Cargo.lock unchanged) ---
FROM chef AS cook
ENV CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER=/usr/bin/x86_64-linux-gnu-gcc
COPY --from=planner /workspace/recipe.json /workspace/recipe.json
WORKDIR /workspace
RUN cargo chef cook --profile lto --target x86_64-unknown-linux-gnu --recipe-path recipe.json

# --- Build quilkin ---
FROM chef AS builder
ENV CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER=/usr/bin/x86_64-linux-gnu-gcc
COPY --from=cook /workspace/target /workspace/target
COPY . /workspace
WORKDIR /workspace
RUN cargo about generate license.html.hbs > license.html
RUN cargo run -p proto-gen -- generate
RUN cargo build --profile=lto --target x86_64-unknown-linux-gnu
# Archive source of MPL/GPL/LGPL/CDDL licensed dependencies for inclusion in the image.
# Review this list before each release.
RUN set -eo pipefail && \
    dependencies=(webpki-roots) && \
    zip="$(pwd)/dependencies-src.zip" && \
    echo UEsFBgAAAAAAAAAAAAAAAAAAAAAAAA== | base64 -d > "$zip" && \
    pushd "${CARGO_HOME:-$HOME/.cargo}/registry/src" && \
    for d in "${dependencies[@]}"; do \
      find . -type d -name "$d-*" | xargs -I {} zip -ruv "$zip" "{}"; \
    done && \
    popd

# --- Runtime image ---
FROM debian:bookworm-slim

WORKDIR /

RUN groupadd --gid 65532 nonroot && \
    useradd --create-home --uid 65532 --gid 65532 --shell /bin/bash nonroot && \
    apt-get update && \
    apt-get install --no-install-recommends -y ca-certificates && \
    rm -rf /var/lib/apt/lists/*

COPY --from=builder /workspace/license.html .
COPY --from=builder /workspace/dependencies-src.zip .
COPY --from=builder --chown=nonroot:nonroot /workspace/target/x86_64-unknown-linux-gnu/lto/quilkin .

USER nonroot
ENTRYPOINT ["/quilkin"]
