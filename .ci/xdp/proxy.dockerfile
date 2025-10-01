FROM debian:bookworm-slim
COPY ./target/debug/quilkin /usr/local/bin
ENTRYPOINT quilkin
