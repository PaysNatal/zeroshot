# Layer-1 image: the capsule-workspace binary + the cache_stress example, built for linux/arm64
# (kind on Apple-Silicon Docker Desktop runs arm64 natively; also matches the Graviton EKS target).
FROM rust:1-bookworm AS build
WORKDIR /src
COPY . .
# s3 feature so a pod can use MinIO as the durable backing behind the node-local cache.
RUN cargo build --release --features s3 --bin capsule-workspace-core --example cache_stress

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /src/target/release/capsule-workspace-core /usr/local/bin/capsule-workspace
COPY --from=build /src/target/release/examples/cache_stress /usr/local/bin/cache_stress
