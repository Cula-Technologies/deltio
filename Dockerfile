FROM --platform=$BUILDPLATFORM rust:1.98 AS build

# Install Protocol Buffers and curl.
RUN apt-get update && apt-get install -y protobuf-compiler curl xz-utils

# Install Zig (used by cargo-zigbuild for cross-compilation).
ARG ZIG_VERSION=0.14.0
RUN ARCH=$(uname -m) && \
    mkdir -p /opt/zig && \
    curl -sSL "https://ziglang.org/download/${ZIG_VERSION}/zig-linux-${ARCH}-${ZIG_VERSION}.tar.xz" \
    | tar -xJ -C /opt/zig --strip-components=1
ENV PATH="/opt/zig:${PATH}"

# Install cargo-zigbuild.
RUN cargo install --locked cargo-zigbuild

# Create a new empty project.
RUN cargo new --bin deltio
WORKDIR /deltio

# The target platform we are compiling for.
# Populated by BuildX
ARG TARGETPLATFORM

# The build platform we are compiling on.
# Populated by BuildX
ARG BUILDPLATFORM

# Add the required Rust target based on the target platform.
RUN <<EOF
  set -e;
  touch .target

  if [ "$TARGETPLATFORM" = "linux/arm64" ]; then
    rustup target add aarch64-unknown-linux-musl
    echo -n "aarch64-unknown-linux-musl" > .target
  elif [ "$TARGETPLATFORM" = "linux/amd64" ]; then
    rustup target add x86_64-unknown-linux-musl
    echo -n "x86_64-unknown-linux-musl" > .target
  elif [ "$TARGETPLATFORM" = "linux/386" ]; then
    rustup target add i686-unknown-linux-musl
    echo -n "i686-unknown-linux-musl" > .target
  fi
EOF

# Copy manifests.
COPY ./.cargo/config.toml ./.cargo/config.toml
COPY ./Cargo.lock ./Cargo.lock
COPY ./Cargo.toml ./Cargo.toml

# Copy bench sources so Cargo can validate the [[bench]] targets
# in the manifest during the dependency caching step.
COPY ./benches ./benches

# Build the shell project first to get a dependency cache.
RUN <<EOF
  set -e;
  TARGET=$(cat .target)

  if [ -z "$TARGET" ]; then
    cargo build --release
    rm ./target/release/deps/deltio*
  else
    cargo zigbuild --target "$TARGET" --release
    rm ./target/*/release/deps/deltio*
  fi

  # Remove the shell project's code files.
  rm src/*.rs
EOF

# Copy the actual source.
COPY ./build.rs ./build.rs
COPY ./proto ./proto
COPY ./src ./src

# Build for release
RUN <<EOF
  set -e;
  TARGET=$(cat .target)

  if [ -z "$TARGET" ]; then
    cargo build --release
    exit 0
  fi

  cargo zigbuild --target "$TARGET" --release
  mv "target/$TARGET/release/deltio" "target/release/deltio"
EOF

# Our final base image.
FROM scratch AS deltio

# Copy the build artifact from the build stage
COPY --from=build /deltio/target/release/deltio .

# Expose the gRPC port and the Prometheus metrics port.
EXPOSE 8085 9091

# Set the startup command to run the binary.
CMD ["./deltio", "--bind", "0.0.0.0:8085"]
