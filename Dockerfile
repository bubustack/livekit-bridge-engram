# --- Build Stage ---
FROM golang:1.26-bookworm AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential pkg-config \
    libopus-dev libopusfile-dev libsoxr-dev libogg-dev libgomp1 \
    && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /runtime-libs && \
    cp /lib/*/libopus.so.*      /runtime-libs/ && \
    cp /lib/*/libopusfile.so.*  /runtime-libs/ && \
    cp /lib/*/libsoxr.so.*      /runtime-libs/ && \
    cp /lib/*/libogg.so.*       /runtime-libs/ && \
    cp /lib/*/libgomp.so.*      /runtime-libs/

WORKDIR /src

# Copy source code.
COPY . .

RUN go mod download

# Build the binary.
RUN CGO_ENABLED=1 GOOS=linux go build -ldflags="-s -w" -o livekit-bridge-engram .

# --- Final Stage ---
FROM gcr.io/distroless/cc-debian12:nonroot

COPY --from=builder /runtime-libs /runtime-libs
COPY --from=builder /src/livekit-bridge-engram /livekit-bridge-engram

ENV LD_LIBRARY_PATH=/runtime-libs

ENTRYPOINT ["/livekit-bridge-engram"]
