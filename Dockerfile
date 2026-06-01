# ─── Build stage ───────────────────────────────────────────────────
FROM rust:1.84-alpine AS builder

RUN apk add --no-cache musl-dev pkgconfig openssl-dev

WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY src ./src

# Build with static linking for maximum portability
RUN cargo build --release && \
    cp target/release/ghostsync /ghostsync && \
    strip /ghostsync

# ─── Runtime stage ─────────────────────────────────────────────────
FROM alpine:3.21

RUN apk add --no-cache ca-certificates tzdata

COPY --from=builder /ghostsync /usr/local/bin/ghostsync

EXPOSE 9710

ENTRYPOINT ["ghostsync"]
CMD ["--help"]
