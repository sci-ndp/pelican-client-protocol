FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd ./cmd
COPY internal ./internal
RUN CGO_ENABLED=0 go build -o /out/stomp-server-go ./cmd/messagequeue

FROM alpine:3.20
# Pinned to the latest stable pelican CLI release (statically linked, so it
# runs fine on Alpine's musl libc with no extra runtime deps) -- the
# messagequeue app's PELICAN_URL event source shells out to this binary to
# list a federation directory.
ARG PELICAN_VERSION=7.26.0
RUN apk add --no-cache curl && \
    ARCH="$(uname -m)" && \
    case "$ARCH" in \
      x86_64) PELICAN_ARCH=x86_64 ;; \
      aarch64) PELICAN_ARCH=arm64 ;; \
      *) echo "unsupported architecture: $ARCH" >&2; exit 1 ;; \
    esac && \
    curl -sL "https://github.com/PelicanPlatform/pelican/releases/download/v${PELICAN_VERSION}/pelican_Linux_${PELICAN_ARCH}.tar.gz" \
      -o /tmp/pelican.tar.gz && \
    tar -xzf /tmp/pelican.tar.gz -C /tmp && \
    mv "/tmp/pelican-${PELICAN_VERSION}/pelican" /usr/local/bin/pelican && \
    rm -rf /tmp/pelican.tar.gz "/tmp/pelican-${PELICAN_VERSION}" && \
    apk del curl
RUN mkdir -p /data
COPY --from=build /out/stomp-server-go /usr/local/bin/stomp-server-go
CMD ["stomp-server-go", "--port", "8080"]
