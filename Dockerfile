FROM golang:1.24-alpine AS build

ENV GO111MODULE=on

RUN apk add --no-cache git make bash curl vim
RUN git config --global http.postBuffer 524288000

WORKDIR /kubo

# COPY build/disable-blockstore-arc-cache.patch /patches/

RUN git clone https://github.com/ipfs/kubo . && git checkout v0.36.0
# && git apply /patches/disable-blockstore-arc-cache.patch

COPY . /kubo/ipfs-go-ds-storj

# Build the kubo binary with the Storj datastore plugin from the current source code.
RUN echo "Adding the storjds plugin.."
RUN echo "" >> plugin/loader/preload_list
RUN echo "storjds storj.io/ipfs-go-ds-storj/plugin 0" >> plugin/loader/preload_list
RUN echo "Get plugin list to verify.."
RUN cat plugin/loader/preload_list

RUN go mod edit -replace storj.io/ipfs-go-ds-storj=./ipfs-go-ds-storj && \
    go mod tidy && \
    # Next line is expected to fail
    make build || true && \
    go mod tidy && \
    make build

# Target image

FROM ipfs/kubo:v0.36.0

# Copy the ipfs from the build container.
ENV SRC_DIR /kubo
COPY --from=build $SRC_DIR/cmd/ipfs/ipfs /usr/local/bin/ipfs
COPY --from=build $SRC_DIR/ipfs-go-ds-storj/docker/container_daemon /usr/local/bin/start_ipfs

# Fix permissions on start_ipfs (ignore the build machine's permissions)
RUN chmod 0755 /usr/local/bin/start_ipfs
