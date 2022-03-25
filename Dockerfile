FROM golang:1.16.12-buster

ENV GO111MODULE=on

WORKDIR /go-ipfs

RUN git clone https://github.com/ipfs/go-ipfs . && \
    git checkout v0.12.1 && \
    go get github.com/kaloyan-raev/ipfs-go-ds-storj/plugin@main && \
    echo -e "\nstorjds github.com/kaloyan-raev/ipfs-go-ds-storj/plugin 0" >> "plugin/loader/preload_list;" && \
    go mod tidy && \
    make build

# Target image
FROM ipfs/go-ipfs:v0.12.1

# Copy the IPFS binary with Storj plugin from the build container.
ENV SRC_DIR /go-ipfs
COPY --from=0 $SRC_DIR/cmd/ipfs/ipfs /usr/local/bin/ipfs