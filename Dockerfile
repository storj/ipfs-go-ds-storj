FROM golang:1.16.12-buster as build

ENV GO111MODULE=on

WORKDIR /go-ipfs

RUN git clone https://github.com/ipfs/go-ipfs . && \
    git checkout v0.12.1

COPY . /go-ipfs/ipfs-go-ds-storj

# Build the go-ipfs binary with the Storj datastore plugin from the current source code.
RUN go get github.com/kaloyan-raev/ipfs-go-ds-storj/plugin && \
    go mod edit -replace github.com/kaloyan-raev/ipfs-go-ds-storj/plugin=./ipfs-go-ds-storj/plugin && \
    echo -e "\nstorjds github.com/kaloyan-raev/ipfs-go-ds-storj/plugin 0" >> "plugin/loader/preload_list;" && \
    go mod tidy && \
    make build

# Target image
FROM ipfs/go-ipfs:v0.12.1

# Copy the ipfs from the build container.
ENV SRC_DIR /go-ipfs
COPY --from=build $SRC_DIR/cmd/ipfs/ipfs /usr/local/bin/ipfs