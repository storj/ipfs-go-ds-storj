FROM golang:1.17.5

ENV GO111MODULE=on

WORKDIR /go-ipfs

RUN git clone https://github.com/ipfs/go-ipfs . && \
    git checkout v0.12.0 && \
    go get github.com/kaloyan-raev/ipfs-go-ds-storj/plugin@main && \
    echo -e "\nstorjds github.com/kaloyan-raev/ipfs-go-ds-storj/plugin 0" >> "plugin/loader/preload_list;" && \
    go mod tidy && \
    make build