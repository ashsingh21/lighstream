ARG FDB_VERSION
FROM foundationdb/foundationdb:${FDB_VERSION} as fdb
FROM silkeh/clang as clang
FROM rust:1.67
ARG FDB_VERSION

WORKDIR /tmp

RUN apt-get update
# dnsutils is needed to have dig installed to create cluster file
RUN apt-get install -y --no-install-recommends ca-certificates dnsutils
RUN apt-get install -y libclang-dev
RUN apt-get install -y protobuf-compiler

RUN wget "https://github.com/apple/foundationdb/releases/download/${FDB_VERSION}/foundationdb-clients_${FDB_VERSION}-1_amd64.deb"
RUN dpkg -i foundationdb-clients_${FDB_VERSION}-1_amd64.deb

WORKDIR /usr/src/lightstream
COPY . .

COPY start.bash /start.bash

RUN cargo install --bin lightstream --path .

RUN lightstream