FROM rust:1.67

WORKDIR /usr/src/lightstream
COPY . .

RUN cargo install --path .

CMD ["lightstream"]