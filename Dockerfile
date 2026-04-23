FROM golang:1.23-bookworm as builder

WORKDIR /app

COPY . .

RUN go build -o checker cmd/checker/*.go

FROM ubuntu:24.04

RUN apt update && apt install -y ca-certificates wget
RUN update-ca-certificates

WORKDIR /app

COPY --from=builder /app/checker /app

ENTRYPOINT ["/app/checker"]
