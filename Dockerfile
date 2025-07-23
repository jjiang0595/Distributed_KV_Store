FROM golang:1.24-alpine AS builder
LABEL authors="jerry"

WORKDIR /app

COPY go.mod go.sum ./

RUN go mod download

COPY . .
COPY config/docker/config-node1.yaml /app/config.yaml

RUN CGO_ENABLED=0 go build -o server -tags netgo -ldflags='-s -w' ./cmd/server

FROM alpine:latest

WORKDIR /app

COPY --from=builder /app/server .
COPY --from=builder /app/config.yaml /app/config.yaml

RUN adduser -D appuser
USER appuser

EXPOSE 8080
EXPOSE 6000

ENTRYPOINT ["./server", "-config", "/app/config.yaml"]