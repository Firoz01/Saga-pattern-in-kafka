
FROM golang:1.16-alpine as builder

ARG GIT_TAG_NAME

RUN apk --no-cache update && \
apk --no-cache add git gcc libc-dev
  
  # Kafka Go client is based on the C library librdkafka
ENV CGO_ENABLED 1
ENV GOFLAGS -mod=vendor
ENV GOOS=linux
ENV GOARCH=amd64

RUN export GO111MODULE=on

RUN go build -tags musl -ldflags "-s -w -X main.Version=$GIT_TAG_NAME" -o bin/main ./cmd/main