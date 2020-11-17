FROM golang:alpine AS builder
WORKDIR /go/src/github.com/filetrust/archive-adaptation-service
COPY . .
RUN cd cmd \
    && env GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o  archive-adaptation-service .

FROM scratch
COPY --from=builder /go/src/github.com/filetrust/archive-adaptation-service/cmd/archive-adaptation-service /bin/archive-adaptation-service

ENTRYPOINT ["/bin/archive-adaptation-service"]
