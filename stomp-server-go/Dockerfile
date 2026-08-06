FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd ./cmd
COPY internal ./internal
RUN CGO_ENABLED=0 go build -o /out/stomp-server-go ./cmd/messagequeue

FROM alpine:3.20
RUN mkdir -p /data
COPY --from=build /out/stomp-server-go /usr/local/bin/stomp-server-go
CMD ["stomp-server-go", "--port", "8080"]
