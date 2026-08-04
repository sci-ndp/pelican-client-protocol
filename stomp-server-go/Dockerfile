FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY *.go .
RUN CGO_ENABLED=0 go build -o /out/stomp-server-go .

FROM alpine:3.20
COPY --from=build /out/stomp-server-go /usr/local/bin/stomp-server-go
CMD ["stomp-server-go", "--port", "8080"]
