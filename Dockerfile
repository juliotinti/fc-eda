FROM golang:1.26-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG SERVICE=walletcore
RUN CGO_ENABLED=1 go build -o /service ./cmd/${SERVICE}

FROM debian:bookworm-slim
COPY --from=build /service /usr/local/bin/service
ENTRYPOINT ["/usr/local/bin/service"]
