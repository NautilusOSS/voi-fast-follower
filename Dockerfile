FROM golang:1.25-alpine AS build
WORKDIR /src
RUN apk add --no-cache git ca-certificates
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/follower ./cmd/follower

FROM alpine:3.21
RUN apk add --no-cache ca-certificates tzdata
WORKDIR /app
COPY --from=build /out/follower /app/follower
COPY migrations /app/migrations
COPY web /app/web
COPY config.example.yaml /app/config.example.yaml
ENV WEB_PATH=/app/web
EXPOSE 9090
ENTRYPOINT ["/app/follower"]
