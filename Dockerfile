FROM golang:1.25-alpine AS build
WORKDIR /src
RUN apk add --no-cache git ca-certificates
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/follower ./cmd/follower

FROM alpine:3.21
RUN apk add --no-cache ca-certificates tzdata wget su-exec \
  && adduser -D -H -u 10001 follower
WORKDIR /app
COPY --from=build /out/follower /app/follower
COPY migrations /app/migrations
COPY web /app/web
COPY config.example.yaml /app/config.example.yaml
COPY deploy/docker-entrypoint.sh /app/docker-entrypoint.sh
RUN chmod +x /app/docker-entrypoint.sh \
  && mkdir -p /var/lib/voi-fast-follower/archive \
  && chown -R follower:follower /var/lib/voi-fast-follower /app
ENV WEB_PATH=/app/web \
    MIGRATIONS_PATH=/app/migrations \
    ARCHIVE_PATH=/var/lib/voi-fast-follower/archive
EXPOSE 9090
ENTRYPOINT ["/app/docker-entrypoint.sh"]
