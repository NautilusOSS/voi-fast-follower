.PHONY: test race build docker conduit compose-prod tidy

test:
	go test ./... -count=1

race:
	go test ./internal/follower ./internal/storage ./internal/stream ./internal/health ./internal/config -race -count=1

build:
	mkdir -p bin
	go build -o bin/follower ./cmd/follower
	go build -o bin/replay ./cmd/replay
	go build -o bin/verify ./cmd/verify
	go build -o bin/bootstrap ./cmd/bootstrap

conduit:
	cd plugins/conduit && go build -o ../../bin/conduit ./cmd/conduit

docker:
	docker build -t voi-fast-follower:local .

compose-prod:
	docker compose -f docker-compose.prod.yml up --build -d

tidy:
	go mod tidy
	cd plugins/conduit && go mod tidy
