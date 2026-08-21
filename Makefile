.PHONY: all test race vet bench up down tidy

all: test race vet

tidy:
	go mod tidy

test:
	go test ./...

race:
	go test -race ./...

vet:
	go vet ./...

bench:
	go test -bench=. -benchmem ./...

up:
	docker compose up -d

down:
	docker compose down -v