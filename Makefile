.PHONY: build test vet fmt up down clean logs ps workers

build:
	go build -o bin/ ./cmd/...

test:
	go test -race ./...

vet:
	go vet ./...

fmt:
	gofmt -w .

# Build images and start the controller and four workers.
up:
	docker compose up -d --build

# Stop the cluster but keep stored data.
down:
	docker compose down

# Stop the cluster and delete all volumes (all stored objects).
clean:
	docker compose down -v
	rm -rf bin

logs:
	docker compose logs -f

ps:
	docker compose ps

workers:
	@curl -s localhost:$${CONTROLLER_PORT:-8090}/internal/workers
