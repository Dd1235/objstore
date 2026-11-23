.PHONY: build test acceptance acceptance-docker vet fmt up down clean logs ps workers

build:
	go build -o bin/ ./cmd/...

test:
	go test -race ./...

# End-to-end assertions for every requirement of the project statement.
# Starts its own controller and four workers; needs nothing but Go.
acceptance:
	./scripts/acceptance.sh

# The same assertions against the Docker Compose cluster (run "make up" first).
acceptance-docker:
	./scripts/acceptance.sh -docker

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
