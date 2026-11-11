# objstore

A mini distributed cloud object store written in Go. One controller splits objects into 1 MiB chunks and
stores two copies of each chunk on four storage workers, detects worker failures and corrupted data, and
restores replication when it can. Everything runs locally with Docker Compose.

See [docs/design.md](docs/design.md) for the architecture, on-disk format and correctness rules.

## Requirements

- Go 1.25 or later
- Docker with the Compose plugin

## Quick start

```sh
make up          # build images, start the controller and 4 workers
make workers     # worker membership as JSON (controller on localhost:8090)
make logs        # follow all logs
make down        # stop, keeping stored data
make clean       # stop and delete all volumes
```

The controller is published on host port 8090; set `CONTROLLER_PORT` to change it.
Liveness timeouts can be tuned with `SUSPECT_AFTER`, `DEAD_AFTER` and `REJOIN_AFTER`.

### Worker failure

```sh
docker stop worker2     # suspect after 3 s, dead after 20 s
make workers
docker start worker2    # rejoining, then alive after 10 s of steady heartbeats
docker logs controller  # state transitions with reasons
```

## Development

```sh
make test     # go test -race ./...
make vet
make build    # binaries in ./bin
```

Run a controller and a worker directly, without Docker:

```sh
./bin/controller -listen :18080
./bin/worker -id w1 -listen :18081 -addr http://localhost:18081 -data ./data/w1 -controller http://localhost:18080
```

## Layout

| Path | Contents |
| --- | --- |
| `cmd/controller` | Controller binary |
| `cmd/worker` | Worker binary |
| `internal/api` | JSON types and error helpers shared by controller and workers |
| `internal/chunker` | Splits a stream into 1 MiB chunks with SHA-256 checksums |
| `internal/ids` | Object and chunk identifiers |
| `internal/membership` | Worker liveness state machine (alive / suspect / dead) |
| `internal/controller` | Controller HTTP server and background loops |
| `internal/worker` | Worker HTTP server, heartbeat agent, scrubber |
| `internal/worker/store` | On-disk chunk store with checksum verification |
| `internal/envflag` | Flag defaults from environment variables |

## Status

- [x] Chunking, identifiers, worker chunk store with checksum verification
- [x] Worker internal API, registration, heartbeats, background scrubber
- [x] Membership with grace period and rejoin delay
- [x] Docker Compose deployment
- [ ] Client REST API (upload, download, metadata, delete, list)
- [ ] Durable controller metadata
- [ ] Repair loop and garbage collection
- [ ] Client CLI and demo scripts
