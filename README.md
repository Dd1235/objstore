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
Liveness timeouts can be tuned with `SUSPECT_AFTER`, `DEAD_AFTER` and `REJOIN_AFTER`, and repair timing
with `REPAIR_INTERVAL` (5s) and `REPAIR_DELAY` (20s, the wait before the first pass after start).

## Using the API

```sh
API=localhost:8090/api/v1/objects

# Upload (multipart; metadata is optional). Returns the object ID, chunks and replica locations.
curl -s -F 'metadata={"owner":"alice"}' -F file=@video.mp4 $API

# Download and check it matches
curl -s -o copy.mp4 $API/<id>
shasum -a 256 video.mp4 copy.mp4

# Metadata: get, then add/replace keys (null removes a key) without re-uploading
curl -s $API/<id>/metadata
curl -s -X PATCH -d '{"status":"reviewed","owner":null}' $API/<id>/metadata

# List (oldest first; ?limit=&offset= for paging) and delete
curl -s $API
curl -s -X DELETE $API/<id>

# Where the chunks are, and whether every chunk has its 2 copies
curl -s localhost:8090/internal/workers/worker1/chunks
curl -s localhost:8090/internal/replication
```

### Worker failure

Upload something a few MiB or larger first, so that `worker2` holds some of its chunks. Then watch
`/internal/replication` (`replication_maintained`, the per-status chunk counts, and `copies` per worker):

```sh
STATUS=localhost:8090/internal/replication
curl -s $STATUS          # before: replication_maintained true, every chunk healthy
docker stop worker2
curl -s $STATUS          # 3 s: worker2 suspect, its chunks at_risk (grace period, nothing copied yet)
curl -s $API/<id> -o x   # downloads still work from the other copy
curl -s $STATUS          # 20 s: worker2 dead, its chunks under_replicated
curl -s $STATUS          # within 5 s more: copied to other workers, replication_maintained true again
docker start worker2
curl -s $STATUS          # rejoining for 10 s, then alive: the extra copies are trimmed back to 2
docker logs controller   # state changes, "chunk copied" and "copy dropped" with reasons
```

### Data corruption

Workers run on distroless images, so corrupt a chunk through a helper container that mounts the
worker's volume:

```sh
CHUNK=<object id>_000000
docker run --rm -v objstore_worker1-data:/data alpine \
  sh -c "printf '\x00\x00\x00\x00' | dd of=/data/chunks/$CHUNK bs=1 seek=5000 conv=notrunc"
curl -s -o copy.bin $API/<object id>   # still correct: served from the other copy
docker logs controller | grep -E 'bad replica|repaired'   # the bad copy was rewritten
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
| `internal/placement` | Rendezvous hashing for chunk placement |
| `internal/repair` | Repair planner: chunk status and the copy, adopt and drop actions that restore 2 copies |
| `internal/meta` | Object metadata store (bbolt on disk, in-memory for tests) |
| `internal/controller` | Client REST API, upload/download/delete paths, membership driver, repair loop |
| `internal/workerclient` | Controller's client for the worker chunk API |
| `internal/worker` | Worker HTTP server, heartbeat agent, scrubber |
| `internal/worker/store` | On-disk chunk store with checksum verification |
| `internal/envflag` | Flag defaults from environment variables |

## Status

- [x] Chunking, identifiers, worker chunk store with checksum verification
- [x] Worker internal API, registration, heartbeats, background scrubber
- [x] Membership with grace period and rejoin delay
- [x] Docker Compose deployment
- [x] Client REST API: upload, download, metadata get/patch, delete, list
- [x] Durable controller metadata (bbolt)
- [x] Reads fall back to the other copy; corrupt or missing copies repaired on read
- [x] Repair loop: re-replicate chunks of dead workers, reconcile returning workers, trim surplus
- [x] Replication status endpoint for the failure demo
- [ ] Garbage collection of orphaned chunks and stale pending uploads
- [ ] Client CLI and demo scripts
