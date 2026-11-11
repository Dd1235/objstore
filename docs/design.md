# Design notes

A distributed object store with 1 controller, 4 storage workers, 1 MiB chunks and replication factor 2,
deployed with Docker Compose. Clients talk only to the controller; the controller talks to workers over
an internal HTTP API.

```
          client (CLI / curl)
                 |
                 |  REST  /api/v1/objects...
                 v
          +-------------+   metadata (embedded store, fsync)
          | controller  |---------------------------------- /data (named volume)
          +-------------+
           |   ^  register / heartbeat
  PUT/GET/ |   |
  DELETE   v   |
   +---------+ +---------+ +---------+ +---------+
   | worker1 | | worker2 | | worker3 | | worker4 |   each: chunk files on its own named volume
   +---------+ +---------+ +---------+ +---------+
```

## Components

| Component | Responsibility |
| --- | --- |
| Controller | Client REST API, object metadata, chunk placement, worker membership, repair and garbage collection |
| Worker | Stores chunk files, verifies a SHA-256 on every read, scrubs in the background, reports inventory |
| Client CLI | Upload, download, metadata and listing commands against the controller |

## Core rule: deterministic state machines

Decision logic (membership, placement, upload state, repair planning, GC) is written as plain state
machines. They take an event plus the current time and return state changes and actions; they never read
the clock, spawn goroutines or do I/O themselves. HTTP servers, tickers and disk access are thin drivers
around them. This keeps the logic unit-testable without sleeps and lets a later simulation harness run the
same code under a seeded scheduler.

## Identifiers

- Object ID: random UUID (version 4). Objects are immutable; a new upload gets a new ID.
- Chunk ID: `<object id>_<index>` with the index zero-padded to 6 digits, e.g.
  `0b9f6c1e-2a7d-4c4e-9d0e-5c3a1f2b7e10_000003`. Because objects are immutable, a chunk ID always names
  the same bytes, so any two valid copies are interchangeable.
- Chunk size: 1 MiB (1,048,576 bytes); the last chunk may be shorter. An empty object has zero chunks.

## Worker storage

Each chunk is one file, `/data/chunks/<chunk id>`, written to `/data/tmp/` first, fsynced, then renamed
into place. The file starts with a 44-byte header:

| Offset | Size | Field |
| --- | --- | --- |
| 0 | 4 | magic `OSC1` |
| 4 | 8 | data length, big-endian uint64 |
| 12 | 32 | SHA-256 of the data |
| 44 | n | chunk data |

Every read re-hashes the data and compares it with the header. A mismatch marks the chunk corrupt and the
read fails, so a worker never serves bytes it cannot vouch for. The controller also checks each chunk
against the checksum in its own metadata (end-to-end check).

## Client API

| Method | Path | Success | Notes |
| --- | --- | --- | --- |
| POST | `/api/v1/objects` | 201 | multipart: `file` (required), `metadata` (JSON object of strings); returns the object's metadata and a `Location` header |
| GET | `/api/v1/objects/{id}` | 200 | original bytes with `Content-Type`, `Content-Length`, `ETag` (SHA-256), `Content-Disposition`; HEAD returns headers only |
| GET | `/api/v1/objects/{id}/metadata` | 200 | system metadata, chunk list with replica locations, custom metadata |
| PATCH | `/api/v1/objects/{id}/metadata` | 200 | JSON object; string values add or replace keys, `null` removes a key |
| DELETE | `/api/v1/objects/{id}` | 204 | |
| GET | `/api/v1/objects?limit=&offset=` | 200 | oldest first; `next_offset` when more remain |
| GET | `/internal/workers` | 200 | membership state of every worker |
| GET | `/internal/workers/{id}/chunks` | 200 | the worker's chunk inventory, proxied by the controller |

Errors are JSON `{"error": "<code>", "message": "..."}` with 400, 404, 409, 500 or 503.
Custom metadata is limited to 64 keys, keys up to 128 bytes, values up to 2048 bytes.
The content type comes from the file part's header, else the file extension, else content sniffing.

## Upload, download and delete

**Upload.** The controller writes a `pending` metadata record, then streams the file through the chunker.
Each chunk goes to the top two workers in rendezvous-hash order among alive workers; if a write fails the
next worker in that order is tried. Only when every chunk has two acknowledged copies does the record
become `committed` and visible. Fewer than two usable workers gives 503. On any failure the chunks written
so far are deleted in the background and the pending record removed (C2).

**Download.** Chunks are read in order from replicas on alive workers first, then suspect ones; dead
workers are skipped. Each chunk is checked twice: by the worker against its file header, and by the
controller against the checksum in its metadata. A corrupt or missing copy is skipped and then rewritten
in the background from the copy that just passed the check (read repair, C5). If no valid copy of the
first chunk is reachable the request fails with 503; if a later chunk fails, the connection is aborted so
the client gets a short body rather than wrong bytes.

**Delete.** The record becomes a durable `deleted` tombstone before 204 is returned, so the object is
never served again (I6). Its chunks are removed once no download of it is in progress (C6), and the
tombstone is dropped when every copy is confirmed gone. Copies on unreachable workers are left for
garbage collection.

## Controller metadata

Object records live in a bbolt database at `/data/meta.db` on the controller's volume. Every write
transaction is fsynced before the request returns (C3). Membership is soft state: after a controller
restart, workers get `unknown_worker` on their next heartbeat and register again within a second.

## Membership

Workers register on start and heartbeat every second. The controller keeps each worker in one of three
states:

| State | Entered when | Effect |
| --- | --- | --- |
| alive | registered, or steady heartbeats | used for new placements and reads; its replicas count |
| suspect | no heartbeat for `suspect-after` (3 s) | not used for placements; replicas still count (grace period) |
| dead | no heartbeat for `dead-after` (20 s) | replicas no longer count; repair starts |

A dead worker that comes back is held in `suspect` (rejoining) until it has heartbeated steadily for
`rejoin-after` (10 s), and its replicas count only after the controller reconciles its inventory. A brief
pause therefore never triggers re-replication, and a flapping worker cannot bounce straight back to alive.

## Correctness obligations

| # | Scenario | Required behaviour |
| --- | --- | --- |
| C1 | Worker returns after its chunks were re-replicated | Reconcile its inventory, verify checksums, trim surplus so each chunk ends with exactly 2 valid copies on distinct workers |
| C2 | Upload fails partway | Object stays pending until every chunk has both replicas acknowledged; failed uploads are aborted and garbage-collected; pending objects are never listed or served |
| C3 | Controller restarts | Metadata is durable; on restart the chunk map is cross-checked against worker inventories |
| C4 | Heartbeats flap | alive / suspect / dead with a grace period and hysteresis on return |
| C5 | Silent corruption | Checksum verified on every read; background scrubber; repair copies only from a freshly verified replica |
| C6 | Delete during download | Deletes are tombstoned; chunk removal waits for in-flight reads; a download never returns mixed data |

## Invariants

| ID | Invariant |
| --- | --- |
| I1 | Every acknowledged object is readable and byte-identical to what was uploaded |
| I2 | Corrupted bytes are never returned while a valid replica exists |
| I3 | After faults stop, every chunk eventually has 2 valid copies on distinct workers |
| I4 | No orphan chunks remain after the garbage-collection horizon |
| I5 | Committed metadata survives a controller restart |
| I6 | A deleted object is never served after its DELETE returns 204 |
