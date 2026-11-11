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
