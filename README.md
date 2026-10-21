# Mycelium

A small object store in the style of RADOS. You can PUT, GET and DELETE whole objects
over HTTP. Each object is erasure-coded across several nodes and survives node failures.

The design, including every rule and route, is in [docs/system-design.md](docs/system-design.md). This README covers how to build and run it.

## How it works

```
client ──HTTP──▶ gateway ──▶ primary node ──▶ holder nodes (one shard each)
                    │              │
                    └── GET /map ──┴── heartbeat, join, backfill_done ──▶ monitor
```

- **Monitor**: owns the *map*, which says which nodes are `up` and which nodes hold each
  placement group (PG). Every map change bumps the map's version number, the *epoch*. You can
  run one monitor, or three or more replicated with Raft.
- **Gateway**: stateless. It hashes the object id to a PG and forwards the request to that
  PG's primary. When it gets `421` or `503`, it refetches the map and retries.
- **Node**: holds shards for its PGs. It is also the *primary* of some PGs: it orders writes,
  runs recovery (*peering*), and copies data to replacement nodes (*backfill*).

An object becomes `k + m` shards, one per slot of its PG (`k = 1` is plain replication).
When a node goes silent, the monitor marks it `out` and gives its slots to a spare node. The
primary then backfills the spare.

## Code layout

| Path | Language | What |
| --- | --- | --- |
| `core/mapmodel` | Go | Map type and the rules derived from it: `pg(id)`, `U`, `B`, primary, versions |
| `core/wire` | Go | Request/reply types, status codes, HTTP helpers |
| `core/seal` | C++ | Object envelope encryption (ML-KEM-768 + AES-256-GCM, Botan), used by the gateway |
| `monitor/transitions` | Go | **All map rules**: pure `(map, event) -> map'` |
| `monitor/{liveness,store,api,raft}` | Go | Heartbeat timer, durable map file, HTTP API, Raft |
| `gateway/{front,mapcache}` | Go | Request forwarding and the cached map |
| `node/agent` | Go | Boot, heartbeats, map refresh, the **serve gate** |
| `node/holder` | Go | Shard HTTP routes |
| `node/peers` | Go | Client the primary uses to reach holders, including its own slot |
| `node/primary` | Go | Object I/O, peering, backfill |
| `node/store` | C++ | Shard files, the per-PG **fence**, fsync |
| `node/codec` | C++ | Erasure coding (ISA-L) and sha256 (Botan) |
| `cmd/{monitor,gateway,node}` | Go | Binaries |
| `cmd/keyring` | Go | Creates the key ring files the gateway loads |
| `test/` | Go | Whole-cluster scenarios, run in one process |

The C++ packages are compiled by cgo as part of `go build`. There is no separate build step.

Suggested reading order: [docs/system-design.md](docs/system-design.md), then
[mapmodel.go](core/mapmodel/mapmodel.go), [transitions.go](monitor/transitions/transitions.go),
[primary.go](node/primary/primary.go), and finally [io.go](node/primary/io.go) and
[peering.go](node/primary/peering.go).

## Design decisions

The decisions that keep the system correct are recorded as short ADRs in
[docs/adr/](docs/adr). Read them before changing the protocol:

| ADR | Decision |
| --- | --- |
| [0001](docs/adr/0001-map-single-source-of-truth.md) | The map is the single source of truth |
| [0002](docs/adr/0002-placement-fixed-table.md) | Placement: fixed hash, explicit table, no rebalancing |
| [0003](docs/adr/0003-versions-and-client-semantics.md) | Versions and client semantics |
| [0004](docs/adr/0004-two-phase-writes.md) | Writes: stage everywhere, then commit everywhere |
| [0005](docs/adr/0005-fencing.md) | Fencing, not clocks, protects writes |
| [0006](docs/adr/0006-peering.md) | Peering decides the truth after a failure |
| [0007](docs/adr/0007-backfill-before-b.md) | A node enters `B` only through backfill |
| [0008](docs/adr/0008-liveness-and-serve-gate.md) | Liveness, the serve gate and the one timing assumption |
| [0009](docs/adr/0009-raft-monitors.md) | Monitor replication with Raft and leader leases |
| [0010](docs/adr/0010-shard-durability-and-integrity.md) | Shard durability and integrity |
| [0011](docs/adr/0011-gateway-object-encryption.md) | Objects are encrypted at the gateway, the only holder of secret keys |

## Build

Requirements: Go 1.27 (any Go from 1.21 on downloads it automatically on first build), a
C++20 compiler, `pkg-config`, `curl`, Python 3 and ISA-L. Botan 3.12 is built from source.

```sh
sudo apt install g++ pkg-config libisal-dev curl python3   # Debian/Ubuntu
make botan      # once: builds a static, minimal Botan 3.12.0 into .botan/
make            # builds bin/monitor, bin/gateway, bin/node, bin/keyring
```

`make` sets `CGO_LDFLAGS_ALLOW`, because `botan-3.pc` emits link flags that cgo rejects by
default, and points `PKG_CONFIG_PATH` at `.botan/`. If you call `go` directly, set both:

```sh
export CGO_LDFLAGS_ALLOW='-fstack-protector|-m64' PKG_CONFIG_PATH=$PWD/.botan/lib/pkgconfig
go build ./...
```

## Run a local cluster

```sh
make cluster    # 1 monitor (:7000), 4 nodes (:9001-9004), 1 gateway (:8080), k=2 m=1
```

The first three nodes to join form the PG table. The fourth is a spare. Logs, data and pid
files go to `run/`. Ctrl-C stops everything, and `make clean` resets all state. To change the
size, set `NODES`, `K` and `M`, for example `NODES=5 K=1 M=2 ./scripts/cluster.sh`.

In another terminal:

```sh
curl -i -X PUT --data-binary 'hello' localhost:8080/objects/greeting   # 200, X-Version: <epoch>.<seq>
curl -i localhost:8080/objects/greeting                                # hello
curl -i -X DELETE localhost:8080/objects/greeting                      # 200
curl -i localhost:8080/objects/greeting                                # 404
curl -s localhost:7000/map                                             # the map, as JSON
```

To see recovery, kill a node that appears in the map's `pgs`, then watch the map change:

```sh
curl -X PUT --data-binary 'still here' localhost:8080/objects/a
kill $(cat run/node1.pid)
sleep 7;  curl -s localhost:7000/map   # node1 is "out"; the spare holds its slots, "backfilling": true
sleep 20; curl localhost:8080/objects/a # still here
```

Recovery has two phases. First, the monitor marks the node `out` after `grace` (5s here).
Then the primaries backfill the spare, which takes about 15s in this demo. A PG answers `503`
until at least `min_size` of its slots are up and not backfilling. `min_size` defaults to
`k+1`, so with `k=2 m=1` a PG is unavailable until the spare is backfilled. Adding
`-min-size 2` to the monitor command in `scripts/cluster.sh` (then `make clean`) keeps PGs
serviceable during backfill, with less redundancy for new writes in the meantime.

To restart a killed node, copy its command line from the first line of its log
(`head -1 run/node1.log`). Because it was out, it purges its data and rejoins as a spare.

### Encryption

The gateway can seal every object before it reaches a node ([ADR 0011](docs/adr/0011-gateway-object-encryption.md)).
Nodes then hold only ciphertext, and only the gateway has keys.

```sh
bin/keyring gen -id 1 -out ring.key        # a KEK with its secret key (mode 0600)
KEYRING=ring.key make cluster              # or: bin/gateway ... -keyring ring.key
bin/keyring pub -in ring.key -out ring.pub # public half only: a gateway given this can PUT but not GET
```

Without `-keyring` the gateway stores plaintext. The first KEK in a ring file seals; to add
one, `cat new.key old.key > ring.key`. Rewrapping old objects (rotation) is not implemented.

### Replicated monitors (Raft)

Start three monitors with the same bootstrap flags, and give each node and the gateway all
three URLs:

```sh
P=m1=http://127.0.0.1:7001,m2=http://127.0.0.1:7002,m3=http://127.0.0.1:7003
bin/monitor -id m1 -peers $P -listen 127.0.0.1:7001 -data run/m1 &
bin/monitor -id m2 -peers $P -listen 127.0.0.1:7002 -data run/m2 &
bin/monitor -id m3 -peers $P -listen 127.0.0.1:7003 -data run/m3 &
bin/node -monitors http://127.0.0.1:7001,http://127.0.0.1:7002,http://127.0.0.1:7003 ...
```

Every binary prints its flags with `-h`.

## Test

```sh
make test               # everything, with the race detector (about a minute)
go test -short ./...    # unit and component tests only (a few seconds)
```

Each test guards a decision in [docs/adr/](docs/adr), so a failure means a guarantee broke:

- **Unit tests** sit next to their packages:
  - `mapmodel`: pinned hash, derived sets, versions
  - `transitions`: placement, guards, determinism
  - `primary`: `winner()`
  - `agent`: serve gate
- **Component tests** use the real C++ store and codec through cgo, and run the monitor
  in-process, both as a single instance and as three Raft replicas.
- **[test/](test)** starts a monitor, several nodes and a gateway in one process, with short
  lease and grace, and checks five scenarios:
  - the client contract;
  - node failure followed by backfill;
  - primary failover under concurrent writes;
  - peering rolling a write forward or back;
  - a cut-off primary.

  These scenarios are skipped with `-short`.

## Status

All components are implemented and covered by the tests above. Non-goals, such as transport auth,
rebalancing and range I/O, are listed at the end of [docs/system-design.md](docs/system-design.md).


## Project history

Mycelium was originally developed as an encrypted erasure-coded
object store focused on post-quantum security.

The current architecture (v2) is a ground-up redesign focused on
distributed-systems correctness, recovery, primary ownership,
peering and backfill. Security features from the original design
will be reintroduced progressively on top of this foundation.
