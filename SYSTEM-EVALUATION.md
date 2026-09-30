# Mycelium System Evaluation

## How this was measured

Everything below comes from running the benchmark harness in
[cmd/BenchmarkMain.cpp](cmd/BenchmarkMain.cpp) against a real cluster. The harness doesn't mock
anything: it drives the actual `StorageClient` API through a sweep of erasure settings (`k` from
2 to 8, `m` from 1 to 4) and object sizes from 1 KB to 1 MB, then records timing and outcome for
every `PUT`, `GET` and `REPAIR`. Each record now carries three timed phases, `crypto_ms`,
`transport_ms` and `metadata_ms`, plus the operation total, so the whole operation is accounted
for rather than partially. A second executable, `metadata_benchmark`, exercises the metadata layer
on its own (connection-pool throughput and `list()` scaling), which the single-threaded latency
sweep can't capture. The notebooks in [docs/metrics/](docs/metrics/README.md), `01_performance`,
`02_reliability` and `03_metadata_scaling`, turn those records into the charts and tables this
report leans on, and the design rationale lives in the [ADRs](docs/adr/README.md). Where I make a
claim, I've tried to point at the chart or the ADR that backs it.

## The short version

Mycelium does what it set out to do. Objects encrypt, split, store, survive node loss, and come back
intact, and the post-quantum machinery costs almost nothing to run. The part that used to be a
guess, where the rest of the time went, is now measured end to end.

If you only remember three things:

- **The post-quantum crypto is basically free.** ML-KEM-768 plus AES-256-GCM is a minor fraction
  of every small operation and grows only with payload size. Security was never the thing slowing
  this system down.
- **Writes are the expensive path, and the network is why.** A `PUT` does more work than a `GET`
  (extra parity to encode and extra shards to ship), and on both paths shard transport over HTTP
  is the dominant phase.
- **The old blind spot is gone.** The metadata layer is now timed (`metadata_ms`) on every
  operation and relieved structurally, a connection pool replaced the single serialized
  connection and `list()` is a single JOIN instead of an N+1. The three phases now sum to close to
  the measured total, so latency is fully attributable.

## Where the time goes

The phase breakdown in [01_performance.ipynb](docs/metrics/01_performance.ipynb) splits each
operation into crypto, transport and metadata. Transport is the largest share of a write; crypto
is negligible for small objects and rises to a minority share only on megabyte payloads (it scales
with the bytes encrypted); metadata is a small, now-visible slice that matters most for tiny
objects, where fixed per-call database cost isn't amortized, and fades to noise for large ones.
That cheap crypto is exactly the outcome [ADR-0001](docs/adr/0001-quantum-encryption.md) was
hoping for: a per-object DEK wrapped by an ML-KEM-768 KEK is cheap enough that you never have to
think about it again.

The important change since the last evaluation is that there is no longer a large unexplained gap
between the timed phases and the total. Previously the PostgreSQL path was uninstrumented and
showed up only as missing time, worst of all on `REPAIR`. Now `metadata_ms` is recorded on `PUT`,
`GET` and `REPAIR` alike (the `REPAIR` record splits its metadata cost across the load and the
versioned commit), so the phases account for the operation. That blind spot, the central caveat of
the previous report, has been closed, and the structural causes behind it have been addressed:
[ADR-0006](docs/adr/0006-concurrency-contract.md)'s single-connection bottleneck is now a pooled
set of connections handed out under a condition variable, and
[ADR-0003](docs/adr/0003-dual-storage.md)'s N+1 in `list()` is a single `LEFT JOIN` ordered by
`(id, shard_index)` that assembles each object in one pass. The `metadata_benchmark` harness and
[03_metadata_scaling.ipynb](docs/metrics/03_metadata_scaling.ipynb) measure those two changes
directly.

## Durability behaves exactly like the theory says

This is the part that's genuinely satisfying. The reliability notebook
([02_reliability.ipynb](docs/metrics/02_reliability.ipynb)) shows recovery tracking
Reed-Solomon's guarantee with no surprises: an object comes back if, and only if, at least `k`
shards survive. Configurations with generous parity (`m=4`) recover every time; configurations
with `m=1` fall off a cliff the moment more than one shard goes missing.

The headline "overall repair success" number is therefore a little misleading on its own, it's an
average across both robust and fragile configurations, and that average hides the fact that the
outcome is completely deterministic once you know how many shards were lost. The ISA-L codec
chosen in [ADR-0002](docs/adr/0002-erasure-codec.md) is doing precisely what it promised. The
practical lesson is about parameter choice, not codec behaviour: `m=1` is a trap under correlated
or multi-node failure.

## Small objects pay a tax

Storage overhead converges nicely on the theoretical `(k+m)/k` rate for large objects, but small
ones are punished. A 1 KB object at `2+1` carries the full per-shard framing, a nonce, a GCM tag,
and a msgpack envelope on every fragment, negligible for a megabyte and ruinous for a kilobyte.
Higher `k` amortizes this well: at equivalent durability, an `8+2` profile is far leaner than
`2+2`. The same fixed-cost effect shows up in latency, where the metadata phase is a visible
fraction of a tiny operation but vanishes on large ones. The takeaway is that Mycelium rewards
batching small objects and choosing wider stripes.

## Repair is the wild card, but it's no longer a black box

Repair is still the slowest and most variable operation, reconstruction is cheap, and the work
around it dominates, but two things that used to inflate and obscure it are fixed. It is now fully
phase-timed like `PUT` and `GET`, so its crypto, transport and metadata costs are individually
visible instead of hiding in the total. And per the amended
[ADR-0004](docs/adr/0004-shard-transport.md), the health probes (`is_node_healthy`, `probe_shard`)
and `init()` now obtain clients from the same thread-local per-node cache the hot path uses, so
repair no longer spins up a fresh HTTP client on every probe. What remains variable is intrinsic:
how many shards must be rebuilt and how many nodes must be probed to place them.

## What got fixed since the last evaluation

Several items the previous report flagged as the highest-leverage work are now done:

- **Metadata cost is measured.** `metadata_ms` is a first-class phase on every operation and a
  column in the CSV schema; the hidden tax is no longer a guess.
- **The single-connection serialization is gone.** `MetadataStore` owns a connection pool and
  hands connections out under a mutex/condition-variable, removing the one-call-at-a-time
  bottleneck described in ADR-0006.
- **`list()` is one query.** The N+1 round-trip per object is replaced by a single `LEFT JOIN`.
- **The write path is trimmed.** `encode()` takes its input by `const` reference, eliminating the
  per-write defensive object copy ADR-0002 flagged, and thread-local HTTP client reuse now covers
  `init()` and the health probes, not just the transport hot paths.

## What I'd fix next

With the measurement and write-path work landed, the remaining gaps are about durability policy,
storage hygiene and key management rather than raw latency:

1. **Pick erasure defaults on purpose.** Avoid `m=1` in anything that matters. A `4+2` or `6+3`
   profile buys tolerance for two or three simultaneous losses at a sane overhead.
2. **Fix the benchmark failure model.** The harness still leaves the
   `std::min(requested_failures, m)` clamp commented out in
   [cmd/BenchmarkMain.cpp](cmd/BenchmarkMain.cpp) and drops the raw `BENCHMARK_FAILED_NODES`
   count. Sweeping `failed_nodes` from 0 to `m+1` deliberately would chart the durability cliff
   explicitly instead of leaving it implicit.
3. **Add garbage collection for orphaned shards.** ADR-0006 still documents that concurrent
   `put`/`remove` interleavings, lost upsert races, and failed repairs leave inert encrypted
   shards on nodes with no reclaim path. They cause no correctness problem, but they leak storage;
   a GC sweep needs to exist.
4. **Harden key management before this is more than a POC.** The default keystore still sits in
   world-accessible `/tmp/myc_kek.json` and is a single point of failure, and `rotate()` mints a
   new KEK but never re-wraps existing objects' DEKs (ADR-0001). Since crypto isn't a performance
   concern, a re-wrap sweep, a keystore backup, and a non-`/tmp` default all come essentially for
   free.

## Security audit: vulnerabilities and fixes

A read-through of the implementation against the ADRs confirms the cryptographic core is sound:
AES-256-GCM per shard, ML-KEM-768 KEM wrapping, per-shard random nonces, the shard index bound as
AEAD associated data, and DEK scrubbing are all implemented as
[ADR-0001](docs/adr/0001-quantum-encryption.md) describes, and SQL is fully parameterized through
prepared statements in [core/MetadataStore.cpp](core/MetadataStore.cpp) (no injection surface). The
exploitable risk is not in the math; it is concentrated at the trust boundary, which the ADRs never
addressed. The findings below are ordered by severity, each with a simple, high-yield fix.

### Critical

- **No authentication or authorization on any endpoint.** Both
  [core/StorageClientServer.cpp](core/StorageClientServer.cpp) and
  [core/StorageNodeServer.cpp](core/StorageNodeServer.cpp) register routes with no auth. Anyone
  with network reach can `GET`/`PUT`/`DELETE`/`list`/`repair` any object, and can `DELETE` raw
  shards directly on a node. Deleting `k` shards makes an object permanently unrecoverable, an
  integrity and availability attack erasure coding cannot defend against.
  *Fix:* add a `set_pre_routing_handler` on both servers that checks a constant-time
  `Authorization: Bearer <token>` from an environment variable and returns `401` otherwise (~15
  lines, no new dependency).

### High

- **KEK keystore default is world-readable and creation races.** The default path
  `/tmp/myc_kek.json` in [core/StorageClient.cpp](core/StorageClient.cpp) is a predictable,
  world-writable location holding the ML-KEM private keys. In `save_kek_file`
  ([core/Crypto.cpp](core/Crypto.cpp)) the file is created with `std::ofstream` under the process
  umask and only downgraded to `0600` afterward, leaving a window where the key file is
  world-readable; the write is also non-atomic, so a crash can leave a truncated keystore.
  *Fix:* require `MYC_KEYSTORE_PATH` (fail closed if unset, drop the `/tmp` default), `umask(077)`
  or `open(..., O_WRONLY|O_CREAT|O_EXCL, 0600)` before writing, and write to a temp file +
  `fsync` + `rename` for atomicity.
- **All traffic is plaintext HTTP.** Both servers use `httplib::Server` over cleartext HTTP/1.1, and
  the client API carries object plaintext as base64 in PUT/GET bodies
  ([core/StorageClientServer.cpp](core/StorageClientServer.cpp)). The post-quantum guarantee is
  at-rest only; on the wire confidentiality is zero.
  *Fix:* terminate TLS via `httplib::SSLServer`/`SSLClient` (or a TLS reverse proxy / mTLS between
  client and nodes), paired with the auth token so it is never sent in cleartext.

### Medium

- **Unbounded request body → memory-exhaustion DoS.** The PUT handler base64-decodes the entire
  body before checking `bytes.size()` against `k * shard_size`, and cpp-httplib's default payload
  cap is effectively unlimited.
  *Fix:* call `server_.set_payload_max_length(N)` on both servers.
- **Deserialization of untrusted node responses (msgpack bomb).** `fetch_encrypted_shards`
  ([core/ShardTransport.cpp](core/ShardTransport.cpp)) calls `EncryptedShard::deserialize`
  ([include/Models.hpp](include/Models.hpp)) on bytes returned by a node; a compromised node can
  return crafted msgpack with huge length prefixes that allocate before the try/catch fails.
  *Fix:* reject payloads larger than `shard_size` plus a small fixed overhead before deserializing,
  and apply an `msgpack::unpack` size limit.
- **Internal error details leaked to clients.** Handlers return `e.what()` verbatim in `500`
  responses, exposing file paths, node `host:port`, and SQL/LMDB error strings.
  *Fix:* return a generic `{"error":"internal error"}` body and log details server-side only; also
  map `GET /objects/:id` not-found to `404` ([ADR-0005](docs/adr/0005-http-surface.md) already
  flags this).
- **Unsalted SHA-256 of plaintext stored in metadata.** The `checksum` column holds a SHA-256 of
  the plaintext, giving anyone with metadata DB access a confirmation oracle for low-entropy or
  known objects.
  *Fix:* store an HMAC-SHA-256 (keyed) or hash the ciphertext instead.

### Low

- **`parse_address` uses unchecked `std::stoi`** ([core/ShardTransport.cpp](core/ShardTransport.cpp));
  a malformed registered address throws and aborts the write path. *Fix:* validate the address
  format at `register_node` time.
- **Residual DEK copy not scrubbed.** `recover_dek` in [core/StorageClient.cpp](core/StorageClient.cpp)
  returns the DEK by value; the function-local copy is not wiped, contrary to ADR-0001's
  immediate-scrub claim. *Fix:* scrub the local before returning.

The three Critical/High items (authentication, keystore hardening, TLS) are each small changes that
together bring the running system in line with the confidentiality and integrity goals stated in
ADR-0001. The ADRs themselves never specify auth or TLS, so this is also an architectural gap worth
recording, not merely an implementation oversight.

## Closing thought

Mycelium already delivers the hard parts: post-quantum confidentiality at rest and erasure durability
that matches the math, both at negligible cryptographic cost. The previous evaluation's defining
caveat, an unmeasured, serialized metadata layer, has been closed: every phase is now timed, the
connection pool and JOIN relieve the database path, and the write path has shed its avoidable
copies and client churn. What's left is policy and hygiene, choose parity to match the real
failure model, reclaim orphaned shards, and harden key management, and this moves from a correct,
fully instrumented proof-of-concept toward something you could actually run.
