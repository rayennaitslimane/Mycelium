# ADR 0010: Shard durability and integrity

Status: Accepted · 2026-10-18

## Context
Every guarantee above assumes that what a holder acknowledged is on disk and that what it
returns is what was written.

## Decision
- **Durable before acknowledged.** Every store change (put, commit, discard, fence) is durable
  before the call returns ([shard_store.cpp](../../node/store/shard_store.cpp)):
  - new files are written to a temp file, fsynced, then renamed into place;
  - the directory is fsynced after any rename or delete;
  - temp files left by a crash are never treated as data, and are removed when their object's
    directory is next listed.
- **Checksums everywhere.** The sha256 of each shard is checked on every write (by the holder)
  and on every read (by the store, and again by the primary).
  - A bad checksum makes that shard unavailable.
  - Corruption is never served or repaired silently; there is no scrubbing (a spec non-goal).
- **Layout:** one file per version, named `<epoch>-<seq>.<s|c|sd>` (staged, committed, staged
  tombstone). Commit and discard are renames and deletes within one directory.
- **Erasure coding:** objects are `k+m` shards with Cauchy Reed–Solomon (ISA-L). Any `k` shards
  rebuild the object. `k=1` means `n` plain copies.
- **Language split:** the store and codec are C++ behind a small C interface, called in-process
  from Go through cgo.

## Protects
Invariant 1 across crashes, and the meaning of "held by ≥ `k` slots" in peering (ADR 0006): a
counted shard must be decodable.

## If violated
- **Acknowledging before fsync:** a crash loses a write that the protocol counted as held
  everywhere.
- **Skipping checksums:** corrupt data is decoded and served, or counted as a winner.

## Rejected
- **One file per object, rewritten in place:** a crash could tear it, and staged and committed
  versions couldn't coexist.
- **An embedded key-value store (for example RocksDB):** more machinery than the per-version
  file model needs.
