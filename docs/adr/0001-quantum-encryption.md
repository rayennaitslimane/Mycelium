# ADR-0001: Quantum Encryption

**Status:** Accepted  
**Date:** 2026-06-11

## Context

myc must protect stored objects against both classical and quantum adversaries. The system needs:
- Confidentiality for data at rest across multiple storage nodes
- A key hierarchy that limits blast radius (a single compromised node must not expose all objects)
- Key management that can survive key compromise via rotation without re-encrypting all stored data immediately
- Protection against shard-swapping attacks where an adversary reorders ciphertext blocks

The design must be implementable today without waiting for full NIST PQC standardisation rollout, and must integrate cleanly with an erasure-coded shard model where each shard is encrypted independently.

## Decision

### Key hierarchy: ML-KEM-768 KEM + AES-256-GCM wrapping for per-object DEKs

- Each stored object receives a freshly generated 256-bit **Data Encryption Key (DEK)** drawn from `Botan::AutoSeeded_RNG`.
- Each shard of the object is encrypted independently with **AES-256-GCM** using that DEK.
- The DEK is wrapped using an **ML-KEM-768 Key Encryption Key (KEK)** through Botan's KEM API: the code encapsulates a shared secret with `PK_KEM_Encryptor` (`HKDF(SHA-256)`), then encrypts the DEK with **AES-256-GCM** under that shared secret.
- The wrapped DEK is stored in PostgreSQL alongside the object metadata, not in the shard nodes.
- The active KEK private key lives on disk at a configurable path (default `/tmp/myc_kek.json`, overridable via `MYC_KEYSTORE_PATH`), written with `0600` file permissions and `0700` on the parent directory.

### Shard index as AEAD associated data

Each shard's AES-256-GCM encryption binds the `uint32_t shard index` as **Associated Authenticated Data (AAD)**:

```cpp
enc->set_associated_data(
    reinterpret_cast<const uint8_t*>(&e.index),
    sizeof(e.index)
);
```

This makes any ciphertext-swapping or reordering between shard slots a detectable authentication failure - the tag verification will fail if shard `i`'s ciphertext is presented under shard `j`'s index.

### Per-shard nonces

A fresh 12-byte nonce is generated per shard per encryption. Nonces are stored alongside the ciphertext in the `EncryptedShard` msgpack envelope. This prevents nonce reuse even if the same object is overwritten.

### DEK zeroisation

After the DEK is used to encrypt or decrypt shards it is immediately wiped using `Botan::secure_scrub_memory`, preventing it from lingering in process memory:

```cpp
Botan::secure_scrub_memory(dek.data(), dek.size());
```

### Key rotation

`StorageClient::rotate()` generates a new ML-KEM-768 key pair, adds it to the in-process key ring, promotes it to `active_kek_id_`, and persists the updated ring to disk. New objects written after rotation are wrapped under the new KEK. The old KEK is **retained** in the ring to allow reads of objects that were written before the rotation. This is an append-only ring - no keys are deleted during rotation.

> **Known limitation:** `rotate()` does not re-wrap the DEKs of existing objects under the new KEK. Old objects remain permanently tied to the KEK that was active at their write time. Full re-encryption would require a background sweep over all object metadata, which is deferred to a future milestone.

### DEK recovery fallback

On `get()`, the code first attempts to decrypt the wrapped DEK using the KEK ID stored in object metadata. If that fails (e.g., key mismatch or stale metadata), it falls back to trying all keys in the ring. This is a safety net, not the expected path.

## Consequences

**Positive:**
- Post-quantum confidentiality at rest: an adversary who captures all shard nodes and the metadata database cannot decrypt objects without the ML-KEM-768 private key.
- Per-object DEKs limit blast radius: compromise of one object's DEK does not affect any other object.
- AAD on shard index provides integrity against cross-shard substitution attacks.
- Key rotation is non-disruptive to reads - the ring retains all historical KEKs.
- DEK is scrubbed from memory immediately after use.

**Negative / Risks:**
- The KEK file on disk is the single point of failure for the entire key ring. If it is lost and no backup exists, all objects become unrecoverable. Backup strategy is out of scope for this POC.
- The default keystore path (`/tmp/myc_kek.json`) is in a world-accessible directory. The `MYC_KEYSTORE_PATH` environment variable must be set in any environment beyond local testing.
- Old objects are never re-wrapped after rotation; if the old KEK is later confirmed compromised, those objects remain at risk until manually re-encrypted.
- `StorageClient` thread safety is now defined by [ADR-0006](0006-concurrency-contract.md): `kek_ring_` and `active_kek_id_` are protected by a `std::shared_mutex`. Concurrent conflicting mutations on the same object ID remain intentionally unordered.
