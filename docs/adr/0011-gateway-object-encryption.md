# ADR 0011: Object encryption at the gateway

Status: Accepted · 2026-10-21

## Context
Objects are stored as plain shards on every node. Security here is a layer on top of the protocol.

## Decision
**The fewer systems that possess a secret key, the fewer systems can leak it.** So only the
gateway touches keys, and nodes and the monitor never see plaintext or a key.

- **Where:** the gateway seals the body of a `PUT` and opens the body of a `GET`
  ([front.go](../../gateway/front/front.go)). The primary, holders, peering, backfill and
  `reencode` see opaque bytes and are unchanged. Only the primary's size limit grows by the
  fixed `seal.Overhead`.
- **Envelope:** a random 32-byte DEK encrypts the object with AES-256-GCM. The DEK is wrapped
  under a key derived (HKDF-SHA-256) from an ML-KEM-768 encapsulation to one KEK of the ring.
  The blob is `ver suite kek_id kem_ct wrapped_dek nonce ct||tag`, 1170 bytes larger than the
  plaintext ([seal.hpp](../../core/seal/seal.hpp)).
- **Binding:** the object id is authenticated with the data, so a node cannot serve one
  object's ciphertext as another's. The data's AAD holds only `ver`, `suite` and the id,
  nothing about the KEK, so a later rotation can rewrap the DEK without touching the data.
- **Ring:** KEKs have an id, the encapsulation key and, optionally, a secret seed. The first
  entry seals; any entry opens blobs carrying its id. **A public-only entry is valid:** a
  gateway holding only public keys can write but cannot read, so ingest gateways need no
  secret. The ring is a local file (`-keyring`, made by `cmd/keyring`), never part of the map.
- **Implementation:** C++ on Botan 3.12 behind a small C ABI, called through cgo like `codec`.
  Keys stay inside C++. The ring is immutable and safe for concurrent use.
- **Botan 3.12, statically linked,** built by `scripts/build-botan.sh` into `.botan/`, so the
  version and the ML-KEM-capable module set do not depend on the distro (Ubuntu has no Botan 3
  package; a `static_assert` in `seal.cpp` enforces the 3.12 floor). Other libraries, such as
  ISA-L, stay dynamic. Botan 2 and 3 cannot share a process, so `codec` and `store` moved to
  Botan 3 too.
- **Opt-in:** without `-keyring` the gateway stores plaintext and logs a warning. Objects
  written either way are not readable the other way (`502`).

## Protects
That a compromised node, disk or backup yields only ciphertext, and that a stolen write-path
gateway yields no ability to read.

## If violated
- **Sealing in the primary or holders:** every node needs secret keys, so every node can leak them.
- **Leaving the object id out of the AAD:** a faulty holder can swap ciphertexts between objects.

## Non-goals
- **Rotation.** Every blob carries its `kek_id` and the ring holds many KEKs, so adding a new
  first KEK works today and old objects stay readable. Rewrapping old objects needs a
  conditional `PUT` (there is none, writes are last-writer-wins) and is left for later.
- **Rollback detection.** Serving an older version of the same object is not detected.
- **Metadata privacy.** Ids, sizes (to within the overhead) and access patterns are visible to nodes.
- **Key storage.** The ring file is plain and expected to be `0600`; a KMS or file encryption
  would replace only the loader.
- **Transport security.** Still the Auth/TLS non-goal in the system design.

## Rejected
- **A symmetric KEK:** AES-256 is already quantum-resistant, but it cannot give write-only gateways.
- **Sealing in the primary:** atomic with rotation, but spreads secret keys over every node.
- **Per-chunk or streaming encryption:** objects are whole and bounded by `max_object`.
