// Object envelope encryption (Botan 3): a random per-object DEK encrypts the data with
// AES-256-GCM, and the DEK is wrapped under a key derived from an ML-KEM-768 encapsulation
// to one KEK of the ring. Pure: no I/O. A Ring is immutable after construction, so it is
// safe for concurrent use.
//
// Blob: ver(1) suite(1) kek_id(4, BE) kem_ct(1088) wrapped_dek(32+16) nonce(12) ct||tag.
// Ring file: records kek_id(4, BE) flags(1) ek(1184) [seed(64) if flags & 1]. The first
// record is the KEK used to seal; any record can open the blobs sealed under its id.
#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <stdexcept>
#include <vector>

namespace mycelium {

enum class SealError { Invalid, UnknownKek, NoSecretKey, AuthFailed };

struct SealException : std::runtime_error {
  SealError code;
  SealException(SealError c, const char* what) : std::runtime_error(what), code(c) {}
};

inline constexpr size_t kEncapsKeySize = 1184;
inline constexpr size_t kSeedSize = 64;
inline constexpr size_t kRecordHeaderSize = 5;
inline constexpr size_t kPublicRecordSize = kRecordHeaderSize + kEncapsKeySize;
inline constexpr size_t kSecretRecordSize = kPublicRecordSize + kSeedSize;
// Bytes a blob adds to the plaintext.
inline constexpr size_t kSealOverhead = 1 + 1 + 4 + 1088 + 48 + 12 + 16;

class Ring {
 public:
  Ring();
  ~Ring();
  Ring(Ring&&) noexcept;
  Ring& operator=(Ring&&) noexcept;

  // Throws SealException(Invalid) on a malformed file, a duplicate id, or a seed that does
  // not match its encapsulation key. At least one record is required.
  static Ring parse(std::span<const uint8_t> file);

  struct Impl;
  const Impl& impl() const { return *impl_; }

 private:
  std::unique_ptr<Impl> impl_;
};

// Returns pt.size() + kSealOverhead bytes. id is authenticated, not stored.
std::vector<uint8_t> seal(const Ring& ring, std::span<const uint8_t> id, std::span<const uint8_t> pt);

// Throws SealException: Invalid (not a blob), UnknownKek, NoSecretKey (public-only entry),
// AuthFailed (tampered, wrong id, or wrong key).
std::vector<uint8_t> open(const Ring& ring, std::span<const uint8_t> id, std::span<const uint8_t> blob);

// A fresh record with a secret key (kSecretRecordSize bytes).
std::vector<uint8_t> generate_record(uint32_t kek_id);

// The same ring file with every seed dropped. Throws SealException(Invalid) on a malformed file.
std::vector<uint8_t> public_ring(std::span<const uint8_t> file);

}  // namespace mycelium
