#include "seal.hpp"

#include <botan/aead.h>
#include <botan/exceptn.h>
#include <botan/kdf.h>
#include <botan/ml_kem.h>
#include <botan/pubkey.h>
#include <botan/system_rng.h>
#include <botan/version.h>

#include <algorithm>
#include <cstring>

static_assert(BOTAN_VERSION_MAJOR == 3 && BOTAN_VERSION_MINOR >= 12, "seal needs Botan >= 3.12");

namespace mycelium {

namespace {

constexpr uint8_t kVersion = 1;
constexpr uint8_t kSuiteMlKem768Aes256Gcm = 1;
constexpr size_t kCtSize = 1088;
constexpr size_t kDekSize = 32;
constexpr size_t kTagSize = 16;
constexpr size_t kNonceSize = 12;
constexpr size_t kWrappedSize = kDekSize + kTagSize;
constexpr size_t kPrefixSize = 1 + 1 + 4 + kCtSize;  // the part of the header the wrap authenticates
constexpr std::string_view kWrapLabel = "mycelium/seal/v1/wrap";

static_assert(kPrefixSize + kWrappedSize + kNonceSize + kTagSize == kSealOverhead);

Botan::ML_KEM_Mode mode() { return Botan::ML_KEM_Mode(Botan::ML_KEM_Mode::ML_KEM_768); }

uint32_t get_be32(const uint8_t* p) {
  return uint32_t(p[0]) << 24 | uint32_t(p[1]) << 16 | uint32_t(p[2]) << 8 | uint32_t(p[3]);
}

void put_be32(uint8_t* p, uint32_t v) {
  p[0] = uint8_t(v >> 24);
  p[1] = uint8_t(v >> 16);
  p[2] = uint8_t(v >> 8);
  p[3] = uint8_t(v);
}

struct Record {
  uint32_t id;
  std::span<const uint8_t> ek;
  std::span<const uint8_t> seed;  // empty for a public-only record
};

std::vector<Record> parse_records(std::span<const uint8_t> file) {
  std::vector<Record> out;
  while (!file.empty()) {
    if (file.size() < kRecordHeaderSize) throw SealException(SealError::Invalid, "truncated ring record");
    const uint8_t flags = file[4];
    const size_t size = kPublicRecordSize + (flags == 1 ? kSeedSize : 0);
    if (flags > 1 || file.size() < size) throw SealException(SealError::Invalid, "bad ring record");
    Record r{get_be32(file.data()), file.subspan(kRecordHeaderSize, kEncapsKeySize), {}};
    if (flags == 1) r.seed = file.subspan(kPublicRecordSize, kSeedSize);
    out.push_back(r);
    file = file.subspan(size);
  }
  if (out.empty()) throw SealException(SealError::Invalid, "empty ring");
  return out;
}

// One AES-256-GCM operation. Decryption throws SealException(AuthFailed) on a bad tag.
template <typename Buf>
std::vector<uint8_t> gcm(Botan::Cipher_Dir dir, const Buf& key, std::span<const uint8_t> nonce,
                         std::span<const uint8_t> aad, std::span<const uint8_t> in) {
  auto m = Botan::AEAD_Mode::create_or_throw("AES-256/GCM", dir);
  m->set_key(key);
  m->set_associated_data(aad);
  m->start(nonce);
  std::vector<uint8_t> buf(in.begin(), in.end());
  try {
    m->finish(buf);
  } catch (const Botan::Invalid_Authentication_Tag&) {
    throw SealException(SealError::AuthFailed, "authentication failed");
  }
  return buf;
}

// key(32) || nonce(12), unique per object because the KEM shared secret is.
Botan::secure_vector<uint8_t> wrap_material(std::span<const uint8_t> shared_secret) {
  auto kdf = Botan::KDF::create_or_throw("HKDF(SHA-256)");
  return kdf->derive_key(kDekSize + kNonceSize, shared_secret, "", kWrapLabel);
}

std::vector<uint8_t> data_aad(uint8_t ver, uint8_t suite, std::span<const uint8_t> id) {
  std::vector<uint8_t> aad{ver, suite};
  aad.insert(aad.end(), id.begin(), id.end());
  return aad;
}

}  // namespace

struct Ring::Impl {
  struct Kek {
    uint32_t id;
    std::unique_ptr<Botan::ML_KEM_PublicKey> pub;
    std::unique_ptr<Botan::ML_KEM_PrivateKey> priv;  // null for a public-only entry
  };
  std::vector<Kek> keks;  // keks[0] seals

  const Kek* find(uint32_t id) const {
    for (const auto& k : keks)
      if (k.id == id) return &k;
    return nullptr;
  }
};

Ring::Ring() = default;
Ring::~Ring() = default;
Ring::Ring(Ring&&) noexcept = default;
Ring& Ring::operator=(Ring&&) noexcept = default;

Ring Ring::parse(std::span<const uint8_t> file) {
  Ring ring;
  ring.impl_ = std::make_unique<Impl>();
  try {
    for (const Record& r : parse_records(file)) {
      if (ring.impl_->find(r.id)) throw SealException(SealError::Invalid, "duplicate kek id");
      Impl::Kek k{r.id, std::make_unique<Botan::ML_KEM_PublicKey>(r.ek, mode()), nullptr};
      if (!r.seed.empty()) {
        k.priv = std::make_unique<Botan::ML_KEM_PrivateKey>(r.seed, mode());
        const auto ek = k.priv->raw_public_key_bits();
        if (!std::ranges::equal(ek, r.ek)) throw SealException(SealError::Invalid, "seed does not match key");
      }
      ring.impl_->keks.push_back(std::move(k));
    }
  } catch (const Botan::Exception&) {
    throw SealException(SealError::Invalid, "invalid ring key");
  }
  return ring;
}

std::vector<uint8_t> seal(const Ring& ring, std::span<const uint8_t> id, std::span<const uint8_t> pt) {
  auto& rng = Botan::system_rng();
  const auto& kek = ring.impl().keks.front();

  Botan::PK_KEM_Encryptor enc(*kek.pub, "Raw");
  const auto kem = enc.encrypt(rng, kDekSize);
  const auto& ct = kem.encapsulated_shared_key();

  std::vector<uint8_t> out(kPrefixSize);
  out[0] = kVersion;
  out[1] = kSuiteMlKem768Aes256Gcm;
  put_be32(&out[2], kek.id);
  std::memcpy(&out[6], ct.data(), kCtSize);

  Botan::secure_vector<uint8_t> dek(kDekSize);
  rng.randomize(dek);
  const auto wm = wrap_material(kem.shared_key());
  const std::span<const uint8_t> wm_span(wm);
  const auto wrapped = gcm(Botan::Cipher_Dir::Encryption, wm_span.first(kDekSize), wm_span.subspan(kDekSize),
                           {out.data(), kPrefixSize}, dek);

  std::array<uint8_t, kNonceSize> nonce;
  rng.randomize(nonce);
  out.insert(out.end(), wrapped.begin(), wrapped.end());
  out.insert(out.end(), nonce.begin(), nonce.end());
  const auto body = gcm(Botan::Cipher_Dir::Encryption, dek, nonce, data_aad(kVersion, kSuiteMlKem768Aes256Gcm, id), pt);
  out.insert(out.end(), body.begin(), body.end());
  return out;
}

std::vector<uint8_t> open(const Ring& ring, std::span<const uint8_t> id, std::span<const uint8_t> blob) {
  if (blob.size() < kSealOverhead || blob[0] != kVersion || blob[1] != kSuiteMlKem768Aes256Gcm)
    throw SealException(SealError::Invalid, "not a sealed object");
  const auto* kek = ring.impl().find(get_be32(&blob[2]));
  if (!kek) throw SealException(SealError::UnknownKek, "unknown kek id");
  if (!kek->priv) throw SealException(SealError::NoSecretKey, "no secret key for kek");

  // ML-KEM decapsulation never fails (implicit rejection): a wrong key or ciphertext
  // yields an unrelated secret, which the wrap tag then rejects.
  Botan::PK_KEM_Decryptor dec(*kek->priv, Botan::system_rng(), "Raw");
  const auto ss = dec.decrypt(blob.subspan(6, kCtSize), kDekSize);
  const auto wm = wrap_material(ss);
  const std::span<const uint8_t> wm_span(wm);
  const auto dek = gcm(Botan::Cipher_Dir::Decryption, wm_span.first(kDekSize), wm_span.subspan(kDekSize),
                       blob.first(kPrefixSize), blob.subspan(kPrefixSize, kWrappedSize));

  const auto nonce = blob.subspan(kPrefixSize + kWrappedSize, kNonceSize);
  return gcm(Botan::Cipher_Dir::Decryption, dek, nonce, data_aad(blob[0], blob[1], id),
             blob.subspan(kPrefixSize + kWrappedSize + kNonceSize));
}

std::vector<uint8_t> generate_record(uint32_t kek_id) {
  const Botan::ML_KEM_PrivateKey key(Botan::system_rng(), mode());
  const auto ek = key.raw_public_key_bits();
  const auto seed = key.raw_private_key_bits();
  if (ek.size() != kEncapsKeySize || seed.size() != kSeedSize) throw std::runtime_error("unexpected key sizes");
  std::vector<uint8_t> rec(kRecordHeaderSize);
  put_be32(rec.data(), kek_id);
  rec[4] = 1;
  rec.insert(rec.end(), ek.begin(), ek.end());
  rec.insert(rec.end(), seed.begin(), seed.end());
  return rec;
}

std::vector<uint8_t> public_ring(std::span<const uint8_t> file) {
  std::vector<uint8_t> out;
  for (const Record& r : parse_records(file)) {
    const size_t at = out.size();
    out.resize(at + kRecordHeaderSize);
    put_be32(&out[at], r.id);
    out[at + 4] = 0;
    out.insert(out.end(), r.ek.begin(), r.ek.end());
  }
  return out;
}

}  // namespace mycelium
