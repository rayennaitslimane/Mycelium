// Translates the C ABI to mycelium::Ring. No exception crosses it.
#include "seal.h"

#include <algorithm>
#include <cstring>

#include "seal.hpp"

struct mc_ring {
  mycelium::Ring impl;
};

namespace {

mc_seal_status status_of(const mycelium::SealException& e) {
  switch (e.code) {
    case mycelium::SealError::Invalid: return MC_SEAL_INVALID;
    case mycelium::SealError::UnknownKek: return MC_SEAL_UNKNOWN_KEK;
    case mycelium::SealError::NoSecretKey: return MC_SEAL_NO_SECRET_KEY;
    case mycelium::SealError::AuthFailed: return MC_SEAL_AUTH_FAILED;
  }
  return MC_SEAL_INTERNAL;
}

void copy_out(const std::vector<uint8_t>& v, uint8_t* out) {
  if (!v.empty()) std::memcpy(out, v.data(), v.size());
}

}  // namespace

extern "C" {

mc_seal_status mc_ring_load(const uint8_t* file, size_t len, mc_ring** out) {
  try {
    *out = new mc_ring{mycelium::Ring::parse({file, len})};
    return MC_SEAL_OK;
  } catch (const mycelium::SealException& e) {
    return status_of(e);
  } catch (...) {
    return MC_SEAL_INTERNAL;
  }
}

void mc_ring_free(mc_ring* r) { delete r; }

size_t mc_seal_overhead(void) { return mycelium::kSealOverhead; }

mc_seal_status mc_seal(const mc_ring* r, const uint8_t* id, size_t id_len, const uint8_t* pt,
                       size_t pt_len, uint8_t* out) {
  try {
    copy_out(mycelium::seal(r->impl, {id, id_len}, {pt, pt_len}), out);
    return MC_SEAL_OK;
  } catch (const mycelium::SealException& e) {
    return status_of(e);
  } catch (...) {
    return MC_SEAL_INTERNAL;
  }
}

mc_seal_status mc_open(const mc_ring* r, const uint8_t* id, size_t id_len, const uint8_t* blob,
                       size_t blob_len, uint8_t* out) {
  try {
    copy_out(mycelium::open(r->impl, {id, id_len}, {blob, blob_len}), out);
    return MC_SEAL_OK;
  } catch (const mycelium::SealException& e) {
    return status_of(e);
  } catch (...) {
    return MC_SEAL_INTERNAL;
  }
}

size_t mc_ring_secret_record_size(void) { return mycelium::kSecretRecordSize; }

mc_seal_status mc_ring_generate(uint32_t kek_id, uint8_t* out) {
  try {
    copy_out(mycelium::generate_record(kek_id), out);
    return MC_SEAL_OK;
  } catch (...) {
    return MC_SEAL_INTERNAL;
  }
}

mc_seal_status mc_ring_public(const uint8_t* file, size_t len, uint8_t* out, size_t* out_len) {
  try {
    const auto v = mycelium::public_ring({file, len});
    copy_out(v, out);
    *out_len = v.size();
    return MC_SEAL_OK;
  } catch (const mycelium::SealException& e) {
    return status_of(e);
  } catch (...) {
    return MC_SEAL_INTERNAL;
  }
}

}  // extern "C"
