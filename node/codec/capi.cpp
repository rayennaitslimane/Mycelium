// Translates the C ABI to mycelium::Codec. No exception crosses it.
#include "codec.h"

#include <cstring>

#include "codec.hpp"

struct mc_codec {
  mycelium::Codec impl;
};

extern "C" {

mc_codec_status mc_codec_new(int k, int m, mc_codec** out) {
  try {
    *out = new mc_codec{mycelium::Codec(k, m)};
    return MC_CODEC_OK;
  } catch (...) {
    return MC_CODEC_INVALID;
  }
}

void mc_codec_free(mc_codec* c) { delete c; }

size_t mc_codec_shard_size(const mc_codec* c, size_t object_size) {
  return c->impl.shard_size(object_size);
}

mc_codec_status mc_codec_encode(const mc_codec* c, const uint8_t* obj, size_t len, uint8_t* out) {
  try {
    const size_t total = c->impl.shard_size(len) * size_t(c->impl.k() + c->impl.m());
    c->impl.encode({obj, len}, {out, total});
    return MC_CODEC_OK;
  } catch (...) {
    return MC_CODEC_INVALID;
  }
}

mc_codec_status mc_codec_decode(const mc_codec* c, const uint8_t* shards, const uint8_t* present,
                                size_t shard_len, uint8_t* out, size_t object_size) {
  try {
    const size_t n = size_t(c->impl.k() + c->impl.m());
    c->impl.decode({shards, n * shard_len}, {present, n}, shard_len, {out, object_size});
    return MC_CODEC_OK;
  } catch (const mycelium::TooFewShards&) {
    return MC_CODEC_TOO_FEW_SHARDS;
  } catch (...) {
    return MC_CODEC_INVALID;
  }
}

void mc_sha256(const uint8_t* data, size_t len, uint8_t out[32]) {
  const auto sum = mycelium::sha256({data, len});
  std::memcpy(out, sum.data(), sum.size());
}

}  // extern "C"
