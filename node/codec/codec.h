// C ABI over mycelium::Codec, consumed by cgo. Shards travel as one contiguous
// buffer of n * shard_len bytes, slot i at offset i * shard_len.
#ifndef MYCELIUM_CODEC_H
#define MYCELIUM_CODEC_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct mc_codec mc_codec;

typedef enum {
  MC_CODEC_OK = 0,
  MC_CODEC_INVALID,
  MC_CODEC_TOO_FEW_SHARDS,
} mc_codec_status;

mc_codec_status mc_codec_new(int k, int m, mc_codec** out);
void mc_codec_free(mc_codec* c);

size_t mc_codec_shard_size(const mc_codec* c, size_t object_size);

// out must hold n * mc_codec_shard_size(c, len) bytes.
mc_codec_status mc_codec_encode(const mc_codec* c, const uint8_t* obj, size_t len, uint8_t* out);

// present[i] != 0 marks slot i as available; at least k must be. out holds object_size bytes.
mc_codec_status mc_codec_decode(const mc_codec* c, const uint8_t* shards, const uint8_t* present,
                                size_t shard_len, uint8_t* out, size_t object_size);

void mc_sha256(const uint8_t* data, size_t len, uint8_t out[32]);

#ifdef __cplusplus
}
#endif

#endif
