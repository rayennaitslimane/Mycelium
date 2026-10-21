// C ABI over mycelium::Ring, consumed by cgo. The caller allocates every output buffer.
#ifndef MYCELIUM_SEAL_H
#define MYCELIUM_SEAL_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct mc_ring mc_ring;

typedef enum {
  MC_SEAL_OK = 0,
  MC_SEAL_INVALID,
  MC_SEAL_UNKNOWN_KEK,
  MC_SEAL_NO_SECRET_KEY,
  MC_SEAL_AUTH_FAILED,
  MC_SEAL_INTERNAL,
} mc_seal_status;

// Parses a ring file. The buffer is not retained.
mc_seal_status mc_ring_load(const uint8_t* file, size_t len, mc_ring** out);
void mc_ring_free(mc_ring* r);

size_t mc_seal_overhead(void);

// out must hold pt_len + mc_seal_overhead() bytes.
mc_seal_status mc_seal(const mc_ring* r, const uint8_t* id, size_t id_len, const uint8_t* pt,
                       size_t pt_len, uint8_t* out);

// out must hold blob_len - mc_seal_overhead() bytes.
mc_seal_status mc_open(const mc_ring* r, const uint8_t* id, size_t id_len, const uint8_t* blob,
                       size_t blob_len, uint8_t* out);

size_t mc_ring_secret_record_size(void);

// out must hold mc_ring_secret_record_size() bytes.
mc_seal_status mc_ring_generate(uint32_t kek_id, uint8_t* out);

// out must hold len bytes; *out_len receives the size written.
mc_seal_status mc_ring_public(const uint8_t* file, size_t len, uint8_t* out, size_t* out_len);

#ifdef __cplusplus
}
#endif

#endif
