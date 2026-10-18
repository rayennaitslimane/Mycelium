// C ABI over mycelium::ShardStore, consumed by cgo. Ids are byte strings with an
// explicit length. Every fenced operation fails with MC_STORE_FENCED and sets
// *fence_out when epoch < fence. Outputs are malloc'd and freed with the
// mc_store_free* functions.
#ifndef MYCELIUM_STORE_H
#define MYCELIUM_STORE_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct mc_store mc_store;

typedef enum {
  MC_STORE_OK = 0,
  MC_STORE_NOT_FOUND,
  MC_STORE_FENCED,
  MC_STORE_CHECKSUM,
  MC_STORE_IO,
  MC_STORE_INVALID,
} mc_store_status;

typedef struct {
  uint64_t epoch;
  uint64_t seq;
} mc_version;

typedef struct {
  const char* id;
  size_t id_len;
} mc_id;

typedef struct {
  const char* id;
  size_t id_len;
  mc_version version;
} mc_id_version;

typedef struct {
  char* id;
  size_t id_len;
  mc_version* versions;
  size_t n;
} mc_id_versions;

typedef struct {
  mc_version version;
  uint64_t size;  // whole object size
  uint8_t sha256[32];
  int deleted;
} mc_shard_meta;

mc_store_status mc_store_open(const char* root, mc_store** out);
void mc_store_close(mc_store* s);

// Verifies sha256 and stores the version staged beside older ones (fsync).
mc_store_status mc_store_put(mc_store* s, uint32_t pg, uint64_t epoch, const char* id, size_t id_len,
                             const mc_shard_meta* meta, const uint8_t* data, size_t len,
                             uint64_t* fence_out);

// version == NULL means the latest committed version. *data_out is freed with mc_store_free.
mc_store_status mc_store_get(mc_store* s, uint32_t pg, uint64_t epoch, const char* id, size_t id_len,
                             const mc_version* version, mc_shard_meta* meta_out,
                             uint8_t** data_out, size_t* len_out, uint64_t* fence_out);

// Promotes each version and deletes older ones; a committed tombstone removes the
// object. Versions not held are ignored.
mc_store_status mc_store_commit(mc_store* s, uint32_t pg, uint64_t epoch,
                                const mc_id_version* items, size_t n, uint64_t* fence_out);

// Deletes each version whatever its state. Versions not held are ignored.
mc_store_status mc_store_discard(mc_store* s, uint32_t pg, uint64_t epoch,
                                 const mc_id_version* items, size_t n, uint64_t* fence_out);

// Raises the fence to epoch and returns every version of the dirty objects.
mc_store_status mc_store_scan(mc_store* s, uint32_t pg, uint64_t epoch, mc_id_versions** out,
                              size_t* n_out, uint64_t* fence_out);

// Versions held (staged or committed) for each id; ids not held are omitted.
mc_store_status mc_store_lookup(mc_store* s, uint32_t pg, uint64_t epoch, const mc_id* ids,
                                size_t n, mc_id_versions** out, size_t* n_out, uint64_t* fence_out);

// Latest committed version (one per item) of each local object. Not fenced.
mc_store_status mc_store_list(mc_store* s, uint32_t pg, mc_id_versions** out, size_t* n_out);

mc_store_status mc_store_purge_all(mc_store* s);

void mc_store_free(void* p);
void mc_store_free_id_versions(mc_id_versions* p, size_t n);

#ifdef __cplusplus
}
#endif

#endif
