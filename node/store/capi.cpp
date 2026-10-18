// Translates the C ABI to mycelium::ShardStore and its exceptions to status codes.
// No exception crosses it.
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <new>

#include "shard_store.hpp"
#include "store.h"

struct mc_store {
  mycelium::ShardStore impl;
};

namespace {

using mycelium::IdVersion;
using mycelium::IdVersions;
using mycelium::Version;

Version ver(const mc_version& v) { return {v.epoch, v.seq}; }

mc_version cver(const Version& v) { return {v.epoch, v.seq}; }

std::string str(const char* p, size_t n) { return n ? std::string(p, n) : std::string(); }

std::vector<IdVersion> items_in(const mc_id_version* items, size_t n) {
  std::vector<IdVersion> out;
  for (size_t i = 0; i < n; i++) out.push_back({str(items[i].id, items[i].id_len), ver(items[i].version)});
  return out;
}

void* xmalloc(size_t n) {
  void* p = std::malloc(n ? n : 1);
  if (!p) throw std::bad_alloc();
  return p;
}

// Copies out to malloc'd C memory; the caller frees it with mc_store_free_id_versions.
mc_id_versions* export_ivs(const std::vector<IdVersions>& v, size_t* n_out) {
  auto* out = static_cast<mc_id_versions*>(xmalloc(v.size() * sizeof(mc_id_versions)));
  std::memset(out, 0, v.size() * sizeof(mc_id_versions));
  try {
    for (size_t i = 0; i < v.size(); i++) {
      out[i].id = static_cast<char*>(xmalloc(v[i].id.size()));
      std::memcpy(out[i].id, v[i].id.data(), v[i].id.size());
      out[i].id_len = v[i].id.size();
      out[i].versions = static_cast<mc_version*>(xmalloc(v[i].versions.size() * sizeof(mc_version)));
      for (size_t j = 0; j < v[i].versions.size(); j++) out[i].versions[j] = cver(v[i].versions[j]);
      out[i].n = v[i].versions.size();
    }
  } catch (...) {
    mc_store_free_id_versions(out, v.size());
    throw;
  }
  *n_out = v.size();
  return out;
}

template <class F>
mc_store_status guard(uint64_t* fence_out, F&& f) {
  try {
    return f();
  } catch (const mycelium::Fenced& e) {
    if (fence_out) *fence_out = e.fence;
    return MC_STORE_FENCED;
  } catch (const mycelium::ChecksumError&) {
    return MC_STORE_CHECKSUM;
  } catch (const std::invalid_argument& e) {
    std::fprintf(stderr, "mycelium store: %s\n", e.what());
    return MC_STORE_INVALID;
  } catch (const std::exception& e) {
    std::fprintf(stderr, "mycelium store: %s\n", e.what());
    return MC_STORE_IO;
  } catch (...) {
    return MC_STORE_IO;
  }
}

}  // namespace

extern "C" {

mc_store_status mc_store_open(const char* root, mc_store** out) {
  return guard(nullptr, [&] {
    *out = new mc_store{mycelium::ShardStore(root)};
    return MC_STORE_OK;
  });
}

void mc_store_close(mc_store* s) { delete s; }

mc_store_status mc_store_put(mc_store* s, uint32_t pg, uint64_t epoch, const char* id, size_t id_len,
                             const mc_shard_meta* meta, const uint8_t* data, size_t len,
                             uint64_t* fence_out) {
  return guard(fence_out, [&] {
    mycelium::Shard sh;
    sh.meta.id = str(id, id_len);
    sh.meta.version = ver(meta->version);
    sh.meta.size = meta->size;
    std::memcpy(sh.meta.sha256.data(), meta->sha256, 32);
    sh.meta.deleted = meta->deleted != 0;
    if (len) sh.data.assign(data, data + len);
    s->impl.put_staged(pg, epoch, sh);
    return MC_STORE_OK;
  });
}

mc_store_status mc_store_get(mc_store* s, uint32_t pg, uint64_t epoch, const char* id, size_t id_len,
                             const mc_version* version, mc_shard_meta* meta_out,
                             uint8_t** data_out, size_t* len_out, uint64_t* fence_out) {
  return guard(fence_out, [&] {
    std::optional<Version> v;
    if (version) v = ver(*version);
    auto sh = s->impl.get(pg, epoch, str(id, id_len), v);
    if (!sh) return MC_STORE_NOT_FOUND;
    meta_out->version = cver(sh->meta.version);
    meta_out->size = sh->meta.size;
    std::memcpy(meta_out->sha256, sh->meta.sha256.data(), 32);
    meta_out->deleted = sh->meta.deleted ? 1 : 0;
    *data_out = nullptr;
    *len_out = sh->data.size();
    if (!sh->data.empty()) {
      *data_out = static_cast<uint8_t*>(xmalloc(sh->data.size()));
      std::memcpy(*data_out, sh->data.data(), sh->data.size());
    }
    return MC_STORE_OK;
  });
}

mc_store_status mc_store_commit(mc_store* s, uint32_t pg, uint64_t epoch,
                                const mc_id_version* items, size_t n, uint64_t* fence_out) {
  return guard(fence_out, [&] {
    s->impl.commit(pg, epoch, items_in(items, n));
    return MC_STORE_OK;
  });
}

mc_store_status mc_store_discard(mc_store* s, uint32_t pg, uint64_t epoch,
                                 const mc_id_version* items, size_t n, uint64_t* fence_out) {
  return guard(fence_out, [&] {
    s->impl.discard(pg, epoch, items_in(items, n));
    return MC_STORE_OK;
  });
}

mc_store_status mc_store_scan(mc_store* s, uint32_t pg, uint64_t epoch, mc_id_versions** out,
                              size_t* n_out, uint64_t* fence_out) {
  return guard(fence_out, [&] {
    *out = export_ivs(s->impl.scan(pg, epoch), n_out);
    return MC_STORE_OK;
  });
}

mc_store_status mc_store_lookup(mc_store* s, uint32_t pg, uint64_t epoch, const mc_id* ids,
                                size_t n, mc_id_versions** out, size_t* n_out, uint64_t* fence_out) {
  return guard(fence_out, [&] {
    std::vector<std::string> in;
    for (size_t i = 0; i < n; i++) in.push_back(str(ids[i].id, ids[i].id_len));
    *out = export_ivs(s->impl.lookup(pg, epoch, in), n_out);
    return MC_STORE_OK;
  });
}

mc_store_status mc_store_list(mc_store* s, uint32_t pg, mc_id_versions** out, size_t* n_out) {
  return guard(nullptr, [&] {
    std::vector<IdVersions> v;
    for (auto& it : s->impl.list(pg)) v.push_back({std::move(it.id), {it.version}});
    *out = export_ivs(v, n_out);
    return MC_STORE_OK;
  });
}

mc_store_status mc_store_purge_all(mc_store* s) {
  return guard(nullptr, [&] {
    s->impl.purge_all();
    return MC_STORE_OK;
  });
}

void mc_store_free(void* p) { std::free(p); }

void mc_store_free_id_versions(mc_id_versions* p, size_t n) {
  if (!p) return;
  for (size_t i = 0; i < n; i++) {
    std::free(p[i].id);
    std::free(p[i].versions);
  }
  std::free(p);
}

}  // extern "C"
