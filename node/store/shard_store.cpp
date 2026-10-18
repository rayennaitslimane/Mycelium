#include "shard_store.hpp"

#include <botan/hash.h>
#include <fcntl.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <charconv>
#include <fstream>
#include <system_error>

namespace mycelium {

namespace fs = std::filesystem;

namespace {

constexpr char kMagic[4] = {'M', 'C', 'S', '1'};
constexpr size_t kFixedHeader = 4 + 4;   // magic, id length
constexpr size_t kMetaSize = 8 + 1 + 32;  // size, deleted, sha256

[[noreturn]] void throw_errno(const std::string& what) {
  throw std::system_error(errno, std::generic_category(), what);
}

void put_le(std::vector<uint8_t>& b, uint64_t v, int bytes) {
  for (int i = 0; i < bytes; i++) b.push_back(uint8_t(v >> (8 * i)));
}

uint64_t get_le(const uint8_t* p, int bytes) {
  uint64_t v = 0;
  for (int i = 0; i < bytes; i++) v |= uint64_t(p[i]) << (8 * i);
  return v;
}

std::vector<uint8_t> encode(const Shard& s) {
  std::vector<uint8_t> b;
  b.reserve(kFixedHeader + s.meta.id.size() + kMetaSize + s.data.size());
  b.insert(b.end(), kMagic, kMagic + 4);
  put_le(b, s.meta.id.size(), 4);
  b.insert(b.end(), s.meta.id.begin(), s.meta.id.end());
  put_le(b, s.meta.size, 8);
  b.push_back(s.meta.deleted ? 1 : 0);
  b.insert(b.end(), s.meta.sha256.begin(), s.meta.sha256.end());
  b.insert(b.end(), s.data.begin(), s.data.end());
  return b;
}

std::vector<uint8_t> read_file(const fs::path& p, size_t limit = SIZE_MAX) {
  std::ifstream f(p, std::ios::binary);
  if (!f) throw_errno("open " + p.string());
  std::vector<uint8_t> b;
  char buf[64 * 1024];
  while (b.size() < limit) {
    f.read(buf, std::streamsize(std::min(sizeof buf, limit - b.size())));
    if (f.gcount() <= 0) break;
    b.insert(b.end(), buf, buf + f.gcount());
  }
  if (f.bad()) throw_errno("read " + p.string());
  return b;
}

std::string hex(std::span<const uint8_t> b) {
  static const char* digits = "0123456789abcdef";
  std::string s;
  for (uint8_t c : b) {
    s.push_back(digits[c >> 4]);
    s.push_back(digits[c & 15]);
  }
  return s;
}

}  // namespace

ShardStore::ShardStore(fs::path root) : root_(std::move(root)) { fs::create_directories(root_); }

void ShardStore::put_staged(uint32_t pg, uint64_t epoch, const Shard& shard) {
  std::shared_lock pl(purge_mu_);
  PG& g = pg_state(pg);
  std::lock_guard l(g.mu);
  check_fence(pg, g, epoch);
  if (sha256(shard.data) != shard.meta.sha256) {
    throw ChecksumError();
  }
  const fs::path dir = obj_dir(pg, shard.meta.id);
  for (const Entry& e : entries(dir)) {
    if (e.version == shard.meta.version) return;
  }
  ensure_dir(pg_dir(pg));
  ensure_dir(dir);
  write_durable(dir / file_name(shard.meta.version, false, shard.meta.deleted), encode(shard));
}

std::optional<Shard> ShardStore::get(uint32_t pg, uint64_t epoch, const std::string& id,
                                     std::optional<Version> version) {
  std::shared_lock pl(purge_mu_);
  PG& g = pg_state(pg);
  std::lock_guard l(g.mu);
  check_fence(pg, g, epoch);
  const auto es = entries(obj_dir(pg, id));
  for (auto it = es.rbegin(); it != es.rend(); ++it) {
    if (version ? it->version == *version : (it->committed && !it->deleted)) {
      return read_shard(*it);
    }
  }
  return std::nullopt;
}

void ShardStore::commit(uint32_t pg, uint64_t epoch, std::span<const IdVersion> items) {
  std::shared_lock pl(purge_mu_);
  PG& g = pg_state(pg);
  std::lock_guard l(g.mu);
  check_fence(pg, g, epoch);
  for (const IdVersion& it : items) {
    const fs::path dir = obj_dir(pg, it.id);
    const auto es = entries(dir);
    auto target = std::find_if(es.begin(), es.end(), [&](const Entry& e) { return e.version == it.version; });
    if (target == es.end()) continue;
    if (target->deleted) {
      for (const Entry& e : es) {
        if (e.version <= it.version) fs::remove(e.path);
      }
    } else {
      if (!target->committed) fs::rename(target->path, dir / file_name(it.version, true, false));
      for (const Entry& e : es) {
        if (e.version < it.version) fs::remove(e.path);
      }
    }
    sync_dir(dir);
    remove_if_empty(dir);
  }
}

void ShardStore::discard(uint32_t pg, uint64_t epoch, std::span<const IdVersion> items) {
  std::shared_lock pl(purge_mu_);
  PG& g = pg_state(pg);
  std::lock_guard l(g.mu);
  check_fence(pg, g, epoch);
  for (const IdVersion& it : items) {
    const fs::path dir = obj_dir(pg, it.id);
    for (const Entry& e : entries(dir)) {
      if (e.version == it.version) {
        fs::remove(e.path);
        sync_dir(dir);
        remove_if_empty(dir);
      }
    }
  }
}

std::vector<IdVersions> ShardStore::scan(uint32_t pg, uint64_t epoch) {
  std::shared_lock pl(purge_mu_);
  PG& g = pg_state(pg);
  std::lock_guard l(g.mu);
  check_fence(pg, g, epoch);
  if (epoch > read_fence(pg, g)) write_fence(pg, g, epoch);
  std::vector<IdVersions> out;
  std::error_code ec;
  for (const auto& d : fs::directory_iterator(pg_dir(pg), ec)) {
    if (!d.is_directory()) continue;
    const auto es = entries(d.path());
    if (std::none_of(es.begin(), es.end(), [](const Entry& e) { return !e.committed; })) continue;
    IdVersions iv{read_id(es.front().path), {}};
    for (const Entry& e : es) iv.versions.push_back(e.version);
    out.push_back(std::move(iv));
  }
  return out;
}

std::vector<IdVersions> ShardStore::lookup(uint32_t pg, uint64_t epoch, std::span<const std::string> ids) {
  std::shared_lock pl(purge_mu_);
  PG& g = pg_state(pg);
  std::lock_guard l(g.mu);
  check_fence(pg, g, epoch);
  std::vector<IdVersions> out;
  for (const std::string& id : ids) {
    const auto es = entries(obj_dir(pg, id));
    if (es.empty()) continue;
    IdVersions iv{id, {}};
    for (const Entry& e : es) iv.versions.push_back(e.version);
    out.push_back(std::move(iv));
  }
  return out;
}

std::vector<IdVersion> ShardStore::list(uint32_t pg) {
  std::shared_lock pl(purge_mu_);
  PG& g = pg_state(pg);
  std::lock_guard l(g.mu);
  std::vector<IdVersion> out;
  std::error_code ec;
  for (const auto& d : fs::directory_iterator(pg_dir(pg), ec)) {
    if (!d.is_directory()) continue;
    const auto es = entries(d.path());
    for (auto it = es.rbegin(); it != es.rend(); ++it) {
      if (it->committed && !it->deleted) {
        out.push_back({read_id(it->path), it->version});
        break;
      }
    }
  }
  return out;
}

void ShardStore::purge_all() {
  std::unique_lock pl(purge_mu_);
  std::lock_guard l(mu_);
  for (const auto& d : fs::directory_iterator(root_)) {
    if (d.path().filename().string().starts_with("pg-")) fs::remove_all(d.path());
  }
  pgs_.clear();
  sync_dir(root_);
}

ShardStore::PG& ShardStore::pg_state(uint32_t pg) {
  std::lock_guard l(mu_);
  auto& p = pgs_[pg];
  if (!p) p = std::make_unique<PG>();
  return *p;
}

uint64_t ShardStore::read_fence(uint32_t pg, PG& g) {
  if (!g.fence) {
    const fs::path p = pg_dir(pg) / "fence";
    uint64_t f = 0;
    if (fs::exists(p)) {
      const auto b = read_file(p);
      std::from_chars(reinterpret_cast<const char*>(b.data()), reinterpret_cast<const char*>(b.data() + b.size()), f);
    }
    g.fence = f;
  }
  return *g.fence;
}

void ShardStore::check_fence(uint32_t pg, PG& g, uint64_t epoch) {
  if (const uint64_t f = read_fence(pg, g); epoch < f) {
    throw Fenced(f);
  }
}

void ShardStore::write_fence(uint32_t pg, PG& g, uint64_t epoch) {
  ensure_dir(pg_dir(pg));
  const std::string s = std::to_string(epoch);
  write_durable(pg_dir(pg) / "fence", {reinterpret_cast<const uint8_t*>(s.data()), s.size()});
  g.fence = epoch;
}

fs::path ShardStore::pg_dir(uint32_t pg) const { return root_ / ("pg-" + std::to_string(pg)); }

fs::path ShardStore::obj_dir(uint32_t pg, const std::string& id) const {
  return pg_dir(pg) / ("o" + hex(sha256({reinterpret_cast<const uint8_t*>(id.data()), id.size()})));
}

std::vector<ShardStore::Entry> ShardStore::entries(const fs::path& obj_dir) {
  std::vector<Entry> out;
  std::error_code ec;
  for (const auto& d : fs::directory_iterator(obj_dir, ec)) {
    const std::string name = d.path().filename().string();
    if (name.ends_with(".tmp")) {  // left by a crash during write_durable
      fs::remove(d.path(), ec);
      continue;
    }
    const size_t dash = name.find('-'), dot = name.find('.');
    if (dash == std::string::npos || dot == std::string::npos || dot < dash) continue;
    Entry e{};
    const char* p = name.data();
    if (std::from_chars(p, p + dash, e.version.epoch).ec != std::errc{} ||
        std::from_chars(p + dash + 1, p + dot, e.version.seq).ec != std::errc{}) {
      continue;
    }
    const std::string state = name.substr(dot + 1);
    if (state != "s" && state != "c" && state != "sd") continue;
    e.committed = state == "c";
    e.deleted = state == "sd";
    e.path = d.path();
    out.push_back(std::move(e));
  }
  std::sort(out.begin(), out.end(), [](const Entry& a, const Entry& b) { return a.version < b.version; });
  return out;
}

std::string ShardStore::file_name(Version v, bool committed, bool deleted) {
  return std::to_string(v.epoch) + "-" + std::to_string(v.seq) + "." + (committed ? "c" : "s") +
         (deleted ? "d" : "");
}

Shard ShardStore::read_shard(const Entry& e) {
  const auto b = read_file(e.path);
  if (b.size() < kFixedHeader || !std::equal(kMagic, kMagic + 4, b.begin())) throw ChecksumError();
  const size_t id_len = get_le(&b[4], 4);
  if (b.size() < kFixedHeader + id_len + kMetaSize) throw ChecksumError();
  const uint8_t* p = b.data() + kFixedHeader;
  Shard s;
  s.meta.id.assign(reinterpret_cast<const char*>(p), id_len);
  p += id_len;
  s.meta.version = e.version;
  s.meta.committed = e.committed;
  s.meta.size = get_le(p, 8);
  s.meta.deleted = p[8] != 0;
  std::copy(p + 9, p + 9 + 32, s.meta.sha256.begin());
  s.data.assign(p + kMetaSize, b.data() + b.size());
  if (sha256(s.data) != s.meta.sha256) throw ChecksumError();
  return s;
}

std::string ShardStore::read_id(const fs::path& file) {
  auto b = read_file(file, kFixedHeader);
  if (b.size() < kFixedHeader || !std::equal(kMagic, kMagic + 4, b.begin())) {
    throw std::runtime_error("corrupt shard header: " + file.string());
  }
  const size_t id_len = get_le(&b[4], 4);
  b = read_file(file, kFixedHeader + id_len);
  if (b.size() < kFixedHeader + id_len) throw std::runtime_error("corrupt shard header: " + file.string());
  return std::string(b.begin() + kFixedHeader, b.end());
}

void ShardStore::write_durable(const fs::path& path, std::span<const uint8_t> bytes) {
  fs::path tmp = path;
  tmp += ".tmp";
  const int fd = ::open(tmp.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
  if (fd < 0) throw_errno("open " + tmp.string());
  size_t off = 0;
  while (off < bytes.size()) {
    const ssize_t w = ::write(fd, bytes.data() + off, bytes.size() - off);
    if (w < 0 && errno == EINTR) continue;
    if (w < 0) {
      const int err = errno;
      ::close(fd);
      throw std::system_error(err, std::generic_category(), "write " + tmp.string());
    }
    off += size_t(w);
  }
  if (::fsync(fd) != 0) {
    const int err = errno;
    ::close(fd);
    throw std::system_error(err, std::generic_category(), "fsync " + tmp.string());
  }
  if (::close(fd) != 0) throw_errno("close " + tmp.string());
  fs::rename(tmp, path);
  sync_dir(path.parent_path());
}

void ShardStore::sync_dir(const fs::path& dir) {
  const int fd = ::open(dir.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
  if (fd < 0) throw_errno("open " + dir.string());
  const int r = ::fsync(fd);
  const int err = errno;
  ::close(fd);
  if (r != 0) throw std::system_error(err, std::generic_category(), "fsync " + dir.string());
}

void ShardStore::ensure_dir(const fs::path& dir) {
  if (fs::create_directory(dir)) sync_dir(dir.parent_path());
}

void ShardStore::remove_if_empty(const fs::path& obj_dir) {
  std::error_code ec;
  if (fs::is_empty(obj_dir, ec) && !ec && fs::remove(obj_dir, ec)) {
    sync_dir(obj_dir.parent_path());
  }
}

std::array<uint8_t, 32> ShardStore::sha256(std::span<const uint8_t> bytes) {
  auto h = Botan::HashFunction::create_or_throw("SHA-256");
  h->update(bytes.data(), bytes.size());
  std::array<uint8_t, 32> out;
  h->final(out.data());
  return out;
}

}  // namespace mycelium
