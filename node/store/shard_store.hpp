// Shard files per PG: staged/committed versions, tombstones, the per-PG fence,
// checksums and fsync. No network, no map.
//
// Layout: <root>/pg-<pg>/fence holds the fence epoch; <root>/pg-<pg>/o<sha256(id)>/
// holds one file per version, named <epoch>-<seq>.<s|c|sd> (staged, committed,
// staged tombstone). A committed tombstone is never kept: committing it removes the
// object. Each file is: "MCS1", u32 id length, id, u64 size, u8 deleted, sha256[32],
// data (integers little-endian). Every change is fsynced (file, then directory)
// before returning. All operations on a PG run under its mutex, so the fence check
// and the operation it guards are atomic.
#pragma once

#include <array>
#include <compare>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <span>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

namespace mycelium {

struct Version {
  uint64_t epoch = 0;
  uint64_t seq = 0;
  auto operator<=>(const Version&) const = default;
};

struct IdVersion {
  std::string id;
  Version version;
};

struct IdVersions {
  std::string id;
  std::vector<Version> versions;
};

struct ShardMeta {
  std::string id;
  Version version;
  uint64_t size = 0;  // whole object size
  std::array<uint8_t, 32> sha256{};
  bool deleted = false;
  bool committed = false;
};

struct Shard {
  ShardMeta meta;
  std::vector<uint8_t> data;
};

struct Fenced : std::runtime_error {
  explicit Fenced(uint64_t f) : std::runtime_error("fenced"), fence(f) {}
  uint64_t fence;
};

struct ChecksumError : std::runtime_error {
  ChecksumError() : std::runtime_error("checksum mismatch") {}
};

class ShardStore {
 public:
  explicit ShardStore(std::filesystem::path root);

  // Verifies sha256, stores the version staged beside older ones. Idempotent.
  void put_staged(uint32_t pg, uint64_t epoch, const Shard& shard);

  // Latest committed if version is empty; staged versions only when named. Verifies sha256.
  std::optional<Shard> get(uint32_t pg, uint64_t epoch, const std::string& id,
                           std::optional<Version> version);

  // Promotes each version and deletes older ones; a committed tombstone removes the
  // object (every version up to it). Versions not held are ignored. Idempotent.
  void commit(uint32_t pg, uint64_t epoch, std::span<const IdVersion> items);

  // Deletes each version whatever its state. Idempotent.
  void discard(uint32_t pg, uint64_t epoch, std::span<const IdVersion> items);

  // Raises the fence to epoch, returns every version of each dirty object.
  std::vector<IdVersions> scan(uint32_t pg, uint64_t epoch);

  // Versions held (staged or committed) for each id; ids not held are omitted.
  std::vector<IdVersions> lookup(uint32_t pg, uint64_t epoch, std::span<const std::string> ids);

  // Latest committed version of each local object. Not fenced.
  std::vector<IdVersion> list(uint32_t pg);

  void purge_all();

 private:
  struct PG {
    std::mutex mu;
    std::optional<uint64_t> fence;  // cached fence file
  };

  struct Entry {
    Version version;
    bool committed;
    bool deleted;
    std::filesystem::path path;
  };

  PG& pg_state(uint32_t pg);
  uint64_t read_fence(uint32_t pg, PG& g);
  void check_fence(uint32_t pg, PG& g, uint64_t epoch);  // throws Fenced
  void write_fence(uint32_t pg, PG& g, uint64_t epoch);

  std::filesystem::path pg_dir(uint32_t pg) const;
  std::filesystem::path obj_dir(uint32_t pg, const std::string& id) const;

  static std::vector<Entry> entries(const std::filesystem::path& obj_dir);  // sorted by version
  static std::string file_name(Version v, bool committed, bool deleted);
  static Shard read_shard(const Entry& e);  // throws ChecksumError
  static std::string read_id(const std::filesystem::path& file);

  // Writes a temp file, fsyncs it, renames it into place and fsyncs the directory.
  static void write_durable(const std::filesystem::path& path, std::span<const uint8_t> bytes);
  static void sync_dir(const std::filesystem::path& dir);
  static void ensure_dir(const std::filesystem::path& dir);
  static void remove_if_empty(const std::filesystem::path& obj_dir);
  static std::array<uint8_t, 32> sha256(std::span<const uint8_t> bytes);  // Botan

  std::filesystem::path root_;
  std::shared_mutex purge_mu_;  // shared by every operation, exclusive for purge_all
  std::mutex mu_;               // guards pgs_
  std::unordered_map<uint32_t, std::unique_ptr<PG>> pgs_;
};

}  // namespace mycelium
