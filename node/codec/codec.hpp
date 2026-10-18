// Erasure encode/decode (ISA-L) and sha256 (Botan). Pure: no I/O, no state beyond tables.
// A Codec is immutable after construction, so it is safe for concurrent use.
#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <span>
#include <stdexcept>
#include <vector>

namespace mycelium {

struct TooFewShards : std::runtime_error {
  TooFewShards() : std::runtime_error("fewer than k shards present") {}
};

// Codec splits an object into k data shards (zero-padded stripes) plus m coding
// shards (Cauchy Reed-Solomon over GF(2^8)). k == 1 yields n plain copies.
class Codec {
 public:
  Codec(int k, int m);  // throws std::invalid_argument unless 1 <= k, 0 <= m, k+m <= 255

  int k() const;
  int m() const;
  size_t shard_size(size_t object_size) const;

  // out holds n * shard_size(obj.size()) bytes.
  void encode(std::span<const uint8_t> obj, std::span<uint8_t> out) const;

  // shards holds n * shard_len bytes; present[i] != 0 marks slot i available.
  // out receives the object (out.size() bytes). Throws TooFewShards.
  void decode(std::span<const uint8_t> shards, std::span<const uint8_t> present, size_t shard_len,
              std::span<uint8_t> out) const;

 private:
  int k_;
  int m_;
  std::vector<uint8_t> matrix_;  // n x k encode matrix: identity on top of Cauchy rows
  std::vector<uint8_t> tables_;  // ISA-L expanded tables for the m coding rows
};

std::array<uint8_t, 32> sha256(std::span<const uint8_t> bytes);

}  // namespace mycelium
