#include "codec.hpp"

#include <botan/hash.h>
#include <isa-l.h>

#include <algorithm>
#include <climits>
#include <cstring>

namespace mycelium {

Codec::Codec(int k, int m) : k_(k), m_(m) {
  if (k < 1 || m < 0 || k + m > 255) {
    throw std::invalid_argument("codec: need 1 <= k, 0 <= m, k+m <= 255");
  }
  if (k_ == 1 || m_ == 0) {
    return;  // plain copies, or no coding shards: no matrix needed
  }
  const int n = k_ + m_;
  matrix_.resize(size_t(n) * k_);
  gf_gen_cauchy1_matrix(matrix_.data(), n, k_);
  tables_.resize(size_t(k_) * m_ * 32);
  ec_init_tables(k_, m_, &matrix_[size_t(k_) * k_], tables_.data());
}

int Codec::k() const { return k_; }

int Codec::m() const { return m_; }

size_t Codec::shard_size(size_t object_size) const { return (object_size + k_ - 1) / k_; }

void Codec::encode(std::span<const uint8_t> obj, std::span<uint8_t> out) const {
  const size_t len = shard_size(obj.size());
  const int n = k_ + m_;
  if (out.size() != len * n || len > INT_MAX) {
    throw std::invalid_argument("codec: bad output size");
  }
  for (int i = 0; i < k_; i++) {
    uint8_t* dst = out.data() + i * len;
    const size_t lo = std::min(obj.size(), i * len);
    const size_t cnt = std::min(len, obj.size() - lo);
    if (cnt > 0) {
      std::memcpy(dst, obj.data() + lo, cnt);
    }
    std::memset(dst + cnt, 0, len - cnt);
  }
  if (len == 0 || m_ == 0) {
    return;
  }
  if (k_ == 1) {
    for (int i = 1; i < n; i++) {
      std::memcpy(out.data() + i * len, out.data(), len);
    }
    return;
  }
  std::vector<uint8_t*> data(k_), coding(m_);
  for (int i = 0; i < k_; i++) data[i] = out.data() + i * len;
  for (int i = 0; i < m_; i++) coding[i] = out.data() + (k_ + i) * len;
  ec_encode_data(int(len), k_, m_, const_cast<uint8_t*>(tables_.data()), data.data(), coding.data());
}

void Codec::decode(std::span<const uint8_t> shards, std::span<const uint8_t> present, size_t shard_len,
                   std::span<uint8_t> out) const {
  const int n = k_ + m_;
  if (shards.size() != shard_len * n || present.size() != size_t(n) || out.size() > shard_len * k_ ||
      shard_len > INT_MAX) {
    throw std::invalid_argument("codec: bad buffer sizes");
  }
  std::vector<int> avail;
  for (int i = 0; i < n; i++) {
    if (present[i]) avail.push_back(i);
  }
  if (int(avail.size()) < k_) {
    throw TooFewShards();
  }
  if (out.empty()) {
    return;
  }
  if (k_ == 1) {
    std::memcpy(out.data(), shards.data() + avail[0] * shard_len, out.size());
    return;
  }

  // data = inv(A_R) * shards_R for the first k available rows R; only the rows of
  // missing data shards are computed.
  std::vector<int> missing;
  for (int i = 0; i < k_; i++) {
    if (!present[i]) missing.push_back(i);
  }
  std::vector<uint8_t> recovered(missing.size() * shard_len);
  if (!missing.empty()) {
    std::vector<uint8_t> sub(size_t(k_) * k_), inv(size_t(k_) * k_);
    for (int r = 0; r < k_; r++) {
      std::memcpy(&sub[size_t(r) * k_], &matrix_[size_t(avail[r]) * k_], k_);
    }
    if (gf_invert_matrix(sub.data(), inv.data(), k_) != 0) {
      throw std::runtime_error("codec: singular decode matrix");
    }
    const int rows = int(missing.size());
    std::vector<uint8_t> dm(size_t(rows) * k_), tables(size_t(k_) * rows * 32);
    for (int r = 0; r < rows; r++) {
      std::memcpy(&dm[size_t(r) * k_], &inv[size_t(missing[r]) * k_], k_);
    }
    ec_init_tables(k_, rows, dm.data(), tables.data());
    std::vector<uint8_t*> src(k_), dst(rows);
    for (int r = 0; r < k_; r++) src[r] = const_cast<uint8_t*>(shards.data() + avail[r] * shard_len);
    for (int r = 0; r < rows; r++) dst[r] = recovered.data() + r * shard_len;
    ec_encode_data(int(shard_len), k_, rows, tables.data(), src.data(), dst.data());
  }

  size_t j = 0;
  for (int i = 0; i < k_ && i * shard_len < out.size(); i++) {
    const uint8_t* s = present[i] ? shards.data() + i * shard_len : recovered.data() + (j++) * shard_len;
    std::memcpy(out.data() + i * shard_len, s, std::min(shard_len, out.size() - i * shard_len));
  }
}

std::array<uint8_t, 32> sha256(std::span<const uint8_t> bytes) {
  auto h = Botan::HashFunction::create_or_throw("SHA-256");
  h->update(bytes.data(), bytes.size());
  std::array<uint8_t, 32> out;
  h->final(out.data());
  return out;
}

}  // namespace mycelium
