#include <gtest/gtest.h>

#include "Models.hpp"
#include "Crypto.hpp"

#include <botan/auto_rng.h>
#include <botan/pk_algs.h>
#include <botan/pkcs8.h>
#include <botan/pubkey.h>

#include <array>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <stdexcept>
#include <vector>

namespace {

// Helper to build deterministic plaintext shards
std::vector<PlainShard> MakePlainShards() {
    std::vector<PlainShard> shards;

    PlainShard s0;
    s0.index = 0;
    s0.bytes = {1, 2, 3, 4, 5, 6, 7, 8};
    shards.push_back(s0);

    PlainShard s1;
    s1.index = 1;
    s1.bytes = {10, 20, 30, 40, 50, 60, 70, 80};
    shards.push_back(s1);

    PlainShard s2;
    s2.index = 2;
    s2.bytes = {100, 101, 102, 103, 104, 105, 106, 107};
    shards.push_back(s2);

    return shards;
}

// Fixed test key (32 bytes for AES-256)
std::array<uint8_t, 32> MakeTestKey() {
    return {
        0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
        0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F,
        0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17,
        0x18, 0x19, 0x1A, 0x1B, 0x1C, 0x1D, 0x1E, 0x1F
    };
}

} // namespace

TEST(CryptoTest, HappySerializeDeserializePath) {
    EncryptedShard original;
    original.index = 42;
    original.nonce = {1, 2, 3, 4, 5};
    original.ciphertext = {10, 20, 30, 40, 50, 60};

    Bytes serialized = original.serialize();
    EncryptedShard decoded = EncryptedShard::deserialize(serialized);

    EXPECT_EQ(decoded.index, original.index);
    EXPECT_EQ(decoded.nonce, original.nonce);
    EXPECT_EQ(decoded.ciphertext, original.ciphertext);
}

TEST(CryptoTest, DeserializeFailsOnEmptyBuffer) {
    Bytes empty;

    EXPECT_THROW(
        EncryptedShard::deserialize(empty),
        std::invalid_argument
    );
}

TEST(CryptoTest, ChecksumIsDeterministic) {
    Bytes data = {1, 2, 3, 4, 5};

    Bytes c1 = checksum_sha256(data);
    Bytes c2 = checksum_sha256(data);

    EXPECT_EQ(c1, c2);
    EXPECT_EQ(c1.size(), 32u); // SHA-256 digest size
}

TEST(CryptoTest, ChecksumChangesWhenDataChanges) {
    Bytes d1 = {1, 2, 3, 4, 5};
    Bytes d2 = {1, 2, 3, 4, 6};

    Bytes c1 = checksum_sha256(d1);
    Bytes c2 = checksum_sha256(d2);

    EXPECT_NE(c1, c2);
}

TEST(CryptoTest, EncryptDecryptRoundTripSucceeds) {
    const auto key = MakeTestKey();
    const auto original_shards = MakePlainShards();

    std::vector<EncryptedShard> encrypted = encrypt_shards(original_shards, key);
    std::vector<PlainShard> decrypted = decrypt_shards(encrypted, key, false);

    ASSERT_EQ(decrypted.size(), original_shards.size());

    for (size_t i = 0; i < original_shards.size(); ++i) {
        EXPECT_EQ(decrypted[i].index, original_shards[i].index);
        EXPECT_EQ(decrypted[i].bytes, original_shards[i].bytes);
    }
}

TEST(CryptoTest, EncryptionPreservesShardIndexes) {
    const auto key = MakeTestKey();
    const auto original_shards = MakePlainShards();

    std::vector<EncryptedShard> encrypted = encrypt_shards(original_shards, key);

    ASSERT_EQ(encrypted.size(), original_shards.size());

    for (size_t i = 0; i < original_shards.size(); ++i) {
        EXPECT_EQ(encrypted[i].index, original_shards[i].index);
    }
}

TEST(CryptoTest, CiphertextDiffersFromPlaintext) {
    const auto key = MakeTestKey();
    const auto original_shards = MakePlainShards();

    std::vector<EncryptedShard> encrypted = encrypt_shards(original_shards, key);

    ASSERT_EQ(encrypted.size(), original_shards.size());

    for (size_t i = 0; i < original_shards.size(); ++i) {
        EXPECT_NE(encrypted[i].ciphertext, original_shards[i].bytes);
    }
}

TEST(CryptoTest, DecryptFailsWhenCiphertextIsTampered) {
    const auto key = MakeTestKey();
    const auto original_shards = MakePlainShards();

    std::vector<EncryptedShard> encrypted = encrypt_shards(original_shards, key);
    ASSERT_FALSE(encrypted.empty());
    ASSERT_FALSE(encrypted[0].ciphertext.empty());

    // Tamper with ciphertext
    encrypted[0].ciphertext[0] ^= 0xFF;

    EXPECT_THROW(
        decrypt_shards(encrypted, key, false),
        std::runtime_error
    );
}

TEST(CryptoTest, DecryptFailsWhenNonceIsTampered) {
    const auto key = MakeTestKey();
    const auto original_shards = MakePlainShards();

    std::vector<EncryptedShard> encrypted = encrypt_shards(original_shards, key);
    ASSERT_FALSE(encrypted.empty());
    ASSERT_FALSE(encrypted[0].nonce.empty());

    // Tamper with nonce
    encrypted[0].nonce[0] ^= 0xFF;

    EXPECT_THROW(
        decrypt_shards(encrypted, key, false),
        std::runtime_error
    );
}

TEST(CryptoTest, DecryptFailsWithWrongKey) {
    const auto correct_key = MakeTestKey();
    auto wrong_key = MakeTestKey();
    wrong_key[0] ^= 0xAA; // make key different

    const auto original_shards = MakePlainShards();

    std::vector<EncryptedShard> encrypted = encrypt_shards(original_shards, correct_key);

    EXPECT_THROW(
        decrypt_shards(encrypted, wrong_key, false),
        std::runtime_error
    );
}

TEST(CryptoTest, DecryptFailsWhenIndexAADIsTampered) {
    const auto key = MakeTestKey();
    const auto original_shards = MakePlainShards();

    std::vector<EncryptedShard> encrypted = encrypt_shards(original_shards, key);
    ASSERT_FALSE(encrypted.empty());

    // Tamper with authenticated metadata (AAD)
    encrypted[0].index ^= 1u;

    EXPECT_THROW(
        decrypt_shards(encrypted, key, false),
        std::runtime_error
    );
}

TEST(CryptoTest, EncryptDecryptEmptyShardListSucceeds) {
    const auto key = MakeTestKey();
    std::vector<PlainShard> empty;

    std::vector<EncryptedShard> encrypted = encrypt_shards(empty, key);
    std::vector<PlainShard> decrypted = decrypt_shards(encrypted, key, false);

    EXPECT_TRUE(encrypted.empty());
    EXPECT_TRUE(decrypted.empty());
}

TEST(CryptoTest, DekWrapUnwrapRoundTripSucceeds) {
    Botan::AutoSeeded_RNG rng;
    auto kek = Botan::create_private_key("ML-KEM", rng, "ML-KEM-768");

    std::array<uint8_t, 32> dek;
    rng.randomize(dek.data(), dek.size());

    Bytes wrapped = encrypt_dek(dek, *kek->public_key());
    std::array<uint8_t, 32> recovered = decrypt_dek(wrapped, *kek);

    EXPECT_EQ(recovered, dek);
}

TEST(CryptoTest, DekWrapProducesDifferentCiphertextEachTime) {
    Botan::AutoSeeded_RNG rng;
    auto kek = Botan::create_private_key("ML-KEM", rng, "ML-KEM-768");

    std::array<uint8_t, 32> dek{};
    dek.fill(0x42);

    Bytes wrapped1 = encrypt_dek(dek, *kek->public_key());
    Bytes wrapped2 = encrypt_dek(dek, *kek->public_key());

    EXPECT_NE(wrapped1, wrapped2);
}

TEST(CryptoTest, DekUnwrapFailsWithWrongKey) {
    Botan::AutoSeeded_RNG rng;
    auto kek1 = Botan::create_private_key("ML-KEM", rng, "ML-KEM-768");
    auto kek2 = Botan::create_private_key("ML-KEM", rng, "ML-KEM-768");

    std::array<uint8_t, 32> dek{};
    dek.fill(0xAB);

    Bytes wrapped = encrypt_dek(dek, *kek1->public_key());

    EXPECT_THROW(decrypt_dek(wrapped, *kek2), std::exception);
}

TEST(CryptoTest, DekUnwrapFailsOnTruncatedPayload) {
    Botan::AutoSeeded_RNG rng;
    auto kek = Botan::create_private_key("ML-KEM", rng, "ML-KEM-768");

    std::array<uint8_t, 32> dek{};
    Bytes wrapped = encrypt_dek(dek, *kek->public_key());

    Bytes truncated(wrapped.begin(), wrapped.begin() + 100);

    EXPECT_THROW(decrypt_dek(truncated, *kek), std::runtime_error);
}

TEST(CryptoTest, DekUnwrapFailsOnTamperedCiphertext) {
    Botan::AutoSeeded_RNG rng;
    auto kek = Botan::create_private_key("ML-KEM", rng, "ML-KEM-768");

    std::array<uint8_t, 32> dek{};
    dek.fill(0x77);

    Bytes wrapped = encrypt_dek(dek, *kek->public_key());
    wrapped.back() ^= 0xFF;

    EXPECT_THROW(decrypt_dek(wrapped, *kek), std::exception);
}

// =========================================================
// Tests for HKDF-based KEM key derivation
// =========================================================

TEST(CryptoTest, DekWrapUnwrapWithKdfProducesCorrectKey) {
    // Verify that encrypt_dek/decrypt_dek round-trip produces the exact
    // original DEK when using HKDF-derived shared secret.
    Botan::AutoSeeded_RNG rng;
    auto kek = Botan::create_private_key("ML-KEM", rng, "ML-KEM-768");

    std::array<uint8_t, 32> dek;
    rng.randomize(dek.data(), dek.size());

    Bytes wrapped = encrypt_dek(dek, *kek->public_key());
    std::array<uint8_t, 32> recovered = decrypt_dek(wrapped, *kek);

    // Byte-exact comparison: no corruption from scrubbing or KDF mismatch
    for (size_t i = 0; i < 32; ++i) {
        EXPECT_EQ(recovered[i], dek[i]) << "Mismatch at byte " << i;
    }
}

TEST(CryptoTest, DekWrapUnwrapMultipleKeysIndependent) {
    // Each KEK produces a different wrapped DEK that only it can unwrap.
    // This verifies the KDF incorporates key-specific material.
    Botan::AutoSeeded_RNG rng;
    auto kek_a = Botan::create_private_key("ML-KEM", rng, "ML-KEM-768");
    auto kek_b = Botan::create_private_key("ML-KEM", rng, "ML-KEM-768");

    std::array<uint8_t, 32> dek{};
    dek.fill(0x55);

    Bytes wrapped_a = encrypt_dek(dek, *kek_a->public_key());
    Bytes wrapped_b = encrypt_dek(dek, *kek_b->public_key());

    // Same DEK wrapped with different keys produces different ciphertext
    EXPECT_NE(wrapped_a, wrapped_b);

    // Each key can only unwrap its own wrapped DEK
    std::array<uint8_t, 32> recovered_a = decrypt_dek(wrapped_a, *kek_a);
    std::array<uint8_t, 32> recovered_b = decrypt_dek(wrapped_b, *kek_b);

    EXPECT_EQ(recovered_a, dek);
    EXPECT_EQ(recovered_b, dek);

    // Cross-decryption fails
    EXPECT_THROW(decrypt_dek(wrapped_a, *kek_b), std::exception);
    EXPECT_THROW(decrypt_dek(wrapped_b, *kek_a), std::exception);
}

TEST(CryptoTest, DekWrapOutputSizeIsConsistent) {
    // Verify wrapped DEK size is deterministic (encap_key + nonce + ciphertext+tag)
    // regardless of DEK content.
    Botan::AutoSeeded_RNG rng;
    auto kek = Botan::create_private_key("ML-KEM", rng, "ML-KEM-768");

    std::array<uint8_t, 32> dek1{};
    dek1.fill(0x00);
    std::array<uint8_t, 32> dek2{};
    dek2.fill(0xFF);

    Bytes wrapped1 = encrypt_dek(dek1, *kek->public_key());
    Bytes wrapped2 = encrypt_dek(dek2, *kek->public_key());

    EXPECT_EQ(wrapped1.size(), wrapped2.size());
    // ML-KEM-768 encap key = 1088, GCM nonce = 12, ciphertext = 32 + 16 tag
    EXPECT_GT(wrapped1.size(), 1088u + 12u + 32u);
}

TEST(CryptoTest, FullEncryptDecryptPipelineWithKdf) {
    // End-to-end: generate DEK, encrypt shards, wrap DEK, then unwrap and decrypt.
    // Verifies HKDF integration doesn't break the full pipeline.
    Botan::AutoSeeded_RNG rng;
    auto kek = Botan::create_private_key("ML-KEM", rng, "ML-KEM-768");

    std::array<uint8_t, 32> dek;
    rng.randomize(dek.data(), dek.size());

    auto plain_shards = MakePlainShards();

    // Encrypt shards with DEK
    std::vector<EncryptedShard> encrypted = encrypt_shards(plain_shards, dek);

    // Wrap DEK with KEK (uses HKDF internally)
    Bytes wrapped_dek = encrypt_dek(dek, *kek->public_key());

    // Unwrap DEK
    std::array<uint8_t, 32> recovered_dek = decrypt_dek(wrapped_dek, *kek);
    EXPECT_EQ(recovered_dek, dek);

    // Decrypt shards with recovered DEK
    std::vector<PlainShard> decrypted = decrypt_shards(encrypted, recovered_dek, false);

    ASSERT_EQ(decrypted.size(), plain_shards.size());
    for (size_t i = 0; i < plain_shards.size(); ++i) {
        EXPECT_EQ(decrypted[i].index, plain_shards[i].index);
        EXPECT_EQ(decrypted[i].bytes, plain_shards[i].bytes);
    }
}

// =========================================================
// Robustness tests: best-effort, tampering, realistic failures
// =========================================================

TEST(CryptoTest, BestEffortDropsCorruptedShards) {
    const auto key = MakeTestKey();
    const auto original_shards = MakePlainShards(); // 3 shards

    std::vector<EncryptedShard> encrypted = encrypt_shards(original_shards, key);
    ASSERT_EQ(encrypted.size(), 3u);

    // Tamper with shard 0's ciphertext
    encrypted[0].ciphertext[0] ^= 0xFF;

    // best_effort=true should silently drop the corrupted shard
    std::vector<PlainShard> decrypted = decrypt_shards(encrypted, key, true);

    ASSERT_EQ(decrypted.size(), 2u);
    EXPECT_EQ(decrypted[0].index, original_shards[1].index);
    EXPECT_EQ(decrypted[0].bytes, original_shards[1].bytes);
    EXPECT_EQ(decrypted[1].index, original_shards[2].index);
    EXPECT_EQ(decrypted[1].bytes, original_shards[2].bytes);
}

TEST(CryptoTest, BestEffortDropsMultipleCorruptedShards) {
    const auto key = MakeTestKey();
    const auto original_shards = MakePlainShards(); // 3 shards

    std::vector<EncryptedShard> encrypted = encrypt_shards(original_shards, key);
    ASSERT_EQ(encrypted.size(), 3u);

    // Tamper with shards 0 and 2
    encrypted[0].ciphertext[0] ^= 0xFF;
    encrypted[2].nonce[0] ^= 0xFF;

    std::vector<PlainShard> decrypted = decrypt_shards(encrypted, key, true);

    ASSERT_EQ(decrypted.size(), 1u);
    EXPECT_EQ(decrypted[0].index, original_shards[1].index);
    EXPECT_EQ(decrypted[0].bytes, original_shards[1].bytes);
}

TEST(CryptoTest, ShardSwapAttackRejectedByAAD) {
    const auto key = MakeTestKey();
    const auto original_shards = MakePlainShards();

    std::vector<EncryptedShard> encrypted = encrypt_shards(original_shards, key);
    ASSERT_GE(encrypted.size(), 2u);

    // Swap shard 1's ciphertext+nonce into shard 0's slot.
    // The AAD (index) mismatch must cause GCM to reject.
    encrypted[0].ciphertext = encrypted[1].ciphertext;
    encrypted[0].nonce = encrypted[1].nonce;

    // Strict mode: should throw on the swapped shard
    EXPECT_THROW(
        decrypt_shards(encrypted, key, false),
        std::runtime_error
    );

    // Best-effort: shard 0 dropped, shard 1 and 2 survive
    std::vector<PlainShard> decrypted = decrypt_shards(encrypted, key, true);
    ASSERT_EQ(decrypted.size(), 2u);
    EXPECT_EQ(decrypted[0].index, original_shards[1].index);
    EXPECT_EQ(decrypted[0].bytes, original_shards[1].bytes);
    EXPECT_EQ(decrypted[1].index, original_shards[2].index);
    EXPECT_EQ(decrypted[1].bytes, original_shards[2].bytes);
}

TEST(CryptoTest, TruncatedCiphertextHandledGracefully) {
    const auto key = MakeTestKey();
    const auto original_shards = MakePlainShards();

    std::vector<EncryptedShard> encrypted = encrypt_shards(original_shards, key);
    ASSERT_FALSE(encrypted.empty());
    ASSERT_GT(encrypted[0].ciphertext.size(), 2u);

    // Truncate ciphertext to simulate partial network receive
    encrypted[0].ciphertext.resize(encrypted[0].ciphertext.size() / 2);

    // Strict mode: should throw
    EXPECT_THROW(
        decrypt_shards(encrypted, key, false),
        std::runtime_error
    );

    // Best-effort: drops the truncated shard, returns the rest
    std::vector<PlainShard> decrypted = decrypt_shards(encrypted, key, true);
    ASSERT_EQ(decrypted.size(), 2u);
    EXPECT_EQ(decrypted[0].index, original_shards[1].index);
    EXPECT_EQ(decrypted[1].index, original_shards[2].index);
}

TEST(CryptoTest, DecryptSucceedsWithOutOfOrderShards) {
    const auto key = MakeTestKey();
    const auto original_shards = MakePlainShards();

    std::vector<EncryptedShard> encrypted = encrypt_shards(original_shards, key);
    ASSERT_EQ(encrypted.size(), 3u);

    // Reverse the order to simulate out-of-order arrival
    std::reverse(encrypted.begin(), encrypted.end());

    std::vector<PlainShard> decrypted = decrypt_shards(encrypted, key, false);

    ASSERT_EQ(decrypted.size(), original_shards.size());
    // Results come back in the reversed order, but data is correct per-index
    for (const auto& d : decrypted) {
        EXPECT_EQ(d.bytes, original_shards[d.index].bytes);
    }
}

// =========================
// KEK File Persistence
// =========================

TEST(CryptoTest, SaveAndLoadKekFileRoundTrip) {
    namespace fs = std::filesystem;
    const std::string path = "/tmp/myc_kek_roundtrip.json";

    // Cleanup
    fs::remove(path);

    Botan::AutoSeeded_RNG rng;
    std::unordered_map<std::string, std::unique_ptr<Botan::Private_Key>> keys;
    auto k1 = Botan::create_private_key("ML-KEM", rng, "ML-KEM-768");
    auto k2 = Botan::create_private_key("ML-KEM", rng, "ML-KEM-768");

    std::string pem1 = Botan::PKCS8::PEM_encode(*k1);
    std::string pem2 = Botan::PKCS8::PEM_encode(*k2);

    keys.emplace("kek-100", std::move(k1));
    keys.emplace("kek-200", std::move(k2));

    save_kek_file(path, keys, "kek-200");

    KekFileData loaded = load_kek_file(path);

    EXPECT_EQ(loaded.active_kek_id, "kek-200");
    EXPECT_EQ(loaded.keys.size(), 2u);
    EXPECT_TRUE(loaded.keys.count("kek-100"));
    EXPECT_TRUE(loaded.keys.count("kek-200"));

    // Verify PEM content matches
    EXPECT_EQ(Botan::PKCS8::PEM_encode(*loaded.keys.at("kek-100")), pem1);
    EXPECT_EQ(Botan::PKCS8::PEM_encode(*loaded.keys.at("kek-200")), pem2);

    fs::remove(path);
}

TEST(CryptoTest, SaveKekFileSetsPermissions0600) {
    namespace fs = std::filesystem;
    const std::string path = "/tmp/myc_kek_perms.json";

    fs::remove(path);

    Botan::AutoSeeded_RNG rng;
    std::unordered_map<std::string, std::unique_ptr<Botan::Private_Key>> keys;
    keys.emplace("kek-1", Botan::create_private_key("ML-KEM", rng, "ML-KEM-768"));

    save_kek_file(path, keys, "kek-1");

    auto perms = fs::status(path).permissions();
    EXPECT_EQ(perms, (fs::perms::owner_read | fs::perms::owner_write));

    fs::remove(path);
}

TEST(CryptoTest, LoadKekFileThrowsOnCorruptJson) {
    const std::string path = "/tmp/myc_kek_corrupt.json";

    std::ofstream f(path);
    f << "not valid json {{{{";
    f.close();

    EXPECT_THROW(load_kek_file(path), std::runtime_error);

    std::filesystem::remove(path);
}

TEST(CryptoTest, LoadKekFileThrowsOnMissingFile) {
    EXPECT_THROW(load_kek_file("/tmp/myc_nonexistent_kek.json"), std::runtime_error);
}

TEST(CryptoTest, LoadedKeyCanDecryptDekFromOriginal) {
    namespace fs = std::filesystem;
    const std::string path = "/tmp/myc_kek_decrypt.json";

    fs::remove(path);

    Botan::AutoSeeded_RNG rng;
    auto original_key = Botan::create_private_key("ML-KEM", rng, "ML-KEM-768");

    // Encrypt a DEK with the original key
    std::array<uint8_t, 32> dek;
    rng.randomize(dek.data(), dek.size());
    Bytes wrapped = encrypt_dek(dek, *original_key->public_key());

    // Save and reload
    std::unordered_map<std::string, std::unique_ptr<Botan::Private_Key>> keys;
    keys.emplace("kek-orig", std::move(original_key));
    save_kek_file(path, keys, "kek-orig");

    KekFileData loaded = load_kek_file(path);

    // Decrypt with loaded key
    auto recovered = decrypt_dek(wrapped, *loaded.keys.at("kek-orig"));
    EXPECT_EQ(recovered, dek);

    fs::remove(path);
}