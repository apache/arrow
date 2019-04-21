// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "parquet/crypto_keytools/wrapped_key_manager.h"

#include <chrono>
#include <openssl/rand.h>
#include <boost/beast/core/detail/base64.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string.hpp>

#include "arrow/util/utf8.h"
#include "arrow/memory_pool.cc"
#include "parquet/types.h"
#include "parquet/util/memory.h"
#include "parquet/util/crypto.h"
#include "parquet/encryption.h"

#include <iostream>

#include <vector>
#include <random>

namespace parquet {

constexpr int RND_MAX_BYTES = 16;
constexpr int DATA_KEY_SIZE = 16;

WrappedKeyManager::WrappedKeyManager(std::shared_ptr<KmsClient> kms_client):
  WrappedKeyManager (kms_client, false, nullptr, "") {}

WrappedKeyManager::WrappedKeyManager(
    std::shared_ptr<KmsClient> kms_client,
    bool wrapLocally,
    std::shared_ptr<WrappedKeyStore> wrappedKeyStore,
    std::string fileID) : kms_client_(kms_client),
                          wrap_locally_(wrapLocally),
                          wrapped_key_store_(wrappedKeyStore),
                          file_id_(fileID) {
  if (nullptr != wrappedKeyStore) {
    ParquetException::NYI("WrappedKeyStore option is not implemented yet");
  }
  if (!wrapLocally && !kms_client->supportsServerSideWrapping()) {
    throw UnsupportedOperationException("KMS client doesn't support "
                                        "server-side wrapping");
  }
}

WrappedKeyManager::WrappedKeyRetriever::WrappedKeyRetriever(
    std::shared_ptr<KmsClient> kms_client,
    bool unwrap_locally,
    std::shared_ptr<WrappedKeyStore> key_store,
    std::string file_id):
  kms_client_(kms_client), unwrap_locally_(unwrap_locally),
  key_store_(key_store), file_id_(file_id) {
  if (nullptr != key_store_) {
    ParquetException::NYI("WrappedKeyStore option is not implemented yet");
  }
}

const std::string& WrappedKeyManager::WrappedKeyRetriever::GetKey(
    const std::string& key_metadata) {
  std::vector<std::string> parts;
  boost::algorithm::split(parts, key_metadata,
                          boost::algorithm::is_any_of(":"),
                          boost::algorithm::token_compress_on);

  //key_metadata is expected to be in UTF8 encoding
  arrow::util::InitializeUTF8();
  if (!arrow::util::ValidateUTF8(reinterpret_cast<const uint8_t*>(key_metadata.c_str()),
                                 key_metadata.size()))
    throw ParquetException("master_key_id should be in UTF8 encoding");

  if (parts.size() != 2) throw ParquetException("Wrong key material structure");
  std::string encoded_wrapped_data_key = parts[0];
  std::string master_key_id = parts[1];

  if (unwrap_locally_) {
    std::string wrapped_data_key =
      boost::beast::detail::base64_decode(encoded_wrapped_data_key);
    std::string encoded_master_key = "";

    try {
      encoded_master_key = kms_client_->getKeyFromServer(master_key_id);
    } catch (UnsupportedOperationException &e) {
      std::stringstream ss;
      ss << "KMS client doesnt support key fetching  " << e.what() << "\n";
      throw ParquetException(ss.str());
    }
    if (encoded_master_key.empty()) {
      std::string error_msg = "Failed to get from KMS "
        "the master key" + master_key_id;
      throw ParquetException(error_msg);
    }
    std::string master_key =
      boost::beast::detail::base64_decode(encoded_master_key);
    std::string AAD = master_key_id;
    std::shared_ptr<EncryptionProperties> encryption =
      std::make_shared<EncryptionProperties>(
          ParquetCipher::AES_GCM_V1, "", "", "");
    auto pool = ::arrow::default_memory_pool();
    int64_t data_key_size =
      (int64_t)encryption->CalculatePlainSize(
          (uint32_t)wrapped_data_key.size());
    std::shared_ptr<ResizableBuffer> data_key_buffer;
    PARQUET_THROW_NOT_OK(arrow::AllocateResizableBuffer(
        pool, data_key_size, &data_key_buffer));

    char* wrapped_data_key_bytes = (char*)(wrapped_data_key.c_str());
    char* master_key_bytes = (char*)(master_key.c_str());
    char* aad_bytes = (char*)(AAD.c_str());

    parquet_encryption::Decrypt(
        ParquetCipher::AES_GCM_V1, false,
        reinterpret_cast<uint8_t*>(wrapped_data_key_bytes),
        (int)wrapped_data_key.size(),
        reinterpret_cast<uint8_t*>(master_key_bytes),
        (int)master_key.size(), reinterpret_cast<uint8_t*>(aad_bytes),
        (int)AAD.size(), data_key_buffer->mutable_data());
    data_key_.assign(reinterpret_cast<char const*>
                     (data_key_buffer->mutable_data()), DATA_KEY_SIZE);
  } else {
    std::string encoded_data_key;
    try {
      encoded_data_key = kms_client_->unwrapDataKeyInServer(
          encoded_wrapped_data_key,
          master_key_id);
    } catch (UnsupportedOperationException &e) {
      std::stringstream ss;
      ss << "KMS client doesnt support key fetching " << e.what() << "\n";
      throw ParquetException(ss.str());
    }
    if (encoded_data_key.empty()) {
      std::string error_msg =
        "Failed to get from KMS the master key" + master_key_id;
      throw ParquetException(error_msg);
    }

    data_key_ = boost::beast::detail::base64_decode(encoded_data_key);
  }

  return data_key_;
}

std::shared_ptr<DecryptionKeyRetriever> WrappedKeyManager::getKeyRetriever() {
  return std::shared_ptr<DecryptionKeyRetriever>(new WrappedKeyRetriever(
      kms_client_,
      wrap_locally_,
      wrapped_key_store_,
      file_id_));
}


// Generates random data encryption key, and creates its metadata.
// The metadata is comprised of the wrapped data key (encrypted
// with master key), and the identity of the master key.
std::unique_ptr<ParquetKey> WrappedKeyManager::generateKey(
    std::string &master_key_id) {
  uint8_t data_key[DATA_KEY_SIZE];
  std::string encoded_wrapped_data_key;
  memset(data_key, 0, DATA_KEY_SIZE);

  // Random data_key
  RAND_bytes(data_key, sizeof(data_key));

  if (wrap_locally_) {
    std::string encoded_master_key;

    try {
      encoded_master_key = kms_client_->getKeyFromServer(master_key_id);
    } catch (KeyAccessDeniedException &e) {
      std::stringstream ss;
      ss << "Unauthorized to fetch key:" + master_key_id + " "
         << e.what() << "\n";
      throw ParquetException(ss.str());
    } catch (UnsupportedOperationException &e) {
      std::stringstream ss;
      ss << "KMS client doesnt support key fetching: " << e.what() << "\n";
      throw ParquetException(ss.str());
    }
    if (encoded_master_key.empty()) {
      throw ParquetException("Failed to get encoded_master_key from KMS");
    }

    std::string master_key = boost::beast::detail::base64_decode(
        encoded_master_key);
    std::shared_ptr<EncryptionProperties> encryption =
      std::make_shared<EncryptionProperties>(ParquetCipher::AES_GCM_V1, "",
                                             "", "");
    auto pool = ::arrow::default_memory_pool();
    int64_t wrapped_data_key_buffer_size =
      (int64_t)encryption->CalculateCipherSize(DATA_KEY_SIZE);

    std::string AAD = master_key_id;
    char* aad_bytes = (char*)(AAD.c_str());
    char* master_key_bytes = (char*)(master_key.c_str());
    std::shared_ptr<Buffer> data;
    std::shared_ptr<ResizableBuffer> wrapped_data_key_buffer;
    PARQUET_THROW_NOT_OK(arrow::AllocateResizableBuffer(
        pool,
        wrapped_data_key_buffer_size,
        &wrapped_data_key_buffer));
    wrapped_data_key_buffer_size = parquet_encryption::Encrypt(
        ParquetCipher::AES_GCM_V1, false,
        reinterpret_cast<uint8_t*>(data_key),
        DATA_KEY_SIZE,
        reinterpret_cast<uint8_t*>(master_key_bytes),
        (int)master_key.size(),
        reinterpret_cast<uint8_t*>(aad_bytes),
        (int)AAD.size(),
        wrapped_data_key_buffer->mutable_data());
    std::string wrapped_data_key(
        reinterpret_cast<char const*>(wrapped_data_key_buffer->mutable_data()),
        wrapped_data_key_buffer_size) ;

    encoded_wrapped_data_key =
      boost::beast::detail::base64_encode(wrapped_data_key);
  } else {
    std::string encoded_data_key =
      boost::beast::detail::base64_encode(data_key, DATA_KEY_SIZE);
    try {
      encoded_wrapped_data_key =
        kms_client_->wrapDataKeyInServer(encoded_data_key, master_key_id);
    } catch (KeyAccessDeniedException &e) {
      std::string error_msg =
        "Unauthorized to fetch key: " + master_key_id + e.what();
      throw ParquetException(error_msg);
    } catch (UnsupportedOperationException &e) {
      std::stringstream ss;
      ss << "KMS client doesnt support key fetching  " << e.what() << "\n";
      throw ParquetException(ss.str());
    }
    if (encoded_wrapped_data_key.empty()) {
      throw ParquetException("Failed to get encoded data key from server");
    }
  }

  //wrapped_key_material is expected to be in UTF8 encoding
  std::string wrapped_key_material =
    encoded_wrapped_data_key + ":" + master_key_id;
  arrow::util::InitializeUTF8();
  if (!arrow::util::ValidateUTF8(reinterpret_cast<const uint8_t*>(wrapped_key_material.c_str()),
                                 wrapped_key_material.size()))
    throw ParquetException("wrapped_key_material should be in UTF8 encoding");

  std::string data_key_str(reinterpret_cast<char const*>(data_key),
                           DATA_KEY_SIZE) ;
  std::unique_ptr<ParquetKey> key(new ParquetKey(data_key_str,
                                                 wrapped_key_material));
  return key;
}

} // namespace parquet
