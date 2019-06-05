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

#ifndef PARQUET_FILE_KEY_MANAGER_H
#define PARQUET_FILE_KEY_MANAGER_H

#include <map>
#include <string>

#include "parquet/crypto_keytools/key_with_metadata.h"
#include "parquet/encryption.h"
#include "parquet/util/visibility.h"

namespace parquet {

/// Management of file encryption keys, and their metadata.
/// For scalability, implementing code is recommended to run locally / not to make remote
/// calls - except for calls to KMS server, via the pre-defined KmsClient interface.
///
/// Implementing class instance should be created per each Parquet file. The methods don't
/// need to be thread-safe.
class PARQUET_EXPORT FileKeyManager {
 public:
  virtual void initialize(std::map<std::string, std::string> configuration,
                          KmsClient kms_client) = 0;

  /// Generates or fetched a column data encryption key, and creates its metadata.
  /// Eg can generate a random data key, and wrap it with a master key. column_key_id is
  /// the master key then.
  /// Or can fetch the data key from KMS. column_key_id is the data key in this case.
  virtual void KeyWithMetadata getColumnEncryptionKey(parquet::schema::ColumnPath column,
                                                      std::string column_key_id) = 0;

  /// Footer key metadata can store additional information (such as KMS instance identity)
  /// - so it doesnt have to be stored in each column key metadata.
  virtual KeyWithMetadata getFooterEncryptionKey(std::string footer_key_id) = 0;

  virtual DecryptionKeyRetriever getDecryptionKeyRetriever() = 0;

  /// Wipes keys in memory.
  /// Flushes key metadata to wrapped key store (if in use).
  virtual void close() = 0;
};

}  // namespace parquet

#endif  // PARQUET_FILE_KEY_MANAGER_H
