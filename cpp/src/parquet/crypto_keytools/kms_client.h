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

#ifndef PARQUET_KMS_CLIENT_H
#define PARQUET_KMS_CLIENT_H

#include <map>
#include <string>

#include "parquet/util/visibility.h"

namespace parquet {

/// Implementing class instance should not be created per each Parquet file.
/// Rather per tenant or session or dataset, to enable efficient key caching.
/// The methods need to be thread-safe.
class PARQUET_EXPORT KmsClient {
 public:
  /// Pass configuration with client-specific parameters
  virtual void setConfiguration(std::map<std::string, std::string> conf) = 0;

  /// Supports key wrapping (envelope encryption of data key by master key) inside
  /// KMS server.
  virtual bool supportsServerSideWrapping() = 0;

  /// Retrieves a key stored in KMS server.
  /// Implementation of this method is not required (can just return null) if KMS supports
  /// server side wrapping and application doesn't plan to use local (client-side)
  /// wrapping.
  /// IMPORTANT: if implemented, must throw KeyAccessDeniedException when unauthorized to
  /// get the key.
  /// If your KMS client code throws runtime exceptions related to access/permission
  /// problems (such as Hadoop AccessControlException), make sure to catch them
  /// and throw the KeyAccessDeniedException.
  ///
  /// Parameter key_identifier is a string that uniquely identifies the key in KMS:
  /// ranging from a simple key ID, to e.g. a JSON with key ID, KMS instance etc.
  /// Returns Base64 encoded data key.
  virtual std::string getKeyFromServer(std::string& key_identifier) = 0;

  /// Encrypts (wraps) data key in KMS server, using the master key.
  /// The result includes everything returned by KMS (often a JSON).
  /// Implementation of this method must throw an UnsupportedOperationException if KMS
  /// doesn't support server side wrapping.
  /// Implementation of this method is not required (can just return null) if applications
  /// plan to store data keys in KMS (no wrapping), or plan to wrap data keys locally.
  ///
  /// IMPORTANT: if implemented, must throw KeyAccessDeniedException when unauthorized to
  /// wrap with the given master key.
  /// If your KMS client code throws runtime exceptions related to access/permission
  /// problems (such as Hadoop AccessControlException), make sure to catch them and throw
  /// the KeyAccessDeniedException.
  ///
  /// Parameter data_key is Base64 encoded data key.
  /// Parameter master_key_identifier is a string that uniquely identifies the wrapper
  /// (master) key in KMS: ranging from a simple key ID, to e.g. a JSON with key ID, 
  /// KMS instance etc.
  virtual std::string wrapDataKeyInServer(const std::string& data_key,
                                          const std::string& master_key_identifier) = 0;

  /// Decrypts (unwraps) data key in KMS server, using the master key.
  /// Implementation of this method must throw an UnsupportedOperationException if KMS
  /// doesn't support server side wrapping.
  /// Implementation of this method is not required (can just return null) if applications
  /// plan to store data keys in KMS (no wrapping), or plan to wrap data keys locally.
  ///
  /// IMPORTANT: if implemented, must throw KeyAccessDeniedException when unauthorized to
  /// unwrap with the given master key.
  /// If your KMS client code throws runtime exceptions related to access/permission
  /// problems (such as Hadoop AccessControlException), make sure to catch them and throw
  /// the KeyAccessDeniedException.
  ///
  /// Parameter wrapped_data_key includes everything returned by KMS upon wrapping.
  /// Parameter master_key_identifier is a string that uniquely identifies the wrapper
  /// (master) key in KMS: ranging from a simple key ID, to e.g. a JSON with key ID,
  /// KMS instance etc.
  /// Returns Base64 encoded data key.
  virtual std::string unwrapDataKeyInServer(const std::string& wrapped_data_key,
                                            const std::string& master_key_identifier) = 0;
};

}  // namespace parquet

#endif  // PARQUET_KMS_CLIENT_H
