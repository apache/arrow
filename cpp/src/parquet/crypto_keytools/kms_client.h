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

#include <string>

#include "parquet/util/visibility.h"

namespace parquet {

class PARQUET_EXPORT KmsClient {
 public:
  
  virtual bool supportsServerSideWrapping() = 0;

  // Encrypts (wraps) data key in KMS server, using the master key.
  // The result includes everything returned by KMS (often a JSON).
  // Implementation of this method must throw an UnsupportedOperationException
  // if KMS doesn't support server side wrapping.
  // Implementation of this method is not required (can just return null) if
  // applications plan to store data keys in KMS (no wrapping),
  // or plan to wrap data keys locally.
  virtual std::string wrapDataKeyInServer(const std::string &data_key,
					  const std::string &master_key_identifier) = 0;

  // Retrieves a key stored in KMS server.
  // Implementation of this method is not required (can just return null) if KMS
  // supports server side wrapping
  // and application doesn't plan to use local (client-side) wrapping.
  virtual std::string getKeyFromServer(std::string &key_identifier) = 0;

  // Decrypts (unwraps) data key in KMS server, using the master key.
  // Implementation of this method must throw an UnsupportedOperationException
  // if KMS doesn't support server side wrapping.
  // Implementation of this method is not required (can just return null)
  // if applications plan to store data keys in KMS (no wrapping),
  // or plan to wrap data keys locally. 
  virtual std::string unwrapDataKeyInServer(const std::string &wrapped_daya_key,
					    const std::string &master_key_identifier) = 0;
};
 
} // namespace parquet

#endif  // PARQUET_KMS_CLIENT_H
