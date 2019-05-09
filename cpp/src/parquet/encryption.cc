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

#include "encryption.h"

#include <string.h>

namespace parquet {

// integer key retriever
void IntegerKeyIdRetriever::PutKey(uint32_t key_id, const std::string& key) {
  key_map_.insert(std::make_pair(key_id, key));
}

const std::string& IntegerKeyIdRetriever::GetKey(const std::string& key_metadata) {
  uint32_t key_id;
  memcpy(reinterpret_cast<uint8_t*>(&key_id), key_metadata.c_str(), 4);

  return key_map_[key_id];
}

// string key retriever
void StringKeyIdRetriever::PutKey(const std::string& key_id, const std::string& key) {
  key_map_.insert(std::make_pair(key_id, key));
}

const std::string& StringKeyIdRetriever::GetKey(const std::string& key_id) {
  return key_map_[key_id];
}

}  // namespace parquet
