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

#ifndef PARQUET_KEY_WITH_METADATA_H
#define PARQUET_KEY_WITH_METADATA_H

#include <string>

#include "parquet/util/visibility.h"

namespace parquet {

class PARQUET_EXPORT KeyWithMetadata {
 public:
  KeyWithMetadata(const std::string& key_bytes, const std::string& key_metadata) {
    this.key_bytes_ = key_bytes;
    this.key_metadata_ = key_metadata;
  }

  const std::string& key_bytes() { return key_bytes_; }

  const std::string& key_metadata() { return key_metadata_; }

 private:
  std::string key_bytes_;
  std::string key_metadata_;
};

}  // namespace parquet

#endif  // PARQUET_KEY_WITH_METADATA_H
