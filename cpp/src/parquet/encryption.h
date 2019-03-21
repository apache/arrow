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

#ifndef PARQUET_ENCRYPTION_H
#define PARQUET_ENCRYPTION_H

#include <map>
#include <string>

#include <parquet/util/visibility.h>
#include "parquet/exception.h"

namespace parquet {

class PARQUET_EXPORT DecryptionKeyRetriever {
 public:
  virtual const std::string& GetKey(const std::string& key_metadata) = 0;
  virtual ~DecryptionKeyRetriever() {}
};

// Simple integer key retriever
class PARQUET_EXPORT IntegerKeyIdRetriever : public DecryptionKeyRetriever {
 public:
  void PutKey(uint32_t key_id, const std::string& key);
  const std::string& GetKey(const std::string& key_metadata);

 private:
  std::map<uint32_t, std::string> key_map_;
};

// Simple string key retriever
class PARQUET_EXPORT StringKeyIdRetriever : public DecryptionKeyRetriever {
 public:
  void PutKey(const std::string& key_id, const std::string& key);
  const std::string& GetKey(const std::string& key_metadata);

 private:
  std::map<std::string, std::string> key_map_;
};

class PARQUET_EXPORT HiddenColumnException : public ParquetException {
 public:
  HiddenColumnException(const std::string& columnPath)
    : ParquetException(columnPath.c_str()) {}
};

}  // namespace parquet

#endif  // PARQUET_ENCRYPTION_H
