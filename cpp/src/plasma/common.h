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

#ifndef PLASMA_COMMON_H
#define PLASMA_COMMON_H

#include <cstring>
#include <string>
// TODO(pcm): Convert getopt and sscanf in the store to use more idiomatic C++
// and get rid of the next three lines:
#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include "arrow/status.h"
#include "arrow/util/logging.h"

constexpr int64_t kUniqueIDSize = 20;

class UniqueID {
 public:
  static UniqueID from_random();
  static UniqueID from_binary(const std::string& binary);
  bool operator==(const UniqueID& rhs) const;
  const uint8_t* data() const;
  uint8_t* mutable_data();
  std::string binary() const;
  std::string hex() const;

 private:
  uint8_t id_[kUniqueIDSize];
};

static_assert(std::is_pod<UniqueID>::value, "UniqueID must be plain old data");

struct UniqueIDHasher {
  // ObjectID hashing function.
  size_t operator()(const UniqueID& id) const {
    size_t result;
    std::memcpy(&result, id.data(), sizeof(size_t));
    return result;
  }
};

typedef UniqueID ObjectID;

arrow::Status plasma_error_status(int plasma_error);

#endif  // PLASMA_COMMON_H
