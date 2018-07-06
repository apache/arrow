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

#include "plasma/common.h"

#include <limits>
#include <mutex>
#include <random>

#include "plasma/plasma_generated.h"

namespace plasma {

using arrow::Status;

UniqueID UniqueID::from_random() {
  UniqueID id;
  uint8_t* data = id.mutable_data();
  // NOTE(pcm): The right way to do this is to have one std::mt19937 per
  // thread (using the thread_local keyword), but that's not supported on
  // older versions of macOS (see https://stackoverflow.com/a/29929949)
  static std::mutex mutex;
  std::lock_guard<std::mutex> lock(mutex);
  static std::mt19937 generator;
  std::uniform_int_distribution<uint32_t> dist(0, std::numeric_limits<uint8_t>::max());
  for (int i = 0; i < kUniqueIDSize; i++) {
    data[i] = static_cast<uint8_t>(dist(generator));
  }
  return id;
}

UniqueID UniqueID::from_binary(const std::string& binary) {
  UniqueID id;
  std::memcpy(&id, binary.data(), sizeof(id));
  return id;
}

const uint8_t* UniqueID::data() const { return id_; }

uint8_t* UniqueID::mutable_data() { return id_; }

std::string UniqueID::binary() const {
  return std::string(reinterpret_cast<const char*>(id_), kUniqueIDSize);
}

std::string UniqueID::hex() const {
  constexpr char hex[] = "0123456789abcdef";
  std::string result;
  for (int i = 0; i < kUniqueIDSize; i++) {
    unsigned int val = id_[i];
    result.push_back(hex[val >> 4]);
    result.push_back(hex[val & 0xf]);
  }
  return result;
}

// This code is from https://sites.google.com/site/murmurhash/
// and is public domain.
uint64_t MurmurHash64A(const void* key, int len, unsigned int seed) {
  const uint64_t m = 0xc6a4a7935bd1e995;
  const int r = 47;

  uint64_t h = seed ^ (len * m);

  const uint64_t* data = reinterpret_cast<const uint64_t*>(key);
  const uint64_t* end = data + (len / 8);

  while (data != end) {
    uint64_t k = *data++;

    k *= m;
    k ^= k >> r;
    k *= m;

    h ^= k;
    h *= m;
  }

  const unsigned char* data2 = reinterpret_cast<const unsigned char*>(data);

  switch (len & 7) {
    case 7:
      h ^= uint64_t(data2[6]) << 48;
    case 6:
      h ^= uint64_t(data2[5]) << 40;
    case 5:
      h ^= uint64_t(data2[4]) << 32;
    case 4:
      h ^= uint64_t(data2[3]) << 24;
    case 3:
      h ^= uint64_t(data2[2]) << 16;
    case 2:
      h ^= uint64_t(data2[1]) << 8;
    case 1:
      h ^= uint64_t(data2[0]);
      h *= m;
  }

  h ^= h >> r;
  h *= m;
  h ^= h >> r;

  return h;
}

size_t UniqueID::hash() const { return MurmurHash64A(&id_[0], kUniqueIDSize, 0); }

bool UniqueID::operator==(const UniqueID& rhs) const {
  return std::memcmp(data(), rhs.data(), kUniqueIDSize) == 0;
}

Status plasma_error_status(PlasmaError plasma_error) {
  switch (plasma_error) {
    case PlasmaError::OK:
      return Status::OK();
    case PlasmaError::ObjectExists:
      return Status::PlasmaObjectExists("object already exists in the plasma store");
    case PlasmaError::ObjectNonexistent:
      return Status::PlasmaObjectNonexistent("object does not exist in the plasma store");
    case PlasmaError::OutOfMemory:
      return Status::PlasmaStoreFull("object does not fit in the plasma store");
    default:
      ARROW_LOG(FATAL) << "unknown plasma error code " << static_cast<int>(plasma_error);
  }
  return Status::OK();
}

ARROW_EXPORT ObjectStatus ObjectStatusLocal = ObjectStatus::Local;
ARROW_EXPORT ObjectStatus ObjectStatusRemote = ObjectStatus::Remote;

const PlasmaStoreInfo* plasma_config;

}  // namespace plasma
