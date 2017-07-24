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

namespace plasma {

constexpr int64_t kUniqueIDSize = 20;

class ARROW_EXPORT UniqueID {
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

/// Size of object hash digests.
constexpr int64_t kDigestSize = sizeof(uint64_t);

/// Object request data structure. Used for Wait.
struct ObjectRequest {
  /// The ID of the requested object. If ID_NIL request any object.
  ObjectID object_id;
  /// Request associated to the object. It can take one of the following values:
  ///  - PLASMA_QUERY_LOCAL: return if or when the object is available in the
  ///    local Plasma Store.
  ///  - PLASMA_QUERY_ANYWHERE: return if or when the object is available in
  ///    the system (i.e., either in the local or a remote Plasma Store).
  int type;
  /// Object status. Same as the status returned by plasma_status() function
  /// call. This is filled in by plasma_wait_for_objects1():
  ///  - ObjectStatus_Local: object is ready at the local Plasma Store.
  ///  - ObjectStatus_Remote: object is ready at a remote Plasma Store.
  ///  - ObjectStatus_Nonexistent: object does not exist in the system.
  ///  - PLASMA_CLIENT_IN_TRANSFER, if the object is currently being scheduled
  ///    for being transferred or it is transferring.
  int status;
};

enum ObjectRequestType {
  /// Query for object in the local plasma store.
  PLASMA_QUERY_LOCAL = 1,
  /// Query for object in the local plasma store or in a remote plasma store.
  PLASMA_QUERY_ANYWHERE
};

extern int ObjectStatusLocal;
extern int ObjectStatusRemote;

}  // namespace plasma

#endif  // PLASMA_COMMON_H
