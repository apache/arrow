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
#pragma once

#include "skyhook/protocol/rados_protocol.h"

#include <sys/stat.h>
#include <sstream>

#include "arrow/compute/expression.h"
#include "arrow/record_batch.h"
#include "arrow/table.h"
#include "arrow/type.h"

#define SCAN_UNKNOWN_ERR_MSG "something went wrong while scanning file fragment"
#define SCAN_ERR_CODE 25
#define SCAN_ERR_MSG "failed to scan file fragment"
#define SCAN_REQ_DESER_ERR_CODE 26
#define SCAN_REQ_DESER_ERR_MSG "failed to deserialize scan request"
#define SCAN_RES_SER_ERR_CODE 27
#define SCAN_RES_SER_ERR_MSG "failed to serialize result table"

namespace skyhook {

/// An enum to represent the different
/// types of file formats that Skyhook supports.
struct SkyhookFileType {
  enum type { PARQUET, IPC };
};

/// A struct encapsulating all the parameters
/// required to be serialized in the form of flatbuffers for
/// sending to the cls.
struct ScanRequest {
  arrow::compute::Expression filter_expression;
  arrow::compute::Expression partition_expression;
  std::shared_ptr<arrow::Schema> projection_schema;
  std::shared_ptr<arrow::Schema> dataset_schema;
  int64_t file_size;
  SkyhookFileType::type file_format;
};

/// Utility functions to serialize and deserialize scan requests and result Arrow tables.
arrow::Status SerializeScanRequest(ScanRequest& req, ceph::bufferlist* bl);
arrow::Status DeserializeScanRequest(ceph::bufferlist& bl, ScanRequest* req);
arrow::Status SerializeTable(const std::shared_ptr<arrow::Table>& table,
                             ceph::bufferlist* bl);
arrow::Status DeserializeTable(ceph::bufferlist& bl, bool use_threads,
                               arrow::RecordBatchVector* batches);

/// Utility function to invoke a RADOS object class function on an RADOS object.
arrow::Status ExecuteObjectClassFn(const std::shared_ptr<rados::RadosConn>& connection,
                                   const std::string& oid, const std::string& fn,
                                   ceph::bufferlist& in, ceph::bufferlist& out);

/// An interface for translating the name of a file in CephFS to its
/// corresponding object ID in RADOS assuming 1:1 mapping between a file
/// and it's underlying object.
class SkyhookDirectObjectAccess {
 public:
  explicit SkyhookDirectObjectAccess(std::shared_ptr<rados::RadosConn> connection)
      : connection_(std::move(connection)) {}

  ~SkyhookDirectObjectAccess() = default;

  /// Execute a POSIX stat on a file.
  arrow::Status Stat(const std::string& path, struct stat& st) {
    struct stat file_st;
    if (stat(path.c_str(), &file_st) < 0)
      return arrow::Status::Invalid("stat returned non-zero exit code.");
    st = file_st;
    return arrow::Status::OK();
  }

  /// Convert a file inode to RADOS object ID.
  std::string ConvertInodeToOID(uint64_t inode) {
    std::stringstream ss;
    /// In Ceph, the underlying stripes that make up a file are
    /// named in the format [hex(inode)].[8-bit-binary(stripe_index)].
    ss << std::hex << inode;

    /// Since in Skyhook, we ensure a single stripe per file,
    /// we can assume the stripe index to be always 0 and hence
    /// hardcode it's 8-bit binary form.
    std::string oid(ss.str() + ".00000000");
    return oid;
  }

  /// Execute an object class method. It uses the `librados::exec` api to
  /// perform object clsass method calls on the storage node and
  /// stores the result in an output bufferlist.
  arrow::Status Exec(uint64_t inode, const std::string& fn, ceph::bufferlist& in,
                     ceph::bufferlist& out) {
    std::string oid = ConvertInodeToOID(inode);
    return ExecuteObjectClassFn(connection_, oid, fn, in, out);
  }

 private:
  std::shared_ptr<rados::RadosConn> connection_;
};

}  // namespace skyhook
