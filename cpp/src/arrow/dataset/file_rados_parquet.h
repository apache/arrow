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

// This API is EXPERIMENTAL.
#define _FILE_OFFSET_BITS 64

#pragma once

#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <functional>
#include <memory>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "arrow/compute/exec/expression.h"
#include "arrow/dataset/dataset.h"
#include "arrow/dataset/discovery.h"
#include "arrow/dataset/file_base.h"
#include "arrow/dataset/file_parquet.h"
#include "arrow/dataset/rados.h"
#include "arrow/dataset/scanner.h"
#include "arrow/dataset/type_fwd.h"
#include "arrow/dataset/visibility.h"
#include "arrow/filesystem/api.h"
#include "arrow/filesystem/path_util.h"
#include "arrow/io/api.h"
#include "arrow/ipc/api.h"
#include "arrow/util/iterator.h"
#include "arrow/util/macros.h"

#include "parquet/arrow/writer.h"
#include "parquet/exception.h"

namespace arrow {
namespace dataset {

class ARROW_DS_EXPORT RadosCluster {
 public:
  struct RadosConnectionCtx {
    std::string ceph_config_path;
    std::string data_pool;
    std::string user_name;
    std::string cluster_name;
    std::string cls_name;
  };
  explicit RadosCluster(RadosConnectionCtx& ctx)
      : ctx(ctx), rados(new RadosWrapper()), ioCtx(new IoCtxWrapper()) {}

  ~RadosCluster() { Shutdown(); }

  Status Connect() {
    if (rados->init2(ctx.user_name.c_str(), ctx.cluster_name.c_str(), 0))
      return Status::Invalid("librados::init2 returned non-zero exit code.");

    if (rados->conf_read_file(ctx.ceph_config_path.c_str()))
      return Status::Invalid("librados::conf_read_file returned non-zero exit code.");

    if (rados->connect())
      return Status::Invalid("librados::connect returned non-zero exit code.");

    if (rados->ioctx_create(ctx.data_pool.c_str(), ioCtx))
      return Status::Invalid("librados::ioctx_create returned non-zero exit code.");

    return Status::OK();
  }

  Status Shutdown() {
    rados->shutdown();
    return Status::OK();
  }

  RadosConnectionCtx ctx;
  RadosInterface* rados;
  IoCtxInterface* ioCtx;
};

class ARROW_DS_EXPORT DirectObjectAccess {
 public:
  explicit DirectObjectAccess(const std::shared_ptr<RadosCluster>& cluster)
      : cluster_(std::move(cluster)) {}

  Status Stat(const std::string& path, struct stat& st) {
    struct stat file_st;
    if (stat(path.c_str(), &file_st) < 0)
      return Status::ExecutionError("stat returned non-zero exit code.");
    st = file_st;
    return Status::OK();
  }

  std::string ConvertFileInodeToObjectID(uint64_t inode) {
    std::stringstream ss;
    ss << std::hex << inode;
    std::string oid(ss.str() + ".00000000");
    return oid;
  }

  Status Exec(uint64_t inode, const std::string& fn, ceph::bufferlist& in,
              ceph::bufferlist& out) {
    std::string oid = ConvertFileInodeToObjectID(inode);
    if (cluster_->ioCtx->exec(oid.c_str(), cluster_->ctx.cls_name.c_str(), fn.c_str(), in,
                              out)) {
      return Status::ExecutionError("librados::exec returned non-zero exit code.");
    }
    return Status::OK();
  }

 protected:
  std::shared_ptr<RadosCluster> cluster_;
};

class ARROW_DS_EXPORT RadosParquetFileFormat : public ParquetFileFormat {
 public:
  explicit RadosParquetFileFormat(const std::string&, const std::string&,
                                  const std::string&, const std::string&);

  explicit RadosParquetFileFormat(std::shared_ptr<DirectObjectAccess> doa)
      : doa_(std::move(doa)) {}

  std::string type_name() const override { return "rados-parquet"; }

  bool splittable() const { return true; }

  bool Equals(const FileFormat& other) const override {
    return type_name() == other.type_name();
  }

  Result<bool> IsSupported(const FileSource& source) const override { return true; }

  Result<std::shared_ptr<Schema>> Inspect(const FileSource& source) const override;

  Result<ScanTaskIterator> ScanFile(
      const std::shared_ptr<ScanOptions>& options,
      const std::shared_ptr<FileFragment>& file) const override;

  Result<std::shared_ptr<FileWriter>> MakeWriter(
      std::shared_ptr<io::OutputStream> destination, std::shared_ptr<Schema> schema,
      std::shared_ptr<FileWriteOptions> options) const override {
    return Status::NotImplemented("Use the Python API");
  }

  std::shared_ptr<FileWriteOptions> DefaultWriteOptions() override { return NULLPTR; }

 protected:
  std::shared_ptr<DirectObjectAccess> doa_;
};

ARROW_DS_EXPORT Status SerializeScanRequest(std::shared_ptr<ScanOptions>& options,
                                            int64_t& file_size, ceph::bufferlist& bl);

ARROW_DS_EXPORT Status DeserializeScanRequest(compute::Expression* filter,
                                              compute::Expression* partition,
                                              std::shared_ptr<Schema>* projected_schema,
                                              std::shared_ptr<Schema>* dataset_schema,
                                              int64_t& file_size, ceph::bufferlist& bl);

ARROW_DS_EXPORT Status SerializeTable(std::shared_ptr<Table>& table,
                                      ceph::bufferlist& bl);

ARROW_DS_EXPORT Status DeserializeTable(RecordBatchVector& batches, ceph::bufferlist& bl);

}  // namespace dataset
}  // namespace arrow
