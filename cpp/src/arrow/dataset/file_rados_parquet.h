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

#include <functional>
#include <memory>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include <cephfs/ceph_ll_client.h>
#include <cephfs/libcephfs.h>
#include "arrow/api.h"
#include "arrow/dataset/dataset.h"
#include "arrow/dataset/discovery.h"
#include "arrow/dataset/file_base.h"
#include "arrow/dataset/rados.h"
#include "arrow/dataset/rados_utils.h"
#include "arrow/dataset/scanner.h"
#include "arrow/dataset/type_fwd.h"
#include "arrow/dataset/visibility.h"
#include "arrow/filesystem/api.h"
#include "arrow/filesystem/path_util.h"
#include "arrow/util/iterator.h"

namespace arrow {
namespace dataset {

class ARROW_DS_EXPORT RadosCluster {
 public:
  explicit RadosCluster(std::string conf_path)
      : pool_name("cephfs_data"),
        user_name("client.admin"),
        cluster_name("ceph"),
        ceph_config_path(conf_path),
        flags(0),
        cls_name("arrow"),
        rados(new RadosWrapper()),
        ioCtx(new IoCtxWrapper()) {}

  Status Connect() {
    if (rados->init2(user_name.c_str(), cluster_name.c_str(), flags))
      return Status::Invalid("librados::init2 returned non-zero exit code.");

    if (rados->conf_read_file(ceph_config_path.c_str()))
      return Status::Invalid("librados::conf_read_file returned non-zero exit code.");

    if (rados->connect())
      return Status::Invalid("librados::connect returned non-zero exit code.");

    if (rados->ioctx_create(pool_name.c_str(), ioCtx))
      return Status::Invalid("librados::ioctx_create returned non-zero exit code.");

    return Status::OK();
  }

  Status Disconnect() {
    rados->shutdown();
    return Status::OK();
  }

  std::string pool_name;
  std::string user_name;
  std::string cluster_name;
  std::string ceph_config_path;
  uint64_t flags;
  std::string cls_name;

  RadosInterface* rados;
  IoCtxInterface* ioCtx;
};

class ARROW_DS_EXPORT DirectObjectAccess {
 public:
  Status Init(std::shared_ptr<RadosCluster> cluster) {
    cluster_ = cluster;
    const char id[] = "client.admin";

    if (ceph_create(&cmount_, cluster->user_name.c_str()))
      return Status::Invalid("libcephfs::ceph_create returned non-zero exit code.");

    if (ceph_conf_read_file(cmount_, cluster->ceph_config_path.c_str()))
      return Status::Invalid(
          "libcephfs::ceph_conf_read_file returned non-zero exit code.");

    if (ceph_init(cmount_))
      return Status::Invalid("libcephfs::ceph_init returned non-zero exit code.");

    if (ceph_select_filesystem(cmount_, "cephfs"))
      return Status::Invalid(
          "libcephfs::ceph_select_filesystem returned non-zero exit code.");

    if (ceph_mount(cmount_, "/"))
      return Status::Invalid("libcephfs::ceph_mount returned non-zero exit code.");

    return Status::OK();
  }

  Status Exec(const std::string& path, const std::string& fn,
              std::shared_ptr<librados::bufferlist>& in,
              std::shared_ptr<librados::bufferlist>& out) {
    struct ceph_statx stx;
    if (ceph_statx(cmount_, path.c_str(), &stx, 0, 0))
      return Status::IOError("libcephfs::ceph_statx failed");

    uint64_t inode = stx.stx_ino;

    std::stringstream ss;
    ss << std::hex << inode;
    std::string oid(ss.str() + ".00000000");

    librados::bufferlist out_bl;
    if (cluster_->ioCtx->exec(oid.c_str(), cluster_->cls_name.c_str(), fn.c_str(), *in,
                              out_bl)) {
      return Status::ExecutionError("librados::exec returned non-zero exit code.");
    }

    out = std::make_shared<librados::bufferlist>(out_bl);
    return Status::OK();
  }

 protected:
  std::shared_ptr<RadosCluster> cluster_;
  struct ceph_mount_info* cmount_;
};

class ARROW_DS_EXPORT RadosParquetFileFormat : public FileFormat {
 public:
  static Result<std::shared_ptr<RadosParquetFileFormat>> Make(
      const std::string& path_to_config);

  explicit RadosParquetFileFormat(const std::string& path_to_config);

  explicit RadosParquetFileFormat(std::shared_ptr<DirectObjectAccess> doa)
      : doa_(std::move(doa)) {}

  std::string type_name() const override { return "rados-parquet"; }

  bool splittable() const { return true; }

  bool Equals(const FileFormat& other) const override {
    return type_name() == other.type_name();
  }

  Result<bool> IsSupported(const FileSource& source) const override { return true; }

  Result<std::shared_ptr<Schema>> Inspect(const FileSource& source) const override;

  Result<ScanTaskIterator> ScanFile(std::shared_ptr<ScanOptions> options,
                                    std::shared_ptr<ScanContext> context,
                                    FileFragment* file) const override;

  Result<std::shared_ptr<FileWriter>> MakeWriter(
      std::shared_ptr<io::OutputStream> destination, std::shared_ptr<Schema> schema,
      std::shared_ptr<FileWriteOptions> options) const override {
    return Status::NotImplemented("Use the Python API");
  }

  std::shared_ptr<FileWriteOptions> DefaultWriteOptions() override { return NULLPTR; }

 protected:
  std::shared_ptr<DirectObjectAccess> doa_;
};

}  // namespace dataset
}  // namespace arrow
