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
#define _FILE_OFFSET_BITS 64
#pragma once

#include <dirent.h>
#include <fcntl.h>
#include <sys/types.h>
#include <unistd.h>
#include <functional>
#include <memory>
#include <sstream>
#include <string>
#include <utility>
#include <vector>
#include "arrow/dataset/dataset.h"
#include "arrow/dataset/discovery.h"
#include "arrow/dataset/rados.h"
#include "arrow/dataset/rados_utils.h"
#include "arrow/dataset/scanner.h"
#include "arrow/filesystem/localfs.h"
#include "arrow/filesystem/path_util.h"
#include "arrow/filesystem/util_internal.h"
#include "arrow/util/iterator.h"
#include "arrow/util/logging.h"
#include <cephfs/ceph_ll_client.h>
#include <cephfs/libcephfs.h>

namespace arrow {
namespace dataset {

class ARROW_DS_EXPORT RadosDatasetFactoryOptions : public FileSystemFactoryOptions {
 public:
  std::string pool_name;
  std::string user_name;
  std::string cluster_name;
  std::string ceph_config_path;
  uint64_t flags;
  std::string cls_name;
};

class ARROW_DS_EXPORT RadosCluster {
 public:
  RadosCluster(std::string pool, std::string conf_path)
      : pool_name(pool),
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

class ARROW_DS_EXPORT RadosFileSystem : public fs::LocalFileSystem {
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

  std::string type_name() { return "rados"; }

  int64_t Write(const std::string& path, std::shared_ptr<Buffer> buffer) {
    std::string dirname = arrow::fs::internal::GetAbstractPathParent(path).first;
    if (!CreateDir(dirname).ok()) return -1;

    int fd = ceph_open(cmount_, path.c_str(), O_WRONLY | O_CREAT, 0777);
    if (fd < 0) return fd;

    int num_bytes_written =
        ceph_write(cmount_, fd, (char*)buffer->data(), buffer->size(), 0);

    if (int e = ceph_close(cmount_, fd)) return e;

    return num_bytes_written;
  }

  Status CreateDir(const std::string& path, bool recursive = true) {
    if (recursive) {
      if (ceph_mkdirs(cmount_, path.c_str(), 0666))
        return Status::IOError("libcephfs::ceph_mkdirs returned non-zero exit code.");
    } else {
      if (ceph_mkdir(cmount_, path.c_str(), 0666))
        return Status::IOError("libcephfs::ceph_mkdir returned non-zero exit code.");
    }
    return Status::OK();
  }

  Status DeleteDir(const std::string& path) {
    if (ceph_rmdir(cmount_, path.c_str()))
      return Status::IOError("libcephfs::ceph_rmdir returned non-zero exit code.");
    return Status::OK();
  }

  Status DeleteFile(const std::string& path) {
    if (ceph_unlink(cmount_, path.c_str()))
      return Status::IOError("libcephfs::ceph_unlink returned non-zero exit code.");
    return Status::OK();
  }

  Status DeleteFiles(const std::vector<std::string>& paths) {
    Status s;
    for (auto& path : paths) {
      if (!DeleteFile(path).ok())
        return Status::IOError(
            "RadosFileSystem::DeleteFiles returned non-zero exit code.");
    }
    return Status::OK();
  }

  Status Read(const std::string &path, std::shared_ptr<Buffer> &buffer, int64_t position, int64_t nbytes) {
    librados::bufferlist bl;
    int fd = ceph_open(cmount_, path.c_str(), O_RDWR, 0777);
    if (fd < 0)
      return Status::IOError("libcephfs::ceph_open returned negative file descriptor.");
    
    char *buff = new char[nbytes];
    int num_bytes_read = ceph_read(cmount_, fd, buff, nbytes, position);
    if (num_bytes_read < 0) {
      return Status::IOError("libcephfs::ceph_read returned negative number of bytes.");
    }

    buffer = std::make_shared<Buffer>((uint8_t*)buff, nbytes);

    if (ceph_close(cmount_, fd)) 
      return Status::IOError("libcephfs::ceph_close returned non-zero exit code.");

    return Status::OK();
  }

  Status GetSize(const std::string &path, uint64_t &size) {
    struct ceph_statx stx;
    if (ceph_statx(cmount_, path.c_str(), &stx, CEPH_STATX_ALL_STATS, 0))
      return Status::IOError("libcephfs::ceph_statx failed");
    size = stx.stx_size;
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

  Status ListDirRecursive(const std::string& path, std::vector<std::string>& files) {
    struct dirent* de = NULL;
    struct ceph_dir_result* dirr = NULL;

    if (ceph_opendir(cmount_, path.c_str(), &dirr))
      return Status::IOError("libcephfs::ceph_opendir returned non-zero exit code.");

    while ((de = ceph_readdir(cmount_, dirr)) != NULL) {
      std::string entry(de->d_name);

      if (de->d_type == DT_REG) {
        files.push_back(path + "/" + entry);
      } else {
        if (entry == "." || entry == "..") continue;
        ListDirRecursive(path + "/" + entry, files);
      }
    }

    if (ceph_closedir(cmount_, dirr))
      return Status::IOError("libcephfs::ceph_closedir returned non-zero exit code.");

    return Status::OK();
  }

  Status ListDir(const std::string& path, std::vector<std::string>& files) {
    return ListDirRecursive(path, files);
  }

 protected:
  std::shared_ptr<RadosCluster> cluster_;
  struct ceph_mount_info* cmount_;
};

class ARROW_DS_EXPORT RadosFragment : public Fragment {
 public:
  RadosFragment(std::shared_ptr<Schema> schema, std::string path,
                std::shared_ptr<RadosFileSystem> filesystem,
                std::shared_ptr<Expression> partition_expression = scalar(true))
      : Fragment(partition_expression, std::move(schema)),
        path_(std::move(path)),
        filesystem_(std::move(filesystem)) {}

  Result<ScanTaskIterator> Scan(std::shared_ptr<ScanOptions> options,
                                std::shared_ptr<ScanContext> context) override;

  std::string type_name() const override { return "rados"; }

  bool splittable() const override { return false; }

 protected:
  Result<std::shared_ptr<Schema>> ReadPhysicalSchemaImpl() override;
  std::string path_;
  std::shared_ptr<RadosFileSystem> filesystem_;
};

using RadosFragmentVector = std::vector<std::shared_ptr<RadosFragment>>;

class ARROW_DS_EXPORT RadosDataset : public Dataset {
 public:
  static Result<std::shared_ptr<Dataset>> Make(
      std::shared_ptr<Schema> schema, RadosFragmentVector fragments,
      std::shared_ptr<RadosFileSystem> filesystem);

  const std::shared_ptr<Schema>& schema() const { return schema_; }

  std::string type_name() const override { return "rados"; }

  Result<std::shared_ptr<Dataset>> ReplaceSchema(
      std::shared_ptr<Schema> schema) const override;
 protected:
  RadosDataset(std::shared_ptr<Schema> schema, RadosFragmentVector fragments,
               std::shared_ptr<RadosFileSystem> filesystem)
      : Dataset(std::move(schema)),
        fragments_(fragments),
        filesystem_(std::move(filesystem)) {}

  FragmentIterator GetFragmentsImpl(
      std::shared_ptr<Expression> predicate = scalar(true)) override;
  RadosFragmentVector fragments_;
  std::shared_ptr<RadosFileSystem> filesystem_;
};

class ARROW_DS_EXPORT RadosScanTask : public ScanTask {
 public:
  RadosScanTask(std::shared_ptr<ScanOptions> options,
                std::shared_ptr<ScanContext> context, std::string path,
                std::shared_ptr<RadosFileSystem> filesystem)
      : ScanTask(std::move(options), std::move(context)),
        path_(std::move(path)),
        filesystem_(std::move(filesystem)) {}

  Result<RecordBatchIterator> Execute();

 protected:
  std::string path_;
  std::shared_ptr<RadosFileSystem> filesystem_;
  std::shared_ptr<TableBatchReader> reader_;
};

class ARROW_DS_EXPORT RadosDatasetFactory : public DatasetFactory {
 public:
  static Result<std::shared_ptr<DatasetFactory>> Make(
      std::shared_ptr<RadosFileSystem> filesystem, RadosDatasetFactoryOptions options);

  Result<std::vector<std::shared_ptr<Schema>>> InspectSchemas(InspectOptions options);

  Result<std::shared_ptr<Dataset>> Finish(FinishOptions options) override;

 protected:
  RadosDatasetFactory(std::vector<std::string> paths,
                      std::shared_ptr<RadosFileSystem> filesystem,
                      RadosDatasetFactoryOptions options)
      : paths_(paths), filesystem_(std::move(filesystem)), options_(std::move(options)) {}
  std::vector<std::string> paths_;
  std::shared_ptr<RadosFileSystem> filesystem_;
  RadosDatasetFactoryOptions options_;
};

class CephFSParquetWriter {
 public:
  CephFSParquetWriter(std::shared_ptr<RadosFileSystem> filesystem)
      : filesystem_(std::move(filesystem)) {}

  Status WriteTable(std::shared_ptr<Table> table, std::string path) {
    ARROW_ASSIGN_OR_RAISE(auto bos, io::BufferOutputStream::Create());

    PARQUET_THROW_NOT_OK(
        parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), bos, 100));

    ARROW_ASSIGN_OR_RAISE(auto buffer, bos->Finish());

    int64_t num_bytes_written = filesystem_->Write(path, buffer);
    if (num_bytes_written < 0)
      return Status::IOError(
          "CephFSParquetWriter::WriteTable returned non-zero exit code.");
    return Status::OK();
  }

 protected:
  std::shared_ptr<RadosFileSystem> filesystem_;
};

class ARROW_DS_EXPORT ObjectInputFile : public arrow::io::RandomAccessFile {
 public:
  explicit ObjectInputFile(std::shared_ptr<RadosFileSystem> filesystem, std::string &path)
    : filesystem_(std::move(filesystem)),
      path_(path) {Init();}

  arrow::Status Init() {
    uint64_t size;
    ARROW_RETURN_NOT_OK(filesystem_->GetSize(path_, size));
    content_length_ = size;
    return Status::OK();
  }

  arrow::Status CheckClosed() const {
    if (closed_) {
      return arrow::Status::Invalid("Operation on closed stream");
    }
    return arrow::Status::OK();
  }

  arrow::Status CheckPosition(int64_t position, const char* action) const {
    if (position < 0) {
      return arrow::Status::Invalid("Cannot ", action, " from negative position");
    }
    if (position > content_length_) {
      return arrow::Status::IOError("Cannot ", action, " past end of file");
    }
    return arrow::Status::OK();
  }

  arrow::Result<int64_t> ReadAt(int64_t position, int64_t nbytes, void* out) { return 0; }

  arrow::Result<std::shared_ptr<arrow::Buffer>> ReadAt(int64_t position, int64_t nbytes) {
    RETURN_NOT_OK(CheckClosed());
    RETURN_NOT_OK(CheckPosition(position, "read"));

    // No need to allocate more than the remaining number of bytes
    nbytes = std::min(nbytes, content_length_ - position);

    if (nbytes > 0) {
      std::shared_ptr<Buffer> buffer;
      ARROW_RETURN_NOT_OK(filesystem_->Read(path_, buffer, position, nbytes));
      return buffer;
    }
    return std::make_shared<arrow::Buffer>("");
  }

  arrow::Result<std::shared_ptr<arrow::Buffer>> Read(int64_t nbytes) {
    ARROW_ASSIGN_OR_RAISE(auto buffer, ReadAt(pos_, nbytes));
    pos_ += buffer->size();
    return std::move(buffer);
  }

  arrow::Result<int64_t> Read(int64_t nbytes, void* out) {
    ARROW_ASSIGN_OR_RAISE(int64_t bytes_read, ReadAt(pos_, nbytes, out));
    pos_ += bytes_read;
    return bytes_read;
  }

  arrow::Result<int64_t> GetSize() {
    RETURN_NOT_OK(CheckClosed());
    return content_length_;
  }

  arrow::Status Seek(int64_t position) {
    RETURN_NOT_OK(CheckClosed());
    RETURN_NOT_OK(CheckPosition(position, "seek"));

    pos_ = position;
    return arrow::Status::OK();
  }

  arrow::Result<int64_t> Tell() const {
    RETURN_NOT_OK(CheckClosed());
    return pos_;
  }

  arrow::Status Close() {
    closed_ = true;
    return arrow::Status::OK();
  }

  bool closed() const { return closed_; }

 protected:
  std::shared_ptr<RadosFileSystem> filesystem_;
  std::string path_;
  bool closed_ = false;
  int64_t pos_ = 0;
  int64_t content_length_ = -1;
};

}  // namespace dataset
}  // namespace arrow
