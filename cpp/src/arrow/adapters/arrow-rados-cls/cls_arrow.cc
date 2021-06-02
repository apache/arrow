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
#include <rados/objclass.h>
#include <memory>

#include "arrow/api.h"
#include "arrow/compute/exec/expression.h"
#include "arrow/dataset/dataset.h"
#include "arrow/dataset/file_parquet.h"
#include "arrow/dataset/file_rados_parquet.h"
#include "arrow/io/api.h"
#include "arrow/ipc/api.h"
#include "parquet/api/reader.h"
#include "parquet/arrow/reader.h"
#include "parquet/file_reader.h"

CLS_VER(1, 0)
CLS_NAME(arrow)

cls_handle_t h_class;
cls_method_handle_t h_scan_op;

class RandomAccessObject : public arrow::io::RandomAccessFile {
 public:
  explicit RandomAccessObject(cls_method_context_t hctx, int64_t file_size) {
    hctx_ = hctx;
    content_length_ = file_size;
    chunks_ = std::vector<ceph::bufferlist*>();
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
      ceph::bufferlist* bl = new ceph::bufferlist();
      cls_cxx_read(hctx_, position, nbytes, bl);
      chunks_.push_back(bl);
      return std::make_shared<arrow::Buffer>((uint8_t*)bl->c_str(), bl->length());
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
    for (auto chunk : chunks_) {
      delete chunk;
    }
    return arrow::Status::OK();
  }

  bool closed() const { return closed_; }

 protected:
  cls_method_context_t hctx_;
  bool closed_ = false;
  int64_t pos_ = 0;
  int64_t content_length_ = -1;
  std::vector<ceph::bufferlist*> chunks_;
};

static arrow::Status ScanParquetObject(cls_method_context_t hctx,
                                       arrow::compute::Expression filter,
                                       arrow::compute::Expression partition_expression,
                                       std::shared_ptr<arrow::Schema> projection_schema,
                                       std::shared_ptr<arrow::Schema> dataset_schema,
                                       std::shared_ptr<arrow::Table>& t,
                                       int64_t file_size) {
  auto file = std::make_shared<RandomAccessObject>(hctx, file_size);

  arrow::dataset::FileSource source(file);

  auto format = std::make_shared<arrow::dataset::ParquetFileFormat>();

  auto fragment_scan_options =
      std::make_shared<arrow::dataset::ParquetFragmentScanOptions>();

  ARROW_ASSIGN_OR_RAISE(auto fragment,
                        format->MakeFragment(source, partition_expression));
  auto options = std::make_shared<arrow::dataset::ScanOptions>();
  auto builder =
      std::make_shared<arrow::dataset::ScannerBuilder>(dataset_schema, fragment, options);

  ARROW_RETURN_NOT_OK(builder->Filter(filter));
  ARROW_RETURN_NOT_OK(builder->Project(projection_schema->field_names()));
  ARROW_RETURN_NOT_OK(builder->UseThreads(false));
  ARROW_RETURN_NOT_OK(builder->FragmentScanOptions(fragment_scan_options));

  ARROW_ASSIGN_OR_RAISE(auto scanner, builder->Finish());
  ARROW_ASSIGN_OR_RAISE(auto table, scanner->ToTable());

  t = table;

  ARROW_RETURN_NOT_OK(file->Close());
  return arrow::Status::OK();
}

static int scan_op(cls_method_context_t hctx, ceph::bufferlist* in,
                   ceph::bufferlist* out) {
  // the components required to construct a ParquetFragment.
  arrow::compute::Expression filter;
  arrow::compute::Expression partition_expression;
  std::shared_ptr<arrow::Schema> projection_schema;
  std::shared_ptr<arrow::Schema> dataset_schema;
  int64_t file_size;

  // deserialize the scan request
  if (!arrow::dataset::DeserializeScanRequest(&filter, &partition_expression,
                                              &projection_schema, &dataset_schema,
                                              file_size, *in)
           .ok())
    return -1;

  // scan the parquet object
  std::shared_ptr<arrow::Table> table;
  arrow::Status s =
      ScanParquetObject(hctx, filter, partition_expression, projection_schema,
                        dataset_schema, table, file_size);
  if (!s.ok()) {
    CLS_LOG(0, "error: %s", s.message().c_str());
    return -1;
  }

  // serialize the resultant table to send back to the client
  ceph::bufferlist bl;
  if (!arrow::dataset::SerializeTable(table, bl).ok()) return -1;

  *out = bl;
  return 0;
}

void __cls_init() {
  CLS_LOG(0, "loading cls_arrow");

  cls_register("arrow", &h_class);

  cls_register_cxx_method(h_class, "scan_op", CLS_METHOD_RD, scan_op, &h_scan_op);
}
