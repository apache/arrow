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

#include <rados/objclass.h>

#include "arrow/api.h"
#include "arrow/dataset/dataset_rados.h"
#include "arrow/dataset/file_parquet.h"
#include "arrow/dataset/rados_utils.h"
#include "arrow/io/api.h"
#include "arrow/ipc/api.h"
#include "parquet/api/reader.h"
#include "parquet/arrow/reader.h"
#include "parquet/file_reader.h"

CLS_VER(1, 0)
CLS_NAME(arrow)

cls_handle_t h_class;
cls_method_handle_t h_read_ipc_schema;
cls_method_handle_t h_read_parquet_schema;
cls_method_handle_t h_write;
cls_method_handle_t h_scan;

class RandomAccessObject : public arrow::io::RandomAccessFile {
 public:
  explicit RandomAccessObject(cls_method_context_t hctx) { hctx_ = hctx; }

  arrow::Status Init() {
    uint64_t size;
    int e = cls_cxx_stat(hctx_, &size, NULL);
    if (e == 0) {
      content_length_ = size;
      return arrow::Status::OK();
    } else {
      return arrow::Status::ExecutionError("Can't read the object");
    }
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
      librados::bufferlist bl;
      cls_cxx_read(hctx_, position, nbytes, &bl);
      return std::make_shared<arrow::Buffer>((uint8_t*)bl.c_str(), bl.length());
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
  cls_method_context_t hctx_;
  bool closed_ = false;
  int64_t pos_ = 0;
  int64_t content_length_ = -1;
};

static arrow::Status ScanParquetObject(
    cls_method_context_t hctx, std::shared_ptr<arrow::dataset::Expression> filter,
    std::shared_ptr<arrow::dataset::Expression> partition_expression,
    std::shared_ptr<arrow::Schema> schema, std::shared_ptr<arrow::Table>& t) {
  auto file = std::make_shared<RandomAccessObject>(hctx);
  ARROW_RETURN_NOT_OK(file->Init());

  arrow::dataset::FileSource source(file);

  std::unique_ptr<parquet::arrow::FileReader> reader;
  ARROW_RETURN_NOT_OK(
      parquet::arrow::OpenFile(file, arrow::default_memory_pool(), &reader));

  std::shared_ptr<arrow::Schema> table_schema;
  ARROW_RETURN_NOT_OK(reader->GetSchema(&table_schema));

  auto format = std::make_shared<arrow::dataset::ParquetFileFormat>();
  ARROW_ASSIGN_OR_RAISE(auto fragment,
                        format->MakeFragment(source, partition_expression, table_schema));

  auto ctx = std::make_shared<arrow::dataset::ScanContext>();
  auto builder = std::make_shared<arrow::dataset::ScannerBuilder>(schema, fragment, ctx);
  ARROW_RETURN_NOT_OK(builder->Filter(filter));
  ARROW_ASSIGN_OR_RAISE(auto scanner, builder->Finish());
  ARROW_ASSIGN_OR_RAISE(auto table, scanner->ToTable());

  t = table;

  ARROW_RETURN_NOT_OK(file->Close());
  return arrow::Status::OK();
}

static arrow::Status ScanIpcObject(
    cls_method_context_t hctx, std::shared_ptr<arrow::dataset::Expression> filter,
    std::shared_ptr<arrow::dataset::Expression> partition_expression,
    std::shared_ptr<arrow::Schema> schema, std::shared_ptr<arrow::Table>& t) {
  ceph::buffer::list bl;
  if (cls_cxx_read(hctx, 0, 0, &bl) < 0)
    return arrow::Status::ExecutionError("READ_FAILED");

  arrow::RecordBatchVector batches;
  std::shared_ptr<arrow::Buffer> buffer =
      std::make_shared<arrow::Buffer>((uint8_t*)bl.c_str(), bl.length());
  std::shared_ptr<arrow::io::BufferReader> buffer_reader =
      std::make_shared<arrow::io::BufferReader>(buffer);
  /// We can change this to RecordBatchFileReader later, to read from Arrow/Feather files.
  ARROW_ASSIGN_OR_RAISE(auto record_batch_reader,
                        arrow::ipc::RecordBatchStreamReader::Open(buffer_reader));
  ARROW_RETURN_NOT_OK(record_batch_reader->ReadAll(&batches));

  auto ctx = std::make_shared<arrow::dataset::ScanContext>();
  auto fragment = std::make_shared<arrow::dataset::InMemoryFragment>(batches);
  auto table_schema = batches[0]->schema();

  auto builder =
      std::make_shared<arrow::dataset::ScannerBuilder>(table_schema, fragment, ctx);
  ARROW_RETURN_NOT_OK(builder->Filter(filter));
  ARROW_RETURN_NOT_OK(builder->Project(schema->field_names()));
  ARROW_ASSIGN_OR_RAISE(auto scanner, builder->Finish());
  ARROW_ASSIGN_OR_RAISE(auto table, scanner->ToTable());

  t = table;
  return arrow::Status::OK();
}

static int read_parquet_schema(cls_method_context_t hctx, ceph::buffer::list* in,
                               ceph::buffer::list* out) {
  std::shared_ptr<RandomAccessObject> source = std::make_shared<RandomAccessObject>(hctx);
  if (!source->Init().ok()) return -1;

  std::unique_ptr<parquet::arrow::FileReader> reader;
  if (!parquet::arrow::OpenFile(source, arrow::default_memory_pool(), &reader).ok())
    return -1;

  std::shared_ptr<arrow::Schema> schema;
  if (!reader->GetSchema(&schema).ok()) return -1;

  std::shared_ptr<arrow::Buffer> buffer =
      arrow::ipc::SerializeSchema(*schema).ValueOrDie();
  ceph::buffer::list result;
  result.append((char*)buffer->data(), buffer->size());
  *out = result;
  return 0;
}

static int read_ipc_schema(cls_method_context_t hctx, ceph::buffer::list* in,
                           ceph::buffer::list* out) {
  ceph::buffer::list bl;
  if (cls_cxx_read(hctx, 0, 0, &bl) < 0) return -1;

  std::shared_ptr<arrow::Table> table;
  if (!arrow::dataset::DeserializeTableFromBufferlist(&table, bl).ok()) return -1;

  std::shared_ptr<arrow::Schema> schema = table->schema();
  std::shared_ptr<arrow::Buffer> buffer =
      arrow::ipc::SerializeSchema(*schema).ValueOrDie();
  ceph::buffer::list result;
  result.append((char*)buffer->data(), buffer->size());
  *out = result;
  return 0;
}

static int write(cls_method_context_t hctx, ceph::buffer::list* in,
                 ceph::buffer::list* out) {
  if (cls_cxx_create(hctx, false) < 0) return -1;
  if (cls_cxx_write(hctx, 0, in->length(), in) < 0) return -1;
  return 0;
}

static int scan(cls_method_context_t hctx, ceph::buffer::list* in,
                ceph::buffer::list* out) {
  std::shared_ptr<arrow::dataset::Expression> filter;
  std::shared_ptr<arrow::dataset::Expression> partition_expression;
  std::shared_ptr<arrow::Schema> schema;
  int64_t format;
  if (!arrow::dataset::DeserializeScanRequestFromBufferlist(
           &filter, &partition_expression, &schema, &format, *in)
           .ok())
    return -1;

  std::shared_ptr<arrow::Table> table;
  if (format == 1) {
    if (!ScanIpcObject(hctx, filter, partition_expression, schema, table).ok()) return -1;
  } else if (format == 2) {
    if (!ScanParquetObject(hctx, filter, partition_expression, schema, table).ok())
      return -1;
  } else {
    return -1;
  }

  CLS_LOG(0, "table: %s", table->ToString().c_str());

  ceph::buffer::list bl;
  if (!arrow::dataset::SerializeTableToIPCStream(table, bl).ok()) return -1;

  *out = bl;
  return 0;
}

void __cls_init() {
  CLS_LOG(0, "loading cls_arrow");

  cls_register("arrow", &h_class);

  cls_register_cxx_method(h_class, "read_ipc_schema", CLS_METHOD_RD | CLS_METHOD_WR,
                          read_ipc_schema, &h_read_ipc_schema);
  cls_register_cxx_method(h_class, "read_parquet_schema", CLS_METHOD_RD | CLS_METHOD_WR,
                          read_parquet_schema, &h_read_parquet_schema);
  cls_register_cxx_method(h_class, "write", CLS_METHOD_RD | CLS_METHOD_WR, write,
                          &h_write);
  cls_register_cxx_method(h_class, "scan", CLS_METHOD_RD | CLS_METHOD_WR, scan, &h_scan);
}
