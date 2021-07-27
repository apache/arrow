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
#include "arrow/dataset/file_skyhook.h"

#include <mutex>

#include "arrow/api.h"
#include "arrow/compute/exec/expression.h"
#include "arrow/dataset/dataset_internal.h"
#include "arrow/dataset/file_base.h"
#include "arrow/dataset/file_ipc.h"
#include "arrow/dataset/file_parquet.h"
#include "arrow/filesystem/filesystem.h"
#include "arrow/filesystem/path_util.h"
#include "arrow/filesystem/util_internal.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/compression.h"
#include "arrow/util/iterator.h"
#include "arrow/util/logging.h"
#include "parquet/arrow/reader.h"
#include "parquet/file_reader.h"

#include <flatbuffers/flatbuffers.h>

#include "generated/ScanRequest_generated.h"

namespace arrow {

namespace flatbuf = org::apache::arrow::flatbuf;

namespace dataset {

namespace connection {

std::mutex connection_mutex;

RadosConnection::~RadosConnection() { Shutdown(); }

Status RadosConnection::Connect() {
  if (connected) {
    return Status::OK();
  }

  // Locks the mutex. Only one thread can pass here at a time.
  // Another thread handled the connection already.
  std::unique_lock<std::mutex> lock(connection_mutex);
  if (connected) {
    return Status::OK();
  }
  connected = true;

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

Status RadosConnection::Shutdown() {
  rados->shutdown();
  return Status::OK();
}

}  // namespace connection

/// \brief A ScanTask to scan a file fragment in Skyhook format.
class SkyhookScanTask : public ScanTask {
 public:
  SkyhookScanTask(std::shared_ptr<ScanOptions> options,
                  std::shared_ptr<Fragment> fragment, FileSource source,
                  std::shared_ptr<SkyhookDirectObjectAccess> doa, int fragment_format)
      : ScanTask(std::move(options), std::move(fragment)),
        source_(std::move(source)),
        doa_(std::move(doa)),
        fragment_format_(fragment_format) {}

  Result<RecordBatchIterator> Execute() override {
    struct stat st {};
    ARROW_RETURN_NOT_OK(doa_->Stat(source_.path(), st));

    ceph::bufferlist request;
    ARROW_RETURN_NOT_OK(
        SerializeScanRequest(options_, fragment_format_, st.st_size, request));

    ceph::bufferlist result;
    ARROW_RETURN_NOT_OK(doa_->Exec(st.st_ino, "scan_op", request, result));

    RecordBatchVector batches;
    ARROW_RETURN_NOT_OK(DeserializeTable(batches, result, !options_->use_threads));
    return MakeVectorIterator(batches);
  }

 protected:
  FileSource source_;
  std::shared_ptr<SkyhookDirectObjectAccess> doa_;
  int fragment_format_;
};

SkyhookFileFormat::SkyhookFileFormat(const std::string& fragment_format,
                                     const std::string& ceph_config_path,
                                     const std::string& data_pool,
                                     const std::string& user_name,
                                     const std::string& cluster_name,
                                     const std::string& cls_name)
    : SkyhookFileFormat(std::make_shared<connection::RadosConnection>(
          connection::RadosConnection::RadosConnectionCtx(
              ceph_config_path, data_pool, user_name, cluster_name, cls_name))) {
  fragment_format_ = fragment_format;
}

SkyhookFileFormat::SkyhookFileFormat(
    const std::shared_ptr<connection::RadosConnection>& connection) {
  connection->Connect();
  auto doa = std::make_shared<arrow::dataset::SkyhookDirectObjectAccess>(connection);
  doa_ = doa;
}

Result<std::shared_ptr<Schema>> SkyhookFileFormat::Inspect(
    const FileSource& source) const {
  std::shared_ptr<FileFormat> format;
  if (fragment_format_ == "parquet") {
    format = std::make_shared<ParquetFileFormat>();
  } else if (fragment_format_ == "ipc") {
    format = std::make_shared<IpcFileFormat>();
  } else {
    return Status::Invalid("invalid file format");
  }
  std::shared_ptr<Schema> schema;
  ARROW_ASSIGN_OR_RAISE(schema, format->Inspect(source));
  return schema;
}

Result<ScanTaskIterator> SkyhookFileFormat::ScanFile(
    const std::shared_ptr<ScanOptions>& options,
    const std::shared_ptr<FileFragment>& file) const {
  std::shared_ptr<ScanOptions> options_ = std::make_shared<ScanOptions>(*options);
  options_->partition_expression = file->partition_expression();
  options_->dataset_schema = file->dataset_schema();

  int fragment_format = -1;
  if (fragment_format_ == "parquet")
    fragment_format = 0;
  else if (fragment_format_ == "ipc")
    fragment_format = 1;
  else
    return Status::Invalid("Unsupported file format");

  ScanTaskVector v{std::make_shared<SkyhookScanTask>(std::move(options_), std::move(file),
                                                     file->source(), std::move(doa_),
                                                     fragment_format)};
  return MakeVectorIterator(v);
}

Status SerializeScanRequest(std::shared_ptr<ScanOptions>& options, int& file_format,
                            int64_t& file_size, ceph::bufferlist& bl) {
  ARROW_ASSIGN_OR_RAISE(auto filter, compute::Serialize(options->filter));
  ARROW_ASSIGN_OR_RAISE(auto partition,
                        compute::Serialize(options->partition_expression));
  ARROW_ASSIGN_OR_RAISE(auto projected_schema,
                        ipc::SerializeSchema(*options->projected_schema));
  ARROW_ASSIGN_OR_RAISE(auto dataset_schema,
                        ipc::SerializeSchema(*options->dataset_schema));

  flatbuffers::FlatBufferBuilder builder(1024);

  auto filter_vec = builder.CreateVector(filter->data(), filter->size());
  auto partition_vec = builder.CreateVector(partition->data(), partition->size());
  auto projected_schema_vec =
      builder.CreateVector(projected_schema->data(), projected_schema->size());
  auto dataset_schema_vec =
      builder.CreateVector(dataset_schema->data(), dataset_schema->size());

  auto request =
      flatbuf::CreateScanRequest(builder, file_size, file_format, filter_vec,
                                 partition_vec, dataset_schema_vec, projected_schema_vec);
  builder.Finish(request);
  uint8_t* buf = builder.GetBufferPointer();
  int size = builder.GetSize();

  bl.append((char*)buf, size);
  return Status::OK();
}

Status DeserializeScanRequest(compute::Expression* filter, compute::Expression* partition,
                              std::shared_ptr<Schema>* projected_schema,
                              std::shared_ptr<Schema>* dataset_schema, int64_t& file_size,
                              int& file_format, ceph::bufferlist& bl) {
  auto request = flatbuf::GetScanRequest((uint8_t*)bl.c_str());

  ARROW_ASSIGN_OR_RAISE(auto filter_,
                        compute::Deserialize(std::make_shared<Buffer>(
                            request->filter()->data(), request->filter()->size())));
  *filter = filter_;

  ARROW_ASSIGN_OR_RAISE(auto partition_,
                        compute::Deserialize(std::make_shared<Buffer>(
                            request->partition()->data(), request->partition()->size())));
  *partition = partition_;

  ipc::DictionaryMemo empty_memo;
  io::BufferReader projected_schema_reader(request->projection_schema()->data(),
                                           request->projection_schema()->size());
  io::BufferReader dataset_schema_reader(request->dataset_schema()->data(),
                                         request->dataset_schema()->size());

  ARROW_ASSIGN_OR_RAISE(auto projected_schema_,
                        ipc::ReadSchema(&projected_schema_reader, &empty_memo));
  *projected_schema = projected_schema_;

  ARROW_ASSIGN_OR_RAISE(auto dataset_schema_,
                        ipc::ReadSchema(&dataset_schema_reader, &empty_memo));
  *dataset_schema = dataset_schema_;

  file_size = request->file_size();
  file_format = request->file_format();
  return Status::OK();
}

Status SerializeTable(std::shared_ptr<Table>& table, ceph::bufferlist& bl,
                      bool aggressive) {
  ARROW_ASSIGN_OR_RAISE(auto buffer_output_stream, io::BufferOutputStream::Create());

  ipc::IpcWriteOptions options = ipc::IpcWriteOptions::Defaults();

  Compression::type codec;
  if (aggressive) {
    codec = Compression::ZSTD;
  } else {
    codec = Compression::LZ4_FRAME;
  }

  ARROW_ASSIGN_OR_RAISE(options.codec,
                        util::Codec::Create(codec, std::numeric_limits<int>::min()));
  ARROW_ASSIGN_OR_RAISE(
      auto writer, ipc::MakeStreamWriter(buffer_output_stream, table->schema(), options));

  ARROW_RETURN_NOT_OK(writer->WriteTable(*table));
  ARROW_RETURN_NOT_OK(writer->Close());

  ARROW_ASSIGN_OR_RAISE(auto buffer, buffer_output_stream->Finish());
  bl.append((char*)buffer->data(), buffer->size());
  return Status::OK();
}

Status DeserializeTable(RecordBatchVector& batches, ceph::bufferlist& bl,
                        bool use_threads) {
  auto buffer = std::make_shared<Buffer>((uint8_t*)bl.c_str(), bl.length());
  auto buffer_reader = std::make_shared<io::BufferReader>(buffer);
  auto options = ipc::IpcReadOptions::Defaults();
  options.use_threads = use_threads;
  ARROW_ASSIGN_OR_RAISE(
      auto reader, arrow::ipc::RecordBatchStreamReader::Open(buffer_reader, options));
  ARROW_RETURN_NOT_OK(reader->ReadAll(&batches));
  return Status::OK();
}

}  // namespace dataset
}  // namespace arrow
