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
#include "arrow/dataset/file_parquet_rados.h"

#include "arrow/api.h"
#include "arrow/dataset/dataset_internal.h"
#include "arrow/dataset/expression.h"
#include "arrow/dataset/file_base.h"
#include "arrow/filesystem/filesystem.h"
#include "arrow/filesystem/path_util.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/iterator.h"
#include "arrow/util/logging.h"
#include "arrow/filesystem/util_internal.h"


namespace arrow {
namespace dataset {

class RadosParquetScanTask : public ScanTask {
 public:
  RadosParquetScanTask(std::shared_ptr<ScanOptions> options,
                       std::shared_ptr<ScanContext> context, FileSource source,
                       std::shared_ptr<DirectObjectAccess> doa)
      : ScanTask(std::move(options), std::move(context)),
        source_(std::move(source)),
        doa_(std::move(doa)) {}

  Result<RecordBatchIterator> Execute() override {
    std::shared_ptr<librados::bufferlist> in = std::make_shared<librados::bufferlist>();
    std::shared_ptr<librados::bufferlist> out = std::make_shared<librados::bufferlist>();

    ARROW_RETURN_NOT_OK(SerializeScanRequestToBufferlist(
        options_->filter, options_->partition_expression, options_->projector.schema(),
        options_->dataset_schema, in));

    auto path =
        fs::internal::RemoveAncestor("/mnt/cephfs/", source_.path()).value().to_string();

    Status s = doa_->Exec(path, "scan", in, out);
    if (!s.ok()) {
      return Status::ExecutionError(s.message());
    }

    char *scanned_table_buffer = new char[out->length()];
    librados::bufferlist::iterator itr = out->begin();
    itr.copy(out->length(), scanned_table_buffer);

    RecordBatchVector batches;
    auto buffer = std::make_shared<Buffer>((uint8_t*)scanned_table_buffer, out->length());
    auto buffer_reader = std::make_shared<io::BufferReader>(buffer);
    ARROW_ASSIGN_OR_RAISE(auto rb_reader,
                          arrow::ipc::RecordBatchStreamReader::Open(buffer_reader));
    rb_reader->ReadAll(&batches);
    return MakeVectorIterator(batches);
  }

 protected:
  FileSource source_;
  std::shared_ptr<DirectObjectAccess> doa_;
};

Result<bool> RadosParquetFileFormat::IsSupported(const FileSource& source) const {
  return true;
}

bool RadosParquetFileFormat::Equals(const FileFormat& other) const { return true; }

Result<std::shared_ptr<RadosParquetFileFormat>> RadosParquetFileFormat::Make(
    const std::string& path_to_config) {
  auto cluster = std::make_shared<RadosCluster>(path_to_config);
  RETURN_NOT_OK(cluster->Connect());
  auto doa = std::make_shared<arrow::dataset::DirectObjectAccess>();
  RETURN_NOT_OK(doa->Init(cluster));
  return std::make_shared<RadosParquetFileFormat>(doa);
}

RadosParquetFileFormat::RadosParquetFileFormat(const std::string& path_to_config) {
  auto cluster = std::make_shared<RadosCluster>(path_to_config);
  cluster->Connect();
  auto doa = std::make_shared<arrow::dataset::DirectObjectAccess>();
  doa->Init(cluster);
  doa_ = doa;
}

Result<std::shared_ptr<Schema>> RadosParquetFileFormat::Inspect(
    const FileSource& source) const {
  std::shared_ptr<librados::bufferlist> in = std::make_shared<librados::bufferlist>();
  std::shared_ptr<librados::bufferlist> out = std::make_shared<librados::bufferlist>();

  auto path =
      fs::internal::RemoveAncestor("/mnt/cephfs/", source.path()).value().to_string();

  Status s = doa_->Exec(path, "read_schema", in, out);
  if (!s.ok()) return Status::ExecutionError(s.message());

  std::vector<std::shared_ptr<Schema>> schemas;
  ipc::DictionaryMemo empty_memo;
  io::BufferReader schema_reader((uint8_t*)out->c_str(), out->length());
  ARROW_ASSIGN_OR_RAISE(auto schema, ipc::ReadSchema(&schema_reader, &empty_memo));
  return schema;
}

Result<ScanTaskIterator> RadosParquetFileFormat::ScanFile(
    std::shared_ptr<ScanOptions> options, std::shared_ptr<ScanContext> context,
    FileFragment* file) const {
  options->partition_expression = file->partition_expression();
  ARROW_ASSIGN_OR_RAISE(options->dataset_schema, file->ReadPhysicalSchema());

  ScanTaskVector v{std::make_shared<RadosParquetScanTask>(
      std::move(options), std::move(context), file->source(), std::move(doa_))};
  return MakeVectorIterator(v);
}

}  // namespace dataset
}  // namespace arrow
