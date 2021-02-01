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
#include "arrow/dataset/dataset_rados.h"
#include "arrow/dataset/rados_utils.h"
#include "arrow/dataset/scanner.h"

#include <memory>
#include <utility>

#include "arrow/dataset/dataset_internal.h"
#include "arrow/dataset/filter.h"
#include "arrow/filesystem/filesystem.h"
#include "arrow/filesystem/path_util.h"
#include "arrow/table.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/iterator.h"
#include "arrow/util/logging.h"
#include "arrow/util/make_unique.h"

namespace arrow {
namespace dataset {

Result<ScanTaskIterator> RadosFragment::Scan(std::shared_ptr<ScanOptions> options,
                                             std::shared_ptr<ScanContext> context) {
  // inject partition expression and dataset schema in the scan options
  options->partition_expression = partition_expression_;
  ARROW_ASSIGN_OR_RAISE(options->dataset_schema, ReadPhysicalSchema());

  ScanTaskVector v{std::make_shared<RadosScanTask>(
      std::move(options), std::move(context), std::move(path_), std::move(filesystem_))};
  return MakeVectorIterator(v);
}

Result<std::shared_ptr<Schema>> RadosFragment::ReadPhysicalSchemaImpl() {
  return physical_schema_;
}

Result<std::shared_ptr<DatasetFactory>> RadosDatasetFactory::Make(
    std::shared_ptr<RadosFileSystem> filesystem, RadosDatasetFactoryOptions options) {
  std::vector<std::string> paths;
  filesystem->ListDir(options.partition_base_dir, paths);
  if (paths.empty()) {
    RETURN_NOT_OK(Status::Invalid("No files found."));
  }

  return std::shared_ptr<DatasetFactory>(new RadosDatasetFactory(
      std::move(paths), std::move(filesystem), std::move(options)));
}

Result<std::vector<std::shared_ptr<Schema>>> RadosDatasetFactory::InspectSchemas(
    InspectOptions options) {
  std::shared_ptr<librados::bufferlist> in = std::make_shared<librados::bufferlist>();
  std::shared_ptr<librados::bufferlist> out = std::make_shared<librados::bufferlist>();

  if (!filesystem_->Exec(paths_[0], "read_schema", in, out).ok()) {
    return Status::ExecutionError("RadosFileSystem::Exec returned non-zero exit code.");
  }

  std::vector<std::shared_ptr<Schema>> schemas;
  ipc::DictionaryMemo empty_memo;
  io::BufferReader schema_reader((uint8_t*)out->c_str(), out->length());
  ARROW_ASSIGN_OR_RAISE(auto schema, ipc::ReadSchema(&schema_reader, &empty_memo));
  schemas.push_back(schema);

  auto partition_schema = options_.partitioning.partitioning()->schema();
  schemas.push_back(partition_schema);

  return schemas;
}

Result<std::shared_ptr<Dataset>> RadosDatasetFactory::Finish(FinishOptions options) {
  InspectOptions inspect_options;
  ARROW_ASSIGN_OR_RAISE(auto schema, Inspect(inspect_options));

  std::shared_ptr<Partitioning> partitioning = options_.partitioning.partitioning();
  if (partitioning == nullptr) {
    auto factory = options_.partitioning.factory();
    ARROW_ASSIGN_OR_RAISE(partitioning, factory->Finish(schema));
  }

  RadosFragmentVector fragments;
  for (auto& path : paths_) {
    auto fixed_path = StripPrefixAndFilename(path, options_.partition_base_dir);
    ARROW_ASSIGN_OR_RAISE(auto partition, partitioning->Parse(fixed_path));
    fragments.push_back(
        std::make_shared<RadosFragment>(schema, path, filesystem_, partition));
  }
  return RadosDataset::Make(schema, fragments, filesystem_);
}

Result<std::shared_ptr<Dataset>> RadosDataset::ReplaceSchema(
    std::shared_ptr<Schema> schema) const {
  RETURN_NOT_OK(CheckProjectable(*schema_, *schema));
  return RadosDataset::Make(std::move(schema), std::move(fragments_),
                            std::move(filesystem_));
}

Result<std::shared_ptr<Dataset>> RadosDataset::Make(
    std::shared_ptr<Schema> schema, RadosFragmentVector fragments,
    std::shared_ptr<RadosFileSystem> filesystem) {
  return std::shared_ptr<Dataset>(new RadosDataset(schema, fragments, filesystem));
}

FragmentIterator RadosDataset::GetFragmentsImpl(std::shared_ptr<Expression> predicate) {
  FragmentVector fragments;
  for (const auto& fragment : fragments_) {
    bool satisfies = predicate->IsSatisfiableWith(fragment->partition_expression());
    if (satisfies) {
      fragments.push_back(fragment);
    }
  }
  return MakeVectorIterator(std::move(fragments));
}

Result<RecordBatchIterator> RadosScanTask::Execute() {
  struct Impl {
    static Result<RecordBatchIterator> Make(std::shared_ptr<Buffer> buffer) {
      ARROW_ASSIGN_OR_RAISE(auto file, Buffer::GetReader(buffer));
      ARROW_ASSIGN_OR_RAISE(auto reader, ipc::RecordBatchFileReader::Open(
                                             file, ipc::IpcReadOptions::Defaults()));
      return RecordBatchIterator(Impl{std::move(reader), 0});
    }

    Result<std::shared_ptr<RecordBatch>> Next() {
      if (i_ == reader_->num_record_batches()) {
        return nullptr;
      }

      return reader_->ReadRecordBatch(i_++);
    }

    std::shared_ptr<ipc::RecordBatchFileReader> reader_;
    int i_;
  };

  std::shared_ptr<librados::bufferlist> in = std::make_shared<librados::bufferlist>();
  std::shared_ptr<librados::bufferlist> out = std::make_shared<librados::bufferlist>();

  ARROW_RETURN_NOT_OK(SerializeScanRequestToBufferlist(
      options_->filter, options_->partition_expression, options_->projector.schema(),
      options_->dataset_schema, in));

  Status s = filesystem_->Exec(path_, "scan", in, out);
  if (!s.ok()) {
    return Status::ExecutionError(s.message());
  }
  auto buffer = std::make_shared<Buffer>((uint8_t*)out->c_str(), out->length());
  return Impl::Make(buffer);
}

}  // namespace dataset
}  // namespace arrow