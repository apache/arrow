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
  options->format = format_;
  options->partition_expression = partition_expression_;

  ScanTaskVector v{std::make_shared<RadosScanTask>(
      std::move(options), std::move(context), std::move(object_), std::move(cluster_))};
  return MakeVectorIterator(v);
}

Result<std::shared_ptr<Schema>> RadosFragment::ReadPhysicalSchemaImpl() {
  return physical_schema_;
}

Result<std::shared_ptr<DatasetFactory>> RadosDatasetFactory::Make(
    RadosDatasetFactoryOptions options, RadosObjectVector objects) {
  auto cluster =
      std::make_shared<RadosCluster>(options.pool_name_, options.ceph_config_path_);
  cluster->Connect();

  RadosObjectVector discovered_objects;

  if (objects.size() > 0) {
    discovered_objects = objects;
  } else {
    auto objects = cluster->io_ctx_interface_->list();

    for (auto& object_id : objects) {
      if (fs::internal::IsAncestorOf(options.partition_base_dir, object_id)) {
        discovered_objects.push_back(std::make_shared<RadosObject>(object_id));
      }
    }
  }

  if (discovered_objects.empty()) {
    RETURN_NOT_OK(Status::Invalid("No objects found."));
  }

  return std::shared_ptr<DatasetFactory>(new RadosDatasetFactory(
      std::move(discovered_objects), std::move(cluster), std::move(options)));
}

Result<std::vector<std::shared_ptr<Schema>>> RadosDatasetFactory::InspectSchemas(
    InspectOptions options) {
  std::string object_id = objects_[0]->id();
  librados::bufferlist in, out;
  int e;

  switch (options_.format_) {
    case 1:
      e = cluster_->io_ctx_interface_->exec(object_id, cluster_->cls_name_.c_str(),
                                            "read_ipc_schema", in, out);
      if (e != 0) {
        return Status::ExecutionError("call to exec() returned non-zero exit code.");
      }
      break;

    case 2:
      e = cluster_->io_ctx_interface_->exec(object_id, cluster_->cls_name_.c_str(),
                                            "read_parquet_schema", in, out);
      if (e != 0) {
        return Status::ExecutionError("call to exec() returned non-zero exit code.");
      }
      break;

    default:
      break;
  }

  std::vector<std::shared_ptr<Schema>> schemas;
  ipc::DictionaryMemo empty_memo;
  io::BufferReader schema_reader((uint8_t*)out.c_str(), out.length());
  ARROW_ASSIGN_OR_RAISE(auto schema, ipc::ReadSchema(&schema_reader, &empty_memo));
  schemas.push_back(schema);

  auto partition_schema = options_.partitioning.partitioning()->schema();
  schemas.push_back(partition_schema);

  return schemas;
}

Result<std::shared_ptr<Dataset>> RadosDatasetFactory::Finish(FinishOptions options) {
  InspectOptions inspect_options_;

  // basically, these two lines make up of Inspect()
  ARROW_ASSIGN_OR_RAISE(auto schema_vector, InspectSchemas(inspect_options_));
  ARROW_ASSIGN_OR_RAISE(auto schema, UnifySchemas(schema_vector));

  std::shared_ptr<Partitioning> partitioning = options_.partitioning.partitioning();
  if (partitioning == nullptr) {
    auto factory = options_.partitioning.factory();
    ARROW_ASSIGN_OR_RAISE(partitioning, factory->Finish(schema));
  }

  RadosFragmentVector fragments;
  for (auto& object : objects_) {
    auto fixed_path = StripPrefixAndFilename(object->id(), options_.partition_base_dir);
    ARROW_ASSIGN_OR_RAISE(auto partition, partitioning->Parse(fixed_path));
    fragments.push_back(std::make_shared<RadosFragment>(schema, object, cluster_,
                                                        options_.format_, partition));
  }
  return RadosDataset::Make(schema, fragments, cluster_);
}

Status RadosCluster::Connect() {
  int e;
  /// Initialize the cluster handle.
  e = rados_interface_->init2(user_name_.c_str(), cluster_name_.c_str(), flags_);
  if (e != 0) {
    return Status::ExecutionError("call to init2() returned non-zero exit code.");
  }

  /// Read the Ceph config file.
  e = rados_interface_->conf_read_file(ceph_config_path_.c_str());
  if (e != 0) {
    return Status::ExecutionError(
        "call to conf_read_file() returned non-zero exit code.");
  }

  /// Connect to the Ceph cluster.
  e = rados_interface_->connect();
  if (e != 0) {
    return Status::ExecutionError("call to connect() returned non-zero exit code.");
  }

  /// Initialize the I/O context to start doing I/O operations on objects.
  e = rados_interface_->ioctx_create(pool_name_.c_str(), io_ctx_interface_);
  if (e != 0) {
    return Status::ExecutionError("call to ioctx_create() returned non-zero exit code.");
  }

  return Status::OK();
}

Status RadosCluster::Disconnect() {
  rados_interface_->shutdown();
  return Status::OK();
}

Result<std::shared_ptr<Dataset>> RadosDataset::ReplaceSchema(
    std::shared_ptr<Schema> schema) const {
  RETURN_NOT_OK(CheckProjectable(*schema_, *schema));
  return RadosDataset::Make(std::move(schema), std::move(fragments_),
                            std::move(cluster_));
}

Result<std::shared_ptr<Dataset>> RadosDataset::Make(
    std::shared_ptr<Schema> schema, RadosFragmentVector fragments,
    std::shared_ptr<RadosCluster> cluster) {
  return std::shared_ptr<Dataset>(new RadosDataset(schema, fragments, cluster));
}

Status RadosDataset::Write(RecordBatchVector& batches, RadosDatasetFactoryOptions options,
                           std::string object_id) {
  ARROW_ASSIGN_OR_RAISE(auto table, Table::FromRecordBatches(batches));

  librados::bufferlist in, out;

  if (options.format_ == 1) RETURN_NOT_OK(SerializeTableToIPCStream(table, in));
  if (options.format_ == 2) RETURN_NOT_OK(SerializeTableToParquetStream(table, in));

  auto cluster =
      std::make_shared<RadosCluster>(options.pool_name_, options.ceph_config_path_);
  cluster->Connect();

  int e = cluster->io_ctx_interface_->exec(object_id, cluster->cls_name_.c_str(), "write",
                                           in, out);
  if (e != 0) {
    return Status::ExecutionError(
        "call to exec() in RadosDataset::Write() returned non-zero exit code.");
  }
  return Status::OK();
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
  librados::bufferlist in, out;

  /// Serialize the filter Expression and projection Schema into
  /// a librados bufferlist.
  ARROW_RETURN_NOT_OK(SerializeScanRequestToBufferlist(
      options_->filter, options_->partition_expression, options_->projector.schema(),
      options_->format, in));

  /// Trigger a CLS function and pass the serialized operations
  /// down to the storage. The resultant Table will be available inside the `out`
  /// bufferlist subsequently.
  int e = cluster_->io_ctx_interface_->exec(object_->id(), cluster_->cls_name_.c_str(),
                                            "scan", in, out);
  if (e != 0) {
    return Status::ExecutionError(
        "call to exec() in RadosScanTask::Execute() returned non-zero exit code.");
  }

  /// Deserialize the result Table from the `out` bufferlist.
  std::shared_ptr<Table> result_table;
  ARROW_RETURN_NOT_OK(DeserializeTableFromBufferlist(&result_table, out));

  /// Verify whether the Schema of the resultant Table is what was asked for,
  if (!options_->schema()->Equals(*(result_table->schema()))) {
    return Status::Invalid(
        "the schema of the result table doesn't match the schema of the requested "
        "projection.");
  }

  /// Read the result Table into a RecordBatchVector to return to the RadosScanTask
  auto table_reader = std::make_shared<TableBatchReader>(*result_table);
  RecordBatchVector batches;
  ARROW_RETURN_NOT_OK(table_reader->ReadAll(&batches));
  return MakeVectorIterator(batches);
}

}  // namespace dataset
}  // namespace arrow
