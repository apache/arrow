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
#include "arrow/table.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/iterator.h"
#include "arrow/util/logging.h"
#include "arrow/util/make_unique.h"

namespace arrow {
namespace dataset {

Status RadosFragment::WriteFragment(RecordBatchVector& batches,
                                    std::shared_ptr<RadosCluster> cluster,
                                    std::shared_ptr<RadosObject> object) {
  ARROW_ASSIGN_OR_RAISE(auto table, Table::FromRecordBatches(batches));

  librados::bufferlist in, out;
  RETURN_NOT_OK(serialize_table_to_bufferlist(table, in));

  int e = cluster->io_ctx_interface_->exec(object->id(), cluster->cls_name_.c_str(),
                                           "write", in, out);
  if (e != 0) {
    return Status::ExecutionError(
        "call to exec() in RadosFragment::WriteFragment() returned non-zero exit code.");
  }

  return Status::OK();
}

Result<ScanTaskIterator> RadosFragment::Scan(std::shared_ptr<ScanOptions> options,
                                             std::shared_ptr<ScanContext> context) {
  ScanTaskVector v{std::make_shared<RadosScanTask>(
      std::move(options), std::move(context), std::move(object_), std::move(cluster_))};
  return MakeVectorIterator(v);
}

Result<std::shared_ptr<Schema>> RadosFragment::ReadPhysicalSchemaImpl() {
  return physical_schema_;
}

Result<std::shared_ptr<DatasetFactory>> RadosDatasetFactory::Make(
    RadosObjectVector objects, std::shared_ptr<RadosCluster> cluster) {
  return std::shared_ptr<DatasetFactory>(
      new RadosDatasetFactory(std::move(objects), std::move(cluster)));
}

Result<std::vector<std::shared_ptr<Schema>>> RadosDatasetFactory::InspectSchemas(
    InspectOptions options) {
  librados::bufferlist in, out;
  int e = cluster_->io_ctx_interface_->exec(objects_[0]->id(),
                                            cluster_->cls_name_.c_str(), "read", in, out);
  if (e != 0) {
    return Status::ExecutionError("call to exec() returned non-zero exit code.");
  }

  /// Deserialize the result Table from the `out` bufferlist.
  std::shared_ptr<Table> table;
  std::vector<std::shared_ptr<Schema>> schemas;
  ARROW_RETURN_NOT_OK(deserialize_table_from_bufferlist(&table, out));
  schemas.push_back(table->schema());
  return schemas;
}

Result<std::shared_ptr<Dataset>> RadosDatasetFactory::Finish(FinishOptions options) {
  InspectOptions inspect_options_;
  ARROW_ASSIGN_OR_RAISE(auto schemas_, InspectSchemas(inspect_options_));
  return std::make_shared<RadosDataset>(schemas_[0], objects_, cluster_);
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
  return std::make_shared<RadosDataset>(std::move(schema), get_objects_,
                                        std::move(cluster_));
}

FragmentIterator RadosDataset::GetFragmentsImpl(std::shared_ptr<Expression>) {
  auto schema = this->schema();
  auto cluster = this->cluster();

  auto create_fragment =
      [schema, cluster](
          std::shared_ptr<RadosObject> object) -> Result<std::shared_ptr<Fragment>> {
    return std::make_shared<RadosFragment>(std::move(schema), std::move(object),
                                           std::move(cluster));
  };

  return MakeMaybeMapIterator(std::move(create_fragment), get_objects_->Get());
}

Result<RecordBatchIterator> RadosScanTask::Execute() {
  librados::bufferlist in, out;

  /// Serialize the filter Expression and projection Schema into
  /// a librados bufferlist.
  ARROW_RETURN_NOT_OK(serialize_scan_request_to_bufferlist(
      options_->filter, options_->projector.schema(), in));

  /// Trigger a CLS function and pass the serialized operations
  /// down to the storage. The resultant Table will be available inside the `out`
  /// bufferlist subsequently.
  int e = cluster_->io_ctx_interface_->exec(object_->id(), cluster_->cls_name_.c_str(),
                                            "scan", in, out);
  if (e != 0) {
    return Status::ExecutionError("call to exec() returned non-zero exit code.");
  }

  /// Deserialize the result Table from the `out` bufferlist.
  std::shared_ptr<Table> result_table;
  ARROW_RETURN_NOT_OK(deserialize_table_from_bufferlist(&result_table, out));

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
