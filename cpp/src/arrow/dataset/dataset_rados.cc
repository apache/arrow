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

std::shared_ptr<RadosOptions> RadosOptions::FromPoolName(std::string pool_name) {
  std::shared_ptr<RadosOptions> options = std::make_shared<RadosOptions>();
  options->pool_name_ = pool_name;
  options->user_name_ = "client.admin";
  options->cluster_name_ = "ceph";
  options->flags_ = 0;
  options->ceph_config_path_ = "/etc/ceph/ceph.conf";
  options->cls_name_ = "arrow";
  options->cls_method_ = "read_and_scan";
  options->rados_interface_ = new RadosWrapper();
  options->io_ctx_interface_ = new IoCtxWrapper();
  return options;
}

Result<ScanTaskIterator> RadosFragment::Scan(std::shared_ptr<ScanOptions> options,
                                             std::shared_ptr<ScanContext> context) {
  ScanTaskVector v{std::make_shared<RadosScanTask>(std::move(options), std::move(context),
                                                   std::move(object_),
                                                   std::move(rados_options_))};

  return MakeVectorIterator(v);
}

Result<std::shared_ptr<Schema>> RadosFragment::ReadPhysicalSchemaImpl() {
  return physical_schema_;
}

struct VectorObjectGenerator : RadosDataset::RadosObjectGenerator {
  explicit VectorObjectGenerator(RadosObjectVector objects)
      : objects_(std::move(objects)) {}

  RadosObjectIterator Get() const final { return MakeVectorIterator(objects_); }

  RadosObjectVector objects_;
};

RadosDataset::RadosDataset(std::shared_ptr<Schema> schema, RadosObjectVector objects,
                           std::shared_ptr<RadosOptions> rados_options)
    : Dataset(std::move(schema)),
      get_objects_(new VectorObjectGenerator(std::move(objects))),
      rados_options_(std::move(rados_options)) {
  ARROW_CHECK_OK(this->Connect());
}

RadosDataset::~RadosDataset() { ARROW_CHECK_OK(this->Shutdown()); }

Status RadosDataset::Connect() {
  int e;
  /// Initialize the cluster handle.
  e = rados_options_->rados_interface_->init2(rados_options_->user_name_.c_str(),
                                              rados_options_->cluster_name_.c_str(),
                                              rados_options_->flags_);
  if (e != 0) {
    return Status::ExecutionError("call to init2() returned non-zero exit code.");
  }

  /// Read the Ceph config file.
  e = rados_options_->rados_interface_->conf_read_file(
      rados_options_->ceph_config_path_.c_str());
  if (e != 0) {
    return Status::ExecutionError(
        "call to conf_read_file() returned non-zero exit code.");
  }

  /// Connect to the Ceph cluster.
  e = rados_options_->rados_interface_->connect();
  if (e != 0) {
    return Status::ExecutionError("call to connect() returned non-zero exit code.");
  }

  /// Initialize the I/O context to start doing I/O operations on objects.
  e = rados_options_->rados_interface_->ioctx_create(rados_options_->pool_name_.c_str(),
                                                     rados_options_->io_ctx_interface_);
  if (e != 0) {
    return Status::ExecutionError("call to ioctx_create() returned non-zero exit code.");
  }

  return Status::OK();
}

Status RadosDataset::Shutdown() {
  rados_options_->rados_interface_->shutdown();
  return Status::OK();
}

Result<std::shared_ptr<Dataset>> RadosDataset::ReplaceSchema(
    std::shared_ptr<Schema> schema) const {
  RETURN_NOT_OK(CheckProjectable(*schema_, *schema));
  return std::make_shared<RadosDataset>(std::move(schema), get_objects_,
                                        std::move(rados_options_));
}

FragmentIterator RadosDataset::GetFragmentsImpl(std::shared_ptr<Expression>) {
  auto schema = this->schema();
  auto rados_options = this->rados_options();

  auto create_fragment =
      [schema, rados_options](
          std::shared_ptr<RadosObject> object) -> Result<std::shared_ptr<Fragment>> {
    return std::make_shared<RadosFragment>(std::move(schema), std::move(object),
                                           std::move(rados_options));
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
  int e = rados_options_->io_ctx_interface_->exec(
      object_->id(), rados_options_->cls_name_.c_str(),
      rados_options_->cls_method_.c_str(), in, out);
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
