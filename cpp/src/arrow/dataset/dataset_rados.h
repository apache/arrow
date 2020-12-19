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

#pragma once

#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "arrow/dataset/dataset.h"
#include "arrow/dataset/discovery.h"
#include "arrow/dataset/rados.h"
#include "arrow/dataset/scanner.h"
#include "arrow/util/iterator.h"

namespace arrow {
namespace dataset {

class ARROW_DS_EXPORT RadosCluster {
 public:
  RadosCluster(std::string pool_name, std::string ceph_config_path) {
    pool_name_ = pool_name;
    user_name_ = "client.admin";
    cluster_name_ = "ceph";
    flags_ = 0;
    ceph_config_path_ = ceph_config_path;
    cls_name_ = "arrow";
    rados_interface_ = new RadosWrapper();
    io_ctx_interface_ = new IoCtxWrapper();
  }

  Status Connect();

  Status Disconnect();

  std::string pool_name_;
  std::string user_name_;
  std::string cluster_name_;
  std::string ceph_config_path_;
  uint64_t flags_;
  std::string cls_name_;

  RadosInterface* rados_interface_;
  IoCtxInterface* io_ctx_interface_;
};

class ARROW_DS_EXPORT RadosDatasetFactoryOptions : public FileSystemFactoryOptions {
 public:
  std::string pool_name_;
  std::string user_name_;
  std::string cluster_name_;
  std::string ceph_config_path_;
  uint64_t flags_;
  std::string cls_name_;
};

class ARROW_DS_EXPORT RadosObject {
 public:

  explicit RadosObject(std::string id) : id_(id) {}

  std::string id() const { return id_; }

 protected:
  std::string id_;
};

class ARROW_DS_EXPORT RadosFragment : public Fragment {
 public:
  RadosFragment(std::shared_ptr<Schema> schema, 
                std::shared_ptr<RadosObject> object,
                std::shared_ptr<RadosCluster> cluster,
                std::shared_ptr<Expression> partition_expression = scalar(true))
      : Fragment(std::move(partition_expression), std::move(schema)),
        object_(std::move(object)),
        cluster_(std::move(cluster)) {}

  Result<ScanTaskIterator> Scan(std::shared_ptr<ScanOptions> options,
                                std::shared_ptr<ScanContext> context) override;

  std::string type_name() const override { return "rados"; }

  bool splittable() const override { return false; }

 protected:
  Result<std::shared_ptr<Schema>> ReadPhysicalSchemaImpl() override;
  std::shared_ptr<RadosObject> object_;
  std::shared_ptr<RadosCluster> cluster_;
};

using RadosObjectVector = std::vector<std::shared_ptr<RadosObject>>;
using RadosObjectIterator = Iterator<std::shared_ptr<RadosObject>>;
using RadosFragmentVector = std::vector<std::shared_ptr<RadosFragment>>;

class ARROW_DS_EXPORT RadosDataset : public Dataset {
 public:
  static Result<std::shared_ptr<Dataset>> Make(std::shared_ptr<Schema> schema, 
                                               RadosFragmentVector fragments,
                                               std::shared_ptr<RadosCluster> cluster);

  /// TODO: Needs refactoring
  static Status Write(RecordBatchVector& batches,
                      RadosDatasetFactoryOptions options, 
                      std::string object_id);

  const std::shared_ptr<Schema>& schema() const { return schema_; }

  const std::shared_ptr<RadosCluster>& cluster() const { return cluster_; }

  std::string type_name() const override { return "rados"; }

  Result<std::shared_ptr<Dataset>> ReplaceSchema(
      std::shared_ptr<Schema> schema) const override;

 protected:
  RadosDataset(std::shared_ptr<Schema> schema, 
                RadosFragmentVector fragments,
                std::shared_ptr<RadosCluster> cluster)
        : Dataset(std::move(schema)),
          fragments_(fragments),
          cluster_(std::move(cluster)) {}

  FragmentIterator GetFragmentsImpl(
      std::shared_ptr<Expression> predicate = scalar(true)) override;
  
  RadosFragmentVector fragments_;
  std::shared_ptr<RadosCluster> cluster_;
};

class ARROW_DS_EXPORT RadosScanTask : public ScanTask {
 public:
  RadosScanTask(std::shared_ptr<ScanOptions> options,
                std::shared_ptr<ScanContext> context, std::shared_ptr<RadosObject> object,
                std::shared_ptr<RadosCluster> cluster)
      : ScanTask(std::move(options), std::move(context)),
        object_(std::move(object)),
        cluster_(cluster) {}

  Result<RecordBatchIterator> Execute();

 protected:
  std::shared_ptr<RadosObject> object_;
  std::shared_ptr<RadosCluster> cluster_;
};

class ARROW_DS_EXPORT RadosDatasetFactory : public DatasetFactory {
 public:
  static Result<std::shared_ptr<DatasetFactory>> Make(
      RadosDatasetFactoryOptions options);

  static Result<std::shared_ptr<DatasetFactory>> Make(
      RadosObjectVector objects, RadosDatasetFactoryOptions options);

  Result<std::vector<std::shared_ptr<Schema>>> InspectSchemas(InspectOptions options);

  Result<std::shared_ptr<Dataset>> Finish(FinishOptions options) override;

 protected:
  RadosDatasetFactory(RadosObjectVector objects, std::shared_ptr<RadosCluster> cluster, RadosDatasetFactoryOptions options)
      : objects_(std::move(objects)), cluster_(std::move(cluster)), options_(std::move(options)) {}
  
  RadosObjectVector objects_;
  std::shared_ptr<RadosCluster> cluster_;
  RadosDatasetFactoryOptions options_;
};

}  // namespace dataset
}  // namespace arrow
