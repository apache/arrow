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

class ARROW_DS_EXPORT RadosDatasetFactoryOptions {
 public:
  std::vector<std::string> objects_;
  std::string pool_name_;
  std::string user_name_;
  std::string cluster_name_;
  std::string ceph_config_path_;
  uint64_t flags_;
  std::string cls_name_;
};

/// \brief An abstraction to encapsulate information about a
/// RADOS object. Currently, it holds only the object ID.
class ARROW_DS_EXPORT RadosObject {
 public:
  /// \brief Constructs a RadosObject.
  /// \param[in] id the object ID.
  explicit RadosObject(std::string id) : id_(id) {}

  /// \brief Return the object ID.
  std::string id() const { return id_; }

 protected:
  std::string id_;
};

/// \brief A vector of RadosObjects.
using RadosObjectVector = std::vector<std::shared_ptr<RadosObject>>;
/// \brief An iterator over the RadosObjectVector.
using RadosObjectIterator = Iterator<std::shared_ptr<RadosObject>>;

/// \brief Store configuration for connecting to a RADOS cluster and
/// the CLS library and functions to invoke. Also, holds pointers to
/// a RadosInterface and IoCtxInterface instance.
///
/// auto cluster = new RadosCluster("test-pool");
/// cluster->Connect();
/// cluster->Disconnect();
///
/// After the cluster handle is initialized pass it around.
///
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

/// \brief A Fragment that maps to an object stored in the Ceph object store.
class ARROW_DS_EXPORT RadosFragment : public Fragment {
 public:
  /// \brief Construct a RadosFragment instance.
  ///
  /// \param[in] schema the schema of the Table stored in an object.
  /// to which this Fragment maps to.
  /// \param[in] object the RadosObject that this Fragment wraps.
  /// \param[in] cluster the connection information to the RADOS interface.
  RadosFragment(std::shared_ptr<Schema> schema, std::shared_ptr<RadosObject> object,
                std::shared_ptr<RadosCluster> cluster)
      : Fragment(scalar(true), std::move(schema)),
        object_(std::move(object)),
        cluster_(std::move(cluster)) {}

  Result<ScanTaskIterator> Scan(std::shared_ptr<ScanOptions> options,
                                std::shared_ptr<ScanContext> context) override;

  std::string type_name() const override { return "rados"; }

  bool splittable() const override { return false; }

  /// \brief Write data to a Fragment.
  ///
  /// \param[in] batches the vector of RecordBatches to write.
  /// \param[in] cluster the connection information to the RADOS cluster.
  /// \param[in] object a RadosObject instance containing information about the object to
  /// write.
  static Status WriteFragment(RecordBatchVector& batches,
                              std::shared_ptr<RadosCluster> cluster,
                              std::shared_ptr<RadosObject> object);

 protected:
  Result<std::shared_ptr<Schema>> ReadPhysicalSchemaImpl() override;
  std::shared_ptr<RadosObject> object_;
  std::shared_ptr<RadosCluster> cluster_;
};

/// \brief A Dataset to wrap a vector of RadosObjects and generate
/// RadosFragments out of them.
class ARROW_DS_EXPORT RadosDataset : public Dataset {
 public:
  class RadosObjectGenerator {
   public:
    virtual ~RadosObjectGenerator() = default;
    virtual RadosObjectIterator Get() const = 0;
  };

  struct VectorObjectGenerator : RadosObjectGenerator {
    explicit VectorObjectGenerator(RadosObjectVector objects)
        : objects_(std::move(objects)) {}

    RadosObjectIterator Get() const final { return MakeVectorIterator(objects_); }

    RadosObjectVector objects_;
  };

  /// \brief Construct a RadosDataset.
  ///
  /// A RadosDataset is a logical view of a set of objects stored in
  /// a RADOS cluster which contains partitions of a Table in the
  /// form of a vector of RecordBatches. Upon calling Scan on a RadosDataset,
  /// the filter Expression and projection Schema is pushed down to the CLS
  /// where they are applied on an InMemoryFragment wrapping an object containing
  /// a table partition.
  ///
  /// \param[in] schema the schema of the tables referred to by the dataset.
  /// \param[in] get_objects a generator to yield RadosObjects from a RadosObjectVector.
  /// \param[in] cluster the connection information to the RADOS interface.
  RadosDataset(std::shared_ptr<Schema> schema,
               std::shared_ptr<RadosObjectGenerator> get_objects,
               std::shared_ptr<RadosCluster> cluster)
      : Dataset(std::move(schema)),
        get_objects_(std::move(get_objects)),
        cluster_(std::move(cluster)) {}

  /// \brief Constructs a RadosDataset wrapping RadosObjects and
  /// connects to a RADOS cluster.
  ///
  /// \param[in] schema the schema of the tables referred to by the dataset.
  /// \param[in] objects a vector of RadosObjects that comprise a RadosDataset.
  /// \param[in] cluster the connection information to the RADOS interface.
  RadosDataset(std::shared_ptr<Schema> schema, RadosObjectVector objects,
               std::shared_ptr<RadosCluster> cluster)
      : Dataset(std::move(schema)),
        get_objects_(new VectorObjectGenerator(std::move(objects))),
        cluster_(std::move(cluster)) {}

  /// \brief Create a RadosDataset
  ///
  static Result<std::shared_ptr<Dataset>> Make(RadosDatasetFactoryOptions factory_option);

  const std::shared_ptr<Schema>& schema() const { return schema_; }

  /// \brief Returns the cluster handle for this Dataset.
  const std::shared_ptr<RadosCluster>& cluster() const { return cluster_; }

  std::string type_name() const override { return "rados"; }

  Result<std::shared_ptr<Dataset>> ReplaceSchema(
      std::shared_ptr<Schema> schema) const override;

 protected:
  /// \brief Generates RadosFragments from the Dataset.
  FragmentIterator GetFragmentsImpl(
      std::shared_ptr<Expression> predicate = scalar(true)) override;
  std::shared_ptr<RadosObjectGenerator> get_objects_;
  std::shared_ptr<RadosCluster> cluster_;
};

/// \brief A ScanTask to push down operations to the CLS for
/// performing an InMemory Scan of a RadosObject.
class ARROW_DS_EXPORT RadosScanTask : public ScanTask {
 public:
  /// \brief Construct a RadosScanTask object.
  ///
  /// \param[in] options the ScanOptions.
  /// \param[in] context the ScanContext.
  /// \param[in] object the RadosObject to apply the operations.
  /// \param[in] cluster the connection information to the RADOS interface.
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

/// \brief A factory to create a RadosDataset from a vector of RadosObjects.
///
/// The factory takes a vector of RadosObjects and infers the schema of the Table
/// stored in the objects by scanning the first object in the list.
class ARROW_DS_EXPORT RadosDatasetFactory : public DatasetFactory {
 public:
  /// \brief Build a RadosDataset from a vector of RadosObjects.
  ///
  /// \param[in] objects a vector of RadosObjects.
  /// \param[in] cluster the connection information to the RADOS cluster.
  static Result<std::shared_ptr<DatasetFactory>> Make(
      RadosObjectVector objects, std::shared_ptr<RadosCluster> cluster);

  Result<std::vector<std::shared_ptr<Schema>>> InspectSchemas(InspectOptions options);

  Result<std::shared_ptr<Dataset>> Finish(FinishOptions options) override;

 protected:
  RadosDatasetFactory(RadosObjectVector objects, std::shared_ptr<RadosCluster> cluster)
      : objects_(objects), cluster_(std::move(cluster)) {}
  RadosObjectVector objects_;
  std::shared_ptr<RadosCluster> cluster_;
};

}  // namespace dataset
}  // namespace arrow
