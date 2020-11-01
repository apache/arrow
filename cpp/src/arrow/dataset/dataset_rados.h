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
#include "arrow/dataset/rados.h"
#include "arrow/dataset/scanner.h"

namespace arrow {
namespace dataset {

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

/// \brief Store configuration for connecting to a RADOS backend and
/// the CLS library and functions to invoke. Also, holds pointers to
/// a RadosInterface and IoCtxInterface instance.
struct ARROW_DS_EXPORT RadosOptions {
  std::string pool_name_;
  std::string user_name_;
  std::string cluster_name_;
  std::string ceph_config_path_;
  uint64_t flags_;
  std::string cls_name_;
  std::string cls_method_;

  RadosInterface* rados_interface_;
  IoCtxInterface* io_ctx_interface_;

  /// \brief Creates a RadosOptions instance with default values and a pool name.
  ///
  /// \param[in] pool_name the RADOS pool to connect to.
  static std::shared_ptr<RadosOptions> FromPoolName(std::string pool_name);
};

/// \brief A Fragment that maps to an object stored in the Ceph object store.
class ARROW_DS_EXPORT RadosFragment : public Fragment {
 public:
  /// \brief Construct a RadosFragment instance.
  ///
  /// \param[in] schema the schema of the Table stored in an object.
  /// to which this Fragment maps to.
  /// \param[in] object the RadosObject that this Fragment wraps.
  /// \param[in] rados_options the connection information to the RADOS interface.
  RadosFragment(std::shared_ptr<Schema> schema, std::shared_ptr<RadosObject> object,
                std::shared_ptr<RadosOptions> rados_options)
      : Fragment(scalar(true), std::move(schema)),
        object_(std::move(object)),
        rados_options_(std::move(rados_options)) {}

  Result<ScanTaskIterator> Scan(std::shared_ptr<ScanOptions> options,
                                std::shared_ptr<ScanContext> context) override;

  std::string type_name() const override { return "rados"; }

  bool splittable() const override { return false; }

 protected:
  Result<std::shared_ptr<Schema>> ReadPhysicalSchemaImpl() override;
  std::shared_ptr<RadosObject> object_;
  std::shared_ptr<RadosOptions> rados_options_;
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
  /// \param[in] rados_options the connection information to the RADOS interface.
  RadosDataset(std::shared_ptr<Schema> schema,
               std::shared_ptr<RadosObjectGenerator> get_objects,
               std::shared_ptr<RadosOptions> rados_options)
      : Dataset(std::move(schema)),
        get_objects_(std::move(get_objects)),
        rados_options_(std::move(rados_options)) {}

  /// \brief Constructs a RadosDataset wrapping RadosObjects and
  /// connects to a RADOS cluster.
  ///
  /// \param[in] schema the schema of the tables referred to by the dataset.
  /// \param[in] objects a vector of RadosObjects that comprise a RadosDataset.
  /// \param[in] rados_options the connection information to the RADOS interface.
  RadosDataset(std::shared_ptr<Schema> schema, RadosObjectVector objects,
               std::shared_ptr<RadosOptions> rados_options);

  /// \brief The RadosDataset destructor destroys the RadosDataset
  /// and shutdowns the connection to the RADOS cluster.
  ~RadosDataset();

  const std::shared_ptr<Schema>& schema() const { return schema_; }

  /// \brief Returns the RadosOptions for this Dataset.
  const std::shared_ptr<RadosOptions>& rados_options() const { return rados_options_; }

  std::string type_name() const override { return "rados"; }

  Result<std::shared_ptr<Dataset>> ReplaceSchema(
      std::shared_ptr<Schema> schema) const override;

 protected:
  /// \brief Generates RadosFragments from the Dataset.
  FragmentIterator GetFragmentsImpl(
      std::shared_ptr<Expression> predicate = scalar(true)) override;
  std::shared_ptr<RadosObjectGenerator> get_objects_;
  std::shared_ptr<RadosOptions> rados_options_;

  /// \brief Connect to the Rados cluster.
  Status Connect();

  /// \brief Shutdown the connection to the Rados cluster.
  Status Shutdown();
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
  /// \param[in] rados_options the connection information to the RADOS interface.
  RadosScanTask(std::shared_ptr<ScanOptions> options,
                std::shared_ptr<ScanContext> context, std::shared_ptr<RadosObject> object,
                std::shared_ptr<RadosOptions> rados_options)
      : ScanTask(std::move(options), std::move(context)),
        object_(std::move(object)),
        rados_options_(rados_options) {}

  Result<RecordBatchIterator> Execute();

 protected:
  std::shared_ptr<RadosObject> object_;
  std::shared_ptr<RadosOptions> rados_options_;
};

}  // namespace dataset
}  // namespace arrow
