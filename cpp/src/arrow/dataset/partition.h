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
#pragma once

#include "arrow/dataset/dataset.h"
#include "arrow/dataset/type_fwd.h"
#include "arrow/dataset/visibility.h"

namespace arrow {
namespace dataset {

// ----------------------------------------------------------------------
// Computing partition values

// TODO(wesm): API for computing partition keys derived from raw
// values. For example, year(value) instead of simply value, so a
// dataset with a timestamp column might group all data with year 2009
// in the same partition

// /// \brief
// class ScalarTransform {
//  public:
//   virtual Status Transform(const std::shared_ptr<Scalar>& input,
//                            std::shared_ptr<Scalar>* output) const = 0;
// };

// class PartitionField {
//  public:

//  private:
//   std::string field_name_;
// };

// ----------------------------------------------------------------------
// Partition identifiers

/// \brief
class PartitionKey {
 public:

};

/// \brief Intermediate data structure for data parsed from a string
/// partition identifier.
///
/// For example, the identifier "foo=5" might be parsed with a single
/// "foo" field and the value 5. A more complex identifier might be
/// written as "foo=5,bar=2", which would yield two fields and two
/// values
struct PartitionKeyComponents {
  std::vector<std::string> fields;
  std::vector<std::shared_ptr<Scalar>> values;
};

// ----------------------------------------------------------------------
// Partition schemes

/// \brief
class PartitionScheme {
 public:
  virtual ~PartitionScheme() = default;

  /// \brief The name identifying the kind of partition scheme
  virtual std::string name() const = 0;

  virtual bool DirectoryMatches(const std::string& path) const = 0;

  virtual Status ParseDirectory(const std::string& path,
                                PartitionKeyComponents* out) const = 0;
};

/// \brief
class HivePartitionScheme : public PartitionScheme {
 public:


};

// ----------------------------------------------------------------------
//

/// \brief Container for a dataset partition, which consists of a
/// partition identifier, subpartitions, and some data fragments
class ARROW_DS_EXPORT SimplePartition : public Partition{
 public:
  const PartitionKey& key() const { return *key_; }

 private:
  std::unique_ptr<PartitionKey> key_;

  /// \brief Child partitions of this partition. In some partition
  /// schemes, this member is mutually-exclusive with
  std::vector<std::shared_ptr<Partition>> subpartitions_;

  std::vector<std::shared_ptr<DataFragment>> data_fragments_;
};

/// \brief DataSource implementation for partition-based data sources
class PartitionSource : public DataSource {
 public:
  std::unique_ptr<DataFragmentIterator> GetFragments() override;

 protected:
  std::vector<std::shared_ptr<Partition>> partitions_;
};

}  // namespace dataset
}  // namespace arrow
