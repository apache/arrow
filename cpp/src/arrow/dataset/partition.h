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

#include <functional>
#include <memory>
#include <regex>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "arrow/dataset/dataset.h"
#include "arrow/dataset/filter.h"
#include "arrow/dataset/type_fwd.h"
#include "arrow/dataset/visibility.h"

namespace arrow {

namespace fs {
struct FileStats;
}

namespace dataset {

// ----------------------------------------------------------------------
// Partition schemes

struct ARROW_DS_EXPORT UnconvertedKey {
  std::string name, value;
};

/// \brief Helper function for the common case of combining partition information
/// consisting of equality expressions into a single conjunction expression.
/// Fields referenced in keys but absent from schema will be ignored.
ARROW_DS_EXPORT
Result<std::shared_ptr<Expression>> ConvertPartitionKeys(
    const std::vector<UnconvertedKey>& keys, const Schema& schema);

/// \brief Interface for parsing partition expressions from string partition
/// identifiers.
///
/// For example, the identifier "foo=5" might be parsed to an equality expression
/// between the "foo" field and the value 5.
///
/// Some partition schemes may store the field names in a metadata
/// store instead of in file paths, for example
/// dataset_root/2009/11/... could be used when the partition fields
/// are "year" and "month"
///
/// Paths are consumed from left to right. Paths must be relative to
/// the root of a partition; path prefixes must be removed before passing
/// the path to a scheme for parsing.
class ARROW_DS_EXPORT PartitionScheme {
 public:
  virtual ~PartitionScheme() = default;

  /// \brief The name identifying the kind of partition scheme
  virtual std::string name() const = 0;

  /// \brief Parse a path into a partition expression
  ///
  /// \param[in] path the partition identifier to parse
  /// \return the parsed expression
  virtual Result<std::shared_ptr<Expression>> Parse(const std::string& path) const = 0;

  /// \brief Status return + out arg overload
  Status Parse(const std::string& path, std::shared_ptr<Expression>* out) const {
    return Parse(path).Value(out);
  }
};

/// \brief Trivial partition scheme which yields an expression provided on construction.
class ARROW_DS_EXPORT ConstantPartitionScheme : public PartitionScheme {
 public:
  explicit ConstantPartitionScheme(std::shared_ptr<Expression> expr)
      : expression_(std::move(expr)) {}

  std::string name() const override { return "constant_partition_scheme"; }

  Result<std::shared_ptr<Expression>> Parse(const std::string& path) const override;

 private:
  std::shared_ptr<Expression> expression_;
};

/// \brief SchemaPartitionScheme parses one segment of a path for each field in its
/// schema. All fields are required, so paths passed to SchemaPartitionScheme::Parse
/// must contain segments for each field.
///
/// For example given schema<year:int16, month:int8> the path "/2009/11" would be
/// parsed to ("year"_ == 2009 and "month"_ == 11)
class ARROW_DS_EXPORT SchemaPartitionScheme : public PartitionScheme {
 public:
  explicit SchemaPartitionScheme(std::shared_ptr<Schema> schema)
      : schema_(std::move(schema)) {}

  std::string name() const override { return "schema_partition_scheme"; }

  Result<std::shared_ptr<Expression>> Parse(const std::string& path) const override;

  const std::shared_ptr<Schema>& schema() { return schema_; }

 protected:
  std::shared_ptr<Schema> schema_;
};

/// \brief Multi-level, directory based partitioning scheme
/// originating from Apache Hive with all data files stored in the
/// leaf directories. Data is partitioned by static values of a
/// particular column in the schema. Partition keys are represented in
/// the form $key=$value in directory names.
/// Field order is ignored, as are missing or unrecognized field names.
///
/// For example given schema<year:int16, month:int8, day:int8> the path
/// "/day=321/ignored=3.4/year=2009" parses to ("year"_ == 2009 and "day"_ == 321)
class ARROW_DS_EXPORT HivePartitionScheme : public PartitionScheme {
 public:
  explicit HivePartitionScheme(std::shared_ptr<Schema> schema)
      : schema_(std::move(schema)) {}

  std::string name() const override { return "hive_partition_scheme"; }

  Result<std::shared_ptr<Expression>> Parse(const std::string& path) const override;

  std::vector<UnconvertedKey> GetUnconvertedKeys(const std::string& path) const;

  const std::shared_ptr<Schema>& schema() { return schema_; }

 protected:
  std::shared_ptr<Schema> schema_;
};

/// \brief Implementation provided by lambda or other callable
class ARROW_DS_EXPORT FunctionPartitionScheme : public PartitionScheme {
 public:
  explicit FunctionPartitionScheme(
      std::function<Result<std::shared_ptr<Expression>>(const std::string&)> impl,
      std::string name = "function_partition_scheme")
      : impl_(std::move(impl)), name_(std::move(name)) {}

  std::string name() const override { return name_; }

  Result<std::shared_ptr<Expression>> Parse(const std::string& path) const override {
    return impl_(path);
  }

 private:
  std::function<Result<std::shared_ptr<Expression>>(const std::string&)> impl_;
  std::string name_;
};

/// \brief Mapping from path to partition expressions.
using PathPartitions = std::unordered_map<std::string, std::shared_ptr<Expression>>;

Status ApplyPartitionScheme(const PartitionScheme& scheme,
                            std::vector<fs::FileStats> files, PathPartitions* out);

// TODO(bkietz) use RE2 and named groups to provide RegexpPartitionScheme

}  // namespace dataset
}  // namespace arrow
