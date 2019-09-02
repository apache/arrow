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

#include <cstdint>
#include <memory>
#include <vector>

#include "arrow/array.h"
#include "arrow/status.h"
#include "arrow/table.h"
#include "arrow/type.h"
#include "arrow/util/visibility.h"

namespace arrow {
namespace colfmt {

/// \brief Per-field levels and values. Schema fields act as map keys.
class ARROW_EXPORT ColumnMap {
 public:
  struct Column {
    std::shared_ptr<Field> field;
    std::shared_ptr<Int16Array> rep_levels;
    std::shared_ptr<Int16Array> def_levels;
    std::shared_ptr<Array> values;
  };

  /// Replace data associated with the given Column::field
  void Put(const Column& column);

  /// Replace data associated with the given field
  void Put(const std::shared_ptr<Field>& field,
           const std::shared_ptr<Int16Array>& rep_levels,
           const std::shared_ptr<Int16Array>& def_levels,
           const std::shared_ptr<Array>& values = nullptr);

  /// Return number of columns in map
  int size() const { return static_cast<int>(columns_.size()); }

  Result<Column> Get(int i) const;

  Result<Column> Find(const std::shared_ptr<Field>& field) const;

 private:
  std::vector<Column> columns_;
};

/// \class Shredder
/// \brief Shred nested arrays into separate columns.
/// Input arrays may have deeply nested schema consisting of
/// primitives, structs and lists.
/// Unions are currently unsupported.
class ARROW_EXPORT Shredder {
 public:
  /// \brief Create new shredder.
  /// \param[in] schema Schema of the arrays which will be shredded.
  static Result<std::shared_ptr<Shredder>> Create(const std::shared_ptr<Field>& schema,
                                                  MemoryPool* pool);

  ~Shredder();

  const std::shared_ptr<Field>& schema() const;

  /// \brief Shred given nested array into separate columns.
  /// This function may be called multiple times before Finish().
  ///
  /// \param[in] array Array with the schema identical to the schema
  /// shredder was constructed with.
  Status Shred(const Array& array);

  Status Shred(const ChunkedArray& array);

  /// \brief Retrieve accumulated data.
  ///
  /// \return Column data for every field in the schema, including
  /// internal fields. Repetition and definition arrays are returned for all fields.
  /// Value arrays are returned only for fields of primitive types; value arrays
  /// for non-primitive types are returned as NULL.
  Result<ColumnMap> Finish();

 private:
  class ARROW_NO_EXPORT Impl;
  explicit Shredder(std::unique_ptr<Impl> impl);

  std::unique_ptr<Impl> impl_;
};

/// \class Stitcher
/// \brief Given a schema and column data for all leaf nodes, stitch column data
/// into nested array of given schema type.
/// Schema may consist of primitives, structs and lists.
/// Unions are currently unsupported.
class ARROW_EXPORT Stitcher {
 public:
  static Result<std::shared_ptr<Stitcher>> Create(const std::shared_ptr<Field>& schema,
                                                  MemoryPool* pool);

  ~Stitcher();

  const std::shared_ptr<Field>& schema() const;

  /// This method may be called multiple times before Finish()ing.
  ///
  /// \param[in] colmap Column map must contain column data for all leaf fields
  /// of the stitcher schema.
  Status Stitch(const ColumnMap& colmap);

  Result<std::shared_ptr<Array>> Finish();

  /// Print schema nodes, their rep/def levels, and state transition table.
  void DebugPrint(std::ostream& out) const;

 private:
  class ARROW_NO_EXPORT Impl;
  explicit Stitcher(std::unique_ptr<Impl> impl);

  std::unique_ptr<Impl> impl_;
};

}  // namespace colfmt
}  // namespace arrow
