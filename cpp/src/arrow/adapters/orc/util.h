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

#include "arrow/array/builder_base.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/type_fwd.h"
#include "orc/OrcFile.hh"

namespace liborc = orc;

namespace arrow {
namespace adapters {
namespace orc {

/// \brief Bridge between an Arrow Field and an ORC column index.
///
/// Represents a field in the Arrow schema mapped to its corresponding ORC column ID
/// for statistics lookup. ORC uses depth-first pre-order numbering where column 0
/// is the root struct.
struct ARROW_EXPORT OrcSchemaField {
  /// Arrow field definition
  std::shared_ptr<Field> field;
  /// ORC physical column ID (for GetColumnStatistics lookup)
  int orc_column_id;
  /// Child fields for nested types (struct, list, map)
  std::vector<OrcSchemaField> children;
  /// Returns true if this is a leaf field (no children)
  bool is_leaf() const { return children.empty(); }
};

/// \brief Bridge between Arrow schema and ORC schema.
///
/// Maps Arrow field paths to ORC physical column indices for statistics lookup.
struct ARROW_EXPORT OrcSchemaManifest {
  /// Top-level fields (parallel to Arrow schema fields)
  std::vector<OrcSchemaField> schema_fields;

  /// \brief Find the OrcSchemaField for a given Arrow field path.
  ///
  /// Path is a sequence of child indices starting from the root.
  /// For example, path {1, 0} means schema_fields[1].children[0].
  ///
  /// \param[in] path sequence of child indices from root to target field
  /// \return pointer to the field if found, nullptr otherwise
  const OrcSchemaField* GetField(const std::vector<int>& path) const;
};

Result<std::shared_ptr<DataType>> GetArrowType(const liborc::Type* type);

Result<std::unique_ptr<liborc::Type>> GetOrcType(const Schema& schema);

Result<std::shared_ptr<const KeyValueMetadata>> GetFieldMetadata(
    const liborc::Type* type);

Result<std::shared_ptr<Field>> GetArrowField(const std::string& name,
                                             const liborc::Type* type,
                                             bool nullable = true);

ARROW_EXPORT Status AppendBatch(const liborc::Type* type,
                                liborc::ColumnVectorBatch* batch, int64_t offset,
                                int64_t length, arrow::ArrayBuilder* builder);

/// \brief Write a chunked array to an orc::ColumnVectorBatch
///
/// \param[in] chunked_array the chunked array
/// \param[in] length the orc::ColumnVectorBatch size limit
/// \param[in,out] arrow_chunk_offset The current chunk being processed
/// \param[in,out] arrow_index_offset The index of the arrow_chunk_offset array
/// before or after a process
/// \param[in,out] column_vector_batch the orc::ColumnVectorBatch to be filled
/// \return Status
ARROW_EXPORT Status WriteBatch(const ChunkedArray& chunked_array, int64_t length,
                               int* arrow_chunk_offset, int64_t* arrow_index_offset,
                               liborc::ColumnVectorBatch* column_vector_batch);

/// \brief Get the major version provided by the official ORC C++ library.
ARROW_EXPORT int GetOrcMajorVersion();

}  // namespace orc
}  // namespace adapters
}  // namespace arrow
