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

/// Public API for the "Feather" file format, originally created at
/// http://github.com/wesm/feather

#ifndef ARROW_IPC_FEATHER_INTERNAL_H
#define ARROW_IPC_FEATHER_INTERNAL_H

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "flatbuffers/flatbuffers.h"

#include "arrow/buffer.h"
#include "arrow/ipc/feather.h"
#include "arrow/ipc/feather_generated.h"
#include "arrow/type.h"

namespace arrow {
namespace ipc {
namespace feather {

typedef std::vector<flatbuffers::Offset<fbs::Column>> ColumnVector;
typedef flatbuffers::FlatBufferBuilder FBB;
typedef flatbuffers::Offset<flatbuffers::String> FBString;

struct ColumnType {
  enum type { PRIMITIVE, CATEGORY, TIMESTAMP, DATE, TIME };
};

struct ArrayMetadata {
  ArrayMetadata() {}

  ArrayMetadata(fbs::Type type, int64_t offset, int64_t length, int64_t null_count,
      int64_t total_bytes)
      : type(type),
        offset(offset),
        length(length),
        null_count(null_count),
        total_bytes(total_bytes) {}

  bool Equals(const ArrayMetadata& other) const {
    return this->type == other.type && this->offset == other.offset &&
           this->length == other.length && this->null_count == other.null_count &&
           this->total_bytes == other.total_bytes;
  }

  fbs::Type type;
  int64_t offset;
  int64_t length;
  int64_t null_count;
  int64_t total_bytes;
};

struct CategoryMetadata {
  ArrayMetadata levels;
  bool ordered;
};

struct TimestampMetadata {
  TimeUnit unit;

  // A timezone name known to the Olson timezone database. For display purposes
  // because the actual data is all UTC
  std::string timezone;
};

struct TimeMetadata {
  TimeUnit unit;
};

static constexpr const char* kFeatherMagicBytes = "FEA1";
static constexpr const int kFeatherDefaultAlignment = 8;

class ColumnBuilder;

class TableBuilder {
 public:
  explicit TableBuilder(int64_t num_rows);
  ~TableBuilder() = default;

  FBB& fbb();
  Status Finish();
  std::shared_ptr<Buffer> GetBuffer() const;

  std::unique_ptr<ColumnBuilder> AddColumn(const std::string& name);
  void SetDescription(const std::string& description);
  void SetNumRows(int64_t num_rows);
  void add_column(const flatbuffers::Offset<fbs::Column>& col);

 private:
  flatbuffers::FlatBufferBuilder fbb_;
  ColumnVector columns_;

  friend class ColumnBuilder;

  bool finished_;
  std::string description_;
  int64_t num_rows_;
};

class TableMetadata {
 public:
  TableMetadata() {}
  ~TableMetadata() = default;

  Status Open(const std::shared_ptr<Buffer>& buffer) {
    metadata_buffer_ = buffer;
    table_ = fbs::GetCTable(buffer->data());

    if (table_->version() < kFeatherVersion) {
      std::cout << "This Feather file is old"
                << " and will not be readable beyond the 0.3.0 release" << std::endl;
    }
    return Status::OK();
  }

  bool HasDescription() const { return table_->description() != 0; }

  std::string GetDescription() const {
    if (!HasDescription()) { return std::string(""); }
    return table_->description()->str();
  }

  int version() const { return table_->version(); }
  int64_t num_rows() const { return table_->num_rows(); }
  int64_t num_columns() const { return table_->columns()->size(); }

  const fbs::Column* column(int i) { return table_->columns()->Get(i); }

 private:
  std::shared_ptr<Buffer> metadata_buffer_;
  const fbs::CTable* table_;
};

static inline flatbuffers::Offset<fbs::PrimitiveArray> GetPrimitiveArray(
    FBB& fbb, const ArrayMetadata& array) {
  return fbs::CreatePrimitiveArray(fbb, array.type, fbs::Encoding_PLAIN, array.offset,
      array.length, array.null_count, array.total_bytes);
}

static inline fbs::TimeUnit ToFlatbufferEnum(TimeUnit unit) {
  return static_cast<fbs::TimeUnit>(static_cast<int>(unit));
}

static inline TimeUnit FromFlatbufferEnum(fbs::TimeUnit unit) {
  return static_cast<TimeUnit>(static_cast<int>(unit));
}

// Convert Feather enums to Flatbuffer enums

const fbs::TypeMetadata COLUMN_TYPE_ENUM_MAPPING[] = {
    fbs::TypeMetadata_NONE,               // PRIMITIVE
    fbs::TypeMetadata_CategoryMetadata,   // CATEGORY
    fbs::TypeMetadata_TimestampMetadata,  // TIMESTAMP
    fbs::TypeMetadata_DateMetadata,       // DATE
    fbs::TypeMetadata_TimeMetadata        // TIME
};

static inline fbs::TypeMetadata ToFlatbufferEnum(ColumnType::type column_type) {
  return COLUMN_TYPE_ENUM_MAPPING[column_type];
}

static inline void FromFlatbuffer(const fbs::PrimitiveArray* values, ArrayMetadata* out) {
  out->type = values->type();
  out->offset = values->offset();
  out->length = values->length();
  out->null_count = values->null_count();
  out->total_bytes = values->total_bytes();
}

class ColumnBuilder {
 public:
  ColumnBuilder(TableBuilder* parent, const std::string& name);
  ~ColumnBuilder() = default;

  flatbuffers::Offset<void> CreateColumnMetadata();

  Status Finish();
  void SetValues(const ArrayMetadata& values);
  void SetUserMetadata(const std::string& data);
  void SetCategory(const ArrayMetadata& levels, bool ordered = false);
  void SetTimestamp(TimeUnit unit);
  void SetTimestamp(TimeUnit unit, const std::string& timezone);
  void SetDate();
  void SetTime(TimeUnit unit);
  FBB& fbb();

 private:
  TableBuilder* parent_;

  std::string name_;
  ArrayMetadata values_;
  std::string user_metadata_;

  // Column metadata

  // Is this a primitive type, or one of the types having metadata? Default is
  // primitive
  ColumnType::type type_;

  // Type-specific metadata union
  CategoryMetadata meta_category_;
  TimeMetadata meta_time_;

  TimestampMetadata meta_timestamp_;

  FBB* fbb_;
};

}  // namespace feather
}  // namespace ipc
}  // namespace arrow

#endif  // ARROW_IPC_FEATHER_INTERNAL_H
