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

#ifndef ARROW_COLUMN_H
#define ARROW_COLUMN_H

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "arrow/type.h"
#include "arrow/util/visibility.h"

namespace arrow {

class Array;
class Status;

typedef std::vector<std::shared_ptr<Array>> ArrayVector;

// A data structure managing a list of primitive Arrow arrays logically as one
// large array
class ARROW_EXPORT ChunkedArray {
 public:
  explicit ChunkedArray(const ArrayVector& chunks);

  // @returns: the total length of the chunked array; computed on construction
  int64_t length() const { return length_; }

  int64_t null_count() const { return null_count_; }

  int num_chunks() const { return static_cast<int>(chunks_.size()); }

  std::shared_ptr<Array> chunk(int i) const { return chunks_[i]; }

  const ArrayVector& chunks() const { return chunks_; }

  bool Equals(const ChunkedArray& other) const;
  bool Equals(const std::shared_ptr<ChunkedArray>& other) const;

 protected:
  ArrayVector chunks_;
  int64_t length_;
  int64_t null_count_;
};

// An immutable column data structure consisting of a field (type metadata) and
// a logical chunked data array (which can be validated as all being the same
// type).
class ARROW_EXPORT Column {
 public:
  Column(const std::shared_ptr<Field>& field, const ArrayVector& chunks);
  Column(const std::shared_ptr<Field>& field, const std::shared_ptr<ChunkedArray>& data);

  Column(const std::shared_ptr<Field>& field, const std::shared_ptr<Array>& data);

  int64_t length() const { return data_->length(); }

  int64_t null_count() const { return data_->null_count(); }

  std::shared_ptr<Field> field() const { return field_; }

  // @returns: the column's name in the passed metadata
  const std::string& name() const { return field_->name; }

  // @returns: the column's type according to the metadata
  std::shared_ptr<DataType> type() const { return field_->type; }

  // @returns: the column's data as a chunked logical array
  std::shared_ptr<ChunkedArray> data() const { return data_; }

  bool Equals(const Column& other) const;
  bool Equals(const std::shared_ptr<Column>& other) const;

  // Verify that the column's array data is consistent with the passed field's
  // metadata
  Status ValidateData();

 protected:
  std::shared_ptr<Field> field_;
  std::shared_ptr<ChunkedArray> data_;
};

}  // namespace arrow

#endif  // ARROW_COLUMN_H
