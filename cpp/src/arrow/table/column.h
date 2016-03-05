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

#ifndef ARROW_TABLE_COLUMN_H
#define ARROW_TABLE_COLUMN_H

#include <memory>
#include <string>
#include <vector>

#include "arrow/array.h"
#include "arrow/field.h"

namespace arrow {

typedef std::vector<std::shared_ptr<Array> > ArrayVector;

// A data structure managing a list of primitive Arrow arrays logically as one
// large array
class ChunkedArray {
 public:
  explicit ChunkedArray(const ArrayVector& chunks);

  // @returns: the total length of the chunked array; computed on construction
  int64_t length() const {
    return length_;
  }

  int64_t null_count() const {
    return null_count_;
  }

  int num_chunks() const {
    return chunks_.size();
  }

  const std::shared_ptr<Array>& chunk(int i) const {
    return chunks_[i];
  }

 protected:
  ArrayVector chunks_;
  int64_t length_;
  int64_t null_count_;
};

// An immutable column data structure consisting of a field (type metadata) and
// a logical chunked data array (which can be validated as all being the same
// type).
class Column {
 public:
  Column(const std::shared_ptr<Field>& field, const ArrayVector& chunks);
  Column(const std::shared_ptr<Field>& field,
      const std::shared_ptr<ChunkedArray>& data);

  Column(const std::shared_ptr<Field>& field, const std::shared_ptr<Array>& data);

  int64_t length() const {
    return data_->length();
  }

  int64_t null_count() const {
    return data_->null_count();
  }

  // @returns: the column's name in the passed metadata
  const std::string& name() const {
    return field_->name;
  }

  // @returns: the column's type according to the metadata
  const std::shared_ptr<DataType>& type() const {
    return field_->type;
  }

  // @returns: the column's data as a chunked logical array
  const std::shared_ptr<ChunkedArray>& data() const {
    return data_;
  }
  // Verify that the column's array data is consistent with the passed field's
  // metadata
  Status ValidateData();

 protected:
  std::shared_ptr<Field> field_;
  std::shared_ptr<ChunkedArray> data_;
};

} // namespace arrow

#endif  // ARROW_TABLE_COLUMN_H
