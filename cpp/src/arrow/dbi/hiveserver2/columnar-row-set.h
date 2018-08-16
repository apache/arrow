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
#include <string>
#include <vector>

#include "arrow/util/macros.h"
#include "arrow/util/visibility.h"

namespace arrow {
namespace hiveserver2 {

// The Column class is used to access data that was fetched in columnar format.
// The contents of the data can be accessed through the data() fn, which returns
// a ptr to a vector containing the contents of this column in the fetched
// results, avoiding copies. This vector will be of size length().
//
// If any of the values are null, they will be represented in the data vector as
// default values, i.e. 0 for numeric types. The nulls() fn returns a ptr to a
// bit array representing which values are null, and the IsNull() fn is provided
// for convenience when working with this bit array. The user should check
// IsNull() to distinguish between actual instances of the default values and nulls.
//
// A Column object is returned from a ColumnarRowSet and is only valid as long
// as that ColumnarRowSet still exists.
//
// Example:
// unique_ptr<Int32Column> col = columnar_row_set->GetInt32Col();
// for (int i = 0; i < col->length(); i++) {
//   if (col->IsNull(i)) {
//     cout << "NULL\n";
//   } else {
//     cout << col->data()[i] << "\n";
//   }
// }
class ARROW_EXPORT Column {
 public:
  virtual ~Column() {}

  virtual int64_t length() const = 0;

  const uint8_t* nulls() const { return nulls_; }
  int64_t nulls_size() const { return nulls_size_; }

  // Returns true iff the value for the i-th row within this set of data for this
  // column is null.
  bool IsNull(int64_t i) const { return (nulls_[i / 8] & (1 << (i % 8))) != 0; }

 protected:
  explicit Column(const std::string* nulls);

  // The memory for these ptrs is owned by the ColumnarRowSet that
  // created this Column.
  //
  // Due to the issue described in HUE-2722, the null bitmap may have fewer
  // bytes than expected for some versions of Hive, so we retain the ability to
  // check the buffer size in case this happens.
  const uint8_t* nulls_;
  int64_t nulls_size_;
};

template <class T>
class ARROW_EXPORT TypedColumn : public Column {
 public:
  const std::vector<T>& data() const { return *data_; }
  int64_t length() const { return data().size(); }

  // Returns the value for the i-th row within this set of data for this column.
  const T& GetData(int64_t i) const { return data()[i]; }

 private:
  // For access to the c'tor.
  friend class ColumnarRowSet;

  TypedColumn(const std::string* nulls, const std::vector<T>* data)
      : Column(nulls), data_(data) {}

  const std::vector<T>* data_;
};

typedef TypedColumn<bool> BoolColumn;
typedef TypedColumn<int8_t> ByteColumn;
typedef TypedColumn<int16_t> Int16Column;
typedef TypedColumn<int32_t> Int32Column;
typedef TypedColumn<int64_t> Int64Column;
typedef TypedColumn<double> DoubleColumn;
typedef TypedColumn<std::string> StringColumn;
typedef TypedColumn<std::string> BinaryColumn;

// A ColumnarRowSet represents the full results returned by a call to
// Operation::Fetch() when a columnar format is being used.
//
// ColumnarRowSet provides access to specific columns by their type and index in
// the results. All Column objects returned from a given ColumnarRowSet will have
// the same length(). A Column object returned by a ColumnarRowSet is only valid
// as long as the ColumnarRowSet still exists.
//
// Example:
// unique_ptr<Operation> op;
// session->ExecuteStatement("select int_col, string_col from tbl", &op);
// unique_ptr<ColumnarRowSet> columnar_row_set;
// if (op->Fetch(&columnar_row_set).ok()) {
//   unique_ptr<Int32Column> int32_col = columnar_row_set->GetInt32Col(0);
//   unique_ptr<StringColumn> string_col = columnar_row_set->GetStringCol(1);
// }
class ARROW_EXPORT ColumnarRowSet {
 public:
  ~ColumnarRowSet();

  std::unique_ptr<BoolColumn> GetBoolCol(int i) const;
  std::unique_ptr<ByteColumn> GetByteCol(int i) const;
  std::unique_ptr<Int16Column> GetInt16Col(int i) const;
  std::unique_ptr<Int32Column> GetInt32Col(int i) const;
  std::unique_ptr<Int64Column> GetInt64Col(int i) const;
  std::unique_ptr<DoubleColumn> GetDoubleCol(int i) const;
  std::unique_ptr<StringColumn> GetStringCol(int i) const;
  std::unique_ptr<BinaryColumn> GetBinaryCol(int i) const;

  template <typename T>
  std::unique_ptr<T> GetCol(int i) const;

 private:
  // Hides Thrift objects from the header.
  struct ColumnarRowSetImpl;

  ARROW_DISALLOW_COPY_AND_ASSIGN(ColumnarRowSet);

  // For access to the c'tor.
  friend class Operation;

  explicit ColumnarRowSet(ColumnarRowSetImpl* impl);

  std::unique_ptr<ColumnarRowSetImpl> impl_;
};

}  // namespace hiveserver2
}  // namespace arrow
