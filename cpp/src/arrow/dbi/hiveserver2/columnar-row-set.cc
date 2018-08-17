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

#include "arrow/dbi/hiveserver2/columnar-row-set.h"

#include <string>
#include <vector>

#include "arrow/dbi/hiveserver2/TCLIService.h"
#include "arrow/dbi/hiveserver2/thrift-internal.h"

#include "arrow/util/logging.h"

namespace hs2 = apache::hive::service::cli::thrift;

namespace arrow {
namespace hiveserver2 {

Column::Column(const std::string* nulls) {
  DCHECK(nulls);
  nulls_ = reinterpret_cast<const uint8_t*>(nulls->c_str());
  nulls_size_ = static_cast<int64_t>(nulls->size());
}

ColumnarRowSet::ColumnarRowSet(ColumnarRowSetImpl* impl) : impl_(impl) {}

ColumnarRowSet::~ColumnarRowSet() = default;

template <typename T>
struct type_helpers {};

#define VALUE_GETTER(COLUMN_TYPE, VALUE_TYPE, ATTR_NAME)                       \
  template <>                                                                  \
  struct type_helpers<COLUMN_TYPE> {                                           \
    static const std::vector<VALUE_TYPE>* GetValues(const hs2::TColumn& col) { \
      return &col.ATTR_NAME.values;                                            \
    }                                                                          \
                                                                               \
    static const std::string* GetNulls(const hs2::TColumn& col) {              \
      return &col.ATTR_NAME.nulls;                                             \
    }                                                                          \
  };

VALUE_GETTER(BoolColumn, bool, boolVal);
VALUE_GETTER(ByteColumn, int8_t, byteVal);
VALUE_GETTER(Int16Column, int16_t, i16Val);
VALUE_GETTER(Int32Column, int32_t, i32Val);
VALUE_GETTER(Int64Column, int64_t, i64Val);
VALUE_GETTER(DoubleColumn, double, doubleVal);
VALUE_GETTER(StringColumn, std::string, stringVal);

#undef VALUE_GETTER

template <typename T>
std::unique_ptr<T> ColumnarRowSet::GetCol(int i) const {
  using helper = type_helpers<T>;

  DCHECK_LT(i, static_cast<int>(impl_->resp.results.columns.size()));

  const hs2::TColumn& col = impl_->resp.results.columns[i];
  return std::unique_ptr<T>(new T(helper::GetNulls(col), helper::GetValues(col)));
}

#define TYPED_GETTER(FUNC_NAME, TYPE)                            \
  std::unique_ptr<TYPE> ColumnarRowSet::FUNC_NAME(int i) const { \
    return GetCol<TYPE>(i);                                      \
  }                                                              \
  template std::unique_ptr<TYPE> ColumnarRowSet::GetCol<TYPE>(int i) const;

TYPED_GETTER(GetBoolCol, BoolColumn);
TYPED_GETTER(GetByteCol, ByteColumn);
TYPED_GETTER(GetInt16Col, Int16Column);
TYPED_GETTER(GetInt32Col, Int32Column);
TYPED_GETTER(GetInt64Col, Int64Column);
TYPED_GETTER(GetDoubleCol, DoubleColumn);
TYPED_GETTER(GetStringCol, StringColumn);

#undef TYPED_GETTER

// BinaryColumn is an alias for StringColumn
std::unique_ptr<BinaryColumn> ColumnarRowSet::GetBinaryCol(int i) const {
  return GetCol<BinaryColumn>(i);
}

}  // namespace hiveserver2
}  // namespace arrow
