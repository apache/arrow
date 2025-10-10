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

#include "arrow/flight/sql/odbc/odbc_impl/accessors/primitive_array_accessor.h"

#include "arrow/flight/sql/odbc/odbc_impl/diagnostics.h"
#include "arrow/testing/builder.h"
#include "gtest/gtest.h"

namespace arrow::flight::sql::odbc {

template <typename ARROW_ARRAY, CDataType TARGET_TYPE>
void TestPrimitiveArraySqlAccessor() {
  typedef typename ARROW_ARRAY::TypeClass::c_type c_type;

  std::vector<c_type> values = {0, 1, 2, 3, 127};

  std::shared_ptr<Array> array;
  ArrayFromVector<typename ARROW_ARRAY::TypeClass>(values, &array);

  PrimitiveArrayFlightSqlAccessor<ARROW_ARRAY, TARGET_TYPE> accessor(array.get());

  std::vector<c_type> buffer(values.size());
  std::vector<ssize_t> str_len_buffer(values.size());

  ColumnBinding binding(TARGET_TYPE, 0, 0, buffer.data(), values.size(),
                        str_len_buffer.data());

  int64_t value_offset = 0;
  Diagnostics diagnostics("Dummy", "Dummy", OdbcVersion::V_3);
  ASSERT_EQ(values.size(),
            accessor.GetColumnarData(&binding, 0, values.size(), value_offset, false,
                                     diagnostics, nullptr));

  for (int i = 0; i < values.size(); ++i) {
    ASSERT_EQ(sizeof(c_type), str_len_buffer[i]);
    ASSERT_EQ(values[i], buffer[i]);
  }
}

TEST(PrimitiveArrayFlightSqlAccessor, TestInt64ArrayCDataTypeSbigint) {
  TestPrimitiveArraySqlAccessor<Int64Array, CDataType_SBIGINT>();
}

TEST(PrimitiveArrayFlightSqlAccessor, TestInt32ArrayCDataTypeSlong) {
  TestPrimitiveArraySqlAccessor<Int32Array, CDataType_SLONG>();
}

TEST(PrimitiveArrayFlightSqlAccessor, TestInt16ArrayCDataTypeSshort) {
  TestPrimitiveArraySqlAccessor<Int16Array, CDataType_SSHORT>();
}

TEST(PrimitiveArrayFlightSqlAccessor, TestInt8ArrayCDataTypeStinyint) {
  TestPrimitiveArraySqlAccessor<Int8Array, CDataType_STINYINT>();
}

TEST(PrimitiveArrayFlightSqlAccessor, TestUInt64ArrayCDataTypeUbigint) {
  TestPrimitiveArraySqlAccessor<UInt64Array, CDataType_UBIGINT>();
}

TEST(PrimitiveArrayFlightSqlAccessor, TestUInt32ArrayCDataTypeUlong) {
  TestPrimitiveArraySqlAccessor<UInt32Array, CDataType_ULONG>();
}

TEST(PrimitiveArrayFlightSqlAccessor, TestUInt16ArrayCDataTypeUshort) {
  TestPrimitiveArraySqlAccessor<UInt16Array, CDataType_USHORT>();
}

TEST(PrimitiveArrayFlightSqlAccessor, TestUInt8ArrayCDataTypeUtinyint) {
  TestPrimitiveArraySqlAccessor<UInt8Array, CDataType_UTINYINT>();
}

TEST(PrimitiveArrayFlightSqlAccessor, TestFloatArrayCDataTypeFloat) {
  TestPrimitiveArraySqlAccessor<FloatArray, CDataType_FLOAT>();
}

TEST(PrimitiveArrayFlightSqlAccessor, TestDoubleArrayCDataTypeDouble) {
  TestPrimitiveArraySqlAccessor<DoubleArray, CDataType_DOUBLE>();
}

}  // namespace arrow::flight::sql::odbc
