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

#include "gtest/gtest.h"

#include <memory>

#include "arrow/python/platform.h"

#include "arrow/array.h"
#include "arrow/builder.h"
#include "arrow/table.h"
#include "arrow/test-util.h"

#include "arrow/python/arrow_to_pandas.h"
#include "arrow/python/builtin_convert.h"
#include "arrow/python/helpers.h"

namespace arrow {
namespace py {

TEST(PyBuffer, InvalidInputObject) {
  std::shared_ptr<Buffer> res;
  PyObject* input = Py_None;
  auto old_refcnt = Py_REFCNT(input);
  ASSERT_RAISES(PythonError, PyBuffer::FromPyObject(input, &res));
  PyErr_Clear();
  ASSERT_EQ(old_refcnt, Py_REFCNT(input));
}

TEST(OwnedRef, TestMoves) {
  PyAcquireGIL lock;
  std::vector<OwnedRef> vec;
  PyObject *u, *v;
  u = PyList_New(0);
  v = PyList_New(0);
  {
    OwnedRef ref(u);
    vec.push_back(std::move(ref));
    ASSERT_EQ(ref.obj(), nullptr);
  }
  vec.emplace_back(v);
  ASSERT_EQ(Py_REFCNT(u), 1);
  ASSERT_EQ(Py_REFCNT(v), 1);
}

TEST(OwnedRefNoGIL, TestMoves) {
  std::vector<OwnedRefNoGIL> vec;
  PyObject *u, *v;
  {
    PyAcquireGIL lock;
    u = PyList_New(0);
    v = PyList_New(0);
  }
  {
    OwnedRefNoGIL ref(u);
    vec.push_back(std::move(ref));
    ASSERT_EQ(ref.obj(), nullptr);
  }
  vec.emplace_back(v);
  ASSERT_EQ(Py_REFCNT(u), 1);
  ASSERT_EQ(Py_REFCNT(v), 1);
}

class DecimalTest : public ::testing::Test {
 public:
  DecimalTest() : lock_(), decimal_module_(), decimal_constructor_() {
    auto s = internal::ImportModule("decimal", &decimal_module_);
    DCHECK(s.ok()) << s.message();
    DCHECK_NE(decimal_module_.obj(), NULLPTR);

    s = internal::ImportFromModule(decimal_module_, "Decimal", &decimal_constructor_);
    DCHECK(s.ok()) << s.message();

    DCHECK_NE(decimal_constructor_.obj(), NULLPTR);
  }

  OwnedRef CreatePythonDecimal(const std::string& string_value) {
    OwnedRef ref(internal::DecimalFromString(decimal_constructor_.obj(), string_value));
    return ref;
  }

 private:
  PyAcquireGIL lock_;
  OwnedRef decimal_module_;
  OwnedRef decimal_constructor_;
};

TEST_F(DecimalTest, TestPythonDecimalToString) {
  std::string decimal_string("-39402950693754869342983");

  OwnedRef python_object = this->CreatePythonDecimal(decimal_string);
  ASSERT_NE(python_object.obj(), nullptr);

  std::string string_result;
  ASSERT_OK(internal::PythonDecimalToString(python_object.obj(), &string_result));
}

TEST_F(DecimalTest, TestInferPrecisionAndScale) {
  std::string decimal_string("-394029506937548693.42983");
  OwnedRef python_decimal(this->CreatePythonDecimal(decimal_string));

  int32_t precision;
  int32_t scale;

  ASSERT_OK(
      internal::InferDecimalPrecisionAndScale(python_decimal.obj(), &precision, &scale));

  const auto expected_precision =
      static_cast<int32_t>(decimal_string.size() - 2);  // 1 for -, 1 for .
  const int32_t expected_scale = 5;

  ASSERT_EQ(expected_precision, precision);
  ASSERT_EQ(expected_scale, scale);
}

TEST_F(DecimalTest, TestInferPrecisionAndNegativeScale) {
  std::string decimal_string("-3.94042983E+10");
  OwnedRef python_decimal(this->CreatePythonDecimal(decimal_string));

  int32_t precision;
  int32_t scale;

  ASSERT_OK(
      internal::InferDecimalPrecisionAndScale(python_decimal.obj(), &precision, &scale));

  const auto expected_precision = 9;
  const int32_t expected_scale = -2;

  ASSERT_EQ(expected_precision, precision);
  ASSERT_EQ(expected_scale, scale);
}

TEST(PandasConversionTest, TestObjectBlockWriteFails) {
  StringBuilder builder;
  const char value[] = {'\xf1', '\0'};

  for (int i = 0; i < 1000; ++i) {
    ASSERT_OK(builder.Append(value, static_cast<int32_t>(strlen(value))));
  }

  std::shared_ptr<Array> arr;
  ASSERT_OK(builder.Finish(&arr));

  auto f1 = field("f1", utf8());
  auto f2 = field("f2", utf8());
  auto f3 = field("f3", utf8());
  std::vector<std::shared_ptr<Field>> fields = {f1, f2, f3};
  std::vector<std::shared_ptr<Array>> cols = {arr, arr, arr};

  auto schema = ::arrow::schema(fields);
  auto table = Table::Make(schema, cols);

  PyObject* out;
  Py_BEGIN_ALLOW_THREADS;
  PandasOptions options;
  MemoryPool* pool = default_memory_pool();
  ASSERT_RAISES(UnknownError, ConvertTableToPandas(options, table, 2, pool, &out));
  Py_END_ALLOW_THREADS;
}

TEST(BuiltinConversionTest, TestMixedTypeFails) {
  PyAcquireGIL lock;
  MemoryPool* pool = default_memory_pool();
  std::shared_ptr<Array> arr;

  OwnedRef list_ref(PyList_New(3));
  PyObject* list = list_ref.obj();

  ASSERT_NE(list, nullptr);

  PyObject* str = PyUnicode_FromString("abc");
  ASSERT_NE(str, nullptr);

  PyObject* integer = PyLong_FromLong(1234L);
  ASSERT_NE(integer, nullptr);

  PyObject* doub = PyFloat_FromDouble(123.0234);
  ASSERT_NE(doub, nullptr);

  // This steals a reference to each object, so we don't need to decref them later
  // just the list
  ASSERT_EQ(PyList_SetItem(list, 0, str), 0);
  ASSERT_EQ(PyList_SetItem(list, 1, integer), 0);
  ASSERT_EQ(PyList_SetItem(list, 2, doub), 0);

  ASSERT_RAISES(UnknownError, ConvertPySequence(list, pool, &arr));
}

TEST_F(DecimalTest, FromPythonDecimalRescaleNotTruncateable) {
  // We fail when truncating values that would lose data if cast to a decimal type with
  // lower scale
  Decimal128 value;
  OwnedRef python_decimal(this->CreatePythonDecimal("1.001"));
  auto type = ::arrow::decimal(10, 2);
  const auto& decimal_type = static_cast<const DecimalType&>(*type);
  ASSERT_RAISES(Invalid, internal::DecimalFromPythonDecimal(python_decimal.obj(),
                                                            decimal_type, &value));
}

TEST_F(DecimalTest, FromPythonDecimalRescaleTruncateable) {
  // We allow truncation of values that do not lose precision when dividing by 10 * the
  // difference between the scales, e.g., 1.000 -> 1.00
  Decimal128 value;
  OwnedRef python_decimal(this->CreatePythonDecimal("1.000"));
  auto type = ::arrow::decimal(10, 2);
  const auto& decimal_type = static_cast<const DecimalType&>(*type);
  ASSERT_OK(
      internal::DecimalFromPythonDecimal(python_decimal.obj(), decimal_type, &value));
  ASSERT_EQ(100, value.low_bits());
}

TEST_F(DecimalTest, TestOverflowFails) {
  Decimal128 value;
  int32_t precision;
  int32_t scale;
  OwnedRef python_decimal(
      this->CreatePythonDecimal("9999999999999999999999999999999999999.9"));
  ASSERT_OK(
      internal::InferDecimalPrecisionAndScale(python_decimal.obj(), &precision, &scale));
  ASSERT_EQ(38, precision);
  ASSERT_EQ(1, scale);

  auto type = ::arrow::decimal(38, 38);
  const auto& decimal_type = static_cast<const DecimalType&>(*type);
  ASSERT_RAISES(Invalid, internal::DecimalFromPythonDecimal(python_decimal.obj(),
                                                            decimal_type, &value));
}

}  // namespace py
}  // namespace arrow
