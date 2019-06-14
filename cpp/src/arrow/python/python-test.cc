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
#include "arrow/testing/gtest_util.h"
#include "arrow/util/decimal.h"

#include "arrow/python/arrow_to_pandas.h"
#include "arrow/python/decimal.h"
#include "arrow/python/helpers.h"
#include "arrow/python/python_to_arrow.h"
#include "arrow/util/checked_cast.h"

namespace arrow {

using internal::checked_cast;

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

TEST(CheckPyError, TestStatus) {
  PyAcquireGIL lock;
  Status st;

  auto check_error = [](Status& st, const char* expected_message = "some error") {
    st = CheckPyError();
    ASSERT_EQ(st.message(), expected_message);
    ASSERT_FALSE(PyErr_Occurred());
  };

  for (PyObject* exc_type : {PyExc_Exception, PyExc_SyntaxError}) {
    PyErr_SetString(exc_type, "some error");
    check_error(st);
    ASSERT_TRUE(st.IsUnknownError());
  }

  PyErr_SetString(PyExc_TypeError, "some error");
  check_error(st);
  ASSERT_TRUE(st.IsTypeError());

  PyErr_SetString(PyExc_ValueError, "some error");
  check_error(st);
  ASSERT_TRUE(st.IsInvalid());

  PyErr_SetString(PyExc_KeyError, "some error");
  check_error(st, "'some error'");
  ASSERT_TRUE(st.IsKeyError());

  for (PyObject* exc_type : {PyExc_OSError, PyExc_IOError}) {
    PyErr_SetString(exc_type, "some error");
    check_error(st);
    ASSERT_TRUE(st.IsIOError());
  }

  PyErr_SetString(PyExc_NotImplementedError, "some error");
  check_error(st);
  ASSERT_TRUE(st.IsNotImplemented());

  // No override if a specific status code is given
  PyErr_SetString(PyExc_TypeError, "some error");
  st = CheckPyError(StatusCode::SerializationError);
  ASSERT_TRUE(st.IsSerializationError());
  ASSERT_EQ(st.message(), "some error");
  ASSERT_FALSE(PyErr_Occurred());
}

class DecimalTest : public ::testing::Test {
 public:
  DecimalTest() : lock_(), decimal_constructor_() {
    OwnedRef decimal_module;

    Status status = internal::ImportModule("decimal", &decimal_module);
    DCHECK_OK(status);

    status = internal::ImportFromModule(decimal_module, "Decimal", &decimal_constructor_);
    DCHECK_OK(status);
  }

  OwnedRef CreatePythonDecimal(const std::string& string_value) {
    OwnedRef ref(internal::DecimalFromString(decimal_constructor_.obj(), string_value));
    return ref;
  }

  PyObject* decimal_constructor() const { return decimal_constructor_.obj(); }

 private:
  PyAcquireGIL lock_;
  OwnedRef decimal_constructor_;
};

TEST_F(DecimalTest, TestPythonDecimalToString) {
  std::string decimal_string("-39402950693754869342983");

  OwnedRef python_object(this->CreatePythonDecimal(decimal_string));
  ASSERT_NE(python_object.obj(), nullptr);

  std::string string_result;
  ASSERT_OK(internal::PythonDecimalToString(python_object.obj(), &string_result));
}

TEST_F(DecimalTest, TestInferPrecisionAndScale) {
  std::string decimal_string("-394029506937548693.42983");
  OwnedRef python_decimal(this->CreatePythonDecimal(decimal_string));

  internal::DecimalMetadata metadata;
  ASSERT_OK(metadata.Update(python_decimal.obj()));

  const auto expected_precision =
      static_cast<int32_t>(decimal_string.size() - 2);  // 1 for -, 1 for .
  const int32_t expected_scale = 5;

  ASSERT_EQ(expected_precision, metadata.precision());
  ASSERT_EQ(expected_scale, metadata.scale());
}

TEST_F(DecimalTest, TestInferPrecisionAndNegativeScale) {
  std::string decimal_string("-3.94042983E+10");
  OwnedRef python_decimal(this->CreatePythonDecimal(decimal_string));

  internal::DecimalMetadata metadata;
  ASSERT_OK(metadata.Update(python_decimal.obj()));

  const auto expected_precision = 9;
  const int32_t expected_scale = -2;

  ASSERT_EQ(expected_precision, metadata.precision());
  ASSERT_EQ(expected_scale, metadata.scale());
}

TEST_F(DecimalTest, TestInferAllLeadingZeros) {
  std::string decimal_string("0.001");
  OwnedRef python_decimal(this->CreatePythonDecimal(decimal_string));

  internal::DecimalMetadata metadata;
  ASSERT_OK(metadata.Update(python_decimal.obj()));
  ASSERT_EQ(3, metadata.precision());
  ASSERT_EQ(3, metadata.scale());
}

TEST_F(DecimalTest, TestInferAllLeadingZerosExponentialNotationPositive) {
  std::string decimal_string("0.01E5");
  OwnedRef python_decimal(this->CreatePythonDecimal(decimal_string));
  internal::DecimalMetadata metadata;
  ASSERT_OK(metadata.Update(python_decimal.obj()));
  ASSERT_EQ(4, metadata.precision());
  ASSERT_EQ(0, metadata.scale());
}

TEST_F(DecimalTest, TestInferAllLeadingZerosExponentialNotationNegative) {
  std::string decimal_string("0.01E3");
  OwnedRef python_decimal(this->CreatePythonDecimal(decimal_string));
  internal::DecimalMetadata metadata;
  ASSERT_OK(metadata.Update(python_decimal.obj()));
  ASSERT_EQ(2, metadata.precision());
  ASSERT_EQ(0, metadata.scale());
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
  options.use_threads = true;
  MemoryPool* pool = default_memory_pool();
  ASSERT_RAISES(UnknownError, ConvertTableToPandas(options, table, pool, &out));
  Py_END_ALLOW_THREADS;
}

TEST(BuiltinConversionTest, TestMixedTypeFails) {
  PyAcquireGIL lock;

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

  std::shared_ptr<ChunkedArray> arr;
  ASSERT_RAISES(TypeError, ConvertPySequence(list, {}, &arr));
}

TEST_F(DecimalTest, FromPythonDecimalRescaleNotTruncateable) {
  // We fail when truncating values that would lose data if cast to a decimal type with
  // lower scale
  Decimal128 value;
  OwnedRef python_decimal(this->CreatePythonDecimal("1.001"));
  auto type = ::arrow::decimal(10, 2);
  const auto& decimal_type = checked_cast<const DecimalType&>(*type);
  ASSERT_RAISES(Invalid, internal::DecimalFromPythonDecimal(python_decimal.obj(),
                                                            decimal_type, &value));
}

TEST_F(DecimalTest, FromPythonDecimalRescaleTruncateable) {
  // We allow truncation of values that do not lose precision when dividing by 10 * the
  // difference between the scales, e.g., 1.000 -> 1.00
  Decimal128 value;
  OwnedRef python_decimal(this->CreatePythonDecimal("1.000"));
  auto type = ::arrow::decimal(10, 2);
  const auto& decimal_type = checked_cast<const DecimalType&>(*type);
  ASSERT_OK(
      internal::DecimalFromPythonDecimal(python_decimal.obj(), decimal_type, &value));
  ASSERT_EQ(100, value.low_bits());
  ASSERT_EQ(0, value.high_bits());

  ASSERT_OK(internal::DecimalFromPyObject(python_decimal.obj(), decimal_type, &value));
  ASSERT_EQ(100, value.low_bits());
  ASSERT_EQ(0, value.high_bits());
}

TEST_F(DecimalTest, FromPythonNegativeDecimalRescale) {
  Decimal128 value;
  OwnedRef python_decimal(this->CreatePythonDecimal("-1.000"));
  auto type = ::arrow::decimal(10, 9);
  const auto& decimal_type = checked_cast<const DecimalType&>(*type);
  ASSERT_OK(
      internal::DecimalFromPythonDecimal(python_decimal.obj(), decimal_type, &value));
  ASSERT_EQ(-1000000000, value);
}

TEST_F(DecimalTest, FromPythonInteger) {
  Decimal128 value;
  OwnedRef python_long(PyLong_FromLong(42));
  auto type = ::arrow::decimal(10, 2);
  const auto& decimal_type = checked_cast<const DecimalType&>(*type);
  ASSERT_OK(internal::DecimalFromPyObject(python_long.obj(), decimal_type, &value));
  ASSERT_EQ(4200, value);
}

TEST_F(DecimalTest, TestOverflowFails) {
  Decimal128 value;
  OwnedRef python_decimal(
      this->CreatePythonDecimal("9999999999999999999999999999999999999.9"));
  internal::DecimalMetadata metadata;
  ASSERT_OK(metadata.Update(python_decimal.obj()));
  ASSERT_EQ(38, metadata.precision());
  ASSERT_EQ(1, metadata.scale());

  auto type = ::arrow::decimal(38, 38);
  const auto& decimal_type = checked_cast<const DecimalType&>(*type);
  ASSERT_RAISES(Invalid, internal::DecimalFromPythonDecimal(python_decimal.obj(),
                                                            decimal_type, &value));
}

TEST_F(DecimalTest, TestNoneAndNaN) {
  OwnedRef list_ref(PyList_New(4));
  PyObject* list = list_ref.obj();

  ASSERT_NE(list, nullptr);

  PyObject* constructor = this->decimal_constructor();
  PyObject* decimal_value = internal::DecimalFromString(constructor, "1.234");
  ASSERT_NE(decimal_value, nullptr);

  Py_INCREF(Py_None);
  PyObject* missing_value1 = Py_None;
  ASSERT_NE(missing_value1, nullptr);

  PyObject* missing_value2 = PyFloat_FromDouble(NPY_NAN);
  ASSERT_NE(missing_value2, nullptr);

  PyObject* missing_value3 = internal::DecimalFromString(constructor, "nan");
  ASSERT_NE(missing_value3, nullptr);

  // This steals a reference to each object, so we don't need to decref them later,
  // just the list
  ASSERT_EQ(0, PyList_SetItem(list, 0, decimal_value));
  ASSERT_EQ(0, PyList_SetItem(list, 1, missing_value1));
  ASSERT_EQ(0, PyList_SetItem(list, 2, missing_value2));
  ASSERT_EQ(0, PyList_SetItem(list, 3, missing_value3));

  std::shared_ptr<ChunkedArray> arr;
  ASSERT_OK(ConvertPySequence(list, {}, &arr));

  auto c0 = arr->chunk(0);
  ASSERT_TRUE(c0->IsValid(0));
  ASSERT_TRUE(c0->IsNull(1));
  ASSERT_TRUE(c0->IsNull(2));
  ASSERT_TRUE(c0->IsNull(3));
}

TEST_F(DecimalTest, TestMixedPrecisionAndScale) {
  std::vector<std::string> strings{{"0.001", "1.01E5", "1.01E5"}};

  OwnedRef list_ref(PyList_New(static_cast<Py_ssize_t>(strings.size())));
  PyObject* list = list_ref.obj();

  ASSERT_NE(list, nullptr);

  // PyList_SetItem steals a reference to the item so we don't decref it later
  PyObject* decimal_constructor = this->decimal_constructor();
  for (Py_ssize_t i = 0; i < static_cast<Py_ssize_t>(strings.size()); ++i) {
    const int result = PyList_SetItem(
        list, i, internal::DecimalFromString(decimal_constructor, strings.at(i)));
    ASSERT_EQ(0, result);
  }

  std::shared_ptr<ChunkedArray> arr;
  ASSERT_OK(ConvertPySequence(list, {}, &arr));
  const auto& type = checked_cast<const DecimalType&>(*arr->type());

  int32_t expected_precision = 9;
  int32_t expected_scale = 3;
  ASSERT_EQ(expected_precision, type.precision());
  ASSERT_EQ(expected_scale, type.scale());
}

TEST_F(DecimalTest, TestMixedPrecisionAndScaleSequenceConvert) {
  PyAcquireGIL lock;

  PyObject* value1 = this->CreatePythonDecimal("0.01").detach();
  ASSERT_NE(value1, nullptr);

  PyObject* value2 = this->CreatePythonDecimal("0.001").detach();
  ASSERT_NE(value2, nullptr);

  OwnedRef list_ref(PyList_New(2));
  PyObject* list = list_ref.obj();

  // This steals a reference to each object, so we don't need to decref them later
  // just the list
  ASSERT_EQ(PyList_SetItem(list, 0, value1), 0);
  ASSERT_EQ(PyList_SetItem(list, 1, value2), 0);

  std::shared_ptr<ChunkedArray> arr;
  ASSERT_OK(ConvertPySequence(list, {}, &arr));

  const auto& type = checked_cast<const Decimal128Type&>(*arr->type());
  ASSERT_EQ(3, type.precision());
  ASSERT_EQ(3, type.scale());
}

TEST_F(DecimalTest, SimpleInference) {
  OwnedRef value(this->CreatePythonDecimal("0.01"));
  ASSERT_NE(value.obj(), nullptr);
  internal::DecimalMetadata metadata;
  ASSERT_OK(metadata.Update(value.obj()));
  ASSERT_EQ(2, metadata.precision());
  ASSERT_EQ(2, metadata.scale());
}

TEST_F(DecimalTest, UpdateWithNaN) {
  internal::DecimalMetadata metadata;
  OwnedRef nan_value(this->CreatePythonDecimal("nan"));
  ASSERT_OK(metadata.Update(nan_value.obj()));
  ASSERT_EQ(std::numeric_limits<int32_t>::min(), metadata.precision());
  ASSERT_EQ(std::numeric_limits<int32_t>::min(), metadata.scale());
}

}  // namespace py
}  // namespace arrow
