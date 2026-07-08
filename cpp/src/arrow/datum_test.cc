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

#include <chrono>
#include <memory>
#include <string>

#include <gtest/gtest.h>

#include "arrow/array/array_base.h"
#include "arrow/array/array_binary.h"
#include "arrow/array/builder_primitive.h"
#include "arrow/array/builder_run_end.h"
#include "arrow/chunked_array.h"
#include "arrow/datum.h"
#include "arrow/record_batch.h"
#include "arrow/scalar.h"
#include "arrow/table.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/type_fwd.h"
#include "arrow/util/checked_cast.h"

namespace arrow {

using internal::checked_cast;

// ----------------------------------------------------------------------
// Datum

template <typename T>
void CheckImplicitConstructor(Datum::Kind expected_kind) {
  std::shared_ptr<T> value;
  Datum datum = value;
  ASSERT_EQ(expected_kind, datum.kind());
}

TEST(Datum, ImplicitConstructors) {
  CheckImplicitConstructor<Scalar>(Datum::SCALAR);

  CheckImplicitConstructor<Array>(Datum::ARRAY);

  // Instantiate from array subclass
  CheckImplicitConstructor<BinaryArray>(Datum::ARRAY);

  CheckImplicitConstructor<ChunkedArray>(Datum::CHUNKED_ARRAY);
  CheckImplicitConstructor<RecordBatch>(Datum::RECORD_BATCH);

  CheckImplicitConstructor<Table>(Datum::TABLE);
}

TEST(Datum, Constructors) {
  Datum val(std::make_shared<Int64Scalar>(1));
  AssertTypeEqual(*int64(), *val.type());
  ASSERT_TRUE(val.is_scalar());
  ASSERT_FALSE(val.is_array());
  ASSERT_EQ(1, val.length());

  const Int64Scalar& val_as_i64 = checked_cast<const Int64Scalar&>(*val.scalar());
  const Int64Scalar& val_as_i64_2 = val.scalar_as<Int64Scalar>();
  ASSERT_EQ(1, val_as_i64.value);
  ASSERT_EQ(1, val_as_i64_2.value);

  auto arr = ArrayFromJSON(int64(), "[1, 2, 3, 4]");
  auto sel_indices = ArrayFromJSON(int32(), "[0, 3]");

  Datum val2(arr);
  ASSERT_EQ(Datum::ARRAY, val2.kind());
  AssertTypeEqual(*int64(), *val2.type());
  AssertArraysEqual(*arr, *val2.make_array());
  ASSERT_TRUE(val2.is_array());
  ASSERT_FALSE(val2.is_scalar());
  ASSERT_EQ(arr->length(), val2.length());

  auto Check = [&](const Datum& v) { AssertArraysEqual(*arr, *v.make_array()); };

  // Copy constructor
  Datum val3 = val2;
  Check(val3);

  // Copy assignment
  Datum val4;
  val4 = val2;
  Check(val4);

  // Move constructor
  Datum val5 = std::move(val2);
  Check(val5);

  // Move assignment
  Datum val6;
  val6 = std::move(val4);
  Check(val6);

  AssertDatumsEqual(Datum{std::chrono::nanoseconds{1235}},
                    Datum{DurationScalar{1235, TimeUnit::NANO}});

  AssertDatumsEqual(Datum{std::chrono::microseconds{58}},
                    Datum{DurationScalar{58, TimeUnit::MICRO}});

  AssertDatumsEqual(Datum{std::chrono::milliseconds{952}},
                    Datum{DurationScalar{952, TimeUnit::MILLI}});

  AssertDatumsEqual(Datum{std::chrono::seconds{625}},
                    Datum{DurationScalar{625, TimeUnit::SECOND}});

  AssertDatumsEqual(Datum{std::chrono::minutes{2}},
                    Datum{DurationScalar{120, TimeUnit::SECOND}});

  // finer than nanoseconds; we can't represent this without truncation
  using picoseconds = std::chrono::duration<int64_t, std::pico>;
  static_assert(!std::is_constructible_v<Datum, picoseconds>);

  // between seconds and milliseconds; we could represent this as milliseconds safely, but
  // it's a pain to support
  using centiseconds = std::chrono::duration<int64_t, std::centi>;
  static_assert(!std::is_constructible_v<Datum, centiseconds>);
}

TEST(Datum, NullCount) {
  Datum val1(std::make_shared<Int8Scalar>(1));
  ASSERT_EQ(0, val1.null_count());

  Datum val2(MakeNullScalar(int8()));
  ASSERT_EQ(1, val2.null_count());

  Datum val3(ArrayFromJSON(int8(), "[1, null, null, null]"));
  ASSERT_EQ(3, val3.null_count());
}

TEST(Datum, ComputeLogicalNullCount) {
  // For most scalars, is_valid already reflects logical validity.
  Datum valid_scalar(std::make_shared<Int8Scalar>(1));
  ASSERT_EQ(0, valid_scalar.ComputeLogicalNullCount());

  Datum null_scalar(MakeNullScalar(int8()));
  ASSERT_EQ(1, null_scalar.ComputeLogicalNullCount());

  // Arrays and scalars of type null() are entirely null.
  Datum null_type_arr(ArrayFromJSON(null(), "[null, null, null]"));
  ASSERT_EQ(3, null_type_arr.null_count());
  ASSERT_EQ(3, null_type_arr.ComputeLogicalNullCount());

  Datum null_type_scalar(MakeNullScalar(null()));
  ASSERT_EQ(1, null_type_scalar.null_count());
  ASSERT_EQ(1, null_type_scalar.ComputeLogicalNullCount());

  // For arrays with a validity bitmap, the logical null count matches
  // null_count().
  Datum int8_arr(ArrayFromJSON(int8(), "[1, null, null, null]"));
  ASSERT_EQ(3, int8_arr.null_count());
  ASSERT_EQ(3, int8_arr.ComputeLogicalNullCount());

  // Union arrays carry logical nulls in their children without a top-level
  // validity bitmap, so null_count() is 0 while the logical null count is not.
  auto union_type = sparse_union({field("a", int8()), field("b", boolean())});
  auto union_arr =
      ArrayFromJSON(union_type, R"([[0, null], [1, true], [0, 5], [1, null]])");
  Datum union_datum(union_arr);
  ASSERT_EQ(0, union_datum.null_count());
  ASSERT_EQ(2, union_datum.ComputeLogicalNullCount());

  // Scalars extracted from an array preserve its logical validity.
  auto scalar_logical_null_count = [](const Array& arr,
                                      int64_t index) -> Result<int64_t> {
    ARROW_ASSIGN_OR_RAISE(auto scalar, arr.GetScalar(index));
    return Datum(scalar).ComputeLogicalNullCount();
  };
  ASSERT_OK_AND_EQ(1, scalar_logical_null_count(*union_arr, 0));
  ASSERT_OK_AND_EQ(0, scalar_logical_null_count(*union_arr, 1));

  // Chunked arrays sum the logical null count over the chunks.
  auto union_chunk = ArrayFromJSON(union_type, R"([[0, 1], [1, null]])");
  ASSERT_OK_AND_ASSIGN(auto chunked, ChunkedArray::Make({union_arr, union_chunk}));
  Datum chunked_datum(chunked);
  ASSERT_EQ(0, chunked_datum.null_count());
  ASSERT_EQ(3, chunked_datum.ComputeLogicalNullCount());

  // Run-end encoded arrays carry logical nulls in their values child, also
  // without a top-level validity bitmap.
  auto pool = default_memory_pool();
  auto ree_type = run_end_encoded(int32(), int32());
  RunEndEncodedBuilder ree_builder(pool, std::make_shared<Int32Builder>(pool),
                                   std::make_shared<Int32Builder>(pool), ree_type);
  ASSERT_OK(ree_builder.AppendScalar(*MakeScalar<int32_t>(7), 2));
  ASSERT_OK(ree_builder.AppendNulls(3));
  ASSERT_OK_AND_ASSIGN(auto ree_arr, ree_builder.Finish());
  Datum ree_datum(ree_arr);
  ASSERT_EQ(0, ree_datum.null_count());
  ASSERT_EQ(3, ree_datum.ComputeLogicalNullCount());
  ASSERT_OK_AND_EQ(0, scalar_logical_null_count(*ree_arr, 0));
  ASSERT_OK_AND_EQ(1, scalar_logical_null_count(*ree_arr, 2));

  // Dictionary arrays have a validity bitmap on the indices, but a valid
  // index referencing a null dictionary value is also a logical null.
  auto dict_type = dictionary(int32(), utf8());
  auto dict_arr = DictArrayFromJSON(dict_type, /*indices=*/"[0, 1, null, 1]",
                                    /*dictionary=*/R"([null, "a"])");
  Datum dict_datum(dict_arr);
  ASSERT_EQ(1, dict_datum.null_count());
  ASSERT_EQ(2, dict_datum.ComputeLogicalNullCount());

  // A DictionaryScalar's is_valid only reflects index validity, but the
  // logical null count also accounts for a valid index referencing a null
  // dictionary value, consistently with the array path.
  ASSERT_OK_AND_ASSIGN(auto dict_scalar, dict_arr->GetScalar(0));
  Datum dict_scalar_datum(dict_scalar);
  ASSERT_EQ(0, dict_scalar_datum.null_count());
  ASSERT_EQ(1, dict_scalar_datum.ComputeLogicalNullCount());
  ASSERT_OK_AND_EQ(0, scalar_logical_null_count(*dict_arr, 1));
  ASSERT_OK_AND_EQ(1, scalar_logical_null_count(*dict_arr, 2));
}

TEST(Datum, MutableArray) {
  auto arr = ArrayFromJSON(int8(), "[1, 2, 3, 4]");

  Datum val(arr);

  val.mutable_array()->length = 0;
  ASSERT_EQ(0, val.array()->length);
}

TEST(Datum, ToString) {
  auto arr = ArrayFromJSON(int8(), "[1, 2, 3, 4]");

  Datum v1(arr);
  Datum v2(std::make_shared<Int8Scalar>(1));

  ASSERT_EQ("Array([\n  1,\n  2,\n  3,\n  4\n])", v1.ToString());
  ASSERT_EQ("Scalar(1)", v2.ToString());
}

TEST(Datum, TotalBufferSize) {
  auto arr = ArrayFromJSON(int8(), "[1, 2, 3, 4]");
  Datum arr_datum(arr);
  ASSERT_OK_AND_ASSIGN(std::shared_ptr<ChunkedArray> chunked_arr,
                       ChunkedArray::Make({arr}));
  Datum chunked_datum(chunked_arr);
  std::shared_ptr<Schema> schm = schema({field("a", int8())});
  Datum rb_datum(RecordBatch::Make(schm, 4, {arr}));
  Datum tab_datum(Table::Make(std::move(schm), {std::move(arr)}, 4));

  ASSERT_EQ(4, arr_datum.TotalBufferSize());
  ASSERT_EQ(4, chunked_datum.TotalBufferSize());
  ASSERT_EQ(4, rb_datum.TotalBufferSize());
  ASSERT_EQ(4, tab_datum.TotalBufferSize());
}

}  // namespace arrow
