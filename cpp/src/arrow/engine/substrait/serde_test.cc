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

#include "arrow/engine/substrait/serde.h"

#include <gtest/gtest.h>

#include "arrow/engine/substrait/extension_types.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/matchers.h"

namespace arrow {
namespace engine {

TEST(Substrait, BasicTypeRoundTrip) {
  for (auto type : {
           boolean(),

           int8(),
           int16(),
           int32(),
           int64(),

           float32(),
           float64(),

           date32(),
           timestamp(TimeUnit::MICRO),
           timestamp(TimeUnit::MICRO, "UTC"),
           time64(TimeUnit::MICRO),

           decimal128(27, 5),

           struct_({
               field("", int64()),
               field("", list(utf8())),
           }),

           uuid(),
           fixed_char(32),
           varchar(1024),
       }) {
    ARROW_SCOPED_TRACE(type->ToString());
    ASSERT_OK_AND_ASSIGN(auto serialized, SerializeType(*type));
    ASSERT_OK_AND_ASSIGN(auto roundtripped, DeserializeType(*serialized));
    ASSERT_EQ(*roundtripped, *type);
  }
}

TEST(Substrait, UnsupportedTypes) {
  for (auto type : {
           uint8(),
           uint16(),
           uint32(),
           uint64(),

           float16(),

           date64(),
           timestamp(TimeUnit::SECOND),
           timestamp(TimeUnit::NANO),
           timestamp(TimeUnit::MICRO, "New York"),
           time32(TimeUnit::SECOND),
           time32(TimeUnit::MILLI),
           time64(TimeUnit::NANO),
           month_interval(),
           day_time_interval(),

           decimal256(76, 67),

           sparse_union({field("i8", int8()), field("f32", float32())}),
           dense_union({field("i8", int8()), field("f32", float32())}),
           dictionary(int32(), utf8()),

           fixed_size_list(float16(), 3),

           duration(TimeUnit::MICRO),

           large_utf8(),
           large_binary(),
           large_list(utf8()),

           month_day_nano_interval(),
       }) {
    ARROW_SCOPED_TRACE(type->ToString());
    ASSERT_THAT(SerializeType(*type), Raises(StatusCode::NotImplemented));
  }
}

TEST(Substrait, BasicLiteralRoundTrip) {
  for (Datum datum : {
           Datum(true),

           Datum(int8_t(34)),
           Datum(int16_t(34)),
           Datum(int32_t(34)),
           Datum(int64_t(34)),

           Datum(3.5F),
           Datum(7.125),
       }) {
    ARROW_SCOPED_TRACE(datum.scalar()->ToString());
    ASSERT_OK_AND_ASSIGN(auto serialized, SerializeExpression(compute::literal(datum)));
    ASSERT_OK_AND_ASSIGN(auto roundtripped, DeserializeExpression(*serialized));
    ASSERT_TRUE(roundtripped.literal());
    ASSERT_THAT(*roundtripped.literal(), DataEq(datum));
  }
}

}  // namespace engine
}  // namespace arrow

