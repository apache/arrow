/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include "converters.hpp"

#include <arrow/util/ree_util.h>

namespace red_arrow {
  VALUE ArrayValueConverter::convert(const arrow::ListArray& array,
                                     const int64_t i) {
    return list_array_value_converter_->convert(array, i);
  }

  VALUE ArrayValueConverter::convert(const arrow::LargeListArray& array,
                                     const int64_t i) {
    return large_list_array_value_converter_->convert(array, i);
  }

  VALUE ArrayValueConverter::convert(const arrow::FixedSizeListArray& array,
                                     const int64_t i) {
    return fixed_size_list_array_value_converter_->convert(array, i);
  }

  VALUE ArrayValueConverter::convert(const arrow::StructArray& array,
                                     const int64_t i) {
    return struct_array_value_converter_->convert(array, i);
  }

  VALUE ArrayValueConverter::convert(const arrow::MapArray& array,
                                     const int64_t i) {
    return map_array_value_converter_->convert(array, i);
  }

  VALUE ArrayValueConverter::convert(const arrow::UnionArray& array,
                                     const int64_t i) {
    return union_array_value_converter_->convert(array, i);
  }

  VALUE ArrayValueConverter::convert(const arrow::DictionaryArray& array,
                                     const int64_t i) {
    return dictionary_array_value_converter_->convert(array, i);
  }

  namespace {
    class RunEndEncodedArrayValueConverter : public arrow::ArrayVisitor {
    public:
      RunEndEncodedArrayValueConverter(ArrayValueConverter* converter,
                                      int64_t index)
        : array_value_converter_(converter),
          index_(index),
          result_(Qnil) {
      }

      VALUE convert(const arrow::Array& values) {
        check_status(values.Accept(this), "[raw-records][run-end-encoded-array]");
        return result_;
      }

#define VISIT(TYPE)                                                     \
      arrow::Status Visit(const arrow::TYPE ## Array& array) override {  \
        if (!array.IsNull(index_)) {                                     \
          result_ = array_value_converter_->convert(array, index_);      \
        }                                                               \
        return arrow::Status::OK();                                      \
      }

      VISIT(Null)
      VISIT(Boolean)
      VISIT(Int8)
      VISIT(Int16)
      VISIT(Int32)
      VISIT(Int64)
      VISIT(UInt8)
      VISIT(UInt16)
      VISIT(UInt32)
      VISIT(UInt64)
      VISIT(HalfFloat)
      VISIT(Float)
      VISIT(Double)
      VISIT(Binary)
      VISIT(LargeBinary)
      VISIT(String)
      VISIT(LargeString)
      VISIT(FixedSizeBinary)
      VISIT(Date32)
      VISIT(Date64)
      VISIT(Time32)
      VISIT(Time64)
      VISIT(Timestamp)
      VISIT(MonthInterval)
      VISIT(DayTimeInterval)
      VISIT(MonthDayNanoInterval)
      VISIT(Duration)
      VISIT(List)
      VISIT(LargeList)
      VISIT(FixedSizeList)
      VISIT(Struct)
      VISIT(Map)
      VISIT(SparseUnion)
      VISIT(DenseUnion)
      VISIT(Dictionary)
      VISIT(RunEndEncoded)
      VISIT(Decimal128)
      VISIT(Decimal256)

#undef VISIT

    private:
      ArrayValueConverter* array_value_converter_;
      int64_t index_;
      VALUE result_;
    };
  }

  VALUE ArrayValueConverter::convert(const arrow::RunEndEncodedArray& array,
                                     const int64_t i) {
    auto physical_index = arrow::ree_util::FindPhysicalIndex(
      arrow::ArraySpan(*array.data()), i, array.offset());
    RunEndEncodedArrayValueConverter converter(this, physical_index);
    return converter.convert(*array.values());
  }
}
