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

#ifndef ARROW_TYPE_FWD_H
#define ARROW_TYPE_FWD_H

namespace arrow {

class Status;

// Type forward declarations for the TypeVisitor
struct DataType;
struct Field;
struct NullType;
struct BooleanType;
struct Int8Type;
struct Int16Type;
struct Int32Type;
struct Int64Type;
struct UInt8Type;
struct UInt16Type;
struct UInt32Type;
struct UInt64Type;
struct HalfFloatType;
struct FloatType;
struct DoubleType;
struct StringType;
struct BinaryType;
struct DateType;
struct TimeType;
struct TimestampType;
struct DecimalType;
struct ListType;
struct StructType;
struct DenseUnionType;
struct SparseUnionType;

class TypeVisitor {
 public:
  virtual Status Visit(const NullType& type) = 0;
  virtual Status Visit(const BooleanType& type) = 0;
  virtual Status Visit(const Int8Type& type) = 0;
  virtual Status Visit(const Int16Type& type) = 0;
  virtual Status Visit(const Int32Type& type) = 0;
  virtual Status Visit(const Int64Type& type) = 0;
  virtual Status Visit(const UInt8Type& type) = 0;
  virtual Status Visit(const UInt16Type& type) = 0;
  virtual Status Visit(const UInt32Type& type) = 0;
  virtual Status Visit(const UInt64Type& type) = 0;
  virtual Status Visit(const HalfFloatType& type) = 0;
  virtual Status Visit(const FloatType& type) = 0;
  virtual Status Visit(const DoubleType& type) = 0;
  virtual Status Visit(const StringType& type) = 0;
  virtual Status Visit(const BinaryType& type) = 0;
  virtual Status Visit(const DateType& type) = 0;
  virtual Status Visit(const TimeType& type) = 0;
  virtual Status Visit(const TimestampType& type) = 0;
  virtual Status Visit(const DecimalType& type) = 0;
  virtual Status Visit(const ListType& type) = 0;
  virtual Status Visit(const StructType& type) = 0;
  virtual Status Visit(const DenseUnionType& type) = 0;
  virtual Status Visit(const SparseUnionType& type) = 0;
};

class NullArray;
class BooleanArray;
class StringArray;
class BinaryArray;
class DecimalArray;
class ListArray;
class StructArray;
class DenseUnionArray;
class SparseUnionArray;

template <typename TypeClass>
class NumericArray;

class DateArray;
class TimeArray;

using HalfFloatArray = NumericArray<HalfFloatType>;
using FloatArray = NumericArray<FloatType>;
using DoubleArray = NumericArray<DoubleType>;
using Int8Array = NumericArray<Int8Type>;
using UInt8Array = NumericArray<UInt8Type>;
using Int16Array = NumericArray<Int16Type>;
using UInt16Array = NumericArray<UInt16Type>;
using Int32Array = NumericArray<Int32Type>;
using UInt32Array = NumericArray<UInt32Type>;
using Int64Array = NumericArray<Int64Type>;
using UInt64Array = NumericArray<UInt64Type>;
using TimestampArray = NumericArray<TimestampType>;

class ArrayVisitor {
 public:
  virtual Status Visit(const NullArray& array) = 0;
  virtual Status Visit(const BooleanArray& array) = 0;
  virtual Status Visit(const Int8Array& array) = 0;
  virtual Status Visit(const Int16Array& array) = 0;
  virtual Status Visit(const Int32Array& array) = 0;
  virtual Status Visit(const Int64Array& array) = 0;
  virtual Status Visit(const UInt8Array& array) = 0;
  virtual Status Visit(const UInt16Array& array) = 0;
  virtual Status Visit(const UInt32Array& array) = 0;
  virtual Status Visit(const UInt64Array& array) = 0;
  virtual Status Visit(const HalfFloatArray& array) = 0;
  virtual Status Visit(const FloatArray& array) = 0;
  virtual Status Visit(const DoubleArray& array) = 0;
  virtual Status Visit(const StringArray& array) = 0;
  virtual Status Visit(const BinaryArray& array) = 0;
  virtual Status Visit(const DateArray& array) = 0;
  virtual Status Visit(const TimeArray& array) = 0;
  virtual Status Visit(const TimestampArray& array) = 0;
  virtual Status Visit(const DecimalArray& array) = 0;
  virtual Status Visit(const ListArray& array) = 0;
  virtual Status Visit(const StructArray& array) = 0;
  virtual Status Visit(const DenseUnionArray& array) = 0;
  virtual Status Visit(const SparseUnionArray& array) = 0;
};

}  // namespace arrow

#endif  // ARROW_TYPE_FWD_H
