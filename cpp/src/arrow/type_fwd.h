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

struct DataType;
class Array;
class ArrayBuilder;
struct Field;

class Buffer;
class MemoryPool;
class RecordBatch;
class Schema;

struct NullType;
class NullArray;

struct BooleanType;
class BooleanArray;
class BooleanBuilder;

struct BinaryType;
class BinaryArray;
class BinaryBuilder;

struct StringType;
class StringArray;
class StringBuilder;

struct ListType;
class ListArray;
class ListBuilder;

struct StructType;
class StructArray;
class StructBuilder;

struct DecimalType;
class DecimalArray;

struct UnionType;
class UnionArray;

template <typename TypeClass>
class NumericArray;

template <typename TypeClass>
class NumericBuilder;

#define _NUMERIC_TYPE_DECL(KLASS)                 \
  struct KLASS##Type;                             \
  using KLASS##Array = NumericArray<KLASS##Type>; \
  using KLASS##Builder = NumericBuilder<KLASS##Type>;

_NUMERIC_TYPE_DECL(Int8);
_NUMERIC_TYPE_DECL(Int16);
_NUMERIC_TYPE_DECL(Int32);
_NUMERIC_TYPE_DECL(Int64);
_NUMERIC_TYPE_DECL(UInt8);
_NUMERIC_TYPE_DECL(UInt16);
_NUMERIC_TYPE_DECL(UInt32);
_NUMERIC_TYPE_DECL(UInt64);
_NUMERIC_TYPE_DECL(HalfFloat);
_NUMERIC_TYPE_DECL(Float);
_NUMERIC_TYPE_DECL(Double);

#undef _NUMERIC_TYPE_DECL

struct DateType;
using DateArray = NumericArray<DateType>;
using DateBuilder = NumericBuilder<DateType>;

struct TimeType;
class TimeArray;

struct TimestampType;
using TimestampArray = NumericArray<TimestampType>;
using TimestampBuilder = NumericBuilder<TimestampType>;

struct IntervalType;
using IntervalArray = NumericArray<IntervalType>;

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
  virtual Status Visit(const IntervalType& type) = 0;
  virtual Status Visit(const DecimalType& type) = 0;
  virtual Status Visit(const ListType& type) = 0;
  virtual Status Visit(const StructType& type) = 0;
  virtual Status Visit(const UnionType& type) = 0;
};

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
  virtual Status Visit(const IntervalArray& array) = 0;
  virtual Status Visit(const DecimalArray& array) = 0;
  virtual Status Visit(const ListArray& array) = 0;
  virtual Status Visit(const StructArray& array) = 0;
  virtual Status Visit(const UnionArray& array) = 0;
};

}  // namespace arrow

#endif  // ARROW_TYPE_FWD_H
