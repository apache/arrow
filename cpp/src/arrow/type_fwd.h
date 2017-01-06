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

class DictionaryType;
class DictionaryArray;

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
using TimeArray = NumericArray<TimeType>;
using TimeBuilder = NumericBuilder<TimeType>;

struct TimestampType;
using TimestampArray = NumericArray<TimestampType>;
using TimestampBuilder = NumericBuilder<TimestampType>;

struct IntervalType;
using IntervalArray = NumericArray<IntervalType>;

}  // namespace arrow

#endif  // ARROW_TYPE_FWD_H
