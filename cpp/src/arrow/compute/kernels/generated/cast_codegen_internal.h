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

// THIS FILE IS AUTOMATICALLY GENERATED, DO NOT EDIT
// Generated by codegen.py script
#define BOOLEAN_CASES(TEMPLATE) \
  TEMPLATE(BooleanType, UInt8Type) \
  TEMPLATE(BooleanType, Int8Type) \
  TEMPLATE(BooleanType, UInt16Type) \
  TEMPLATE(BooleanType, Int16Type) \
  TEMPLATE(BooleanType, UInt32Type) \
  TEMPLATE(BooleanType, Int32Type) \
  TEMPLATE(BooleanType, UInt64Type) \
  TEMPLATE(BooleanType, Int64Type) \
  TEMPLATE(BooleanType, FloatType) \
  TEMPLATE(BooleanType, DoubleType) \
  TEMPLATE(BooleanType, StringType) \
  TEMPLATE(BooleanType, LargeStringType)

#define UINT8_CASES(TEMPLATE) \
  TEMPLATE(UInt8Type, BooleanType) \
  TEMPLATE(UInt8Type, Int8Type) \
  TEMPLATE(UInt8Type, UInt16Type) \
  TEMPLATE(UInt8Type, Int16Type) \
  TEMPLATE(UInt8Type, UInt32Type) \
  TEMPLATE(UInt8Type, Int32Type) \
  TEMPLATE(UInt8Type, UInt64Type) \
  TEMPLATE(UInt8Type, Int64Type) \
  TEMPLATE(UInt8Type, FloatType) \
  TEMPLATE(UInt8Type, DoubleType) \
  TEMPLATE(UInt8Type, StringType) \
  TEMPLATE(UInt8Type, LargeStringType)

#define INT8_CASES(TEMPLATE) \
  TEMPLATE(Int8Type, BooleanType) \
  TEMPLATE(Int8Type, UInt8Type) \
  TEMPLATE(Int8Type, UInt16Type) \
  TEMPLATE(Int8Type, Int16Type) \
  TEMPLATE(Int8Type, UInt32Type) \
  TEMPLATE(Int8Type, Int32Type) \
  TEMPLATE(Int8Type, UInt64Type) \
  TEMPLATE(Int8Type, Int64Type) \
  TEMPLATE(Int8Type, FloatType) \
  TEMPLATE(Int8Type, DoubleType) \
  TEMPLATE(Int8Type, StringType) \
  TEMPLATE(Int8Type, LargeStringType)

#define UINT16_CASES(TEMPLATE) \
  TEMPLATE(UInt16Type, BooleanType) \
  TEMPLATE(UInt16Type, UInt8Type) \
  TEMPLATE(UInt16Type, Int8Type) \
  TEMPLATE(UInt16Type, Int16Type) \
  TEMPLATE(UInt16Type, UInt32Type) \
  TEMPLATE(UInt16Type, Int32Type) \
  TEMPLATE(UInt16Type, UInt64Type) \
  TEMPLATE(UInt16Type, Int64Type) \
  TEMPLATE(UInt16Type, FloatType) \
  TEMPLATE(UInt16Type, DoubleType) \
  TEMPLATE(UInt16Type, StringType) \
  TEMPLATE(UInt16Type, LargeStringType)

#define INT16_CASES(TEMPLATE) \
  TEMPLATE(Int16Type, BooleanType) \
  TEMPLATE(Int16Type, UInt8Type) \
  TEMPLATE(Int16Type, Int8Type) \
  TEMPLATE(Int16Type, UInt16Type) \
  TEMPLATE(Int16Type, UInt32Type) \
  TEMPLATE(Int16Type, Int32Type) \
  TEMPLATE(Int16Type, UInt64Type) \
  TEMPLATE(Int16Type, Int64Type) \
  TEMPLATE(Int16Type, FloatType) \
  TEMPLATE(Int16Type, DoubleType) \
  TEMPLATE(Int16Type, StringType) \
  TEMPLATE(Int16Type, LargeStringType)

#define UINT32_CASES(TEMPLATE) \
  TEMPLATE(UInt32Type, BooleanType) \
  TEMPLATE(UInt32Type, UInt8Type) \
  TEMPLATE(UInt32Type, Int8Type) \
  TEMPLATE(UInt32Type, UInt16Type) \
  TEMPLATE(UInt32Type, Int16Type) \
  TEMPLATE(UInt32Type, Int32Type) \
  TEMPLATE(UInt32Type, UInt64Type) \
  TEMPLATE(UInt32Type, Int64Type) \
  TEMPLATE(UInt32Type, FloatType) \
  TEMPLATE(UInt32Type, DoubleType) \
  TEMPLATE(UInt32Type, StringType) \
  TEMPLATE(UInt32Type, LargeStringType)

#define UINT64_CASES(TEMPLATE) \
  TEMPLATE(UInt64Type, BooleanType) \
  TEMPLATE(UInt64Type, UInt8Type) \
  TEMPLATE(UInt64Type, Int8Type) \
  TEMPLATE(UInt64Type, UInt16Type) \
  TEMPLATE(UInt64Type, Int16Type) \
  TEMPLATE(UInt64Type, UInt32Type) \
  TEMPLATE(UInt64Type, Int32Type) \
  TEMPLATE(UInt64Type, Int64Type) \
  TEMPLATE(UInt64Type, FloatType) \
  TEMPLATE(UInt64Type, DoubleType) \
  TEMPLATE(UInt64Type, StringType) \
  TEMPLATE(UInt64Type, LargeStringType)

#define INT32_CASES(TEMPLATE) \
  TEMPLATE(Int32Type, BooleanType) \
  TEMPLATE(Int32Type, UInt8Type) \
  TEMPLATE(Int32Type, Int8Type) \
  TEMPLATE(Int32Type, UInt16Type) \
  TEMPLATE(Int32Type, Int16Type) \
  TEMPLATE(Int32Type, UInt32Type) \
  TEMPLATE(Int32Type, UInt64Type) \
  TEMPLATE(Int32Type, Int64Type) \
  TEMPLATE(Int32Type, FloatType) \
  TEMPLATE(Int32Type, DoubleType) \
  TEMPLATE(Int32Type, StringType) \
  TEMPLATE(Int32Type, LargeStringType)

#define INT64_CASES(TEMPLATE) \
  TEMPLATE(Int64Type, BooleanType) \
  TEMPLATE(Int64Type, UInt8Type) \
  TEMPLATE(Int64Type, Int8Type) \
  TEMPLATE(Int64Type, UInt16Type) \
  TEMPLATE(Int64Type, Int16Type) \
  TEMPLATE(Int64Type, UInt32Type) \
  TEMPLATE(Int64Type, Int32Type) \
  TEMPLATE(Int64Type, UInt64Type) \
  TEMPLATE(Int64Type, FloatType) \
  TEMPLATE(Int64Type, DoubleType) \
  TEMPLATE(Int64Type, StringType) \
  TEMPLATE(Int64Type, LargeStringType)

#define FLOAT_CASES(TEMPLATE) \
  TEMPLATE(FloatType, BooleanType) \
  TEMPLATE(FloatType, UInt8Type) \
  TEMPLATE(FloatType, Int8Type) \
  TEMPLATE(FloatType, UInt16Type) \
  TEMPLATE(FloatType, Int16Type) \
  TEMPLATE(FloatType, UInt32Type) \
  TEMPLATE(FloatType, Int32Type) \
  TEMPLATE(FloatType, UInt64Type) \
  TEMPLATE(FloatType, Int64Type) \
  TEMPLATE(FloatType, DoubleType) \
  TEMPLATE(FloatType, StringType) \
  TEMPLATE(FloatType, LargeStringType)

#define DOUBLE_CASES(TEMPLATE) \
  TEMPLATE(DoubleType, BooleanType) \
  TEMPLATE(DoubleType, UInt8Type) \
  TEMPLATE(DoubleType, Int8Type) \
  TEMPLATE(DoubleType, UInt16Type) \
  TEMPLATE(DoubleType, Int16Type) \
  TEMPLATE(DoubleType, UInt32Type) \
  TEMPLATE(DoubleType, Int32Type) \
  TEMPLATE(DoubleType, UInt64Type) \
  TEMPLATE(DoubleType, Int64Type) \
  TEMPLATE(DoubleType, FloatType) \
  TEMPLATE(DoubleType, StringType) \
  TEMPLATE(DoubleType, LargeStringType)

#define DATE32_CASES(TEMPLATE) \
  TEMPLATE(Date32Type, Date64Type)

#define DATE64_CASES(TEMPLATE) \
  TEMPLATE(Date64Type, Date32Type)

#define TIME32_CASES(TEMPLATE) \
  TEMPLATE(Time32Type, Time32Type) \
  TEMPLATE(Time32Type, Time64Type)

#define TIME64_CASES(TEMPLATE) \
  TEMPLATE(Time64Type, Time32Type) \
  TEMPLATE(Time64Type, Time64Type)

#define TIMESTAMP_CASES(TEMPLATE) \
  TEMPLATE(TimestampType, Date32Type) \
  TEMPLATE(TimestampType, Date64Type) \
  TEMPLATE(TimestampType, TimestampType)

#define DURATION_CASES(TEMPLATE) \
  TEMPLATE(DurationType, DurationType)

#define BINARY_CASES(TEMPLATE) \
  TEMPLATE(BinaryType, StringType)

#define LARGEBINARY_CASES(TEMPLATE) \
  TEMPLATE(LargeBinaryType, LargeStringType)

#define STRING_CASES(TEMPLATE) \
  TEMPLATE(StringType, BooleanType) \
  TEMPLATE(StringType, UInt8Type) \
  TEMPLATE(StringType, Int8Type) \
  TEMPLATE(StringType, UInt16Type) \
  TEMPLATE(StringType, Int16Type) \
  TEMPLATE(StringType, UInt32Type) \
  TEMPLATE(StringType, Int32Type) \
  TEMPLATE(StringType, UInt64Type) \
  TEMPLATE(StringType, Int64Type) \
  TEMPLATE(StringType, FloatType) \
  TEMPLATE(StringType, DoubleType) \
  TEMPLATE(StringType, TimestampType)

#define LARGESTRING_CASES(TEMPLATE) \
  TEMPLATE(LargeStringType, BooleanType) \
  TEMPLATE(LargeStringType, UInt8Type) \
  TEMPLATE(LargeStringType, Int8Type) \
  TEMPLATE(LargeStringType, UInt16Type) \
  TEMPLATE(LargeStringType, Int16Type) \
  TEMPLATE(LargeStringType, UInt32Type) \
  TEMPLATE(LargeStringType, Int32Type) \
  TEMPLATE(LargeStringType, UInt64Type) \
  TEMPLATE(LargeStringType, Int64Type) \
  TEMPLATE(LargeStringType, FloatType) \
  TEMPLATE(LargeStringType, DoubleType) \
  TEMPLATE(LargeStringType, TimestampType)

#define DICTIONARY_CASES(TEMPLATE) \
  TEMPLATE(DictionaryType, UInt8Type) \
  TEMPLATE(DictionaryType, Int8Type) \
  TEMPLATE(DictionaryType, UInt16Type) \
  TEMPLATE(DictionaryType, Int16Type) \
  TEMPLATE(DictionaryType, UInt32Type) \
  TEMPLATE(DictionaryType, Int32Type) \
  TEMPLATE(DictionaryType, UInt64Type) \
  TEMPLATE(DictionaryType, Int64Type) \
  TEMPLATE(DictionaryType, FloatType) \
  TEMPLATE(DictionaryType, DoubleType) \
  TEMPLATE(DictionaryType, Date32Type) \
  TEMPLATE(DictionaryType, Date64Type) \
  TEMPLATE(DictionaryType, Time32Type) \
  TEMPLATE(DictionaryType, Time64Type) \
  TEMPLATE(DictionaryType, TimestampType) \
  TEMPLATE(DictionaryType, DurationType) \
  TEMPLATE(DictionaryType, NullType) \
  TEMPLATE(DictionaryType, BinaryType) \
  TEMPLATE(DictionaryType, FixedSizeBinaryType) \
  TEMPLATE(DictionaryType, StringType) \
  TEMPLATE(DictionaryType, Decimal128Type)
