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

#pragma once

#define PARQUET_VARIANT_DIRECT_APPEND_LIST(V)                                 \
  V(Int8, (int8_t value), value)                                              \
  V(Int16, (int16_t value), value)                                            \
  V(Int32, (int32_t value), value)                                            \
  V(Int64, (int64_t value), value)                                            \
  V(Float, (float value), value)                                              \
  V(Double, (double value), value)                                            \
  V(Binary, (std::string_view value), value)                                  \
  V(String, (std::string_view value), value)                                  \
  V(Date, (int32_t value), value)                                             \
  V(TimeNTZMicros, (int64_t value), value)                                    \
  V(Uuid, (std::string_view value), value)                                    \
  V(Decimal4, (const ::arrow::Decimal32& value, uint8_t scale), value, scale) \
  V(Decimal8, (const ::arrow::Decimal64& value, uint8_t scale), value, scale) \
  V(Decimal16, (const ::arrow::Decimal128& value, uint8_t scale), value, scale)

#define PARQUET_VARIANT_SPECIAL_APPEND_LIST(V)                                      \
  V(VariantNull, ())                                                                \
  V(Boolean, (bool value), value)                                                   \
  V(ShortString, (std::string_view value), value)                                   \
  V(TimestampMicros, (int64_t value, bool adjusted_to_utc), value, adjusted_to_utc) \
  V(TimestampNanos, (int64_t value, bool adjusted_to_utc), value, adjusted_to_utc)
