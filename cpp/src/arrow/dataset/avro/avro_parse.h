/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once

#include "arrow/dataset/avro/avro_raits.h"
#include "arrow/dataset/avro/resolving_reader.h"

///
/// Standalone parse functions for Avro types.

namespace arrow {
namespace avro {

/// The main parse entry point function.  Takes a parser (either validating or
/// plain) and the object that should receive the parsed data.

template <typename Reader, typename T>
Status Parse(Reader* p, T* val) {
  return Parse(p, val, is_serializable<T>());
}

template <typename T>
Status Parse(ResolvingReader* p, T* val) {
  return TranslatingParse(p, val, is_serializable<T>());
}

/// Type trait should be set to is_serializable in otherwise force the compiler to
/// complain.

template <typename Reader, typename T>
void Parse(Reader* p, T* val, const std::false_type&) {
  static_assert(sizeof(T) == 0, "Not a valid type to parse");
}

template <typename Reader, typename T>
Status TranslatingParse(Reader* p, T* val, const std::false_type&) {
  static_assert(sizeof(T) == 0, "Not a valid type to parse");
}

// @{

/// The remainder of the file includes default implementations for serializable types.

template <typename Reader, typename T>
Status Parse(Reader* p, T* val, const std::true_type&) {
  return p.ReadValue(val);
}

template <typename Reader>
Status Parse(Reader* p, std::vector<uint8_t>* val, const std::true_type&) {
  return p.ReadBytes(val);
}

template <typename T>
Status TranslatingParse(ResolvingReader* p, T* val, const std::true_type&) {
  return p.Parse(val);
}

// @}

}  // namespace avro
}  // namespace arrow 

#endif
