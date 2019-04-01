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

#include <string>
#include <unordered_map>

#include "arrow/array.h"
#include "arrow/json/parser.h"
#include "arrow/util/task-group.h"

namespace arrow {
namespace json {

/// \brief interface for conversion of Arrays
///
/// This does not change nested structure:
/// - Arrays with non-nested type will never be converted to arrays with
///   nested type.
/// - Arrays with nested type will be converted to arrays with the same
///   nested type, modulo conversions applied to their child arrays.
/// - Children may not be added by a Converter, though some may be removed.
///
/// Converters are not required to be correct for arbitrary input- only
/// for unconverted arrays emitted by a corresponding parser.
class ARROW_EXPORT Converter {
 public:
  virtual ~Converter() = default;

  /// convert an array
  /// on failure, this converter may be promoted to another converter which
  /// *can* convert the given input.
  virtual Status Convert(const std::shared_ptr<Array>& in,
                         std::shared_ptr<Array>* out) = 0;

  MemoryPool* pool() { return pool_; }

 protected:
  ARROW_DISALLOW_COPY_AND_ASSIGN(Converter);

  Converter(MemoryPool* pool) : pool_(pool) {}

  MemoryPool* pool_;
};

/// \brief produce a single promotable converter
///
/// The output converter will be promotable.
Status MakeConverter(MemoryPool* pool, const std::shared_ptr<DataType>& out_type,
                     std::unique_ptr<Converter>* out);

/// \brief produce a flattened mapping from field paths to converters
///
/// Field paths are a concatenation of field names, delimited by '\0'.
/// For example, in a row {"a":{"b":{"c":13}}} the path 13 would be "a\0b\0c".
/// Fields are taken from options.explicit_schema
///
/// These converters will not be promotable.
std::unordered_map<std::string, std::unique_ptr<Converter>> MakeConverters(
    MemoryPool* pool, const ParseOptions& options);

}  // namespace json
}  // namespace arrow
