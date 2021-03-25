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

#include <memory>

#include "arrow/csv/converter.h"
#include "arrow/csv/options.h"
#include "arrow/util/logging.h"

namespace arrow {
namespace csv {

enum class InferKind {
  Null,
  Integer,
  Boolean,
  Real,
  Date,
  Timestamp,
  TimestampNS,
  TextDict,
  BinaryDict,
  Text,
  Binary
};

class InferStatus {
 public:
  explicit InferStatus(const ConvertOptions& options)
      : kind_(InferKind::Null), can_loosen_type_(true), options_(options) {}

  InferKind kind() const { return kind_; }

  bool can_loosen_type() const { return can_loosen_type_; }

  void LoosenType(const Status& conversion_error) {
    DCHECK(can_loosen_type_);

    switch (kind_) {
      case InferKind::Null:
        return SetKind(InferKind::Integer);
      case InferKind::Integer:
        return SetKind(InferKind::Boolean);
      case InferKind::Boolean:
        return SetKind(InferKind::Date);
      case InferKind::Date:
        return SetKind(InferKind::Timestamp);
      case InferKind::Timestamp:
        return SetKind(InferKind::TimestampNS);
      case InferKind::TimestampNS:
        return SetKind(InferKind::Real);
      case InferKind::Real:
        if (options_.auto_dict_encode) {
          return SetKind(InferKind::TextDict);
        } else {
          return SetKind(InferKind::Text);
        }
      case InferKind::TextDict:
        if (conversion_error.IsIndexError()) {
          // Cardinality too large, fall back to non-dict encoding
          return SetKind(InferKind::Text);
        } else {
          // Assuming UTF8 validation failure
          return SetKind(InferKind::BinaryDict);
        }
        break;
      case InferKind::BinaryDict:
        // Assuming cardinality too large
        return SetKind(InferKind::Binary);
      case InferKind::Text:
        // Assuming UTF8 validation failure
        return SetKind(InferKind::Binary);
      default:
        ARROW_LOG(FATAL) << "Shouldn't come here";
    }
  }

  Result<std::shared_ptr<Converter>> MakeConverter(MemoryPool* pool) {
    auto make_converter =
        [&](std::shared_ptr<DataType> type) -> Result<std::shared_ptr<Converter>> {
      return Converter::Make(type, options_, pool);
    };

    auto make_dict_converter =
        [&](std::shared_ptr<DataType> type) -> Result<std::shared_ptr<Converter>> {
      ARROW_ASSIGN_OR_RAISE(auto dict_converter,
                            DictionaryConverter::Make(type, options_, pool));
      dict_converter->SetMaxCardinality(options_.auto_dict_max_cardinality);
      return dict_converter;
    };

    switch (kind_) {
      case InferKind::Null:
        return make_converter(null());
      case InferKind::Integer:
        return make_converter(int64());
      case InferKind::Boolean:
        return make_converter(boolean());
      case InferKind::Date:
        return make_converter(date32());
      case InferKind::Timestamp:
        return make_converter(timestamp(TimeUnit::SECOND));
      case InferKind::TimestampNS:
        return make_converter(timestamp(TimeUnit::NANO));
      case InferKind::Real:
        return make_converter(float64());
      case InferKind::Text:
        return make_converter(utf8());
      case InferKind::Binary:
        return make_converter(binary());
      case InferKind::TextDict:
        return make_dict_converter(utf8());
      case InferKind::BinaryDict:
        return make_dict_converter(binary());
    }
    return Status::UnknownError("Shouldn't come here");
  }

 protected:
  void SetKind(InferKind kind) {
    kind_ = kind;
    if (kind == InferKind::Binary) {
      // Binary is the catch-all type
      can_loosen_type_ = false;
    }
  }

  InferKind kind_;
  bool can_loosen_type_;
  const ConvertOptions& options_;
};

}  // namespace csv
}  // namespace arrow
