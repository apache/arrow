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

// This file is for tests only and is not installed as a public header.

#include <string>
#include <vector>

#include "arrow/extension/variant.h"

namespace arrow::extension::variant {

/// \brief A visitor that records all callbacks as a vector of strings
///        for easy assertion in tests.
class RecordingVisitor : public VariantVisitor {
 public:
  std::vector<std::string> events;

  Status Null() override {
    events.push_back("Null");
    return Status::OK();
  }
  Status Bool(bool value) override {
    events.push_back(std::string("Bool(") + (value ? "true" : "false") + ")");
    return Status::OK();
  }
  Status Int8(int8_t value) override {
    events.push_back("Int8(" + std::to_string(value) + ")");
    return Status::OK();
  }
  Status Int16(int16_t value) override {
    events.push_back("Int16(" + std::to_string(value) + ")");
    return Status::OK();
  }
  Status Int32(int32_t value) override {
    events.push_back("Int32(" + std::to_string(value) + ")");
    return Status::OK();
  }
  Status Int64(int64_t value) override {
    events.push_back("Int64(" + std::to_string(value) + ")");
    return Status::OK();
  }
  Status Float(float value) override {
    events.push_back("Float(" + std::to_string(value) + ")");
    return Status::OK();
  }
  Status Double(double value) override {
    events.push_back("Double(" + std::to_string(value) + ")");
    return Status::OK();
  }
  Status Decimal4(const uint8_t* /*bytes*/, int32_t scale) override {
    events.push_back("Decimal4(scale=" + std::to_string(scale) + ")");
    return Status::OK();
  }
  Status Decimal8(const uint8_t* /*bytes*/, int32_t scale) override {
    events.push_back("Decimal8(scale=" + std::to_string(scale) + ")");
    return Status::OK();
  }
  Status Decimal16(const uint8_t* /*bytes*/, int32_t scale) override {
    events.push_back("Decimal16(scale=" + std::to_string(scale) + ")");
    return Status::OK();
  }
  Status Date(int32_t days) override {
    events.push_back("Date(" + std::to_string(days) + ")");
    return Status::OK();
  }
  Status TimestampMicros(int64_t micros) override {
    events.push_back("TimestampMicros(" + std::to_string(micros) + ")");
    return Status::OK();
  }
  Status TimestampMicrosNTZ(int64_t micros) override {
    events.push_back("TimestampMicrosNTZ(" + std::to_string(micros) + ")");
    return Status::OK();
  }
  Status String(std::string_view value) override {
    events.push_back("String(\"" + std::string(value) + "\")");
    return Status::OK();
  }
  Status Binary(std::string_view value) override {
    events.push_back("Binary(len=" + std::to_string(value.size()) + ")");
    return Status::OK();
  }
  Status TimeNTZ(int64_t micros) override {
    events.push_back("TimeNTZ(" + std::to_string(micros) + ")");
    return Status::OK();
  }
  Status TimestampNanos(int64_t nanos) override {
    events.push_back("TimestampNanos(" + std::to_string(nanos) + ")");
    return Status::OK();
  }
  Status TimestampNanosNTZ(int64_t nanos) override {
    events.push_back("TimestampNanosNTZ(" + std::to_string(nanos) + ")");
    return Status::OK();
  }
  Status UUID(const uint8_t* /*bytes*/) override {
    events.push_back("UUID");
    return Status::OK();
  }
  Status StartObject(int32_t num_fields) override {
    events.push_back("StartObject(" + std::to_string(num_fields) + ")");
    return Status::OK();
  }
  Status FieldName(std::string_view name) override {
    events.push_back("FieldName(\"" + std::string(name) + "\")");
    return Status::OK();
  }
  Status EndObject() override {
    events.push_back("EndObject");
    return Status::OK();
  }
  Status StartArray(int32_t num_elements) override {
    events.push_back("StartArray(" + std::to_string(num_elements) + ")");
    return Status::OK();
  }
  Status EndArray() override {
    events.push_back("EndArray");
    return Status::OK();
  }
};

}  // namespace arrow::extension::variant
