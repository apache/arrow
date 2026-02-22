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

#include "arrow/flight/sql/odbc/odbc_impl/json_converter.h"

#include <rapidjson/rapidjson.h>
#include <rapidjson/writer.h>
#include <boost/beast/core/detail/base64.hpp>
#include "arrow/builder.h"
#include "arrow/flight/sql/odbc/odbc_impl/util.h"
#include "arrow/scalar.h"
#include "arrow/visitor.h"

using boost::beast::detail::base64::encode;
using boost::beast::detail::base64::encoded_size;
namespace base64 = boost::beast::detail::base64;

namespace arrow::flight::sql::odbc {

using util::ThrowIfNotOK;

namespace {
template <typename ScalarT>
Status ConvertScalarToStringAndWrite(const ScalarT& scalar,
                                     rapidjson::Writer<rapidjson::StringBuffer>& writer) {
  ARROW_ASSIGN_OR_RAISE(auto string_scalar, scalar.CastTo(arrow::utf8()))
  const auto& view = reinterpret_cast<StringScalar*>(string_scalar.get())->view();
  writer.String(view.data(), view.length(), true);
  return Status::OK();
}

template <typename BinaryScalarT>
Status ConvertBinaryToBase64StringAndWrite(
    const BinaryScalarT& scalar, rapidjson::Writer<rapidjson::StringBuffer>& writer) {
  const auto& view = scalar.view();
  size_t encoded_size = base64::encoded_size(view.length());
  std::vector<char> encoded(std::max(encoded_size, static_cast<size_t>(1)));
  base64::encode(&encoded[0], view.data(), view.length());
  writer.String(&encoded[0], encoded_size, true);
  return Status::OK();
}

template <typename ListScalarT>
Status WriteListScalar(const ListScalarT& scalar,
                       rapidjson::Writer<rapidjson::StringBuffer>& writer,
                       ScalarVisitor* visitor) {
  writer.StartArray();
  for (int64_t i = 0; i < scalar.value->length(); ++i) {
    if (scalar.value->IsNull(i)) {
      writer.Null();
    } else {
      const auto& result = scalar.value->GetScalar(i);
      ThrowIfNotOK(result.status());
      ThrowIfNotOK(result.ValueOrDie()->Accept(visitor));
    }
  }

  writer.EndArray();
  return Status::OK();
}

class ScalarToJson : public ScalarVisitor {
 private:
  rapidjson::StringBuffer string_buffer_;
  rapidjson::Writer<rapidjson::StringBuffer> writer_{string_buffer_};

 public:
  void Reset() {
    string_buffer_.Clear();
    writer_.Reset(string_buffer_);
  }

  std::string ToString() { return string_buffer_.GetString(); }

  Status Visit(const NullScalar& scalar) override {
    writer_.Null();

    return Status::OK();
  }

  Status Visit(const BooleanScalar& scalar) override {
    writer_.Bool(scalar.value);

    return Status::OK();
  }

  Status Visit(const Int8Scalar& scalar) override {
    writer_.Int(scalar.value);

    return Status::OK();
  }

  Status Visit(const Int16Scalar& scalar) override {
    writer_.Int(scalar.value);

    return Status::OK();
  }

  Status Visit(const Int32Scalar& scalar) override {
    writer_.Int(scalar.value);

    return Status::OK();
  }

  Status Visit(const Int64Scalar& scalar) override {
    writer_.Int64(scalar.value);

    return Status::OK();
  }

  Status Visit(const UInt8Scalar& scalar) override {
    writer_.Uint(scalar.value);

    return Status::OK();
  }

  Status Visit(const UInt16Scalar& scalar) override {
    writer_.Uint(scalar.value);

    return Status::OK();
  }

  Status Visit(const UInt32Scalar& scalar) override {
    writer_.Uint(scalar.value);

    return Status::OK();
  }

  Status Visit(const UInt64Scalar& scalar) override {
    writer_.Uint64(scalar.value);

    return Status::OK();
  }

  Status Visit(const HalfFloatScalar& scalar) override {
    return Status::NotImplemented("Cannot convert HalfFloatScalar to JSON.");
  }

  Status Visit(const FloatScalar& scalar) override {
    writer_.Double(scalar.value);

    return Status::OK();
  }

  Status Visit(const DoubleScalar& scalar) override {
    writer_.Double(scalar.value);

    return Status::OK();
  }

  Status Visit(const StringScalar& scalar) override {
    const auto& view = scalar.view();
    writer_.String(view.data(), view.length());

    return Status::OK();
  }

  Status Visit(const BinaryScalar& scalar) override {
    return ConvertBinaryToBase64StringAndWrite(scalar, writer_);
  }

  Status Visit(const LargeStringScalar& scalar) override {
    const auto& view = scalar.view();
    writer_.String(view.data(), view.length());

    return Status::OK();
  }

  Status Visit(const LargeBinaryScalar& scalar) override {
    return ConvertBinaryToBase64StringAndWrite(scalar, writer_);
  }

  Status Visit(const FixedSizeBinaryScalar& scalar) override {
    return ConvertBinaryToBase64StringAndWrite(scalar, writer_);
  }

  Status Visit(const Date64Scalar& scalar) override {
    return ConvertScalarToStringAndWrite(scalar, writer_);
  }

  Status Visit(const Date32Scalar& scalar) override {
    return ConvertScalarToStringAndWrite(scalar, writer_);
  }

  Status Visit(const Time32Scalar& scalar) override {
    return ConvertScalarToStringAndWrite(scalar, writer_);
  }

  Status Visit(const Time64Scalar& scalar) override {
    return ConvertScalarToStringAndWrite(scalar, writer_);
  }

  Status Visit(const TimestampScalar& scalar) override {
    return ConvertScalarToStringAndWrite(scalar, writer_);
  }

  Status Visit(const DayTimeIntervalScalar& scalar) override {
    return ConvertScalarToStringAndWrite(scalar, writer_);
  }

  Status Visit(const MonthDayNanoIntervalScalar& scalar) override {
    return ConvertScalarToStringAndWrite(scalar, writer_);
  }

  Status Visit(const MonthIntervalScalar& scalar) override {
    return ConvertScalarToStringAndWrite(scalar, writer_);
  }

  Status Visit(const DurationScalar& scalar) override {
    // GH-47857 TODO: Append TimeUnit on conversion
    return ConvertScalarToStringAndWrite(scalar, writer_);
  }

  Status Visit(const Decimal128Scalar& scalar) override {
    const auto& view = scalar.ToString();
    writer_.RawValue(view.data(), view.length(), rapidjson::kNumberType);

    return Status::OK();
  }

  Status Visit(const Decimal256Scalar& scalar) override {
    const auto& view = scalar.ToString();
    writer_.RawValue(view.data(), view.length(), rapidjson::kNumberType);

    return Status::OK();
  }

  Status Visit(const ListScalar& scalar) override {
    return WriteListScalar(scalar, writer_, this);
  }

  Status Visit(const LargeListScalar& scalar) override {
    return WriteListScalar(scalar, writer_, this);
  }

  Status Visit(const MapScalar& scalar) override {
    return WriteListScalar(scalar, writer_, this);
  }

  Status Visit(const FixedSizeListScalar& scalar) override {
    return WriteListScalar(scalar, writer_, this);
  }

  Status Visit(const StructScalar& scalar) override {
    writer_.StartObject();

    const std::shared_ptr<StructType>& data_type =
        std::static_pointer_cast<StructType>(scalar.type);
    for (int i = 0; i < data_type->num_fields(); ++i) {
      const auto& result = scalar.field(i);
      ThrowIfNotOK(result.status());
      const auto& value = result.ValueOrDie();
      writer_.Key(data_type->field(i)->name().c_str());
      if (value->is_valid) {
        ThrowIfNotOK(value->Accept(this));
      } else {
        writer_.Null();
      }
    }
    writer_.EndObject();
    return Status::OK();
  }

  Status Visit(const DictionaryScalar& scalar) override {
    return Status::NotImplemented("Cannot convert DictionaryScalar to JSON.");
  }

  Status Visit(const SparseUnionScalar& scalar) override {
    return scalar.child_value().get()->Accept(this);
  }

  Status Visit(const DenseUnionScalar& scalar) override {
    return scalar.child_value().get()->Accept(this);
  }

  Status Visit(const ExtensionScalar& scalar) override {
    return Status::NotImplemented("Cannot convert ExtensionScalar to JSON.");
  }
};
}  // namespace

std::string ConvertToJson(const Scalar& scalar) {
  static thread_local ScalarToJson converter;
  converter.Reset();
  ThrowIfNotOK(scalar.Accept(&converter));

  return converter.ToString();
}

arrow::Result<std::shared_ptr<Array>> ConvertToJson(const std::shared_ptr<Array>& input) {
  StringBuilder builder;
  int64_t length = input->length();
  RETURN_NOT_OK(builder.ReserveData(length));

  for (int64_t i = 0; i < length; ++i) {
    if (input->IsNull(i)) {
      RETURN_NOT_OK(builder.AppendNull());
    } else {
      ARROW_ASSIGN_OR_RAISE(auto scalar, input->GetScalar(i))
      RETURN_NOT_OK(builder.Append(ConvertToJson(*scalar)));
    }
  }

  return builder.Finish();
}

}  // namespace arrow::flight::sql::odbc
