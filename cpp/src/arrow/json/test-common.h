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

#include <algorithm>
#include <memory>
#include <random>
#include <string>
#include <vector>

#include <rapidjson/writer.h>

#include "arrow/testing/gtest_util.h"
#include "arrow/testing/util.h"
#include "arrow/util/string_view.h"
#include "arrow/visitor_inline.h"

namespace arrow {
namespace json {

using rapidjson::StringBuffer;
using Writer = rapidjson::Writer<StringBuffer>;

inline static Status OK(bool ok) { return ok ? Status::OK() : Status::Invalid(""); }

template <typename Engine>
static Status Generate(const std::shared_ptr<DataType>& type, Engine& e, Writer* writer);

template <typename Engine>
static Status Generate(const std::vector<std::shared_ptr<Field>>& fields, Engine& e,
                       Writer* writer);

template <typename Engine>
static Status Generate(const std::shared_ptr<Schema>& schm, Engine& e, Writer* writer) {
  return Generate(schm->fields(), e, writer);
}

template <typename Engine>
struct GenerateImpl {
  Status Visit(const BooleanType&) {
    return OK(writer.Bool(std::uniform_int_distribution<uint16_t>{}(e)&1));
  }
  template <typename T>
  Status Visit(T const&, enable_if_unsigned_integer<T>* = nullptr) {
    auto val = std::uniform_int_distribution<>{}(e);
    return OK(writer.Uint64(static_cast<typename T::c_type>(val)));
  }
  template <typename T>
  Status Visit(T const&, enable_if_signed_integer<T>* = nullptr) {
    auto val = std::uniform_int_distribution<>{}(e);
    return OK(writer.Int64(static_cast<typename T::c_type>(val)));
  }
  template <typename T>
  Status Visit(T const&, enable_if_floating_point<T>* = nullptr) {
    auto val = std::normal_distribution<typename T::c_type>{0, 1 << 10}(e);
    return OK(writer.Double(val));
  }
  Status Visit(HalfFloatType const&) {
    auto val = std::normal_distribution<double>{0, 1 << 10}(e);
    return OK(writer.Double(val));
  }
  template <typename T>
  Status Visit(T const&, enable_if_binary<T>* = nullptr) {
    auto size = std::poisson_distribution<>{4}(e);
    std::uniform_int_distribution<uint16_t> gen_char(32, 127);  // FIXME generate UTF8
    std::string s(size, '\0');
    for (char& ch : s) ch = static_cast<char>(gen_char(e));
    return OK(writer.String(s.c_str()));
  }
  template <typename T>
  Status Visit(
      T const& t, typename std::enable_if<!is_number<T>::value>::type* = nullptr,
      typename std::enable_if<!std::is_base_of<BinaryType, T>::value>::type* = nullptr) {
    return Status::Invalid("can't generate a value of type " + t.name());
  }
  Status Visit(const ListType& t) {
    auto size = std::poisson_distribution<>{4}(e);
    writer.StartArray();
    for (int i = 0; i != size; ++i) RETURN_NOT_OK(Generate(t.value_type(), e, &writer));
    return OK(writer.EndArray(size));
  }
  Status Visit(const StructType& t) { return Generate(t.children(), e, &writer); }
  Engine& e;
  rapidjson::Writer<rapidjson::StringBuffer>& writer;
};

template <typename Engine>
static Status Generate(const std::shared_ptr<DataType>& type, Engine& e, Writer* writer) {
  if (std::uniform_real_distribution<>{0, 1}(e) < .2) {
    // one out of 5 chance of null, anywhere
    writer->Null();
    return Status::OK();
  }
  GenerateImpl<Engine> visitor = {e, *writer};
  return VisitTypeInline(*type, &visitor);
}

template <typename Engine>
static Status Generate(const std::vector<std::shared_ptr<Field>>& fields, Engine& e,
                       Writer* writer) {
  RETURN_NOT_OK(OK(writer->StartObject()));
  for (const auto& f : fields) {
    writer->Key(f->name().c_str());
    RETURN_NOT_OK(Generate(f->type(), e, writer));
  }
  return OK(writer->EndObject(static_cast<int>(fields.size())));
}

Status MakeBuffer(util::string_view data, std::shared_ptr<Buffer>* out) {
  RETURN_NOT_OK(AllocateBuffer(default_memory_pool(), data.size(), out));
  std::copy(std::begin(data), std::end(data), (*out)->mutable_data());
  return Status::OK();
}

// scalar values (numbers and strings) are parsed into a
// dictionary<index:int32, value:string>. This can be decoded for ease of comparison
Status DecodeStringDictionary(const DictionaryArray& dict_array,
                              std::shared_ptr<Array>* decoded) {
  const StringArray& dict = static_cast<const StringArray&>(*dict_array.dictionary());
  const Int32Array& indices = static_cast<const Int32Array&>(*dict_array.indices());
  StringBuilder builder;
  RETURN_NOT_OK(builder.Resize(indices.length()));
  for (int64_t i = 0; i != indices.length(); ++i) {
    if (indices.IsNull(i)) {
      builder.UnsafeAppendNull();
      continue;
    }
    auto value = dict.GetView(indices.GetView(i));
    RETURN_NOT_OK(builder.ReserveData(value.size()));
    builder.UnsafeAppend(value);
  }
  return builder.Finish(decoded);
}

}  // namespace json
}  // namespace arrow
