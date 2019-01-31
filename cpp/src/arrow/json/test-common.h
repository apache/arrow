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

#include <random>
#include <sstream>
#include <string>

#include "arrow/test-util.h"
#include "arrow/util/string_view.h"
#include "arrow/visitor_inline.h"

namespace arrow {
namespace json {

template <typename Engine>
static Status GenerateValue(const std::shared_ptr<DataType>& type, Engine& e,
                            std::ostream& os);

template <typename Engine>
static Status GenerateObject(const std::vector<std::shared_ptr<Field>>& fields, Engine& e,
                             std::ostream& os);

template <typename Element>
static Status CommaDelimited(int count, std::ostream& os, Element&& element);

template <typename Engine>
struct GenerateValueImpl {
  template <typename T>
  Status Visit(T const&, enable_if_integer<T>* = nullptr) {
    auto val = std::uniform_int_distribution<>{}(e);
    os << static_cast<typename T::c_type>(val);
    return Status::OK();
  }
  template <typename T>
  Status Visit(T const&, enable_if_floating_point<T>* = nullptr) {
    os << std::normal_distribution<typename T::c_type>{0, 1 << 10}(e);
    return Status::OK();
  }
  Status Visit(HalfFloatType const&) {
    os << std::uniform_int_distribution<HalfFloatType::c_type>{}(e);
    return Status::OK();
  }
  template <typename T>
  Status Visit(T const&, enable_if_binary<T>* = nullptr) {
    auto size = std::poisson_distribution<>{4}(e);
    std::uniform_int_distribution<short> gen_char(32, 127);  // FIXME generate UTF8
    std::string s(size, '\0');
    for (char& ch : s) ch = static_cast<char>(gen_char(e));
    os << std::quoted(s);
    return Status::OK();
  }
  template <typename T>
  Status Visit(
      T const& t, typename std::enable_if<!is_number<T>::value>::type* = nullptr,
      typename std::enable_if<!std::is_base_of<BinaryType, T>::value>::type* = nullptr) {
    return Status::Invalid("can't generate a value of type " + t.name());
  }
  Status Visit(const ListType& t) {
    auto size = std::poisson_distribution<>{4}(e);
    os << "[";
    RETURN_NOT_OK(CommaDelimited(
        size, os, [&](int) { return GenerateValue(t.value_type(), e, os); }));
    os << "]";
    return Status::OK();
  }
  Status Visit(const StructType& t) { return GenerateObject(t.children(), e, os); }
  Engine& e;
  std::ostream& os;
};

template <typename Engine>
static Status GenerateValue(const std::shared_ptr<DataType>& type, Engine& e,
                            std::ostream& os) {
  if (std::uniform_real_distribution<>{0, 1}(e) < .2) {
    // one out of 5 chance of null, anywhere
    os << "null";
    return Status::OK();
  }
  GenerateValueImpl<Engine> visitor{e, os};
  return VisitTypeInline(*type, &visitor);
}

template <typename Element>
static Status CommaDelimited(int count, std::ostream& os, Element&& element) {
  bool first = true;
  for (int i = 0; i != count; ++i) {
    if (first)
      first = false;
    else
      os << ",";
    RETURN_NOT_OK(element(i));
  }
  return Status::OK();
}

template <typename Engine>
static Status GenerateObject(const std::vector<std::shared_ptr<Field>>& fields, Engine& e,
                             std::ostream& os) {
  os << "{";
  RETURN_NOT_OK(CommaDelimited(static_cast<int>(fields.size()), os, [&](int i) {
    os << std::quoted(fields[i]->name()) << ":";
    return GenerateValue(fields[i]->type(), e, os);
  }));
  os << "}";
  return Status::OK();
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
