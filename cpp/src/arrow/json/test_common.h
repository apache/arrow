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
#include <random>
#include <sstream>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "arrow/array.h"
#include "arrow/array/builder_binary.h"
#include "arrow/io/memory.h"
#include "arrow/json/converter.h"
#include "arrow/json/options.h"
#include "arrow/json/parser.h"
#include "arrow/json/rapidjson_defs.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/type.h"
#include "arrow/util/checked_cast.h"
#include "arrow/visit_type_inline.h"

#include "rapidjson/document.h"
#include "rapidjson/prettywriter.h"
#include "rapidjson/reader.h"
#include "rapidjson/writer.h"

namespace arrow {

using internal::checked_cast;

namespace json {

namespace rj = arrow::rapidjson;

using rj::StringBuffer;
using std::string_view;
using Writer = rj::Writer<StringBuffer>;

struct GenerateOptions {
  // Probability of a field being written
  double field_probability = 1.0;
  // Probability of a value being null
  double null_probability = 0.2;
  // Whether to randomize the order of written fields
  bool randomize_field_order = false;

  static constexpr GenerateOptions Defaults() { return GenerateOptions{}; }
};

inline static Status OK(bool ok) { return ok ? Status::OK() : Status::Invalid(""); }

template <typename Engine>
inline static Status Generate(
    const std::shared_ptr<DataType>& type, Engine& e, Writer* writer,
    const GenerateOptions& options = GenerateOptions::Defaults());

template <typename Engine>
inline static Status Generate(
    const std::vector<std::shared_ptr<Field>>& fields, Engine& e, Writer* writer,
    const GenerateOptions& options = GenerateOptions::Defaults());

template <typename Engine>
inline static Status Generate(
    const std::shared_ptr<Schema>& schm, Engine& e, Writer* writer,
    const GenerateOptions& options = GenerateOptions::Defaults()) {
  return Generate(schm->fields(), e, writer, options);
}

template <typename Engine>
struct GenerateImpl {
  Status Visit(const NullType&) { return OK(writer.Null()); }

  Status Visit(const BooleanType&) {
    return OK(writer.Bool(std::uniform_int_distribution<uint16_t>{}(e) & 1));
  }

  template <typename T>
  enable_if_physical_unsigned_integer<T, Status> Visit(const T&) {
    auto val = std::uniform_int_distribution<>{}(e);
    return OK(writer.Uint64(static_cast<typename T::c_type>(val)));
  }

  template <typename T>
  enable_if_physical_signed_integer<T, Status> Visit(const T&) {
    auto val = std::uniform_int_distribution<>{}(e);
    return OK(writer.Int64(static_cast<typename T::c_type>(val)));
  }

  template <typename T>
  enable_if_physical_floating_point<T, Status> Visit(const T&) {
    auto val = std::normal_distribution<typename T::c_type>{0, 1 << 10}(e);
    return OK(writer.Double(val));
  }

  Status GenerateUtf8(const DataType&) {
    // Generate random UTF-8 encoded strings from valid Unicode scalar values.
    auto num_codepoints = std::poisson_distribution<>{4}(e);
    std::string s;
    s.reserve(num_codepoints * 3);

    for (int i = 0; i < num_codepoints; ++i) {
      uint32_t codepoint;
      std::uniform_int_distribution<uint32_t> plane_dist(0, 3);
      uint32_t plane = plane_dist(e);

      if (plane == 0) {
        // Basic Multilingual Plane (BMP): U+0000 to U+FFFF
        // Exclude surrogate code points (U+D800 to U+DFFF)
        // https://www.unicode.org/versions/Unicode15.1.0/ch03.pdf (Section 3.8, D71)
        // Exclude control chars below U+0020 for readability
        // Generate from two ranges with equal probability (overrepresents the smaller
        // upper range):
        // - Lower: U+0020 to U+D7FF (55,776 values, 50% selection probability)
        // - Upper: U+E000 to U+FFFD (8,190 values, 50% selection probability)
        if (std::bernoulli_distribution(0.5)(e)) {
          // Lower range: U+0020 to U+D7FF (before surrogate range)
          codepoint = std::uniform_int_distribution<uint32_t>(0x0020, 0xD7FF)(e);
        } else {
          // Upper range: U+E000 to U+FFFD (after surrogate range)
          // Note: Stops at U+FFFD to exclude noncharacters U+FFFE and U+FFFF
          // Other noncharacters (U+FDD0-U+FDEF, plane-ending pairs) are included
          // as they are valid Unicode scalar values per the Unicode Standard
          codepoint = std::uniform_int_distribution<uint32_t>(0xE000, 0xFFFD)(e);
        }
      } else if (plane == 1) {
        // Supplementary Multilingual Plane (SMP): U+10000 to U+1FFFF
        // https://www.unicode.org/roadmaps/smp/
        codepoint = std::uniform_int_distribution<uint32_t>(0x10000, 0x1FFFF)(e);
      } else if (plane == 2) {
        // Supplementary Ideographic Plane (SIP): U+20000 to U+2FFFF
        // https://www.unicode.org/roadmaps/sip/
        codepoint = std::uniform_int_distribution<uint32_t>(0x20000, 0x2FFFF)(e);
      } else {
        // Planes 3–16: U+30000–U+10FFFF
        // Includes TIP, SSP, PUA-A, PUA-B, and unassigned planes: U+30000 to U+10FFFF
        // Max valid Unicode codepoint is U+10FFFF per the Standard
        // https://www.unicode.org/versions/Unicode15.1.0/ch03.pdf (Section 3.4, D9)
        codepoint = std::uniform_int_distribution<uint32_t>(0x30000, 0x10FFFF)(e);
      }

      // Encode as UTF-8 per RFC 3629 (Section 3: UTF-8 definition)
      // https://www.rfc-editor.org/rfc/rfc3629.html#section-3
      if (codepoint <= 0x7F) {
        // 1-byte sequence: 0xxxxxxx
        s.push_back(static_cast<char>(codepoint));
      } else if (codepoint <= 0x7FF) {
        // 2-byte sequence: 110xxxxx 10xxxxxx
        s.push_back(static_cast<char>(0xC0 | (codepoint >> 6)));
        s.push_back(static_cast<char>(0x80 | (codepoint & 0x3F)));
      } else if (codepoint <= 0xFFFF) {
        // 3-byte sequence: 1110xxxx 10xxxxxx 10xxxxxx
        s.push_back(static_cast<char>(0xE0 | (codepoint >> 12)));
        s.push_back(static_cast<char>(0x80 | ((codepoint >> 6) & 0x3F)));
        s.push_back(static_cast<char>(0x80 | (codepoint & 0x3F)));
      } else {
        // 4-byte sequence: 11110xxx 10xxxxxx 10xxxxxx 10xxxxxx
        s.push_back(static_cast<char>(0xF0 | (codepoint >> 18)));
        s.push_back(static_cast<char>(0x80 | ((codepoint >> 12) & 0x3F)));
        s.push_back(static_cast<char>(0x80 | ((codepoint >> 6) & 0x3F)));
        s.push_back(static_cast<char>(0x80 | (codepoint & 0x3F)));
      }
    }
    // Using c_str() is safe here because generation excludes U+0000 (no embedded nulls).
    // U+0000 can only exist in plane 0 (BMP), and BMP generation starts at U+0020.
    return OK(writer.String(s.c_str()));
  }

  template <typename T>
  enable_if_base_binary<T, Status> Visit(const T& t) {
    return GenerateUtf8(t);
  }

  Status Visit(const BinaryViewType& t) { return GenerateUtf8(t); }

  template <typename T>
  enable_if_list_like<T, Status> Visit(const T& t) {
    auto size = std::poisson_distribution<>{4}(e);
    writer.StartArray();
    for (int i = 0; i < size; ++i) {
      RETURN_NOT_OK(Generate(t.value_type(), e, &writer, options));
    }
    return OK(writer.EndArray(size));
  }

  Status Visit(const ListViewType& t) { return NotImplemented(t); }

  Status Visit(const LargeListViewType& t) { return NotImplemented(t); }

  Status Visit(const StructType& t) { return Generate(t.fields(), e, &writer, options); }

  Status Visit(const DayTimeIntervalType& t) { return NotImplemented(t); }

  Status Visit(const MonthDayNanoIntervalType& t) { return NotImplemented(t); }

  Status Visit(const DictionaryType& t) { return NotImplemented(t); }

  Status Visit(const ExtensionType& t) { return NotImplemented(t); }

  Status Visit(const Decimal128Type& t) { return NotImplemented(t); }

  Status Visit(const FixedSizeBinaryType& t) { return NotImplemented(t); }

  Status Visit(const UnionType& t) { return NotImplemented(t); }

  Status Visit(const RunEndEncodedType& t) { return NotImplemented(t); }

  Status NotImplemented(const DataType& t) {
    return Status::NotImplemented("random generation of arrays of type ", t);
  }

  Engine& e;
  rj::Writer<rj::StringBuffer>& writer;
  const GenerateOptions& options;
};

template <typename Engine>
inline static Status Generate(const std::shared_ptr<DataType>& type, Engine& e,
                              Writer* writer, const GenerateOptions& options) {
  if (std::bernoulli_distribution(options.null_probability)(e)) {
    writer->Null();
    return Status::OK();
  }
  GenerateImpl<Engine> visitor = {e, *writer, options};
  return VisitTypeInline(*type, &visitor);
}

template <typename Engine>
inline static Status Generate(const std::vector<std::shared_ptr<Field>>& fields,
                              Engine& e, Writer* writer, const GenerateOptions& options) {
  RETURN_NOT_OK(OK(writer->StartObject()));

  int num_fields = 0;
  auto write_field = [&](const Field& f) {
    ++num_fields;
    writer->Key(f.name().c_str());
    return Generate(f.type(), e, writer, options);
  };

  std::bernoulli_distribution bool_dist(options.field_probability);
  if (options.randomize_field_order) {
    std::vector<size_t> indices;
    indices.reserve(static_cast<size_t>(fields.size() * options.field_probability));
    for (size_t i = 0; i < fields.size(); ++i) {
      if (bool_dist(e)) {
        indices.push_back(i);
      }
    }
    std::shuffle(indices.begin(), indices.end(), e);
    for (auto i : indices) {
      RETURN_NOT_OK(write_field(*fields[i]));
    }
  } else {
    for (const auto& f : fields) {
      if (bool_dist(e)) {
        RETURN_NOT_OK(write_field(*f));
      }
    }
  }

  return OK(writer->EndObject(num_fields));
}

inline static Status MakeStream(string_view src_str,
                                std::shared_ptr<io::InputStream>* out) {
  auto src = std::make_shared<Buffer>(src_str);
  *out = std::make_shared<io::BufferReader>(src);
  return Status::OK();
}

// scalar values (numbers and strings) are parsed into a
// dictionary<index:int32, value:string>. This can be decoded for ease of comparison
inline static Status DecodeStringDictionary(const DictionaryArray& dict_array,
                                            std::shared_ptr<Array>* decoded) {
  const StringArray& dict = checked_cast<const StringArray&>(*dict_array.dictionary());
  const Int32Array& indices = checked_cast<const Int32Array&>(*dict_array.indices());
  StringBuilder builder;
  RETURN_NOT_OK(builder.Resize(indices.length()));
  for (int64_t i = 0; i < indices.length(); ++i) {
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

inline static Status ParseFromString(ParseOptions options, string_view src_str,
                                     std::shared_ptr<Array>* parsed) {
  auto src = std::make_shared<Buffer>(src_str);
  std::unique_ptr<BlockParser> parser;
  RETURN_NOT_OK(BlockParser::Make(options, &parser));
  RETURN_NOT_OK(parser->Parse(src));
  return parser->Finish(parsed);
}

inline static Status ParseFromString(ParseOptions options, string_view src_str,
                                     std::shared_ptr<StructArray>* parsed) {
  std::shared_ptr<Array> parsed_non_struct;
  RETURN_NOT_OK(ParseFromString(options, src_str, &parsed_non_struct));
  *parsed = internal::checked_pointer_cast<StructArray>(parsed_non_struct);
  return Status::OK();
}

static inline std::string PrettyPrint(string_view one_line) {
  rj::Document document;

  // Must pass size to avoid ASAN issues.
  document.Parse(one_line.data(), one_line.size());
  rj::StringBuffer sb;
  rj::PrettyWriter<rj::StringBuffer> writer(sb);
  document.Accept(writer);
  return sb.GetString();
}

template <typename T>
std::string RowsOfOneColumn(std::string_view name, std::initializer_list<T> values,
                            decltype(std::to_string(*values.begin()))* = nullptr) {
  std::stringstream ss;
  for (auto value : values) {
    ss << R"({")" << name << R"(":)" << std::to_string(value) << "}\n";
  }
  return ss.str();
}

inline std::string RowsOfOneColumn(std::string_view name,
                                   std::initializer_list<std::string> values) {
  std::stringstream ss;
  for (auto value : values) {
    ss << R"({")" << name << R"(":)" << value << "}\n";
  }
  return ss.str();
}

inline static std::string scalars_only_src() {
  return R"(
    { "hello": 3.5, "world": false, "yo": "thing" }
    { "hello": 3.25, "world": null }
    { "hello": 3.125, "world": null, "yo": "\u5fcd" }
    { "hello": 0.0, "world": true, "yo": null }
  )";
}

inline static std::string nested_src() {
  return R"(
    { "hello": 3.5, "world": false, "yo": "thing", "arr": [1, 2, 3], "nuf": {} }
    { "hello": 3.25, "world": null, "arr": [2], "nuf": null }
    { "hello": 3.125, "world": null, "yo": "\u5fcd", "arr": [], "nuf": { "ps": 78 } }
    { "hello": 0.0, "world": true, "yo": null, "arr": null, "nuf": { "ps": 90 } }
  )";
}

inline static std::string null_src() {
  return R"(
    { "plain": null, "list1": [], "list2": [], "struct": { "plain": null } }
    { "plain": null, "list1": [], "list2": [null], "struct": {} }
  )";
}

inline static std::string unquoted_decimal_src() {
  return R"(
    { "price": 30.04, "cost":30.001 }
    { "price": 1.23, "cost":1.229 }
  )";
}

inline static std::string mixed_decimal_src() {
  return R"(
    { "price": 30.04, "cost": 30.001 }
    { "price": "1.23", "cost": "1.229" }
  )";
}

}  // namespace json
}  // namespace arrow
