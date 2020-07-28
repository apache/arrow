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
#include <string>
#include <unordered_map>
#include <vector>

#include "arrow/status.h"
#include "gandiva/function_holder.h"
#include "gandiva/node.h"
#include "gandiva/visibility.h"
#include "rapidjson/document.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"
#include "re2/re2.h"
#include "re2/stringpiece.h"

using std::string;
using std::vector;
using map_str_str = std::unordered_map<string, string>;

namespace gandiva {
/// Function Holder to extract all groups from a given string with certain pattern
class GANDIVA_EXPORT RexHolder : public FunctionHolder {
 public:
  ~RexHolder() override = default;

  static bool IsArrowStringLiteral(arrow::Type::type type) {
    return type == arrow::Type::STRING || type == arrow::Type::BINARY;
  }

  inline static Status Make(const FunctionNode& node,
                            std::shared_ptr<RexHolder>* holder) {
    ARROW_RETURN_IF(node.children().size() != 2,
                    Status::Invalid("'rex' function requires two parameters"));

    auto literal = dynamic_cast<LiteralNode*>(node.children().at(1).get());
    ARROW_RETURN_IF(
        literal == nullptr,
        Status::Invalid("'rex' function requires a literal as the second parameter"));

    auto literal_type = literal->return_type()->id();
    ARROW_RETURN_IF(
        !IsArrowStringLiteral(literal_type),
        Status::Invalid(
            "'rex' function requires a string literal as the second parameter"));

    return Make(arrow::util::get<string>(literal->holder()), holder);
  };

  inline static Status Make(const string& regex_pattern,
                            std::shared_ptr<RexHolder>* holder) {
    auto rex_holder = std::shared_ptr<RexHolder>(new RexHolder(regex_pattern));
    *holder = rex_holder;
    return Status::OK();
  };

  rapidjson::Document operator()(const string& data) {
    re2::StringPiece input(data);
    RE2 pattern(pattern_);
    std::map<int, string> field_keys = pattern.CapturingGroupNames();
    field_keys.size();
    int keys_count = pattern.NumberOfCapturingGroups();
    string field_values[keys_count];
    vector<RE2::Arg> arg_vector;
    vector<RE2::Arg*> arg_prt_vector;
    for (int i = 0; i < keys_count; i++) {
      arg_vector.push_back(RE2::Arg(&field_values[i]));
    }
    for (auto& arg : arg_vector) {
      arg_prt_vector.push_back(&arg);
    }
    const RE2::Arg* matches[keys_count];
    std::move(arg_prt_vector.begin(), arg_prt_vector.end(), matches);
    bool is_matched = RE2::FullMatchN(input, pattern, matches, keys_count);
    rapidjson::Document ret;
    string ret_string = "{";
    if (is_matched) {
      for (int i = 0; i < keys_count; i++) {
        ret_string +=
            "\"" + field_keys[i + 1] + "\"" + ":" + "\"" + field_values[i] + "\"" + ",";
      }
      ret_string.pop_back();
    }
    ret_string += "}";
    ret.Parse(ret_string.data());
    return ret;
  }

  static void _get_keys_values(const char* map_string, size_t len, string& keys,
                               vector<int64_t>& key_offsets, string& values,
                               vector<int64_t>& value_offsets,
                               vector<int64_t>& map_offsets) {
    rapidjson::Document document;
    document.Parse(map_string, len);

    int64_t key_offset = keys.length();
    int64_t value_offset = values.length();
    int64_t map_offset = map_offsets.back();
    for (auto& kv : document.GetObject()) {
      keys += kv.name.GetString();
      key_offset += kv.name.GetStringLength();
      key_offsets.push_back(key_offset);
      values += kv.value.GetString();
      value_offset += kv.value.GetStringLength();
      value_offsets.push_back(value_offset);
      map_offset++;
    }
    map_offsets.push_back(map_offset);
  }

  static void _get_keys_values(const char* map_string, size_t len, vector<string>& keys,
                               vector<string>& values, vector<int32_t>& map_offsets) {
    rapidjson::Document document;
    document.Parse(map_string, len);

    int64_t map_offset = map_offsets.back();
    for (auto& kv : document.GetObject()) {
      keys.push_back(kv.name.GetString());
      values.push_back(kv.value.GetString());
      map_offset++;
    }
    map_offsets.push_back(map_offset);
  }

  static void get_keys_values(const string& map_string, vector<string>& keys,
                              vector<string>& values, vector<int32_t>& map_offsets) {
    keys.clear();
    values.clear();
    map_offsets = {0};

    size_t found = 0;
    while (true) {
      auto curr_find = map_string.find("}", found);
      if (curr_find == string::npos) {
        break;
      }
      _get_keys_values(map_string.data() + found, ++curr_find - found, keys, values,
                       map_offsets);
      found = curr_find;
    }
  }

  static void get_keys_values(const string& map_string, string& keys,
                              vector<int64_t>& key_offsets, string& values,
                              vector<int64_t>& value_offsets,
                              vector<int64_t>& map_offsets) {
    keys.clear();
    values.clear();
    key_offsets = {0};
    value_offsets = {0};
    map_offsets = {0};

    size_t found = 0;
    while (true) {
      auto curr_find = map_string.find("}", found);
      if (curr_find == string::npos) {
        break;
      }
      _get_keys_values(map_string.data() + found, ++curr_find - found, keys, key_offsets,
                       values, value_offsets, map_offsets);
      found = curr_find;
    }
  }

  static string stringify(rapidjson::Document document) {
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    document.Accept(writer);
    return buffer.GetString();
  }

  static rapidjson::Document parse(const string& map_string) {
    rapidjson::Document ret;
    ret.Parse(map_string.data());
    return ret;
  }

 private:
  explicit RexHolder(const string& pattern) : pattern_(pattern), regex_(pattern) {}

  string pattern_;  // posix pattern string, to help debugging
  RE2 regex_;       // compiled regex for the pattern
};

}  // namespace gandiva
