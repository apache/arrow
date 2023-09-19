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

#include <rapidjson/document.h>
#include <filesystem>
#include <fstream>
#include <unordered_map>
#include <vector>

#include <arrow/type.h>
#include <gandiva/function_registry_external.h>

namespace gandiva {
namespace rj = rapidjson;

class JsonRegistryParser {
 public:
  static arrow::Result<std::vector<NativeFunction>> Parse(std::string_view json) {
    rj::Document doc;
    doc.Parse(reinterpret_cast<const rj::Document::Ch*>(json.data()),
              static_cast<size_t>(json.size()));

    if (doc.HasParseError()) {
      return Status::Invalid("JSON parse error (offset ", doc.GetErrorOffset(),
                             "): ", doc.GetParseError());
    }
    if (!doc.IsObject()) {
      return Status::TypeError("Not a JSON object");
    }
    const auto& functions = doc["functions"];
    if (!functions.IsArray()) {
      return Status::TypeError("'functions' property is expected to be a JSON array");
    }

    std::vector<NativeFunction> funcs;
    for (const auto& func : functions.GetArray()) {
      ARROW_ASSIGN_OR_RAISE(auto name, GetString(func, "name"));
      ARROW_ASSIGN_OR_RAISE(auto aliases, GetAliases(func));
      ARROW_ASSIGN_OR_RAISE(auto param_types, ParseParamTypes(func));
      ARROW_ASSIGN_OR_RAISE(auto ret_type, ParseDataType(func["return_type"]));
      ARROW_ASSIGN_OR_RAISE(auto result_nullable_type, ParseResultNullable(func));
      ARROW_ASSIGN_OR_RAISE(auto pc_name, GetString(func, "pc_name"));
      auto flags = GetFlags(func);
      funcs.emplace_back(name, aliases, param_types, ret_type, result_nullable_type,
                         pc_name, flags);
    }
    return funcs;
  }

 private:
  static arrow::Result<std::string> GetString(const rj::GenericValue<rj::UTF8<>>& func,
                                              const std::string& key) {
    if (!func.HasMember(key.c_str())) {
      return Status::Invalid("'" + key + "'" + " property is missing");
    }
    if (!func[key.c_str()].IsString()) {
      return Status::TypeError("'" + key + "'" + " property should be a string");
    }
    return func[key.c_str()].GetString();
  }

  static arrow::Result<ResultNullableType> ParseResultNullable(
      const rj::GenericValue<rj::UTF8<>>& func) {
    std::string nullable;
    if (!func.HasMember("result_nullable")) {
      nullable = "if_null";
    } else {
      if (!func["result_nullable"].IsString()) {
        return Status::TypeError("result_nullable property should be a string");
      }
      nullable = func["result_nullable"].GetString();
    }
    if (nullable == "if_null") {
      return ResultNullableType::kResultNullIfNull;
    } else if (nullable == "never") {
      return ResultNullableType::kResultNullNever;
    } else if (nullable == "internal") {
      return ResultNullableType::kResultNullInternal;
    } else {
      return Status::Invalid("Unsupported result_nullable value: " + nullable +
                             ". Only if_null/never/internal are supported");
    }
  }
  static int32_t GetFlags(const rj::GenericValue<rj::UTF8<>>& func) {
    int32_t flags = 0;
    for (auto const& [flag_name, flag_value] :
         {std::make_pair("needs_context", NativeFunction::kNeedsContext),
          std::make_pair("needs_function_holder", NativeFunction::kNeedsFunctionHolder),
          std::make_pair("can_return_errors", NativeFunction::kCanReturnErrors)}) {
      if (func.HasMember(flag_name) && func[flag_name].GetBool()) {
        flags |= flag_value;
      }
    }
    return flags;
  }

  static arrow::Result<std::vector<std::string>> GetAliases(
      const rj::GenericValue<rj::UTF8<>>& func) {
    std::vector<std::string> aliases;
    if (!func.HasMember("aliases")) {
      return aliases;
    }
    if (func["aliases"].IsArray()) {
      for (const auto& alias : func["aliases"].GetArray()) {
        aliases.emplace_back(alias.GetString());
      }
    } else {
      return Status::TypeError("'aliases' property is expected to be a JSON array");
    }
    return aliases;
  }

  static arrow::Result<arrow::DataTypeVector> ParseParamTypes(
      const rj::GenericValue<rj::UTF8<>>& func) {
    arrow::DataTypeVector param_types;
    if (!func.HasMember("param_types")) {
      return param_types;
    }
    if (!func["param_types"].IsArray()) {
      return Status::Invalid("'param_types' property is expected to be a JSON array");
    }
    for (const auto& param_type : func["param_types"].GetArray()) {
      ARROW_ASSIGN_OR_RAISE(auto type, ParseDataType(param_type))
      param_types.push_back(type);
    }
    return param_types;
  }

  static arrow::Result<std::shared_ptr<arrow::DataType>> ParseTimestampDataType(
      const rj::GenericValue<rj::UTF8<>>& data_type) {
    if (!data_type.HasMember("unit")) {
      return Status::Invalid("'unit' property is required for timestamp data type");
    }
    const std::string unit_name = data_type["unit"].GetString();
    arrow::TimeUnit::type unit;
    if (unit_name == "second") {
      unit = arrow::TimeUnit::SECOND;
    } else if (unit_name == "milli") {
      unit = arrow::TimeUnit::MILLI;
    } else if (unit_name == "micro") {
      unit = arrow::TimeUnit::MICRO;
    } else if (unit_name == "nano") {
      unit = arrow::TimeUnit::NANO;
    } else {
      return Status::Invalid("Unsupported timestamp unit name: ", unit_name);
    }
    return arrow::timestamp(unit);
  }

  static arrow::Result<std::shared_ptr<arrow::DataType>> ParseDecimalDataType(
      const rj::GenericValue<rj::UTF8<>>& data_type) {
    if (!data_type.HasMember("precision") || !data_type["precision"].IsInt()) {
      return Status::Invalid(
          "'precision' property is required for decimal data type and should be an "
          "integer");
    }
    if (!data_type.HasMember("scale") || !data_type["scale"].IsInt()) {
      return Status::Invalid(
          "'scale' property is required for decimal data type and should be an integer");
    }
    auto precision = data_type["precision"].GetInt();
    auto scale = data_type["scale"].GetInt();
    const std::string type_name = data_type["type"].GetString();
    if (type_name == "decimal128") {
      return arrow::decimal128(precision, scale);
    } else if (type_name == "decimal256") {
      return arrow::decimal256(precision, scale);
    }
    return arrow::decimal(precision, scale);
  }

  static arrow::Result<std::shared_ptr<arrow::DataType>> ParseListDataType(
      const rj::GenericValue<rj::UTF8<>>& data_type) {
    if (!data_type.HasMember("value_type") || !data_type["value_type"].IsObject()) {
      return Status::TypeError(
          "'value_type' property is required for list data type and should be an object");
    }
    ARROW_ASSIGN_OR_RAISE(auto value_type, ParseDataType(data_type["value_type"]));
    return arrow::list(value_type);
  }

  static arrow::Result<std::shared_ptr<arrow::DataType>> ParseComplexDataType(
      const rj::GenericValue<rj::UTF8<>>& data_type) {
    static const std::unordered_map<
        std::string, std::function<arrow::Result<std::shared_ptr<arrow::DataType>>(
                         const rj::GenericValue<rj::UTF8<>>&)>>
        complex_type_map = {{"timestamp", ParseTimestampDataType},
                            {"decimal", ParseDecimalDataType},
                            {"decimal128", ParseDecimalDataType},
                            {"decimal256", ParseDecimalDataType},
                            {"list", ParseListDataType}};
    const std::string type_name = data_type["type"].GetString();
    auto it = complex_type_map.find(type_name);
    if (it == complex_type_map.end()) {
      return Status::Invalid("Unsupported complex type name: ", type_name);
    }
    return it->second(data_type);
  }

  static arrow::Result<std::shared_ptr<arrow::DataType>> ParseDataType(
      const rj::GenericValue<rj::UTF8<>>& data_type) {
    if (!data_type.HasMember("type")) {
      return Status::Invalid("'type' property is required for data type");
    }
    auto type_name = data_type["type"].GetString();
    auto type = ParseDataTypeFromName(type_name);
    if (type == nullptr) {
      return ParseComplexDataType(data_type);
    } else {
      return type;
    }
  }

  static std::shared_ptr<arrow::DataType> ParseDataTypeFromName(
      const std::string& type_name) {
    static const std::unordered_map<std::string, std::shared_ptr<arrow::DataType>>
        simple_type_map = {{"null", arrow::null()},
                           {"boolean", arrow::boolean()},
                           {"uint8", arrow::uint8()},
                           {"int8", arrow::int8()},
                           {"uint16", arrow::uint16()},
                           {"int16", arrow::int16()},
                           {"uint32", arrow::uint32()},
                           {"int32", arrow::int32()},
                           {"uint64", arrow::uint64()},
                           {"int64", arrow::int64()},
                           {"float16", arrow::float16()},
                           {"float32", arrow::float32()},
                           {"float64", arrow::float64()},
                           {"utf8", arrow::utf8()},
                           {"large_utf8", arrow::large_utf8()},
                           {"binary", arrow::binary()},
                           {"large_binary", arrow::large_binary()},
                           {"date32", arrow::date32()},
                           {"date64", arrow::date64()},
                           {"day_time_interval", arrow::day_time_interval()},
                           {"month_interval", arrow::month_interval()}};

    auto it = simple_type_map.find(type_name);
    return it != simple_type_map.end() ? it->second : nullptr;
  }
};

// iterate all files under registry_dir by file names
std::vector<std::filesystem::path> ListAllFiles(const std::string& registry_dir) {
  if (registry_dir.empty()) {
    return {};
  }
  std::vector<std::filesystem::path> filenames;
  for (const auto& entry : std::filesystem::directory_iterator(registry_dir)) {
    filenames.push_back(entry.path());
  }

  std::sort(filenames.begin(), filenames.end());
  return filenames;
}

arrow::Result<std::vector<NativeFunction>> GetExternalFunctionRegistry(
    const std::string& registry_dir) {
  std::vector<NativeFunction> registry;
  auto filenames = ListAllFiles(registry_dir);
  for (const auto& entry : filenames) {
    if (entry.extension() == ".json") {
      std::ifstream file(entry);
      std::string content((std::istreambuf_iterator<char>(file)),
                          std::istreambuf_iterator<char>());

      auto funcs_result = JsonRegistryParser::Parse(content);
      if (!funcs_result.ok()) {
        return funcs_result.status().WithMessage(
            "Failed to parse json file: ", entry.string(),
            ". Error: ", funcs_result.status().message());
      }
      auto funcs = *funcs_result;
      // insert all funcs into registry
      registry.insert(registry.end(), funcs.begin(), funcs.end());
    }
  }

  return registry;
}
}  // namespace gandiva
