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

#include "arrow/engine/substrait/relation_internal.h"

#include "arrow/compute/api_scalar.h"
#include "arrow/compute/exec/options.h"
#include "arrow/dataset/file_base.h"
#include "arrow/dataset/file_ipc.h"
#include "arrow/dataset/file_parquet.h"
#include "arrow/dataset/plan.h"
#include "arrow/dataset/scanner.h"
#include "arrow/engine/substrait/expression_internal.h"
#include "arrow/engine/substrait/type_internal.h"
#include "arrow/filesystem/localfs.h"
#include "arrow/filesystem/path_util.h"
#include "arrow/filesystem/util_internal.h"
#include "arrow/util/uri.h"

namespace arrow {
namespace engine {

template <typename RelMessage>
Status CheckRelCommon(const RelMessage& rel) {
  if (rel.has_common()) {
    if (rel.common().has_emit()) {
      return Status::NotImplemented("substrait::RelCommon::Emit");
    }
    if (rel.common().has_hint()) {
      return Status::NotImplemented("substrait::RelCommon::Hint");
    }
    if (rel.common().has_advanced_extension()) {
      return Status::NotImplemented("substrait::RelCommon::advanced_extension");
    }
  }
  if (rel.has_advanced_extension()) {
    return Status::NotImplemented("substrait AdvancedExtensions");
  }
  return Status::OK();
}

Result<DeclarationInfo> FromProto(const substrait::Rel& rel,
                                  const ExtensionSet& ext_set) {
  static bool dataset_init = false;
  if (!dataset_init) {
    dataset_init = true;
    dataset::internal::Initialize();
  }

  switch (rel.rel_type_case()) {
    case substrait::Rel::RelTypeCase::kRead: {
      const auto& read = rel.read();
      RETURN_NOT_OK(CheckRelCommon(read));

      ARROW_ASSIGN_OR_RAISE(auto base_schema, FromProto(read.base_schema(), ext_set));

      auto scan_options = std::make_shared<dataset::ScanOptions>();
      scan_options->use_threads = true;

      // FIXME: FieldPath is not supported in scan filter. See ARROW-14658
      if (read.has_filter()) {
        // ARROW_ASSIGN_OR_RAISE(scan_options->filter, FromProto(read.filter(), ext_set));
      }

      if (read.has_projection()) {
        // NOTE: scan_options->projection is not used by the scanner and thus can't be
        // used for this
        return Status::NotImplemented("substrait::ReadRel::projection");
      }

      if (!read.has_local_files()) {
        return Status::NotImplemented(
            "substrait::ReadRel with read_type other than LocalFiles");
      }

      if (read.local_files().has_advanced_extension()) {
        return Status::NotImplemented(
            "substrait::ReadRel::LocalFiles::advanced_extension");
      }

      auto head = read.local_files().items().at(0);
      if (head.path_type_case() == substrait::ReadRel_LocalFiles_FileOrFiles::kUriFile &&
          util::string_view{head.uri_file()}.starts_with("iterator:")) {
        const auto& index = head.uri_file().substr(9);
        return compute::Declaration{"source_index",
                                    compute::SourceIndexOptions{std::stoi(index)}};
      }

      ARROW_ASSIGN_OR_RAISE(auto filesystem, fs::FileSystemFromUri(head.uri_file()));
      std::shared_ptr<dataset::FileFormat> format;
      auto filesystem = std::make_shared<fs::LocalFileSystem>();
      std::vector<fs::FileInfo> files;

      for (const auto& item : read.local_files().items()) {
        std::string path;
        if (item.path_type_case() ==
            substrait::ReadRel_LocalFiles_FileOrFiles::kUriPath) {
          path = item.uri_path();
        } else if (item.path_type_case() ==
                   substrait::ReadRel_LocalFiles_FileOrFiles::kUriFile) {
          path = item.uri_file();
        } else if (item.path_type_case() ==
                   substrait::ReadRel_LocalFiles_FileOrFiles::kUriFolder) {
          path = item.uri_folder();
        } else {
          path = item.uri_path_glob();
        }

        switch (item.file_format_case()) {
          case substrait::ReadRel_LocalFiles_FileOrFiles::kParquet:
            format = std::make_shared<dataset::ParquetFileFormat>();
            break;
          case substrait::ReadRel_LocalFiles_FileOrFiles::kArrow:
            format = std::make_shared<dataset::IpcFileFormat>();
            break;
          default:
            return Status::NotImplemented(
                "unknown substrait::ReadRel::LocalFiles::FileOrFiles::file_format");
        }

        if (!util::string_view{path}.starts_with("file:///")) {
          return Status::NotImplemented("substrait::ReadRel::LocalFiles item (", path,
                                        ") with other than local filesystem "
                                        "(file:///)");
        }

        path = path.substr(7);
        if (item.path_type_case() ==
            substrait::ReadRel_LocalFiles_FileOrFiles::kUriPath) {
          ARROW_ASSIGN_OR_RAISE(auto file, filesystem->GetFileInfo(path));
          if (file.type() == fs::FileType::File) {
          // Read all row groups if both start and length are not specified.
          int64_t start_offset = item.length() == 0 && item.start() == 0
                                    ? -1
                                    : static_cast<int64_t>(item.start());
          int64_t length = static_cast<int64_t>(item.length());
            files.push_back(std::move(file));
          } else if (file.type() == fs::FileType::Directory) {
            fs::FileSelector selector;
            selector.base_dir = path;
            selector.recursive = true;
            ARROW_ASSIGN_OR_RAISE(auto discovered_files,
                                  filesystem->GetFileInfo(selector));
            std::move(files.begin(), files.end(), std::back_inserter(discovered_files));
          }
        }
        if (item.path_type_case() ==
            substrait::ReadRel_LocalFiles_FileOrFiles::kUriFile) {
          // Read all row groups if both start and length are not specified.
          int64_t start_offset = item.length() == 0 && item.start() == 0
                                    ? -1
                                    : static_cast<int64_t>(item.start());
          int64_t length = static_cast<int64_t>(item.length());
          files.emplace_back(path, fs::FileType::File);
        } else if (item.path_type_case() ==
                   substrait::ReadRel_LocalFiles_FileOrFiles::kUriFolder) {
          fs::FileSelector selector;
          selector.base_dir = path;
          selector.recursive = true;
          ARROW_ASSIGN_OR_RAISE(auto discovered_files, filesystem->GetFileInfo(selector));
          std::move(discovered_files.begin(), discovered_files.end(),
                    std::back_inserter(files));
        } else {
          ARROW_ASSIGN_OR_RAISE(auto discovered_files,
                                fs::internal::GlobFiles(filesystem, path));
          std::move(discovered_files.begin(), discovered_files.end(),
                    std::back_inserter(files));
        }
      }

      ARROW_ASSIGN_OR_RAISE(auto ds_factory, dataset::FileSystemDatasetFactory::Make(
                                                 std::move(filesystem), std::move(files),
                                                 std::move(format), {}));

      auto num_columns = static_cast<int>(base_schema->fields().size());
      ARROW_ASSIGN_OR_RAISE(auto ds, ds_factory->Finish(std::move(base_schema)));

      return DeclarationInfo{
          compute::Declaration{
              "scan", dataset::ScanNodeOptions{std::move(ds), std::move(scan_options)}},
          num_columns};
    }

    case substrait::Rel::RelTypeCase::kFilter: {
      const auto& filter = rel.filter();
      RETURN_NOT_OK(CheckRelCommon(filter));

      if (!filter.has_input()) {
        return Status::Invalid("substrait::FilterRel with no input relation");
      }
      ARROW_ASSIGN_OR_RAISE(auto input, FromProto(filter.input(), ext_set));

      if (!filter.has_condition()) {
        return Status::Invalid("substrait::FilterRel with no condition expression");
      }
      ARROW_ASSIGN_OR_RAISE(auto condition, FromProto(filter.condition(), ext_set));

      return DeclarationInfo{
          compute::Declaration::Sequence({
              std::move(input.declaration),
              {"filter", compute::FilterNodeOptions{std::move(condition)}},
          }),
          input.num_columns};
    }

    case substrait::Rel::RelTypeCase::kProject: {
      const auto& project = rel.project();
      RETURN_NOT_OK(CheckRelCommon(project));

      if (!project.has_input()) {
        return Status::Invalid("substrait::ProjectRel with no input relation");
      }
      ARROW_ASSIGN_OR_RAISE(auto input, FromProto(project.input(), ext_set));

      // NOTE: Substrait ProjectRels *append* columns, while Acero's project node replaces
      // them. Therefore, we need to prefix all the current columns for compatibility.
      std::vector<compute::Expression> expressions;
      expressions.reserve(input.num_columns + project.expressions().size());
      for (int i = 0; i < input.num_columns; i++) {
        expressions.emplace_back(compute::field_ref(FieldRef(i)));
      }
      for (const auto& expr : project.expressions()) {
        expressions.emplace_back();
        ARROW_ASSIGN_OR_RAISE(expressions.back(), FromProto(expr, ext_set));
      }

      auto num_columns = static_cast<int>(expressions.size());
      return DeclarationInfo{
          compute::Declaration::Sequence({
              std::move(input.declaration),
              {"project", compute::ProjectNodeOptions{std::move(expressions)}},
          }),
          num_columns};
    }

    case substrait::Rel::RelTypeCase::kJoin: {
      const auto& join = rel.join();
      RETURN_NOT_OK(CheckRelCommon(join));

      if (!join.has_left()) {
        return Status::Invalid("substrait::JoinRel with no left relation");
      }

      if (!join.has_right()) {
        return Status::Invalid("substrait::JoinRel with no right relation");
      }

      compute::JoinType join_type;
      switch (join.type()) {
        case substrait::JoinRel::JOIN_TYPE_UNSPECIFIED:
          return Status::NotImplemented("Unspecified join type is not supported");
        case substrait::JoinRel::JOIN_TYPE_INNER:
          join_type = compute::JoinType::INNER;
          break;
        case substrait::JoinRel::JOIN_TYPE_OUTER:
          join_type = compute::JoinType::FULL_OUTER;
          break;
        case substrait::JoinRel::JOIN_TYPE_LEFT:
          join_type = compute::JoinType::LEFT_OUTER;
          break;
        case substrait::JoinRel::JOIN_TYPE_RIGHT:
          join_type = compute::JoinType::RIGHT_OUTER;
          break;
        case substrait::JoinRel::JOIN_TYPE_SEMI:
          join_type = compute::JoinType::LEFT_SEMI;
          break;
        case substrait::JoinRel::JOIN_TYPE_ANTI:
          join_type = compute::JoinType::LEFT_ANTI;
          break;
        default:
          return Status::Invalid("Unsupported join type");
      }

      ARROW_ASSIGN_OR_RAISE(auto left, FromProto(join.left(), ext_set));
      ARROW_ASSIGN_OR_RAISE(auto right, FromProto(join.right(), ext_set));

      if (!join.has_expression()) {
        return Status::Invalid("substrait::JoinRel with no expression");
      }

      ARROW_ASSIGN_OR_RAISE(auto expression, FromProto(join.expression(), ext_set));

      const auto* callptr = expression.call();
      if (!callptr) {
        return Status::Invalid(
            "A join rel's expression must be a simple equality between keys but got ",
            expression.ToString());
      }

      compute::JoinKeyCmp join_key_cmp;
      if (callptr->function_name == "equal") {
        join_key_cmp = compute::JoinKeyCmp::EQ;
      } else if (callptr->function_name == "is_not_distinct_from") {
        join_key_cmp = compute::JoinKeyCmp::IS;
      } else {
        return Status::Invalid(
            "Only `equal` or `is_not_distinct_from` are supported for join key "
            "comparison but got ",
            callptr->function_name);
      }

      // TODO: ARROW-166241 Add Suffix support for Substrait
      const auto* left_keys = callptr->arguments[0].field_ref();
      const auto* right_keys = callptr->arguments[1].field_ref();
      if (!left_keys || !right_keys) {
        return Status::Invalid("Left keys for join cannot be null");
      }
      compute::HashJoinNodeOptions join_options{{std::move(*left_keys)},
                                                {std::move(*right_keys)}};
      join_options.join_type = join_type;
      join_options.key_cmp = {join_key_cmp};
      compute::Declaration join_dec{"hashjoin", std::move(join_options)};
      auto num_columns = left.num_columns + right.num_columns;
      join_dec.inputs.emplace_back(std::move(left.declaration));
      join_dec.inputs.emplace_back(std::move(right.declaration));
      return DeclarationInfo{std::move(join_dec), num_columns};
    }

    case substrait::Rel::RelTypeCase::kAggregate: {
      const auto& aggregate = rel.aggregate();
      RETURN_NOT_OK(CheckRelCommon(aggregate));

      if (!aggregate.has_input()) {
        return Status::Invalid("substrait::AggregateRel with no input relation");
      }
      ARROW_ASSIGN_OR_RAISE(auto input, FromProto(aggregate.input(), ext_set));

      compute::AggregateNodeOptions opts{{}, {}, {}};

      if (aggregate.groupings_size() > 1) {
        return Status::Invalid("substrait::AggregateRel has " +
                               std::to_string(aggregate.groupings_size()) + " groupings");
      }

      for (const auto& grouping : aggregate.groupings()) {
        for (const auto& expr : grouping.grouping_expressions()) {
          ARROW_ASSIGN_OR_RAISE(auto key_expr, FromProto(expr, ext_set));
          if (auto field = key_expr.field_ref()) {
            opts.keys.emplace_back(*field);
          } else {
            return Status::Invalid(
                "substrait::AggregateRel grouping key is not a field reference: " +
                key_expr.ToString());
          }
        }
      }

      // group by will first output targets then keys
      // We need a post projector to first output keys then targets
      // TODO: use options to control this behavior
      std::vector<compute::Expression> reordered_fields(opts.keys.size());
      int32_t reordered_field_pos = 0;

      for (const auto& measure : aggregate.measures()) {
        if (measure.has_filter()) {
          // invalid
          return Status::Invalid("substrait::AggregateRel has filter.");
        }

        auto agg_func = measure.measure();
        ARROW_ASSIGN_OR_RAISE(auto decoded_function,
                              ext_set.DecodeFunction(agg_func.function_reference()));

        if (!agg_func.sorts().empty()) {
          return Status::Invalid("substrait::AggregateRel aggregate function #" +
                                 decoded_function.name.to_string() + " has sort.");
        }

        std::vector<FieldRef> target_fields;
        target_fields.reserve(agg_func.args_size());
        for (const auto& arg : agg_func.args()) {
          ARROW_ASSIGN_OR_RAISE(auto target_expr, FromProto(arg, ext_set));
          if (auto target_field = target_expr.field_ref()) {
            target_fields.emplace_back(*target_field);
          } else {
            return Status::Invalid(
                "substrait::AggregateRel measure's arg is not a field reference: " +
                target_expr.ToString());
          }
        }

        int32_t target_field_idx = 0;
        if (decoded_function.name == "mean") {
          switch (agg_func.phase()) {
            case ::substrait::AggregationPhase::
                AGGREGATION_PHASE_INITIAL_TO_INTERMEDIATE: {
              static std::vector<std::string> function_names = {"hash_sum", "hash_count"};
              for (const std::string& func : function_names) {
                opts.aggregates.push_back({func, nullptr});
                opts.targets.emplace_back(target_fields[target_field_idx]);
                opts.names.emplace_back(func + " " + opts.targets.back().ToString());
                reordered_fields.emplace_back(compute::field_ref(reordered_field_pos++));
              }
              target_field_idx++;
              break;
            }
            case ::substrait::AggregationPhase::
                AGGREGATION_PHASE_INTERMEDIATE_TO_RESULT: {
              static std::vector<std::string> function_names = {"hash_sum", "hash_sum"};
              for (const std::string& func : function_names) {
                opts.aggregates.push_back({func, nullptr});
                opts.targets.emplace_back(std::move(target_fields[target_field_idx]));
                opts.names.emplace_back(func + " " + opts.targets.back().ToString());
                target_field_idx++;
              }
              reordered_fields.emplace_back(
                  compute::call("divide", {compute::field_ref(reordered_field_pos++),
                                           compute::field_ref(reordered_field_pos++)}));
              break;
            }
            default:
              return Status::Invalid("substrait::AggregateRel unsupported phase " +
                                     std::to_string(agg_func.phase()));
          }
        } else if (decoded_function.name == "count" &&
                   agg_func.phase() == ::substrait::AggregationPhase::
                                           AGGREGATION_PHASE_INTERMEDIATE_TO_RESULT) {
          std::string func = "hash_sum";
          opts.aggregates.push_back({func, nullptr});
          opts.targets.emplace_back(std::move(target_fields[target_field_idx]));
          opts.names.emplace_back(func + " " + opts.targets.back().ToString());
          target_field_idx++;
          reordered_fields.emplace_back(compute::field_ref(reordered_field_pos++));
        } else {
          std::string func = opts.keys.empty()
                                 ? decoded_function.name.to_string()
                                 : "hash_" + decoded_function.name.to_string();
          opts.aggregates.push_back({func, nullptr});
          opts.targets.emplace_back(std::move(target_fields[target_field_idx]));
          opts.names.emplace_back(func + " " + opts.targets.back().ToString());
          target_field_idx++;
          reordered_fields.emplace_back(compute::field_ref(reordered_field_pos++));
        }

        if (target_field_idx != agg_func.args_size()) {
          return Status::Invalid("substrait::AggregateRel aggregate function #" +
                                 decoded_function.name.to_string() +
                                 " not all arguments are consumed.");
        }
      }

      for (size_t i = 0; i < opts.keys.size(); ++i) {
        reordered_fields[i] = compute::field_ref(reordered_field_pos++);
      }

      auto aggregate_decl = compute::Declaration{"aggregate", std::move(opts)};

      auto post_project_decl = compute::Declaration{
          "project", compute::ProjectNodeOptions{std::move(reordered_fields)}};

      return compute::Declaration::Sequence(
          {std::move(input), std::move(aggregate_decl), std::move(post_project_decl)});
    }

    default:
      break;
  }

  return Status::NotImplemented(
      "conversion to arrow::compute::Declaration from Substrait relation ",
      rel.DebugString());
}

}  // namespace engine
}  // namespace arrow
