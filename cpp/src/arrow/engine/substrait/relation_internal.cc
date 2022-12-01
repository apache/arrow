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
#include "arrow/util/checked_cast.h"
#include "arrow/util/string.h"
#include "arrow/util/uri.h"

#include <memory>

namespace arrow {

using internal::checked_cast;
using internal::StartsWith;
using internal::ToChars;
using internal::UriFromAbsolutePath;

namespace engine {

struct EmitInfo {
  std::vector<compute::Expression> expressions;
  std::shared_ptr<Schema> schema;
};

Result<EmitInfo> GetEmitInfo(const substrait::RelCommon& rel_common,
                             const std::shared_ptr<Schema>& input_schema) {
  const auto& emit = rel_common.emit();
  int emit_size = emit.output_mapping_size();
  std::vector<compute::Expression> proj_field_refs(emit_size);
  EmitInfo emit_info;
  FieldVector emit_fields(emit_size);
  for (int i = 0; i < emit_size; i++) {
    int32_t map_id = emit.output_mapping(i);
    proj_field_refs[i] = compute::field_ref(FieldRef(map_id));
    emit_fields[i] = input_schema->field(map_id);
  }
  emit_info.expressions = std::move(proj_field_refs);
  emit_info.schema = schema(std::move(emit_fields));
  return std::move(emit_info);
}

Result<DeclarationInfo> ProcessEmit(std::optional<substrait::RelCommon> rel_common_opt,
                                    const DeclarationInfo& no_emit_declr,
                                    const std::shared_ptr<Schema>& schema) {
  if (rel_common_opt) {
    switch (rel_common_opt->emit_kind_case()) {
      case substrait::RelCommon::EmitKindCase::kDirect:
        return no_emit_declr;
      case substrait::RelCommon::EmitKindCase::kEmit: {
        ARROW_ASSIGN_OR_RAISE(auto emit_info, GetEmitInfo(*rel_common_opt, schema));
        return DeclarationInfo{
            compute::Declaration::Sequence(
                {no_emit_declr.declaration,
                 {"project",
                  compute::ProjectNodeOptions{std::move(emit_info.expressions)}}}),
            std::move(emit_info.schema)};
      }
      default:
        return Status::Invalid("Invalid emit case");
    }
  } else {
    return no_emit_declr;
  }
}

template <typename RelMessage>
Result<DeclarationInfo> ProcessEmit(const RelMessage& rel,
                                    const DeclarationInfo& no_emit_declr,
                                    const std::shared_ptr<Schema>& schema) {
  return ProcessEmit(rel.has_common() ? std::optional(rel.common()) : std::nullopt,
                     no_emit_declr, schema);
}

Result<DeclarationInfo> ProcessExtensionEmit(
    const DeclarationInfo& no_emit_declr, const std::vector<int>& emit_info,
    const std::vector<int>& field_output_indices) {
  const std::shared_ptr<Schema>& input_schema = no_emit_declr.output_schema;
  std::vector<compute::Expression> proj_field_refs;
  proj_field_refs.reserve(emit_info.size());
  FieldVector emit_fields;
  emit_fields.reserve(emit_info.size());

  for (int emit_idx : emit_info) {
    if (emit_idx < 0 || static_cast<size_t>(emit_idx) >= field_output_indices.size()) {
      return Status::Invalid("Out of bounds emit index ", emit_idx);
    }
    int field_idx = field_output_indices[emit_idx];
    if (field_idx < 0) {
      return Status::Invalid("Non-output emit index ", emit_idx);
    }
    proj_field_refs.push_back(compute::field_ref(FieldRef(field_idx)));
    emit_fields.push_back(input_schema->field(field_idx));
  }

  std::shared_ptr<Schema> emit_schema = schema(std::move(emit_fields));

  return DeclarationInfo{
      compute::Declaration::Sequence(
          {no_emit_declr.declaration,
           {"project", compute::ProjectNodeOptions{std::move(proj_field_refs)}}}),
      std::move(emit_schema)};
}

Result<RelationInfo> GetExtensionRelationInfo(
    const substrait::Rel& rel, const ExtensionSet& ext_set,
    const ConversionOptions& conversion_options) {
  switch (rel.rel_type_case()) {
    case substrait::Rel::RelTypeCase::kExtensionLeaf: {
      const auto& ext = rel.extension_leaf();
      return conversion_options.extension_provider->MakeRel({}, ext.detail(), ext_set);
    }

    case substrait::Rel::RelTypeCase::kExtensionSingle: {
      const auto& ext = rel.extension_single();
      ARROW_ASSIGN_OR_RAISE(DeclarationInfo input,
                            FromProto(ext.input(), ext_set, conversion_options));
      return conversion_options.extension_provider->MakeRel({input}, ext.detail(),
                                                            ext_set);
    }

    case substrait::Rel::RelTypeCase::kExtensionMulti: {
      const auto& ext = rel.extension_multi();
      std::vector<DeclarationInfo> inputs;
      for (const auto& input : ext.inputs()) {
        ARROW_ASSIGN_OR_RAISE(auto input_info,
                              FromProto(input, ext_set, conversion_options));
        inputs.push_back(std::move(input_info));
      }
      return conversion_options.extension_provider->MakeRel(std::move(inputs),
                                                            ext.detail(), ext_set);
    }

    default: {
      return Status::Invalid("Invalid extension relation case ", rel.rel_type_case());
    }
  }
}

std::optional<substrait::RelCommon> GetExtensionRelCommon(const substrait::Rel& rel) {
  switch (rel.rel_type_case()) {
    case substrait::Rel::RelTypeCase::kExtensionLeaf: {
      const auto& ext = rel.extension_leaf();
      return ext.has_common() ? std::optional(ext.common()) : std::nullopt;
    }

    case substrait::Rel::RelTypeCase::kExtensionSingle: {
      const auto& ext = rel.extension_single();
      return ext.has_common() ? std::optional(ext.common()) : std::nullopt;
    }

    case substrait::Rel::RelTypeCase::kExtensionMulti: {
      const auto& ext = rel.extension_multi();
      return ext.has_common() ? std::optional(ext.common()) : std::nullopt;
    }

    default: {
      return std::nullopt;
    }
  }
}

template <typename RelMessage>
Status CheckRelCommon(const RelMessage& rel,
                      const ConversionOptions& conversion_options) {
  if (rel.has_common()) {
    if (rel.common().has_hint() &&
        conversion_options.strictness == ConversionStrictness::EXACT_ROUNDTRIP) {
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

Status DiscoverFilesFromDir(const std::shared_ptr<fs::LocalFileSystem>& local_fs,
                            const std::string& dirpath,
                            std::vector<fs::FileInfo>* rel_fpaths) {
  // Define a selector for a recursive descent
  fs::FileSelector selector;
  selector.base_dir = dirpath;
  selector.recursive = true;

  ARROW_ASSIGN_OR_RAISE(auto file_infos, local_fs->GetFileInfo(selector));
  for (auto& file_info : file_infos) {
    if (file_info.IsFile()) {
      rel_fpaths->push_back(std::move(file_info));
    }
  }

  return Status::OK();
}

Result<DeclarationInfo> FromProto(const substrait::Rel& rel, const ExtensionSet& ext_set,
                                  const ConversionOptions& conversion_options) {
  static bool dataset_init = false;
  if (!dataset_init) {
    dataset_init = true;
    dataset::internal::Initialize();
  }

  switch (rel.rel_type_case()) {
    case substrait::Rel::RelTypeCase::kRead: {
      const auto& read = rel.read();
      RETURN_NOT_OK(CheckRelCommon(read, conversion_options));

      // Get the base schema for the read relation
      ARROW_ASSIGN_OR_RAISE(auto base_schema,
                            FromProto(read.base_schema(), ext_set, conversion_options));

      auto scan_options = std::make_shared<dataset::ScanOptions>();
      scan_options->use_threads = true;

      if (read.has_filter()) {
        ARROW_ASSIGN_OR_RAISE(scan_options->filter,
                              FromProto(read.filter(), ext_set, conversion_options));
      }

      if (read.has_projection()) {
        return Status::NotImplemented("substrait::ReadRel::projection");
      }

      if (read.has_named_table()) {
        if (!conversion_options.named_table_provider) {
          return Status::Invalid(
              "plan contained a named table but a NamedTableProvider has not been "
              "configured");
        }

        if (read.named_table().names().empty()) {
          return Status::Invalid("names for NamedTable not provided");
        }

        const NamedTableProvider& named_table_provider =
            conversion_options.named_table_provider;
        const substrait::ReadRel::NamedTable& named_table = read.named_table();
        std::vector<std::string> table_names(named_table.names().begin(),
                                             named_table.names().end());
        ARROW_ASSIGN_OR_RAISE(compute::Declaration source_decl,
                              named_table_provider(table_names));

        if (!source_decl.IsValid()) {
          return Status::Invalid("Invalid NamedTable Source");
        }

        return ProcessEmit(std::move(read),
                           DeclarationInfo{std::move(source_decl), base_schema},
                           std::move(base_schema));
      }

      if (!read.has_local_files()) {
        return Status::NotImplemented(
            "substrait::ReadRel with read_type other than LocalFiles");
      }

      if (read.local_files().has_advanced_extension()) {
        return Status::NotImplemented(
            "substrait::ReadRel::LocalFiles::advanced_extension");
      }

      std::shared_ptr<dataset::FileFormat> format;
      auto filesystem = std::make_shared<fs::LocalFileSystem>();
      std::vector<fs::FileInfo> files;

      for (const auto& item : read.local_files().items()) {
        // Validate properties of the `FileOrFiles` item
        if (item.partition_index() != 0) {
          return Status::NotImplemented(
              "non-default substrait::ReadRel::LocalFiles::FileOrFiles::partition_index");
        }

        if (item.start() != 0) {
          return Status::NotImplemented(
              "non-default substrait::ReadRel::LocalFiles::FileOrFiles::start offset");
        }

        if (item.length() != 0) {
          return Status::NotImplemented(
              "non-default substrait::ReadRel::LocalFiles::FileOrFiles::length");
        }

        // Extract and parse the read relation's source URI
        ::arrow::internal::Uri item_uri;
        switch (item.path_type_case()) {
          case substrait::ReadRel::LocalFiles::FileOrFiles::kUriPath:
            RETURN_NOT_OK(item_uri.Parse(item.uri_path()));
            break;

          case substrait::ReadRel::LocalFiles::FileOrFiles::kUriFile:
            RETURN_NOT_OK(item_uri.Parse(item.uri_file()));
            break;

          case substrait::ReadRel::LocalFiles::FileOrFiles::kUriFolder:
            RETURN_NOT_OK(item_uri.Parse(item.uri_folder()));
            break;

          default:
            RETURN_NOT_OK(item_uri.Parse(item.uri_path_glob()));
            break;
        }

        // Validate the URI before processing
        if (!item_uri.is_file_scheme()) {
          return Status::NotImplemented("substrait::ReadRel::LocalFiles item (",
                                        item_uri.ToString(),
                                        ") does not have file scheme (file:///)");
        }

        if (item_uri.port() != -1) {
          return Status::NotImplemented("substrait::ReadRel::LocalFiles item (",
                                        item_uri.ToString(),
                                        ") should not have a port number in path");
        }

        if (!item_uri.query_string().empty()) {
          return Status::NotImplemented("substrait::ReadRel::LocalFiles item (",
                                        item_uri.ToString(),
                                        ") should not have a query string in path");
        }

        switch (item.file_format_case()) {
          case substrait::ReadRel::LocalFiles::FileOrFiles::kParquet:
            format = std::make_shared<dataset::ParquetFileFormat>();
            break;
          case substrait::ReadRel::LocalFiles::FileOrFiles::kArrow:
            format = std::make_shared<dataset::IpcFileFormat>();
            break;
          default:
            return Status::NotImplemented(
                "unsupported file format ",
                "(see substrait::ReadRel::LocalFiles::FileOrFiles::file_format)");
        }

        // Handle the URI as appropriate
        switch (item.path_type_case()) {
          case substrait::ReadRel::LocalFiles::FileOrFiles::kUriFile: {
            files.emplace_back(item_uri.path(), fs::FileType::File);
            break;
          }

          case substrait::ReadRel::LocalFiles::FileOrFiles::kUriFolder: {
            RETURN_NOT_OK(DiscoverFilesFromDir(filesystem, item_uri.path(), &files));
            break;
          }

          case substrait::ReadRel::LocalFiles::FileOrFiles::kUriPath: {
            ARROW_ASSIGN_OR_RAISE(auto file_info,
                                  filesystem->GetFileInfo(item_uri.path()));

            switch (file_info.type()) {
              case fs::FileType::File: {
                files.push_back(std::move(file_info));
                break;
              }
              case fs::FileType::Directory: {
                RETURN_NOT_OK(DiscoverFilesFromDir(filesystem, item_uri.path(), &files));
                break;
              }
              case fs::FileType::NotFound:
                return Status::Invalid("Unable to find file for URI path");
              case fs::FileType::Unknown:
                [[fallthrough]];
              default:
                return Status::NotImplemented("URI path is of unknown file type.");
            }
            break;
          }

          case substrait::ReadRel::LocalFiles::FileOrFiles::kUriPathGlob: {
            ARROW_ASSIGN_OR_RAISE(auto globbed_files,
                                  fs::internal::GlobFiles(filesystem, item_uri.path()));
            std::move(globbed_files.begin(), globbed_files.end(),
                      std::back_inserter(files));
            break;
          }

          default: {
            return Status::Invalid("Unrecognized file type in LocalFiles");
          }
        }
      }

      ARROW_ASSIGN_OR_RAISE(auto ds_factory, dataset::FileSystemDatasetFactory::Make(
                                                 std::move(filesystem), std::move(files),
                                                 std::move(format), {}));

      ARROW_ASSIGN_OR_RAISE(auto ds, ds_factory->Finish(base_schema));

      DeclarationInfo scan_declaration{
          compute::Declaration{"scan", dataset::ScanNodeOptions{ds, scan_options}},
          base_schema};

      return ProcessEmit(std::move(read), std::move(scan_declaration),
                         std::move(base_schema));
    }

    case substrait::Rel::RelTypeCase::kFilter: {
      const auto& filter = rel.filter();
      RETURN_NOT_OK(CheckRelCommon(filter, conversion_options));

      if (!filter.has_input()) {
        return Status::Invalid("substrait::FilterRel with no input relation");
      }
      ARROW_ASSIGN_OR_RAISE(auto input,
                            FromProto(filter.input(), ext_set, conversion_options));

      if (!filter.has_condition()) {
        return Status::Invalid("substrait::FilterRel with no condition expression");
      }
      ARROW_ASSIGN_OR_RAISE(auto condition,
                            FromProto(filter.condition(), ext_set, conversion_options));
      DeclarationInfo filter_declaration{
          compute::Declaration::Sequence({
              std::move(input.declaration),
              {"filter", compute::FilterNodeOptions{std::move(condition)}},
          }),
          input.output_schema};

      return ProcessEmit(std::move(filter), std::move(filter_declaration),
                         input.output_schema);
    }

    case substrait::Rel::RelTypeCase::kProject: {
      const auto& project = rel.project();
      RETURN_NOT_OK(CheckRelCommon(project, conversion_options));
      if (!project.has_input()) {
        return Status::Invalid("substrait::ProjectRel with no input relation");
      }
      ARROW_ASSIGN_OR_RAISE(auto input,
                            FromProto(project.input(), ext_set, conversion_options));

      // NOTE: Substrait ProjectRels *append* columns, while Acero's project node replaces
      // them. Therefore, we need to prefix all the current columns for compatibility.
      std::vector<compute::Expression> expressions;
      int num_columns = input.output_schema->num_fields();
      expressions.reserve(num_columns + project.expressions().size());
      for (int i = 0; i < num_columns; i++) {
        expressions.emplace_back(compute::field_ref(FieldRef(i)));
      }

      int i = 0;
      auto project_schema = input.output_schema;
      for (const auto& expr : project.expressions()) {
        std::shared_ptr<Field> project_field;
        ARROW_ASSIGN_OR_RAISE(compute::Expression des_expr,
                              FromProto(expr, ext_set, conversion_options));
        auto bound_expr = des_expr.Bind(*input.output_schema);
        if (auto* expr_call = bound_expr->call()) {
          project_field = field(expr_call->function_name,
                                expr_call->kernel->signature->out_type().type());
        } else if (auto* field_ref = des_expr.field_ref()) {
          ARROW_ASSIGN_OR_RAISE(FieldPath field_path,
                                field_ref->FindOne(*input.output_schema));
          ARROW_ASSIGN_OR_RAISE(project_field, field_path.Get(*input.output_schema));
        } else if (auto* literal = des_expr.literal()) {
          project_field = field("field_" + ToChars(num_columns + i), literal->type());
        }
        ARROW_ASSIGN_OR_RAISE(
            project_schema,
            project_schema->AddField(num_columns + i, std::move(project_field)));
        i++;
        expressions.emplace_back(des_expr);
      }

      DeclarationInfo project_declaration{
          compute::Declaration::Sequence({
              std::move(input.declaration),
              {"project", compute::ProjectNodeOptions{std::move(expressions)}},
          }),
          project_schema};

      return ProcessEmit(std::move(project), std::move(project_declaration),
                         std::move(project_schema));
    }

    case substrait::Rel::RelTypeCase::kJoin: {
      const auto& join = rel.join();
      RETURN_NOT_OK(CheckRelCommon(join, conversion_options));

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

      ARROW_ASSIGN_OR_RAISE(auto left,
                            FromProto(join.left(), ext_set, conversion_options));
      ARROW_ASSIGN_OR_RAISE(auto right,
                            FromProto(join.right(), ext_set, conversion_options));

      if (!join.has_expression()) {
        return Status::Invalid("substrait::JoinRel with no expression");
      }

      ARROW_ASSIGN_OR_RAISE(auto expression,
                            FromProto(join.expression(), ext_set, conversion_options));

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

      // Create output schema from left, right relations and join keys
      FieldVector combined_fields = left.output_schema->fields();
      const FieldVector& right_fields = right.output_schema->fields();
      combined_fields.insert(combined_fields.end(), right_fields.begin(),
                             right_fields.end());
      std::shared_ptr<Schema> join_schema = schema(std::move(combined_fields));

      // adjust the join_keys according to Substrait definition where
      // the join fields are defined by considering the `join_schema` which
      // is the combination of the left and right relation schema.

      // TODO: ARROW-16624 Add Suffix support for Substrait
      const auto* left_keys = callptr->arguments[0].field_ref();
      const auto* right_keys = callptr->arguments[1].field_ref();
      // Validating JoinKeys
      if (!left_keys || !right_keys) {
        return Status::Invalid(
            "join condition must include references to both left and right inputs");
      }
      int num_left_fields = left.output_schema->num_fields();
      const auto* right_field_path = right_keys->field_path();
      std::vector<int> adjusted_field_indices(right_field_path->indices());
      adjusted_field_indices[0] -= num_left_fields;
      FieldPath adjusted_right_keys(adjusted_field_indices);
      compute::HashJoinNodeOptions join_options{{std::move(*left_keys)},
                                                {std::move(adjusted_right_keys)}};
      join_options.join_type = join_type;
      join_options.key_cmp = {join_key_cmp};
      compute::Declaration join_dec{"hashjoin", std::move(join_options)};
      join_dec.inputs.emplace_back(std::move(left.declaration));
      join_dec.inputs.emplace_back(std::move(right.declaration));

      DeclarationInfo join_declaration{std::move(join_dec), join_schema};

      return ProcessEmit(std::move(join), std::move(join_declaration),
                         std::move(join_schema));
    }
    case substrait::Rel::RelTypeCase::kAggregate: {
      const auto& aggregate = rel.aggregate();
      RETURN_NOT_OK(CheckRelCommon(aggregate, conversion_options));

      if (!aggregate.has_input()) {
        return Status::Invalid("substrait::AggregateRel with no input relation");
      }

      ARROW_ASSIGN_OR_RAISE(auto input,
                            FromProto(aggregate.input(), ext_set, conversion_options));

      if (aggregate.groupings_size() > 1) {
        return Status::NotImplemented(
            "Grouping sets not supported.  AggregateRel::groupings may not have more "
            "than one item");
      }

      // prepare output schema from aggregates
      auto input_schema = input.output_schema;
      // store key fields to be used when output schema is created
      std::vector<int> key_field_ids;
      std::vector<FieldRef> keys;
      if (aggregate.groupings_size() > 0) {
        const substrait::AggregateRel::Grouping& group = aggregate.groupings(0);
        int grouping_expr_size = group.grouping_expressions_size();
        keys.reserve(grouping_expr_size);
        key_field_ids.reserve(grouping_expr_size);
        for (int exp_id = 0; exp_id < grouping_expr_size; exp_id++) {
          ARROW_ASSIGN_OR_RAISE(
              compute::Expression expr,
              FromProto(group.grouping_expressions(exp_id), ext_set, conversion_options));
          const FieldRef* field_ref = expr.field_ref();
          if (field_ref) {
            ARROW_ASSIGN_OR_RAISE(auto match, field_ref->FindOne(*input_schema));
            key_field_ids.emplace_back(std::move(match[0]));
            keys.emplace_back(std::move(*field_ref));
          } else {
            return Status::Invalid(
                "The grouping expression for an aggregate must be a direct reference.");
          }
        }
      }

      int measure_size = aggregate.measures_size();
      std::vector<compute::Aggregate> aggregates;
      aggregates.reserve(measure_size);
      // store aggregate fields to be used when output schema is created
      std::vector<int> agg_src_field_ids(measure_size);
      for (int measure_id = 0; measure_id < measure_size; measure_id++) {
        const auto& agg_measure = aggregate.measures(measure_id);
        if (agg_measure.has_measure()) {
          if (agg_measure.has_filter()) {
            return Status::NotImplemented("Aggregate filters are not supported.");
          }
          const auto& agg_func = agg_measure.measure();
          ARROW_ASSIGN_OR_RAISE(
              SubstraitCall aggregate_call,
              FromProto(agg_func, !keys.empty(), ext_set, conversion_options));
          ExtensionIdRegistry::SubstraitAggregateToArrow converter;
          if (aggregate_call.id().uri.empty() || aggregate_call.id().uri[0] == '/') {
            ARROW_ASSIGN_OR_RAISE(
                converter, ext_set.registry()->GetSubstraitAggregateToArrowFallback(
                               aggregate_call.id().name));
          } else {
            ARROW_ASSIGN_OR_RAISE(
                converter,
                ext_set.registry()->GetSubstraitAggregateToArrow(aggregate_call.id()));
          }
          ARROW_ASSIGN_OR_RAISE(compute::Aggregate arrow_agg, converter(aggregate_call));

          // find aggregate field ids from schema
          const auto field_ref = arrow_agg.target;
          ARROW_ASSIGN_OR_RAISE(auto match, field_ref.FindOne(*input_schema));
          agg_src_field_ids[measure_id] = match[0];

          aggregates.push_back(std::move(arrow_agg));
        } else {
          return Status::Invalid("substrait::AggregateFunction not provided");
        }
      }
      FieldVector output_fields;
      output_fields.reserve(key_field_ids.size() + agg_src_field_ids.size());
      // extract aggregate fields to output schema
      for (int id = 0; id < static_cast<int>(agg_src_field_ids.size()); id++) {
        output_fields.emplace_back(input_schema->field(agg_src_field_ids[id]));
      }
      // extract key fields to output schema
      for (int id = 0; id < static_cast<int>(key_field_ids.size()); id++) {
        output_fields.emplace_back(input_schema->field(key_field_ids[id]));
      }

      std::shared_ptr<Schema> aggregate_schema = schema(std::move(output_fields));

      DeclarationInfo aggregate_declaration{
          compute::Declaration::Sequence(
              {std::move(input.declaration),
               {"aggregate", compute::AggregateNodeOptions{aggregates, keys}}}),
          aggregate_schema};

      return ProcessEmit(std::move(aggregate), std::move(aggregate_declaration),
                         std::move(aggregate_schema));
    }

    case substrait::Rel::RelTypeCase::kExtensionLeaf:
    case substrait::Rel::RelTypeCase::kExtensionSingle:
    case substrait::Rel::RelTypeCase::kExtensionMulti: {
      ARROW_ASSIGN_OR_RAISE(auto ext_rel_info,
                            GetExtensionRelationInfo(rel, ext_set, conversion_options));
      const auto& ext_decl_info = ext_rel_info.decl_info;
      auto ext_common_opt = GetExtensionRelCommon(rel);
      bool has_emit = ext_common_opt && ext_common_opt->emit_kind_case() ==
                                            substrait::RelCommon::EmitKindCase::kEmit;
      if (!ext_rel_info.field_output_indices) {
        if (!has_emit) {
          return ProcessEmit(ext_common_opt, ext_decl_info, ext_decl_info.output_schema);
        }
        return Status::NotImplemented("Emit not supported by ",
                                      ext_decl_info.declaration.factory_name);
      }
      std::vector<int> emit_order;
      if (has_emit) {
        const auto& emit_info = ext_common_opt->emit();
        emit_order.reserve(emit_info.output_mapping_size());
        for (const auto& emit_idx : emit_info.output_mapping()) {
          emit_order.push_back(emit_idx);
        }
      } else {
        // default output mapping
        int emit_size = 0;
        for (const auto& input : ext_rel_info.inputs) {
          emit_size += input.output_schema->num_fields();
        }
        emit_order.reserve(emit_size);
        for (int emit_idx = 0; emit_idx < emit_size; emit_idx++) {
          emit_order.push_back(emit_idx);
        }
      }
      return ProcessExtensionEmit(std::move(ext_decl_info), emit_order,
                                  *ext_rel_info.field_output_indices);
    }

    default:
      break;
  }

  return Status::NotImplemented(
      "conversion to arrow::compute::Declaration from Substrait relation ",
      rel.DebugString());
}

namespace {

Result<std::shared_ptr<Schema>> ExtractSchemaToBind(const compute::Declaration& declr) {
  std::shared_ptr<Schema> bind_schema;
  if (declr.factory_name == "scan") {
    const auto& opts = checked_cast<const dataset::ScanNodeOptions&>(*(declr.options));
    bind_schema = opts.dataset->schema();
  } else if (declr.factory_name == "filter") {
    auto input_declr = std::get<compute::Declaration>(declr.inputs[0]);
    ARROW_ASSIGN_OR_RAISE(bind_schema, ExtractSchemaToBind(input_declr));
  } else if (declr.factory_name == "sink") {
    // Note that the sink has no output_schema
    return bind_schema;
  } else {
    return Status::Invalid("Schema extraction failed, unsupported factory ",
                           declr.factory_name);
  }
  return bind_schema;
}

Result<std::unique_ptr<substrait::ReadRel>> ScanRelationConverter(
    const std::shared_ptr<Schema>& schema, const compute::Declaration& declaration,
    ExtensionSet* ext_set, const ConversionOptions& conversion_options) {
  auto read_rel = std::make_unique<substrait::ReadRel>();
  const auto& scan_node_options =
      checked_cast<const dataset::ScanNodeOptions&>(*declaration.options);
  auto dataset =
      dynamic_cast<dataset::FileSystemDataset*>(scan_node_options.dataset.get());
  if (dataset == nullptr) {
    return Status::Invalid(
        "Can only convert scan node with FileSystemDataset to a Substrait plan.");
  }

  // set schema
  ARROW_ASSIGN_OR_RAISE(auto named_struct,
                        ToProto(*dataset->schema(), ext_set, conversion_options));
  read_rel->set_allocated_base_schema(named_struct.release());

  // set local files
  auto read_rel_lfs = std::make_unique<substrait::ReadRel::LocalFiles>();
  for (const auto& file : dataset->files()) {
    auto read_rel_lfs_ffs =
        std::make_unique<substrait::ReadRel::LocalFiles::FileOrFiles>();
    read_rel_lfs_ffs->set_uri_path(UriFromAbsolutePath(file));
    // set file format
    auto format_type_name = dataset->format()->type_name();
    if (format_type_name == "parquet") {
      read_rel_lfs_ffs->set_allocated_parquet(
          new substrait::ReadRel::LocalFiles::FileOrFiles::ParquetReadOptions());
    } else if (format_type_name == "ipc") {
      read_rel_lfs_ffs->set_allocated_arrow(
          new substrait::ReadRel::LocalFiles::FileOrFiles::ArrowReadOptions());
    } else if (format_type_name == "orc") {
      read_rel_lfs_ffs->set_allocated_orc(
          new substrait::ReadRel::LocalFiles::FileOrFiles::OrcReadOptions());
    } else {
      return Status::NotImplemented("Unsupported file type: ", format_type_name);
    }
    read_rel_lfs->mutable_items()->AddAllocated(read_rel_lfs_ffs.release());
  }
  read_rel->set_allocated_local_files(read_rel_lfs.release());
  return std::move(read_rel);
}

Result<std::unique_ptr<substrait::FilterRel>> FilterRelationConverter(
    const std::shared_ptr<Schema>& schema, const compute::Declaration& declaration,
    ExtensionSet* ext_set, const ConversionOptions& conversion_options) {
  auto filter_rel = std::make_unique<substrait::FilterRel>();
  const auto& filter_node_options =
      checked_cast<const compute::FilterNodeOptions&>(*(declaration.options));

  auto filter_expr = filter_node_options.filter_expression;
  compute::Expression bound_expression;
  if (!filter_expr.IsBound()) {
    ARROW_ASSIGN_OR_RAISE(bound_expression, filter_expr.Bind(*schema));
  }

  if (declaration.inputs.size() == 0) {
    return Status::Invalid("Filter node doesn't have an input.");
  }

  // handling input
  auto declr_input = declaration.inputs[0];
  ARROW_ASSIGN_OR_RAISE(
      auto input_rel,
      ToProto(std::get<compute::Declaration>(declr_input), ext_set, conversion_options));
  filter_rel->set_allocated_input(input_rel.release());

  ARROW_ASSIGN_OR_RAISE(auto subs_expr,
                        ToProto(bound_expression, ext_set, conversion_options));
  filter_rel->set_allocated_condition(subs_expr.release());
  return std::move(filter_rel);
}

}  // namespace

Status SerializeAndCombineRelations(const compute::Declaration& declaration,
                                    ExtensionSet* ext_set,
                                    std::unique_ptr<substrait::Rel>* rel,
                                    const ConversionOptions& conversion_options) {
  const auto& factory_name = declaration.factory_name;
  ARROW_ASSIGN_OR_RAISE(auto schema, ExtractSchemaToBind(declaration));
  // Note that the sink declaration factory doesn't exist for serialization as
  // Substrait doesn't deal with a sink node definition

  if (factory_name == "scan") {
    ARROW_ASSIGN_OR_RAISE(
        auto read_rel,
        ScanRelationConverter(schema, declaration, ext_set, conversion_options));
    (*rel)->set_allocated_read(read_rel.release());
  } else if (factory_name == "filter") {
    ARROW_ASSIGN_OR_RAISE(
        auto filter_rel,
        FilterRelationConverter(schema, declaration, ext_set, conversion_options));
    (*rel)->set_allocated_filter(filter_rel.release());
  } else if (factory_name == "sink") {
    // Generally when a plan is deserialized the declaration will be a sink declaration.
    // Since there is no Sink relation in substrait, this function would be recursively
    // called on the input of the Sink declaration.
    auto sink_input_decl = std::get<compute::Declaration>(declaration.inputs[0]);
    RETURN_NOT_OK(
        SerializeAndCombineRelations(sink_input_decl, ext_set, rel, conversion_options));
  } else {
    return Status::NotImplemented("Factory ", factory_name,
                                  " not implemented for roundtripping.");
  }

  return Status::OK();
}

Result<std::unique_ptr<substrait::Rel>> ToProto(
    const compute::Declaration& declr, ExtensionSet* ext_set,
    const ConversionOptions& conversion_options) {
  auto rel = std::make_unique<substrait::Rel>();
  RETURN_NOT_OK(SerializeAndCombineRelations(declr, ext_set, &rel, conversion_options));
  return std::move(rel);
}

}  // namespace engine
}  // namespace arrow
