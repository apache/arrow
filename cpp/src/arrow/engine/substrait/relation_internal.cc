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

#include <cstdint>
#include <functional>
#include <iterator>
#include <memory>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

#include "arrow/acero/aggregate_node.h"
#include "arrow/acero/exec_plan.h"
#include "arrow/acero/options.h"
#include "arrow/compute/api_aggregate.h"
#include "arrow/compute/expression.h"
#include "arrow/compute/kernel.h"
#include "arrow/dataset/dataset.h"
#include "arrow/dataset/discovery.h"
#include "arrow/dataset/file_base.h"
#include "arrow/dataset/file_ipc.h"
#include "arrow/dataset/file_parquet.h"
#include "arrow/dataset/plan.h"
#include "arrow/dataset/scanner.h"
#include "arrow/datum.h"
#include "arrow/engine/substrait/expression_internal.h"
#include "arrow/engine/substrait/extension_set.h"
#include "arrow/engine/substrait/options.h"
#include "arrow/engine/substrait/options_internal.h"
#include "arrow/engine/substrait/relation.h"
#include "arrow/engine/substrait/type_internal.h"
#include "arrow/engine/substrait/util.h"
#include "arrow/engine/substrait/util_internal.h"
#include "arrow/filesystem/filesystem.h"
#include "arrow/filesystem/localfs.h"
#include "arrow/filesystem/type_fwd.h"
#include "arrow/filesystem/util_internal.h"
#include "arrow/io/type_fwd.h"
#include "arrow/status.h"
#include "arrow/type.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/logging.h"
#include "arrow/util/string.h"
#include "arrow/util/uri.h"

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

template <typename RelMessage>
Result<EmitInfo> GetEmitInfo(const RelMessage& rel,
                             const std::shared_ptr<Schema>& input_schema) {
  const auto& emit = rel.common().emit();
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

Result<DeclarationInfo> ProcessEmitProject(
    std::optional<substrait::RelCommon> rel_common_opt,
    const DeclarationInfo& project_declr, const std::shared_ptr<Schema>& input_schema) {
  if (rel_common_opt) {
    switch (rel_common_opt->emit_kind_case()) {
      case substrait::RelCommon::EmitKindCase::kDirect:
        return project_declr;
      case substrait::RelCommon::EmitKindCase::kEmit: {
        const auto& emit = rel_common_opt->emit();
        int emit_size = emit.output_mapping_size();
        const auto& proj_options = checked_cast<const acero::ProjectNodeOptions&>(
            *project_declr.declaration.options);
        FieldVector emit_fields(emit_size);
        std::vector<compute::Expression> emit_proj_exprs(emit_size);
        for (int i = 0; i < emit_size; i++) {
          int32_t map_id = emit.output_mapping(i);
          emit_fields[i] = input_schema->field(map_id);
          emit_proj_exprs[i] = std::move(proj_options.expressions[map_id]);
        }
        // Note: DeclarationInfo is created by considering the input to the
        // ProjectRel and the ProjectNodeOptions are set by only considering
        // what is in the emit expression in Substrait.
        return DeclarationInfo{
            acero::Declaration::Sequence(
                {std::get<acero::Declaration>(project_declr.declaration.inputs[0]),
                 {"project", acero::ProjectNodeOptions{std::move(emit_proj_exprs)}}}),
            schema(std::move(emit_fields))};
      }
      default:
        return Status::Invalid("Invalid emit case");
    }
  } else {
    return project_declr;
  }
}

template <typename RelMessage>
Result<DeclarationInfo> ProcessEmit(const RelMessage& rel,
                                    const DeclarationInfo& no_emit_declr,
                                    const std::shared_ptr<Schema>& schema) {
  if (rel.has_common()) {
    switch (rel.common().emit_kind_case()) {
      case substrait::RelCommon::EmitKindCase::kDirect:
        return no_emit_declr;
      case substrait::RelCommon::EmitKindCase::kEmit: {
        ARROW_ASSIGN_OR_RAISE(auto emit_info, GetEmitInfo(rel, schema));
        return DeclarationInfo{
            acero::Declaration::Sequence({no_emit_declr.declaration,
                                          {"project", acero::ProjectNodeOptions{std::move(
                                                          emit_info.expressions)}}}),
            std::move(emit_info.schema)};
      }
      default:
        return Status::Invalid("Invalid emit case");
    }
  } else {
    return no_emit_declr;
  }
}
/// In the specialization, a single ProjectNode is being used to
/// get the Acero relation with or without emit.
template <>
Result<DeclarationInfo> ProcessEmit(const substrait::ProjectRel& rel,
                                    const DeclarationInfo& no_emit_declr,
                                    const std::shared_ptr<Schema>& schema) {
  return ProcessEmitProject(rel.has_common() ? std::optional(rel.common()) : std::nullopt,
                            no_emit_declr, schema);
}

Result<DeclarationInfo> ProcessExtensionEmit(const DeclarationInfo& no_emit_declr,
                                             const std::vector<int>& emit_order) {
  const std::shared_ptr<Schema>& input_schema = no_emit_declr.output_schema;
  std::vector<compute::Expression> proj_field_refs;
  proj_field_refs.reserve(emit_order.size());
  FieldVector emit_fields;
  emit_fields.reserve(emit_order.size());

  for (int emit_idx : emit_order) {
    if (emit_idx < 0 || emit_idx >= input_schema->num_fields()) {
      return Status::Invalid("Out of bounds emit index ", emit_idx);
    }
    proj_field_refs.push_back(compute::field_ref(FieldRef(emit_idx)));
    emit_fields.push_back(input_schema->field(emit_idx));
  }

  std::shared_ptr<Schema> emit_schema = schema(std::move(emit_fields));

  return DeclarationInfo{
      acero::Declaration::Sequence(
          {no_emit_declr.declaration,
           {"project", acero::ProjectNodeOptions{std::move(proj_field_refs)}}}),
      std::move(emit_schema)};
}

Result<DeclarationInfo> GetExtensionInfo(const substrait::Rel& rel,
                                         const ExtensionSet& ext_set,
                                         const ConversionOptions& conv_opts,
                                         std::vector<DeclarationInfo>* inputs_arg) {
  if (inputs_arg == nullptr) {
    std::vector<DeclarationInfo> inputs_tmp;
    return GetExtensionInfo(rel, ext_set, conv_opts, &inputs_tmp);
  }
  std::vector<DeclarationInfo>& inputs = *inputs_arg;
  inputs.clear();
  switch (rel.rel_type_case()) {
    case substrait::Rel::RelTypeCase::kExtensionLeaf: {
      const auto& ext = rel.extension_leaf();
      DefaultExtensionDetails detail{ext.detail()};
      return conv_opts.extension_provider->MakeRel(conv_opts, inputs, detail, ext_set);
    }

    case substrait::Rel::RelTypeCase::kExtensionSingle: {
      const auto& ext = rel.extension_single();
      ARROW_ASSIGN_OR_RAISE(DeclarationInfo input_info,
                            FromProto(ext.input(), ext_set, conv_opts));
      inputs.push_back(std::move(input_info));
      DefaultExtensionDetails detail{ext.detail()};
      return conv_opts.extension_provider->MakeRel(conv_opts, inputs, detail, ext_set);
    }

    case substrait::Rel::RelTypeCase::kExtensionMulti: {
      const auto& ext = rel.extension_multi();
      for (const auto& input : ext.inputs()) {
        ARROW_ASSIGN_OR_RAISE(auto input_info, FromProto(input, ext_set, conv_opts));
        inputs.push_back(std::move(input_info));
      }
      DefaultExtensionDetails detail{ext.detail()};
      return conv_opts.extension_provider->MakeRel(conv_opts, inputs, detail, ext_set);
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

namespace internal {

Result<compute::Aggregate> ParseAggregateMeasure(
    const substrait::AggregateRel::Measure& agg_measure, const ExtensionSet& ext_set,
    const ConversionOptions& conversion_options, bool is_hash,
    const std::shared_ptr<Schema> input_schema) {
  if (agg_measure.has_measure()) {
    if (agg_measure.has_filter()) {
      return Status::NotImplemented("Aggregate filters are not supported.");
    }
    const auto& agg_func = agg_measure.measure();
    ARROW_ASSIGN_OR_RAISE(SubstraitCall aggregate_call,
                          FromProto(agg_func, is_hash, ext_set, conversion_options));
    ExtensionIdRegistry::SubstraitAggregateToArrow converter;
    if (aggregate_call.id().uri.empty() || aggregate_call.id().uri[0] == '/') {
      ARROW_ASSIGN_OR_RAISE(converter,
                            ext_set.registry()->GetSubstraitAggregateToArrowFallback(
                                aggregate_call.id().name));
    } else {
      ARROW_ASSIGN_OR_RAISE(converter, ext_set.registry()->GetSubstraitAggregateToArrow(
                                           aggregate_call.id()));
    }
    return converter(aggregate_call);
  } else {
    return Status::Invalid("substrait::AggregateFunction not provided");
  }
}

ARROW_ENGINE_EXPORT Result<DeclarationInfo> MakeAggregateDeclaration(
    acero::Declaration input_decl, std::shared_ptr<Schema> aggregate_schema,
    std::vector<compute::Aggregate> aggregates, std::vector<FieldRef> keys,
    std::vector<FieldRef> segment_keys) {
  return DeclarationInfo{
      acero::Declaration::Sequence(
          {std::move(input_decl),
           {"aggregate", acero::AggregateNodeOptions{aggregates, keys, segment_keys}}}),
      aggregate_schema};
}

}  // namespace internal

namespace {

struct SortBehavior {
  compute::NullPlacement null_placement;
  compute::SortOrder sort_order;

  static Result<SortBehavior> Make(substrait::SortField::SortDirection dir) {
    SortBehavior sort_behavior;
    switch (dir) {
      case substrait::SortField::SortDirection::
          SortField_SortDirection_SORT_DIRECTION_UNSPECIFIED:
        return Status::Invalid("The substrait plan does not specify a sort direction");
      case substrait::SortField::SortDirection::
          SortField_SortDirection_SORT_DIRECTION_ASC_NULLS_FIRST:
        sort_behavior.null_placement = compute::NullPlacement::AtStart;
        sort_behavior.sort_order = compute::SortOrder::Ascending;
        break;
      case substrait::SortField::SortDirection::
          SortField_SortDirection_SORT_DIRECTION_ASC_NULLS_LAST:
        sort_behavior.null_placement = compute::NullPlacement::AtEnd;
        sort_behavior.sort_order = compute::SortOrder::Ascending;
        break;
      case substrait::SortField::SortDirection::
          SortField_SortDirection_SORT_DIRECTION_DESC_NULLS_FIRST:
        sort_behavior.null_placement = compute::NullPlacement::AtStart;
        sort_behavior.sort_order = compute::SortOrder::Descending;
        break;
      case substrait::SortField::SortDirection::
          SortField_SortDirection_SORT_DIRECTION_DESC_NULLS_LAST:
        sort_behavior.null_placement = compute::NullPlacement::AtEnd;
        sort_behavior.sort_order = compute::SortOrder::Descending;
        break;
      case substrait::SortField::SortDirection::
          SortField_SortDirection_SORT_DIRECTION_CLUSTERED:
      default:
        return Status::NotImplemented(
            "Acero does not support the specified sort direction: ", dir);
    }
    return sort_behavior;
  }
};

}  // namespace

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
        ARROW_ASSIGN_OR_RAISE(acero::Declaration source_decl,
                              named_table_provider(table_names, *base_schema));

        if (!source_decl.IsValid()) {
          return Status::Invalid("Invalid NamedTable Source");
        }

        return ProcessEmit(read, DeclarationInfo{std::move(source_decl), base_schema},
                           base_schema);
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
              "non-default "
              "substrait::ReadRel::LocalFiles::FileOrFiles::partition_index");
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
          acero::Declaration{"scan", dataset::ScanNodeOptions{ds, scan_options}},
          base_schema};

      return ProcessEmit(read, scan_declaration, base_schema);
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
          acero::Declaration::Sequence({
              std::move(input.declaration),
              {"filter", acero::FilterNodeOptions{std::move(condition)}},
          }),
          input.output_schema};

      return ProcessEmit(filter, filter_declaration, input.output_schema);
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
        ARROW_ASSIGN_OR_RAISE(compute::Expression bound_expr,
                              des_expr.Bind(*input.output_schema));
        if (auto* expr_call = bound_expr.call()) {
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
          acero::Declaration::Sequence({
              std::move(input.declaration),
              {"project", acero::ProjectNodeOptions{std::move(expressions)}},
          }),
          project_schema};

      return ProcessEmit(project, project_declaration, project_schema);
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

      acero::JoinType join_type;
      switch (join.type()) {
        case substrait::JoinRel::JOIN_TYPE_UNSPECIFIED:
          return Status::NotImplemented("Unspecified join type is not supported");
        case substrait::JoinRel::JOIN_TYPE_INNER:
          join_type = acero::JoinType::INNER;
          break;
        case substrait::JoinRel::JOIN_TYPE_OUTER:
          join_type = acero::JoinType::FULL_OUTER;
          break;
        case substrait::JoinRel::JOIN_TYPE_LEFT:
          join_type = acero::JoinType::LEFT_OUTER;
          break;
        case substrait::JoinRel::JOIN_TYPE_RIGHT:
          join_type = acero::JoinType::RIGHT_OUTER;
          break;
        case substrait::JoinRel::JOIN_TYPE_SEMI:
          join_type = acero::JoinType::LEFT_SEMI;
          break;
        case substrait::JoinRel::JOIN_TYPE_ANTI:
          join_type = acero::JoinType::LEFT_ANTI;
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

      acero::JoinKeyCmp join_key_cmp;
      if (callptr->function_name == "equal") {
        join_key_cmp = acero::JoinKeyCmp::EQ;
      } else if (callptr->function_name == "is_not_distinct_from") {
        join_key_cmp = acero::JoinKeyCmp::IS;
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
      acero::HashJoinNodeOptions join_options{{std::move(*left_keys)},
                                              {std::move(adjusted_right_keys)}};
      join_options.join_type = join_type;
      join_options.key_cmp = {join_key_cmp};
      acero::Declaration join_dec{"hashjoin", std::move(join_options)};
      join_dec.inputs.emplace_back(std::move(left.declaration));
      join_dec.inputs.emplace_back(std::move(right.declaration));

      DeclarationInfo join_declaration{std::move(join_dec), join_schema};

      return ProcessEmit(join, join_declaration, join_schema);
    }
    case substrait::Rel::RelTypeCase::kFetch: {
      const auto& fetch = rel.fetch();
      RETURN_NOT_OK(CheckRelCommon(fetch, conversion_options));

      if (!fetch.has_input()) {
        return Status::Invalid("substrait::FetchRel with no input relation");
      }

      ARROW_ASSIGN_OR_RAISE(auto input,
                            FromProto(fetch.input(), ext_set, conversion_options));

      int64_t offset = fetch.offset();
      int64_t count = fetch.count();

      acero::Declaration fetch_dec{
          "fetch", {input.declaration}, acero::FetchNodeOptions(offset, count)};

      DeclarationInfo fetch_declaration{std::move(fetch_dec), input.output_schema};
      return ProcessEmit(fetch, std::move(fetch_declaration),
                         fetch_declaration.output_schema);
    }
    case substrait::Rel::RelTypeCase::kSort: {
      const auto& sort = rel.sort();
      RETURN_NOT_OK(CheckRelCommon(sort, conversion_options));

      if (!sort.has_input()) {
        return Status::Invalid("substrait::SortRel with no input relation");
      }

      ARROW_ASSIGN_OR_RAISE(auto input,
                            FromProto(sort.input(), ext_set, conversion_options));

      if (sort.sorts_size() == 0) {
        return Status::Invalid("substrait::SortRel with no sorts");
      }

      std::vector<compute::SortKey> sort_keys;
      sort_keys.reserve(sort.sorts_size());
      // Substrait allows null placement to differ for each field.  Acero expects it to
      // be consistent across all fields.  So we grab the null placement from the first
      // key and verify all other keys have the same null placement
      std::optional<SortBehavior> sample_sort_behavior;
      for (const auto& sort : sort.sorts()) {
        ARROW_ASSIGN_OR_RAISE(SortBehavior sort_behavior,
                              SortBehavior::Make(sort.direction()));
        if (sample_sort_behavior) {
          if (sample_sort_behavior->null_placement != sort_behavior.null_placement) {
            return Status::NotImplemented(
                "substrait::SortRel with ordering with mixed null placement");
          }
        } else {
          sample_sort_behavior = sort_behavior;
        }
        if (sort.sort_kind_case() != substrait::SortField::SortKindCase::kDirection) {
          return Status::NotImplemented("substrait::SortRel with custom sort function");
        }
        ARROW_ASSIGN_OR_RAISE(compute::Expression expr,
                              FromProto(sort.expr(), ext_set, conversion_options));
        const FieldRef* field_ref = expr.field_ref();
        if (field_ref) {
          sort_keys.push_back(compute::SortKey(*field_ref, sort_behavior.sort_order));
        } else {
          return Status::Invalid("Sort key expressions must be a direct reference.");
        }
      }

      DCHECK(sample_sort_behavior.has_value());
      acero::Declaration sort_dec{
          "order_by",
          {input.declaration},
          acero::OrderByNodeOptions(compute::Ordering(
              std::move(sort_keys), sample_sort_behavior->null_placement))};

      DeclarationInfo sort_declaration{std::move(sort_dec), input.output_schema};
      return ProcessEmit(sort, std::move(sort_declaration),
                         sort_declaration.output_schema);
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
      std::vector<FieldRef> keys;
      if (aggregate.groupings_size() > 0) {
        const substrait::AggregateRel::Grouping& group = aggregate.groupings(0);
        int grouping_expr_size = group.grouping_expressions_size();
        keys.reserve(grouping_expr_size);
        for (int exp_id = 0; exp_id < grouping_expr_size; exp_id++) {
          ARROW_ASSIGN_OR_RAISE(
              compute::Expression expr,
              FromProto(group.grouping_expressions(exp_id), ext_set, conversion_options));
          const FieldRef* field_ref = expr.field_ref();
          if (field_ref) {
            keys.emplace_back(std::move(*field_ref));
          } else {
            return Status::Invalid(
                "The grouping expression for an aggregate must be a direct reference.");
          }
        }
      }

      const int measure_size = aggregate.measures_size();
      std::vector<compute::Aggregate> aggregates;
      aggregates.reserve(measure_size);
      for (int measure_id = 0; measure_id < measure_size; measure_id++) {
        const auto& agg_measure = aggregate.measures(measure_id);
        ARROW_ASSIGN_OR_RAISE(
            auto aggregate,
            internal::ParseAggregateMeasure(agg_measure, ext_set, conversion_options,
                                            /*is_hash=*/!keys.empty(), input_schema));
        aggregates.push_back(std::move(aggregate));
      }

      ARROW_ASSIGN_OR_RAISE(auto aggregate_schema,
                            acero::aggregate::MakeOutputSchema(
                                input_schema, keys, /*segment_keys=*/{}, aggregates));

      ARROW_ASSIGN_OR_RAISE(
          auto aggregate_declaration,
          internal::MakeAggregateDeclaration(std::move(input.declaration),
                                             aggregate_schema, std::move(aggregates),
                                             std::move(keys), /*segment_keys=*/{}));

      return ProcessEmit(std::move(aggregate), std::move(aggregate_declaration),
                         std::move(aggregate_schema));
    }

    case substrait::Rel::RelTypeCase::kExtensionLeaf:
    case substrait::Rel::RelTypeCase::kExtensionSingle:
    case substrait::Rel::RelTypeCase::kExtensionMulti: {
      std::vector<DeclarationInfo> ext_rel_inputs;
      ARROW_ASSIGN_OR_RAISE(
          auto ext_decl_info,
          GetExtensionInfo(rel, ext_set, conversion_options, &ext_rel_inputs));
      auto ext_common_opt = GetExtensionRelCommon(rel);
      bool has_emit = ext_common_opt && ext_common_opt->emit_kind_case() ==
                                            substrait::RelCommon::EmitKindCase::kEmit;
      // Set up the emit order - an ordered list of indices that specifies an output
      // mapping as expected by Substrait. This is a sublist of [0..N), where N is the
      // total number of input fields across all inputs of the relation, that selects
      // from these input fields.
      if (has_emit) {
        std::vector<int> emit_order;
        // the emit order is defined in the Substrait plan - pick it up
        const auto& emit_info = ext_common_opt->emit();
        emit_order.reserve(emit_info.output_mapping_size());
        for (const auto& emit_idx : emit_info.output_mapping()) {
          emit_order.push_back(emit_idx);
        }
        return ProcessExtensionEmit(std::move(ext_decl_info), emit_order);
      } else {
        return ext_decl_info;
      }
    }

    case substrait::Rel::RelTypeCase::kSet: {
      const auto& set = rel.set();
      RETURN_NOT_OK(CheckRelCommon(set, conversion_options));

      if (set.inputs_size() < 2) {
        return Status::Invalid(
            "substrait::SetRel with inadequate number of input relations, ",
            set.inputs_size());
      }
      substrait::SetRel_SetOp op = set.op();
      // Note: at the moment Acero only supports UNION_ALL operation
      switch (op) {
        case substrait::SetRel::SET_OP_UNSPECIFIED:
        case substrait::SetRel::SET_OP_MINUS_PRIMARY:
        case substrait::SetRel::SET_OP_MINUS_MULTISET:
        case substrait::SetRel::SET_OP_INTERSECTION_PRIMARY:
        case substrait::SetRel::SET_OP_INTERSECTION_MULTISET:
        case substrait::SetRel::SET_OP_UNION_DISTINCT:
          return Status::NotImplemented(
              "NotImplemented union type : ",
              EnumToString(op, *substrait::SetRel_SetOp_descriptor()));
        case substrait::SetRel::SET_OP_UNION_ALL:
          break;
        default:
          return Status::Invalid("Unknown union type");
      }
      int input_size = set.inputs_size();
      acero::Declaration union_declr{"union", acero::ExecNodeOptions{}};
      std::shared_ptr<Schema> union_schema;
      for (int input_id = 0; input_id < input_size; input_id++) {
        ARROW_ASSIGN_OR_RAISE(
            auto input, FromProto(set.inputs(input_id), ext_set, conversion_options));
        union_declr.inputs.emplace_back(std::move(input.declaration));
        if (union_schema == nullptr) {
          union_schema = input.output_schema;
        }
      }

      auto set_declaration = DeclarationInfo{union_declr, union_schema};
      return ProcessEmit(set, set_declaration, union_schema);
    }

    default:
      break;
  }

  return Status::NotImplemented(
      "conversion to arrow::acero::Declaration from Substrait relation ",
      rel.DebugString());
}

namespace {

Result<std::shared_ptr<Schema>> ExtractSchemaToBind(const acero::Declaration& declr) {
  std::shared_ptr<Schema> bind_schema;
  if (declr.factory_name == "scan") {
    const auto& opts = checked_cast<const dataset::ScanNodeOptions&>(*(declr.options));
    bind_schema = opts.dataset->schema();
  } else if (declr.factory_name == "filter") {
    auto input_declr = std::get<acero::Declaration>(declr.inputs[0]);
    ARROW_ASSIGN_OR_RAISE(bind_schema, ExtractSchemaToBind(input_declr));
  } else if (declr.factory_name == "named_table") {
    const auto& opts = checked_cast<const acero::NamedTableNodeOptions&>(*declr.options);
    bind_schema = opts.schema;
  } else if (declr.factory_name == "sink") {
    // Note that the sink has no output_schema
    return bind_schema;
  } else {
    return Status::Invalid("Schema extraction failed, unsupported factory ",
                           declr.factory_name);
  }
  return bind_schema;
}

Result<std::unique_ptr<substrait::ReadRel>> NamedTableRelationConverter(
    const std::shared_ptr<Schema>& schema, const acero::Declaration& declaration,
    ExtensionSet* ext_set, const ConversionOptions& conversion_options) {
  auto read_rel = std::make_unique<substrait::ReadRel>();
  const auto& named_table_options =
      checked_cast<const acero::NamedTableNodeOptions&>(*declaration.options);

  // set schema
  ARROW_ASSIGN_OR_RAISE(auto named_struct, ToProto(*schema, ext_set, conversion_options));
  read_rel->set_allocated_base_schema(named_struct.release());

  if (named_table_options.names.empty()) {
    return Status::Invalid("Table names cannot be empty");
  }

  auto read_rel_tn = std::make_unique<substrait::ReadRel::NamedTable>();
  for (auto& name : named_table_options.names) {
    read_rel_tn->add_names(name);
  }
  read_rel->set_allocated_named_table(read_rel_tn.release());

  return std::move(read_rel);
}

Result<std::unique_ptr<substrait::ReadRel>> ScanRelationConverter(
    const std::shared_ptr<Schema>& schema, const acero::Declaration& declaration,
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
  ARROW_ASSIGN_OR_RAISE(auto named_struct, ToProto(*schema, ext_set, conversion_options));
  read_rel->set_allocated_base_schema(named_struct.release());

  // set local files
  auto read_rel_lfs = std::make_unique<substrait::ReadRel::LocalFiles>();
  for (const auto& file : dataset->files()) {
    auto read_rel_lfs_ffs =
        std::make_unique<substrait::ReadRel::LocalFiles::FileOrFiles>();
    ARROW_ASSIGN_OR_RAISE(auto uri_path, UriFromAbsolutePath(file));
    read_rel_lfs_ffs->set_uri_path(std::move(uri_path));
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
    const std::shared_ptr<Schema>& schema, const acero::Declaration& declaration,
    ExtensionSet* ext_set, const ConversionOptions& conversion_options) {
  auto filter_rel = std::make_unique<substrait::FilterRel>();
  const auto& filter_node_options =
      checked_cast<const acero::FilterNodeOptions&>(*(declaration.options));

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
  ARROW_ASSIGN_OR_RAISE(auto input_rel, ToProto(std::get<acero::Declaration>(declr_input),
                                                ext_set, conversion_options));
  filter_rel->set_allocated_input(input_rel.release());

  ARROW_ASSIGN_OR_RAISE(auto subs_expr,
                        ToProto(bound_expression, ext_set, conversion_options));
  filter_rel->set_allocated_condition(subs_expr.release());
  return std::move(filter_rel);
}

}  // namespace

Status SerializeAndCombineRelations(const acero::Declaration& declaration,
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
  } else if (factory_name == "named_table") {
    ARROW_ASSIGN_OR_RAISE(
        auto read_rel,
        NamedTableRelationConverter(schema, declaration, ext_set, conversion_options));
    (*rel)->set_allocated_read(read_rel.release());
  } else if (factory_name == "sink") {
    // Generally when a plan is deserialized the declaration will be a sink declaration.
    // Since there is no Sink relation in substrait, this function would be recursively
    // called on the input of the Sink declaration.
    auto sink_input_decl = std::get<acero::Declaration>(declaration.inputs[0]);
    RETURN_NOT_OK(
        SerializeAndCombineRelations(sink_input_decl, ext_set, rel, conversion_options));
  } else {
    return Status::NotImplemented("Factory ", factory_name,
                                  " not implemented for roundtripping.");
  }

  return Status::OK();
}

Result<std::unique_ptr<substrait::Rel>> ToProto(
    const acero::Declaration& declr, ExtensionSet* ext_set,
    const ConversionOptions& conversion_options) {
  auto rel = std::make_unique<substrait::Rel>();
  RETURN_NOT_OK(SerializeAndCombineRelations(declr, ext_set, &rel, conversion_options));
  return std::move(rel);
}

}  // namespace engine
}  // namespace arrow
