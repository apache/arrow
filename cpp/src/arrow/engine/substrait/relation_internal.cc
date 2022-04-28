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
#include <iostream>
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

Result<FieldRef> FromProto(const substrait::Expression& expr, const std::string& what) {
  int32_t index;
  switch (expr.rex_type_case()) {
    case substrait::Expression::RexTypeCase::kSelection: {
      const auto& selection = expr.selection();
      switch (selection.root_type_case()) {
        case substrait::Expression_FieldReference::RootTypeCase::kRootReference: {
          break;
        }
        default: {
          return Status::NotImplemented(std::string(
              "substrait::Expression with non-root-reference for ") + what);
        }
      }
      switch (selection.reference_type_case()) {
        case substrait::Expression_FieldReference::ReferenceTypeCase::kDirectReference: {
          const auto& direct_reference = selection.direct_reference();
          switch (direct_reference.reference_type_case()) {
            case substrait::Expression_ReferenceSegment::ReferenceTypeCase::kStructField:
            {
              break;
            }
            default: {
              return Status::NotImplemented(std::string(
                  "substrait::Expression with non-struct-field for ") + what);
            }
          }
          const auto& struct_field = direct_reference.struct_field();
          if (struct_field.has_child()) {
            return Status::NotImplemented(std::string(
                "substrait::Expression with non-flat struct-field for ") + what);
          }
          index = struct_field.field();
          break;
        }
        default: {
          return Status::NotImplemented(std::string(
              "substrait::Expression with non-direct reference for ") + what);
        }
      }
      break;
    }
    default: {
      return Status::NotImplemented(std::string(
          "substrait::AsOfMergeRel with non-selection for ") + what);
    }
  }
  return FieldRef(FieldPath({index}));
}

Result<std::vector<FieldRef>> FromProto(
    const google::protobuf::RepeatedPtrField<substrait::Expression>& exprs,
    const std::string& what) {
  std::vector<FieldRef> fields;
  int size = exprs.size();
  for (int i = 0; i < size; i++) {
      ARROW_ASSIGN_OR_RAISE(
          FieldRef field, FromProto(exprs[i], what));
      fields.push_back(field);
  }
  return fields;
}

Result<compute::Declaration> FromProtoInternal(
    const substrait::Rel& rel,
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

      if (read.has_filter()) {
        ARROW_ASSIGN_OR_RAISE(scan_options->filter, FromProto(read.filter(), ext_set));
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

      std::shared_ptr<dataset::FileFormat> format;
      auto filesystem = std::make_shared<fs::LocalFileSystem>();
      std::vector<std::shared_ptr<dataset::FileFragment>> fragments;

      for (const auto& item : read.local_files().items()) {
        if (item.path_type_case() !=
            substrait::ReadRel_LocalFiles_FileOrFiles::kUriFile) {
          return Status::NotImplemented(
              "substrait::ReadRel::LocalFiles::FileOrFiles with "
              "path_type other than uri_file");
        }

        util::string_view uri_file{item.uri_file()};

        if (item.format() ==
            substrait::ReadRel::LocalFiles::FileOrFiles::FILE_FORMAT_PARQUET) {
          format = std::make_shared<dataset::ParquetFileFormat>();
        } else if (uri_file.ends_with(".arrow")) {
          format = std::make_shared<dataset::IpcFileFormat>();
        } else if (uri_file.ends_with(".feather")) {
          format = std::make_shared<dataset::IpcFileFormat>();
        } else {
          return Status::NotImplemented(
              "substrait::ReadRel::LocalFiles::FileOrFiles::format "
              "other than FILE_FORMAT_PARQUET and not recognized");
        }

        if (!uri_file.starts_with("file:///")) {
          return Status::NotImplemented(
              "substrait::ReadRel::LocalFiles::FileOrFiles::uri_file "
              "with other than local filesystem (file:///)");
        }
        auto path = item.uri_file().substr(7);

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

        ARROW_ASSIGN_OR_RAISE(auto fragment, format->MakeFragment(dataset::FileSource{
                                                 std::move(path), filesystem}));
        fragments.push_back(std::move(fragment));
      }

      ARROW_ASSIGN_OR_RAISE(
          auto ds, dataset::FileSystemDataset::Make(
                       std::move(base_schema), /*root_partition=*/compute::literal(true),
                       std::move(format), std::move(filesystem), std::move(fragments)));

      return compute::Declaration{
          "scan", dataset::ScanNodeOptions{std::move(ds), std::move(scan_options)}};
    }

    case substrait::Rel::RelTypeCase::kWrite: {
      const auto& write = rel.write();
      RETURN_NOT_OK(CheckRelCommon(write));

      if (!write.has_input()) {
        return Status::Invalid("substrait::WriteRel with no input relation");
      }
      ARROW_ASSIGN_OR_RAISE(auto input, FromProtoInternal(write.input(), ext_set));

      if (!write.has_local_files()) {
        return Status::NotImplemented(
            "substrait::WriteRel with write_type other than LocalFiles");
      }

      if (write.local_files().has_advanced_extension()) {
        return Status::NotImplemented(
            "substrait::WriteRel::LocalFiles::advanced_extension");
      }

      std::shared_ptr<dataset::FileFormat> format;
      auto filesystem = std::make_shared<fs::LocalFileSystem>();

      if (write.local_files().items().size() != 1) {
        return Status::NotImplemented(
            "substrait::WriteRel with non-single LocalFiles items");
      }

      dataset::FileSystemDatasetWriteOptions write_options;
      write_options.filesystem = filesystem;
      write_options.partitioning = std::make_shared<dataset::EmptyPartitioning>();

      for (const auto& item : write.local_files().items()) {
        if (item.path_type_case() !=
            substrait::WriteRel_LocalFiles_FileOrFiles::kUriFile) {
          return Status::NotImplemented(
              "substrait::WriteRel::LocalFiles::FileOrFiles with "
              "path_type other than uri_file");
        }

        util::string_view uri_file{item.uri_file()};

        if (item.format() ==
            substrait::WriteRel::LocalFiles::FileOrFiles::FILE_FORMAT_PARQUET) {
          format = std::make_shared<dataset::ParquetFileFormat>();
        } else if (uri_file.ends_with(".arrow")) {
          format = std::make_shared<dataset::IpcFileFormat>();
        } else if (uri_file.ends_with(".feather")) {
          format = std::make_shared<dataset::IpcFileFormat>();
        } else {
          return Status::NotImplemented(
              "substrait::WriteRel::LocalFiles::FileOrFiles::format "
              "other than FILE_FORMAT_PARQUET and not recognized");
        }
        write_options.file_write_options = format->DefaultWriteOptions();

        if (!uri_file.starts_with("file:///")) {
          return Status::NotImplemented(
              "substrait::WriteRel::LocalFiles::FileOrFiles::uri_file "
              "with other than local filesystem (file:///)");
        }
        auto path = item.uri_file().substr(7);

        if (item.partition_index() != 0) {
          return Status::NotImplemented(
              "non-default "
              "substrait::WriteRel::LocalFiles::FileOrFiles::partition_index");
        }

        if (item.start_row() != 0) {
          return Status::NotImplemented(
              "non-default substrait::ReadRel::LocalFiles::FileOrFiles::start_row");
        }

        if (item.number_of_rows() != 0) {
          return Status::NotImplemented(
              "non-default substrait::ReadRel::LocalFiles::FileOrFiles::number_of_rows");
        }

        auto path_pair = fs::internal::GetAbstractPathParent(path);
        write_options.basename_template = path_pair.second;
        write_options.base_dir = path_pair.first;
      }

      return compute::Declaration::Sequence({
          std::move(input),
          {"tee", dataset::WriteNodeOptions{std::move(write_options), nullptr}},
      });
    }

    case substrait::Rel::RelTypeCase::kFilter: {
      const auto& filter = rel.filter();
      RETURN_NOT_OK(CheckRelCommon(filter));

      if (!filter.has_input()) {
        return Status::Invalid("substrait::FilterRel with no input relation");
      }
      ARROW_ASSIGN_OR_RAISE(auto input, FromProtoInternal(filter.input(), ext_set));

      if (!filter.has_condition()) {
        return Status::Invalid("substrait::FilterRel with no condition expression");
      }
      ARROW_ASSIGN_OR_RAISE(auto condition, FromProto(filter.condition(), ext_set));

      return compute::Declaration::Sequence({
          std::move(input),
          {"filter", compute::FilterNodeOptions{std::move(condition)}},
      });
    }

    case substrait::Rel::RelTypeCase::kProject: {
      const auto& project = rel.project();
      RETURN_NOT_OK(CheckRelCommon(project));

      if (!project.has_input()) {
        return Status::Invalid("substrait::ProjectRel with no input relation");
      }
      ARROW_ASSIGN_OR_RAISE(auto input, FromProtoInternal(project.input(), ext_set));

      std::vector<compute::Expression> expressions;
      for (const auto& expr : project.expressions()) {
        expressions.emplace_back();
        ARROW_ASSIGN_OR_RAISE(expressions.back(), FromProto(expr, ext_set));
      }

      return compute::Declaration::Sequence({
          std::move(input),
          {"project",
           compute::ProjectNodeOptions{std::move(expressions)}
          },
      });
    }

    case substrait::Rel::RelTypeCase::kAsOfMerge: {
      const auto& as_of_merge = rel.as_of_merge();
      RETURN_NOT_OK(CheckRelCommon(as_of_merge));

      auto inputs_size = as_of_merge.inputs_size();
      if (inputs_size < 2) {
        return Status::Invalid("substrait::AsOfMergeRel with fewer than 2 inputs");
      }
      if (inputs_size > 6) {
        return Status::Invalid("substrait::AsOfMergeRel with more than 6 inputs");
      }
      if (as_of_merge.version_case() != substrait::AsOfMergeRel::VersionCase::kV1) {
        return Status::Invalid("substrait::AsOfMergeRel with unsupported version");
      }

      const auto& v1 = as_of_merge.v1();
      ARROW_ASSIGN_OR_RAISE(
          auto key_fields, FromProto(v1.key_fields(), "AsOfMerge key field"));
      ARROW_ASSIGN_OR_RAISE(
          auto time_fields, FromProto(v1.time_fields(), "AsOfMerge time field"));
      int64_t tolerance = as_of_merge.v1().tolerance();

      std::vector<compute::Declaration::Input> inputs;
      inputs.reserve(inputs_size);
      for (auto input_rel : as_of_merge.inputs()) {
        ARROW_ASSIGN_OR_RAISE(auto decl, FromProtoInternal(input_rel, ext_set));
        auto input = compute::Declaration::Input(decl);
        inputs.push_back(input);
      }
      return compute::Declaration{
          "as_of_merge",
          inputs,
          compute::AsOfMergeV1NodeOptions{key_fields, time_fields, tolerance}
      };
    }

    default:
      break;
  }

  return Status::NotImplemented(
      "conversion to arrow::compute::Declaration from Substrait relation ",
      rel.DebugString());
}

Result<compute::Declaration> FromProto(const substrait::Rel& rel,
                                       const ExtensionSet& ext_set,
                                       const std::vector<std::string>& names) {
  ARROW_ASSIGN_OR_RAISE(auto input, FromProtoInternal(rel, ext_set));
  int names_size = names.size();
  std::vector<compute::Expression> expressions;
  for (int i = 0; i < names_size; i++) {
    expressions.push_back(compute::field_ref(FieldRef(i)));
  }
  return compute::Declaration::Sequence({
      std::move(input),
      {"project",
       compute::ProjectNodeOptions{std::move(expressions), std::move(names)}
      },
  });
}

}  // namespace engine
}  // namespace arrow
