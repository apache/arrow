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

#include "arrow/dataset/partition.h"

#include <algorithm>
#include <chrono>
#include <memory>
#include <set>
#include <stack>
#include <utility>
#include <vector>

#include "arrow/array/array_base.h"
#include "arrow/array/array_nested.h"
#include "arrow/array/builder_binary.h"
#include "arrow/compute/api_scalar.h"
#include "arrow/dataset/dataset_internal.h"
#include "arrow/dataset/file_base.h"
#include "arrow/dataset/filter.h"
#include "arrow/dataset/scanner.h"
#include "arrow/dataset/scanner_internal.h"
#include "arrow/filesystem/filesystem.h"
#include "arrow/filesystem/path_util.h"
#include "arrow/scalar.h"
#include "arrow/util/iterator.h"
#include "arrow/util/logging.h"
#include "arrow/util/range.h"
#include "arrow/util/sort.h"
#include "arrow/util/string_view.h"

namespace arrow {

using internal::checked_cast;
using internal::checked_pointer_cast;
using util::string_view;

namespace dataset {

std::shared_ptr<Partitioning> Partitioning::Default() {
  class DefaultPartitioning : public Partitioning {
   public:
    DefaultPartitioning() : Partitioning(::arrow::schema({})) {}

    std::string type_name() const override { return "default"; }

    Result<std::shared_ptr<Expression>> Parse(const std::string& path) const override {
      return scalar(true);
    }

    Result<std::string> Format(const Expression& expr) const override {
      return Status::NotImplemented("formatting paths from ", type_name(),
                                    " Partitioning");
    }

    Result<std::vector<PartitionedBatch>> Partition(
        const std::shared_ptr<RecordBatch>& batch) const override {
      return std::vector<PartitionedBatch>{{batch, scalar(true)}};
    }
  };

  return std::make_shared<DefaultPartitioning>();
}

Status KeyValuePartitioning::VisitKeys(
    const Expression& expr,
    const std::function<Status(const std::string& name,
                               const std::shared_ptr<Scalar>& value)>& visitor) {
  if (expr.type() == ExpressionType::AND) {
    const auto& and_ = checked_cast<const AndExpression&>(expr);
    RETURN_NOT_OK(VisitKeys(*and_.left_operand(), visitor));
    RETURN_NOT_OK(VisitKeys(*and_.right_operand(), visitor));
    return Status::OK();
  }

  if (expr.type() != ExpressionType::COMPARISON) {
    return Status::OK();
  }

  const auto& cmp = checked_cast<const ComparisonExpression&>(expr);
  if (cmp.op() != compute::CompareOperator::EQUAL) {
    return Status::OK();
  }

  auto lhs = cmp.left_operand().get();
  auto rhs = cmp.right_operand().get();
  if (lhs->type() != ExpressionType::FIELD) std::swap(lhs, rhs);

  if (lhs->type() != ExpressionType::FIELD || rhs->type() != ExpressionType::SCALAR) {
    return Status::OK();
  }

  return visitor(checked_cast<const FieldExpression*>(lhs)->name(),
                 checked_cast<const ScalarExpression*>(rhs)->value());
}

Result<std::unordered_map<std::string, std::shared_ptr<Scalar>>>
KeyValuePartitioning::GetKeys(const Expression& expr) {
  std::unordered_map<std::string, std::shared_ptr<Scalar>> keys;
  RETURN_NOT_OK(
      VisitKeys(expr, [&](const std::string& name, const std::shared_ptr<Scalar>& value) {
        keys.emplace(name, value);
        return Status::OK();
      }));
  return keys;
}

Status KeyValuePartitioning::SetDefaultValuesFromKeys(const Expression& expr,
                                                      RecordBatchProjector* projector) {
  return KeyValuePartitioning::VisitKeys(
      expr, [projector](const std::string& name, const std::shared_ptr<Scalar>& value) {
        ARROW_ASSIGN_OR_RAISE(auto match,
                              FieldRef(name).FindOneOrNone(*projector->schema()));
        if (!match) {
          return Status::OK();
        }
        return projector->SetDefaultValue(match, value);
      });
}

inline std::shared_ptr<Expression> ConjunctionFromGroupingRow(Scalar* row) {
  ScalarVector* values = &checked_cast<StructScalar*>(row)->value;
  ExpressionVector equality_expressions(values->size());
  for (size_t i = 0; i < values->size(); ++i) {
    const std::string& name = row->type->field(static_cast<int>(i))->name();
    equality_expressions[i] = equal(field_ref(name), scalar(std::move(values->at(i))));
  }
  return and_(std::move(equality_expressions));
}

Result<std::vector<Partitioning::PartitionedBatch>> KeyValuePartitioning::Partition(
    const std::shared_ptr<RecordBatch>& batch) const {
  FieldVector by_fields;
  ArrayVector by_columns;

  std::shared_ptr<RecordBatch> rest = batch;
  for (const auto& partition_field : schema_->fields()) {
    ARROW_ASSIGN_OR_RAISE(
        auto match, FieldRef(partition_field->name()).FindOneOrNone(*rest->schema()))

    if (match) {
      by_fields.push_back(partition_field);
      by_columns.push_back(rest->column(match[0]));
      ARROW_ASSIGN_OR_RAISE(rest, rest->RemoveColumn(match[0]));
    }
  }

  if (by_fields.empty()) {
    // no fields to group by; return the whole batch
    return std::vector<PartitionedBatch>{{batch, scalar(true)}};
  }

  ARROW_ASSIGN_OR_RAISE(auto by,
                        StructArray::Make(std::move(by_columns), std::move(by_fields)));
  ARROW_ASSIGN_OR_RAISE(auto groupings_and_values, MakeGroupings(*by));
  auto groupings =
      checked_pointer_cast<ListArray>(groupings_and_values->GetFieldByName("groupings"));
  auto unique_rows = groupings_and_values->GetFieldByName("values");

  ARROW_ASSIGN_OR_RAISE(auto grouped_batches, ApplyGroupings(*groupings, rest));

  std::vector<PartitionedBatch> out(grouped_batches.size());
  for (size_t i = 0; i < out.size(); ++i) {
    ARROW_ASSIGN_OR_RAISE(auto row, unique_rows->GetScalar(i));
    out[i].partition_expression = ConjunctionFromGroupingRow(row.get());
    out[i].batch = std::move(grouped_batches[i]);
  }
  return out;
}

Result<std::shared_ptr<Expression>> KeyValuePartitioning::ConvertKey(
    const Key& key) const {
  ARROW_ASSIGN_OR_RAISE(auto match, FieldRef(key.name).FindOneOrNone(*schema_));
  if (!match) {
    return scalar(true);
  }

  auto field_index = match[0];
  auto field = schema_->field(field_index);

  std::shared_ptr<Scalar> converted;

  if (field->type()->id() == Type::DICTIONARY) {
    if (dictionaries_.empty() || dictionaries_[field_index] == nullptr) {
      return Status::Invalid("No dictionary provided for dictionary field ",
                             field->ToString());
    }

    DictionaryScalar::ValueType value;
    value.dictionary = dictionaries_[field_index];

    if (!value.dictionary->type()->Equals(
            checked_cast<const DictionaryType&>(*field->type()).value_type())) {
      return Status::TypeError("Dictionary supplied for field ", field->ToString(),
                               " had incorrect type ",
                               value.dictionary->type()->ToString());
    }

    // look up the partition value in the dictionary
    ARROW_ASSIGN_OR_RAISE(converted, Scalar::Parse(value.dictionary->type(), key.value));
    ARROW_ASSIGN_OR_RAISE(auto index, compute::IndexIn(converted, value.dictionary));
    value.index = index.scalar();
    if (!value.index->is_valid) {
      return Status::Invalid("Dictionary supplied for field ", field->ToString(),
                             " does not contain '", key.value, "'");
    }
    converted = std::make_shared<DictionaryScalar>(std::move(value), field->type());
  } else {
    ARROW_ASSIGN_OR_RAISE(converted, Scalar::Parse(field->type(), key.value));
  }

  return equal(field_ref(field->name()), scalar(std::move(converted)));
}

Result<std::shared_ptr<Expression>> KeyValuePartitioning::Parse(
    const std::string& path) const {
  ExpressionVector expressions;

  for (const Key& key : ParseKeys(path)) {
    ARROW_ASSIGN_OR_RAISE(auto expr, ConvertKey(key));

    if (expr->Equals(true)) continue;

    expressions.push_back(std::move(expr));
  }

  return and_(std::move(expressions));
}

Result<std::string> KeyValuePartitioning::Format(const Expression& expr) const {
  std::vector<Scalar*> values{static_cast<size_t>(schema_->num_fields()), nullptr};

  RETURN_NOT_OK(VisitKeys(expr, [&](const std::string& name,
                                    const std::shared_ptr<Scalar>& value) {
    ARROW_ASSIGN_OR_RAISE(auto match, FieldRef(name).FindOneOrNone(*schema_));
    if (match) {
      const auto& field = schema_->field(match[0]);
      if (!value->type->Equals(field->type())) {
        return Status::TypeError("scalar ", value->ToString(), " (of type ", *value->type,
                                 ") is invalid for ", field->ToString());
      }

      values[match[0]] = value.get();
    }
    return Status::OK();
  }));

  return FormatValues(values);
}

std::vector<KeyValuePartitioning::Key> DirectoryPartitioning::ParseKeys(
    const std::string& path) const {
  std::vector<Key> keys;

  int i = 0;
  for (auto&& segment : fs::internal::SplitAbstractPath(path)) {
    if (i >= schema_->num_fields()) break;

    keys.push_back({schema_->field(i++)->name(), std::move(segment)});
  }

  return keys;
}

inline util::optional<int> NextValid(const std::vector<Scalar*>& values, int first_null) {
  auto it = std::find_if(values.begin() + first_null + 1, values.end(),
                         [](Scalar* v) { return v != nullptr; });

  if (it == values.end()) {
    return util::nullopt;
  }

  return static_cast<int>(it - values.begin());
}

Result<std::string> DirectoryPartitioning::FormatValues(
    const std::vector<Scalar*>& values) const {
  std::vector<std::string> segments(static_cast<size_t>(schema_->num_fields()));

  for (int i = 0; i < schema_->num_fields(); ++i) {
    if (values[i] != nullptr) {
      segments[i] = values[i]->ToString();
      continue;
    }

    if (auto illegal_index = NextValid(values, i)) {
      // XXX maybe we should just ignore keys provided after the first absent one?
      return Status::Invalid("No partition key for ", schema_->field(i)->name(),
                             " but a key was provided subsequently for ",
                             schema_->field(*illegal_index)->name(), ".");
    }

    // if all subsequent keys are absent we'll just print the available keys
    break;
  }

  return fs::internal::JoinAbstractPath(std::move(segments));
}

class KeyValuePartitioningInspectImpl {
 public:
  explicit KeyValuePartitioningInspectImpl(const PartitioningFactoryOptions& options)
      : options_(options) {}

  static Result<std::shared_ptr<DataType>> InferType(const std::string& name,
                                                     const std::set<std::string>& reprs,
                                                     int max_partition_dictionary_size) {
    if (reprs.empty()) {
      return Status::Invalid("No segments were available for field '", name,
                             "'; couldn't infer type");
    }

    bool all_integral = std::all_of(reprs.begin(), reprs.end(), [](string_view repr) {
      // TODO(bkietz) use ParseUnsigned or so
      return repr.find_first_not_of("0123456789") == string_view::npos;
    });

    if (all_integral) {
      return int32();
    }

    if (reprs.size() > static_cast<size_t>(max_partition_dictionary_size)) {
      return utf8();
    }

    return dictionary(int32(), utf8());
  }

  int GetOrInsertField(const std::string& name) {
    auto name_index =
        name_to_index_.emplace(name, static_cast<int>(name_to_index_.size())).first;

    if (static_cast<size_t>(name_index->second) >= values_.size()) {
      values_.resize(name_index->second + 1);
    }
    return name_index->second;
  }

  void InsertRepr(const std::string& name, std::string repr) {
    InsertRepr(GetOrInsertField(name), std::move(repr));
  }

  void InsertRepr(int index, std::string repr) { values_[index].insert(std::move(repr)); }

  Result<std::shared_ptr<Schema>> Finish(ArrayVector* dictionaries) {
    dictionaries->clear();

    if (options_.max_partition_dictionary_size != 0) {
      dictionaries->resize(name_to_index_.size());
    }

    std::vector<std::shared_ptr<Field>> fields(name_to_index_.size());

    for (const auto& name_index : name_to_index_) {
      const auto& name = name_index.first;
      auto index = name_index.second;
      ARROW_ASSIGN_OR_RAISE(auto type, InferType(name, values_[index],
                                                 options_.max_partition_dictionary_size));
      if (type->id() == Type::DICTIONARY) {
        StringBuilder builder;
        for (const auto& repr : values_[index]) {
          RETURN_NOT_OK(builder.Append(repr));
        }
        RETURN_NOT_OK(builder.Finish(&dictionaries->at(index)));
      }
      fields[index] = field(name, std::move(type));
    }

    return ::arrow::schema(std::move(fields));
  }

  std::vector<std::string> FieldNames() {
    std::vector<std::string> names(name_to_index_.size());

    for (auto kv : name_to_index_) {
      names[kv.second] = kv.first;
    }
    return names;
  }

 private:
  std::unordered_map<std::string, int> name_to_index_;
  std::vector<std::set<std::string>> values_;
  const PartitioningFactoryOptions& options_;
};

class DirectoryPartitioningFactory : public PartitioningFactory {
 public:
  DirectoryPartitioningFactory(std::vector<std::string> field_names,
                               PartitioningFactoryOptions options)
      : field_names_(std::move(field_names)), options_(options) {}

  std::string type_name() const override { return "schema"; }

  Result<std::shared_ptr<Schema>> Inspect(
      const std::vector<std::string>& paths) override {
    KeyValuePartitioningInspectImpl impl(options_);

    for (const auto& name : field_names_) {
      impl.GetOrInsertField(name);
    }

    for (auto path : paths) {
      size_t field_index = 0;
      for (auto&& segment : fs::internal::SplitAbstractPath(path)) {
        if (field_index == field_names_.size()) break;

        impl.InsertRepr(static_cast<int>(field_index++), std::move(segment));
      }
    }

    return impl.Finish(&dictionaries_);
  }

  Result<std::shared_ptr<Partitioning>> Finish(
      const std::shared_ptr<Schema>& schema) const override {
    for (FieldRef ref : field_names_) {
      // ensure all of field_names_ are present in schema
      RETURN_NOT_OK(ref.FindOne(*schema).status());
    }

    // drop fields which aren't in field_names_
    auto out_schema = SchemaFromColumnNames(schema, field_names_);

    return std::make_shared<DirectoryPartitioning>(std::move(out_schema), dictionaries_);
  }

 private:
  std::vector<std::string> field_names_;
  ArrayVector dictionaries_;
  PartitioningFactoryOptions options_;
};

std::shared_ptr<PartitioningFactory> DirectoryPartitioning::MakeFactory(
    std::vector<std::string> field_names, PartitioningFactoryOptions options) {
  return std::shared_ptr<PartitioningFactory>(
      new DirectoryPartitioningFactory(std::move(field_names), options));
}

util::optional<KeyValuePartitioning::Key> HivePartitioning::ParseKey(
    const std::string& segment) {
  auto name_end = string_view(segment).find_first_of('=');
  if (name_end == string_view::npos) {
    return util::nullopt;
  }

  return Key{segment.substr(0, name_end), segment.substr(name_end + 1)};
}

std::vector<KeyValuePartitioning::Key> HivePartitioning::ParseKeys(
    const std::string& path) const {
  std::vector<Key> keys;

  for (const auto& segment : fs::internal::SplitAbstractPath(path)) {
    if (auto key = ParseKey(segment)) {
      keys.push_back(std::move(*key));
    }
  }

  return keys;
}

Result<std::string> HivePartitioning::FormatValues(
    const std::vector<Scalar*>& values) const {
  std::vector<std::string> segments(static_cast<size_t>(schema_->num_fields()));

  for (int i = 0; i < schema_->num_fields(); ++i) {
    const std::string& name = schema_->field(i)->name();

    if (values[i] == nullptr) {
      if (!NextValid(values, i)) break;

      // If no key is available just provide a placeholder segment to maintain the
      // field_index <-> path nesting relation
      segments[i] = name;
    } else {
      segments[i] = name + "=" + values[i]->ToString();
    }
  }

  return fs::internal::JoinAbstractPath(std::move(segments));
}

class HivePartitioningFactory : public PartitioningFactory {
 public:
  explicit HivePartitioningFactory(PartitioningFactoryOptions options)
      : options_(options) {}

  std::string type_name() const override { return "hive"; }

  Result<std::shared_ptr<Schema>> Inspect(
      const std::vector<std::string>& paths) override {
    KeyValuePartitioningInspectImpl impl(options_);

    for (auto path : paths) {
      for (auto&& segment : fs::internal::SplitAbstractPath(path)) {
        if (auto key = HivePartitioning::ParseKey(segment)) {
          impl.InsertRepr(key->name, key->value);
        }
      }
    }

    field_names_ = impl.FieldNames();
    return impl.Finish(&dictionaries_);
  }

  Result<std::shared_ptr<Partitioning>> Finish(
      const std::shared_ptr<Schema>& schema) const override {
    if (dictionaries_.empty()) {
      return std::make_shared<HivePartitioning>(schema, dictionaries_);
    } else {
      for (FieldRef ref : field_names_) {
        // ensure all of field_names_ are present in schema
        RETURN_NOT_OK(ref.FindOne(*schema).status());
      }

      // drop fields which aren't in field_names_
      auto out_schema = SchemaFromColumnNames(schema, field_names_);

      return std::make_shared<HivePartitioning>(std::move(out_schema), dictionaries_);
    }
  }

 private:
  std::vector<std::string> field_names_;
  ArrayVector dictionaries_;
  PartitioningFactoryOptions options_;
};

std::shared_ptr<PartitioningFactory> HivePartitioning::MakeFactory(
    PartitioningFactoryOptions options) {
  return std::shared_ptr<PartitioningFactory>(new HivePartitioningFactory(options));
}

std::string StripPrefixAndFilename(const std::string& path, const std::string& prefix) {
  auto maybe_base_less = fs::internal::RemoveAncestor(prefix, path);
  auto base_less = maybe_base_less ? maybe_base_less->to_string() : path;
  auto basename_filename = fs::internal::GetAbstractPathParent(base_less);
  return basename_filename.first;
}

std::vector<std::string> StripPrefixAndFilename(const std::vector<std::string>& paths,
                                                const std::string& prefix) {
  std::vector<std::string> result;
  result.reserve(paths.size());
  for (const auto& path : paths) {
    result.emplace_back(StripPrefixAndFilename(path, prefix));
  }
  return result;
}

std::vector<std::string> StripPrefixAndFilename(const std::vector<fs::FileInfo>& files,
                                                const std::string& prefix) {
  std::vector<std::string> result;
  result.reserve(files.size());
  for (const auto& info : files) {
    result.emplace_back(StripPrefixAndFilename(info.path(), prefix));
  }
  return result;
}

Result<std::shared_ptr<Schema>> PartitioningOrFactory::GetOrInferSchema(
    const std::vector<std::string>& paths) {
  if (auto part = partitioning()) {
    return part->schema();
  }

  return factory()->Inspect(paths);
}

}  // namespace dataset
}  // namespace arrow
