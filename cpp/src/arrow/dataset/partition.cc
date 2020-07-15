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
#include "arrow/array/builder_binary.h"
#include "arrow/compute/api_scalar.h"
#include "arrow/dataset/dataset_internal.h"
#include "arrow/dataset/file_base.h"
#include "arrow/dataset/filter.h"
#include "arrow/dataset/scanner.h"
#include "arrow/filesystem/filesystem.h"
#include "arrow/filesystem/path_util.h"
#include "arrow/scalar.h"
#include "arrow/util/iterator.h"
#include "arrow/util/range.h"
#include "arrow/util/sort.h"
#include "arrow/util/string_view.h"

namespace arrow {
namespace dataset {

using util::string_view;

using arrow::internal::checked_cast;

Result<std::shared_ptr<Expression>> Partitioning::Parse(const std::string& path) const {
  ExpressionVector expressions;
  int i = 0;

  for (auto segment : fs::internal::SplitAbstractPath(path)) {
    ARROW_ASSIGN_OR_RAISE(auto expr, Parse(segment, i++));
    if (expr->Equals(true)) {
      continue;
    }

    expressions.push_back(std::move(expr));
  }

  return and_(std::move(expressions));
}

std::shared_ptr<Partitioning> Partitioning::Default() {
  return std::make_shared<DefaultPartitioning>();
}

Result<WritePlan> PartitioningFactory::MakeWritePlan(std::shared_ptr<Schema> schema,
                                                     FragmentIterator fragment_it) {
  return Status::NotImplemented("MakeWritePlan from PartitioningFactory of type ",
                                type_name());
}

Result<WritePlan> PartitioningFactory::MakeWritePlan(
    std::shared_ptr<Schema> schema, FragmentIterator fragment_it,
    std::shared_ptr<Schema> partition_schema) {
  return Status::NotImplemented("MakeWritePlan from PartitioningFactory of type ",
                                type_name());
}

Result<std::shared_ptr<Expression>> SegmentDictionaryPartitioning::Parse(
    const std::string& segment, int i) const {
  if (static_cast<size_t>(i) < dictionaries_.size()) {
    auto it = dictionaries_[i].find(segment);
    if (it != dictionaries_[i].end()) {
      return it->second;
    }
  }

  return scalar(true);
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
        if (match.indices().empty()) {
          return Status::OK();
        }
        return projector->SetDefaultValue(match, value);
      });
}

Result<std::shared_ptr<Expression>> KeyValuePartitioning::ConvertKey(
    const Key& key, const Schema& schema, const ArrayVector& dictionaries) {
  ARROW_ASSIGN_OR_RAISE(auto match, FieldRef(key.name).FindOneOrNone(schema));
  if (match.indices().empty()) {
    return scalar(true);
  }

  auto field_index = match.indices()[0];
  auto field = schema.field(field_index);

  std::shared_ptr<Scalar> converted;

  if (field->type()->id() == Type::DICTIONARY) {
    if (dictionaries.empty() || dictionaries[field_index] == nullptr) {
      return Status::Invalid("No dictionary provided for dictionary field ",
                             field->ToString());
    }

    DictionaryScalar::ValueType value;
    value.dictionary = dictionaries[field_index];

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

  return equal(field_ref(field->name()), scalar(converted));
}

Result<std::shared_ptr<Expression>> KeyValuePartitioning::Parse(
    const std::string& segment, int i) const {
  if (auto key = ParseKey(segment, i)) {
    return ConvertKey(*key, *schema_, dictionaries_);
  }

  return scalar(true);
}

Result<std::string> KeyValuePartitioning::Format(const Expression& expr, int i) const {
  if (expr.type() != ExpressionType::COMPARISON) {
    return Status::Invalid(expr.ToString(), " is not a comparison expression");
  }

  const auto& cmp = checked_cast<const ComparisonExpression&>(expr);
  if (cmp.op() != compute::CompareOperator::EQUAL) {
    return Status::Invalid(expr.ToString(), " is not an equality comparison expression");
  }

  if (cmp.left_operand()->type() != ExpressionType::FIELD) {
    return Status::Invalid(expr.ToString(), " LHS is not a field");
  }
  const auto& lhs = checked_cast<const FieldExpression&>(*cmp.left_operand());

  if (cmp.right_operand()->type() != ExpressionType::SCALAR) {
    return Status::Invalid(expr.ToString(), " RHS is not a scalar");
  }
  const auto& rhs = checked_cast<const ScalarExpression&>(*cmp.right_operand());

  auto expected_type = schema_->GetFieldByName(lhs.name())->type();
  if (!rhs.value()->type->Equals(expected_type)) {
    return Status::TypeError(expr.ToString(), " expected RHS to have type ",
                             *expected_type);
  }

  return FormatKey({lhs.name(), rhs.value()->ToString()}, i);
}

util::optional<KeyValuePartitioning::Key> DirectoryPartitioning::ParseKey(
    const std::string& segment, int i) const {
  if (i >= schema_->num_fields()) {
    return util::nullopt;
  }

  return Key{schema_->field(i)->name(), segment};
}

Result<std::string> DirectoryPartitioning::FormatKey(const Key& key, int i) const {
  if (schema_->GetFieldIndex(key.name) != i) {
    return Status::Invalid("field ", key.name, " in unexpected position ", i,
                           " for schema ", *schema_);
  }
  return key.value;
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

  struct MakeWritePlanImpl;

  Result<WritePlan> MakeWritePlan(std::shared_ptr<Schema> schema,
                                  FragmentIterator fragments) override;

  Result<WritePlan> MakeWritePlan(std::shared_ptr<Schema> schema,
                                  FragmentIterator fragments,
                                  std::shared_ptr<Schema> partition_schema) override;

 private:
  std::vector<std::string> field_names_;
  ArrayVector dictionaries_;
  PartitioningFactoryOptions options_;
};

struct DirectoryPartitioningFactory::MakeWritePlanImpl {
  using Indices = std::basic_string<int>;

  MakeWritePlanImpl(DirectoryPartitioningFactory* factory, std::shared_ptr<Schema> schema,
                    FragmentVector source_fragments)
      : this_(factory),
        schema_(std::move(schema)),
        source_fragments_(std::move(source_fragments)),
        right_hand_sides_(source_fragments_.size(), Indices(num_fields(), -1)) {}

  int num_fields() const { return static_cast<int>(this_->field_names_.size()); }

  // For a KeyValuePartitioning, every partition expression will be an equality
  // ComparisonExpression where the left operand is a FieldExpression and the right is a
  // ScalarExpression. Comparing Scalars directly is expensive, so first assemble a
  // dictionary containing the scalars from the right operands of every partition
  // expression. This allows later stages of MakeWritePlan to handle a scalar by its
  // dictionary code, which is both more compact to store and cheap to compare.
  //
  // Scalars are stored such that the dictionary code of a fragment's RHS in the
  // partition expression for a given field is given by
  //     int code = right_hand_sides_[fragment_index][field_index];
  // and the corresponding scalar can be retrieved with
  //     std::shared_ptr<Scalar> scalar = scalar_dict_.code_to_scalar[code];
  Status DictEncodeRightHandSides() {
    if (source_fragments_.empty()) {
      return Status::OK();
    }

    for (size_t fragment_i = 0; fragment_i < source_fragments_.size(); ++fragment_i) {
      const auto& fragment = source_fragments_[fragment_i];

      auto insert_representable_into_dict = [this, fragment_i](
                                                const std::string& name,
                                                const std::shared_ptr<Scalar>& value) {
        auto it = std::find(this_->field_names_.begin(), this_->field_names_.end(), name);
        if (it == this_->field_names_.end()) {
          return Status::OK();
        }

        auto field_i = it - this_->field_names_.begin();

        int code = scalar_dict_.GetOrInsert(value);
        right_hand_sides_[fragment_i][field_i] = code;

        return Status::OK();
      };

      RETURN_NOT_OK(KeyValuePartitioning::VisitKeys(*fragment->partition_expression(),
                                                    insert_representable_into_dict));

      auto it = std::find(right_hand_sides_[fragment_i].begin(),
                          right_hand_sides_[fragment_i].end(), -1);
      if (it != right_hand_sides_[fragment_i].end()) {
        // NB: this is an error when writing DirectoryPartitioning but not
        // HivePartitioning (as it will be valid to simply omit segments)
        return Status::Invalid(
            "fragment ", fragment_i, " had no partition expression for field '",
            this_->field_names_.at(it - right_hand_sides_[fragment_i].begin()), "'");
      }
    }

    return Status::OK();
  }

  // Infer the Partitioning schema from partition expressions.
  // For example if one partition expression is "omega"_ == 13
  // we can infer that the field "omega" has type int32
  Result<std::shared_ptr<Schema>> InferPartitioningSchema() const {
    if (source_fragments_.empty()) {
      return Status::Invalid(
          "No fragments were provided so the Partitioning schema could not be "
          "inferred.");
    }

    // NB: under DirectoryPartitioning every fragment has a partition expression for every
    // field, so we can infer the schema by looking only at the first fragment. This will
    // be more complicated for HivePartitioning.
    int fragment_i = 0;

    FieldVector fields(num_fields());
    for (int field_i = 0; field_i < num_fields(); ++field_i) {
      const auto& name = this_->field_names_[field_i];
      const auto& type =
          scalar_dict_.code_to_scalar[right_hand_sides_[fragment_i][field_i]]->type;
      fields[field_i] = field(name, type);
    }

    return schema(std::move(fields));
  }

  // reconstitute fragment_i's partition expression for field_i by reading the right
  // hand side from the scalar dictionary and constructing an equality
  // ComparisonExpression
  std::shared_ptr<Expression> PartitionExpression(size_t fragment_i, int field_i) {
    auto left_hand_side = field_ref(this_->field_names_[field_i]);
    auto right_hand_side =
        scalar(scalar_dict_.code_to_scalar[right_hand_sides_[fragment_i][field_i]]);
    return equal(std::move(left_hand_side), std::move(right_hand_side));
  }

  // create a guid by stringifying the number of milliseconds since the epoch
  std::string Guid() {
    using std::chrono::duration_cast;
    using std::chrono::milliseconds;
    using std::chrono::steady_clock;
    auto milliseconds_since_epoch =
        duration_cast<milliseconds>(steady_clock::now().time_since_epoch()).count();
    return std::to_string(milliseconds_since_epoch);
  }

  Result<WritePlan> Finish(std::shared_ptr<Schema> partitioning_schema = nullptr) && {
    WritePlan out;

    RETURN_NOT_OK(DictEncodeRightHandSides());

    if (partitioning_schema == nullptr) {
      ARROW_ASSIGN_OR_RAISE(partitioning_schema, InferPartitioningSchema());
    }
    ARROW_ASSIGN_OR_RAISE(out.partitioning,
                          this_->Finish(std::move(partitioning_schema)));

    // There's no guarantee that all Fragments have the same schema.
    ARROW_ASSIGN_OR_RAISE(out.schema,
                          UnifySchemas({out.partitioning->schema(), schema_}));

    // Lexicographic ordering WRT right_hand_sides_ ensures that source_fragments_ are in
    // a depth first visitation order WRT their partition expressions. This makes
    // generation of the full directory tree far simpler since a directory's files are
    // grouped.
    auto permutation = arrow::internal::ArgSort(right_hand_sides_);
    arrow::internal::Permute(permutation, &source_fragments_);
    arrow::internal::Permute(permutation, &right_hand_sides_);

    // the basename of out.paths[i] is stored in segments[i] (full paths will be assembled
    // after segments is complete)
    std::vector<std::string> segments;

    // out.paths[parents[i]] is the parent directory of out.paths[i]
    std::vector<int> parents;

    // current_right_hand_sides[field_i] is the RHS dictionary code for the current
    // partition expression corresponding to field_i
    Indices current_right_hand_sides(num_fields(), -1);

    // out.paths[current_parents[field_i]] is the current ancestor directory corresponding
    // to field_i
    Indices current_parents(num_fields() + 1, -1);

    for (size_t fragment_i = 0; fragment_i < source_fragments_.size(); ++fragment_i) {
      int field_i = 0;
      for (; field_i < num_fields(); ++field_i) {
        // these directories have already been created and we're still writing their
        // children
        if (right_hand_sides_[fragment_i][field_i] != current_right_hand_sides[field_i]) {
          break;
        }
      }

      for (; field_i < num_fields(); ++field_i) {
        // push a new directory
        current_parents[field_i + 1] = static_cast<int>(parents.size());
        parents.push_back(current_parents[field_i]);

        auto partition_expression = PartitionExpression(fragment_i, field_i);

        // format segment for partition_expression
        ARROW_ASSIGN_OR_RAISE(auto segment,
                              out.partitioning->Format(*partition_expression, field_i));
        segment.push_back(fs::internal::kSep);
        segments.push_back(std::move(segment));

        // store partition_expression for use in the written Dataset
        out.fragment_or_partition_expressions.emplace_back(
            std::move(partition_expression));

        current_right_hand_sides[field_i] = right_hand_sides_[fragment_i][field_i];
      }

      // push a fragment (not attempting to give files meaningful names)
      parents.push_back(current_parents[field_i]);
      segments.emplace_back(Guid() + "_" + std::to_string(fragment_i));

      // store a fragment for writing to disk
      out.fragment_or_partition_expressions.emplace_back(
          std::move(source_fragments_[fragment_i]));
    }

    // render paths from segments
    for (size_t i = 0; i < segments.size(); ++i) {
      if (parents[i] == -1) {
        out.paths.push_back(segments[i]);
        continue;
      }

      out.paths.push_back(out.paths[parents[i]] + segments[i]);
    }

    return out;
  }

  DirectoryPartitioningFactory* this_;
  std::shared_ptr<Schema> schema_;
  FragmentVector source_fragments_;

  struct {
    std::unordered_map<std::shared_ptr<Scalar>, int, Scalar::Hash, Scalar::PtrsEqual>
        scalar_to_code;

    ScalarVector code_to_scalar;

    int GetOrInsert(const std::shared_ptr<Scalar>& scalar) {
      int new_code = static_cast<int>(code_to_scalar.size());

      auto it_inserted = scalar_to_code.emplace(scalar, new_code);
      if (!it_inserted.second) {
        return it_inserted.first->second;
      }

      code_to_scalar.push_back(scalar);
      return new_code;
    }
  } scalar_dict_;
  std::vector<Indices> right_hand_sides_;
};

Result<WritePlan> DirectoryPartitioningFactory::MakeWritePlan(
    std::shared_ptr<Schema> schema, FragmentIterator fragment_it,
    std::shared_ptr<Schema> partition_schema) {
  ARROW_ASSIGN_OR_RAISE(auto fragments, fragment_it.ToVector());
  return MakeWritePlanImpl(this, schema, std::move(fragments)).Finish(partition_schema);
}

Result<WritePlan> DirectoryPartitioningFactory::MakeWritePlan(
    std::shared_ptr<Schema> schema, FragmentIterator fragment_it) {
  ARROW_ASSIGN_OR_RAISE(auto fragments, fragment_it.ToVector());
  return MakeWritePlanImpl(this, schema, std::move(fragments)).Finish();
}

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

Result<std::string> HivePartitioning::FormatKey(const Key& key, int i) const {
  return key.name + "=" + key.value;
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
