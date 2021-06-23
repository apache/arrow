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

#include "arrow/ipc/dictionary.h"

#include <algorithm>
#include <cstdint>
#include <memory>
#include <set>
#include <unordered_map>
#include <utility>
#include <vector>

#include "arrow/array.h"
#include "arrow/array/concatenate.h"
#include "arrow/array/validate.h"
#include "arrow/extension_type.h"
#include "arrow/record_batch.h"
#include "arrow/status.h"
#include "arrow/type.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/logging.h"

namespace std {
template <>
struct hash<arrow::FieldPath> {
  size_t operator()(const arrow::FieldPath& path) const { return path.hash(); }
};
}  // namespace std

namespace arrow {

using internal::checked_cast;

namespace ipc {

using internal::FieldPosition;

// ----------------------------------------------------------------------
// DictionaryFieldMapper implementation

struct DictionaryFieldMapper::Impl {
  using FieldPathMap = std::unordered_map<FieldPath, int64_t>;

  FieldPathMap field_path_to_id;

  void ImportSchema(const Schema& schema) {
    ImportFields(FieldPosition(), schema.fields());
  }

  Status AddSchemaFields(const Schema& schema) {
    if (!field_path_to_id.empty()) {
      return Status::Invalid("Non-empty DictionaryFieldMapper");
    }
    ImportSchema(schema);
    return Status::OK();
  }

  Status AddField(int64_t id, std::vector<int> field_path) {
    const auto pair = field_path_to_id.emplace(FieldPath(std::move(field_path)), id);
    if (!pair.second) {
      return Status::KeyError("Field already mapped to id");
    }
    return Status::OK();
  }

  Result<int64_t> GetFieldId(std::vector<int> field_path) const {
    const auto it = field_path_to_id.find(FieldPath(std::move(field_path)));
    if (it == field_path_to_id.end()) {
      return Status::KeyError("Dictionary field not found");
    }
    return it->second;
  }

  int num_fields() const { return static_cast<int>(field_path_to_id.size()); }

  int num_dicts() const {
    std::set<int64_t> uniqueIds;

    for (auto& kv : field_path_to_id) {
      uniqueIds.insert(kv.second);
    }

    return static_cast<int>(uniqueIds.size());
  }

 private:
  void ImportFields(const FieldPosition& pos,
                    const std::vector<std::shared_ptr<Field>>& fields) {
    for (int i = 0; i < static_cast<int>(fields.size()); ++i) {
      ImportField(pos.child(i), *fields[i]);
    }
  }

  void ImportField(const FieldPosition& pos, const Field& field) {
    const DataType* type = field.type().get();
    if (type->id() == Type::EXTENSION) {
      type = checked_cast<const ExtensionType&>(*type).storage_type().get();
    }
    if (type->id() == Type::DICTIONARY) {
      InsertPath(pos);
      // Import nested dictionaries
      ImportFields(pos,
                   checked_cast<const DictionaryType&>(*type).value_type()->fields());
    } else {
      ImportFields(pos, type->fields());
    }
  }

  void InsertPath(const FieldPosition& pos) {
    const int64_t id = field_path_to_id.size();
    const auto pair = field_path_to_id.emplace(FieldPath(pos.path()), id);
    DCHECK(pair.second);  // was inserted
    ARROW_UNUSED(pair);
  }
};

DictionaryFieldMapper::DictionaryFieldMapper() : impl_(new Impl) {}

DictionaryFieldMapper::DictionaryFieldMapper(const Schema& schema) : impl_(new Impl) {
  impl_->ImportSchema(schema);
}

DictionaryFieldMapper::~DictionaryFieldMapper() {}

Status DictionaryFieldMapper::AddSchemaFields(const Schema& schema) {
  return impl_->AddSchemaFields(schema);
}

Status DictionaryFieldMapper::AddField(int64_t id, std::vector<int> field_path) {
  return impl_->AddField(id, std::move(field_path));
}

Result<int64_t> DictionaryFieldMapper::GetFieldId(std::vector<int> field_path) const {
  return impl_->GetFieldId(std::move(field_path));
}

int DictionaryFieldMapper::num_fields() const { return impl_->num_fields(); }

int DictionaryFieldMapper::num_dicts() const { return impl_->num_dicts(); }

// ----------------------------------------------------------------------
// DictionaryMemo implementation

namespace {

bool HasUnresolvedNestedDict(const ArrayData& data) {
  if (data.type->id() == Type::DICTIONARY) {
    if (data.dictionary == nullptr) {
      return true;
    }
    if (HasUnresolvedNestedDict(*data.dictionary)) {
      return true;
    }
  }
  for (const auto& child : data.child_data) {
    if (HasUnresolvedNestedDict(*child)) {
      return true;
    }
  }
  return false;
}

}  // namespace

struct DictionaryMemo::Impl {
  // Map of dictionary id to dictionary array(s) (several in case of deltas)
  std::unordered_map<int64_t, ArrayDataVector> id_to_dictionary_;
  std::unordered_map<int64_t, std::shared_ptr<DataType>> id_to_type_;
  DictionaryFieldMapper mapper_;

  Result<decltype(id_to_dictionary_)::iterator> FindDictionary(int64_t id) {
    auto it = id_to_dictionary_.find(id);
    if (it == id_to_dictionary_.end()) {
      return Status::KeyError("Dictionary with id ", id, " not found");
    }
    return it;
  }

  Result<std::shared_ptr<ArrayData>> ReifyDictionary(int64_t id, MemoryPool* pool) {
    ARROW_ASSIGN_OR_RAISE(auto it, FindDictionary(id));
    ArrayDataVector* data_vector = &it->second;

    DCHECK(!data_vector->empty());
    if (data_vector->size() > 1) {
      // There are deltas, we need to concatenate them to the first dictionary.
      ArrayVector to_combine;
      to_combine.reserve(data_vector->size());
      // IMPORTANT: At this point, the dictionary data may be untrusted.
      // We need to validate it, as concatenation can crash on invalid or
      // corrupted data.  Full validation is necessary for certain types
      // (for example nested dictionaries).
      for (const auto& data : *data_vector) {
        if (HasUnresolvedNestedDict(*data)) {
          return Status::NotImplemented(
              "Encountered delta dictionary with an unresolved nested dictionary");
        }
        RETURN_NOT_OK(::arrow::internal::ValidateArray(*data));
        RETURN_NOT_OK(::arrow::internal::ValidateArrayFull(*data));
        to_combine.push_back(MakeArray(data));
      }
      ARROW_ASSIGN_OR_RAISE(auto combined_dict, Concatenate(to_combine, pool));
      *data_vector = {combined_dict->data()};
    }

    return data_vector->back();
  }
};

DictionaryMemo::DictionaryMemo() : impl_(new Impl()) {}

DictionaryMemo::~DictionaryMemo() {}

DictionaryFieldMapper& DictionaryMemo::fields() { return impl_->mapper_; }

const DictionaryFieldMapper& DictionaryMemo::fields() const { return impl_->mapper_; }

Result<std::shared_ptr<DataType>> DictionaryMemo::GetDictionaryType(int64_t id) const {
  const auto it = impl_->id_to_type_.find(id);
  if (it == impl_->id_to_type_.end()) {
    return Status::KeyError("No record of dictionary type with id ", id);
  }
  return it->second;
}

// Returns KeyError if dictionary not found
Result<std::shared_ptr<ArrayData>> DictionaryMemo::GetDictionary(int64_t id,
                                                                 MemoryPool* pool) const {
  return impl_->ReifyDictionary(id, pool);
}

Status DictionaryMemo::AddDictionaryType(int64_t id,
                                         const std::shared_ptr<DataType>& type) {
  // AddDictionaryType expects the dict value type
  DCHECK_NE(type->id(), Type::DICTIONARY);
  const auto pair = impl_->id_to_type_.emplace(id, type);
  if (!pair.second && !pair.first->second->Equals(*type)) {
    return Status::KeyError("Conflicting dictionary types for id ", id);
  }
  return Status::OK();
}

bool DictionaryMemo::HasDictionary(int64_t id) const {
  const auto it = impl_->id_to_dictionary_.find(id);
  return it != impl_->id_to_dictionary_.end();
}

Status DictionaryMemo::AddDictionary(int64_t id,
                                     const std::shared_ptr<ArrayData>& dictionary) {
  const auto pair = impl_->id_to_dictionary_.emplace(id, ArrayDataVector{dictionary});
  if (!pair.second) {
    return Status::KeyError("Dictionary with id ", id, " already exists");
  }
  return Status::OK();
}

Status DictionaryMemo::AddDictionaryDelta(int64_t id,
                                          const std::shared_ptr<ArrayData>& dictionary) {
  ARROW_ASSIGN_OR_RAISE(auto it, impl_->FindDictionary(id));
  it->second.push_back(dictionary);
  return Status::OK();
}

Result<bool> DictionaryMemo::AddOrReplaceDictionary(
    int64_t id, const std::shared_ptr<ArrayData>& dictionary) {
  ArrayDataVector value{dictionary};

  auto pair = impl_->id_to_dictionary_.emplace(id, value);
  if (pair.second) {
    // Inserted
    return true;
  } else {
    // Update existing value
    pair.first->second = std::move(value);
    return false;
  }
}

// ----------------------------------------------------------------------
// CollectDictionaries implementation

namespace {

struct DictionaryCollector {
  const DictionaryFieldMapper& mapper_;
  DictionaryVector dictionaries_;

  Status WalkChildren(const FieldPosition& position, const DataType& type,
                      const Array& array) {
    for (int i = 0; i < type.num_fields(); ++i) {
      auto boxed_child = MakeArray(array.data()->child_data[i]);
      RETURN_NOT_OK(Visit(position.child(i), type.field(i), boxed_child.get()));
    }
    return Status::OK();
  }

  Status Visit(const FieldPosition& position, const std::shared_ptr<Field>& field,
               const Array* array) {
    const DataType* type = array->type().get();

    if (type->id() == Type::EXTENSION) {
      type = checked_cast<const ExtensionType&>(*type).storage_type().get();
      array = checked_cast<const ExtensionArray&>(*array).storage().get();
    }
    if (type->id() == Type::DICTIONARY) {
      const auto& dict_array = checked_cast<const DictionaryArray&>(*array);
      auto dictionary = dict_array.dictionary();

      // Traverse the dictionary to first gather any nested dictionaries
      // (so that they appear in the output before their parent)
      const auto& dict_type = checked_cast<const DictionaryType&>(*type);
      RETURN_NOT_OK(WalkChildren(position, *dict_type.value_type(), *dictionary));

      // Then record the dictionary itself
      ARROW_ASSIGN_OR_RAISE(int64_t id, mapper_.GetFieldId(position.path()));
      dictionaries_.emplace_back(id, dictionary);
    } else {
      RETURN_NOT_OK(WalkChildren(position, *type, *array));
    }
    return Status::OK();
  }

  Status Collect(const RecordBatch& batch) {
    FieldPosition position;
    const Schema& schema = *batch.schema();
    dictionaries_.reserve(mapper_.num_fields());

    for (int i = 0; i < schema.num_fields(); ++i) {
      RETURN_NOT_OK(Visit(position.child(i), schema.field(i), batch.column(i).get()));
    }
    return Status::OK();
  }
};

struct DictionaryResolver {
  const DictionaryMemo& memo_;
  MemoryPool* pool_;

  Status VisitChildren(const ArrayDataVector& data_vector, FieldPosition parent_pos) {
    int i = 0;
    for (const auto& data : data_vector) {
      // Some data entries may be missing if reading only a subset of the schema
      if (data != nullptr) {
        RETURN_NOT_OK(VisitField(parent_pos.child(i), data.get()));
      }
      ++i;
    }
    return Status::OK();
  }

  Status VisitField(FieldPosition field_pos, ArrayData* data) {
    const DataType* type = data->type.get();
    if (type->id() == Type::EXTENSION) {
      type = checked_cast<const ExtensionType&>(*type).storage_type().get();
    }
    if (type->id() == Type::DICTIONARY) {
      ARROW_ASSIGN_OR_RAISE(const int64_t id,
                            memo_.fields().GetFieldId(field_pos.path()));
      ARROW_ASSIGN_OR_RAISE(data->dictionary, memo_.GetDictionary(id, pool_));
      // Resolve nested dictionary data
      RETURN_NOT_OK(VisitField(field_pos, data->dictionary.get()));
    }
    // Resolve child data
    return VisitChildren(data->child_data, field_pos);
  }
};

}  // namespace

Result<DictionaryVector> CollectDictionaries(const RecordBatch& batch,
                                             const DictionaryFieldMapper& mapper) {
  DictionaryCollector collector{mapper, {}};
  RETURN_NOT_OK(collector.Collect(batch));
  return std::move(collector.dictionaries_);
}

namespace internal {

Status CollectDictionaries(const RecordBatch& batch, DictionaryMemo* memo) {
  RETURN_NOT_OK(memo->fields().AddSchemaFields(*batch.schema()));
  ARROW_ASSIGN_OR_RAISE(const auto dictionaries,
                        CollectDictionaries(batch, memo->fields()));
  for (const auto& pair : dictionaries) {
    RETURN_NOT_OK(memo->AddDictionary(pair.first, pair.second->data()));
  }
  return Status::OK();
}

}  // namespace internal

Status ResolveDictionaries(const ArrayDataVector& columns, const DictionaryMemo& memo,
                           MemoryPool* pool) {
  DictionaryResolver resolver{memo, pool};
  return resolver.VisitChildren(columns, FieldPosition());
}

}  // namespace ipc
}  // namespace arrow
