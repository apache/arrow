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

#include <cstdint>
#include <iostream>
#include <memory>
#include <sstream>
#include <utility>

#include "arrow/array.h"
#include "arrow/record_batch.h"
#include "arrow/status.h"
#include "arrow/type.h"

namespace arrow {
namespace ipc {

// ----------------------------------------------------------------------

DictionaryMemo::DictionaryMemo() {}

Status DictionaryMemo::GetDictionaryType(int64_t id,
                                         std::shared_ptr<DataType>* type) const {
  auto it = id_to_type_.find(id);
  if (it == id_to_type_.end()) {
    return Status::KeyError("No record of dictionary type with id ", id);
  }
  *type = it->second;
  return Status::OK();
}

// Returns KeyError if dictionary not found
Status DictionaryMemo::GetDictionary(int64_t id,
                                     std::shared_ptr<Array>* dictionary) const {
  auto it = id_to_dictionary_.find(id);
  if (it == id_to_dictionary_.end()) {
    return Status::KeyError("Dictionary with id ", id, " not found");
  }
  *dictionary = it->second;
  return Status::OK();
}

Status DictionaryMemo::AddFieldInternal(int64_t id, const std::shared_ptr<Field>& field) {
  field_to_id_[field.get()] = id;

  if (field->type()->id() != Type::DICTIONARY) {
    return Status::Invalid("Field type was not DictionaryType",
                           field->type()->ToString());
  }

  std::shared_ptr<DataType> value_type =
      static_cast<const DictionaryType&>(*field->type()).value_type();

  // Add the value type for the dictionary
  auto it = id_to_type_.find(id);
  if (it != id_to_type_.end()) {
    if (!it->second->Equals(*value_type)) {
      return Status::Invalid("Field with dictionary id 0 seen but had type ",
                             it->second->ToString(), "and not ", value_type->ToString());
    }
  } else {
    // Newly-observed dictionary id
    id_to_type_[id] = value_type;
  }
  return Status::OK();
}

Status DictionaryMemo::GetOrAssignId(const std::shared_ptr<Field>& field, int64_t* out) {
  auto it = field_to_id_.find(field.get());
  if (it != field_to_id_.end()) {
    // Field already observed, return the id
    *out = it->second;
  } else {
    int64_t new_id = *out = static_cast<int64_t>(field_to_id_.size());
    RETURN_NOT_OK(AddFieldInternal(new_id, field));
  }
  return Status::OK();
}

Status DictionaryMemo::AddField(int64_t id, const std::shared_ptr<Field>& field) {
  auto it = field_to_id_.find(field.get());
  if (it != field_to_id_.end()) {
    return Status::KeyError("Field is already in memo: ", field->ToString());
  } else {
    RETURN_NOT_OK(AddFieldInternal(id, field));
    return Status::OK();
  }
}

Status DictionaryMemo::GetId(const Field& field, int64_t* id) const {
  auto it = field_to_id_.find(&field);
  if (it != field_to_id_.end()) {
    // Field recorded, return the id
    *id = it->second;
    return Status::OK();
  } else {
    return Status::KeyError("Field with memory address ",
                            reinterpret_cast<int64_t>(&field), " not found");
  }
}

bool DictionaryMemo::HasDictionary(const Field& field) const {
  auto it = field_to_id_.find(&field);
  return it != field_to_id_.end();
}

bool DictionaryMemo::HasDictionary(int64_t id) const {
  auto it = id_to_dictionary_.find(id);
  return it != id_to_dictionary_.end();
}

Status DictionaryMemo::AddDictionary(int64_t id,
                                     const std::shared_ptr<Array>& dictionary) {
  if (HasDictionary(id)) {
    return Status::KeyError("Dictionary with id ", id, " already exists");
  }
  id_to_dictionary_[id] = dictionary;
  return Status::OK();
}

// ----------------------------------------------------------------------
// CollectDictionaries implementation

struct DictionaryCollector {
  DictionaryMemo* dictionary_memo_;

  Status WalkChildren(const DataType& type, const Array& array) {
    for (int i = 0; i < type.num_children(); ++i) {
      auto boxed_child = MakeArray(array.data()->child_data[i]);
      RETURN_NOT_OK(Visit(type.child(i), *boxed_child));
    }
    return Status::OK();
  }

  Status Visit(const std::shared_ptr<Field>& field, const Array& array) {
    auto type = array.type();
    if (type->id() == Type::DICTIONARY) {
      const auto& dict_array = static_cast<const DictionaryArray&>(array);
      auto dictionary = dict_array.dictionary();
      int64_t id = -1;
      RETURN_NOT_OK(dictionary_memo_->GetOrAssignId(field, &id));
      RETURN_NOT_OK(dictionary_memo_->AddDictionary(id, dictionary));

      // Traverse the dictionary to gather any nested dictionaries
      const auto& dict_type = static_cast<const DictionaryType&>(*type);
      RETURN_NOT_OK(WalkChildren(*dict_type.value_type(), *dictionary));
    } else {
      RETURN_NOT_OK(WalkChildren(*type, array));
    }
    return Status::OK();
  }

  Status Collect(const RecordBatch& batch) {
    const Schema& schema = *batch.schema();
    for (int i = 0; i < schema.num_fields(); ++i) {
      RETURN_NOT_OK(Visit(schema.field(i), *batch.column(i)));
    }
    return Status::OK();
  }
};

Status CollectDictionaries(const RecordBatch& batch, DictionaryMemo* memo) {
  DictionaryCollector collector{memo};
  return collector.Collect(batch);
}

}  // namespace ipc
}  // namespace arrow
