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

#include "arrow/python/dict.h"

#include <vector>

namespace arrow {
namespace py {

Status DictBuilder::Finish(std::shared_ptr<Array> key_tuple_data,
                           std::shared_ptr<Array> key_dict_data,
                           std::shared_ptr<Array> val_list_data,
                           std::shared_ptr<Array> val_tuple_data,
                           std::shared_ptr<Array> val_dict_data,
                           std::shared_ptr<arrow::Array>* out) {
  // lists and dicts can't be keys of dicts in Python, that is why for
  // the keys we do not need to collect sublists
  std::shared_ptr<Array> keys, vals;
  RETURN_NOT_OK(keys_.Finish(nullptr, key_tuple_data, key_dict_data, &keys));
  RETURN_NOT_OK(vals_.Finish(val_list_data, val_tuple_data, val_dict_data, &vals));
  auto keys_field = std::make_shared<Field>("keys", keys->type());
  auto vals_field = std::make_shared<Field>("vals", vals->type());
  auto type = std::make_shared<StructType>(
      std::vector<std::shared_ptr<Field>>({keys_field, vals_field}));
  std::vector<std::shared_ptr<Array>> field_arrays({keys, vals});
  DCHECK(keys->length() == vals->length());
  out->reset(new StructArray(type, keys->length(), field_arrays));
  return Status::OK();
}

}  // namespace py
}  // namespace arrow
