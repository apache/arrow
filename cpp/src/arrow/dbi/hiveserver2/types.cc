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

#include "arrow/dbi/hiveserver2/types.h"

#include "arrow/dbi/hiveserver2/thrift_internal.h"

#include "arrow/util/logging.h"

namespace arrow {
namespace hiveserver2 {

const PrimitiveType* ColumnDesc::GetPrimitiveType() const {
  return static_cast<PrimitiveType*>(type_.get());
}

const CharacterType* ColumnDesc::GetCharacterType() const {
  DCHECK(type_->type_id() == ColumnType::TypeId::CHAR ||
         type_->type_id() == ColumnType::TypeId::VARCHAR);
  return static_cast<CharacterType*>(type_.get());
}

const DecimalType* ColumnDesc::GetDecimalType() const {
  DCHECK(type_->type_id() == ColumnType::TypeId::DECIMAL);
  return static_cast<DecimalType*>(type_.get());
}

std::string PrimitiveType::ToString() const { return TypeIdToString(type_id_); }

}  // namespace hiveserver2
}  // namespace arrow
