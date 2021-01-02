/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#pragma once

#include <arrow/api.h>

#include <arrow-glib/basic-data-type.h>

GArrowDataType *
garrow_data_type_new_raw(std::shared_ptr<arrow::DataType> *arrow_data_type);
std::shared_ptr<arrow::DataType>
garrow_data_type_get_raw(GArrowDataType *data_type);

GArrowExtensionDataTypeRegistry *
garrow_extension_data_type_registry_new_raw(
  std::shared_ptr<arrow::ExtensionTypeRegistry> *arrow_registry);
std::shared_ptr<arrow::ExtensionTypeRegistry>
garrow_extension_data_type_registry_get_raw(
  GArrowExtensionDataTypeRegistry *registry);

namespace garrow {
  class GExtensionType : public arrow::ExtensionType {
  public:
    explicit GExtensionType(GArrowExtensionDataType *garrow_data_type);
    ~GExtensionType();

    GArrowExtensionDataType *
    garrow_data_type() const;

    GType
    array_gtype() const;

    std::string extension_name() const override;

    bool ExtensionEquals(const arrow::ExtensionType& other) const override;

    std::shared_ptr<arrow::Array>
    MakeArray(std::shared_ptr<arrow::ArrayData> data) const override;

    arrow::Result<std::shared_ptr<arrow::DataType>>
    Deserialize(std::shared_ptr<arrow::DataType> storage_data_type,
                const std::string& serialized_data) const override;

    std::string
    Serialize() const override;

  private:
    GArrowExtensionDataType *garrow_data_type_;
  };
}
