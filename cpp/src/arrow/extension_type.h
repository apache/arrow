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

/// User-defined extension types. EXPERIMENTAL in 0.13.0
/// \since 0.13.0

#pragma once

#include <memory>
#include <string>

#include "arrow/array.h"
#include "arrow/type.h"
#include "arrow/util/visibility.h"

namespace arrow {

class TypeVisitor;

class ExtensionTypeTraits {
 public:
  virtual ~ExtensionTypeTraits() = default;

  /// \brief Name used for serialization, and lookups for deeserialization
  virtual std::string extension_name() const = 0;

  /// \brief Determine if two instances of the same extension types are
  /// equal. Invoked from ExtensionType::Equals
  /// \param[in] other the type to compare this type with
  /// \return bool true if type instances are equal
  virtual bool ExtensionEquals(const ExtensionType& other) const = 0;
};

/// \brief The base class for custom / user-defined types.
class ARROW_EXPORT ExtensionType : public DataType, public ExtensionTypeTraits {
 public:
  static constexpr Type::type type_id = Type::EXTENSION;

  std::shared_ptr<DataType> storage_type() const { return storage_type_; }

  std::string ToString() const override;
  std::string name() const override;

 protected:
  explicit ExtensionType(std::shared_ptr<DataType> storage_type)
      : DataType(Type::EXTENSION), storage_type_(storage_type) {}

  std::shared_ptr<DataType> storage_type_;
};

class ARROW_EXPORT ExtensionArray : public Array {
 public:
  /// \brief The physical storage for the extension array
  std::shared_ptr<Array> storage() const { return storage_; }

 protected:
  explicit ExtensionArray(const std::shared_ptr<ArrayData>& data) { SetData(data); }

  void SetData(const std::shared_ptr<ArrayData>& data);

  std::shared_ptr<Array> storage_;
};

/// \brief Serializer interface for user-defined types
class ExtensionTypeAdapter {
 public:
  /// \brief Wrap built-in Array type in a user-defined ExtensionArray instance
  /// \param[in] data the physical storage for the extension type
  virtual std::shared_ptr<Array> WrapArray(std::shared_ptr<ArrayData> data) = 0;

  virtual Status Deserialize(std::shared_ptr<DataType> storage_type,
                             const std::string& serialized_data,
                             std::shared_ptr<DataType>* out) = 0;

  virtual std::string Serialize(const ExtensionType& type) = 0;
};

/// \brief
ARROW_EXPORT
Status RegisterExtensionType(const std::string& type_name,
                             std::unique_ptr<ExtensionTypeAdapter> wrapper);

ARROW_EXPORT
Status UnregisterExtensionType(const std::string& type_name);

ARROW_EXPORT
ExtensionTypeAdapter* GetExtensionType(const std::string& type_name);

}  // namespace arrow
