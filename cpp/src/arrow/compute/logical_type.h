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

// Metadata objects for creating well-typed expressions. These are distinct
// from (and higher level than) arrow::DataType as some type parameters (like
// decimal scale and precision) may not be known at expression build time, and
// these are resolved later on evaluation
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

#pragma once

#include <string>

#include "arrow/util/visibility.h"

namespace arrow {

class Status;

namespace compute {

class TypeClass {
 public:
  enum Id {
    ANY,
    NUMERIC,
    INTEGER,
    FLOATING,
    NULL_,
    BOOL,
    UINT8,
    INT8,
    UINT16,
    INT16,
    UINT32,
    INT32,
    UINT64,
    INT64,
    FLOAT,
    DOUBLE,
    BINARY,
    UTF8,
    DATE,
    TIME,
    TIMESTAMP,
    DECIMAL,
    LIST,
    STRUCT
  };

  Id id() const { return id_; }

  virtual std::string ToString() const = 0;

  /// \brief Check if expression is an instance of this type class
  virtual bool IsInstance(const Expr& expr) const = 0;

 protected:
  explicit LogicalType(Id id) : id_(id) {}
  Id id_;
};

namespace type {

class Any : public TypeClass {
 public:
  Any() : LogicalType(LogicalType::ANY) {}

  bool IsInstance(const Expr& expr) const override;
}

class Null : public TypeClass {
 public:
  Null() : LogicalType(LogicalType::NULL_) {}

  bool IsInstance(const Expr& expr) const override;
}

class Integer : public TypeClass {
 public:
  Integer() : LogicalType(LogicalType::INTEGER) {}

  bool IsInstance(const Expr& expr) const override;
}

};
