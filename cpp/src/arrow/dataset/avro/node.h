/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once


#include <cassert>
#include <memory>

#include "arrow/dataset/avro/exception.h"
#include "arrow/dataset/avro/logical_type.h"
#include "arrow/dataset/avro/schema_resolution.h"
#include "arrow/dataset/avro/types.h"

namespace arrow {
namespace avro {

class GenericDatum;

class Name {
 public:
  Name() {}
  Name(const std::string& fullname);
  Name(const std::string& simpleName, const std::string& ns)
      : ns_(ns), simpleName_(simpleName) {
    check();
  }

  const std::string FullName() const;
  const std::string& ns() const { return ns_; }
  const std::string& simple_name() const { return simpleName_; }

  void set_ns(const std::string& n) { ns_ = n; }
  void set_simple_name(const std::string& n) { simpleName_ = n; }
  void SetFullName(const std::string& n);

  bool operator<(const Name& n) const;
  void check() const;
  bool operator==(const Name& n) const;
  bool operator!=(const Name& n) const { return !((*this) == n); }
  void clear() {
    ns_.clear();
    simpleName_.clear();
  }
  operator std::string() const { return fullname(); }
 private:
  std::string ns_;
  std::string simpleName_;


};

inline std::ostream& operator<<(std::ostream& os, const Name& n) {
  return os << n.fullname();
}

/// Node is the building block for parse trees.  Each node represents an avro
/// type.  Compound types have leaf nodes that represent the types they are
/// composed of.
///
/// The user does not use the Node object directly, they interface with Schema
/// objects.
///
/// The Node object uses reference-counted pointers.  This is so that schemas
/// may be reused in other schemas, without needing to worry about memory
/// deallocation for nodes that are added to multiple schema parse trees.
///
/// Node has minimal implementation, serving as an abstract base class for
/// different node types.
///

class Node {
 ARROW_DISALLOW_COPY_AND_ASSIGN(Node);
 public:
  explicit Node(Type type) : type_(type), logicalType_(LogicalType::NONE), locked_(false) {}

  virtual ~Node();

  Type type() const { return type_; }

  LogicalType logical_type() const { return logicalType_; }

  void set_logical_type(LogicalType logicalType);

  void lock() { locked_ = true; }

  bool locked() const { return locked_; }

  virtual bool HasName() const = 0;

  Status set_name(const Name& name) {
    ARROW_RETURN_NOT_OK(CheckLock());
    ARROW_RETURN_NOT_OK(CheckName(name));
    DoSetName(name);
    return Status::OK();
  }
  virtual const Name& name() const = 0;

  virtual const std::string& doc() const = 0;
  Status set_doc(const std::string& doc) {
    ARROW_RETURN_NOT_OK(CheckLock());
    DoSetDoc(doc);
    return Status::OK();
  }

  Status AddLeaf(const NodePtr& newLeaf) {
    ARROW_RETURN_NOT_OK(CheckLock());
    DoAddLeaf(newLeaf);
    return Status::OK();
  }
  virtual size_t leaves() const = 0;
  virtual const NodePtr& LeafAt(int index) const = 0;
  virtual Result<const GenericDatum&> DefaultValueAt(int index) {
    return Status::KeyError("No default value at: ", index);
  }

  void AddName(const std::string& name) {
    ARROW_RETURN_NOT_OK(CheckLock());
    ARROW_RETURN_NOT_OK(CheckName(name));
    DoAddName(name);
    return Status::OK();
  }
  virtual size_t Names() const = 0;
  virtual const Result<std::string&> NameAt(int index) const = 0;
  virtual bool NameIndex(const std::string& name, size_t& index) const = 0;

  void SetFixedSize(int size) {
    checkLock();
    doSetFixedSize(size);
  }
  virtual int fixedSize() const = 0;

  virtual bool isValid() const = 0;

  virtual SchemaResolution resolve(const Node& reader) const = 0;

  virtual void PrintJson(std::ostream& os, int depth) const = 0;

  virtual void PrintBasicInfo(std::ostream& os) const = 0;

  virtual void SetLeafToSymbolic(int index, const NodePtr& node) = 0;

  // Serialize the default value GenericDatum g for the node contained
  // in a record node.
  virtual void PrintDefaultToJson(const GenericDatum& g, std::ostream& os,
                                  int depth) const = 0;

 protected:
  Status CheckLock() const {
    if (ARROW_PREDICT_FALSE(locked())) {
      return Status::Unknown("Cannot modify locked schema");
    }
  }

  virtual Status CheckName(const Name& name) const { return name.Check(); }

  virtual void DoSetName(const Name& name) = 0;
  virtual void DoSetDoc(const std::string& name) = 0;

  virtual void DoAddLeaf(const NodePtr& newLeaf) = 0;
  virtual void DoAddName(const std::string& name) = 0;
  virtual void DoSetFixedSize(int size) = 0;

 private:
  const Type type_;
  LogicalType logicalType_;
  bool locked_;
};

}  // namespace avro
}

namespace std {
inline std::ostream& operator<<(std::ostream& os, const avro::Node& n) {
  n.PrintJson(os, 0);
  return os;
}
}  // namespace std

#endif
