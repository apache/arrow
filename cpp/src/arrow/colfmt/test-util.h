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

#include "arrow/api.h"
#include "arrow/testing/gtest_util.h"
#include <random>
#include <limits>
#include <string>
#include <vector>
#include <memory>

namespace arrow {
namespace colfmt {

class Dataset {
 public:
  Dataset(const std::shared_ptr<arrow::Field>& schema,
          const std::shared_ptr<arrow::Array>& array)
      : schema_(schema),
        array_(array) {}

  std::shared_ptr<arrow::Field> schema() { return schema_; }
  std::shared_ptr<arrow::Array> array() { return array_; }

 protected:
  std::shared_ptr<arrow::Field> schema_;
  std::shared_ptr<arrow::Array> array_;
};

class DatasetGenerator {
 public:
  DatasetGenerator(arrow::MemoryPool* pool, uint32_t seed)
      : pool_(pool),
        rng_(seed),
        pr_roll_(0.0, 1.0) {}

  /// Generates random data with the following shape:
  /// optional struct "s0" {
  ///   optional struct "s1" {
  ///     optional struct "s2" {
  ///       optional uint64 "f0";
  ///       optional uint64 "f1";
  ///       optional uint64 "f2";
  ///     }
  ///   }
  /// }
  ///
  /// \param num_docs number of documents to generate
  /// \param num_structs number of nested structs (depth)
  /// \param num_fields number of u64 fields at the lowest level
  /// \param pr_struct_null probability that at least one of the structs is null
  /// \param pr_field_null probability that any given u64 field is null
  Dataset GenerateNestedStruct(int num_docs, int num_structs, int num_fields,
                               double pr_struct_null, double pr_field_null);

  /// Generates random data with the following shape:
  /// optional list "list0" {
  ///   optional list "list1" {
  ///     optional list "list2" {
  ///       optional uint64 "item";
  ///     }
  ///   }
  /// }
  ///
  /// \param num_docs number of documents to generate
  /// \param num_levels number of nested lists (depth)
  /// \param num_nodes number of nodes (lists + items) per document
  /// \param pr_null probability that any given node (list or item) is null
  Dataset GenerateNestedList(int num_docs, int num_levels, int num_nodes,
                             double pr_null);

 private:
  struct Node {
    int parent{-1};
    bool null{false};
    int level{0};
    int first_child{-1};
    int last_child{-1};
    int right_sibling{-1};
  };

  void GenerateRandomTree(int num_nodes, int num_levels, double pr_null,
                          std::vector<Node>* nodes);

  /// Convert tree to format suitable for drawing with dot(1).
  void ConvertTreeToDotFormat(const std::vector<Node>& nodes,
                              std::ostream* out);

  int NextPreorder(const std::vector<Node>& nodes, int index);

  arrow::MemoryPool* pool_;
  std::default_random_engine rng_;
  std::uniform_real_distribution<double> pr_roll_;
  std::uniform_int_distribution<uint64_t> u64_roll_;
};

Dataset DatasetGenerator::GenerateNestedStruct(int num_docs, int num_structs,
                                               int num_fields, double pr_struct_null,
                                               double pr_field_null) {
  std::vector<std::shared_ptr<arrow::StructBuilder>> struct_builders;
  std::vector<std::shared_ptr<arrow::UInt64Builder>> field_builders;

  // instantiate builders
  for (int i = 0; i < num_fields; i++) {
    field_builders.push_back(std::make_shared<arrow::UInt64Builder>(pool_));
  }
  for (int i = 0; i < num_structs; i++) {
    std::vector<std::shared_ptr<arrow::Field>> fields;
    std::vector<std::shared_ptr<arrow::ArrayBuilder>> builders;
    if (struct_builders.empty()) {
      for (int j = 0; j < num_fields; j++) {
        std::string name = "f" + std::to_string(j);
        fields.push_back(arrow::field(name, field_builders[j]->type(), true));
        builders.push_back(field_builders[j]);
      }
    } else {
      std::string name = "s" + std::to_string(num_structs-1-struct_builders.size());
      fields.push_back(arrow::field(name, struct_builders[0]->type(), true));
      builders.push_back(struct_builders[0]);
    }
    struct_builders.insert(
        struct_builders.begin(),
        std::make_shared<arrow::StructBuilder>(arrow::struct_(fields), pool_, builders));
  }
  std::shared_ptr<arrow::Field> schema =
      arrow::field("s0", struct_builders[0]->type(), true);

  // generate random documents
  for (int i = 0; i < num_docs; i++) {
    int null_index = std::numeric_limits<int>::max();
    if (pr_roll_(rng_) < pr_struct_null) {
      null_index = u64_roll_(rng_) % num_structs;
    }
    for (int j = 0; j < num_structs; j++) {
      if (j >= null_index) {
        ABORT_NOT_OK(struct_builders[j]->AppendNull());
      } else {
        ABORT_NOT_OK(struct_builders[j]->Append());
      }
    }
    for (int j = 0; j < num_fields; j++) {
      if (null_index < num_structs || pr_roll_(rng_) < pr_field_null) {
        ABORT_NOT_OK(field_builders[j]->AppendNull());
      } else {
        ABORT_NOT_OK(field_builders[j]->Append(u64_roll_(rng_)));
      }
    }
  }
  std::shared_ptr<arrow::Array> array;
  ABORT_NOT_OK(struct_builders[0]->Finish(&array));

  return Dataset(schema, array);
}

void DatasetGenerator::ConvertTreeToDotFormat(
    const std::vector<DatasetGenerator::Node>& nodes,
    std::ostream* out) {
  *out << "digraph tree{" << std::endl;
  for (int i = 0; i < (int)nodes.size(); i++) {
    *out << "  " << i;
    if (nodes[i].null) {
      *out << " [label=\"null\"]";
    }
    *out << ";" << std::endl;
  }
  for (int i = 0; i < (int)nodes.size(); i++) {
    if (nodes[i].parent != -1) {
      *out << nodes[i].parent << " -> " << i << ";" << std::endl;
    }
  }
  *out << "}" << std::endl;
}

void DatasetGenerator::GenerateRandomTree(int num_nodes, int num_levels, double pr_null,
                                          std::vector<DatasetGenerator::Node>* nodes) {
  std::vector<int> allowed_parents;

  nodes->push_back(Node());
  allowed_parents.push_back(0);

  while ((int)nodes->size() < num_nodes) {
    int p = 0;
    if (allowed_parents.size() > 1) {
      p = allowed_parents[u64_roll_(rng_) % (allowed_parents.size() - 1)];
    }

    Node node;
    node.parent = p;
    node.null = pr_roll_(rng_) < pr_null;
    node.level = (*nodes)[p].level + 1;
    node.first_child = -1;
    node.last_child = -1;
    node.right_sibling = -1;
    nodes->push_back(node);

    if (!node.null && node.level + 1 < num_levels) {
      allowed_parents.push_back(nodes->size() - 1);
    }

    if ((*nodes)[p].first_child == -1) {
      (*nodes)[p].first_child = nodes->size() - 1;
      (*nodes)[p].last_child = nodes->size() - 1;
    } else {
      (*nodes)[(*nodes)[p].last_child].right_sibling = nodes->size() - 1;
      (*nodes)[p].last_child = nodes->size() - 1;
    }
  }
}

int DatasetGenerator::NextPreorder(const std::vector<DatasetGenerator::Node>& nodes,
                                   int index) {
  if (nodes[index].first_child != -1) {
    return nodes[index].first_child;
  } else {
    while (index != -1 && nodes[index].right_sibling == -1) {
      index = nodes[index].parent;
    }
    if (index != -1) {
      return nodes[index].right_sibling;
    }
  }
  return -1;
}

Dataset DatasetGenerator::GenerateNestedList(int num_docs, int num_levels, int num_nodes,
                                             double pr_null) {
  std::shared_ptr<arrow::UInt64Builder> item_builder =
    std::make_shared<UInt64Builder>(pool_);
  std::vector<std::shared_ptr<arrow::ListBuilder>> list_builders;

  // instantiate builders
  for (int i = 0; i < num_levels; i++) {
    std::shared_ptr<arrow::Field> field;
    std::shared_ptr<arrow::ArrayBuilder> builder;
    if (list_builders.empty()) {
      field = arrow::field("item", item_builder->type(), true);
      builder = item_builder;
    } else {
      std::string name = "list" + std::to_string(list_builders.size()-1-i);
      field = arrow::field(name, list_builders[0]->type(), true);
      builder = list_builders[0];
    }
    list_builders.insert(
        list_builders.begin(),
        std::make_shared<arrow::ListBuilder>(pool_, builder, arrow::list(field)));
  }
  std::shared_ptr<arrow::Field> schema =
    arrow::field("list0", list_builders[0]->type(), true);

  for (int d = 0; d < num_docs; d++) {
    std::vector<Node> nodes;
    // one more level is for items
    GenerateRandomTree(num_nodes, num_levels+1, pr_null, &nodes);

    int i = 0;
    while (i != -1) {
      if (nodes[i].level == num_levels) {
        // lowest-level item
        if (nodes[i].null) {
          item_builder->AppendNull();
        } else {
          item_builder->Append(u64_roll_(rng_));
        }
      } else {
        // list
        if (nodes[i].null) {
          list_builders[nodes[i].level]->AppendNull();
        } else {
          list_builders[nodes[i].level]->Append();
        }
      }
      i = NextPreorder(nodes, i);
    }
  }
  std::shared_ptr<arrow::Array> array;
  ABORT_NOT_OK(list_builders[0]->Finish(&array));
  return Dataset(schema, array);
}

}  // namespace colfmt
}  // namespace arrow
