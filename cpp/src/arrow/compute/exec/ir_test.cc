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

#include "arrow/compute/exec/ir_consumer.h"

#include <fstream>

#include <gflags/gflags.h>

#include "arrow/compute/exec/options.h"
#include "arrow/compute/exec/test_util.h"
#include "arrow/io/file.h"
#include "arrow/testing/matchers.h"
#include "arrow/util/io_util.h"
#include "arrow/util/string_view.h"

#include "generated/Plan_generated.h"

using testing::ElementsAre;
using testing::ElementsAreArray;
using testing::Eq;
using testing::HasSubstr;
using testing::Optional;
using testing::UnorderedElementsAreArray;

namespace ir = org::apache::arrow::computeir::flatbuf;
namespace flatbuf = org::apache::arrow::flatbuf;

DEFINE_string(computeir_dir, "",
              "Directory containing Flatbuffer schemas for Arrow compute IR.\n"
              "This is currently $ARROW_REPO/experimental/computeir/");

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  if (std::system("flatc --version") != 0) {
    std::cout << "flatc not available, skipping tests" << std::endl;
    return 0;
  }

  int ret = RUN_ALL_TESTS();
  gflags::ShutDownCommandLineFlags();
  return ret;
}

namespace arrow {
namespace compute {

std::shared_ptr<Buffer> FlatbufferFromJSON(std::string root_type,
                                           util::string_view json) {
  static std::unique_ptr<arrow::internal::TemporaryDir> dir;

  if (!dir) {
    if (FLAGS_computeir_dir == "") {
      std::cout << "Required argument -computeir_dir was not provided!" << std::endl;
      std::abort();
    }

    dir = *arrow::internal::TemporaryDir::Make("ir_json_");
  }

  auto json_path = dir->path().ToString() + "ir.json";
  std::ofstream{json_path} << json;

  std::string cmd = "flatc --binary " + FLAGS_computeir_dir + "/Plan.fbs" +
                    " --root-type org.apache.arrow.computeir.flatbuf." + root_type + " " +
                    json_path;

  if (int err = std::system(cmd.c_str())) {
    std::cerr << cmd << " failed with error code: " << err;
    std::abort();
  }

  auto bin = *io::MemoryMappedFile::Open("ir.bin", io::FileMode::READ);
  return *bin->Read(*bin->GetSize());
}

template <typename Ir>
auto ConvertJSON(util::string_view json) -> decltype(Convert(std::declval<Ir>())) {
  std::string root_type;
  if (std::is_same<Ir, ir::Literal>::value) {
    root_type = "Literal";
  } else if (std::is_same<Ir, ir::Expression>::value) {
    root_type = "Expression";
  } else if (std::is_same<Ir, ir::Relation>::value) {
    root_type = "Relation";
  } else if (std::is_same<Ir, ir::Plan>::value) {
    root_type = "Plan";
  } else {
    std::cout << "Unknown Ir class in.";
    std::abort();
  }

  auto buf = FlatbufferFromJSON(root_type, json);
  return ConvertRoot<Ir>(*buf);
}

TEST(Literal, Int64) {
  ASSERT_THAT(ConvertJSON<ir::Literal>(R"({
    type: {
      type_type: "Int",
      type: { bitWidth: 64, is_signed: true }
    }
  })"),
              ResultWith(DataEq(std::make_shared<Int64Scalar>())));

  ASSERT_THAT(ConvertJSON<ir::Literal>(R"({
    type: {
      type_type: "Int",
      type: { bitWidth: 64, is_signed: true }
    },
    impl_type: "Int64Literal",
    impl: { value: 42 }
  })"),
              ResultWith(DataEq<int64_t>(42)));
}

TEST(Expression, Comparison) {
  ASSERT_THAT(ConvertJSON<ir::Expression>(R"({
    impl_type: "Call",
    impl: {
      name: "equal",
      arguments: [
        {
          impl_type: "FieldRef",
          impl: {
            ref_type: "FieldIndex",
            ref: {
              position: 2
            }
          }
        },
        {
          impl_type: "Literal",
          impl: {
            type: {
              type_type: "Int",
              type: { bitWidth: 64, is_signed: true }
            },
            impl_type: "Int64Literal",
            impl: { value: 42 }
          }
        }
      ]
    }
  })"),
              ResultWith(Eq(equal(field_ref(2), literal<int64_t>(42)))));
}

TEST(Relation, Filter) {
  ASSERT_THAT(
      ConvertJSON<ir::Relation>(R"({
    impl_type: "Filter",
    impl: {
      id: { id: 1 },
      rel: {
        impl_type: "Source",
        impl: {
          id: { id: 0 },
          name: "test source",
          schema: {
            endianness: "Little",
            fields: [
              {
                name: "i32",
                type_type: "Int",
                type: {
                  bitWidth: 32,
                  is_signed: true
                },
                nullable: true
              },
              {
                name: "f64",
                type_type: "FloatingPoint",
                type: {
                  precision: "DOUBLE"
                },
                nullable: true
              },
              {
                name: "i64",
                type_type: "Int",
                type: {
                  bitWidth: 64,
                  is_signed: true
                },
                nullable: true
              }
            ]
          }
        }
      },
      predicate: {
        impl_type: "Call",
        impl: {
          name: "equal",
          arguments: [
            {
              impl_type: "FieldRef",
              impl: {
                ref_type: "FieldIndex",
                ref: {
                  position: 2
                }
              }
            },
            {
              impl_type: "Literal",
              impl: {
                type: {
                  type_type: "Int",
                  type: { bitWidth: 64, is_signed: true }
                },
                impl_type: "Int64Literal",
                impl: { value: 42 }
              }
            }
          ]
        }
      }
    }
  })"),
      ResultWith(Eq(Declaration::Sequence({
          {"catalog_source",
           CatalogSourceNodeOptions{"test source", schema({
                                                       field("i32", int32()),
                                                       field("f64", float64()),
                                                       field("i64", int64()),
                                                   })},
           "0"},
          {"filter", FilterNodeOptions{equal(field_ref(2), literal<int64_t>(42))}, "1"},
      }))));
}

TEST(Relation, AggregateSimple) {
  ASSERT_THAT(ConvertJSON<ir::Relation>(R"({
            "impl": {
                id: {id: 1},
                "groupings": [
                    {
                        "keys": [
                            {
                                "impl": {
                                    "ref": {
                                        "position": 0
                                    },
                                    "ref_type": "FieldIndex",
                                    "relation_index": 0
                                },
                                "impl_type": "FieldRef"
                            }
                        ]
                    }
                ],
                "measures": [
                    {
                        "impl": {
                            "arguments": [
                                {
                                    "impl": {
                                        "ref": {
                                            "position": 1
                                        },
                                        "ref_type": "FieldIndex",
                                        "relation_index": 0
                                    },
                                    "impl_type": "FieldRef"
                                }
                            ],
                            "name": "sum"
                        },
                        "impl_type": "Call"
                    },
                    {
                        "impl": {
                            "arguments": [
                                {
                                    "impl": {
                                        "ref": {
                                            "position": 2
                                        },
                                        "ref_type": "FieldIndex",
                                        "relation_index": 0
                                    },
                                    "impl_type": "FieldRef"
                                }
                            ],
                            "name": "mean"
                        },
                        "impl_type": "Call"
                    }
                ],
                "rel": {
                    "impl": {
                        id: {id: 0},
                        "name": "tbl",
                        "schema": {
                            "endianness": "Little",
                            "fields": [
                                {
                                    "name": "foo",
                                    "nullable": true,
                                    "type": {
                                        "bitWidth": 32,
                                        "is_signed": true
                                    },
                                    "type_type": "Int"
                                },
                                {
                                    "name": "bar",
                                    "nullable": true,
                                    "type": {
                                        "bitWidth": 64,
                                        "is_signed": true
                                    },
                                    "type_type": "Int"
                                },
                                {
                                    "name": "baz",
                                    "nullable": true,
                                    "type": {
                                        "precision": "DOUBLE"
                                    },
                                    "type_type": "FloatingPoint"
                                }
                            ]
                        }
                    },
                    "impl_type": "Source"
                }
            },
            "impl_type": "Aggregate"
})"),
              ResultWith(Eq(Declaration::Sequence({
                  {"catalog_source",
                   CatalogSourceNodeOptions{"tbl", schema({
                                                       field("foo", int32()),
                                                       field("bar", int64()),
                                                       field("baz", float64()),
                                                   })},
                   "0"},
                  {"aggregate",
                   AggregateNodeOptions{/*aggregates=*/{
                                            {"sum", nullptr},
                                            {"mean", nullptr},
                                        },
                                        /*targets=*/{1, 2},
                                        /*names=*/
                                        {
                                            "sum FieldRef.FieldPath(1)",
                                            "mean FieldRef.FieldPath(2)",
                                        },
                                        /*keys=*/{0}},
                   "1"},
              }))));
}

TEST(Relation, AggregateWithHaving) {
  ASSERT_THAT(
      ConvertJSON<ir::Relation>(R"({
            "impl": {
                id: {id: 3},
                "predicate": {
                    "impl": {
                        "arguments": [
                            {
                                            "impl": {
                                                "ref": {
                                                    "position": 0
                                                },
                                                "ref_type": "FieldIndex",
                                                "relation_index": 0
                                            },
                                            "impl_type": "FieldRef"
                            },
                            {
                                "impl": {
                                    "impl": {
                                        "value": 10
                                    },
                                    "impl_type": "Int8Literal",
                                    "type": {
                                        "nullable": true,
                                        "type": {
                                            "bitWidth": 8,
                                            "is_signed": true
                                        },
                                        "type_type": "Int"
                                    }
                                },
                                "impl_type": "Literal"
                            }
                        ],
                        "name": "greater"
                    },
                    "impl_type": "Call"
                },
                "rel": {
                    "impl": {
                        id: {id: 2},
                        "groupings": [
                            {
                                "keys": [
                                    {
                                        "impl": {
                                            "ref": {
                                                "position": 0
                                            },
                                            "ref_type": "FieldIndex",
                                            "relation_index": 0
                                        },
                                        "impl_type": "FieldRef"
                                    }
                                ]
                            }
                        ],
                        "measures": [
                            {
                                "impl": {
                                    "arguments": [
                                        {
                                            "impl": {
                                                "ref": {
                                                    "position": 1
                                                },
                                                "ref_type": "FieldIndex",
                                                "relation_index": 0
                                            },
                                            "impl_type": "FieldRef"
                                        }
                                    ],
                                    "name": "sum"
                                },
                                "impl_type": "Call"
                            },
                            {
                                "impl": {
                                    "arguments": [
                                        {
                                            "impl": {
                                                "ref": {
                                                    "position": 2
                                                },
                                                "ref_type": "FieldIndex",
                                                "relation_index": 0
                                            },
                                            "impl_type": "FieldRef"
                                        }
                                    ],
                                    "name": "mean"
                                },
                                "impl_type": "Call"
                            }
                        ],
                        "rel": {
                            "impl": {
                                id: {id: 1},
                                "predicate": {
                                    "impl": {
                                        "arguments": [
                                            {
                                                "impl": {
                                                    "ref": {
                                                        "position": 0
                                                    },
                                                    "ref_type": "FieldIndex",
                                                    "relation_index": 0
                                                },
                                                "impl_type": "FieldRef"
                                            },
                                            {
                                                "impl": {
                                                    "impl": {
                                                        "value": 3
                                                    },
                                                    "impl_type": "Int8Literal",
                                                    "type": {
                                                        "nullable": true,
                                                        "type": {
                                                            "bitWidth": 8,
                                                            "is_signed": true
                                                        },
                                                        "type_type": "Int"
                                                    }
                                                },
                                                "impl_type": "Literal"
                                            }
                                        ],
                                        "name": "less"
                                    },
                                    "impl_type": "Call"
                                },
                                "rel": {
                                    "impl": {
                                        id: {id: 0},
                                        "name": "tbl",
                                        "schema": {
                                            "endianness": "Little",
                                            "fields": [
                                                {
                                                    "name": "foo",
                                                    "nullable": true,
                                                    "type": {
                                                        "bitWidth": 32,
                                                        "is_signed": true
                                                    },
                                                    "type_type": "Int"
                                                },
                                                {
                                                    "name": "bar",
                                                    "nullable": true,
                                                    "type": {
                                                        "bitWidth": 64,
                                                        "is_signed": true
                                                    },
                                                    "type_type": "Int"
                                                },
                                                {
                                                    "name": "baz",
                                                    "nullable": true,
                                                    "type": {
                                                        "precision": "DOUBLE"
                                                    },
                                                    "type_type": "FloatingPoint"
                                                }
                                            ]
                                        }
                                    },
                                    "impl_type": "Source"
                                }
                            },
                            "impl_type": "Filter"
                        }
                    },
                    "impl_type": "Aggregate"
                }
            },
            "impl_type": "Filter"
})"),
      ResultWith(Eq(Declaration::Sequence({
          {"catalog_source",
           CatalogSourceNodeOptions{"tbl", schema({
                                               field("foo", int32()),
                                               field("bar", int64()),
                                               field("baz", float64()),
                                           })},
           "0"},
          {"filter", FilterNodeOptions{less(field_ref(0), literal<int8_t>(3))}, "1"},
          {"aggregate",
           AggregateNodeOptions{/*aggregates=*/{
                                    {"sum", nullptr},
                                    {"mean", nullptr},
                                },
                                /*targets=*/{1, 2},
                                /*names=*/
                                {
                                    "sum FieldRef.FieldPath(1)",
                                    "mean FieldRef.FieldPath(2)",
                                },
                                /*keys=*/{0}},
           "2"},
          {"filter", FilterNodeOptions{greater(field_ref(0), literal<int8_t>(10))}, "3"},
      }))));
}

TEST(Relation, ProjectionWithFilter) {
  ASSERT_THAT(
      ConvertJSON<ir::Relation>(R"({
            "impl": {
                id: {id:2},
                "predicate": {
                    "impl": {
                        "arguments": [
                            {
                                "impl": {
                                    "ref": {
                                        "position": 0
                                    },
                                    "ref_type": "FieldIndex",
                                    "relation_index": 0
                                },
                                "impl_type": "FieldRef"
                            },
                            {
                                "impl": {
                                    "impl": {
                                        "value": 3
                                    },
                                    "impl_type": "Int8Literal",
                                    "type": {
                                        "nullable": true,
                                        "type": {
                                            "bitWidth": 8,
                                            "is_signed": true
                                        },
                                        "type_type": "Int"
                                    }
                                },
                                "impl_type": "Literal"
                            }
                        ],
                        "name": "less"
                    },
                    "impl_type": "Call"
                },
                "rel": {
                    "impl": {
                        id: {id:1},
                        "expressions": [
                            {
                                "impl": {
                                    "ref": {
                                        "position": 1
                                    },
                                    "ref_type": "FieldIndex",
                                    "relation_index": 0
                                },
                                "impl_type": "FieldRef"
                            },
                            {
                                "impl": {
                                    "ref": {
                                        "position": 2
                                    },
                                    "ref_type": "FieldIndex",
                                    "relation_index": 0
                                },
                                "impl_type": "FieldRef"
                            }
                        ],
                        "rel": {
                            "impl": {
                                id: {id:0},
                                "name": "tbl",
                                "schema": {
                                    "endianness": "Little",
                                    "fields": [
                                        {
                                            "name": "foo",
                                            "nullable": true,
                                            "type": {
                                                "bitWidth": 32,
                                                "is_signed": true
                                            },
                                            "type_type": "Int"
                                        },
                                        {
                                            "name": "bar",
                                            "nullable": true,
                                            "type": {
                                                "bitWidth": 64,
                                                "is_signed": true
                                            },
                                            "type_type": "Int"
                                        },
                                        {
                                            "name": "baz",
                                            "nullable": true,
                                            "type": {
                                                "precision": "DOUBLE"
                                            },
                                            "type_type": "FloatingPoint"
                                        }
                                    ]
                                }
                            },
                            "impl_type": "Source"
                        }
                    },
                    "impl_type": "Project"
                }
            },
            "impl_type": "Filter"
})"),
      ResultWith(Eq(Declaration::Sequence({
          {"catalog_source",
           CatalogSourceNodeOptions{"tbl", schema({
                                               field("foo", int32()),
                                               field("bar", int64()),
                                               field("baz", float64()),
                                           })},
           "0"},
          {"project", ProjectNodeOptions{/*expressions=*/{field_ref(1), field_ref(2)}},
           "1"},
          {"filter", FilterNodeOptions{less(field_ref(0), literal<int8_t>(3))}, "2"},
      }))));
}

TEST(Relation, ProjectionWithSort) {
  ASSERT_THAT(
      ConvertJSON<ir::Relation>(R"({
            "impl": {
                id: {id:2},
                "keys": [
                    {
                        "expression": {
                            "impl": {
                                "ref": {
                                    "position": 0
                                },
                                "ref_type": "FieldIndex",
                                "relation_index": 0
                            },
                            "impl_type": "FieldRef"
                        },
                        "ordering": "NULLS_THEN_ASCENDING"
                    },
                    {
                        "expression": {
                            "impl": {
                                "ref": {
                                    "position": 1
                                },
                                "ref_type": "FieldIndex",
                                "relation_index": 0
                            },
                            "impl_type": "FieldRef"
                        },
                        "ordering": "NULLS_THEN_DESCENDING"
                    }
                ],
                "rel": {
                    "impl": {
                        id: {id:1},
                        "expressions": [
                            {
                                "impl": {
                                    "ref": {
                                        "position": 0
                                    },
                                    "ref_type": "FieldIndex",
                                    "relation_index": 0
                                },
                                "impl_type": "FieldRef"
                            },
                            {
                                "impl": {
                                    "ref": {
                                        "position": 1
                                    },
                                    "ref_type": "FieldIndex",
                                    "relation_index": 0
                                },
                                "impl_type": "FieldRef"
                            },
                            {
                                "impl": {
                                    "ref": {
                                        "position": 2
                                    },
                                    "ref_type": "FieldIndex",
                                    "relation_index": 0
                                },
                                "impl_type": "FieldRef"
                            }
                        ],
                        "rel": {
                            "impl": {
                                id: {id: 0},
                                "name": "tbl",
                                "schema": {
                                    "endianness": "Little",
                                    "fields": [
                                        {
                                            "name": "foo",
                                            "nullable": true,
                                            "type": {
                                                "bitWidth": 32,
                                                "is_signed": true
                                            },
                                            "type_type": "Int"
                                        },
                                        {
                                            "name": "bar",
                                            "nullable": true,
                                            "type": {
                                                "bitWidth": 64,
                                                "is_signed": true
                                            },
                                            "type_type": "Int"
                                        },
                                        {
                                            "name": "baz",
                                            "nullable": true,
                                            "type": {
                                                "precision": "DOUBLE"
                                            },
                                            "type_type": "FloatingPoint"
                                        }
                                    ]
                                }
                            },
                            "impl_type": "Source"
                        }
                    },
                    "impl_type": "Project"
                }
            },
            "impl_type": "OrderBy"
})"),
      ResultWith(Eq(Declaration::Sequence({
          {"catalog_source",
           CatalogSourceNodeOptions{"tbl", schema({
                                               field("foo", int32()),
                                               field("bar", int64()),
                                               field("baz", float64()),
                                           })},
           "0"},
          {"project",
           ProjectNodeOptions{/*expressions=*/{field_ref(0), field_ref(1), field_ref(2)}},
           "1"},
          {"order_by_sink",
           OrderBySinkNodeOptions{SortOptions{{
                                                  SortKey{0, SortOrder::Ascending},
                                                  SortKey{1, SortOrder::Descending},
                                              },
                                              NullPlacement::AtStart},
                                  nullptr},
           "2"},
      }))));
}

}  // namespace compute
}  // namespace arrow
