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
  int ret = RUN_ALL_TESTS();
  gflags::ShutDownCommandLineFlags();
  return ret;
}

namespace arrow {
namespace compute {

bool HaveFlatbufferCompiler() {
  if (int err = std::system("flatc --version")) {
    return false;
  }
  return true;
}

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

  auto st = SetWorkingDir(dir->path());
  if (!st.ok()) st.Abort();

  std::ofstream{"ir.json"} << json;

  std::string cmd = "flatc --binary " + FLAGS_computeir_dir + "/Plan.fbs" +
                    " --root-type org.apache.arrow.computeir.flatbuf." + root_type +
                    " ir.json";
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
  if (!HaveFlatbufferCompiler()) GTEST_SKIP() << "flatc unavailable";

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
  if (!HaveFlatbufferCompiler()) GTEST_SKIP() << "flatc unavailable";

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
  if (!HaveFlatbufferCompiler()) GTEST_SKIP() << "flatc unavailable";

  ASSERT_THAT(
      ConvertJSON<ir::Relation>(R"({
    impl_type: "Filter",
    impl: {
      base: {
        output_mapping_type: "PassThrough",
        output_mapping: {}
      },
      rel: {
        impl_type: "Source",
        impl: {
          base: {
            output_mapping_type: "PassThrough",
            output_mapping: {}
          },
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
                                                   })}},
          {"filter", FilterNodeOptions{equal(field_ref(2), literal<int64_t>(42))}},
      }))));
}

TEST(Relation, AggregateSimple) {
  if (!HaveFlatbufferCompiler()) GTEST_SKIP() << "flatc unavailable";

  ASSERT_THAT(
      ConvertJSON<ir::Relation>(R"({
            "impl": {
                "base": {
                    "output_mapping": {},
                    "output_mapping_type": "PassThrough"
                },
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
                        "base": {
                            "output_mapping": {},
                            "output_mapping_type": "PassThrough"
                        },
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
          {"catalog_source", CatalogSourceNodeOptions{"tbl", schema({
                                                                 field("foo", int32()),
                                                                 field("bar", int64()),
                                                                 field("baz", float64()),
                                                             })}},
          {"aggregate", AggregateNodeOptions{/*aggregates=*/{
                                                 {"sum", nullptr},
                                                 {"mean", nullptr},
                                             },
                                             /*targets=*/{1, 2},
                                             /*names=*/
                                             {
                                                 "sum FieldRef.FieldPath(1)",
                                                 "mean FieldRef.FieldPath(2)",
                                             },
                                             /*keys=*/{0}}},
      }))));
}

TEST(Relation, AggregateWithHaving) {
  if (!HaveFlatbufferCompiler()) GTEST_SKIP() << "flatc unavailable";

  ASSERT_THAT(
      ConvertJSON<ir::Relation>(R"({
            "impl": {
                "base": {
                    "output_mapping": {},
                    "output_mapping_type": "PassThrough"
                },
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
                        "base": {
                            "output_mapping": {},
                            "output_mapping_type": "PassThrough"
                        },
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
                                "base": {
                                    "output_mapping": {},
                                    "output_mapping_type": "PassThrough"
                                },
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
                                        "base": {
                                            "output_mapping": {},
                                            "output_mapping_type": "PassThrough"
                                        },
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
          {"catalog_source", CatalogSourceNodeOptions{"tbl", schema({
                                                                 field("foo", int32()),
                                                                 field("bar", int64()),
                                                                 field("baz", float64()),
                                                             })}},
          {"filter", FilterNodeOptions{less(field_ref(0), literal<int8_t>(3))}},
          {"aggregate", AggregateNodeOptions{/*aggregates=*/{
                                                 {"sum", nullptr},
                                                 {"mean", nullptr},
                                             },
                                             /*targets=*/{1, 2},
                                             /*names=*/
                                             {
                                                 "sum FieldRef.FieldPath(1)",
                                                 "mean FieldRef.FieldPath(2)",
                                             },
                                             /*keys=*/{0}}},
          {"filter", FilterNodeOptions{greater(field_ref(0), literal<int8_t>(10))}},
      }))));
}

TEST(Relation, ProjectionWithFilter) {
  if (!HaveFlatbufferCompiler()) GTEST_SKIP() << "flatc unavailable";

  ASSERT_THAT(
      ConvertJSON<ir::Relation>(R"({
            "impl": {
                "base": {
                    "output_mapping": {},
                    "output_mapping_type": "PassThrough"
                },
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
                        "base": {
                            "output_mapping": {},
                            "output_mapping_type": "PassThrough"
                        },
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
                                "base": {
                                    "output_mapping": {},
                                    "output_mapping_type": "PassThrough"
                                },
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
          {"catalog_source", CatalogSourceNodeOptions{"tbl", schema({
                                                                 field("foo", int32()),
                                                                 field("bar", int64()),
                                                                 field("baz", float64()),
                                                             })}},
          {"project", ProjectNodeOptions{/*expressions=*/{field_ref(1), field_ref(2)}}},
          {"filter", FilterNodeOptions{less(field_ref(0), literal<int8_t>(3))}},
      }))));
}

TEST(Relation, ProjectionWithSort) {
  if (!HaveFlatbufferCompiler()) GTEST_SKIP() << "flatc unavailable";

  ASSERT_THAT(
      ConvertJSON<ir::Relation>(R"({
            "impl": {
                "base": {
                    "output_mapping": {},
                    "output_mapping_type": "PassThrough"
                },
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
                        "base": {
                            "output_mapping": {},
                            "output_mapping_type": "PassThrough"
                        },
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
                                "base": {
                                    "output_mapping": {},
                                    "output_mapping_type": "PassThrough"
                                },
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
          {"catalog_source", CatalogSourceNodeOptions{"tbl", schema({
                                                                 field("foo", int32()),
                                                                 field("bar", int64()),
                                                                 field("baz", float64()),
                                                             })}},
          {"project", ProjectNodeOptions{
                          /*expressions=*/{field_ref(0), field_ref(1), field_ref(2)}}},
          {"order_by_sink",
           OrderBySinkNodeOptions{SortOptions{{
                                                  SortKey{0, SortOrder::Ascending},
                                                  SortKey{1, SortOrder::Descending},
                                              },
                                              NullPlacement::AtStart},
                                  nullptr}},
      }))));
}

}  // namespace compute
}  // namespace arrow
