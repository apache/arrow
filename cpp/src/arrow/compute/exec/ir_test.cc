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
                }
              },
              {
                name: "f64",
                type_type: "FloatingPoint",
                type: {
                  precision: "DOUBLE"
                }
              },
              {
                name: "i64",
                type_type: "Int",
                type: {
                  bitWidth: 64,
                  is_signed: true
                }
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

}  // namespace compute
}  // namespace arrow
