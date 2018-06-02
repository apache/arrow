/*
 * Copyright (C) 2017-2018 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <memory>
#include <vector>
#include <gtest/gtest.h>
#include "gandiva/expression.h"
#include "codegen/dex.h"
#include "codegen/func_descriptor.h"
#include "codegen/function_registry.h"
#include "codegen/llvm_generator.h"

namespace gandiva {

typedef int64_t (*add_vector_func_t)(int64_t *elements, int nelements);

class TestLLVMGenerator : public ::testing::Test {
 protected:
  void FillBitMap(uint8_t *bmap, int nrecords);
  void ByteWiseIntersectBitMaps(uint8_t *dst, const std::vector<uint8_t *> &srcs,
                                int nrecords);

  FunctionRegistry registry_;
};

void TestLLVMGenerator::FillBitMap(uint8_t *bmap, int nrecords) {
  int nbytes = nrecords / 8;
  unsigned int cur;

  for (int i = 0; i < nbytes; ++i) {
    rand_r(&cur);
    bmap[i] = cur % UINT8_MAX;
  }
}

void TestLLVMGenerator::ByteWiseIntersectBitMaps(uint8_t *dst,
                                                 const std::vector<uint8_t *> &srcs,
                                                 int nrecords) {
  int nbytes = nrecords / 8;
  for (int i = 0; i < nbytes; ++i) {
    dst[i] = 0xff;
    for (uint32_t j = 0; j < srcs.size(); ++j) {
      dst[i] &= srcs[j][i];
    }
  }
}

TEST_F(TestLLVMGenerator, TestAdd) {
  // Setup LLVM generator to do an arithmetic add of two vectors
  std::unique_ptr<LLVMGenerator> generator;
  Status status = LLVMGenerator::Make(&generator);
  EXPECT_TRUE(status.ok());
  Annotator annotator;

  auto field0 = std::make_shared<arrow::Field>("f0", arrow::int32());
  auto desc0 = annotator.CheckAndAddInputFieldDescriptor(field0);
  auto validity_dex0 = std::make_shared<VectorReadValidityDex>(desc0);
  auto value_dex0 = std::make_shared<VectorReadValueDex>(desc0);
  auto pair0 = std::make_shared<ValueValidityPair>(validity_dex0, value_dex0);

  auto field1 = std::make_shared<arrow::Field>("f1", arrow::int32());
  auto desc1 = annotator.CheckAndAddInputFieldDescriptor(field1);
  auto validity_dex1 = std::make_shared<VectorReadValidityDex>(desc1);
  auto value_dex1 = std::make_shared<VectorReadValueDex>(desc1);
  auto pair1 = std::make_shared<ValueValidityPair>(validity_dex1, value_dex1);

  DataTypeVector params{arrow::int32(), arrow::int32()};
  auto func_desc = std::make_shared<FuncDescriptor>("add", params, arrow::int32());
  FunctionSignature signature(func_desc->name(),
                              func_desc->params(),
                              func_desc->return_type());
  const NativeFunction *native_func =
      generator->function_registry_.LookupSignature(signature);

  std::vector<ValueValidityPairPtr> pairs{pair0, pair1};
  auto func_dex = std::make_shared<NonNullableFuncDex>(func_desc, native_func, pairs);

  auto field_sum = std::make_shared<arrow::Field>("out", arrow::int32());
  auto desc_sum = annotator.CheckAndAddInputFieldDescriptor(field_sum);

  llvm::Function *ir_func = nullptr;

  status = generator->CodeGenExprValue(func_dex, desc_sum, 0, &ir_func);
  ASSERT_TRUE(status.ok());

  generator->engine_->FinalizeModule(true, false);
  EvalFunc eval_func = (EvalFunc)generator->engine_->CompiledFunction(ir_func);

  int num_records = 4;
  uint32_t a0[] = {1, 2, 3, 4};
  uint32_t a1[] = {5, 6, 7, 8};
  uint64_t in_bitmap = 0xffffffffffffffffull;

  uint32_t out[] = {0, 0, 0, 0};
  uint64_t out_bitmap = 0;

  uint8_t *addrs[] = {
    reinterpret_cast<uint8_t *>(a0),
    reinterpret_cast<uint8_t *>(&in_bitmap),
    reinterpret_cast<uint8_t *>(a1),
    reinterpret_cast<uint8_t *>(&in_bitmap),
    reinterpret_cast<uint8_t *>(out),
    reinterpret_cast<uint8_t *>(&out_bitmap),
  };
  eval_func(addrs, nullptr, num_records);

  uint32_t expected[] = { 6, 8, 10, 12 };
  for (int i = 0; i  < num_records; i++) {
    EXPECT_EQ(expected[i], out[i]);
  }
}

TEST_F(TestLLVMGenerator, TestNullInternal) {
  // Setup LLVM generator to evaluate a NULL_INTERNAL type function.
  std::unique_ptr<LLVMGenerator> generator;
  Status status = LLVMGenerator::Make(&generator);
  EXPECT_TRUE(status.ok());
  Annotator annotator;

  //generator.enable_ir_traces_ = true;
  auto field0 = std::make_shared<arrow::Field>("f0", arrow::int32());
  auto desc0 = annotator.CheckAndAddInputFieldDescriptor(field0);
  auto validity_dex0 = std::make_shared<VectorReadValidityDex>(desc0);
  auto value_dex0 = std::make_shared<VectorReadValueDex>(desc0);
  auto pair0 = std::make_shared<ValueValidityPair>(validity_dex0, value_dex0);

  DataTypeVector params{arrow::int32()};
  auto func_desc = std::make_shared<FuncDescriptor>("half_or_null", params,
                                                    arrow::int32());
  FunctionSignature signature(func_desc->name(),
                              func_desc->params(),
                              func_desc->return_type());
  const NativeFunction *native_func =
      generator->function_registry_.LookupSignature(signature);

  int local_bitmap_idx = annotator.AddLocalBitMap();
  std::vector<ValueValidityPairPtr> pairs{pair0};
  auto func_dex = std::make_shared<NullableInternalFuncDex>(func_desc, native_func, pairs,
                                                            local_bitmap_idx);

  auto field_result = std::make_shared<arrow::Field>("out", arrow::int32());
  auto desc_result = annotator.CheckAndAddInputFieldDescriptor(field_result);

  llvm::Function *ir_func;
  status = generator->CodeGenExprValue(func_dex, desc_result, 0, &ir_func);
  ASSERT_TRUE(status.ok());

  generator->engine_->FinalizeModule(true /*optimise_ir*/, false /*dump_ir*/);

  EvalFunc eval_func = (EvalFunc)generator->engine_->CompiledFunction(ir_func);

  int num_records = 4;
  uint32_t a0[] = {1, 2, 3, 4};
  uint64_t in_bitmap = 0xffffffffffffffffull;

  uint32_t out[] = {0, 0, 0, 0};
  uint64_t out_bitmap = 0;

  uint64_t local_bitmap = UINT64_MAX;

  uint8_t *addrs[] = {
    reinterpret_cast<uint8_t *>(a0),
    reinterpret_cast<uint8_t *>(&in_bitmap),
    reinterpret_cast<uint8_t *>(out),
    reinterpret_cast<uint8_t *>(&out_bitmap),
  };

  uint8_t *local_bitmap_addrs[] = {
    reinterpret_cast<uint8_t *>(&local_bitmap),
  };

  eval_func(addrs, local_bitmap_addrs, num_records);

  uint32_t expected_value[] = { 0, 1, 0, 2 };
  bool expected_validity[] = { false, true, false, true };

  for (int i = 0; i  < num_records; i++) {
    EXPECT_EQ(expected_value[i], out[i]);
    EXPECT_EQ(expected_validity[i], (local_bitmap & (1 << i)) != 0);
  }
}

TEST_F(TestLLVMGenerator, TestIntersectBitMaps) {
  const int length = 128;
  const int nrecords = length * 8;
  uint8_t src_bitmaps[4][length];
  uint8_t dst_bitmap[length];
  uint8_t expected_bitmap[length];

  for (int i = 0; i < 4; i++) {
    FillBitMap(src_bitmaps[i], nrecords);
  }

  for (int i = 0; i < 4; i++) {
    std::vector<uint8_t *> src_bitmap_ptrs;
    for (int j = 0; j < i; ++j) {
      src_bitmap_ptrs.push_back(src_bitmaps[j]);
    }

    LLVMGenerator::IntersectBitMaps(dst_bitmap, src_bitmap_ptrs, nrecords);
    ByteWiseIntersectBitMaps(expected_bitmap, src_bitmap_ptrs, nrecords);
    EXPECT_EQ(memcmp(dst_bitmap, expected_bitmap, length), 0);
  }
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

} // namespace gandiva
