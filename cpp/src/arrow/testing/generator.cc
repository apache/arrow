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

#include "arrow/testing/generator.h"

#include <algorithm>
#include <memory>
#include <random>
#include <vector>

#include <gtest/gtest.h>

#include "arrow/array.h"
#include "arrow/buffer.h"
#include "arrow/builder.h"
#include "arrow/compute/exec.h"
#include "arrow/datum.h"
#include "arrow/record_batch.h"
#include "arrow/scalar.h"
#include "arrow/table.h"
#include "arrow/testing/builder.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/random.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/macros.h"
#include "arrow/util/string.h"

namespace arrow {

using internal::checked_pointer_cast;

template <typename ArrowType, typename CType = typename TypeTraits<ArrowType>::CType,
          typename BuilderType = typename TypeTraits<ArrowType>::BuilderType>
static inline std::shared_ptr<Array> ConstantArray(int64_t size, CType value) {
  auto type = TypeTraits<ArrowType>::type_singleton();
  auto builder_fn = [&](BuilderType* builder) { builder->UnsafeAppend(value); };
  return ArrayFromBuilderVisitor(type, size, builder_fn).ValueOrDie();
}

std::shared_ptr<arrow::Array> ConstantArrayGenerator::Boolean(int64_t size, bool value) {
  return ConstantArray<BooleanType>(size, value);
}

std::shared_ptr<arrow::Array> ConstantArrayGenerator::UInt8(int64_t size, uint8_t value) {
  return ConstantArray<UInt8Type>(size, value);
}

std::shared_ptr<arrow::Array> ConstantArrayGenerator::Int8(int64_t size, int8_t value) {
  return ConstantArray<Int8Type>(size, value);
}

std::shared_ptr<arrow::Array> ConstantArrayGenerator::UInt16(int64_t size,
                                                             uint16_t value) {
  return ConstantArray<UInt16Type>(size, value);
}

std::shared_ptr<arrow::Array> ConstantArrayGenerator::Int16(int64_t size, int16_t value) {
  return ConstantArray<Int16Type>(size, value);
}

std::shared_ptr<arrow::Array> ConstantArrayGenerator::UInt32(int64_t size,
                                                             uint32_t value) {
  return ConstantArray<UInt32Type>(size, value);
}

std::shared_ptr<arrow::Array> ConstantArrayGenerator::Int32(int64_t size, int32_t value) {
  return ConstantArray<Int32Type>(size, value);
}

std::shared_ptr<arrow::Array> ConstantArrayGenerator::UInt64(int64_t size,
                                                             uint64_t value) {
  return ConstantArray<UInt64Type>(size, value);
}

std::shared_ptr<arrow::Array> ConstantArrayGenerator::Int64(int64_t size, int64_t value) {
  return ConstantArray<Int64Type>(size, value);
}

std::shared_ptr<arrow::Array> ConstantArrayGenerator::Float32(int64_t size, float value) {
  return ConstantArray<FloatType>(size, value);
}

std::shared_ptr<arrow::Array> ConstantArrayGenerator::Float64(int64_t size,
                                                              double value) {
  return ConstantArray<DoubleType>(size, value);
}

std::shared_ptr<arrow::Array> ConstantArrayGenerator::String(int64_t size,
                                                             std::string value) {
  return ConstantArray<StringType>(size, value);
}

std::shared_ptr<arrow::Array> ConstantArrayGenerator::Zeroes(
    int64_t size, const std::shared_ptr<DataType>& type) {
  switch (type->id()) {
    case Type::NA:
      return std::make_shared<NullArray>(size);
    case Type::BOOL:
      return Boolean(size);
    case Type::UINT8:
      return UInt8(size);
    case Type::INT8:
      return Int8(size);
    case Type::UINT16:
      return UInt16(size);
    case Type::INT16:
      return Int16(size);
    case Type::UINT32:
      return UInt32(size);
    case Type::INT32:
      return Int32(size);
    case Type::UINT64:
      return UInt64(size);
    case Type::INT64:
      return Int64(size);
    case Type::TIME64:
    case Type::DATE64:
    case Type::TIMESTAMP: {
      EXPECT_OK_AND_ASSIGN(auto viewed, Int64(size)->View(type));
      return viewed;
    }
    case Type::INTERVAL_DAY_TIME:
    case Type::INTERVAL_MONTHS:
    case Type::TIME32:
    case Type::DATE32: {
      EXPECT_OK_AND_ASSIGN(auto viewed, Int32(size)->View(type));
      return viewed;
    }
    case Type::FLOAT:
      return Float32(size);
    case Type::DOUBLE:
      return Float64(size);
    case Type::STRING:
      return String(size);
    case Type::STRUCT: {
      ArrayVector children;
      children.reserve(type->num_fields());
      for (const auto& field : type->fields()) {
        children.push_back(Zeroes(size, field->type()));
      }
      return std::make_shared<StructArray>(type, size, children);
    }
    default:
      ADD_FAILURE() << "ConstantArrayGenerator::Zeroes is not implemented for " << *type;
      return nullptr;
  }
}

std::shared_ptr<RecordBatch> ConstantArrayGenerator::Zeroes(
    int64_t size, const std::shared_ptr<Schema>& schema) {
  std::vector<std::shared_ptr<Array>> arrays;

  for (const auto& field : schema->fields()) {
    arrays.emplace_back(Zeroes(size, field->type()));
  }

  return RecordBatch::Make(schema, size, arrays);
}

std::shared_ptr<RecordBatchReader> ConstantArrayGenerator::Repeat(
    int64_t n_batch, const std::shared_ptr<RecordBatch> batch) {
  std::vector<std::shared_ptr<RecordBatch>> batches(static_cast<size_t>(n_batch), batch);
  return *RecordBatchReader::Make(batches);
}

std::shared_ptr<RecordBatchReader> ConstantArrayGenerator::Zeroes(
    int64_t n_batch, int64_t batch_size, const std::shared_ptr<Schema>& schema) {
  return Repeat(n_batch, Zeroes(batch_size, schema));
}

Result<std::shared_ptr<Array>> ScalarVectorToArray(const ScalarVector& scalars) {
  if (scalars.empty()) {
    return Status::NotImplemented("ScalarVectorToArray with no scalars");
  }
  std::unique_ptr<arrow::ArrayBuilder> builder;
  RETURN_NOT_OK(MakeBuilder(default_memory_pool(), scalars[0]->type, &builder));
  RETURN_NOT_OK(builder->AppendScalars(scalars));
  std::shared_ptr<Array> out;
  RETURN_NOT_OK(builder->Finish(&out));
  return out;
}

namespace gen {

namespace {
class ConstantGenerator : public ArrayGenerator {
 public:
  explicit ConstantGenerator(std::shared_ptr<Scalar> value) : value_(std::move(value)) {}

  Result<std::shared_ptr<Array>> Generate(int64_t num_rows) override {
    return MakeArrayFromScalar(*value_, num_rows);
  }

  std::shared_ptr<DataType> type() const override { return value_->type; }

 private:
  std::shared_ptr<Scalar> value_;
};

class StepGenerator : public ArrayGenerator {
 public:
  StepGenerator(uint32_t start, uint32_t step) : start_(start), step_(step) {}

  Result<std::shared_ptr<Array>> Generate(int64_t num_rows) override {
    UInt32Builder builder;
    ARROW_RETURN_NOT_OK(builder.Reserve(num_rows));
    uint32_t val = start_;
    for (int64_t i = 0; i < num_rows; i++) {
      builder.UnsafeAppend(val);
      val += step_;
    }
    start_ = val;
    return builder.Finish();
  }

  std::shared_ptr<DataType> type() const override { return uint32(); }

 private:
  uint32_t start_;
  uint32_t step_;
};

static constexpr random::SeedType kTestSeed = 42;

class RandomGenerator : public ArrayGenerator {
 public:
  explicit RandomGenerator(std::shared_ptr<DataType> type) : type_(std::move(type)) {}

  random::RandomArrayGenerator* generator() {
    static random::RandomArrayGenerator instance(kTestSeed);
    return &instance;
  }

  Result<std::shared_ptr<Array>> Generate(int64_t num_rows) override {
    return generator()->ArrayOf(type_, num_rows);
  }

  std::shared_ptr<DataType> type() const override { return type_; }

 private:
  std::shared_ptr<DataType> type_;
};

class DataGeneratorImpl : public DataGenerator,
                          public std::enable_shared_from_this<DataGeneratorImpl> {
 public:
  explicit DataGeneratorImpl(std::vector<GeneratorField> generators)
      : generators_(std::move(generators)) {
    schema_ = DeriveSchemaFromGenerators();
  }

  Result<std::shared_ptr<::arrow::RecordBatch>> RecordBatch(int64_t num_rows) override {
    std::vector<std::shared_ptr<Array>> columns;
    columns.reserve(generators_.size());
    for (auto& field : generators_) {
      ARROW_ASSIGN_OR_RAISE(std::shared_ptr<Array> arr, field.gen->Generate(num_rows));
      columns.push_back(std::move(arr));
    }
    return RecordBatch::Make(schema_, num_rows, std::move(columns));
  }

  Result<std::vector<std::shared_ptr<::arrow::RecordBatch>>> RecordBatches(
      int64_t rows_per_batch, int num_batches) override {
    std::vector<std::shared_ptr<::arrow::RecordBatch>> batches;
    batches.reserve(num_batches);
    for (int i = 0; i < num_batches; i++) {
      ARROW_ASSIGN_OR_RAISE(std::shared_ptr<::arrow::RecordBatch> batch,
                            RecordBatch(rows_per_batch));
      batches.push_back(std::move(batch));
    }
    return batches;
  }

  Result<::arrow::compute::ExecBatch> ExecBatch(int64_t num_rows) override {
    std::vector<Datum> values;
    values.reserve(generators_.size());
    for (auto& field : generators_) {
      ARROW_ASSIGN_OR_RAISE(std::shared_ptr<Array> arr, field.gen->Generate(num_rows));
      values.push_back(std::move(arr));
    }
    return ::arrow::compute::ExecBatch(std::move(values), num_rows);
  }

  Result<std::vector<::arrow::compute::ExecBatch>> ExecBatches(int64_t rows_per_batch,
                                                               int num_batches) override {
    std::vector<::arrow::compute::ExecBatch> batches;
    for (int i = 0; i < num_batches; i++) {
      ARROW_ASSIGN_OR_RAISE(::arrow::compute::ExecBatch batch, ExecBatch(rows_per_batch));
      batches.push_back(std::move(batch));
    }
    return batches;
  }

  Result<std::shared_ptr<::arrow::Table>> Table(int64_t rows_per_chunk,
                                                int num_chunks = 1) override {
    ARROW_ASSIGN_OR_RAISE(RecordBatchVector batches,
                          RecordBatches(rows_per_chunk, num_chunks));
    return ::arrow::Table::FromRecordBatches(batches);
  }

  std::shared_ptr<::arrow::Schema> Schema() override { return schema_; }

  std::unique_ptr<GTestDataGenerator> FailOnError() override;

 private:
  std::shared_ptr<::arrow::Schema> DeriveSchemaFromGenerators() {
    FieldVector fields;
    for (std::size_t i = 0; i < generators_.size(); i++) {
      const GeneratorField& gen = generators_[i];
      std::string name;
      if (gen.name) {
        name = *gen.name;
      } else {
        name = "f" + internal::ToChars(i);
      }
      fields.push_back(field(std::move(name), gen.gen->type()));
    }
    return schema(std::move(fields));
  }

  std::vector<GeneratorField> generators_;
  std::shared_ptr<::arrow::Schema> schema_;
};

class GTestDataGeneratorImpl : public GTestDataGenerator {
 public:
  explicit GTestDataGeneratorImpl(std::shared_ptr<DataGenerator> target)
      : target_(std::move(target)) {}
  std::shared_ptr<::arrow::RecordBatch> RecordBatch(int64_t num_rows) override {
    EXPECT_OK_AND_ASSIGN(auto batch, target_->RecordBatch(num_rows));
    return batch;
  }
  std::vector<std::shared_ptr<::arrow::RecordBatch>> RecordBatches(
      int64_t rows_per_batch, int num_batches) override {
    EXPECT_OK_AND_ASSIGN(auto batches,
                         target_->RecordBatches(rows_per_batch, num_batches));
    return batches;
  }

  ::arrow::compute::ExecBatch ExecBatch(int64_t num_rows) override {
    EXPECT_OK_AND_ASSIGN(auto batch, target_->ExecBatch(num_rows));
    return batch;
  }
  std::vector<::arrow::compute::ExecBatch> ExecBatches(int64_t rows_per_batch,
                                                       int num_batches) override {
    EXPECT_OK_AND_ASSIGN(auto batches, target_->ExecBatches(rows_per_batch, num_batches));
    return batches;
  }

  std::shared_ptr<::arrow::Table> Table(int64_t rows_per_chunk, int num_chunks) override {
    EXPECT_OK_AND_ASSIGN(auto table, target_->Table(rows_per_chunk, num_chunks));
    return table;
  }
  std::shared_ptr<::arrow::Schema> Schema() override { return target_->Schema(); }

 private:
  std::shared_ptr<DataGenerator> target_;
};

// Defined down here to avoid circular dependency between DataGeneratorImpl and
// GTestDataGeneratorImpl
std::unique_ptr<GTestDataGenerator> DataGeneratorImpl::FailOnError() {
  return std::make_unique<GTestDataGeneratorImpl>(shared_from_this());
}

}  // namespace

std::shared_ptr<ArrayGenerator> Constant(std::shared_ptr<Scalar> value) {
  return std::make_shared<ConstantGenerator>(std::move(value));
}

std::shared_ptr<ArrayGenerator> Step(uint32_t start, uint32_t step) {
  return std::make_shared<StepGenerator>(start, step);
}

std::shared_ptr<ArrayGenerator> Random(std::shared_ptr<DataType> type) {
  return std::make_shared<RandomGenerator>(std::move(type));
}

std::shared_ptr<DataGenerator> Gen(std::vector<GeneratorField> fields) {
  return std::make_shared<DataGeneratorImpl>(std::move(fields));
}

}  // namespace gen

}  // namespace arrow
