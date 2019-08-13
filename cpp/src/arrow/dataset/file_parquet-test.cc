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

#include "arrow/dataset/file_parquet.h"

#include "arrow/dataset/test_util.h"
#include "arrow/record_batch.h"
#include "arrow/visitor_inline.h"

namespace arrow {
namespace dataset {

class TestParquetFileFormat : public FileSourceFixtureMixin {
 public:
  TestParquetFileFormat() : ctx_(std::make_shared<ScanContext>()) {}

 protected:
  std::shared_ptr<ScanOptions> opts_;
  std::shared_ptr<ScanContext> ctx_;
};

struct MakeRepeatedArrayImpl {
  template <typename T>
  MakeRepeatedArrayImpl(std::shared_ptr<DataType> type, const T& value,
                        int64_t repetitions)
      : type_(type), typeid_(Typeid<T>), value_(&value), repetitions_(repetitions) {}

  template <typename T>
  enable_if_number<T, Status> Visit(const T& t) {
    typename TypeTraits<T>::BuilderType builder;
    using c_type = typename T::c_type;
    auto value = *static_cast<const c_type*>(value_);
    RETURN_NOT_OK(builder.Resize(repetitions_));
    for (auto i = 0; i < repetitions_; ++i) {
      builder.UnsafeAppend(value);
    }
    return builder.Finish(&out_);
  }

  template <typename T>
  enable_if_binary_like<T, Status> Visit(const T& t) {
    typename TypeTraits<T>::BuilderType builder(type_, default_memory_pool());
    auto value = *static_cast<const util::string_view*>(value_);
    RETURN_NOT_OK(builder.Resize(repetitions_));
    for (auto i = 0; i < repetitions_; ++i) {
      builder.UnsafeAppend(value);
    }
    return builder.Finish(&out_);
  }

  Status Visit(const DataType& t) {
    return Status::Invalid("creating arrays of repeated ", t);
  }

  template <typename T>
  static void Typeid() {}

  std::shared_ptr<DataType> type_;
  void (*typeid_)();
  const void* value_;
  int64_t repetitions_;
  std::shared_ptr<Array> out_;
};

#define ASSERT_OK_AND_ASSIGN_IMPL(status_name, lhs, rexpr) \
  auto status_name = (rexpr);                              \
  ASSERT_OK(status_name.status());                         \
  lhs = std::move(status_name).ValueOrDie();

#define ASSERT_OK_AND_ASSIGN(lhs, rexpr)                                              \
  ASSERT_OK_AND_ASSIGN_IMPL(ARROW_ASSIGN_OR_RAISE_NAME(_error_or_value, __COUNTER__), \
                            lhs, rexpr);

template <typename T>
Result<std::shared_ptr<Array>> MakeRepeatedArray(const std::shared_ptr<DataType>& type,
                                                 int64_t repetitions, const T& t) {
  MakeRepeatedArrayImpl impl(type, t, repetitions);
  RETURN_NOT_OK(VisitTypeInline(*type, &impl));
  return impl.out_;
}

TEST_F(TestParquetFileFormat, ScanFile2) {
  ASSERT_OK_AND_ASSIGN(auto doubles_array, MakeRepeatedArray(float64(), 1 << 10, 0.0));
  auto doubles_batch = RecordBatch::Make(schema({field("doubles", float64())}),
                                         doubles_array->length(), {doubles_array});
  RepeatedRecordBatch double_reader(1 << 20, doubles_batch);
}

TEST_F(TestParquetFileFormat, ScanFile) {
  auto location = GetParquetLocation("data/double_1Grows_1kgroups.parquet");
  auto fragment = std::make_shared<ParquetFragment>(*location, opts_);

  std::unique_ptr<ScanTaskIterator> it;
  ASSERT_OK(fragment->GetTasks(ctx_, &it));
  int64_t row_count = 0;

  ASSERT_OK(it->Visit([&row_count](std::unique_ptr<ScanTask> task) -> Status {
    auto batch_it = task->Scan();

    RETURN_NOT_OK(
        batch_it->Visit([&row_count](std::shared_ptr<RecordBatch> batch) -> Status {
          row_count += batch->num_rows();
          return Status::OK();
        }));

    return Status::OK();
  }));

  ASSERT_EQ(row_count, 1UL << 30);
}

}  // namespace dataset
}  // namespace arrow
