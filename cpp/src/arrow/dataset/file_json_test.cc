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

#include "arrow/dataset/file_json.h"

#include "arrow/dataset/plan.h"
#include "arrow/dataset/test_util_internal.h"
#include "arrow/filesystem/mockfs.h"
#include "arrow/json/parser.h"
#include "arrow/json/rapidjson_defs.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/util.h"

#include "rapidjson/ostreamwrapper.h"
#include "rapidjson/writer.h"

namespace arrow {

using internal::checked_cast;

namespace dataset {

namespace rj = arrow::rapidjson;

#define CASE(TYPE_CLASS)                            \
  case TYPE_CLASS##Type::type_id: {                 \
    const TYPE_CLASS##Type* concrete_ptr = nullptr; \
    return visitor->Visit(concrete_ptr);            \
  }

template <typename VISITOR>
static Status VisitWriteableTypeId(Type::type id, VISITOR* visitor) {
  switch (id) {
    ARROW_GENERATE_FOR_ALL_NUMERIC_TYPES(CASE)
    CASE(Boolean)
    CASE(Struct)
    default:
      return Status::NotImplemented("TypeId: ", id);
  }
}

#undef CASE

// There's currently no proper API for writing JSON files, which is reflected in the JSON
// dataset API as well. However, this ad-hoc implementation is good enough for the shared
// format test fixtures
struct WriteVisitor {
  static Status OK(bool ok) {
    return ok ? Status::OK()
              : Status::Invalid("Unexpected false return from JSON writer");
  }

  template <typename T>
  enable_if_physical_signed_integer<T, Status> Visit(const T*) {
    const auto& scalar = checked_cast<const NumericScalar<T>&>(scalar_);
    return OK(writer_.Int64(scalar.value));
  }

  template <typename T>
  enable_if_physical_unsigned_integer<T, Status> Visit(const T*) {
    const auto& scalar = checked_cast<const NumericScalar<T>&>(scalar_);
    return OK(writer_.Uint64(scalar.value));
  }

  template <typename T>
  enable_if_physical_floating_point<T, Status> Visit(const T*) {
    const auto& scalar = checked_cast<const NumericScalar<T>&>(scalar_);
    return OK(writer_.Double(scalar.value));
  }

  Status Visit(const BooleanType*) {
    const auto& scalar = checked_cast<const BooleanScalar&>(scalar_);
    return OK(writer_.Bool(scalar.value));
  }

  Status Visit(const StructType*) {
    const auto& scalar = checked_cast<const StructScalar&>(scalar_);
    const auto& type = checked_cast<const StructType&>(*scalar.type);
    DCHECK_EQ(type.num_fields(), static_cast<int>(scalar.value.size()));

    RETURN_NOT_OK(OK(writer_.StartObject()));

    for (int i = 0; i < type.num_fields(); ++i) {
      const auto& name = type.field(i)->name();
      RETURN_NOT_OK(
          OK(writer_.Key(name.data(), static_cast<rj::SizeType>(name.length()))));

      const auto& child = *scalar.value[i];
      if (!child.is_valid) {
        RETURN_NOT_OK(OK(writer_.Null()));
        continue;
      }

      WriteVisitor visitor{writer_, child};
      RETURN_NOT_OK(VisitWriteableTypeId(child.type->id(), &visitor));
    }

    RETURN_NOT_OK(OK(writer_.EndObject(type.num_fields())));
    return Status::OK();
  }

  rj::Writer<rj::OStreamWrapper>& writer_;
  const Scalar& scalar_;
};

Status WriteJson(const StructScalar& scalar, rj::OStreamWrapper* sink) {
  rj::Writer<rj::OStreamWrapper> writer(*sink);
  WriteVisitor visitor{writer, scalar};
  return VisitWriteableTypeId(Type::STRUCT, &visitor);
}

class JsonFormatHelper {
 public:
  using FormatType = JsonFileFormat;

  static Result<std::shared_ptr<Buffer>> Write(RecordBatchReader* reader) {
    ARROW_ASSIGN_OR_RAISE(auto scalars, ToScalars(reader));
    std::stringstream ss;
    rj::OStreamWrapper sink(ss);
    for (const auto& scalar : scalars) {
      RETURN_NOT_OK(WriteJson(*scalar, &sink));
      ss << "\n";
    }
    return Buffer::FromString(ss.str());
  }

  static std::shared_ptr<FormatType> MakeFormat() {
    return std::make_shared<FormatType>();
  }

 private:
  static Result<std::vector<std::shared_ptr<StructScalar>>> ToScalars(
      RecordBatchReader* reader) {
    std::vector<std::shared_ptr<StructScalar>> scalars;
    for (auto maybe_batch : *reader) {
      ARROW_ASSIGN_OR_RAISE(auto batch, maybe_batch);
      ARROW_ASSIGN_OR_RAISE(auto array, batch->ToStructArray());
      for (int i = 0; i < array->length(); ++i) {
        ARROW_ASSIGN_OR_RAISE(auto scalar, array->GetScalar(i));
        scalars.push_back(checked_pointer_cast<StructScalar>(std::move(scalar)));
      }
    }
    return scalars;
  }
};

std::shared_ptr<FileSource> ToFileSource(std::string json) {
  return std::make_shared<FileSource>(Buffer::FromString(std::move(json)));
}

// Mixin for additional JSON-specific tests, compatibile with both format APIs.
template <typename T>
class JsonScanMixin {
 public:
  void TestScanWithBOM() {
    auto source = ToFileSource(
        "\xef\xbb\xbf{\"ab\":0,\"cd\":\"foo\"}\n{\"ab\":1,\"cd\":\"bar\"}\n");
    auto fragment = this_->MakeFragment(*source);
    auto dataset_schema = schema({field("ab", int64()), field("cd", utf8())});
    this_->SetSchema(dataset_schema->fields());
    this_->SetJsonOptions();

    int64_t num_rows = 0;
    for (auto maybe_batch : this_->Batches(fragment)) {
      ASSERT_OK_AND_ASSIGN(auto batch, maybe_batch);
      AssertSchemaEqual(dataset_schema, batch->schema());
      num_rows += batch->num_rows();
    }

    ASSERT_EQ(num_rows, 2);
  }

  void TestScanWithCustomParseOptions() {
    auto source = ToFileSource("{\n\"i\":0\n}\n{\n\"i\":1\n}");
    auto fragment = this_->MakeFragment(*source);
    this_->SetSchema({field("i", int64())});

    JsonFragmentScanOptions json_options;
    json_options.parse_options.newlines_in_values = true;
    // These options would raise errors if used, but they should be ignored
    json_options.parse_options.explicit_schema = schema({field("x", utf8())});
    json_options.parse_options.unexpected_field_behavior =
        json::UnexpectedFieldBehavior::Error;
    this_->SetJsonOptions(std::move(json_options));

    int64_t num_rows = 0;
    for (auto maybe_batch : this_->Batches(fragment)) {
      ASSERT_OK_AND_ASSIGN(auto batch, maybe_batch);
      num_rows += batch->num_rows();
    }

    ASSERT_EQ(num_rows, 2);
  }

  void TestScanWithCustomBlockSize() {
    auto source = ToFileSource("{\"i\":0}\n{\"i\":1}\n{\"i\":2}");
    auto fragment = this_->MakeFragment(*source);
    this_->SetSchema({field("i", int64())});

    JsonFragmentScanOptions json_options;
    json_options.read_options.block_size = 8;
    this_->SetJsonOptions(std::move(json_options));

    int64_t num_rows = 0;
    for (auto maybe_batch : this_->Batches(fragment)) {
      ASSERT_OK_AND_ASSIGN(auto batch, maybe_batch);
      // The reader should yield one row per batch, so the scanned batch size shouldn't
      // exceed that.
      EXPECT_LE(batch->num_rows(), 1);
      num_rows += batch->num_rows();
    }

    ASSERT_EQ(num_rows, 3);
  }

  void TestScanWithParallelDecoding() {
    // Test intra-fragment parallelism, which the JSON reader supports independent of the
    // scanner. We set a small block size to stress thread usage alongside the scanner's
    // inter-fragment parallelism (when threading is enabled).
    JsonFragmentScanOptions json_options;
    json_options.read_options.use_threads = true;
    // Should amount to roughly 60 blocks per fragment.
    json_options.read_options.block_size = 1 << 13;
    this_->SetJsonOptions(std::move(json_options));
    this_->TestScan();
  }

 private:
  T* const this_ = static_cast<T*>(this);
};

// Use a reduced number of rows in valgrind to avoid timeouts.
#ifndef ARROW_VALGRIND
constexpr static int64_t kTestMaxNumRows = json::kMaxParserNumRows;
#else
constexpr static int64_t kTestMaxNumRows = 1024;
#endif

class TestJsonFormat : public FileFormatFixtureMixin<JsonFormatHelper, kTestMaxNumRows> {
};

class TestJsonFormatV2
    : public FileFormatFixtureMixinV2<JsonFormatHelper, kTestMaxNumRows> {};

class TestJsonScan : public FileFormatScanMixin<JsonFormatHelper>,
                     public JsonScanMixin<TestJsonScan> {
 public:
  void SetJsonOptions(JsonFragmentScanOptions options = {}) {
    opts_->fragment_scan_options =
        std::make_shared<JsonFragmentScanOptions>(std::move(options));
  }
};

class TestJsonScanNode : public FileFormatScanNodeMixin<JsonFormatHelper>,
                         public JsonScanMixin<TestJsonScanNode> {
 public:
  void SetSchema(FieldVector fields) { SetDatasetSchema(std::move(fields)); }

  void SetJsonOptions(JsonFragmentScanOptions options = {}) {
    json_options_ = std::move(options);
  }

 protected:
  void SetUp() override { internal::Initialize(); }

  const FragmentScanOptions* GetFormatOptions() override { return &json_options_; }

  JsonFragmentScanOptions json_options_;
};

TEST_F(TestJsonFormat, Equals) {
  JsonFileFormat format;
  ASSERT_TRUE(format.Equals(JsonFileFormat()));
  ASSERT_FALSE(format.Equals(DummyFileFormat()));
}

// Common tests for old API
TEST_F(TestJsonFormat, IsSupported) { TestIsSupported(); }
TEST_F(TestJsonFormat, Inspect) { TestInspect(); }
TEST_F(TestJsonFormat, FragmentEquals) { TestFragmentEquals(); }
TEST_F(TestJsonFormat, InspectFailureWithRelevantError) {
  TestInspectFailureWithRelevantError(StatusCode::Invalid, "JSON");
}
TEST_F(TestJsonFormat, CountRows) { TestCountRows(); }

// Common tests for new API
TEST_F(TestJsonFormatV2, IsSupported) { TestIsSupported(); }
TEST_F(TestJsonFormatV2, Inspect) { TestInspect(); }
TEST_F(TestJsonFormatV2, FragmentEquals) { TestFragmentEquals(); }
TEST_F(TestJsonFormatV2, InspectFailureWithRelevantError) {
  TestInspectFailureWithRelevantError(StatusCode::Invalid, "JSON");
}
TEST_F(TestJsonFormatV2, CountRows) { TestCountRows(); }

// Common tests for old scanner
TEST_P(TestJsonScan, Scan) { TestScan(); }
TEST_P(TestJsonScan, ScanBatchSize) { TestScanBatchSize(); }
TEST_P(TestJsonScan, ScanProjected) { TestScanProjected(); }
TEST_P(TestJsonScan, ScanWithDuplicateColumnError) { TestScanWithDuplicateColumnError(); }
TEST_P(TestJsonScan, ScanWithVirtualColumn) { TestScanWithVirtualColumn(); }
TEST_P(TestJsonScan, ScanWithPushdownNulls) { TestScanWithPushdownNulls(); }
TEST_P(TestJsonScan, ScanProjectedMissingCols) { TestScanProjectedMissingCols(); }
TEST_P(TestJsonScan, ScanProjectedNested) { TestScanProjectedNested(); }
// JSON-specific tests for old scanner
TEST_P(TestJsonScan, ScanWithBOM) { TestScanWithBOM(); }
TEST_P(TestJsonScan, ScanWithCustomParseOptions) { TestScanWithCustomParseOptions(); }
TEST_P(TestJsonScan, ScanWithCustomBlockSize) { TestScanWithCustomBlockSize(); }
TEST_P(TestJsonScan, ScanWithParallelDecoding) { TestScanWithParallelDecoding(); }

INSTANTIATE_TEST_SUITE_P(TestJsonScan, TestJsonScan,
                         ::testing::ValuesIn(TestFormatParams::Values()),
                         TestFormatParams::ToTestNameString);

// Common tests for new scanner
TEST_P(TestJsonScanNode, Scan) { TestScan(); }
TEST_P(TestJsonScanNode, ScanMissingFilterField) { TestScanMissingFilterField(); }
TEST_P(TestJsonScanNode, ScanProjected) { TestScanProjected(); }
TEST_P(TestJsonScanNode, ScanProjectedMissingColumns) { TestScanProjectedMissingCols(); }
// JSON-specific tests for new scanner
TEST_P(TestJsonScanNode, ScanWithBOM) { TestScanWithBOM(); }
TEST_P(TestJsonScanNode, ScanWithCustomParseOptions) { TestScanWithCustomParseOptions(); }
TEST_P(TestJsonScanNode, ScanWithCustomBlockSize) { TestScanWithCustomBlockSize(); }
TEST_P(TestJsonScanNode, ScanWithParallelDecoding) { TestScanWithParallelDecoding(); }

INSTANTIATE_TEST_SUITE_P(TestJsonScanNode, TestJsonScanNode,
                         ::testing::ValuesIn(TestFormatParams::Values()),
                         TestFormatParams::ToTestNameString);

}  // namespace dataset
}  // namespace arrow
