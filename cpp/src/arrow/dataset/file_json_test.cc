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
  static Status OK(bool ok) { return ok ? Status::OK() : Status::Invalid("Unexpected false return from JSON writer"); }

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

  rj::Writer<rj::StringBuffer>& writer_;
  const Scalar& scalar_;
};

Result<std::string> WriteJson(const StructScalar& scalar) {
  rj::StringBuffer sink;
  rj::Writer<rj::StringBuffer> writer(sink);
  WriteVisitor visitor{writer, scalar};
  RETURN_NOT_OK(VisitWriteableTypeId(Type::STRUCT, &visitor));
  return sink.GetString();
}

class JsonFormatHelper {
 public:
  using FormatType = JsonFileFormat;

  static Result<std::shared_ptr<Buffer>> Write(RecordBatchReader* reader) {
    ARROW_ASSIGN_OR_RAISE(auto scalars, ToScalars(reader));
    std::string out;
    for (const auto& scalar : scalars) {
      ARROW_ASSIGN_OR_RAISE(auto json, WriteJson(*scalar));
      out += json + "\n";
    }
    return Buffer::FromString(std::move(out));
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

class TestJsonFormat
    : public FileFormatFixtureMixin<JsonFormatHelper, json::kMaxParserNumRows> {};

class TestJsonFormatScan : public FileFormatScanMixin<JsonFormatHelper> {};

class TestJsonFormatScanNode : public FileFormatScanNodeMixin<JsonFormatHelper> {
  void SetUp() override { internal::Initialize(); }

  const FragmentScanOptions* GetFormatOptions() override { return &json_options_; }

 protected:
  JsonFragmentScanOptions json_options_;
};

TEST_F(TestJsonFormat, Equals) {
  JsonFileFormat format;
  ASSERT_TRUE(format.Equals(JsonFileFormat()));
  ASSERT_FALSE(format.Equals(DummyFileFormat()));
}

TEST_F(TestJsonFormat, IsSupported) { TestIsSupported(); }
TEST_F(TestJsonFormat, Inspect) { TestInspect(); }
TEST_F(TestJsonFormat, FragmentEquals) { TestFragmentEquals(); }
TEST_F(TestJsonFormat, InspectFailureWithRelevantError) {
  TestInspectFailureWithRelevantError(StatusCode::Invalid, "JSON");
}
TEST_F(TestJsonFormat, CountRows) { TestCountRows(); }

TEST_P(TestJsonFormatScan, Scan) { TestScan(); }
TEST_P(TestJsonFormatScan, ScanBatchSize) { TestScanBatchSize(); }
TEST_P(TestJsonFormatScan, ScanProjected) { TestScanProjected(); }
TEST_P(TestJsonFormatScan, ScanWithDuplicateColumnError) {
  TestScanWithDuplicateColumnError();
}
TEST_P(TestJsonFormatScan, ScanWithVirtualColumn) { TestScanWithVirtualColumn(); }
TEST_P(TestJsonFormatScan, ScanWithPushdownNulls) { TestScanWithPushdownNulls(); }
TEST_P(TestJsonFormatScan, ScanProjectedMissingCols) { TestScanProjectedMissingCols(); }
TEST_P(TestJsonFormatScan, ScanProjectedNested) { TestScanProjectedNested(); }

INSTANTIATE_TEST_SUITE_P(TestJsonScan, TestJsonFormatScan,
                         ::testing::ValuesIn(TestFormatParams::Values()),
                         TestFormatParams::ToTestNameString);

TEST_P(TestJsonFormatScanNode, Scan) { TestScan(); }
TEST_P(TestJsonFormatScanNode, ScanMissingFilterField) { TestScanMissingFilterField(); }
TEST_P(TestJsonFormatScanNode, ScanProjected) { TestScanProjected(); }
TEST_P(TestJsonFormatScanNode, ScanProjectedMissingColumns) {
  TestScanProjectedMissingCols();
}

INSTANTIATE_TEST_SUITE_P(TestJsonScanNode, TestJsonFormatScanNode,
                         ::testing::ValuesIn(TestFormatParams::Values()),
                         TestFormatParams::ToTestNameString);

}  // namespace dataset
}  // namespace arrow
