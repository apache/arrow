#include "gtest/gtest.h"

#include <memory>
#include <vector>

#include "arrow/buffer.h"
#include "arrow/csv/writer.h"
#include "arrow/io/memory.h"
#include "arrow/record_batch.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/type.h"
#include "arrow/type_fwd.h"

namespace arrow {
namespace csv {

struct TestParams {
  std::shared_ptr<RecordBatch> record_batch;
  WriteOptions options;
  std::string expected_output;
};

WriteOptions DefaultTestOptions(bool include_header) {
  WriteOptions options;
  options.batch_size = 5;
  options.include_header = include_header;
  return options;
}

std::vector<TestParams> GenerateTestCases() {
  auto abc_schema = schema({
      {field("a", uint64())},
      {field("b\"", utf8())},
      {field("c ", int32())},
  });
  auto empty_batch =
      RecordBatch::Make(abc_schema, /*num_rows=*/0,
                        {
                            ArrayFromJSON(abc_schema->field(0)->type(), "[]"),
                            ArrayFromJSON(abc_schema->field(1)->type(), "[]"),
                            ArrayFromJSON(abc_schema->field(2)->type(), "[]"),
                        });
  auto populated_batch = RecordBatchFromJSON(abc_schema, R"([{"a": 1, "c ": -1},
                                                         { "a": 1, "b\"": "abc\"efg", "c ": 2324},
                                                         { "b\"": "abcd", "c ": 5467},
                                                         { },
                                                         { "a": 546, "b\"": "", "c ": 517 },
                                                         { "a": 124, "b\"": "a\"\"b\"" }])");
  std::string expected_without_header = std::string("1,,-1") + "\n" +     // line 1
                                        +R"(1,"abc""efg",2324)" + "\n" +  // line 2
                                        R"(,"abcd",5467)" + "\n" +        // line 3
                                        R"(,,)" + "\n" +                  // line 4
                                        R"(546,"",517)" + "\n" +          // line 5
                                        R"(124,"a""""b""",)" + "\n";      // line 6
  std::string expected_header = std::string(R"("a","b""","c ")") + "\n";

  return std::vector<TestParams>{
      {empty_batch, DefaultTestOptions(/*header=*/false), ""},
      {empty_batch, DefaultTestOptions(/*header=*/true), expected_header},
      {populated_batch, DefaultTestOptions(/*header=*/false), expected_without_header},
      {populated_batch, DefaultTestOptions(/*header=*/true),
       expected_header + expected_without_header}};
}

class TestWriteCsv : public ::testing::TestWithParam<TestParams> {};

TEST_P(TestWriteCsv, TestWrite) {
  ASSERT_OK_AND_ASSIGN(std::shared_ptr<io::BufferOutputStream> out,
                       io::BufferOutputStream::Create());
  WriteOptions options = GetParam().options;

  ASSERT_OK(
      WriteCsv(*GetParam().record_batch, options, default_memory_pool(), out.get()));
  ASSERT_OK_AND_ASSIGN(std::shared_ptr<Buffer> buffer, out->Finish());
  EXPECT_EQ(std::string(reinterpret_cast<const char*>(buffer->data()), buffer->size()),
            GetParam().expected_output);
  ASSERT_OK(out->Reset());

  // Batch size shouldn't matter.
  options.batch_size /= 2;
  ASSERT_OK(
      WriteCsv(*GetParam().record_batch, options, default_memory_pool(), out.get()));
  ASSERT_OK_AND_ASSIGN(buffer, out->Finish());
  EXPECT_EQ(std::string(reinterpret_cast<const char*>(buffer->data()), buffer->size()),
            GetParam().expected_output);
  ASSERT_OK(out->Reset());

  // Table and Record batch should work identically.
  ASSERT_OK_AND_ASSIGN(std::shared_ptr<Table> table,
                       Table::FromRecordBatches({GetParam().record_batch}));
  ASSERT_OK(WriteCsv(*table, options, default_memory_pool(), out.get()));
  ASSERT_OK_AND_ASSIGN(buffer, out->Finish());
  EXPECT_EQ(std::string(reinterpret_cast<const char*>(buffer->data()), buffer->size()),
            GetParam().expected_output);
  ASSERT_OK(out->Reset());
}

INSTANTIATE_TEST_SUITE_P(WriteCsvTest, TestWriteCsv,
                         ::testing::ValuesIn(GenerateTestCases()));

}  // namespace csv
}  // namespace arrow
