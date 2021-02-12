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

#include "arrow/adapters/orc/adapter.h"

#include <gtest/gtest.h>

#include <orc/OrcFile.hh>
#include <string>

#include "arrow/adapters/orc/adapter_util.h"
#include "arrow/api.h"
#include "arrow/array.h"
#include "arrow/compute/cast.h"
#include "arrow/io/api.h"
#include "arrow/io/interfaces.h"
#include "arrow/io/memory.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/random.h"
#include "arrow/type.h"
#include "arrow/util/decimal.h"

namespace liborc = orc;

namespace arrow {

constexpr int kDefaultSmallMemStreamSize = 16384 * 5;  // 80KB
constexpr int kDefaultMemStreamSize = 10 * 1024 * 1024;
static constexpr random::SeedType kRandomSeed = 0x0ff1ce;

using ArrayBuilderVector = std::vector<std::shared_ptr<ArrayBuilder>>;
using ArrayBuilderMatrix = std::vector<ArrayBuilderVector>;
using ArrayMatrix = std::vector<ArrayVector>;
using BufferVector = std::vector<std::shared_ptr<Buffer>>;
using BufferMatrix = std::vector<BufferVector>;
using BufferBuilderVector = std::vector<std::shared_ptr<BufferBuilder>>;
using BufferBuilderMatrix = std::vector<BufferBuilderVector>;

class MemoryOutputStream : public liborc::OutputStream {
 public:
  explicit MemoryOutputStream(ssize_t capacity)
      : data_(capacity), name_("MemoryOutputStream"), length_(0) {}

  uint64_t getLength() const override { return length_; }

  uint64_t getNaturalWriteSize() const override { return natural_write_size_; }

  void write(const void* buf, size_t size) override {
    memcpy(data_.data() + length_, buf, size);
    length_ += size;
  }

  const std::string& getName() const override { return name_; }

  const char* getData() const { return data_.data(); }

  void close() override {}

  void reset() { length_ = 0; }

 private:
  std::vector<char> data_;
  std::string name_;
  uint64_t length_, natural_write_size_;
};

template <typename T, typename U>
void randintpartition(int64_t n, T sum, std::vector<U>* out) {
  const int random_seed = 0;
  std::default_random_engine gen(random_seed);
  out->resize(n, static_cast<T>(0));
  T remaining_sum = sum;
  std::generate(out->begin(), out->end() - 1, [&gen, &remaining_sum] {
    std::uniform_int_distribution<T> d(static_cast<T>(0), remaining_sum);
    auto res = d(gen);
    remaining_sum -= res;
    return static_cast<U>(res);
  });
  (*out)[n - 1] += remaining_sum;
  std::random_shuffle(out->begin(), out->end());
}

std::shared_ptr<ChunkedArray> GenerateRandomChunkedArray(
    const std::shared_ptr<DataType>& data_type, int64_t size, int64_t min_num_chunks,
    int64_t max_num_chunks, double null_probability) {
  arrow::random::RandomArrayGenerator rand(kRandomSeed);
  std::vector<int64_t> num_chunks(1, 0);
  std::vector<int64_t> current_size_chunks;
  arrow::randint<int64_t, int64_t>(1, min_num_chunks, max_num_chunks, &num_chunks);
  int64_t current_num_chunks = num_chunks[0];
  ArrayVector arrays(current_num_chunks, nullptr);
  randintpartition(current_num_chunks, size, &current_size_chunks);
  for (int j = 0; j < current_num_chunks; j++) {
    arrays[j] = rand.ArrayOf(data_type, current_size_chunks[j], null_probability);
  }
  return std::make_shared<ChunkedArray>(arrays);
}

std::shared_ptr<Table> GenerateRandomTable(const std::shared_ptr<Schema>& schema,
                                           int64_t size, int64_t min_num_chunks,
                                           int64_t max_num_chunks,
                                           double null_probability) {
  int num_cols = schema->num_fields();
  ChunkedArrayVector cv;
  for (int col = 0; col < num_cols; col++) {
    cv.push_back(GenerateRandomChunkedArray(schema->field(col)->type(), size,
                                            min_num_chunks, max_num_chunks,
                                            null_probability));
  }
  return Table::Make(schema, cv);
}

void AssertTableWriteReadEqual(const std::shared_ptr<Table>& input_table,
                               const std::shared_ptr<Table>& expected_output_table,
                               const int64_t max_size = kDefaultSmallMemStreamSize) {
  std::shared_ptr<io::BufferOutputStream> buffer_output_stream =
      io::BufferOutputStream::Create(max_size).ValueOrDie();
  std::unique_ptr<adapters::orc::ORCFileWriter> writer =
      adapters::orc::ORCFileWriter::Open(*buffer_output_stream).ValueOrDie();
  ARROW_EXPECT_OK(writer->Write(*input_table));
  ARROW_EXPECT_OK(writer->Close());
  std::shared_ptr<Buffer> buffer = buffer_output_stream->Finish().ValueOrDie();
  std::shared_ptr<io::RandomAccessFile> in_stream(new io::BufferReader(buffer));
  std::unique_ptr<adapters::orc::ORCFileReader> reader;
  ARROW_EXPECT_OK(
      adapters::orc::ORCFileReader::Open(in_stream, default_memory_pool(), &reader));
  std::shared_ptr<Table> actual_output_table;
  ARROW_EXPECT_OK(reader->Read(&actual_output_table));
  AssertTablesEqual(*actual_output_table, *expected_output_table, false, false);
}

void SchemaORCWriteReadTest(const std::shared_ptr<Schema>& schema, int64_t size,
                            int64_t min_num_chunks, int64_t max_num_chunks,
                            double null_probability,
                            int64_t max_size = kDefaultSmallMemStreamSize) {
  const std::shared_ptr<Table> table =
      GenerateRandomTable(schema, size, min_num_chunks, max_num_chunks, null_probability);
  AssertTableWriteReadEqual(table, table, max_size);
}

std::unique_ptr<liborc::Writer> CreateWriter(uint64_t stripe_size,
                                             const liborc::Type& type,
                                             liborc::OutputStream* stream) {
  liborc::WriterOptions options;
  options.setStripeSize(stripe_size);
  options.setCompressionBlockSize(1024);
  options.setMemoryPool(liborc::getDefaultPool());
  options.setRowIndexStride(0);
  return liborc::createWriter(type, stream, options);
}

TEST(TestAdapterRead, readIntAndStringFileMultipleStripes) {
  MemoryOutputStream mem_stream(kDefaultMemStreamSize);
  ORC_UNIQUE_PTR<liborc::Type> type(
      liborc::Type::buildTypeFromString("struct<col1:int,col2:string>"));

  constexpr uint64_t stripe_size = 1024;  // 1K
  constexpr uint64_t stripe_count = 10;
  constexpr uint64_t stripe_row_count = 65535;
  constexpr uint64_t reader_batch_size = 1024;

  auto writer = CreateWriter(stripe_size, *type, &mem_stream);
  auto batch = writer->createRowBatch(stripe_row_count);
  auto struct_batch = internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  auto long_batch =
      internal::checked_cast<liborc::LongVectorBatch*>(struct_batch->fields[0]);
  auto str_batch =
      internal::checked_cast<liborc::StringVectorBatch*>(struct_batch->fields[1]);
  int64_t accumulated = 0;

  for (uint64_t j = 0; j < stripe_count; ++j) {
    char data_buffer[327675];
    uint64_t offset = 0;
    for (uint64_t i = 0; i < stripe_row_count; ++i) {
      std::string str_data = std::to_string(accumulated % stripe_row_count);
      long_batch->data[i] = static_cast<int64_t>(accumulated % stripe_row_count);
      str_batch->data[i] = data_buffer + offset;
      str_batch->length[i] = static_cast<int64_t>(str_data.size());
      memcpy(data_buffer + offset, str_data.c_str(), str_data.size());
      accumulated++;
      offset += str_data.size();
    }
    struct_batch->numElements = stripe_row_count;
    long_batch->numElements = stripe_row_count;
    str_batch->numElements = stripe_row_count;

    writer->add(*batch);
  }

  writer->close();

  std::shared_ptr<io::RandomAccessFile> in_stream(new io::BufferReader(
      std::make_shared<Buffer>(reinterpret_cast<const uint8_t*>(mem_stream.getData()),
                               static_cast<int64_t>(mem_stream.getLength()))));

  std::unique_ptr<adapters::orc::ORCFileReader> reader;
  ASSERT_TRUE(
      adapters::orc::ORCFileReader::Open(in_stream, default_memory_pool(), &reader).ok());

  ASSERT_EQ(stripe_row_count * stripe_count, reader->NumberOfRows());
  ASSERT_EQ(stripe_count, reader->NumberOfStripes());
  accumulated = 0;
  std::shared_ptr<RecordBatchReader> stripe_reader;
  EXPECT_TRUE(reader->NextStripeReader(reader_batch_size, &stripe_reader).ok());
  while (stripe_reader) {
    std::shared_ptr<RecordBatch> record_batch;
    EXPECT_TRUE(stripe_reader->ReadNext(&record_batch).ok());
    while (record_batch) {
      auto int32_array = std::static_pointer_cast<Int32Array>(record_batch->column(0));
      auto str_array = std::static_pointer_cast<StringArray>(record_batch->column(1));
      for (int j = 0; j < record_batch->num_rows(); ++j) {
        EXPECT_EQ(accumulated % stripe_row_count, int32_array->Value(j));
        EXPECT_EQ(std::to_string(accumulated % stripe_row_count),
                  str_array->GetString(j));
        accumulated++;
      }
      EXPECT_TRUE(stripe_reader->ReadNext(&record_batch).ok());
    }
    EXPECT_TRUE(reader->NextStripeReader(reader_batch_size, &stripe_reader).ok());
  }

  // test seek operation
  int64_t start_offset = 830;
  EXPECT_TRUE(reader->Seek(stripe_row_count + start_offset).ok());

  EXPECT_TRUE(reader->NextStripeReader(reader_batch_size, &stripe_reader).ok());
  std::shared_ptr<RecordBatch> record_batch;
  EXPECT_TRUE(stripe_reader->ReadNext(&record_batch).ok());
  while (record_batch) {
    auto int32_array = std::dynamic_pointer_cast<Int32Array>(record_batch->column(0));
    auto str_array = std::dynamic_pointer_cast<StringArray>(record_batch->column(1));
    for (int j = 0; j < record_batch->num_rows(); ++j) {
      std::ostringstream os;
      os << start_offset % stripe_row_count;
      EXPECT_EQ(start_offset % stripe_row_count, int32_array->Value(j));
      EXPECT_EQ(os.str(), str_array->GetString(j));
      start_offset++;
    }
    EXPECT_TRUE(stripe_reader->ReadNext(&record_batch).ok());
  }
}

// WriteORC tests

// Trivial
TEST(TestAdapterWriteTrivial, writeZeroRowsNoConversion) {
  std::shared_ptr<Table> table = TableFromJSON(
      schema({field("bool", boolean()), field("int8", int8()), field("int16", int16()),
              field("int32", int32()), field("int64", int64()), field("float", float32()),
              field("double", float64()), field("decimal128nz", decimal(25, 6)),
              field("decimal128z", decimal(32, 0)), field("date32", date32()),
              field("ts3", timestamp(TimeUnit::NANO)), field("string", utf8()),
              field("binary", binary()),
              field("struct", struct_({field("a", utf8()), field("b", int64())})),
              field("list", list(int32())),
              field("lsl", list(struct_({field("lsl0", list(int32()))}))),
              field("map", map(utf8(), utf8()))}),
      {R"([])"});
  AssertTableWriteReadEqual(table, table, kDefaultSmallMemStreamSize / 16);
}
TEST(TestAdapterWriteTrivial, writeChunklessNoConversion) {
  std::shared_ptr<Table> table = TableFromJSON(
      schema({field("bool", boolean()), field("int8", int8()), field("int16", int16()),
              field("int32", int32()), field("int64", int64()), field("float", float32()),
              field("double", float64()), field("decimal128nz", decimal(25, 6)),
              field("decimal128z", decimal(32, 0)), field("date32", date32()),
              field("ts3", timestamp(TimeUnit::NANO)), field("string", utf8()),
              field("binary", binary()),
              field("struct", struct_({field("a", utf8()), field("b", int64())})),
              field("list", list(int32())),
              field("lsl", list(struct_({field("lsl0", list(int32()))}))),
              field("map", map(utf8(), utf8()))}),
      {});
  AssertTableWriteReadEqual(table, table, kDefaultSmallMemStreamSize / 16);
}
TEST(TestAdapterWriteTrivial, writeZeroRowsWithConversion) {
  std::shared_ptr<Table>
      input_table = TableFromJSON(
          schema({field("date64", date64()), field("ts0", timestamp(TimeUnit::SECOND)),
                  field("ts1", timestamp(TimeUnit::MILLI)),
                  field("ts2", timestamp(TimeUnit::MICRO)),
                  field("large_string", large_utf8()),
                  field("large_binary", large_binary()),
                  field("fixed_size_binary0", fixed_size_binary(0)),
                  field("fixed_size_binary", fixed_size_binary(5)),
                  field("large_list", large_list(int32())),
                  field("fixed_size_list", fixed_size_list(int32(), 3))}),
          {R"([])"}),
      expected_output_table = TableFromJSON(
          schema({field("date64", timestamp(TimeUnit::NANO)),
                  field("ts0", timestamp(TimeUnit::NANO)),
                  field("ts1", timestamp(TimeUnit::NANO)),
                  field("ts2", timestamp(TimeUnit::NANO)), field("large_string", utf8()),
                  field("large_binary", binary()), field("fixed_size_binary0", binary()),
                  field("fixed_size_binary", binary()),
                  field("large_list", list(int32())),
                  field("fixed_size_list", list(int32()))}),
          {R"([])"});
  AssertTableWriteReadEqual(input_table, expected_output_table,
                            kDefaultSmallMemStreamSize / 16);
}
TEST(TestAdapterWriteTrivial, writeChunklessWithConversion) {
  std::shared_ptr<Table>
      input_table = TableFromJSON(
          schema({field("date64", date64()), field("ts0", timestamp(TimeUnit::SECOND)),
                  field("ts1", timestamp(TimeUnit::MILLI)),
                  field("ts2", timestamp(TimeUnit::MICRO)),
                  field("large_string", large_utf8()),
                  field("large_binary", large_binary()),
                  field("fixed_size_binary0", fixed_size_binary(0)),
                  field("fixed_size_binary", fixed_size_binary(5)),
                  field("large_list", large_list(int32())),
                  field("fixed_size_list", fixed_size_list(int32(), 3))}),
          {}),
      expected_output_table = TableFromJSON(
          schema({field("date64", timestamp(TimeUnit::NANO)),
                  field("ts0", timestamp(TimeUnit::NANO)),
                  field("ts1", timestamp(TimeUnit::NANO)),
                  field("ts2", timestamp(TimeUnit::NANO)), field("large_string", utf8()),
                  field("large_binary", binary()), field("fixed_size_binary0", binary()),
                  field("fixed_size_binary", binary()),
                  field("large_list", list(int32())),
                  field("fixed_size_list", list(int32()))}),
          {});
  AssertTableWriteReadEqual(input_table, expected_output_table,
                            kDefaultSmallMemStreamSize / 16);
}

// General
TEST(TestAdapterWriteGeneral, writeCombined) {
  std::shared_ptr<Schema> table_schema =
      schema({field("bool", boolean()), field("int8", int8()), field("int16", int16()),
              field("int32", int32()), field("int64", int64()), field("float", float32()),
              field("double", float64()), field("decimal128nz", decimal(33, 4)),
              field("decimal128z", decimal(35, 0)), field("date32", date32()),
              field("ts3", timestamp(TimeUnit::NANO)), field("string", utf8()),
              field("binary", binary())});
  SchemaORCWriteReadTest(table_schema, 10030, 1, 10, 0, kDefaultSmallMemStreamSize * 5);
  SchemaORCWriteReadTest(table_schema, 9405, 5, 20, 0.6, kDefaultSmallMemStreamSize * 5);
  SchemaORCWriteReadTest(table_schema, 4006, 10, 40, 1);
}

// Converts
// Since Arrow has way more types than ORC type conversions are unavoidable
TEST(TestAdapterWriteConvert, writeAllNulls) {
  std::vector<std::shared_ptr<Field>> input_fields{
      field("date64", date64()),
      field("ts0", timestamp(TimeUnit::SECOND)),
      field("ts1", timestamp(TimeUnit::MILLI)),
      field("ts2", timestamp(TimeUnit::MICRO)),
      field("large_string", large_utf8()),
      field("large_binary", large_binary()),
      field("fixed_size_binary0", fixed_size_binary(0)),
      field("fixed_size_binary", fixed_size_binary(5))},
      output_fields{field("date64", timestamp(TimeUnit::NANO)),
                    field("ts0", timestamp(TimeUnit::NANO)),
                    field("ts1", timestamp(TimeUnit::NANO)),
                    field("ts2", timestamp(TimeUnit::NANO)),
                    field("large_string", utf8()),
                    field("large_binary", binary()),
                    field("fixed_size_binary0", binary()),
                    field("fixed_size_binary", binary())};
  std::shared_ptr<Schema> input_schema = std::make_shared<Schema>(input_fields),
                          output_schema = std::make_shared<Schema>(output_fields);

  int64_t num_rows = 10000;
  int64_t numCols = input_fields.size();

  ArrayBuilderMatrix buildersIn(numCols, ArrayBuilderVector(5, NULLPTR));
  ArrayBuilderVector buildersOut(numCols, NULLPTR);

  for (int i = 0; i < 5; i++) {
    buildersIn[0][i] =
        std::static_pointer_cast<ArrayBuilder>(std::make_shared<Date64Builder>());
    buildersIn[1][i] =
        std::static_pointer_cast<ArrayBuilder>(std::make_shared<TimestampBuilder>(
            timestamp(TimeUnit::SECOND), default_memory_pool()));
    buildersIn[2][i] =
        std::static_pointer_cast<ArrayBuilder>(std::make_shared<TimestampBuilder>(
            timestamp(TimeUnit::MILLI), default_memory_pool()));
    buildersIn[3][i] =
        std::static_pointer_cast<ArrayBuilder>(std::make_shared<TimestampBuilder>(
            timestamp(TimeUnit::MICRO), default_memory_pool()));
    buildersIn[4][i] =
        std::static_pointer_cast<ArrayBuilder>(std::make_shared<LargeStringBuilder>());
    buildersIn[5][i] =
        std::static_pointer_cast<ArrayBuilder>(std::make_shared<LargeBinaryBuilder>());
    buildersIn[6][i] = std::static_pointer_cast<ArrayBuilder>(
        std::make_shared<FixedSizeBinaryBuilder>(fixed_size_binary(0)));
    buildersIn[7][i] = std::static_pointer_cast<ArrayBuilder>(
        std::make_shared<FixedSizeBinaryBuilder>(fixed_size_binary(5)));
  }

  for (int col = 0; col < 4; col++) {
    buildersOut[col] =
        std::static_pointer_cast<ArrayBuilder>(std::make_shared<TimestampBuilder>(
            timestamp(TimeUnit::NANO), default_memory_pool()));
  }
  buildersOut[4] =
      std::static_pointer_cast<ArrayBuilder>(std::make_shared<StringBuilder>());
  for (int col = 5; col < 8; col++) {
    buildersOut[col] =
        std::static_pointer_cast<ArrayBuilder>(std::make_shared<BinaryBuilder>());
  }

  for (int i = 0; i < num_rows; i++) {
    int chunk = i < (num_rows / 2) ? 1 : 3;
    for (int col = 0; col < numCols; col++) {
      ARROW_EXPECT_OK(buildersIn[col][chunk]->AppendNull());
      ARROW_EXPECT_OK(buildersOut[col]->AppendNull());
    }
  }

  ArrayMatrix arraysIn(numCols, ArrayVector(5, NULLPTR));
  ArrayVector arraysOut(numCols, NULLPTR);

  ChunkedArrayVector cvIn, cvOut;
  cvIn.reserve(numCols);
  cvOut.reserve(numCols);

  for (int col = 0; col < numCols; col++) {
    for (int i = 0; i < 5; i++) {
      ARROW_EXPECT_OK(buildersIn[col][i]->Finish(&arraysIn[col][i]));
    }
    ARROW_EXPECT_OK(buildersOut[col]->Finish(&arraysOut[col]));
    cvIn.push_back(std::make_shared<ChunkedArray>(arraysIn[col]));
    cvOut.push_back(std::make_shared<ChunkedArray>(arraysOut[col]));
  }

  std::shared_ptr<Table> input_table = Table::Make(input_schema, cvIn),
                         expected_output_table = Table::Make(output_schema, cvOut);
  AssertTableWriteReadEqual(input_table, expected_output_table);
}
TEST(TestAdapterWriteConvert, writeNoNulls) {
  std::vector<std::shared_ptr<Field>> input_fields{
      field("date64", date64()),
      field("ts0", timestamp(TimeUnit::SECOND)),
      field("ts1", timestamp(TimeUnit::MILLI)),
      field("ts2", timestamp(TimeUnit::MICRO)),
      field("large_string", large_utf8()),
      field("large_binary", large_binary()),
      field("fixed_size_binary0", fixed_size_binary(0)),
      field("fixed_size_binary", fixed_size_binary(2))},
      output_fields{field("date64", timestamp(TimeUnit::NANO)),
                    field("ts0", timestamp(TimeUnit::NANO)),
                    field("ts1", timestamp(TimeUnit::NANO)),
                    field("ts2", timestamp(TimeUnit::NANO)),
                    field("large_string", utf8()),
                    field("large_binary", binary()),
                    field("fixed_size_binary0", binary()),
                    field("fixed_size_binary", binary())};
  std::shared_ptr<Schema> input_schema = std::make_shared<Schema>(input_fields),
                          output_schema = std::make_shared<Schema>(output_fields);

  int64_t num_rows = 10000;
  int64_t numCols = input_fields.size();

  ArrayBuilderMatrix buildersIn(numCols, ArrayBuilderVector(5, NULLPTR));
  ArrayBuilderVector buildersOut(numCols, NULLPTR);

  for (int i = 0; i < 5; i++) {
    buildersIn[0][i] =
        std::static_pointer_cast<ArrayBuilder>(std::make_shared<Date64Builder>());
    buildersIn[1][i] =
        std::static_pointer_cast<ArrayBuilder>(std::make_shared<TimestampBuilder>(
            timestamp(TimeUnit::SECOND), default_memory_pool()));
    buildersIn[2][i] =
        std::static_pointer_cast<ArrayBuilder>(std::make_shared<TimestampBuilder>(
            timestamp(TimeUnit::MILLI), default_memory_pool()));
    buildersIn[3][i] =
        std::static_pointer_cast<ArrayBuilder>(std::make_shared<TimestampBuilder>(
            timestamp(TimeUnit::MICRO), default_memory_pool()));
    buildersIn[4][i] =
        std::static_pointer_cast<ArrayBuilder>(std::make_shared<LargeStringBuilder>());
    buildersIn[5][i] =
        std::static_pointer_cast<ArrayBuilder>(std::make_shared<LargeBinaryBuilder>());
    buildersIn[6][i] = std::static_pointer_cast<ArrayBuilder>(
        std::make_shared<FixedSizeBinaryBuilder>(fixed_size_binary(0)));
    buildersIn[7][i] = std::static_pointer_cast<ArrayBuilder>(
        std::make_shared<FixedSizeBinaryBuilder>(fixed_size_binary(2)));
  }

  for (int col = 0; col < 4; col++) {
    buildersOut[col] =
        std::static_pointer_cast<ArrayBuilder>(std::make_shared<TimestampBuilder>(
            timestamp(TimeUnit::NANO), default_memory_pool()));
  }
  buildersOut[4] =
      std::static_pointer_cast<ArrayBuilder>(std::make_shared<StringBuilder>());
  for (int col = 5; col < 8; col++) {
    buildersOut[col] =
        std::static_pointer_cast<ArrayBuilder>(std::make_shared<BinaryBuilder>());
  }

  char bin1[2], bin2[3], bin3[] = "", string_[12];
  std::string str;

  for (int64_t i = 0; i < num_rows; i++) {
    int chunk = i < (num_rows / 2) ? 1 : 3;
    bin1[0] = i % 256;
    bin1[1] = (i + 1) % 256;
    bin2[0] = (2 * i) % 256;
    bin2[1] = (2 * i + 1) % 256;
    bin2[2] = (i - 1) % 256;
    str = "Arrow " + std::to_string(2 * i);
    snprintf(string_, sizeof(string_), "%s", str.c_str());
    ARROW_EXPECT_OK(std::static_pointer_cast<Date64Builder>(buildersIn[0][chunk])
                        ->Append(INT64_C(1605758461555) + i));
    ARROW_EXPECT_OK(std::static_pointer_cast<TimestampBuilder>(buildersIn[1][chunk])
                        ->Append(INT64_C(1605758461) + i));
    ARROW_EXPECT_OK(std::static_pointer_cast<TimestampBuilder>(buildersIn[2][chunk])
                        ->Append(INT64_C(1605758461000) + i));
    ARROW_EXPECT_OK(std::static_pointer_cast<TimestampBuilder>(buildersIn[3][chunk])
                        ->Append(INT64_C(1605758461000111) + i));
    ARROW_EXPECT_OK(std::static_pointer_cast<LargeStringBuilder>(buildersIn[4][chunk])
                        ->Append(string_));
    ARROW_EXPECT_OK(std::static_pointer_cast<LargeBinaryBuilder>(buildersIn[5][chunk])
                        ->Append(bin2, 3));
    ARROW_EXPECT_OK(std::static_pointer_cast<FixedSizeBinaryBuilder>(buildersIn[6][chunk])
                        ->Append(bin3));
    ARROW_EXPECT_OK(std::static_pointer_cast<FixedSizeBinaryBuilder>(buildersIn[7][chunk])
                        ->Append(bin1));
    ARROW_EXPECT_OK(std::static_pointer_cast<TimestampBuilder>(buildersOut[0])
                        ->Append(INT64_C(1605758461555000000) + INT64_C(1000000) * i));
    ARROW_EXPECT_OK(std::static_pointer_cast<TimestampBuilder>(buildersOut[1])
                        ->Append(INT64_C(1605758461000000000) + INT64_C(1000000000) * i));
    ARROW_EXPECT_OK(std::static_pointer_cast<TimestampBuilder>(buildersOut[2])
                        ->Append(INT64_C(1605758461000000000) + INT64_C(1000000) * i));
    ARROW_EXPECT_OK(std::static_pointer_cast<TimestampBuilder>(buildersOut[3])
                        ->Append(INT64_C(1605758461000111000) + INT64_C(1000) * i));
    ARROW_EXPECT_OK(
        std::static_pointer_cast<StringBuilder>(buildersOut[4])->Append(string_));
    ARROW_EXPECT_OK(
        std::static_pointer_cast<BinaryBuilder>(buildersOut[5])->Append(bin2, 3));
    ARROW_EXPECT_OK(
        std::static_pointer_cast<BinaryBuilder>(buildersOut[6])->Append(bin3, 0));
    ARROW_EXPECT_OK(
        std::static_pointer_cast<BinaryBuilder>(buildersOut[7])->Append(bin1, 2));
  }

  ArrayMatrix arraysIn(numCols, ArrayVector(5, NULLPTR));
  ArrayVector arraysOut(numCols, NULLPTR);

  ChunkedArrayVector cvIn, cvOut;
  cvIn.reserve(numCols);
  cvOut.reserve(numCols);

  for (int col = 0; col < numCols; col++) {
    for (int i = 0; i < 5; i++) {
      ARROW_EXPECT_OK(buildersIn[col][i]->Finish(&arraysIn[col][i]));
    }
    ARROW_EXPECT_OK(buildersOut[col]->Finish(&arraysOut[col]));
    cvIn.push_back(std::make_shared<ChunkedArray>(arraysIn[col]));
    cvOut.push_back(std::make_shared<ChunkedArray>(arraysOut[col]));
  }

  std::shared_ptr<Table> input_table = Table::Make(input_schema, cvIn),
                         expected_output_table = Table::Make(output_schema, cvOut);
  AssertTableWriteReadEqual(input_table, expected_output_table);
}
TEST(TestAdapterWriteConvert, writeMixed) {
  std::vector<std::shared_ptr<Field>> input_fields{
      field("date64", date64()),
      field("ts0", timestamp(TimeUnit::SECOND)),
      field("ts1", timestamp(TimeUnit::MILLI)),
      field("ts2", timestamp(TimeUnit::MICRO)),
      field("large_string", large_utf8()),
      field("large_binary", large_binary()),
      field("fixed_size_binary0", fixed_size_binary(0)),
      field("fixed_size_binary", fixed_size_binary(3))},
      output_fields{field("date64", timestamp(TimeUnit::NANO)),
                    field("ts0", timestamp(TimeUnit::NANO)),
                    field("ts1", timestamp(TimeUnit::NANO)),
                    field("ts2", timestamp(TimeUnit::NANO)),
                    field("large_string", utf8()),
                    field("large_binary", binary()),
                    field("fixed_size_binary0", binary()),
                    field("fixed_size_binary", binary())};
  std::shared_ptr<Schema> input_schema = std::make_shared<Schema>(input_fields),
                          output_schema = std::make_shared<Schema>(output_fields);

  int64_t num_rows = 10000;
  int64_t numCols = input_fields.size();

  ArrayBuilderMatrix buildersIn(numCols, ArrayBuilderVector(5, NULLPTR));
  ArrayBuilderVector buildersOut(numCols, NULLPTR);

  for (int i = 0; i < 5; i++) {
    buildersIn[0][i] =
        std::static_pointer_cast<ArrayBuilder>(std::make_shared<Date64Builder>());
    buildersIn[1][i] =
        std::static_pointer_cast<ArrayBuilder>(std::make_shared<TimestampBuilder>(
            timestamp(TimeUnit::SECOND), default_memory_pool()));
    buildersIn[2][i] =
        std::static_pointer_cast<ArrayBuilder>(std::make_shared<TimestampBuilder>(
            timestamp(TimeUnit::MILLI), default_memory_pool()));
    buildersIn[3][i] =
        std::static_pointer_cast<ArrayBuilder>(std::make_shared<TimestampBuilder>(
            timestamp(TimeUnit::MICRO), default_memory_pool()));
    buildersIn[4][i] =
        std::static_pointer_cast<ArrayBuilder>(std::make_shared<LargeStringBuilder>());
    buildersIn[5][i] =
        std::static_pointer_cast<ArrayBuilder>(std::make_shared<LargeBinaryBuilder>());
    buildersIn[6][i] = std::static_pointer_cast<ArrayBuilder>(
        std::make_shared<FixedSizeBinaryBuilder>(fixed_size_binary(0)));
    buildersIn[7][i] = std::static_pointer_cast<ArrayBuilder>(
        std::make_shared<FixedSizeBinaryBuilder>(fixed_size_binary(3)));
  }

  for (int col = 0; col < 4; col++) {
    buildersOut[col] =
        std::static_pointer_cast<ArrayBuilder>(std::make_shared<TimestampBuilder>(
            timestamp(TimeUnit::NANO), default_memory_pool()));
  }
  buildersOut[4] =
      std::static_pointer_cast<ArrayBuilder>(std::make_shared<StringBuilder>());
  for (int col = 5; col < 8; col++) {
    buildersOut[col] =
        std::static_pointer_cast<ArrayBuilder>(std::make_shared<BinaryBuilder>());
  }

  char bin1[3], bin2[4], bin3[] = "", string_[13];
  std::string str;

  for (int64_t i = 0; i < num_rows; i++) {
    int chunk = i < (num_rows / 2) ? 1 : 3;
    if (i % 2) {
      str = "Arrow " + std::to_string(-4 * i + 8);
      snprintf(string_, sizeof(string_), "%s", str.c_str());
      ARROW_EXPECT_OK(std::static_pointer_cast<Date64Builder>(buildersIn[0][chunk])
                          ->Append(INT64_C(1605758461555) + 3 * i));
      ARROW_EXPECT_OK(
          std::static_pointer_cast<TimestampBuilder>(buildersIn[1][chunk])->AppendNull());
      ARROW_EXPECT_OK(std::static_pointer_cast<TimestampBuilder>(buildersIn[2][chunk])
                          ->Append(INT64_C(1605758461000) - 14 * i));
      ARROW_EXPECT_OK(
          std::static_pointer_cast<TimestampBuilder>(buildersIn[3][chunk])->AppendNull());
      ARROW_EXPECT_OK(std::static_pointer_cast<LargeStringBuilder>(buildersIn[4][chunk])
                          ->Append(string_));
      ARROW_EXPECT_OK(std::static_pointer_cast<LargeBinaryBuilder>(buildersIn[5][chunk])
                          ->AppendNull());
      ARROW_EXPECT_OK(
          std::static_pointer_cast<FixedSizeBinaryBuilder>(buildersIn[6][chunk])
              ->Append(bin3));
      ARROW_EXPECT_OK(
          std::static_pointer_cast<FixedSizeBinaryBuilder>(buildersIn[7][chunk])
              ->AppendNull());
      ARROW_EXPECT_OK(std::static_pointer_cast<TimestampBuilder>(buildersOut[0])
                          ->Append(INT64_C(1605758461555000000) + INT64_C(3000000) * i));
      ARROW_EXPECT_OK(
          std::static_pointer_cast<TimestampBuilder>(buildersOut[1])->AppendNull());
      ARROW_EXPECT_OK(std::static_pointer_cast<TimestampBuilder>(buildersOut[2])
                          ->Append(INT64_C(1605758461000000000) - INT64_C(14000000) * i));
      ARROW_EXPECT_OK(
          std::static_pointer_cast<TimestampBuilder>(buildersOut[3])->AppendNull());
      ARROW_EXPECT_OK(std::static_pointer_cast<StringBuilder>(buildersOut[4])
                          ->Append("Arrow " + std::to_string(-4 * i + 8)));
      ARROW_EXPECT_OK(
          std::static_pointer_cast<BinaryBuilder>(buildersOut[5])->AppendNull());
      ARROW_EXPECT_OK(
          std::static_pointer_cast<BinaryBuilder>(buildersOut[6])->Append(bin3, 0));
      ARROW_EXPECT_OK(
          std::static_pointer_cast<BinaryBuilder>(buildersOut[7])->AppendNull());
    } else {
      bin1[0] = i % 256;
      bin1[1] = (i + 1) % 256;
      bin1[2] = (i - 1) % 256;
      bin2[0] = (29 * i - 192) % 256;
      bin2[1] = (2 * i + 1) % 256;
      bin2[2] = (4 * i + 103) % 256;
      bin2[3] = (17 * i + 122) % 256;
      ARROW_EXPECT_OK(
          std::static_pointer_cast<Date64Builder>(buildersIn[0][chunk])->AppendNull());
      ARROW_EXPECT_OK(std::static_pointer_cast<TimestampBuilder>(buildersIn[1][chunk])
                          ->Append(INT64_C(1605758461) + INT64_C(61) * i));
      ARROW_EXPECT_OK(
          std::static_pointer_cast<TimestampBuilder>(buildersIn[2][chunk])->AppendNull());
      ARROW_EXPECT_OK(std::static_pointer_cast<TimestampBuilder>(buildersIn[3][chunk])
                          ->Append(INT64_C(1605758461000111) + INT64_C(1021) * i));
      ARROW_EXPECT_OK(std::static_pointer_cast<LargeStringBuilder>(buildersIn[4][chunk])
                          ->AppendNull());
      ARROW_EXPECT_OK(std::static_pointer_cast<LargeBinaryBuilder>(buildersIn[5][chunk])
                          ->Append(bin2, 4));
      ARROW_EXPECT_OK(
          std::static_pointer_cast<FixedSizeBinaryBuilder>(buildersIn[6][chunk])
              ->AppendNull());
      ARROW_EXPECT_OK(
          std::static_pointer_cast<FixedSizeBinaryBuilder>(buildersIn[7][chunk])
              ->Append(bin1));
      ARROW_EXPECT_OK(
          std::static_pointer_cast<TimestampBuilder>(buildersOut[0])->AppendNull());
      ARROW_EXPECT_OK(
          std::static_pointer_cast<TimestampBuilder>(buildersOut[1])
              ->Append(INT64_C(1605758461000000000) + INT64_C(61000000000) * i));
      ARROW_EXPECT_OK(
          std::static_pointer_cast<TimestampBuilder>(buildersOut[2])->AppendNull());
      ARROW_EXPECT_OK(std::static_pointer_cast<TimestampBuilder>(buildersOut[3])
                          ->Append(INT64_C(1605758461000111000) + INT64_C(1021000) * i));
      ARROW_EXPECT_OK(
          std::static_pointer_cast<StringBuilder>(buildersOut[4])->AppendNull());
      ARROW_EXPECT_OK(
          std::static_pointer_cast<BinaryBuilder>(buildersOut[5])->Append(bin2, 4));
      ARROW_EXPECT_OK(
          std::static_pointer_cast<BinaryBuilder>(buildersOut[6])->AppendNull());
      ARROW_EXPECT_OK(
          std::static_pointer_cast<BinaryBuilder>(buildersOut[7])->Append(bin1, 3));
    }
  }

  ArrayMatrix arraysIn(numCols, ArrayVector(5, NULLPTR));
  ArrayVector arraysOut(numCols, NULLPTR);

  ChunkedArrayVector cvIn, cvOut;
  cvIn.reserve(numCols);
  cvOut.reserve(numCols);

  for (int col = 0; col < numCols; col++) {
    for (int i = 0; i < 5; i++) {
      ARROW_EXPECT_OK(buildersIn[col][i]->Finish(&arraysIn[col][i]));
    }
    ARROW_EXPECT_OK(buildersOut[col]->Finish(&arraysOut[col]));
    cvIn.push_back(std::make_shared<ChunkedArray>(arraysIn[col]));
    cvOut.push_back(std::make_shared<ChunkedArray>(arraysOut[col]));
  }

  std::shared_ptr<Table> input_table = Table::Make(input_schema, cvIn),
                         expected_output_table = Table::Make(output_schema, cvOut);
  AssertTableWriteReadEqual(input_table, expected_output_table);
}

// Nested types
TEST(TestAdapterWriteNested, WriteList) {
  std::shared_ptr<Schema> table_schema = schema({field("list", list(int32()))});
  int64_t num_rows = 10000;
  arrow::random::RandomArrayGenerator rand(kRandomSeed);
  auto value_array = rand.ArrayOf(int32(), 125 * num_rows, 0);
  std::shared_ptr<Array> array = rand.List(*value_array, num_rows, 1);
  std::shared_ptr<ChunkedArray> chunked_array = std::make_shared<ChunkedArray>(array);
  std::shared_ptr<Table> table = Table::Make(table_schema, {chunked_array});
  AssertTableWriteReadEqual(table, table, kDefaultSmallMemStreamSize * 100);
}
TEST(TestAdapterWriteNested, WriteLargeList) {
  std::shared_ptr<Schema> input_schema = schema({field("list", large_list(int32()))}),
                          output_schema = schema({field("list", list(int32()))});
  int64_t num_rows = 10000;
  arrow::random::RandomArrayGenerator rand(kRandomSeed);
  auto value_array = rand.ArrayOf(int32(), 5 * num_rows, 0.5);
  auto output_offsets = rand.Offsets(num_rows + 1, 0, 5 * num_rows, 0.6, false);
  auto input_offsets = arrow::compute::Cast(*output_offsets, int64()).ValueOrDie();
  std::shared_ptr<Array>
      input_array =
          arrow::LargeListArray::FromArrays(*input_offsets, *value_array).ValueOrDie(),
      output_array =
          arrow::ListArray::FromArrays(*output_offsets, *value_array).ValueOrDie();
  auto input_chunked_array = std::make_shared<ChunkedArray>(input_array),
       output_chunked_array = std::make_shared<ChunkedArray>(output_array);
  std::shared_ptr<Table> input_table = Table::Make(input_schema, {input_chunked_array}),
                         output_table =
                             Table::Make(output_schema, {output_chunked_array});
  AssertTableWriteReadEqual(input_table, output_table, kDefaultSmallMemStreamSize * 10);
}
TEST(TestAdapterWriteNested, WriteFixedSizeList) {
  std::shared_ptr<Schema> input_schema =
                              schema({field("list", fixed_size_list(int32(), 3))}),
                          output_schema = schema({field("list", list(int32()))});
  int64_t num_rows = 10000;
  arrow::random::RandomArrayGenerator rand(kRandomSeed);
  std::shared_ptr<Array> value_array = rand.ArrayOf(int32(), 3 * num_rows, 0.5);
  std::shared_ptr<Buffer> bitmap = rand.NullBitmap(num_rows, 0.4);
  int32_t offsets[num_rows + 1];
  BufferBuilder builder;
  ARROW_EXPECT_OK(builder.Resize(4 * num_rows + 4));
  for (int32_t i = 0; i <= num_rows; i++) {
    offsets[i] = 3 * i;
  }
  ARROW_EXPECT_OK(builder.Append(offsets, 4 * num_rows + 4));
  std::shared_ptr<Buffer> buffer;
  ARROW_EXPECT_OK(builder.Finish(&buffer));
  std::shared_ptr<Array> input_array = std::make_shared<FixedSizeListArray>(
                             input_schema->field(0)->type(), num_rows, value_array,
                             bitmap),
                         output_array = std::make_shared<ListArray>(
                             output_schema->field(0)->type(), num_rows, buffer,
                             value_array, bitmap);
  auto input_chunked_array = std::make_shared<ChunkedArray>(input_array),
       output_chunked_array = std::make_shared<ChunkedArray>(output_array);
  std::shared_ptr<Table> input_table = Table::Make(input_schema, {input_chunked_array}),
                         output_table =
                             Table::Make(output_schema, {output_chunked_array});
  AssertTableWriteReadEqual(input_table, output_table, kDefaultSmallMemStreamSize * 10);
}
TEST(TestAdapterWriteNested, WriteMap) {
  std::shared_ptr<Schema> table_schema = schema({field("map", map(int32(), int32()))});
  int64_t num_rows = 10000;
  arrow::random::RandomArrayGenerator rand(kRandomSeed);
  auto map_type = std::static_pointer_cast<MapType>(table_schema->field(0)->type());
  auto key_array = rand.ArrayOf(map_type->key_type(), 20 * num_rows, 0);
  auto item_array = rand.ArrayOf(map_type->item_type(), 20 * num_rows, 0.8);
  std::shared_ptr<Array> array = rand.Map(key_array, item_array, num_rows, 0.9);
  std::shared_ptr<ChunkedArray> chunked_array = std::make_shared<ChunkedArray>(array);
  std::shared_ptr<Table> table = Table::Make(table_schema, {chunked_array});
  AssertTableWriteReadEqual(table, table, kDefaultSmallMemStreamSize * 25);
}
TEST(TestAdapterWriteNested, WriteStruct) {
  std::vector<std::shared_ptr<Field>> subsubfields{
      field("bool", boolean()),
      field("int8", int8()),
      field("int16", int16()),
      field("int32", int32()),
      field("int64", int64()),
      field("decimal128nz", decimal(38, 6)),
      field("decimal128z", decimal(38, 0)),
      field("date32", date32()),
      field("ts3", timestamp(TimeUnit::NANO)),
      field("string", utf8()),
      field("binary", binary())};
  std::shared_ptr<Schema> table_schema =
      schema({field("struct", struct_({field("struct2", struct_(subsubfields))}))});
  int64_t num_rows = 10000;
  int num_subsubcols = subsubfields.size();
  arrow::random::RandomArrayGenerator rand(kRandomSeed);
  ArrayVector av00(num_subsubcols), av0(1);
  for (int i = 0; i < num_subsubcols; i++) {
    av00[i] = rand.ArrayOf(subsubfields[i]->type(), num_rows, 0.9);
  }
  std::shared_ptr<Buffer> bitmap0 = rand.NullBitmap(num_rows, 0.8);
  av0[0] = std::make_shared<StructArray>(table_schema->field(0)->type()->field(0)->type(),
                                         num_rows, av00, bitmap0);
  std::shared_ptr<Buffer> bitmap = rand.NullBitmap(num_rows, 0.7);
  std::shared_ptr<Array> array = std::make_shared<StructArray>(
      table_schema->field(0)->type(), num_rows, av0, bitmap);
  std::shared_ptr<ChunkedArray> chunked_array = std::make_shared<ChunkedArray>(array);
  std::shared_ptr<Table> table = Table::Make(table_schema, {chunked_array});
  AssertTableWriteReadEqual(table, table, kDefaultSmallMemStreamSize * 10);
}
TEST(TestAdapterWriteNested, WriteListOfStruct) {
  std::shared_ptr<Schema> table_schema =
      schema({field("ls", list(struct_({field("a", int32())})))});
  int64_t num_rows = 1;
  arrow::random::RandomArrayGenerator rand(kRandomSeed);
  ArrayVector av30(1);
  av30[0] = rand.ArrayOf(int32(), 100 * num_rows, 0.9);
  std::shared_ptr<Buffer> bitmap30 = rand.NullBitmap(100 * num_rows, 0.8);
  std::shared_ptr<Array> value_array3 = std::make_shared<StructArray>(
      struct_({field("a", int32())}), 5 * num_rows, av30, bitmap30);
  std::shared_ptr<Array> array3 = rand.List(*value_array3, num_rows, 1);
  std::shared_ptr<ChunkedArray> chunked_array3 = std::make_shared<ChunkedArray>(array3);
  std::shared_ptr<Table> table = Table::Make(table_schema, {chunked_array3});
  AssertTableWriteReadEqual(table, table, kDefaultSmallMemStreamSize * 100);
}
}  // namespace arrow
