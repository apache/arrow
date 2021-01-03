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
#include "arrow/io/api.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/type.h"
#include "arrow/util/decimal.h"

namespace liborc = orc;

namespace arrow {

constexpr int DEFAULT_MEM_STREAM_SIZE = 10 * 1024 * 1024;
constexpr int DEFAULT_SMALL_MEM_STREAM_SIZE = 16384 * 5;  // 80KB

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

class ORCMemWriter {
 public:
  Status Open(const std::shared_ptr<Schema>& schema,
              ORC_UNIQUE_PTR<liborc::OutputStream>& outStream,
              const std::shared_ptr<adapters::orc::ORCWriterOptions>& options) {
    orc_options_ = std::make_shared<liborc::WriterOptions>();
    outStream_ = std::move(outStream);
    ARROW_EXPECT_OK(adapters::orc::GetORCType(schema.get(), &orcSchema_));
    try {
      writer_ = createWriter(*orcSchema_, outStream_.get(), *orc_options_);
    } catch (const liborc::ParseError& e) {
      return Status::IOError(e.what());
    }
    schema_ = schema;
    options_ = options;
    num_cols_ = schema->num_fields();
    return Status::OK();
  }

  Status Write(const std::shared_ptr<Table> table) {
    int64_t numRows = table->num_rows();
    int64_t batch_size = static_cast<int64_t>(options_->get_batch_size());
    std::vector<int64_t> arrowIndexOffset(num_cols_, 0);
    std::vector<int> arrowChunkOffset(num_cols_, 0);
    ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer_->createRowBatch(batch_size);
    liborc::StructVectorBatch* root =
        internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
    std::vector<liborc::ColumnVectorBatch*> fields = root->fields;
    while (numRows > 0) {
      for (int i = 0; i < num_cols_; i++) {
        ARROW_EXPECT_OK(adapters::orc::FillBatch(
            schema_->field(i)->type().get(), fields[i], arrowIndexOffset[i],
            arrowChunkOffset[i], batch_size, table->column(i).get()));
      }
      root->numElements = fields[0]->numElements;
      writer_->add(*batch);
      batch->clear();
      numRows -= batch_size;
    }
    writer_->close();
    return Status::OK();
  }

  liborc::OutputStream* ReleaseOutStream() { return outStream_.release(); }

  ORC_UNIQUE_PTR<liborc::Writer> writer_;
  std::shared_ptr<adapters::orc::ORCWriterOptions> options_;
  std::shared_ptr<liborc::WriterOptions> orc_options_;
  std::shared_ptr<Schema> schema_;
  ORC_UNIQUE_PTR<liborc::OutputStream> outStream_;
  ORC_UNIQUE_PTR<liborc::Type> orcSchema_;
  int num_cols_;
};

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
  MemoryOutputStream mem_stream(DEFAULT_MEM_STREAM_SIZE);
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

// General
TEST(TestAdapterWriteGeneral, writeZeroRows) {
  std::vector<std::shared_ptr<Field>> xFields{field("bool", boolean()),
                                              field("int8", int8()),
                                              field("int16", int16()),
                                              field("int32", int32()),
                                              field("int64", int64()),
                                              field("float", float32()),
                                              field("double", float64()),
                                              field("decimal128nz", decimal(25, 6)),
                                              field("decimal128z", decimal(32, 0)),
                                              field("date32", date32()),
                                              field("ts3", timestamp(TimeUnit::NANO)),
                                              field("string", utf8()),
                                              field("binary", binary())};
  std::shared_ptr<Schema> sharedPtrSchema = std::make_shared<Schema>(xFields);

  int64_t numRows = 0;
  int64_t numCols = xFields.size();
  uint64_t batchSize = 1078;

  ArrayBuilderVector builders(numCols, NULLPTR);
  builders[0] =
      std::static_pointer_cast<ArrayBuilder>(std::make_shared<BooleanBuilder>());
  builders[1] = std::static_pointer_cast<ArrayBuilder>(std::make_shared<Int8Builder>());
  builders[2] = std::static_pointer_cast<ArrayBuilder>(std::make_shared<Int16Builder>());
  builders[3] = std::static_pointer_cast<ArrayBuilder>(std::make_shared<Int32Builder>());
  builders[4] = std::static_pointer_cast<ArrayBuilder>(std::make_shared<Int64Builder>());
  builders[5] = std::static_pointer_cast<ArrayBuilder>(std::make_shared<FloatBuilder>());
  builders[6] = std::static_pointer_cast<ArrayBuilder>(std::make_shared<DoubleBuilder>());
  builders[7] = std::static_pointer_cast<ArrayBuilder>(
      std::make_shared<Decimal128Builder>(decimal(25, 6)));
  builders[8] = std::static_pointer_cast<ArrayBuilder>(
      std::make_shared<Decimal128Builder>(decimal(32, 0)));
  builders[9] = std::static_pointer_cast<ArrayBuilder>(std::make_shared<Date32Builder>());
  builders[10] =
      std::static_pointer_cast<ArrayBuilder>(std::make_shared<TimestampBuilder>(
          timestamp(TimeUnit::NANO), default_memory_pool()));
  builders[11] =
      std::static_pointer_cast<ArrayBuilder>(std::make_shared<StringBuilder>());
  builders[12] =
      std::static_pointer_cast<ArrayBuilder>(std::make_shared<BinaryBuilder>());
  ArrayVector arrays(numCols, NULLPTR);
  ChunkedArrayVector cv;
  cv.reserve(numCols);

  for (int col = 0; col < numCols; col++) {
    ARROW_EXPECT_OK(builders[col]->Finish(&arrays[col]));
    cv.push_back(std::make_shared<ChunkedArray>(arrays[col]));
  }

  std::shared_ptr<Table> table = Table::Make(sharedPtrSchema, cv);

  std::shared_ptr<adapters::orc::ORCWriterOptions> options =
      std::make_shared<adapters::orc::ORCWriterOptions>(batchSize);
  std::unique_ptr<ORCMemWriter> writer =
      std::unique_ptr<ORCMemWriter>(new ORCMemWriter());
  std::unique_ptr<liborc::OutputStream> out_stream =
      std::unique_ptr<liborc::OutputStream>(static_cast<liborc::OutputStream*>(
          new MemoryOutputStream(DEFAULT_SMALL_MEM_STREAM_SIZE / 16)));
  ARROW_EXPECT_OK(writer->Open(sharedPtrSchema, out_stream, options));
  ARROW_EXPECT_OK(writer->Write(table));
  auto output_mem_stream = static_cast<MemoryOutputStream*>(writer->ReleaseOutStream());
  std::shared_ptr<io::RandomAccessFile> in_stream(
      new io::BufferReader(std::make_shared<Buffer>(
          reinterpret_cast<const uint8_t*>(output_mem_stream->getData()),
          static_cast<int64_t>(output_mem_stream->getLength()))));

  std::unique_ptr<adapters::orc::ORCFileReader> reader;
  ASSERT_TRUE(
      adapters::orc::ORCFileReader::Open(in_stream, default_memory_pool(), &reader).ok());
  std::shared_ptr<Table> outputTable;
  ARROW_EXPECT_OK(reader->Read(&outputTable));
  EXPECT_EQ(outputTable->num_columns(), numCols);
  EXPECT_EQ(outputTable->num_rows(), numRows);
  EXPECT_TRUE(outputTable->Equals(*table));
}
TEST(TestAdapterWriteGeneral, writeChunkless) {
  std::vector<std::shared_ptr<Field>> xFieldsSub{std::make_shared<Field>("a", utf8()),
                                                 std::make_shared<Field>("b", int32())};
  std::vector<std::shared_ptr<Field>> xFields{
      field("bool", boolean()),
      field("int8", int8()),
      field("int16", int16()),
      field("int32", int32()),
      field("int64", int64()),
      field("float", float32()),
      field("double", float64()),
      field("decimal128nz", decimal(25, 6)),
      field("decimal128z", decimal(32, 0)),
      field("date32", date32()),
      field("ts3", timestamp(TimeUnit::NANO)),
      field("string", utf8()),
      field("binary", binary()),
      field("struct", struct_(xFieldsSub)),
      field("list", list(int32())),
      field("lsl", list(struct_({field("lsl0", list(int32()))})))};
  std::shared_ptr<Schema> sharedPtrSchema = std::make_shared<Schema>(xFields);

  int64_t numRows = 0;
  int64_t numCols = xFields.size();
  uint64_t batchSize = 2048;

  ChunkedArrayVector cv;
  cv.reserve(numCols);

  ArrayMatrix av(numCols, ArrayVector(0, NULLPTR));

  for (int col = 0; col < numCols; col++) {
    cv.push_back(std::make_shared<ChunkedArray>(av[col], xFields[col]->type()));
  }

  std::shared_ptr<Table> table = Table::Make(sharedPtrSchema, cv);

  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  std::shared_ptr<adapters::orc::ORCWriterOptions> options =
      std::make_shared<adapters::orc::ORCWriterOptions>(batchSize);
  std::unique_ptr<ORCMemWriter> writer =
      std::unique_ptr<ORCMemWriter>(new ORCMemWriter());
  std::unique_ptr<liborc::OutputStream> out_stream =
      std::unique_ptr<liborc::OutputStream>(static_cast<liborc::OutputStream*>(
          new MemoryOutputStream(DEFAULT_SMALL_MEM_STREAM_SIZE / 16)));
  ARROW_EXPECT_OK(writer->Open(sharedPtrSchema, out_stream, options));
  ARROW_EXPECT_OK(writer->Write(table));
  auto output_mem_stream = static_cast<MemoryOutputStream*>(writer->ReleaseOutStream());
  std::shared_ptr<io::RandomAccessFile> in_stream(
      new io::BufferReader(std::make_shared<Buffer>(
          reinterpret_cast<const uint8_t*>(output_mem_stream->getData()),
          static_cast<int64_t>(output_mem_stream->getLength()))));

  std::unique_ptr<adapters::orc::ORCFileReader> reader;
  ASSERT_TRUE(
      adapters::orc::ORCFileReader::Open(in_stream, default_memory_pool(), &reader).ok());
  std::shared_ptr<Table> outputTable;
  ARROW_EXPECT_OK(reader->Read(&outputTable));
  EXPECT_EQ(outputTable->num_columns(), numCols);
  EXPECT_EQ(outputTable->num_rows(), numRows);
  EXPECT_TRUE(outputTable->Equals(*table));
}
TEST(TestAdapterWriteGeneral, writeAllNulls) {
  std::vector<std::shared_ptr<Field>> xFields{field("bool", boolean()),
                                              field("int8", int8()),
                                              field("int16", int16()),
                                              field("int32", int32()),
                                              field("int64", int64()),
                                              field("decimal128nz", decimal(33, 4)),
                                              field("decimal128z", decimal(35, 0)),
                                              field("date32", date32()),
                                              field("ts3", timestamp(TimeUnit::NANO)),
                                              field("string", utf8()),
                                              field("binary", binary())};
  std::shared_ptr<Schema> sharedPtrSchema = std::make_shared<Schema>(xFields);

  int64_t numRows = 10000;
  int64_t numCols = xFields.size();
  uint64_t batchSize = 10000;

  ArrayBuilderMatrix builders(numCols, ArrayBuilderVector(5, NULLPTR));

  for (int i = 0; i < 5; i++) {
    builders[0][i] =
        std::static_pointer_cast<ArrayBuilder>(std::make_shared<BooleanBuilder>());
    builders[1][i] =
        std::static_pointer_cast<ArrayBuilder>(std::make_shared<Int8Builder>());
    builders[2][i] =
        std::static_pointer_cast<ArrayBuilder>(std::make_shared<Int16Builder>());
    builders[3][i] =
        std::static_pointer_cast<ArrayBuilder>(std::make_shared<Int32Builder>());
    builders[4][i] =
        std::static_pointer_cast<ArrayBuilder>(std::make_shared<Int64Builder>());
    builders[5][i] = std::static_pointer_cast<ArrayBuilder>(
        std::make_shared<Decimal128Builder>(decimal(33, 4)));
    builders[6][i] = std::static_pointer_cast<ArrayBuilder>(
        std::make_shared<Decimal128Builder>(decimal(35, 0)));
    builders[7][i] =
        std::static_pointer_cast<ArrayBuilder>(std::make_shared<Date32Builder>());
    builders[8][i] =
        std::static_pointer_cast<ArrayBuilder>(std::make_shared<TimestampBuilder>(
            timestamp(TimeUnit::NANO), default_memory_pool()));
    builders[9][i] =
        std::static_pointer_cast<ArrayBuilder>(std::make_shared<StringBuilder>());
    builders[10][i] =
        std::static_pointer_cast<ArrayBuilder>(std::make_shared<BinaryBuilder>());
  }

  for (int i = 0; i < numRows; i++) {
    int chunk = i < (numRows / 2) ? 1 : 3;
    for (int col = 0; col < numCols; col++) {
      ARROW_EXPECT_OK(builders[col][chunk]->AppendNull());
    }
  }

  ArrayMatrix arrays(numCols, ArrayVector(5, NULLPTR));
  ChunkedArrayVector cv;
  cv.reserve(numCols);

  for (int col = 0; col < numCols; col++) {
    for (int i = 0; i < 5; i++) {
      ARROW_EXPECT_OK(builders[col][i]->Finish(&arrays[col][i]));
    }
    cv.push_back(std::make_shared<ChunkedArray>(arrays[col]));
  }

  std::shared_ptr<Table> table = Table::Make(sharedPtrSchema, cv);

  std::shared_ptr<adapters::orc::ORCWriterOptions> options =
      std::make_shared<adapters::orc::ORCWriterOptions>(batchSize);
  std::unique_ptr<ORCMemWriter> writer =
      std::unique_ptr<ORCMemWriter>(new ORCMemWriter());
  std::unique_ptr<liborc::OutputStream> out_stream =
      std::unique_ptr<liborc::OutputStream>(static_cast<liborc::OutputStream*>(
          new MemoryOutputStream(DEFAULT_SMALL_MEM_STREAM_SIZE)));
  ARROW_EXPECT_OK(writer->Open(sharedPtrSchema, out_stream, options));
  ARROW_EXPECT_OK(writer->Write(table));
  auto output_mem_stream = static_cast<MemoryOutputStream*>(writer->ReleaseOutStream());
  std::shared_ptr<io::RandomAccessFile> in_stream(
      new io::BufferReader(std::make_shared<Buffer>(
          reinterpret_cast<const uint8_t*>(output_mem_stream->getData()),
          static_cast<int64_t>(output_mem_stream->getLength()))));

  std::unique_ptr<adapters::orc::ORCFileReader> reader;
  ASSERT_TRUE(
      adapters::orc::ORCFileReader::Open(in_stream, default_memory_pool(), &reader).ok());
  std::shared_ptr<Table> outputTable;
  ARROW_EXPECT_OK(reader->Read(&outputTable));
  EXPECT_EQ(outputTable->num_columns(), numCols);
  EXPECT_EQ(outputTable->num_rows(), numRows);
  EXPECT_TRUE(outputTable->Equals(*table));
}
TEST(TestAdapterWriteGeneral, writeNoNulls) {
  std::vector<std::shared_ptr<Field>> xFields{field("bool", boolean()),
                                              field("int8", int8()),
                                              field("int16", int16()),
                                              field("int32", int32()),
                                              field("int64", int64()),
                                              field("decimal128nz", decimal(36, 2)),
                                              field("decimal128z", decimal(31, 0)),
                                              field("date32", date32()),
                                              field("ts3", timestamp(TimeUnit::NANO)),
                                              field("string", utf8()),
                                              field("binary", binary())};
  std::shared_ptr<Schema> sharedPtrSchema = std::make_shared<Schema>(xFields);

  int64_t numRows = 10000;
  int64_t numCols = xFields.size();
  uint64_t batchSize = 1025;

  ArrayBuilderMatrix builders(numCols, ArrayBuilderVector(5, NULLPTR));

  for (int i = 0; i < 5; i++) {
    builders[0][i] =
        std::static_pointer_cast<ArrayBuilder>(std::make_shared<BooleanBuilder>());
    builders[1][i] =
        std::static_pointer_cast<ArrayBuilder>(std::make_shared<Int8Builder>());
    builders[2][i] =
        std::static_pointer_cast<ArrayBuilder>(std::make_shared<Int16Builder>());
    builders[3][i] =
        std::static_pointer_cast<ArrayBuilder>(std::make_shared<Int32Builder>());
    builders[4][i] =
        std::static_pointer_cast<ArrayBuilder>(std::make_shared<Int64Builder>());
    builders[5][i] = std::static_pointer_cast<ArrayBuilder>(
        std::make_shared<Decimal128Builder>(decimal(36, 2)));
    builders[6][i] = std::static_pointer_cast<ArrayBuilder>(
        std::make_shared<Decimal128Builder>(decimal(31, 0)));
    builders[7][i] =
        std::static_pointer_cast<ArrayBuilder>(std::make_shared<Date32Builder>());
    builders[8][i] =
        std::static_pointer_cast<ArrayBuilder>(std::make_shared<TimestampBuilder>(
            timestamp(TimeUnit::NANO), default_memory_pool()));
    builders[9][i] =
        std::static_pointer_cast<ArrayBuilder>(std::make_shared<StringBuilder>());
    builders[10][i] =
        std::static_pointer_cast<ArrayBuilder>(std::make_shared<BinaryBuilder>());
  }

  char bin[2], string_[13];
  std::string str;
  for (int64_t i = 0; i < numRows / 2; i++) {
    bin[0] = i % 128;
    bin[1] = bin[0];
    str = "Arrow " + std::to_string(2 * i);
    snprintf(string_, sizeof(string_), "%s", str.c_str());
    ARROW_EXPECT_OK(
        std::static_pointer_cast<BooleanBuilder>(builders[0][1])->Append(true));
    ARROW_EXPECT_OK(
        std::static_pointer_cast<Int8Builder>(builders[1][1])->Append(i % 128));
    ARROW_EXPECT_OK(std::static_pointer_cast<Int16Builder>(builders[2][1])->Append(i));
    ARROW_EXPECT_OK(std::static_pointer_cast<Int32Builder>(builders[3][1])->Append(i));
    ARROW_EXPECT_OK(std::static_pointer_cast<Int64Builder>(builders[4][1])->Append(i));
    ARROW_EXPECT_OK(std::static_pointer_cast<Decimal128Builder>(builders[5][1])
                        ->Append(Decimal128(std::to_string(i) + ".56")));
    ARROW_EXPECT_OK(std::static_pointer_cast<Decimal128Builder>(builders[6][1])
                        ->Append(Decimal128(std::to_string(i))));
    ARROW_EXPECT_OK(
        std::static_pointer_cast<Date32Builder>(builders[7][1])->Append(18600 + i));
    ARROW_EXPECT_OK(std::static_pointer_cast<TimestampBuilder>(builders[8][1])
                        ->Append(INT64_C(1605547718999999999) + i));
    ARROW_EXPECT_OK(
        std::static_pointer_cast<StringBuilder>(builders[9][1])->Append(string_));
    ARROW_EXPECT_OK(
        std::static_pointer_cast<BinaryBuilder>(builders[10][1])->Append(bin, 2));
  }
  for (int64_t i = numRows / 2; i < numRows; i++) {
    bin[0] = i % 256;
    bin[1] = (i / 256) % 256;
    str = "Arrow " + std::to_string(3 - 4 * i);
    snprintf(string_, sizeof(string_), "%s", str.c_str());
    ARROW_EXPECT_OK(
        std::static_pointer_cast<BooleanBuilder>(builders[0][3])->Append(false));
    ARROW_EXPECT_OK(
        std::static_pointer_cast<Int8Builder>(builders[1][3])->Append(-(i % 128)));
    ARROW_EXPECT_OK(
        std::static_pointer_cast<Int16Builder>(builders[2][3])->Append(4 - i));
    ARROW_EXPECT_OK(std::static_pointer_cast<Int32Builder>(builders[3][3])->Append(-i));
    ARROW_EXPECT_OK(std::static_pointer_cast<Int64Builder>(builders[4][3])->Append(-i));
    ARROW_EXPECT_OK(std::static_pointer_cast<Decimal128Builder>(builders[5][3])
                        ->Append(Decimal128(std::to_string(-i) + ".00")));
    ARROW_EXPECT_OK(std::static_pointer_cast<Decimal128Builder>(builders[6][3])
                        ->Append(Decimal128(std::to_string(-i))));
    ARROW_EXPECT_OK(
        std::static_pointer_cast<Date32Builder>(builders[7][3])->Append(18600 - i));
    ARROW_EXPECT_OK(std::static_pointer_cast<TimestampBuilder>(builders[8][3])
                        ->Append(INT64_C(1605557718999999999) - i));
    ARROW_EXPECT_OK(
        std::static_pointer_cast<StringBuilder>(builders[9][3])->Append(string_));
    ARROW_EXPECT_OK(
        std::static_pointer_cast<BinaryBuilder>(builders[10][3])->Append(bin, 2));
  }
  ArrayMatrix arrays(numCols, ArrayVector(5, NULLPTR));
  ChunkedArrayVector cv;
  cv.reserve(numCols);

  for (int col = 0; col < numCols; col++) {
    for (int i = 0; i < 5; i++) {
      ARROW_EXPECT_OK(builders[col][i]->Finish(&arrays[col][i]));
    }
    cv.push_back(std::make_shared<ChunkedArray>(arrays[col]));
  }
  std::shared_ptr<Table> table = Table::Make(sharedPtrSchema, cv);

  std::shared_ptr<adapters::orc::ORCWriterOptions> options =
      std::make_shared<adapters::orc::ORCWriterOptions>(batchSize);
  std::unique_ptr<ORCMemWriter> writer =
      std::unique_ptr<ORCMemWriter>(new ORCMemWriter());
  std::unique_ptr<liborc::OutputStream> out_stream =
      std::unique_ptr<liborc::OutputStream>(static_cast<liborc::OutputStream*>(
          new MemoryOutputStream(DEFAULT_SMALL_MEM_STREAM_SIZE)));
  ARROW_EXPECT_OK(writer->Open(sharedPtrSchema, out_stream, options));
  ARROW_EXPECT_OK(writer->Write(table));
  auto output_mem_stream = static_cast<MemoryOutputStream*>(writer->ReleaseOutStream());
  std::shared_ptr<io::RandomAccessFile> in_stream(
      new io::BufferReader(std::make_shared<Buffer>(
          reinterpret_cast<const uint8_t*>(output_mem_stream->getData()),
          static_cast<int64_t>(output_mem_stream->getLength()))));

  std::unique_ptr<adapters::orc::ORCFileReader> reader;
  ASSERT_TRUE(
      adapters::orc::ORCFileReader::Open(in_stream, default_memory_pool(), &reader).ok());
  std::shared_ptr<Table> outputTable;
  ARROW_EXPECT_OK(reader->Read(&outputTable));
  EXPECT_EQ(outputTable->num_columns(), numCols);
  EXPECT_EQ(outputTable->num_rows(), numRows);
  EXPECT_TRUE(outputTable->Equals(*table));
}
TEST(TestAdapterWriteGeneral, writeMixed) {
  std::vector<std::shared_ptr<Field>> xFields{field("bool", boolean()),
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
  std::shared_ptr<Schema> sharedPtrSchema = std::make_shared<Schema>(xFields);

  int64_t numRows = 10000;
  int64_t numCols = xFields.size();
  uint64_t batchSize = 1;

  ArrayBuilderMatrix builders(numCols, ArrayBuilderVector(5, NULLPTR));

  for (int i = 0; i < 5; i++) {
    builders[0][i] =
        std::static_pointer_cast<ArrayBuilder>(std::make_shared<BooleanBuilder>());
    builders[1][i] =
        std::static_pointer_cast<ArrayBuilder>(std::make_shared<Int8Builder>());
    builders[2][i] =
        std::static_pointer_cast<ArrayBuilder>(std::make_shared<Int16Builder>());
    builders[3][i] =
        std::static_pointer_cast<ArrayBuilder>(std::make_shared<Int32Builder>());
    builders[4][i] =
        std::static_pointer_cast<ArrayBuilder>(std::make_shared<Int64Builder>());
    builders[5][i] = std::static_pointer_cast<ArrayBuilder>(
        std::make_shared<Decimal128Builder>(decimal(38, 6)));
    builders[6][i] = std::static_pointer_cast<ArrayBuilder>(
        std::make_shared<Decimal128Builder>(decimal(38, 0)));
    builders[7][i] =
        std::static_pointer_cast<ArrayBuilder>(std::make_shared<Date32Builder>());
    builders[8][i] =
        std::static_pointer_cast<ArrayBuilder>(std::make_shared<TimestampBuilder>(
            timestamp(TimeUnit::NANO), default_memory_pool()));
    builders[9][i] =
        std::static_pointer_cast<ArrayBuilder>(std::make_shared<StringBuilder>());
    builders[10][i] =
        std::static_pointer_cast<ArrayBuilder>(std::make_shared<BinaryBuilder>());
  }
  char bin1[2], bin2[] = "", string_[12];
  std::string str;
  for (int64_t i = 0; i < numRows / 2; i++) {
    if (i % 2) {
      bin1[0] = i % 256;
      bin1[1] = (i - 1) % 256;
      ARROW_EXPECT_OK(
          std::static_pointer_cast<BooleanBuilder>(builders[0][1])->Append(true));
      ARROW_EXPECT_OK(
          std::static_pointer_cast<Int8Builder>(builders[1][1])->AppendNull());
      ARROW_EXPECT_OK(std::static_pointer_cast<Int16Builder>(builders[2][1])->Append(i));
      ARROW_EXPECT_OK(
          std::static_pointer_cast<Int32Builder>(builders[3][1])->AppendNull());
      ARROW_EXPECT_OK(std::static_pointer_cast<Int64Builder>(builders[4][1])->Append(i));
      ARROW_EXPECT_OK(
          std::static_pointer_cast<Decimal128Builder>(builders[5][1])->AppendNull());
      ARROW_EXPECT_OK(std::static_pointer_cast<Decimal128Builder>(builders[6][1])
                          ->Append(Decimal128(std::to_string(i))));
      ARROW_EXPECT_OK(
          std::static_pointer_cast<Date32Builder>(builders[7][1])->AppendNull());
      ARROW_EXPECT_OK(std::static_pointer_cast<TimestampBuilder>(builders[8][1])
                          ->Append(INT64_C(1605548719999999999) + 2 * i));
      ARROW_EXPECT_OK(
          std::static_pointer_cast<StringBuilder>(builders[9][1])->AppendNull());
      ARROW_EXPECT_OK(
          std::static_pointer_cast<BinaryBuilder>(builders[10][1])->Append(bin1, 2));
    } else {
      str = "Arrow " + std::to_string(2 * i);
      snprintf(string_, sizeof(string_), "%s", str.c_str());
      ARROW_EXPECT_OK(
          std::static_pointer_cast<BooleanBuilder>(builders[0][1])->AppendNull());
      ARROW_EXPECT_OK(
          std::static_pointer_cast<Int8Builder>(builders[1][1])->Append(-(i % 128)));
      ARROW_EXPECT_OK(
          std::static_pointer_cast<Int16Builder>(builders[2][1])->AppendNull());
      ARROW_EXPECT_OK(std::static_pointer_cast<Int32Builder>(builders[3][1])->Append(-i));
      ARROW_EXPECT_OK(
          std::static_pointer_cast<Int64Builder>(builders[4][1])->AppendNull());
      ARROW_EXPECT_OK(std::static_pointer_cast<Decimal128Builder>(builders[5][1])
                          ->Append(Decimal128(std::to_string(i) + ".567890")));
      ARROW_EXPECT_OK(
          std::static_pointer_cast<Decimal128Builder>(builders[6][1])->AppendNull());
      ARROW_EXPECT_OK(
          std::static_pointer_cast<Date32Builder>(builders[7][1])->Append(18600 + i));
      ARROW_EXPECT_OK(
          std::static_pointer_cast<TimestampBuilder>(builders[8][1])->AppendNull());
      ARROW_EXPECT_OK(
          std::static_pointer_cast<StringBuilder>(builders[9][1])->Append(string_));
      ARROW_EXPECT_OK(
          std::static_pointer_cast<BinaryBuilder>(builders[10][1])->AppendNull());
    }
  }
  for (int64_t i = numRows / 2; i < numRows; i++) {
    if (i % 2) {
      ARROW_EXPECT_OK(
          std::static_pointer_cast<BooleanBuilder>(builders[0][3])->Append(false));
      ARROW_EXPECT_OK(
          std::static_pointer_cast<Int8Builder>(builders[1][3])->AppendNull());
      ARROW_EXPECT_OK(std::static_pointer_cast<Int16Builder>(builders[2][3])->Append(-i));
      ARROW_EXPECT_OK(
          std::static_pointer_cast<Int32Builder>(builders[3][3])->AppendNull());
      ARROW_EXPECT_OK(std::static_pointer_cast<Int64Builder>(builders[4][3])->Append(-i));
      ARROW_EXPECT_OK(
          std::static_pointer_cast<Decimal128Builder>(builders[5][3])->AppendNull());
      ARROW_EXPECT_OK(std::static_pointer_cast<Decimal128Builder>(builders[6][3])
                          ->Append(Decimal128(std::to_string(-i))));
      ARROW_EXPECT_OK(
          std::static_pointer_cast<Date32Builder>(builders[7][3])->AppendNull());
      ARROW_EXPECT_OK(std::static_pointer_cast<TimestampBuilder>(builders[8][3])
                          ->Append(INT64_C(1606548719999999999) - INT64_C(29) * i));
      ARROW_EXPECT_OK(
          std::static_pointer_cast<StringBuilder>(builders[9][3])->AppendNull());
      ARROW_EXPECT_OK(
          std::static_pointer_cast<BinaryBuilder>(builders[10][3])->Append(bin2, 0));
    } else {
      str = "Arrow " + std::to_string(2 * i);
      snprintf(string_, sizeof(string_), "%s", str.c_str());
      ARROW_EXPECT_OK(
          std::static_pointer_cast<BooleanBuilder>(builders[0][3])->AppendNull());
      ARROW_EXPECT_OK(
          std::static_pointer_cast<Int8Builder>(builders[1][3])->Append(i % 128));
      ARROW_EXPECT_OK(
          std::static_pointer_cast<Int16Builder>(builders[2][3])->AppendNull());
      ARROW_EXPECT_OK(std::static_pointer_cast<Int32Builder>(builders[3][3])->Append(i));
      ARROW_EXPECT_OK(
          std::static_pointer_cast<Int64Builder>(builders[4][3])->AppendNull());
      ARROW_EXPECT_OK(std::static_pointer_cast<Decimal128Builder>(builders[5][3])
                          ->Append(Decimal128(std::to_string(-i) + ".543211")));
      ARROW_EXPECT_OK(
          std::static_pointer_cast<Decimal128Builder>(builders[6][3])->AppendNull());
      ARROW_EXPECT_OK(
          std::static_pointer_cast<Date32Builder>(builders[7][3])->Append(18600 - i));
      ARROW_EXPECT_OK(
          std::static_pointer_cast<TimestampBuilder>(builders[8][3])->AppendNull());
      ARROW_EXPECT_OK(
          std::static_pointer_cast<StringBuilder>(builders[9][3])->Append(string_));
      ARROW_EXPECT_OK(
          std::static_pointer_cast<BinaryBuilder>(builders[10][3])->AppendNull());
    }
  }
  ArrayMatrix arrays(numCols, ArrayVector(5, NULLPTR));
  ChunkedArrayVector cv;
  cv.reserve(numCols);

  for (int col = 0; col < numCols; col++) {
    for (int i = 0; i < 5; i++) {
      ARROW_EXPECT_OK(builders[col][i]->Finish(&arrays[col][i]));
    }
    cv.push_back(std::make_shared<ChunkedArray>(arrays[col]));
  }
  std::shared_ptr<Table> table = Table::Make(sharedPtrSchema, cv);

  std::shared_ptr<adapters::orc::ORCWriterOptions> options =
      std::make_shared<adapters::orc::ORCWriterOptions>(batchSize);
  std::unique_ptr<ORCMemWriter> writer =
      std::unique_ptr<ORCMemWriter>(new ORCMemWriter());
  std::unique_ptr<liborc::OutputStream> out_stream =
      std::unique_ptr<liborc::OutputStream>(static_cast<liborc::OutputStream*>(
          new MemoryOutputStream(DEFAULT_SMALL_MEM_STREAM_SIZE)));
  ARROW_EXPECT_OK(writer->Open(sharedPtrSchema, out_stream, options));
  ARROW_EXPECT_OK(writer->Write(table));
  auto output_mem_stream = static_cast<MemoryOutputStream*>(writer->ReleaseOutStream());
  std::shared_ptr<io::RandomAccessFile> in_stream(
      new io::BufferReader(std::make_shared<Buffer>(
          reinterpret_cast<const uint8_t*>(output_mem_stream->getData()),
          static_cast<int64_t>(output_mem_stream->getLength()))));

  std::unique_ptr<adapters::orc::ORCFileReader> reader;
  ASSERT_TRUE(
      adapters::orc::ORCFileReader::Open(in_stream, default_memory_pool(), &reader).ok());
  std::shared_ptr<Table> outputTable;
  ARROW_EXPECT_OK(reader->Read(&outputTable));
  EXPECT_EQ(outputTable->num_columns(), numCols);
  EXPECT_EQ(outputTable->num_rows(), numRows);
  EXPECT_TRUE(outputTable->Equals(*table));
}

// Float & Double
// Equality might not work hence we do them separately here
TEST(TestAdapterWriteFloat, writeAllNulls) {
  std::vector<std::shared_ptr<Field>> xFields{field("float", float32()),
                                              field("double", float64())};
  std::shared_ptr<Schema> sharedPtrSchema = std::make_shared<Schema>(xFields);

  int64_t numRows = 10000;
  int64_t numCols = xFields.size();
  uint64_t batchSize = 700;

  ArrayBuilderMatrix builders(numCols, ArrayBuilderVector(5, NULLPTR));

  for (int i = 0; i < 5; i++) {
    builders[0][i] =
        std::static_pointer_cast<ArrayBuilder>(std::make_shared<FloatBuilder>());
    builders[1][i] =
        std::static_pointer_cast<ArrayBuilder>(std::make_shared<DoubleBuilder>());
  }

  for (int i = 0; i < numRows; i++) {
    int chunk = i < (numRows / 2) ? 1 : 3;
    for (int col = 0; col < numCols; col++) {
      ARROW_EXPECT_OK(builders[col][chunk]->AppendNull());
    }
  }

  ArrayMatrix arrays(numCols, ArrayVector(5, NULLPTR));
  ChunkedArrayVector cv;
  cv.reserve(numCols);

  for (int col = 0; col < numCols; col++) {
    for (int i = 0; i < 5; i++) {
      ARROW_EXPECT_OK(builders[col][i]->Finish(&arrays[col][i]));
    }
    cv.push_back(std::make_shared<ChunkedArray>(arrays[col]));
  }
  std::shared_ptr<Table> table = Table::Make(sharedPtrSchema, cv);

  std::shared_ptr<adapters::orc::ORCWriterOptions> options =
      std::make_shared<adapters::orc::ORCWriterOptions>(batchSize);
  std::unique_ptr<ORCMemWriter> writer =
      std::unique_ptr<ORCMemWriter>(new ORCMemWriter());
  std::unique_ptr<liborc::OutputStream> out_stream =
      std::unique_ptr<liborc::OutputStream>(static_cast<liborc::OutputStream*>(
          new MemoryOutputStream(DEFAULT_SMALL_MEM_STREAM_SIZE)));
  ARROW_EXPECT_OK(writer->Open(sharedPtrSchema, out_stream, options));
  ARROW_EXPECT_OK(writer->Write(table));
  auto output_mem_stream = static_cast<MemoryOutputStream*>(writer->ReleaseOutStream());
  std::shared_ptr<io::RandomAccessFile> in_stream(
      new io::BufferReader(std::make_shared<Buffer>(
          reinterpret_cast<const uint8_t*>(output_mem_stream->getData()),
          static_cast<int64_t>(output_mem_stream->getLength()))));

  std::unique_ptr<adapters::orc::ORCFileReader> reader;
  ASSERT_TRUE(
      adapters::orc::ORCFileReader::Open(in_stream, default_memory_pool(), &reader).ok());
  std::shared_ptr<Table> outputTable;
  ARROW_EXPECT_OK(reader->Read(&outputTable));
  EXPECT_EQ(outputTable->num_columns(), numCols);
  EXPECT_EQ(outputTable->num_rows(), numRows);
  EXPECT_TRUE(outputTable->schema()->Equals(*(table->schema())));
  EXPECT_TRUE(outputTable->column(0)
                  ->chunk(0)
                  ->Slice(0, numRows / 2)
                  ->ApproxEquals(table->column(0)->chunk(1)));
  EXPECT_TRUE(outputTable->column(0)
                  ->chunk(0)
                  ->Slice(numRows / 2, numRows / 2)
                  ->ApproxEquals(table->column(0)->chunk(3)));
  EXPECT_TRUE(outputTable->column(1)
                  ->chunk(0)
                  ->Slice(0, numRows / 2)
                  ->ApproxEquals(table->column(1)->chunk(1)));
  EXPECT_TRUE(outputTable->column(1)
                  ->chunk(0)
                  ->Slice(numRows / 2, numRows / 2)
                  ->ApproxEquals(table->column(1)->chunk(3)));
}
TEST(TestAdapterWriteFloat, writeNoNulls) {
  std::vector<std::shared_ptr<Field>> xFields{field("float", float32()),
                                              field("double", float64())};
  std::shared_ptr<Schema> sharedPtrSchema = std::make_shared<Schema>(xFields);

  int64_t numRows = 10000;
  int64_t numCols = xFields.size();
  uint64_t batchSize = 16384;
  ArrayBuilderMatrix builders(numCols, ArrayBuilderVector(5, NULLPTR));

  for (int i = 0; i < 5; i++) {
    builders[0][i] =
        std::static_pointer_cast<ArrayBuilder>(std::make_shared<FloatBuilder>());
    builders[1][i] =
        std::static_pointer_cast<ArrayBuilder>(std::make_shared<DoubleBuilder>());
  }

  for (int i = 0; i < numRows / 2; i++) {
    ARROW_EXPECT_OK(
        std::static_pointer_cast<FloatBuilder>(builders[0][1])->Append(i + 0.7));
    ARROW_EXPECT_OK(
        std::static_pointer_cast<DoubleBuilder>(builders[1][1])->Append(-i + 0.43));
  }
  for (int i = numRows / 2; i < numRows; i++) {
    ARROW_EXPECT_OK(
        std::static_pointer_cast<FloatBuilder>(builders[0][3])->Append(2 * i - 2.12));
    ARROW_EXPECT_OK(
        std::static_pointer_cast<DoubleBuilder>(builders[1][3])->Append(3 * i + 4.12));
  }
  ArrayMatrix arrays(numCols, ArrayVector(5, NULLPTR));
  ChunkedArrayVector cv;
  cv.reserve(numCols);

  for (int col = 0; col < numCols; col++) {
    for (int i = 0; i < 5; i++) {
      ARROW_EXPECT_OK(builders[col][i]->Finish(&arrays[col][i]));
    }
    cv.push_back(std::make_shared<ChunkedArray>(arrays[col]));
  }
  std::shared_ptr<Table> table = Table::Make(sharedPtrSchema, cv);

  std::shared_ptr<adapters::orc::ORCWriterOptions> options =
      std::make_shared<adapters::orc::ORCWriterOptions>(batchSize);
  std::unique_ptr<ORCMemWriter> writer =
      std::unique_ptr<ORCMemWriter>(new ORCMemWriter());
  std::unique_ptr<liborc::OutputStream> out_stream =
      std::unique_ptr<liborc::OutputStream>(static_cast<liborc::OutputStream*>(
          new MemoryOutputStream(DEFAULT_SMALL_MEM_STREAM_SIZE)));
  ARROW_EXPECT_OK(writer->Open(sharedPtrSchema, out_stream, options));
  ARROW_EXPECT_OK(writer->Write(table));
  auto output_mem_stream = static_cast<MemoryOutputStream*>(writer->ReleaseOutStream());
  std::shared_ptr<io::RandomAccessFile> in_stream(
      new io::BufferReader(std::make_shared<Buffer>(
          reinterpret_cast<const uint8_t*>(output_mem_stream->getData()),
          static_cast<int64_t>(output_mem_stream->getLength()))));

  std::unique_ptr<adapters::orc::ORCFileReader> reader;
  ASSERT_TRUE(
      adapters::orc::ORCFileReader::Open(in_stream, default_memory_pool(), &reader).ok());
  std::shared_ptr<Table> outputTable;
  ARROW_EXPECT_OK(reader->Read(&outputTable));
  EXPECT_EQ(outputTable->num_columns(), numCols);
  EXPECT_EQ(outputTable->num_rows(), numRows);
  EXPECT_TRUE(outputTable->schema()->Equals(*(table->schema())));
  EXPECT_TRUE(outputTable->column(0)
                  ->chunk(0)
                  ->Slice(0, numRows / 2)
                  ->ApproxEquals(table->column(0)->chunk(1)));
  EXPECT_TRUE(outputTable->column(0)
                  ->chunk(0)
                  ->Slice(numRows / 2, numRows / 2)
                  ->ApproxEquals(table->column(0)->chunk(3)));
  EXPECT_TRUE(outputTable->column(1)
                  ->chunk(0)
                  ->Slice(0, numRows / 2)
                  ->ApproxEquals(table->column(1)->chunk(1)));
  EXPECT_TRUE(outputTable->column(1)
                  ->chunk(0)
                  ->Slice(numRows / 2, numRows / 2)
                  ->ApproxEquals(table->column(1)->chunk(3)));
}
TEST(TestAdapterWriteFloat, writeMixed) {
  std::vector<std::shared_ptr<Field>> xFields{field("float", float32()),
                                              field("double", float64())};
  std::shared_ptr<Schema> sharedPtrSchema = std::make_shared<Schema>(xFields);

  int64_t numRows = 10000;
  int64_t numCols = xFields.size();
  uint64_t batchSize = 16384;

  ArrayBuilderMatrix builders(numCols, ArrayBuilderVector(5, NULLPTR));

  for (int i = 0; i < 5; i++) {
    builders[0][i] =
        std::static_pointer_cast<ArrayBuilder>(std::make_shared<FloatBuilder>());
    builders[1][i] =
        std::static_pointer_cast<ArrayBuilder>(std::make_shared<DoubleBuilder>());
  }

  for (int i = 0; i < numRows / 2; i++) {
    if (i % 2) {
      ARROW_EXPECT_OK(
          std::static_pointer_cast<FloatBuilder>(builders[0][1])->Append(i + 0.7));
      ARROW_EXPECT_OK(
          std::static_pointer_cast<DoubleBuilder>(builders[1][1])->AppendNull());
    } else {
      ARROW_EXPECT_OK(
          std::static_pointer_cast<FloatBuilder>(builders[0][1])->AppendNull());
      ARROW_EXPECT_OK(
          std::static_pointer_cast<DoubleBuilder>(builders[1][1])->Append(-i + 0.43));
    }
  }
  for (int i = numRows / 2; i < numRows; i++) {
    if (i % 2) {
      ARROW_EXPECT_OK(
          std::static_pointer_cast<FloatBuilder>(builders[0][3])->AppendNull());
      ARROW_EXPECT_OK(
          std::static_pointer_cast<DoubleBuilder>(builders[1][3])->Append(3 * i + 4.12));
    } else {
      ARROW_EXPECT_OK(
          std::static_pointer_cast<FloatBuilder>(builders[0][3])->Append(2 * i - 2.12));
      ARROW_EXPECT_OK(
          std::static_pointer_cast<DoubleBuilder>(builders[1][3])->AppendNull());
    }
  }
  ArrayMatrix arrays(numCols, ArrayVector(5, NULLPTR));
  ChunkedArrayVector cv;
  cv.reserve(numCols);

  for (int col = 0; col < numCols; col++) {
    for (int i = 0; i < 5; i++) {
      ARROW_EXPECT_OK(builders[col][i]->Finish(&arrays[col][i]));
    }
    cv.push_back(std::make_shared<ChunkedArray>(arrays[col]));
  }
  std::shared_ptr<Table> table = Table::Make(sharedPtrSchema, cv);

  std::shared_ptr<adapters::orc::ORCWriterOptions> options =
      std::make_shared<adapters::orc::ORCWriterOptions>(batchSize);
  std::unique_ptr<ORCMemWriter> writer =
      std::unique_ptr<ORCMemWriter>(new ORCMemWriter());
  std::unique_ptr<liborc::OutputStream> out_stream =
      std::unique_ptr<liborc::OutputStream>(static_cast<liborc::OutputStream*>(
          new MemoryOutputStream(DEFAULT_SMALL_MEM_STREAM_SIZE)));
  ARROW_EXPECT_OK(writer->Open(sharedPtrSchema, out_stream, options));
  ARROW_EXPECT_OK(writer->Write(table));
  auto output_mem_stream = static_cast<MemoryOutputStream*>(writer->ReleaseOutStream());
  std::shared_ptr<io::RandomAccessFile> in_stream(
      new io::BufferReader(std::make_shared<Buffer>(
          reinterpret_cast<const uint8_t*>(output_mem_stream->getData()),
          static_cast<int64_t>(output_mem_stream->getLength()))));

  std::unique_ptr<adapters::orc::ORCFileReader> reader;
  ASSERT_TRUE(
      adapters::orc::ORCFileReader::Open(in_stream, default_memory_pool(), &reader).ok());
  std::shared_ptr<Table> outputTable;
  ARROW_EXPECT_OK(reader->Read(&outputTable));
  EXPECT_EQ(outputTable->num_columns(), numCols);
  EXPECT_EQ(outputTable->num_rows(), numRows);
  EXPECT_TRUE(outputTable->schema()->Equals(*(table->schema())));
  EXPECT_TRUE(outputTable->column(0)
                  ->chunk(0)
                  ->Slice(0, numRows / 2)
                  ->ApproxEquals(table->column(0)->chunk(1)));
  EXPECT_TRUE(outputTable->column(0)
                  ->chunk(0)
                  ->Slice(numRows / 2, numRows / 2)
                  ->ApproxEquals(table->column(0)->chunk(3)));
  EXPECT_TRUE(outputTable->column(1)
                  ->chunk(0)
                  ->Slice(0, numRows / 2)
                  ->ApproxEquals(table->column(1)->chunk(1)));
  EXPECT_TRUE(outputTable->column(1)
                  ->chunk(0)
                  ->Slice(numRows / 2, numRows / 2)
                  ->ApproxEquals(table->column(1)->chunk(3)));
}

// Converts
// Since Arrow has way more types than ORC type conversions are unavoidable
TEST(TestAdapterWriteConvert, writeZeroRows) {
  std::vector<std::shared_ptr<Field>> xFieldsIn{
      field("date64", date64()),
      field("ts0", timestamp(TimeUnit::SECOND)),
      field("ts1", timestamp(TimeUnit::MILLI)),
      field("ts2", timestamp(TimeUnit::MICRO)),
      field("large_string", large_utf8()),
      field("large_binary", large_binary()),
      field("fixed_size_binary0", fixed_size_binary(0)),
      field("fixed_size_binary", fixed_size_binary(5))},
      xFieldsOut{field("date64", timestamp(TimeUnit::NANO)),
                 field("ts0", timestamp(TimeUnit::NANO)),
                 field("ts1", timestamp(TimeUnit::NANO)),
                 field("ts2", timestamp(TimeUnit::NANO)),
                 field("large_string", utf8()),
                 field("large_binary", binary()),
                 field("fixed_size_binary0", binary()),
                 field("fixed_size_binary", binary())};
  std::shared_ptr<Schema> sharedPtrSchemaIn = std::make_shared<Schema>(xFieldsIn),
                          sharedPtrSchemaOut = std::make_shared<Schema>(xFieldsOut);

  int64_t numRows = 0;
  int64_t numCols = xFieldsIn.size();
  uint64_t batchSize = 803;

  ArrayBuilderVector buildersIn(numCols, NULLPTR), buildersOut(numCols, NULLPTR);
  buildersIn[0] =
      std::static_pointer_cast<ArrayBuilder>(std::make_shared<Date64Builder>());
  buildersIn[1] =
      std::static_pointer_cast<ArrayBuilder>(std::make_shared<TimestampBuilder>(
          timestamp(TimeUnit::SECOND), default_memory_pool()));
  buildersIn[2] =
      std::static_pointer_cast<ArrayBuilder>(std::make_shared<TimestampBuilder>(
          timestamp(TimeUnit::MILLI), default_memory_pool()));
  buildersIn[3] =
      std::static_pointer_cast<ArrayBuilder>(std::make_shared<TimestampBuilder>(
          timestamp(TimeUnit::MICRO), default_memory_pool()));
  buildersIn[4] =
      std::static_pointer_cast<ArrayBuilder>(std::make_shared<LargeStringBuilder>());
  buildersIn[5] =
      std::static_pointer_cast<ArrayBuilder>(std::make_shared<LargeBinaryBuilder>());
  buildersIn[6] = std::static_pointer_cast<ArrayBuilder>(
      std::make_shared<FixedSizeBinaryBuilder>(fixed_size_binary(0)));
  buildersIn[7] = std::static_pointer_cast<ArrayBuilder>(
      std::make_shared<FixedSizeBinaryBuilder>(fixed_size_binary(5)));

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

  ArrayVector arraysIn(numCols, NULLPTR), arraysOut(numCols, NULLPTR);
  ChunkedArrayVector cvIn, cvOut;
  cvIn.reserve(numCols);
  cvOut.reserve(numCols);

  for (int col = 0; col < numCols; col++) {
    ARROW_EXPECT_OK(buildersIn[col]->Finish(&arraysIn[col]));
    cvIn.push_back(std::make_shared<ChunkedArray>(arraysIn[col]));
    ARROW_EXPECT_OK(buildersOut[col]->Finish(&arraysOut[col]));
    cvOut.push_back(std::make_shared<ChunkedArray>(arraysOut[col]));
  }

  std::shared_ptr<Table> tableIn = Table::Make(sharedPtrSchemaIn, cvIn),
                         tableOut = Table::Make(sharedPtrSchemaOut, cvOut);

  std::shared_ptr<adapters::orc::ORCWriterOptions> options =
      std::make_shared<adapters::orc::ORCWriterOptions>(batchSize);
  std::unique_ptr<ORCMemWriter> writer =
      std::unique_ptr<ORCMemWriter>(new ORCMemWriter());
  std::unique_ptr<liborc::OutputStream> out_stream =
      std::unique_ptr<liborc::OutputStream>(static_cast<liborc::OutputStream*>(
          new MemoryOutputStream(DEFAULT_SMALL_MEM_STREAM_SIZE / 16)));
  ARROW_EXPECT_OK(writer->Open(sharedPtrSchemaIn, out_stream, options));
  ARROW_EXPECT_OK(writer->Write(tableIn));
  auto output_mem_stream = static_cast<MemoryOutputStream*>(writer->ReleaseOutStream());
  std::shared_ptr<io::RandomAccessFile> in_stream(
      new io::BufferReader(std::make_shared<Buffer>(
          reinterpret_cast<const uint8_t*>(output_mem_stream->getData()),
          static_cast<int64_t>(output_mem_stream->getLength()))));

  std::unique_ptr<adapters::orc::ORCFileReader> reader;
  ASSERT_TRUE(
      adapters::orc::ORCFileReader::Open(in_stream, default_memory_pool(), &reader).ok());
  std::shared_ptr<Table> outputTable;
  ARROW_EXPECT_OK(reader->Read(&outputTable));
  EXPECT_EQ(outputTable->num_columns(), numCols);
  EXPECT_EQ(outputTable->num_rows(), numRows);
  EXPECT_TRUE(outputTable->Equals(*tableOut));
}
TEST(TestAdapterWriteConvert, writeChunkless) {
  std::vector<std::shared_ptr<Field>> xFieldsIn{
      field("date64", date64()),
      field("ts0", timestamp(TimeUnit::SECOND)),
      field("ts1", timestamp(TimeUnit::MILLI)),
      field("ts2", timestamp(TimeUnit::MICRO)),
      field("large_string", large_utf8()),
      field("large_binary", large_binary()),
      field("fixed_size_binary0", fixed_size_binary(0)),
      field("fixed_size_binary", fixed_size_binary(5)),
      field("large_list", large_list(int32())),
      field("fixed_size_list", fixed_size_list(int32(), 3)),
      field("map", map(utf8(), utf8()))},
      xFieldsOut{
          field("date64", timestamp(TimeUnit::NANO)),
          field("ts0", timestamp(TimeUnit::NANO)),
          field("ts1", timestamp(TimeUnit::NANO)),
          field("ts2", timestamp(TimeUnit::NANO)),
          field("large_string", utf8()),
          field("large_binary", binary()),
          field("fixed_size_binary0", binary()),
          field("fixed_size_binary", binary()),
          field("large_list", list(int32())),
          field("fixed_size_list", list(int32())),
          field("map", list(struct_({field("key", utf8()), field("value", utf8())})))};
  std::shared_ptr<Schema> sharedPtrSchemaIn = std::make_shared<Schema>(xFieldsIn),
                          sharedPtrSchemaOut = std::make_shared<Schema>(xFieldsOut);

  int64_t numRows = 0;
  int64_t numCols = xFieldsIn.size();
  uint64_t batchSize = 2000;

  ChunkedArrayVector cvIn, cvOut;
  cvIn.reserve(numCols);
  cvOut.reserve(numCols);

  ArrayMatrix avIn(numCols, ArrayVector(0, NULLPTR)),
      avOut(numCols, ArrayVector(0, NULLPTR));

  for (int col = 0; col < numCols; col++) {
    cvIn.push_back(std::make_shared<ChunkedArray>(avIn[col], xFieldsIn[col]->type()));
    cvOut.push_back(std::make_shared<ChunkedArray>(avOut[col], xFieldsOut[col]->type()));
  }

  std::shared_ptr<Table> tableIn = Table::Make(sharedPtrSchemaIn, cvIn),
                         tableOut = Table::Make(sharedPtrSchemaOut, cvOut);

  std::shared_ptr<adapters::orc::ORCWriterOptions> options =
      std::make_shared<adapters::orc::ORCWriterOptions>(batchSize);
  std::unique_ptr<ORCMemWriter> writer =
      std::unique_ptr<ORCMemWriter>(new ORCMemWriter());
  std::unique_ptr<liborc::OutputStream> out_stream =
      std::unique_ptr<liborc::OutputStream>(static_cast<liborc::OutputStream*>(
          new MemoryOutputStream(DEFAULT_SMALL_MEM_STREAM_SIZE / 16)));
  ARROW_EXPECT_OK(writer->Open(sharedPtrSchemaIn, out_stream, options));
  ARROW_EXPECT_OK(writer->Write(tableIn));
  auto output_mem_stream = static_cast<MemoryOutputStream*>(writer->ReleaseOutStream());
  std::shared_ptr<io::RandomAccessFile> in_stream(
      new io::BufferReader(std::make_shared<Buffer>(
          reinterpret_cast<const uint8_t*>(output_mem_stream->getData()),
          static_cast<int64_t>(output_mem_stream->getLength()))));

  std::unique_ptr<adapters::orc::ORCFileReader> reader;
  ASSERT_TRUE(
      adapters::orc::ORCFileReader::Open(in_stream, default_memory_pool(), &reader).ok());
  std::shared_ptr<Table> outputTable;
  ARROW_EXPECT_OK(reader->Read(&outputTable));
  EXPECT_EQ(outputTable->num_columns(), numCols);
  EXPECT_EQ(outputTable->num_rows(), numRows);
  EXPECT_TRUE(outputTable->Equals(*tableOut));
}
TEST(TestAdapterWriteConvert, writeAllNulls) {
  std::vector<std::shared_ptr<Field>> xFieldsIn{
      field("date64", date64()),
      field("ts0", timestamp(TimeUnit::SECOND)),
      field("ts1", timestamp(TimeUnit::MILLI)),
      field("ts2", timestamp(TimeUnit::MICRO)),
      field("large_string", large_utf8()),
      field("large_binary", large_binary()),
      field("fixed_size_binary0", fixed_size_binary(0)),
      field("fixed_size_binary", fixed_size_binary(5))},
      xFieldsOut{field("date64", timestamp(TimeUnit::NANO)),
                 field("ts0", timestamp(TimeUnit::NANO)),
                 field("ts1", timestamp(TimeUnit::NANO)),
                 field("ts2", timestamp(TimeUnit::NANO)),
                 field("large_string", utf8()),
                 field("large_binary", binary()),
                 field("fixed_size_binary0", binary()),
                 field("fixed_size_binary", binary())};
  std::shared_ptr<Schema> sharedPtrSchemaIn = std::make_shared<Schema>(xFieldsIn),
                          sharedPtrSchemaOut = std::make_shared<Schema>(xFieldsOut);

  int64_t numRows = 10000;
  int64_t numCols = xFieldsIn.size();
  uint64_t batchSize = 500;

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

  for (int i = 0; i < numRows; i++) {
    int chunk = i < (numRows / 2) ? 1 : 3;
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

  std::shared_ptr<Table> tableIn = Table::Make(sharedPtrSchemaIn, cvIn),
                         tableOut = Table::Make(sharedPtrSchemaOut, cvOut);

  std::shared_ptr<adapters::orc::ORCWriterOptions> options =
      std::make_shared<adapters::orc::ORCWriterOptions>(batchSize);
  std::unique_ptr<ORCMemWriter> writer =
      std::unique_ptr<ORCMemWriter>(new ORCMemWriter());
  std::unique_ptr<liborc::OutputStream> out_stream =
      std::unique_ptr<liborc::OutputStream>(static_cast<liborc::OutputStream*>(
          new MemoryOutputStream(DEFAULT_SMALL_MEM_STREAM_SIZE)));
  ARROW_EXPECT_OK(writer->Open(sharedPtrSchemaIn, out_stream, options));
  ARROW_EXPECT_OK(writer->Write(tableIn));
  auto output_mem_stream = static_cast<MemoryOutputStream*>(writer->ReleaseOutStream());
  std::shared_ptr<io::RandomAccessFile> in_stream(
      new io::BufferReader(std::make_shared<Buffer>(
          reinterpret_cast<const uint8_t*>(output_mem_stream->getData()),
          static_cast<int64_t>(output_mem_stream->getLength()))));

  std::unique_ptr<adapters::orc::ORCFileReader> reader;
  ASSERT_TRUE(
      adapters::orc::ORCFileReader::Open(in_stream, default_memory_pool(), &reader).ok());
  std::shared_ptr<Table> outputTable;
  ARROW_EXPECT_OK(reader->Read(&outputTable));
  EXPECT_EQ(outputTable->num_columns(), numCols);
  EXPECT_EQ(outputTable->num_rows(), numRows);
  EXPECT_TRUE(outputTable->Equals(*tableOut));
}
TEST(TestAdapterWriteConvert, writeNoNulls) {
  std::vector<std::shared_ptr<Field>> xFieldsIn{
      field("date64", date64()),
      field("ts0", timestamp(TimeUnit::SECOND)),
      field("ts1", timestamp(TimeUnit::MILLI)),
      field("ts2", timestamp(TimeUnit::MICRO)),
      field("large_string", large_utf8()),
      field("large_binary", large_binary()),
      field("fixed_size_binary0", fixed_size_binary(0)),
      field("fixed_size_binary", fixed_size_binary(2))},
      xFieldsOut{field("date64", timestamp(TimeUnit::NANO)),
                 field("ts0", timestamp(TimeUnit::NANO)),
                 field("ts1", timestamp(TimeUnit::NANO)),
                 field("ts2", timestamp(TimeUnit::NANO)),
                 field("large_string", utf8()),
                 field("large_binary", binary()),
                 field("fixed_size_binary0", binary()),
                 field("fixed_size_binary", binary())};
  std::shared_ptr<Schema> sharedPtrSchemaIn = std::make_shared<Schema>(xFieldsIn),
                          sharedPtrSchemaOut = std::make_shared<Schema>(xFieldsOut);

  int64_t numRows = 10000;
  int64_t numCols = xFieldsIn.size();
  uint64_t batchSize = 2001;

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

  for (int64_t i = 0; i < numRows; i++) {
    int chunk = i < (numRows / 2) ? 1 : 3;
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

  std::shared_ptr<Table> tableIn = Table::Make(sharedPtrSchemaIn, cvIn),
                         tableOut = Table::Make(sharedPtrSchemaOut, cvOut);

  std::shared_ptr<adapters::orc::ORCWriterOptions> options =
      std::make_shared<adapters::orc::ORCWriterOptions>(batchSize);
  std::unique_ptr<ORCMemWriter> writer =
      std::unique_ptr<ORCMemWriter>(new ORCMemWriter());
  std::unique_ptr<liborc::OutputStream> out_stream =
      std::unique_ptr<liborc::OutputStream>(static_cast<liborc::OutputStream*>(
          new MemoryOutputStream(DEFAULT_SMALL_MEM_STREAM_SIZE)));
  ARROW_EXPECT_OK(writer->Open(sharedPtrSchemaIn, out_stream, options));
  ARROW_EXPECT_OK(writer->Write(tableIn));
  auto output_mem_stream = static_cast<MemoryOutputStream*>(writer->ReleaseOutStream());
  std::shared_ptr<io::RandomAccessFile> in_stream(
      new io::BufferReader(std::make_shared<Buffer>(
          reinterpret_cast<const uint8_t*>(output_mem_stream->getData()),
          static_cast<int64_t>(output_mem_stream->getLength()))));

  std::unique_ptr<adapters::orc::ORCFileReader> reader;
  ASSERT_TRUE(
      adapters::orc::ORCFileReader::Open(in_stream, default_memory_pool(), &reader).ok());
  std::shared_ptr<Table> outputTable;
  ARROW_EXPECT_OK(reader->Read(&outputTable));
  EXPECT_EQ(outputTable->num_columns(), numCols);
  EXPECT_EQ(outputTable->num_rows(), numRows);
  EXPECT_TRUE(outputTable->Equals(*tableOut));
}
TEST(TestAdapterWriteConvert, writeMixed) {
  std::vector<std::shared_ptr<Field>> xFieldsIn{
      field("date64", date64()),
      field("ts0", timestamp(TimeUnit::SECOND)),
      field("ts1", timestamp(TimeUnit::MILLI)),
      field("ts2", timestamp(TimeUnit::MICRO)),
      field("large_string", large_utf8()),
      field("large_binary", large_binary()),
      field("fixed_size_binary0", fixed_size_binary(0)),
      field("fixed_size_binary", fixed_size_binary(3))},
      xFieldsOut{field("date64", timestamp(TimeUnit::NANO)),
                 field("ts0", timestamp(TimeUnit::NANO)),
                 field("ts1", timestamp(TimeUnit::NANO)),
                 field("ts2", timestamp(TimeUnit::NANO)),
                 field("large_string", utf8()),
                 field("large_binary", binary()),
                 field("fixed_size_binary0", binary()),
                 field("fixed_size_binary", binary())};
  std::shared_ptr<Schema> sharedPtrSchemaIn = std::make_shared<Schema>(xFieldsIn),
                          sharedPtrSchemaOut = std::make_shared<Schema>(xFieldsOut);

  int64_t numRows = 10000;
  int64_t numCols = xFieldsIn.size();
  uint64_t batchSize = 2001;

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

  for (int64_t i = 0; i < numRows; i++) {
    int chunk = i < (numRows / 2) ? 1 : 3;
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

  std::shared_ptr<Table> tableIn = Table::Make(sharedPtrSchemaIn, cvIn),
                         tableOut = Table::Make(sharedPtrSchemaOut, cvOut);

  std::shared_ptr<adapters::orc::ORCWriterOptions> options =
      std::make_shared<adapters::orc::ORCWriterOptions>(batchSize);
  std::unique_ptr<ORCMemWriter> writer =
      std::unique_ptr<ORCMemWriter>(new ORCMemWriter());
  std::unique_ptr<liborc::OutputStream> out_stream =
      std::unique_ptr<liborc::OutputStream>(static_cast<liborc::OutputStream*>(
          new MemoryOutputStream(DEFAULT_SMALL_MEM_STREAM_SIZE)));
  ARROW_EXPECT_OK(writer->Open(sharedPtrSchemaIn, out_stream, options));
  ARROW_EXPECT_OK(writer->Write(tableIn));
  auto output_mem_stream = static_cast<MemoryOutputStream*>(writer->ReleaseOutStream());
  std::shared_ptr<io::RandomAccessFile> in_stream(
      new io::BufferReader(std::make_shared<Buffer>(
          reinterpret_cast<const uint8_t*>(output_mem_stream->getData()),
          static_cast<int64_t>(output_mem_stream->getLength()))));

  std::unique_ptr<adapters::orc::ORCFileReader> reader;
  ASSERT_TRUE(
      adapters::orc::ORCFileReader::Open(in_stream, default_memory_pool(), &reader).ok());
  std::shared_ptr<Table> outputTable;
  ARROW_EXPECT_OK(reader->Read(&outputTable));
  EXPECT_EQ(outputTable->num_columns(), numCols);
  EXPECT_EQ(outputTable->num_rows(), numRows);
  EXPECT_TRUE(outputTable->Equals(*tableOut));
}

// Nested types
TEST(TestAdapterWriteNested, writeZeroRows) {
  std::vector<std::shared_ptr<Field>> xFields0{std::make_shared<Field>("a", utf8()),
                                               std::make_shared<Field>("b", int32())};
  std::vector<std::shared_ptr<Field>> xFieldsIn{
      field("struct", struct_(xFields0)),
      field("list", list(int32())),
      field("large_list", large_list(int32())),
      field("fixed_size_list", fixed_size_list(int32(), 3)),
      field("map", map(utf8(), utf8())),
      field("lsl", list(struct_({field("lsl0", list(int32()))})))},
      xFieldsOut{
          field("struct", struct_(xFields0)),
          field("list", list(int32())),
          field("large_list", list(int32())),
          field("fixed_size_list", list(int32())),
          field("map", list(struct_({field("key", utf8()), field("value", utf8())}))),
          field("lsl", list(struct_({field("lsl0", list(int32()))})))};

  std::shared_ptr<Schema> sharedPtrSchemaIn = std::make_shared<Schema>(xFieldsIn);
  std::shared_ptr<Schema> sharedPtrSchemaOut = std::make_shared<Schema>(xFieldsOut);

  int64_t numRows = 0;
  int64_t numCols = xFieldsIn.size();
  uint64_t batchSize = 16;

  StringBuilder builder01In, keysBuilder4In, itemsBuilder4In;
  Int32Builder builder02In, valuesBuilder1In, offsetsBuilder1In, valuesBuilder2In,
      valuesBuilder3In, offsetsBuilder4In, valuesBuilder500In, offsetsBuilder500In,
      offsetsBuilder5In;
  Int64Builder offsetsBuilder2In;
  std::shared_ptr<Array> array01In, array02In, valuesArray1In, offsetsArray1In,
      valuesArray2In, offsetsArray2In, valuesArray3In, offsetsArray4In, keysArray4In,
      itemsArray4In, valuesArray500In, offsetsArray500In, offsetsArray5In;

  ARROW_EXPECT_OK(builder01In.Finish(&array01In));
  ARROW_EXPECT_OK(builder02In.Finish(&array02In));
  ARROW_EXPECT_OK(valuesBuilder1In.Finish(&valuesArray1In));
  ARROW_EXPECT_OK(offsetsBuilder1In.Append(0));
  ARROW_EXPECT_OK(offsetsBuilder1In.Finish(&offsetsArray1In));
  ARROW_EXPECT_OK(valuesBuilder2In.Finish(&valuesArray2In));
  ARROW_EXPECT_OK(offsetsBuilder2In.Append(0));
  ARROW_EXPECT_OK(offsetsBuilder2In.Finish(&offsetsArray2In));
  ARROW_EXPECT_OK(valuesBuilder3In.Finish(&valuesArray3In));
  ARROW_EXPECT_OK(keysBuilder4In.Finish(&keysArray4In));
  ARROW_EXPECT_OK(itemsBuilder4In.Finish(&itemsArray4In));
  ARROW_EXPECT_OK(offsetsBuilder4In.Append(0));
  ARROW_EXPECT_OK(offsetsBuilder4In.Finish(&offsetsArray4In));
  ARROW_EXPECT_OK(valuesBuilder500In.Finish(&valuesArray500In));
  ARROW_EXPECT_OK(offsetsBuilder500In.Append(0));
  ARROW_EXPECT_OK(offsetsBuilder500In.Finish(&offsetsArray500In));
  ARROW_EXPECT_OK(offsetsBuilder5In.Append(0));
  ARROW_EXPECT_OK(offsetsBuilder5In.Finish(&offsetsArray5In));

  ArrayVector children0In{array01In, array02In};
  auto array0In = std::make_shared<StructArray>(sharedPtrSchemaIn->field(0)->type(),
                                                numRows, children0In);
  auto array1In = ListArray::FromArrays(*offsetsArray1In, *valuesArray1In).ValueOrDie();
  auto array2In =
      LargeListArray::FromArrays(*offsetsArray2In, *valuesArray2In).ValueOrDie();
  auto array3In = std::static_pointer_cast<FixedSizeListArray>(
      FixedSizeListArray::FromArrays(valuesArray3In, 3).ValueOrDie());
  auto array4In = std::static_pointer_cast<MapArray>(
      MapArray::FromArrays(offsetsArray4In, keysArray4In, itemsArray4In).ValueOrDie());
  auto array500In =
      ListArray::FromArrays(*offsetsArray500In, *valuesArray500In).ValueOrDie();
  ArrayVector children50In{array500In};
  auto array50In = std::make_shared<StructArray>(
      sharedPtrSchemaIn->field(5)->type()->field(0)->type(), numRows, children50In);
  auto array5In = ListArray::FromArrays(*offsetsArray5In, *array50In).ValueOrDie();

  ChunkedArrayVector cvIn{
      std::make_shared<ChunkedArray>(array0In), std::make_shared<ChunkedArray>(array1In),
      std::make_shared<ChunkedArray>(array2In), std::make_shared<ChunkedArray>(array3In),
      std::make_shared<ChunkedArray>(array4In), std::make_shared<ChunkedArray>(array5In)};
  std::shared_ptr<Table> tableIn = Table::Make(sharedPtrSchemaIn, cvIn);

  StringBuilder builder01Out, keysBuilder4Out, itemsBuilder4Out;
  Int32Builder builder02Out, valuesBuilder1Out, offsetsBuilder1Out, valuesBuilder2Out,
      offsetsBuilder2Out, valuesBuilder3Out, offsetsBuilder3Out, offsetsBuilder4Out,
      valuesBuilder500Out, offsetsBuilder500Out, offsetsBuilder5Out;
  std::shared_ptr<Array> array01Out, array02Out, valuesArray1Out, offsetsArray1Out,
      valuesArray2Out, offsetsArray2Out, valuesArray3Out, offsetsArray3Out,
      offsetsArray4Out, keysArray4Out, itemsArray4Out, valuesArray500Out,
      offsetsArray500Out, offsetsArray5Out;

  ARROW_EXPECT_OK(builder01Out.Finish(&array01Out));
  ARROW_EXPECT_OK(builder02Out.Finish(&array02Out));
  ARROW_EXPECT_OK(valuesBuilder1Out.Finish(&valuesArray1Out));
  ARROW_EXPECT_OK(offsetsBuilder1Out.Append(0));
  ARROW_EXPECT_OK(offsetsBuilder1Out.Finish(&offsetsArray1Out));
  ARROW_EXPECT_OK(valuesBuilder2Out.Finish(&valuesArray2Out));
  ARROW_EXPECT_OK(offsetsBuilder2Out.Append(0));
  ARROW_EXPECT_OK(offsetsBuilder2Out.Finish(&offsetsArray2Out));
  ARROW_EXPECT_OK(valuesBuilder3Out.Finish(&valuesArray3Out));
  ARROW_EXPECT_OK(offsetsBuilder3Out.Append(0));
  ARROW_EXPECT_OK(offsetsBuilder3Out.Finish(&offsetsArray3Out));
  ARROW_EXPECT_OK(keysBuilder4Out.Finish(&keysArray4Out));
  ARROW_EXPECT_OK(itemsBuilder4Out.Finish(&itemsArray4Out));
  ARROW_EXPECT_OK(offsetsBuilder4Out.Append(0));
  ARROW_EXPECT_OK(offsetsBuilder4Out.Finish(&offsetsArray4Out));
  ARROW_EXPECT_OK(valuesBuilder500Out.Finish(&valuesArray500Out));
  ARROW_EXPECT_OK(offsetsBuilder500Out.Append(0));
  ARROW_EXPECT_OK(offsetsBuilder500Out.Finish(&offsetsArray500Out));
  ARROW_EXPECT_OK(offsetsBuilder5Out.Append(0));
  ARROW_EXPECT_OK(offsetsBuilder5Out.Finish(&offsetsArray5Out));

  ArrayVector children0Out{array01Out, array02Out},
      children4StrOut{keysArray4Out, itemsArray4Out};
  auto array0Out = std::make_shared<StructArray>(sharedPtrSchemaOut->field(0)->type(),
                                                 numRows, children0Out);
  auto array1Out =
      ListArray::FromArrays(*offsetsArray1Out, *valuesArray1Out).ValueOrDie();
  auto array2Out =
      ListArray::FromArrays(*offsetsArray2Out, *valuesArray2Out).ValueOrDie();
  auto array3Out =
      ListArray::FromArrays(*offsetsArray3Out, *valuesArray3Out).ValueOrDie();
  auto array40Out = std::make_shared<StructArray>(
      sharedPtrSchemaOut->field(4)->type()->field(0)->type(), numRows, children4StrOut);
  auto array4Out = ListArray::FromArrays(*offsetsArray4Out, *array40Out).ValueOrDie();
  auto array500Out =
      ListArray::FromArrays(*offsetsArray500Out, *valuesArray500Out).ValueOrDie();
  ArrayVector children50Out{array500Out};
  auto array50Out = std::make_shared<StructArray>(
      sharedPtrSchemaIn->field(5)->type()->field(0)->type(), numRows, children50Out);
  auto array5Out = ListArray::FromArrays(*offsetsArray5Out, *array50Out).ValueOrDie();

  ChunkedArrayVector cvOut{std::make_shared<ChunkedArray>(array0Out),
                           std::make_shared<ChunkedArray>(array1Out),
                           std::make_shared<ChunkedArray>(array2Out),
                           std::make_shared<ChunkedArray>(array3Out),
                           std::make_shared<ChunkedArray>(array4Out),
                           std::make_shared<ChunkedArray>(array5Out)};

  std::shared_ptr<Table> tableOut = Table::Make(sharedPtrSchemaOut, cvOut);

  std::shared_ptr<adapters::orc::ORCWriterOptions> options =
      std::make_shared<adapters::orc::ORCWriterOptions>(batchSize);
  std::unique_ptr<ORCMemWriter> writer =
      std::unique_ptr<ORCMemWriter>(new ORCMemWriter());
  std::unique_ptr<liborc::OutputStream> out_stream =
      std::unique_ptr<liborc::OutputStream>(static_cast<liborc::OutputStream*>(
          new MemoryOutputStream(DEFAULT_SMALL_MEM_STREAM_SIZE)));
  ARROW_EXPECT_OK(writer->Open(sharedPtrSchemaIn, out_stream, options));
  ARROW_EXPECT_OK(writer->Write(tableIn));
  auto output_mem_stream = static_cast<MemoryOutputStream*>(writer->ReleaseOutStream());
  std::shared_ptr<io::RandomAccessFile> in_stream(
      new io::BufferReader(std::make_shared<Buffer>(
          reinterpret_cast<const uint8_t*>(output_mem_stream->getData()),
          static_cast<int64_t>(output_mem_stream->getLength()))));

  std::unique_ptr<adapters::orc::ORCFileReader> reader;
  ASSERT_TRUE(
      adapters::orc::ORCFileReader::Open(in_stream, default_memory_pool(), &reader).ok());
  std::shared_ptr<Table> outputTable;
  ARROW_EXPECT_OK(reader->Read(&outputTable));
  EXPECT_EQ(outputTable->num_columns(), numCols);
  EXPECT_EQ(outputTable->num_rows(), numRows);
  EXPECT_TRUE(outputTable->Equals(*tableOut));
}
TEST(TestAdapterWriteNested, writeMixedListStruct) {
  std::vector<std::shared_ptr<Field>> xFields0{std::make_shared<Field>("a", utf8()),
                                               std::make_shared<Field>("b", int32())};
  std::vector<std::shared_ptr<Field>> xFields{field("struct", struct_(xFields0)),
                                              field("list", list(int32()))};
  std::shared_ptr<Schema> sharedPtrSchema = std::make_shared<Schema>(xFields);
  auto sharedPtrArrowType0 = xFields[0]->type();
  auto sharedPtrArrowType1 = xFields[1]->type();

  int64_t numRows = 10000;
  int64_t numCols = xFields.size();
  int64_t numCols0 = xFields0.size();
  uint64_t batchSize = 1024;

  //#0 struct<a:string,b:int>
  ArrayBuilderMatrix builders0(numCols0, ArrayBuilderVector(5, NULLPTR));

  for (int i = 0; i < 5; i++) {
    builders0[0][i] =
        std::static_pointer_cast<ArrayBuilder>(std::make_shared<StringBuilder>());
    builders0[1][i] =
        std::static_pointer_cast<ArrayBuilder>(std::make_shared<Int32Builder>());
  }
  std::string str;
  char string_[10];
  for (int i = 0; i < numRows; i++) {
    int chunk = i < (numRows / 2) ? 1 : 3;
    str = "Test " + std::to_string(i);
    snprintf(string_, sizeof(string_), "%s", str.c_str());
    if (i % 2) {
      ARROW_EXPECT_OK(
          std::static_pointer_cast<StringBuilder>(builders0[0][chunk])->Append(string_));
      ARROW_EXPECT_OK(
          std::static_pointer_cast<Int32Builder>(builders0[1][chunk])->AppendNull());
    } else {
      ARROW_EXPECT_OK(
          std::static_pointer_cast<StringBuilder>(builders0[0][chunk])->AppendNull());
      ARROW_EXPECT_OK(
          std::static_pointer_cast<Int32Builder>(builders0[1][chunk])->Append(i));
    }
  }

  int arrayBitmapSize = numRows / 16;
  uint8_t bitmaps0[2][arrayBitmapSize];
  for (int i = 0; i < arrayBitmapSize; i++) {
    for (int j = 0; j < 2; j++) {
      bitmaps0[j][i] = 153;  // 10011001
    }
  }

  std::vector<std::shared_ptr<BufferBuilder>> bufferBuilders0(
      2, std::make_shared<BufferBuilder>());
  std::vector<std::shared_ptr<Buffer>> bitmapBuffers0(2, NULLPTR);

  for (int i = 0; i < 2; i++) {
    ARROW_EXPECT_OK(bufferBuilders0[i]->Resize(arrayBitmapSize));
    ARROW_EXPECT_OK(bufferBuilders0[i]->Append(bitmaps0[i], arrayBitmapSize));
    ARROW_EXPECT_OK(bufferBuilders0[i]->Finish(&bitmapBuffers0[i]));
  }

  ArrayMatrix subarrays0(5, ArrayVector(numCols0, NULLPTR));
  ArrayVector av0;
  av0.reserve(5);

  for (int i = 0; i < 5; i++) {
    for (int col = 0; col < numCols0; col++) {
      ARROW_EXPECT_OK(builders0[col][i]->Finish(&subarrays0[i][col]));
    }
    if (i == 1 || i == 3) {
      av0.push_back(std::make_shared<StructArray>(
          sharedPtrArrowType0, numRows / 2, subarrays0[i], bitmapBuffers0[(i - 1) / 2]));
    } else {
      av0.push_back(std::make_shared<StructArray>(sharedPtrArrowType0, 0, subarrays0[i]));
    }
  }

  std::shared_ptr<ChunkedArray> carray0 = std::make_shared<ChunkedArray>(av0);

  //#1 List

  ArrayBuilderVector valuesBuilders1(5, NULLPTR), offsetsBuilders1(3, NULLPTR);
  ArrayVector valuesArrays1(5, NULLPTR), offsetsArrays1(3, NULLPTR);

  for (int i = 0; i < 5; i++) {
    valuesBuilders1[i] =
        std::static_pointer_cast<ArrayBuilder>(std::make_shared<Int32Builder>());
  }
  for (int i = 0; i < 3; i++) {
    offsetsBuilders1[i] =
        std::static_pointer_cast<ArrayBuilder>(std::make_shared<Int32Builder>());
  }
  int arrayOffsetSize = numRows / 2 + 1;
  int32_t offsets1[2][arrayOffsetSize];

  offsets1[0][0] = 0;
  offsets1[1][0] = 0;
  for (int i = 0; i < numRows; i++) {
    int offsetsChunk = i < (numRows / 2) ? 0 : 1;
    int valuesChunk = 2 * offsetsChunk + 1;
    int offsetsOffset = offsetsChunk * numRows / 2;
    switch (i % 4) {
      case 0: {
        offsets1[offsetsChunk][i + 1 - offsetsOffset] =
            offsets1[offsetsChunk][i - offsetsOffset];
        break;
      }
      case 1: {
        ARROW_EXPECT_OK(
            std::static_pointer_cast<Int32Builder>(valuesBuilders1[valuesChunk])
                ->Append(i - 1));
        offsets1[offsetsChunk][i + 1 - offsetsOffset] =
            offsets1[offsetsChunk][i - offsetsOffset] + 1;
        break;
      }
      case 2: {
        for (int j = 0; j < 8; j++) {
          ARROW_EXPECT_OK(valuesBuilders1[valuesChunk]->AppendNull());
        }
        offsets1[offsetsChunk][i + 1 - offsetsOffset] =
            offsets1[offsetsChunk][i - offsetsOffset] + 8;
        break;
      }
      default: {
        for (int j = 0; j < 3; j++) {
          ARROW_EXPECT_OK(valuesBuilders1[valuesChunk]->AppendNull());
        }
        ARROW_EXPECT_OK(
            std::static_pointer_cast<Int32Builder>(valuesBuilders1[valuesChunk])
                ->Append(i - 1));
        for (int j = 0; j < 3; j++) {
          ARROW_EXPECT_OK(valuesBuilders1[valuesChunk]->AppendNull());
        }
        offsets1[offsetsChunk][i + 1 - offsetsOffset] =
            offsets1[offsetsChunk][i - offsetsOffset] + 7;
      }
    }
  }

  uint8_t bitmaps1[2][arrayBitmapSize];
  for (int i = 0; i < arrayBitmapSize; i++) {
    for (int j = 0; j < 2; j++) {
      bitmaps1[j][i] = 165;  // 10100101
    }
  }

  BufferBuilderVector bitmapBufferBuilders1(2, std::make_shared<BufferBuilder>()),
      offsetsBufferBuilders1(2, std::make_shared<BufferBuilder>());
  BufferVector bitmapBuffers1(2, NULLPTR), offsetsBuffers1(2, NULLPTR);

  int arrayOffsetSizeBytes = 4 * arrayOffsetSize;

  for (int i = 0; i < 2; i++) {
    ARROW_EXPECT_OK(bitmapBufferBuilders1[i]->Resize(arrayBitmapSize));
    ARROW_EXPECT_OK(bitmapBufferBuilders1[i]->Append(bitmaps1[i], arrayBitmapSize));
    ARROW_EXPECT_OK(bitmapBufferBuilders1[i]->Finish(&bitmapBuffers1[i]));
    ARROW_EXPECT_OK(offsetsBufferBuilders1[i]->Resize(arrayOffsetSizeBytes));
    ARROW_EXPECT_OK(offsetsBufferBuilders1[i]->Append(offsets1[i], arrayOffsetSizeBytes));
    ARROW_EXPECT_OK(offsetsBufferBuilders1[i]->Finish(&offsetsBuffers1[i]));
  }
  for (int i = 0; i < 3; i++) {
    ARROW_EXPECT_OK(
        std::static_pointer_cast<Int32Builder>(offsetsBuilders1[i])->Append(0));
    ARROW_EXPECT_OK(offsetsBuilders1[i]->Finish(&offsetsArrays1[i]));
  }

  ArrayVector av1;
  av1.reserve(5);

  for (int i = 0; i < 5; i++) {
    ARROW_EXPECT_OK(valuesBuilders1[i]->Finish(&valuesArrays1[i]));
    if (i == 1 || i == 3) {
      av1.push_back(std::make_shared<ListArray>(
          sharedPtrArrowType1, numRows / 2, offsetsBuffers1[(i - 1) / 2],
          valuesArrays1[i], bitmapBuffers1[(i - 1) / 2]));
    } else {
      av1.push_back(
          ListArray::FromArrays(*offsetsArrays1[i / 2], *valuesArrays1[i]).ValueOrDie());
    }
  }

  std::shared_ptr<ChunkedArray> carray1 = std::make_shared<ChunkedArray>(av1);

  ChunkedArrayVector cv{carray0, carray1};  //, carray1};
  std::shared_ptr<Table> table = Table::Make(sharedPtrSchema, cv);

  std::shared_ptr<adapters::orc::ORCWriterOptions> options =
      std::make_shared<adapters::orc::ORCWriterOptions>(batchSize);
  std::unique_ptr<ORCMemWriter> writer =
      std::unique_ptr<ORCMemWriter>(new ORCMemWriter());
  std::unique_ptr<liborc::OutputStream> out_stream =
      std::unique_ptr<liborc::OutputStream>(static_cast<liborc::OutputStream*>(
          new MemoryOutputStream(DEFAULT_SMALL_MEM_STREAM_SIZE)));
  ARROW_EXPECT_OK(writer->Open(sharedPtrSchema, out_stream, options));
  ARROW_EXPECT_OK(writer->Write(table));
  auto output_mem_stream = static_cast<MemoryOutputStream*>(writer->ReleaseOutStream());
  std::shared_ptr<io::RandomAccessFile> in_stream(
      new io::BufferReader(std::make_shared<Buffer>(
          reinterpret_cast<const uint8_t*>(output_mem_stream->getData()),
          static_cast<int64_t>(output_mem_stream->getLength()))));

  std::unique_ptr<adapters::orc::ORCFileReader> reader;
  ASSERT_TRUE(
      adapters::orc::ORCFileReader::Open(in_stream, default_memory_pool(), &reader).ok());
  std::shared_ptr<Table> outputTable;
  ARROW_EXPECT_OK(reader->Read(&outputTable));
  EXPECT_EQ(outputTable->num_columns(), numCols);
  EXPECT_EQ(outputTable->num_rows(), numRows);
  EXPECT_TRUE(outputTable->Equals(*table));
}
TEST(TestAdapterWriteNested, writeMixedConvert) {
  std::vector<std::shared_ptr<Field>> xFieldsIn{
      field("large_list", large_list(int32())),
      field("fixed_size_list", fixed_size_list(int32(), 3)),
      field("map", map(int32(), int32()))},
      xFieldsOut{
          field("large_list", list(int32())), field("fixed_size_list", list(int32())),
          field("map", list(struct_({field("key", int32()), field("value", int32())})))};

  std::shared_ptr<Schema> sharedPtrSchemaIn = std::make_shared<Schema>(xFieldsIn);
  std::shared_ptr<Schema> sharedPtrSchemaOut = std::make_shared<Schema>(xFieldsOut);

  int64_t numRows = 10000;
  int64_t numCols = xFieldsIn.size();
  uint64_t batchSize = 1024;

  ArrayBuilderVector valuesBuilders0In(5, NULLPTR), offsetsBuilders0In(3, NULLPTR),
      valuesBuilders1In(5, NULLPTR), keysBuilders2In(5, NULLPTR),
      itemsBuilders2In(5, NULLPTR), offsetsBuilders2In(3, NULLPTR);
  ArrayVector valuesArrays0In(5, NULLPTR), offsetsArrays0In(3, NULLPTR),
      valuesArrays1In(5, NULLPTR), keysArrays2In(5, NULLPTR), itemsArrays2In(5, NULLPTR),
      offsetsArrays2In(3, NULLPTR);
  std::shared_ptr<ArrayBuilder>
      valuesBuilder0Out =
          std::static_pointer_cast<ArrayBuilder>(std::make_shared<Int32Builder>()),
      valuesBuilder1Out =
          std::static_pointer_cast<ArrayBuilder>(std::make_shared<Int32Builder>()),
      keysBuilder2Out =
          std::static_pointer_cast<ArrayBuilder>(std::make_shared<Int32Builder>()),
      itemsBuilder2Out =
          std::static_pointer_cast<ArrayBuilder>(std::make_shared<Int32Builder>());
  std::shared_ptr<Array> valuesArray0Out, valuesArray1Out, keysArray2Out, itemsArray2Out;

  for (int i = 0; i < 5; i++) {
    valuesBuilders0In[i] =
        std::static_pointer_cast<ArrayBuilder>(std::make_shared<Int32Builder>());
    valuesBuilders1In[i] =
        std::static_pointer_cast<ArrayBuilder>(std::make_shared<Int32Builder>());
    keysBuilders2In[i] =
        std::static_pointer_cast<ArrayBuilder>(std::make_shared<Int32Builder>());
    itemsBuilders2In[i] =
        std::static_pointer_cast<ArrayBuilder>(std::make_shared<Int32Builder>());
  }
  for (int i = 0; i < 3; i++) {
    offsetsBuilders0In[i] =
        std::static_pointer_cast<ArrayBuilder>(std::make_shared<Int64Builder>());
    offsetsBuilders2In[i] =
        std::static_pointer_cast<ArrayBuilder>(std::make_shared<Int32Builder>());
  }
  int arrayOffsetSizeIn = numRows / 2 + 1;
  int arrayOffsetSizeOut = numRows + 1;
  int64_t offsets0In[2][arrayOffsetSizeIn];
  int32_t offsets2In[2][arrayOffsetSizeIn];
  int32_t offsetsOut[numCols][arrayOffsetSizeOut];

  offsets0In[0][0] = 0;
  offsets0In[1][0] = 0;
  offsets2In[0][0] = 0;
  offsets2In[1][0] = 0;
  for (int col = 0; col < numCols; col++) {
    offsetsOut[col][0] = 0;
  }
  for (int i = 0; i < numRows; i++) {
    int offsetsChunk = i < (numRows / 2) ? 0 : 1;
    int valuesChunk = 2 * offsetsChunk + 1;
    int offsetsOffset = offsetsChunk * numRows / 2;
    switch (i % 4) {
      case 0: {
        offsets0In[offsetsChunk][i + 1 - offsetsOffset] =
            offsets0In[offsetsChunk][i - offsetsOffset];
        offsetsOut[0][i + 1] = offsetsOut[0][i];
        for (int j = 0; j < 4; j++) {
          ARROW_EXPECT_OK(
              std::static_pointer_cast<Int32Builder>(keysBuilders2In[valuesChunk])
                  ->Append(2 * i + j));
          ARROW_EXPECT_OK(
              std::static_pointer_cast<Int32Builder>(itemsBuilders2In[valuesChunk])
                  ->Append((2 * i + j) % 8));
          ARROW_EXPECT_OK(
              std::static_pointer_cast<Int32Builder>(keysBuilder2Out)->Append(2 * i + j));
          ARROW_EXPECT_OK(std::static_pointer_cast<Int32Builder>(itemsBuilder2Out)
                              ->Append((2 * i + j) % 8));
        }
        offsets2In[offsetsChunk][i + 1 - offsetsOffset] =
            offsets2In[offsetsChunk][i - offsetsOffset] + 4;
        offsetsOut[2][i + 1] = offsetsOut[2][i] + 4;
        break;
      }
      case 1: {
        ARROW_EXPECT_OK(
            std::static_pointer_cast<Int32Builder>(valuesBuilders0In[valuesChunk])
                ->Append(i - 1));
        ARROW_EXPECT_OK(
            std::static_pointer_cast<Int32Builder>(valuesBuilder0Out)->Append(i - 1));
        offsets0In[offsetsChunk][i + 1 - offsetsOffset] =
            offsets0In[offsetsChunk][i - offsetsOffset] + 1;
        offsetsOut[0][i + 1] = offsetsOut[0][i] + 1;
        for (int j = 0; j < 3; j++) {
          ARROW_EXPECT_OK(
              std::static_pointer_cast<Int32Builder>(keysBuilders2In[valuesChunk])
                  ->Append(2 * i + 2 + j));
          ARROW_EXPECT_OK(std::static_pointer_cast<Int32Builder>(keysBuilder2Out)
                              ->Append(2 * i + 2 + j));
          if (j != 1) {
            ARROW_EXPECT_OK(itemsBuilders2In[valuesChunk]->AppendNull());
            ARROW_EXPECT_OK(itemsBuilder2Out->AppendNull());
          } else {
            ARROW_EXPECT_OK(
                std::static_pointer_cast<Int32Builder>(itemsBuilders2In[valuesChunk])
                    ->Append((2 * i + 2 + j) % 8));
            ARROW_EXPECT_OK(std::static_pointer_cast<Int32Builder>(itemsBuilder2Out)
                                ->Append((2 * i + j + 2) % 8));
          }
        }
        offsets2In[offsetsChunk][i + 1 - offsetsOffset] =
            offsets2In[offsetsChunk][i - offsetsOffset] + 3;
        offsetsOut[2][i + 1] = offsetsOut[2][i] + 3;
        break;
      }
      case 2: {
        for (int j = 0; j < 8; j++) {
          ARROW_EXPECT_OK(valuesBuilders0In[valuesChunk]->AppendNull());
          ARROW_EXPECT_OK(valuesBuilder0Out->AppendNull());
        }
        offsets0In[offsetsChunk][i + 1 - offsetsOffset] =
            offsets0In[offsetsChunk][i - offsetsOffset] + 8;
        offsetsOut[0][i + 1] = offsetsOut[0][i] + 8;
        ARROW_EXPECT_OK(
            std::static_pointer_cast<Int32Builder>(keysBuilders2In[valuesChunk])
                ->Append(2 * i + 3));
        ARROW_EXPECT_OK(
            std::static_pointer_cast<Int32Builder>(itemsBuilders2In[valuesChunk])
                ->Append((2 * i + 3) % 8));

        offsets2In[offsetsChunk][i + 1 - offsetsOffset] =
            offsets2In[offsetsChunk][i - offsetsOffset] + 1;
        ARROW_EXPECT_OK(
            std::static_pointer_cast<Int32Builder>(keysBuilder2Out)->Append(2 * i + 3));
        ARROW_EXPECT_OK(std::static_pointer_cast<Int32Builder>(itemsBuilder2Out)
                            ->Append((2 * i + 3) % 8));
        offsetsOut[2][i + 1] = offsetsOut[2][i] + 1;
        break;
      }
      default: {
        for (int j = 0; j < 3; j++) {
          ARROW_EXPECT_OK(valuesBuilders0In[valuesChunk]->AppendNull());
          ARROW_EXPECT_OK(valuesBuilder0Out->AppendNull());
        }
        ARROW_EXPECT_OK(
            std::static_pointer_cast<Int32Builder>(valuesBuilders0In[valuesChunk])
                ->Append(i - 1));
        ARROW_EXPECT_OK(
            std::static_pointer_cast<Int32Builder>(valuesBuilder0Out)->Append(i - 1));
        for (int j = 0; j < 3; j++) {
          ARROW_EXPECT_OK(valuesBuilders0In[valuesChunk]->AppendNull());
          ARROW_EXPECT_OK(valuesBuilder0Out->AppendNull());
        }
        offsets0In[offsetsChunk][i + 1 - offsetsOffset] =
            offsets0In[offsetsChunk][i - offsetsOffset] + 7;
        offsetsOut[0][i + 1] = offsetsOut[0][i] + 7;
        offsets2In[offsetsChunk][i + 1 - offsetsOffset] =
            offsets2In[offsetsChunk][i - offsetsOffset];
        offsetsOut[2][i + 1] = offsetsOut[2][i];
      }
    }
    ARROW_EXPECT_OK(std::static_pointer_cast<Int32Builder>(valuesBuilders1In[valuesChunk])
                        ->Append(2 * i));
    ARROW_EXPECT_OK(valuesBuilders1In[valuesChunk]->AppendNull());
    ARROW_EXPECT_OK(valuesBuilders1In[valuesChunk]->AppendNull());
    ARROW_EXPECT_OK(
        std::static_pointer_cast<Int32Builder>(valuesBuilder1Out)->Append(2 * i));
    ARROW_EXPECT_OK(valuesBuilder1Out->AppendNull());
    ARROW_EXPECT_OK(valuesBuilder1Out->AppendNull());
    offsetsOut[1][i + 1] = offsetsOut[1][i] + 3;
  }

  int arrayBitmapSizeIn = numRows / 16, arrayBitmapSizeOut = numRows / 8;

  uint8_t bitmapsIn[numCols][2][arrayBitmapSizeIn],
      bitmapsOut[numCols][arrayBitmapSizeOut];
  for (int i = 0; i < arrayBitmapSizeIn; i++) {
    for (int j = 0; j < 2; j++) {
      bitmapsIn[0][j][i] = 90;   // 01011010
      bitmapsIn[1][j][i] = 195;  // 11000011
      bitmapsIn[2][j][i] = 255;  // 11111111
    }
  }
  for (int i = 0; i < arrayBitmapSizeOut; i++) {
    bitmapsOut[0][i] = 90;   // 01011010
    bitmapsOut[1][i] = 195;  // 11000011
    // This is a reader bug we need to fix
    bitmapsOut[2][i] = 255;  // 11111111
  }

  BufferBuilderMatrix bitmapBufferBuildersIn(numCols, BufferBuilderVector(2, NULLPTR));
  BufferMatrix bitmapBuffersIn(numCols, BufferVector(2, NULLPTR));
  for (int i = 0; i < numCols; i++) {
    for (int j = 0; j < 2; j++) {
      bitmapBufferBuildersIn[i][j] = std::make_shared<BufferBuilder>();
    }
  }

  BufferBuilderVector offsetsBufferBuilders0In(2, std::make_shared<BufferBuilder>()),
      offsetsBufferBuilders2In(2, std::make_shared<BufferBuilder>());
  BufferVector offsetsBuffers0In(2, NULLPTR), offsetsBuffers2In(2, NULLPTR);
  BufferBuilderVector bitmapBufferBuildersOut(numCols, std::make_shared<BufferBuilder>()),
      offsetsBufferBuildersOut(numCols, std::make_shared<BufferBuilder>());
  BufferVector bitmapBuffersOut(numCols, NULLPTR), offsetsBuffersOut(numCols, NULLPTR);

  int arrayOffsetSizeBytesInLarge = 8 * arrayOffsetSizeIn;
  int arrayOffsetSizeBytesInNormal = 4 * arrayOffsetSizeIn;
  int arrayOffsetSizeBytesOutNormal = 4 * arrayOffsetSizeOut;

  for (int j = 0; j < numCols; j++) {
    for (int i = 0; i < 2; i++) {
      ARROW_EXPECT_OK(bitmapBufferBuildersIn[j][i]->Resize(arrayBitmapSizeIn));
      ARROW_EXPECT_OK(
          bitmapBufferBuildersIn[j][i]->Append(bitmapsIn[j][i], arrayBitmapSizeIn));
      ARROW_EXPECT_OK(bitmapBufferBuildersIn[j][i]->Finish(&bitmapBuffersIn[j][i]));
    }
  }

  for (int i = 0; i < 2; i++) {
    ARROW_EXPECT_OK(offsetsBufferBuilders0In[i]->Resize(arrayOffsetSizeBytesInLarge));
    ARROW_EXPECT_OK(
        offsetsBufferBuilders0In[i]->Append(offsets0In[i], arrayOffsetSizeBytesInLarge));
    ARROW_EXPECT_OK(offsetsBufferBuilders0In[i]->Finish(&offsetsBuffers0In[i]));

    ARROW_EXPECT_OK(offsetsBufferBuilders2In[i]->Resize(arrayOffsetSizeBytesInNormal));
    ARROW_EXPECT_OK(
        offsetsBufferBuilders2In[i]->Append(offsets2In[i], arrayOffsetSizeBytesInNormal));
    ARROW_EXPECT_OK(offsetsBufferBuilders2In[i]->Finish(&offsetsBuffers2In[i]));
  }

  for (int j = 0; j < numCols; j++) {
    ARROW_EXPECT_OK(bitmapBufferBuildersOut[j]->Resize(arrayBitmapSizeOut));
    ARROW_EXPECT_OK(
        bitmapBufferBuildersOut[j]->Append(bitmapsOut[j], arrayBitmapSizeOut));
    ARROW_EXPECT_OK(bitmapBufferBuildersOut[j]->Finish(&bitmapBuffersOut[j]));
    ARROW_EXPECT_OK(offsetsBufferBuildersOut[j]->Resize(arrayOffsetSizeBytesOutNormal));
    ARROW_EXPECT_OK(offsetsBufferBuildersOut[j]->Append(offsetsOut[j],
                                                        arrayOffsetSizeBytesOutNormal));
    ARROW_EXPECT_OK(offsetsBufferBuildersOut[j]->Finish(&offsetsBuffersOut[j]));
  }

  for (int i = 0; i < 3; i++) {
    ARROW_EXPECT_OK(
        std::static_pointer_cast<Int64Builder>(offsetsBuilders0In[i])->Append(0));
    ARROW_EXPECT_OK(offsetsBuilders0In[i]->Finish(&offsetsArrays0In[i]));
    ARROW_EXPECT_OK(
        std::static_pointer_cast<Int32Builder>(offsetsBuilders2In[i])->Append(0));
    ARROW_EXPECT_OK(offsetsBuilders2In[i]->Finish(&offsetsArrays2In[i]));
  }

  ArrayMatrix arraysIn(numCols, ArrayVector(5, NULLPTR));
  ArrayVector arraysOut(numCols, NULLPTR);

  ChunkedArrayVector cvIn, cvOut;
  cvIn.reserve(numCols);
  cvOut.reserve(numCols);

  for (int i = 0; i < 5; i++) {
    ARROW_EXPECT_OK(valuesBuilders0In[i]->Finish(&valuesArrays0In[i]));
    ARROW_EXPECT_OK(valuesBuilders1In[i]->Finish(&valuesArrays1In[i]));
    ARROW_EXPECT_OK(keysBuilders2In[i]->Finish(&keysArrays2In[i]));
    ARROW_EXPECT_OK(itemsBuilders2In[i]->Finish(&itemsArrays2In[i]));
    if (i == 1 || i == 3) {
      arraysIn[0][i] = std::make_shared<LargeListArray>(
          xFieldsIn[0]->type(), numRows / 2, offsetsBuffers0In[(i - 1) / 2],
          valuesArrays0In[i], bitmapBuffersIn[0][(i - 1) / 2]);
      arraysIn[1][i] = std::make_shared<FixedSizeListArray>(
          xFieldsIn[1]->type(), numRows / 2, valuesArrays1In[i],
          bitmapBuffersIn[1][(i - 1) / 2]);
      arraysIn[2][i] = std::make_shared<MapArray>(
          xFieldsIn[2]->type(), numRows / 2, offsetsBuffers2In[(i - 1) / 2],
          keysArrays2In[i], itemsArrays2In[i], bitmapBuffersIn[2][(i - 1) / 2]);
    } else {
      arraysIn[0][i] =
          LargeListArray::FromArrays(*offsetsArrays0In[i / 2], *valuesArrays0In[i])
              .ValueOrDie();
      arraysIn[1][i] = std::static_pointer_cast<FixedSizeListArray>(
          FixedSizeListArray::FromArrays(valuesArrays1In[i], 3).ValueOrDie());
      arraysIn[2][i] = std::static_pointer_cast<MapArray>(
          MapArray::FromArrays(offsetsArrays2In[i / 2], keysArrays2In[i],
                               itemsArrays2In[i])
              .ValueOrDie());
    }
  }
  ARROW_EXPECT_OK(valuesBuilder0Out->Finish(&valuesArray0Out));
  ARROW_EXPECT_OK(valuesBuilder1Out->Finish(&valuesArray1Out));
  ARROW_EXPECT_OK(keysBuilder2Out->Finish(&keysArray2Out));
  ARROW_EXPECT_OK(itemsBuilder2Out->Finish(&itemsArray2Out));
  arraysOut[0] =
      std::make_shared<ListArray>(xFieldsOut[0]->type(), numRows, offsetsBuffersOut[0],
                                  valuesArray0Out, bitmapBuffersOut[0]);
  arraysOut[1] =
      std::make_shared<ListArray>(xFieldsOut[1]->type(), numRows, offsetsBuffersOut[1],
                                  valuesArray1Out, bitmapBuffersOut[1]);

  ArrayVector children20{keysArray2Out, itemsArray2Out};
  auto arraysOut20 = std::make_shared<StructArray>(
      xFieldsOut[2]->type()->field(0)->type(), 2 * numRows, children20);
  arraysOut[2] =
      std::make_shared<ListArray>(xFieldsOut[2]->type(), numRows, offsetsBuffersOut[2],
                                  arraysOut20, bitmapBuffersOut[2]);

  for (int col = 0; col < numCols; col++) {
    cvIn.push_back(std::make_shared<ChunkedArray>(arraysIn[col]));
    cvOut.push_back(std::make_shared<ChunkedArray>(arraysOut[col]));
  }

  std::shared_ptr<Table> tableIn = Table::Make(sharedPtrSchemaIn, cvIn),
                         tableOut = Table::Make(sharedPtrSchemaOut, cvOut);

  std::shared_ptr<adapters::orc::ORCWriterOptions> options =
      std::make_shared<adapters::orc::ORCWriterOptions>(batchSize);
  std::unique_ptr<ORCMemWriter> writer =
      std::unique_ptr<ORCMemWriter>(new ORCMemWriter());
  std::unique_ptr<liborc::OutputStream> out_stream =
      std::unique_ptr<liborc::OutputStream>(static_cast<liborc::OutputStream*>(
          new MemoryOutputStream(DEFAULT_SMALL_MEM_STREAM_SIZE)));
  ARROW_EXPECT_OK(writer->Open(sharedPtrSchemaIn, out_stream, options));
  ARROW_EXPECT_OK(writer->Write(tableIn));
  auto output_mem_stream = static_cast<MemoryOutputStream*>(writer->ReleaseOutStream());
  std::shared_ptr<io::RandomAccessFile> in_stream(
      new io::BufferReader(std::make_shared<Buffer>(
          reinterpret_cast<const uint8_t*>(output_mem_stream->getData()),
          static_cast<int64_t>(output_mem_stream->getLength()))));

  std::unique_ptr<adapters::orc::ORCFileReader> reader;
  ASSERT_TRUE(
      adapters::orc::ORCFileReader::Open(in_stream, default_memory_pool(), &reader).ok());
  std::shared_ptr<Table> outputTable;
  ARROW_EXPECT_OK(reader->Read(&outputTable));
  EXPECT_EQ(outputTable->num_columns(), numCols);
  EXPECT_EQ(outputTable->num_rows(), numRows);
  EXPECT_TRUE(outputTable->Equals(*tableOut));
}
TEST(TestAdapterWriteNested, writeMixedStructStruct) {
  std::vector<std::shared_ptr<Field>> xFields{field(
      "struct",
      struct_(
          {field("struct2",
                 struct_({field("bool", boolean()), field("int8", int8()),
                          field("int16", int16()), field("int32", int32()),
                          field("int64", int64()), field("decimal128nz", decimal(38, 6)),
                          field("decimal128z", decimal(38, 0)), field("date32", date32()),
                          field("ts3", timestamp(TimeUnit::NANO)),
                          field("string", utf8()), field("binary", binary())}))}))};
  std::shared_ptr<Schema> sharedPtrSchema = std::make_shared<Schema>(xFields);

  int64_t numRows = 10000;
  int64_t numCols = xFields.size();
  int64_t numCols00 = (xFields[0]->type()->field(0)->type()->fields()).size();
  auto sharedPtrArrowType00 = xFields[0]->type()->field(0)->type();
  auto sharedPtrArrowType0 = xFields[0]->type();
  uint64_t batchSize = 1;

  ArrayBuilderMatrix builders(numCols00, ArrayBuilderVector(5, NULLPTR));

  for (int i = 0; i < 5; i++) {
    builders[0][i] =
        std::static_pointer_cast<ArrayBuilder>(std::make_shared<BooleanBuilder>());
    builders[1][i] =
        std::static_pointer_cast<ArrayBuilder>(std::make_shared<Int8Builder>());
    builders[2][i] =
        std::static_pointer_cast<ArrayBuilder>(std::make_shared<Int16Builder>());
    builders[3][i] =
        std::static_pointer_cast<ArrayBuilder>(std::make_shared<Int32Builder>());
    builders[4][i] =
        std::static_pointer_cast<ArrayBuilder>(std::make_shared<Int64Builder>());
    builders[5][i] = std::static_pointer_cast<ArrayBuilder>(
        std::make_shared<Decimal128Builder>(decimal(38, 6)));
    builders[6][i] = std::static_pointer_cast<ArrayBuilder>(
        std::make_shared<Decimal128Builder>(decimal(38, 0)));
    builders[7][i] =
        std::static_pointer_cast<ArrayBuilder>(std::make_shared<Date32Builder>());
    builders[8][i] =
        std::static_pointer_cast<ArrayBuilder>(std::make_shared<TimestampBuilder>(
            timestamp(TimeUnit::NANO), default_memory_pool()));
    builders[9][i] =
        std::static_pointer_cast<ArrayBuilder>(std::make_shared<StringBuilder>());
    builders[10][i] =
        std::static_pointer_cast<ArrayBuilder>(std::make_shared<BinaryBuilder>());
  }
  char bin1[2], bin2[] = "", string_[12];
  std::string str;
  for (int64_t i = 0; i < numRows / 2; i++) {
    if (i % 2) {
      bin1[0] = i % 256;
      bin1[1] = (i - 1) % 256;
      ARROW_EXPECT_OK(
          std::static_pointer_cast<BooleanBuilder>(builders[0][1])->Append(true));
      ARROW_EXPECT_OK(
          std::static_pointer_cast<Int8Builder>(builders[1][1])->AppendNull());
      ARROW_EXPECT_OK(std::static_pointer_cast<Int16Builder>(builders[2][1])->Append(i));
      ARROW_EXPECT_OK(
          std::static_pointer_cast<Int32Builder>(builders[3][1])->AppendNull());
      ARROW_EXPECT_OK(std::static_pointer_cast<Int64Builder>(builders[4][1])->Append(i));
      ARROW_EXPECT_OK(
          std::static_pointer_cast<Decimal128Builder>(builders[5][1])->AppendNull());
      ARROW_EXPECT_OK(std::static_pointer_cast<Decimal128Builder>(builders[6][1])
                          ->Append(Decimal128(std::to_string(i))));
      ARROW_EXPECT_OK(
          std::static_pointer_cast<Date32Builder>(builders[7][1])->AppendNull());
      ARROW_EXPECT_OK(std::static_pointer_cast<TimestampBuilder>(builders[8][1])
                          ->Append(INT64_C(1605548719999999999) + 2 * i));
      ARROW_EXPECT_OK(
          std::static_pointer_cast<StringBuilder>(builders[9][1])->AppendNull());
      ARROW_EXPECT_OK(
          std::static_pointer_cast<BinaryBuilder>(builders[10][1])->Append(bin1, 2));
    } else {
      str = "Arrow " + std::to_string(2 * i);
      snprintf(string_, sizeof(string_), "%s", str.c_str());
      ARROW_EXPECT_OK(
          std::static_pointer_cast<BooleanBuilder>(builders[0][1])->AppendNull());
      ARROW_EXPECT_OK(
          std::static_pointer_cast<Int8Builder>(builders[1][1])->Append(-(i % 128)));
      ARROW_EXPECT_OK(
          std::static_pointer_cast<Int16Builder>(builders[2][1])->AppendNull());
      ARROW_EXPECT_OK(std::static_pointer_cast<Int32Builder>(builders[3][1])->Append(-i));
      ARROW_EXPECT_OK(
          std::static_pointer_cast<Int64Builder>(builders[4][1])->AppendNull());
      ARROW_EXPECT_OK(std::static_pointer_cast<Decimal128Builder>(builders[5][1])
                          ->Append(Decimal128(std::to_string(i) + ".567890")));
      ARROW_EXPECT_OK(
          std::static_pointer_cast<Decimal128Builder>(builders[6][1])->AppendNull());
      ARROW_EXPECT_OK(
          std::static_pointer_cast<Date32Builder>(builders[7][1])->Append(18600 + i));
      ARROW_EXPECT_OK(
          std::static_pointer_cast<TimestampBuilder>(builders[8][1])->AppendNull());
      ARROW_EXPECT_OK(
          std::static_pointer_cast<StringBuilder>(builders[9][1])->Append(string_));
      ARROW_EXPECT_OK(
          std::static_pointer_cast<BinaryBuilder>(builders[10][1])->AppendNull());
    }
  }
  for (int64_t i = numRows / 2; i < numRows; i++) {
    if (i % 2) {
      ARROW_EXPECT_OK(
          std::static_pointer_cast<BooleanBuilder>(builders[0][3])->Append(false));
      ARROW_EXPECT_OK(
          std::static_pointer_cast<Int8Builder>(builders[1][3])->AppendNull());
      ARROW_EXPECT_OK(std::static_pointer_cast<Int16Builder>(builders[2][3])->Append(-i));
      ARROW_EXPECT_OK(
          std::static_pointer_cast<Int32Builder>(builders[3][3])->AppendNull());
      ARROW_EXPECT_OK(std::static_pointer_cast<Int64Builder>(builders[4][3])->Append(-i));
      ARROW_EXPECT_OK(
          std::static_pointer_cast<Decimal128Builder>(builders[5][3])->AppendNull());
      ARROW_EXPECT_OK(std::static_pointer_cast<Decimal128Builder>(builders[6][3])
                          ->Append(Decimal128(std::to_string(-i))));
      ARROW_EXPECT_OK(
          std::static_pointer_cast<Date32Builder>(builders[7][3])->AppendNull());
      ARROW_EXPECT_OK(std::static_pointer_cast<TimestampBuilder>(builders[8][3])
                          ->Append(INT64_C(1606548719999999999) - INT64_C(29) * i));
      ARROW_EXPECT_OK(
          std::static_pointer_cast<StringBuilder>(builders[9][3])->AppendNull());
      ARROW_EXPECT_OK(
          std::static_pointer_cast<BinaryBuilder>(builders[10][3])->Append(bin2, 0));
    } else {
      str = "Arrow " + std::to_string(2 * i);
      snprintf(string_, sizeof(string_), "%s", str.c_str());
      ARROW_EXPECT_OK(
          std::static_pointer_cast<BooleanBuilder>(builders[0][3])->AppendNull());
      ARROW_EXPECT_OK(
          std::static_pointer_cast<Int8Builder>(builders[1][3])->Append(i % 128));
      ARROW_EXPECT_OK(
          std::static_pointer_cast<Int16Builder>(builders[2][3])->AppendNull());
      ARROW_EXPECT_OK(std::static_pointer_cast<Int32Builder>(builders[3][3])->Append(i));
      ARROW_EXPECT_OK(
          std::static_pointer_cast<Int64Builder>(builders[4][3])->AppendNull());
      ARROW_EXPECT_OK(std::static_pointer_cast<Decimal128Builder>(builders[5][3])
                          ->Append(Decimal128(std::to_string(-i) + ".543211")));
      ARROW_EXPECT_OK(
          std::static_pointer_cast<Decimal128Builder>(builders[6][3])->AppendNull());
      ARROW_EXPECT_OK(
          std::static_pointer_cast<Date32Builder>(builders[7][3])->Append(18600 - i));
      ARROW_EXPECT_OK(
          std::static_pointer_cast<TimestampBuilder>(builders[8][3])->AppendNull());
      ARROW_EXPECT_OK(
          std::static_pointer_cast<StringBuilder>(builders[9][3])->Append(string_));
      ARROW_EXPECT_OK(
          std::static_pointer_cast<BinaryBuilder>(builders[10][3])->AppendNull());
    }
  }

  int arrayBitmapSize = numRows / 16;
  uint8_t bitmaps[2][arrayBitmapSize], bitmaps0[2][arrayBitmapSize];
  for (int i = 0; i < arrayBitmapSize; i++) {
    for (int j = 0; j < 2; j++) {
      bitmaps0[j][i] = 191;  // 10111111
      bitmaps[j][i] = 241;   // 11110001
    }
  }

  std::vector<std::shared_ptr<BufferBuilder>> bufferBuilders(
      2, std::make_shared<BufferBuilder>()),
      bufferBuilders0(2, std::make_shared<BufferBuilder>());
  std::vector<std::shared_ptr<Buffer>> bitmapBuffers(2, NULLPTR),
      bitmapBuffers0(2, NULLPTR);

  for (int i = 0; i < 2; i++) {
    ARROW_EXPECT_OK(bufferBuilders[i]->Resize(arrayBitmapSize));
    ARROW_EXPECT_OK(bufferBuilders[i]->Append(bitmaps[i], arrayBitmapSize));
    ARROW_EXPECT_OK(bufferBuilders[i]->Finish(&bitmapBuffers[i]));
    ARROW_EXPECT_OK(bufferBuilders0[i]->Resize(arrayBitmapSize));
    ARROW_EXPECT_OK(bufferBuilders0[i]->Append(bitmaps0[i], arrayBitmapSize));
    ARROW_EXPECT_OK(bufferBuilders0[i]->Finish(&bitmapBuffers0[i]));
  }

  ArrayMatrix subarrays00(5, ArrayVector(numCols00, NULLPTR)),
      subarrays0(5, ArrayVector(1, NULLPTR));
  ArrayVector av0;
  av0.reserve(5);

  for (int i = 0; i < 5; i++) {
    for (int col = 0; col < numCols00; col++) {
      ARROW_EXPECT_OK(builders[col][i]->Finish(&subarrays00[i][col]));
    }
    if (i == 1 || i == 3) {
      subarrays0[i][0] = std::make_shared<StructArray>(
          sharedPtrArrowType00, numRows / 2, subarrays00[i], bitmapBuffers0[(i - 1) / 2]);
      av0.push_back(std::make_shared<StructArray>(
          sharedPtrArrowType0, numRows / 2, subarrays0[i], bitmapBuffers[(i - 1) / 2]));
    } else {
      subarrays0[i][0] =
          std::make_shared<StructArray>(sharedPtrArrowType00, 0, subarrays00[i]);
      av0.push_back(std::make_shared<StructArray>(sharedPtrArrowType0, 0, subarrays0[i]));
    }
  }

  std::shared_ptr<ChunkedArray> carray0 = std::make_shared<ChunkedArray>(av0);

  ChunkedArrayVector cv{carray0};

  std::shared_ptr<Table> table = Table::Make(sharedPtrSchema, cv);

  std::shared_ptr<adapters::orc::ORCWriterOptions> options =
      std::make_shared<adapters::orc::ORCWriterOptions>(batchSize);
  std::unique_ptr<ORCMemWriter> writer =
      std::unique_ptr<ORCMemWriter>(new ORCMemWriter());
  std::unique_ptr<liborc::OutputStream> out_stream =
      std::unique_ptr<liborc::OutputStream>(static_cast<liborc::OutputStream*>(
          new MemoryOutputStream(DEFAULT_SMALL_MEM_STREAM_SIZE * 5)));
  ARROW_EXPECT_OK(writer->Open(sharedPtrSchema, out_stream, options));
  ARROW_EXPECT_OK(writer->Write(table));
  auto output_mem_stream = static_cast<MemoryOutputStream*>(writer->ReleaseOutStream());
  std::shared_ptr<io::RandomAccessFile> in_stream(
      new io::BufferReader(std::make_shared<Buffer>(
          reinterpret_cast<const uint8_t*>(output_mem_stream->getData()),
          static_cast<int64_t>(output_mem_stream->getLength()))));

  std::unique_ptr<adapters::orc::ORCFileReader> reader;
  ASSERT_TRUE(
      adapters::orc::ORCFileReader::Open(in_stream, default_memory_pool(), &reader).ok());
  std::shared_ptr<Table> outputTable;
  ARROW_EXPECT_OK(reader->Read(&outputTable));
  EXPECT_EQ(outputTable->num_columns(), numCols);
  EXPECT_EQ(outputTable->num_rows(), numRows);
  EXPECT_TRUE(outputTable->Equals(*table));
}
TEST(TestAdapterWriteNested, writeMixedListOfStruct) {
  std::vector<std::shared_ptr<Field>> xFields{
      field("ls", list(struct_({field("a", int32())})))};
  std::shared_ptr<Schema> sharedPtrSchema = std::make_shared<Schema>(xFields);
  auto sharedPtrArrowType0 = xFields[0]->type();
  auto sharedPtrArrowType00 = xFields[0]->type()->field(0)->type();

  int64_t numRows = 10000;
  int64_t numListElememts = numRows * 2;
  // int64_t numCols = xFields.size();
  int64_t numCols0 = (xFields[0]->type()->field(0)->type()->fields()).size();
  uint64_t batchSize = 2000;

  //#0 list<struct<a:int>>
  ArrayBuilderMatrix builders0(numCols0, ArrayBuilderVector(5, NULLPTR));

  for (int i = 0; i < 5; i++) {
    builders0[0][i] =
        std::static_pointer_cast<ArrayBuilder>(std::make_shared<Int32Builder>());
  }
  for (int i = 0; i < numListElememts; i++) {
    int chunk = i < (numListElememts / 2) ? 1 : 3;
    if (i % 2) {
      ARROW_EXPECT_OK(
          std::static_pointer_cast<Int32Builder>(builders0[0][chunk])->AppendNull());
    } else {
      ARROW_EXPECT_OK(
          std::static_pointer_cast<Int32Builder>(builders0[0][chunk])->Append(i));
    }
  }
  ArrayBuilderVector offsetsBuilders(3, NULLPTR);
  ArrayVector offsetsArrays(3, NULLPTR);

  for (int i = 0; i < 3; i++) {
    offsetsBuilders[i] =
        std::static_pointer_cast<ArrayBuilder>(std::make_shared<Int32Builder>());
  }
  int arrayOffsetSize = numRows / 2 + 1;
  int32_t offsets[2][arrayOffsetSize];

  offsets[0][0] = 0;
  offsets[1][0] = 0;

  for (int i = 0; i < numRows; i++) {
    int offsetsChunk = i < (numRows / 2) ? 0 : 1;
    int offsetsOffset = offsetsChunk * numRows / 2;
    switch (i % 4) {
      case 0: {
        offsets[offsetsChunk][i + 1 - offsetsOffset] =
            offsets[offsetsChunk][i - offsetsOffset] + 4;
        break;
      }
      case 1: {
        offsets[offsetsChunk][i + 1 - offsetsOffset] =
            offsets[offsetsChunk][i - offsetsOffset] + 3;
        break;
      }
      case 2: {
        offsets[offsetsChunk][i + 1 - offsetsOffset] =
            offsets[offsetsChunk][i - offsetsOffset] + 1;
        break;
      }
      default: {
        offsets[offsetsChunk][i + 1 - offsetsOffset] =
            offsets[offsetsChunk][i - offsetsOffset];
        break;
      }
    }
  }

  int arrayBitmapSize = numRows / 16, arrayBitmapSize0 = numListElememts / 16;
  uint8_t bitmaps[2][arrayBitmapSize], bitmaps0[2][arrayBitmapSize0];
  for (int i = 0; i < arrayBitmapSize0; i++) {
    for (int j = 0; j < 2; j++) {
      bitmaps0[j][i] = 153;  // 10011001
    }
  }
  for (int i = 0; i < arrayBitmapSize; i++) {
    for (int j = 0; j < 2; j++) {
      bitmaps[j][i] = 102;  // 01100110
    }
  }

  std::vector<std::shared_ptr<BufferBuilder>> bufferBuilders0(
      2, std::make_shared<BufferBuilder>()),
      bufferBuilders(2, std::make_shared<BufferBuilder>()),
      offsetsBufferBuilders(2, std::make_shared<BufferBuilder>());
  ;
  std::vector<std::shared_ptr<Buffer>> bitmapBuffers0(2, NULLPTR),
      bitmapBuffers(2, NULLPTR), offsetsBuffers(2, NULLPTR);

  int arrayOffsetSizeBytes = 4 * arrayOffsetSize;

  for (int i = 0; i < 2; i++) {
    ARROW_EXPECT_OK(bufferBuilders0[i]->Resize(arrayBitmapSize0));
    ARROW_EXPECT_OK(bufferBuilders0[i]->Append(bitmaps0[i], arrayBitmapSize0));
    ARROW_EXPECT_OK(bufferBuilders0[i]->Finish(&bitmapBuffers0[i]));
    ARROW_EXPECT_OK(bufferBuilders[i]->Resize(arrayBitmapSize));
    ARROW_EXPECT_OK(bufferBuilders[i]->Append(bitmaps[i], arrayBitmapSize));
    ARROW_EXPECT_OK(bufferBuilders[i]->Finish(&bitmapBuffers[i]));
    ARROW_EXPECT_OK(offsetsBufferBuilders[i]->Resize(arrayOffsetSizeBytes));
    ARROW_EXPECT_OK(offsetsBufferBuilders[i]->Append(offsets[i], arrayOffsetSizeBytes));
    ARROW_EXPECT_OK(offsetsBufferBuilders[i]->Finish(&offsetsBuffers[i]));
  }

  ArrayMatrix subarrays0(5, ArrayVector(numCols0, NULLPTR));
  ArrayVector subarrays(5, NULLPTR);

  ArrayVector av;
  av.reserve(5);

  for (int i = 0; i < 3; i++) {
    ARROW_EXPECT_OK(
        std::static_pointer_cast<Int32Builder>(offsetsBuilders[i])->Append(0));
    ARROW_EXPECT_OK(offsetsBuilders[i]->Finish(&offsetsArrays[i]));
  }

  for (int i = 0; i < 5; i++) {
    for (int col = 0; col < numCols0; col++) {
      ARROW_EXPECT_OK(builders0[col][i]->Finish(&subarrays0[i][col]));
    }
    if (i == 1 || i == 3) {
      subarrays[i] = std::make_shared<StructArray>(
          sharedPtrArrowType00, numRows, subarrays0[i], bitmapBuffers0[(i - 1) / 2]);
      av.push_back(std::make_shared<ListArray>(sharedPtrArrowType0, numRows / 2,
                                               offsetsBuffers[(i - 1) / 2], subarrays[i],
                                               bitmapBuffers[(i - 1) / 2]));
    } else {
      subarrays[i] =
          std::make_shared<StructArray>(sharedPtrArrowType00, 0, subarrays0[i]);
      av.push_back(
          ListArray::FromArrays(*offsetsArrays[i / 2], *subarrays[i]).ValueOrDie());
    }
  }

  std::shared_ptr<ChunkedArray> carray0 = std::make_shared<ChunkedArray>(av);

  ChunkedArrayVector cv{carray0};
  std::shared_ptr<Table> table = Table::Make(sharedPtrSchema, cv);

  std::shared_ptr<adapters::orc::ORCWriterOptions> options =
      std::make_shared<adapters::orc::ORCWriterOptions>(batchSize);
  std::unique_ptr<ORCMemWriter> writer =
      std::unique_ptr<ORCMemWriter>(new ORCMemWriter());
  std::unique_ptr<liborc::OutputStream> out_stream =
      std::unique_ptr<liborc::OutputStream>(static_cast<liborc::OutputStream*>(
          new MemoryOutputStream(DEFAULT_SMALL_MEM_STREAM_SIZE)));
  ARROW_EXPECT_OK(writer->Open(sharedPtrSchema, out_stream, options));
  ARROW_EXPECT_OK(writer->Write(table));
  auto output_mem_stream = static_cast<MemoryOutputStream*>(writer->ReleaseOutStream());
  std::shared_ptr<io::RandomAccessFile> in_stream(
      new io::BufferReader(std::make_shared<Buffer>(
          reinterpret_cast<const uint8_t*>(output_mem_stream->getData()),
          static_cast<int64_t>(output_mem_stream->getLength()))));

  std::unique_ptr<adapters::orc::ORCFileReader> reader;
  ASSERT_TRUE(
      adapters::orc::ORCFileReader::Open(in_stream, default_memory_pool(), &reader).ok());
  std::shared_ptr<Table> outputTable;
  ARROW_EXPECT_OK(reader->Read(&outputTable));
  EXPECT_EQ(outputTable->num_columns(), numCols);
  EXPECT_EQ(outputTable->num_rows(), numRows);
  EXPECT_TRUE(outputTable->Equals(*table));
}

}  // namespace arrow
