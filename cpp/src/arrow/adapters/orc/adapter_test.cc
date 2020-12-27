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
constexpr int DEFAULT_SMALL_MEM_STREAM_SIZE = 16384 * 5;

using ArrayBuilderVector = std::vector<std::shared_ptr<ArrayBuilder>>;
using ArrayBuilderMatrix = std::vector<ArrayBuilderVector>;
using ArrayMatrix = std::vector<ArrayVector>;

// Used in testing of FillBatch functions for nested types
int64_t testListOffsetGenerator(int64_t index) {
  switch (index % 4) {
    case 0:
      return index * 3 / 2;
    case 1:
    case 3:
      return (3 * index - 3) / 2;
    default:
      return (3 * index - 4) / 2;
  }
}

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
  std::vector<std::shared_ptr<Array>> arrays(numCols, NULLPTR);
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

  builders[0] = ArrayBuilderVector(
      5, std::static_pointer_cast<ArrayBuilder>(std::make_shared<BooleanBuilder>()));
  builders[1] = ArrayBuilderVector(
      5, std::static_pointer_cast<ArrayBuilder>(std::make_shared<Int8Builder>()));
  builders[2] = ArrayBuilderVector(
      5, std::static_pointer_cast<ArrayBuilder>(std::make_shared<Int16Builder>()));
  builders[3] = ArrayBuilderVector(
      5, std::static_pointer_cast<ArrayBuilder>(std::make_shared<Int32Builder>()));
  builders[4] = ArrayBuilderVector(
      5, std::static_pointer_cast<ArrayBuilder>(std::make_shared<Int64Builder>()));
  builders[5] =
      ArrayBuilderVector(5, std::static_pointer_cast<ArrayBuilder>(
                                std::make_shared<Decimal128Builder>(decimal(33, 4))));
  builders[6] =
      ArrayBuilderVector(5, std::static_pointer_cast<ArrayBuilder>(
                                std::make_shared<Decimal128Builder>(decimal(35, 0))));
  builders[7] = ArrayBuilderVector(
      5, std::static_pointer_cast<ArrayBuilder>(std::make_shared<Date32Builder>()));
  builders[8] = ArrayBuilderVector(
      5, std::static_pointer_cast<ArrayBuilder>(std::make_shared<TimestampBuilder>(
             timestamp(TimeUnit::NANO), default_memory_pool())));
  builders[9] = ArrayBuilderVector(
      5, std::static_pointer_cast<ArrayBuilder>(std::make_shared<StringBuilder>()));
  builders[10] = ArrayBuilderVector(
      5, std::static_pointer_cast<ArrayBuilder>(std::make_shared<BinaryBuilder>()));

  for (int i = 0; i < numRows / 2; i++) {
    for (int col = 0; col < numCols; col++) {
      ARROW_EXPECT_OK(builders[col][1]->AppendNull());
    }
  }
  for (int i = numRows / 2; i < numRows; i++) {
    for (int col = 0; col < numCols; col++) {
      ARROW_EXPECT_OK(builders[col][3]->AppendNull());
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

  builders[0] = ArrayBuilderVector(
      5, std::static_pointer_cast<ArrayBuilder>(std::make_shared<BooleanBuilder>()));
  builders[1] = ArrayBuilderVector(
      5, std::static_pointer_cast<ArrayBuilder>(std::make_shared<Int8Builder>()));
  builders[2] = ArrayBuilderVector(
      5, std::static_pointer_cast<ArrayBuilder>(std::make_shared<Int16Builder>()));
  builders[3] = ArrayBuilderVector(
      5, std::static_pointer_cast<ArrayBuilder>(std::make_shared<Int32Builder>()));
  builders[4] = ArrayBuilderVector(
      5, std::static_pointer_cast<ArrayBuilder>(std::make_shared<Int64Builder>()));
  builders[5] =
      ArrayBuilderVector(5, std::static_pointer_cast<ArrayBuilder>(
                                std::make_shared<Decimal128Builder>(decimal(36, 2))));
  builders[6] =
      ArrayBuilderVector(5, std::static_pointer_cast<ArrayBuilder>(
                                std::make_shared<Decimal128Builder>(decimal(31, 0))));
  builders[7] = ArrayBuilderVector(
      5, std::static_pointer_cast<ArrayBuilder>(std::make_shared<Date32Builder>()));
  builders[8] = ArrayBuilderVector(
      5, std::static_pointer_cast<ArrayBuilder>(std::make_shared<TimestampBuilder>(
             timestamp(TimeUnit::NANO), default_memory_pool())));
  builders[9] = ArrayBuilderVector(
      5, std::static_pointer_cast<ArrayBuilder>(std::make_shared<StringBuilder>()));
  builders[10] = ArrayBuilderVector(
      5, std::static_pointer_cast<ArrayBuilder>(std::make_shared<BinaryBuilder>()));

  char bin[2], string_[13];
  std::string str;
  for (int64_t i = 0; i < numRows / 2; i++) {
    bin[0] = i % 128;
    bin[1] = bin[0];
    str = "Arrow " + std::to_string(2 * i);
    strcpy(string_, str.c_str());
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
    strcpy(string_, str.c_str());
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

  builders[0] = ArrayBuilderVector(
      5, std::static_pointer_cast<ArrayBuilder>(std::make_shared<BooleanBuilder>()));
  builders[1] = ArrayBuilderVector(
      5, std::static_pointer_cast<ArrayBuilder>(std::make_shared<Int8Builder>()));
  builders[2] = ArrayBuilderVector(
      5, std::static_pointer_cast<ArrayBuilder>(std::make_shared<Int16Builder>()));
  builders[3] = ArrayBuilderVector(
      5, std::static_pointer_cast<ArrayBuilder>(std::make_shared<Int32Builder>()));
  builders[4] = ArrayBuilderVector(
      5, std::static_pointer_cast<ArrayBuilder>(std::make_shared<Int64Builder>()));
  builders[5] =
      ArrayBuilderVector(5, std::static_pointer_cast<ArrayBuilder>(
                                std::make_shared<Decimal128Builder>(decimal(38, 6))));
  builders[6] =
      ArrayBuilderVector(5, std::static_pointer_cast<ArrayBuilder>(
                                std::make_shared<Decimal128Builder>(decimal(38, 0))));
  builders[7] = ArrayBuilderVector(
      5, std::static_pointer_cast<ArrayBuilder>(std::make_shared<Date32Builder>()));
  builders[8] = ArrayBuilderVector(
      5, std::static_pointer_cast<ArrayBuilder>(std::make_shared<TimestampBuilder>(
             timestamp(TimeUnit::NANO), default_memory_pool())));
  builders[9] = ArrayBuilderVector(
      5, std::static_pointer_cast<ArrayBuilder>(std::make_shared<StringBuilder>()));
  builders[10] = ArrayBuilderVector(
      5, std::static_pointer_cast<ArrayBuilder>(std::make_shared<BinaryBuilder>()));

  char bin1[2], bin2[] = "";
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
      ARROW_EXPECT_OK(std::static_pointer_cast<StringBuilder>(builders[9][1])
                          ->Append("Arrow " + std::to_string(2 * i)));
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
      ARROW_EXPECT_OK(std::static_pointer_cast<StringBuilder>(builders[9][3])
                          ->Append("Arrow " + std::to_string(2 * i)));
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

  FloatBuilder builder00, builder01, builder02, builder03, builder04;
  DoubleBuilder builder10, builder11, builder12, builder13, builder14;

  for (int i = 0; i < numRows / 2; i++) {
    ARROW_EXPECT_OK(builder01.AppendNull());
    ARROW_EXPECT_OK(builder11.AppendNull());
  }
  for (int i = numRows / 2; i < numRows; i++) {
    ARROW_EXPECT_OK(builder03.AppendNull());
    ARROW_EXPECT_OK(builder13.AppendNull());
  }
  std::shared_ptr<Array> array00, array01, array02, array03, array04, array10, array11,
      array12, array13, array14;
  ARROW_EXPECT_OK(builder00.Finish(&array00));
  ARROW_EXPECT_OK(builder01.Finish(&array01));
  ARROW_EXPECT_OK(builder02.Finish(&array02));
  ARROW_EXPECT_OK(builder03.Finish(&array03));
  ARROW_EXPECT_OK(builder04.Finish(&array04));
  ARROW_EXPECT_OK(builder10.Finish(&array10));
  ARROW_EXPECT_OK(builder11.Finish(&array11));
  ARROW_EXPECT_OK(builder12.Finish(&array12));
  ARROW_EXPECT_OK(builder13.Finish(&array13));
  ARROW_EXPECT_OK(builder14.Finish(&array14));

  ArrayVector av0{array00, array01, array02, array03, array04},
      av1{array10, array11, array12, array13, array14};
  std::shared_ptr<ChunkedArray> carray0 = std::make_shared<ChunkedArray>(av0);
  std::shared_ptr<ChunkedArray> carray1 = std::make_shared<ChunkedArray>(av1);

  ChunkedArrayVector cv{carray0, carray1};
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

  FloatBuilder builder00, builder01, builder02, builder03, builder04;
  DoubleBuilder builder10, builder11, builder12, builder13, builder14;

  for (int i = 0; i < numRows / 2; i++) {
    ARROW_EXPECT_OK(builder01.Append(i + 0.7));
    ARROW_EXPECT_OK(builder11.Append(-i + 0.43));
  }
  for (int i = numRows / 2; i < numRows; i++) {
    ARROW_EXPECT_OK(builder03.Append(2 * i - 2.12));
    ARROW_EXPECT_OK(builder13.Append(3 * i + 4.12));
  }
  std::shared_ptr<Array> array00, array01, array02, array03, array04, array10, array11,
      array12, array13, array14;
  ARROW_EXPECT_OK(builder00.Finish(&array00));
  ARROW_EXPECT_OK(builder01.Finish(&array01));
  ARROW_EXPECT_OK(builder02.Finish(&array02));
  ARROW_EXPECT_OK(builder03.Finish(&array03));
  ARROW_EXPECT_OK(builder04.Finish(&array04));
  ARROW_EXPECT_OK(builder10.Finish(&array10));
  ARROW_EXPECT_OK(builder11.Finish(&array11));
  ARROW_EXPECT_OK(builder12.Finish(&array12));
  ARROW_EXPECT_OK(builder13.Finish(&array13));
  ARROW_EXPECT_OK(builder14.Finish(&array14));

  ArrayVector av0{array00, array01, array02, array03, array04},
      av1{array10, array11, array12, array13, array14};
  std::shared_ptr<ChunkedArray> carray0 = std::make_shared<ChunkedArray>(av0);
  std::shared_ptr<ChunkedArray> carray1 = std::make_shared<ChunkedArray>(av1);

  ChunkedArrayVector cv{carray0, carray1};
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

  FloatBuilder builder00, builder01, builder02, builder03, builder04;
  DoubleBuilder builder10, builder11, builder12, builder13, builder14;

  for (int i = 0; i < numRows / 2; i++) {
    if (i % 2) {
      ARROW_EXPECT_OK(builder01.Append(i + 0.7));
      ARROW_EXPECT_OK(builder11.AppendNull());
    } else {
      ARROW_EXPECT_OK(builder01.AppendNull());
      ARROW_EXPECT_OK(builder11.Append(-i + 0.43));
    }
  }
  for (int i = numRows / 2; i < numRows; i++) {
    if (i % 2) {
      ARROW_EXPECT_OK(builder03.AppendNull());
      ARROW_EXPECT_OK(builder13.Append(3 * i + 4.12));
    } else {
      ARROW_EXPECT_OK(builder03.Append(2 * i - 2.12));
      ARROW_EXPECT_OK(builder13.AppendNull());
    }
  }
  std::shared_ptr<Array> array00, array01, array02, array03, array04, array10, array11,
      array12, array13, array14;
  ARROW_EXPECT_OK(builder00.Finish(&array00));
  ARROW_EXPECT_OK(builder01.Finish(&array01));
  ARROW_EXPECT_OK(builder02.Finish(&array02));
  ARROW_EXPECT_OK(builder03.Finish(&array03));
  ARROW_EXPECT_OK(builder04.Finish(&array04));
  ARROW_EXPECT_OK(builder10.Finish(&array10));
  ARROW_EXPECT_OK(builder11.Finish(&array11));
  ARROW_EXPECT_OK(builder12.Finish(&array12));
  ARROW_EXPECT_OK(builder13.Finish(&array13));
  ARROW_EXPECT_OK(builder14.Finish(&array14));

  ArrayVector av0{array00, array01, array02, array03, array04},
      av1{array10, array11, array12, array13, array14};
  std::shared_ptr<ChunkedArray> carray0 = std::make_shared<ChunkedArray>(av0);
  std::shared_ptr<ChunkedArray> carray1 = std::make_shared<ChunkedArray>(av1);

  ChunkedArrayVector cv{carray0, carray1};
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

  buildersIn[0] = ArrayBuilderVector(
      5, std::static_pointer_cast<ArrayBuilder>(std::make_shared<Date64Builder>()));
  buildersIn[1] = ArrayBuilderVector(
      5, std::static_pointer_cast<ArrayBuilder>(std::make_shared<TimestampBuilder>(
             timestamp(TimeUnit::SECOND), default_memory_pool())));
  buildersIn[2] = ArrayBuilderVector(
      5, std::static_pointer_cast<ArrayBuilder>(std::make_shared<TimestampBuilder>(
             timestamp(TimeUnit::MILLI), default_memory_pool())));
  buildersIn[3] = ArrayBuilderVector(
      5, std::static_pointer_cast<ArrayBuilder>(std::make_shared<TimestampBuilder>(
             timestamp(TimeUnit::MICRO), default_memory_pool())));
  buildersIn[4] = ArrayBuilderVector(
      5, std::static_pointer_cast<ArrayBuilder>(std::make_shared<LargeStringBuilder>()));
  buildersIn[5] = ArrayBuilderVector(
      5, std::static_pointer_cast<ArrayBuilder>(std::make_shared<LargeBinaryBuilder>()));
  buildersIn[6] = ArrayBuilderVector(
      5, std::static_pointer_cast<ArrayBuilder>(
             std::make_shared<FixedSizeBinaryBuilder>(fixed_size_binary(0))));
  buildersIn[7] = ArrayBuilderVector(
      5, std::static_pointer_cast<ArrayBuilder>(
             std::make_shared<FixedSizeBinaryBuilder>(fixed_size_binary(5))));

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

  for (int i = 0; i < numRows / 2; i++) {
    for (int col = 0; col < numCols; col++) {
      ARROW_EXPECT_OK(buildersIn[col][1]->AppendNull());
    }
  }
  for (int i = numRows / 2; i < numRows; i++) {
    for (int col = 0; col < numCols; col++) {
      ARROW_EXPECT_OK(buildersIn[col][3]->AppendNull());
    }
  }
  for (int i = 0; i < numRows; i++) {
    for (int col = 0; col < numCols; col++) {
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

  buildersIn[0] = ArrayBuilderVector(
      5, std::static_pointer_cast<ArrayBuilder>(std::make_shared<Date64Builder>()));
  buildersIn[1] = ArrayBuilderVector(
      5, std::static_pointer_cast<ArrayBuilder>(std::make_shared<TimestampBuilder>(
             timestamp(TimeUnit::SECOND), default_memory_pool())));
  buildersIn[2] = ArrayBuilderVector(
      5, std::static_pointer_cast<ArrayBuilder>(std::make_shared<TimestampBuilder>(
             timestamp(TimeUnit::MILLI), default_memory_pool())));
  buildersIn[3] = ArrayBuilderVector(
      5, std::static_pointer_cast<ArrayBuilder>(std::make_shared<TimestampBuilder>(
             timestamp(TimeUnit::MICRO), default_memory_pool())));
  buildersIn[4] = ArrayBuilderVector(
      5, std::static_pointer_cast<ArrayBuilder>(std::make_shared<LargeStringBuilder>()));
  buildersIn[5] = ArrayBuilderVector(
      5, std::static_pointer_cast<ArrayBuilder>(std::make_shared<LargeBinaryBuilder>()));
  buildersIn[6] = ArrayBuilderVector(
      5, std::static_pointer_cast<ArrayBuilder>(
             std::make_shared<FixedSizeBinaryBuilder>(fixed_size_binary(0))));
  buildersIn[7] = ArrayBuilderVector(
      5, std::static_pointer_cast<ArrayBuilder>(
             std::make_shared<FixedSizeBinaryBuilder>(fixed_size_binary(2))));

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

  char bin1[2], bin2[3], bin3[] = "";
  for (int64_t i = 0; i < numRows / 2; i++) {
    bin1[0] = i % 256;
    bin1[1] = (i + 1) % 256;
    bin2[0] = (2 * i) % 256;
    bin2[1] = (2 * i + 1) % 256;
    bin2[2] = (i - 1) % 256;
    ARROW_EXPECT_OK(std::static_pointer_cast<Date64Builder>(buildersIn[0][1])
                        ->Append(INT64_C(1605758461555) + i));
    ARROW_EXPECT_OK(std::static_pointer_cast<TimestampBuilder>(buildersIn[1][1])
                        ->Append(INT64_C(1605758461) + i));
    ARROW_EXPECT_OK(std::static_pointer_cast<TimestampBuilder>(buildersIn[2][1])
                        ->Append(INT64_C(1605758461000) + i));
    ARROW_EXPECT_OK(std::static_pointer_cast<TimestampBuilder>(buildersIn[3][1])
                        ->Append(INT64_C(1605758461000111) + i));
    ARROW_EXPECT_OK(std::static_pointer_cast<LargeStringBuilder>(buildersIn[4][1])
                        ->Append("Arrow " + std::to_string(2 * i)));
    ARROW_EXPECT_OK(
        std::static_pointer_cast<LargeBinaryBuilder>(buildersIn[5][1])->Append(bin2, 3));
    ARROW_EXPECT_OK(
        std::static_pointer_cast<FixedSizeBinaryBuilder>(buildersIn[6][1])->Append(bin3));
    ARROW_EXPECT_OK(
        std::static_pointer_cast<FixedSizeBinaryBuilder>(buildersIn[7][1])->Append(bin1));
  }
  for (int64_t i = numRows / 2; i < numRows; i++) {
    bin1[0] = i % 256;
    bin1[1] = (i + 1) % 256;
    bin2[0] = (2 * i) % 256;
    bin2[1] = (2 * i + 1) % 256;
    bin2[2] = (i - 1) % 256;
    ARROW_EXPECT_OK(std::static_pointer_cast<Date64Builder>(buildersIn[0][3])
                        ->Append(INT64_C(1605758461555) + i));
    ARROW_EXPECT_OK(std::static_pointer_cast<TimestampBuilder>(buildersIn[1][3])
                        ->Append(INT64_C(1605758461) + i));
    ARROW_EXPECT_OK(std::static_pointer_cast<TimestampBuilder>(buildersIn[2][3])
                        ->Append(INT64_C(1605758461000) + i));
    ARROW_EXPECT_OK(std::static_pointer_cast<TimestampBuilder>(buildersIn[3][3])
                        ->Append(INT64_C(1605758461000111) + i));
    ARROW_EXPECT_OK(std::static_pointer_cast<LargeStringBuilder>(buildersIn[4][3])
                        ->Append("Arrow " + std::to_string(2 * i)));
    ARROW_EXPECT_OK(
        std::static_pointer_cast<LargeBinaryBuilder>(buildersIn[5][3])->Append(bin2, 3));
    ARROW_EXPECT_OK(
        std::static_pointer_cast<FixedSizeBinaryBuilder>(buildersIn[6][3])->Append(bin3));
    ARROW_EXPECT_OK(
        std::static_pointer_cast<FixedSizeBinaryBuilder>(buildersIn[7][3])->Append(bin1));
  }
  for (int64_t i = 0; i < numRows; i++) {
    bin1[0] = i % 256;
    bin1[1] = (i + 1) % 256;
    bin2[0] = (2 * i) % 256;
    bin2[1] = (2 * i + 1) % 256;
    bin2[2] = (i - 1) % 256;
    ARROW_EXPECT_OK(std::static_pointer_cast<TimestampBuilder>(buildersOut[0])
                        ->Append(INT64_C(1605758461555000000) + INT64_C(1000000) * i));
    ARROW_EXPECT_OK(std::static_pointer_cast<TimestampBuilder>(buildersOut[1])
                        ->Append(INT64_C(1605758461000000000) + INT64_C(1000000000) * i));
    ARROW_EXPECT_OK(std::static_pointer_cast<TimestampBuilder>(buildersOut[2])
                        ->Append(INT64_C(1605758461000000000) + INT64_C(1000000) * i));
    ARROW_EXPECT_OK(std::static_pointer_cast<TimestampBuilder>(buildersOut[3])
                        ->Append(INT64_C(1605758461000111000) + INT64_C(1000) * i));
    ARROW_EXPECT_OK(std::static_pointer_cast<StringBuilder>(buildersOut[4])
                        ->Append("Arrow " + std::to_string(2 * i)));
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

  buildersIn[0] = ArrayBuilderVector(
      5, std::static_pointer_cast<ArrayBuilder>(std::make_shared<Date64Builder>()));
  buildersIn[1] = ArrayBuilderVector(
      5, std::static_pointer_cast<ArrayBuilder>(std::make_shared<TimestampBuilder>(
             timestamp(TimeUnit::SECOND), default_memory_pool())));
  buildersIn[2] = ArrayBuilderVector(
      5, std::static_pointer_cast<ArrayBuilder>(std::make_shared<TimestampBuilder>(
             timestamp(TimeUnit::MILLI), default_memory_pool())));
  buildersIn[3] = ArrayBuilderVector(
      5, std::static_pointer_cast<ArrayBuilder>(std::make_shared<TimestampBuilder>(
             timestamp(TimeUnit::MICRO), default_memory_pool())));
  buildersIn[4] = ArrayBuilderVector(
      5, std::static_pointer_cast<ArrayBuilder>(std::make_shared<LargeStringBuilder>()));
  buildersIn[5] = ArrayBuilderVector(
      5, std::static_pointer_cast<ArrayBuilder>(std::make_shared<LargeBinaryBuilder>()));
  buildersIn[6] = ArrayBuilderVector(
      5, std::static_pointer_cast<ArrayBuilder>(
             std::make_shared<FixedSizeBinaryBuilder>(fixed_size_binary(0))));
  buildersIn[7] = ArrayBuilderVector(
      5, std::static_pointer_cast<ArrayBuilder>(
             std::make_shared<FixedSizeBinaryBuilder>(fixed_size_binary(3))));

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

  char bin1[3], bin2[4], bin3[] = "";
  for (int64_t i = 0; i < numRows; i++) {
    int chunk = i < (numRows / 2) ? 1 : 3;
    if (i % 2) {
      ARROW_EXPECT_OK(std::static_pointer_cast<Date64Builder>(buildersIn[0][chunk])
                          ->Append(INT64_C(1605758461555) + 3 * i));
      ARROW_EXPECT_OK(
          std::static_pointer_cast<TimestampBuilder>(buildersIn[1][chunk])->AppendNull());
      ARROW_EXPECT_OK(std::static_pointer_cast<TimestampBuilder>(buildersIn[2][chunk])
                          ->Append(INT64_C(1605758461000) - 14 * i));
      ARROW_EXPECT_OK(
          std::static_pointer_cast<TimestampBuilder>(buildersIn[3][chunk])->AppendNull());
      ARROW_EXPECT_OK(std::static_pointer_cast<LargeStringBuilder>(buildersIn[4][chunk])
                          ->Append("Arrow " + std::to_string(-4 * i + 8)));
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
// TEST(TestAdapterWriteNested, writeMixedListStruct) {
//   std::vector<std::shared_ptr<Field>> xFields0{std::make_shared<Field>("a", utf8()),
//                                                std::make_shared<Field>("b", int32())};
//   std::vector<std::shared_ptr<Field>> xFields{
//       field("struct", struct_(xFields0))};  //, field("list", list(int32())),
//   // field("lsl", list(struct_({field("lsl0", list(int32()))})))};
//   std::shared_ptr<Schema> sharedPtrSchema = std::make_shared<Schema>(xFields);
//   auto sharedPtrArrowType0 = xFields[0]->type();

//   int64_t numRows = 16;
//   // int64_t numCols = xFields.size();
//   int64_t numCols0 = xFields0.size();
//   uint64_t batchSize = 1024;

//   //#0
//   ArrayBuilderMatrix builders0(numCols0, ArrayBuilderVector(5, NULLPTR));

//   builders0[0] = ArrayBuilderVector(
//       5, std::static_pointer_cast<ArrayBuilder>(std::make_shared<StringBuilder>()));
//   builders0[1] = ArrayBuilderVector(
//       5, std::static_pointer_cast<ArrayBuilder>(std::make_shared<Int32Builder>()));
//   for (int i = 0; i < numRows / 2; i++) {
//     if (i % 2) {
//       ARROW_EXPECT_OK(std::static_pointer_cast<StringBuilder>(builders0[0][1])
//                           ->Append("Test " + std::to_string(i)));
//       ARROW_EXPECT_OK(
//           std::static_pointer_cast<Int32Builder>(builders0[1][1])->AppendNull());
//     } else {
//       ARROW_EXPECT_OK(
//           std::static_pointer_cast<StringBuilder>(builders0[0][1])->AppendNull());
//       ARROW_EXPECT_OK(std::static_pointer_cast<Int32Builder>(builders0[1][1])->Append(i));
//     }
//   }
//   for (int i = numRows / 2; i < numRows; i++) {
//     if (i % 2) {
//       ARROW_EXPECT_OK(std::static_pointer_cast<StringBuilder>(builders0[0][3])
//                           ->Append("Test " + std::to_string(i)));
//       ARROW_EXPECT_OK(
//           std::static_pointer_cast<Int32Builder>(builders0[1][3])->AppendNull());
//     } else {
//       ARROW_EXPECT_OK(
//           std::static_pointer_cast<StringBuilder>(builders0[0][3])->AppendNull());
//       ARROW_EXPECT_OK(std::static_pointer_cast<Int32Builder>(builders0[1][3])->Append(i));
//     }
//   }

//   int arrayBitmapSize = numRows / 16;
//   uint8_t bitmaps0[numCols0][arrayBitmapSize];
//   for (int i = 0; i < arrayBitmapSize; i++) {
//     for (int j = 0; j < numCols0; j++) {
//       bitmaps0[j][i] = 153;  // 10011001
//     }
//   }

//   std::vector<std::shared_ptr<BufferBuilder>> bufferBuilders0(
//       2, std::make_shared<BufferBuilder>());
//   std::vector<std::shared_ptr<Buffer>> bitmapBuffers0(2, NULLPTR);

//   for (int i = 0; i < 2; i++) {
//     ARROW_EXPECT_OK(bufferBuilders0[i]->Resize(arrayBitmapSize));
//     ARROW_EXPECT_OK(bufferBuilders0[i]->Append(bitmaps0[i], arrayBitmapSize));
//     ARROW_EXPECT_OK(bufferBuilders0[i]->Finish(&bitmapBuffers0[i]));
//   }

//   ArrayMatrix subarrays0(5, ArrayVector(numCols0, NULLPTR));
//   ArrayVector av;
//   av.reserve(5);

//   for (int i = 0; i < 5; i++) {
//     for (int col = 0; col < numCols0; col++) {
//       ARROW_EXPECT_OK(builders0[col][i]->Finish(&subarrays0[i][col]));
//     }
//     if (i == 1 || i == 3) {
//       RecordProperty("o0" + std::to_string(i), subarrays0[i][0]->ToString());
//       // RecordProperty("o0" + std::to_string(i), subarrays0[i][0]->);
//       RecordProperty("o1" + std::to_string(i), subarrays0[i][1]->ToString());
//       av.push_back(std::make_shared<StructArray>(
//           sharedPtrArrowType0, numRows / 2, subarrays0[i], bitmapBuffers0[(i - 1) /
//           2]));
//     } else {
//       av.push_back(std::make_shared<StructArray>(sharedPtrArrowType0, 0,
//       subarrays0[i]));
//     }
//   }

//   std::shared_ptr<ChunkedArray> carray = std::make_shared<ChunkedArray>(av);

//   ChunkedArrayVector cv{carray};
//   std::shared_ptr<Table> table = Table::Make(sharedPtrSchema, cv);

//   std::shared_ptr<adapters::orc::ORCWriterOptions> options =
//       std::make_shared<adapters::orc::ORCWriterOptions>(batchSize);
//   std::unique_ptr<adapters::orc::ORCFileWriter> writer;

//   // std::unique_ptr<liborc::OutputStream> out_stream =
//   //     std::unique_ptr<liborc::OutputStream>(static_cast<liborc::OutputStream*>(
//   //         new MemoryOutputStream(DEFAULT_SMALL_MEM_STREAM_SIZE)));
//   std::shared_ptr<io::FileOutputStream> file =
//       std::move(io::FileOutputStream::Open(
//                     "/Users/karlkatzen/Documents/code/orc-files/Structtest1.orc",
//                     false))
//           .ValueOrDie();
//   ARROW_EXPECT_OK(
//       adapters::orc::ORCFileWriter::Open(sharedPtrSchema, file, options, &writer));
//   RecordProperty("o", table->ToString());
//   // ARROW_EXPECT_OK(writer->Write(table));
//   // auto output_mem_stream =
//   // static_cast<MemoryOutputStream*>(writer->ReleaseOutStream());
//   // std::shared_ptr<io::RandomAccessFile> in_stream(
//   //     new io::BufferReader(std::make_shared<Buffer>(
//   //         reinterpret_cast<const uint8_t*>(output_mem_stream->getData()),
//   //         static_cast<int64_t>(output_mem_stream->getLength()))));

//   // std::unique_ptr<adapters::orc::ORCFileReader> reader;
//   // ASSERT_TRUE(
//   //     adapters::orc::ORCFileReader::Open(in_stream, default_memory_pool(),
//   //     &reader).ok());
//   // std::shared_ptr<Table> outputTable;
//   // ARROW_EXPECT_OK(reader->Read(&outputTable));
//   // EXPECT_EQ(outputTable->num_columns(), numCols);
//   // EXPECT_EQ(outputTable->num_rows(), numRows);
//   // EXPECT_TRUE(outputTable->Equals(*table));
// }  // namespace arrow
// // Struct
// TEST(TestAdapterWriteNested, writeStructMixedParentHasNulls) {
//   std::vector<std::shared_ptr<Field>> xFields00{std::make_shared<Field>("a", utf8()),
//                                                 std::make_shared<Field>("b", int32())};
//   std::vector<std::shared_ptr<Field>> xFields0{field("c", struct_(xFields00))};
//   std::vector<std::shared_ptr<Field>> xFields{field("struct", struct_(xFields0))};
//   std::shared_ptr<Schema> sharedPtrSchema = std::make_shared<Schema>(xFields);

//   int64_t numRows = 4;
//   // int64_t numCols = xFields.size();
//   uint64_t batchSize = 4;

//   StringBuilder builder1;
//   (void)(builder1.Append("A"));
//   (void)(builder1.Append("AB"));
//   (void)(builder1.AppendNull());
//   (void)(builder1.AppendNull());

//   Int32Builder builder2;
//   (void)(builder2.Append(3));
//   (void)(builder2.AppendNull());
//   (void)(builder2.Append(25));
//   (void)(builder2.AppendNull());

//   std::shared_ptr<Array> array1;
//   (void)(builder1.Finish(&array1));
//   std::shared_ptr<Array> array2;
//   (void)(builder2.Finish(&array2));

//   std::vector<std::shared_ptr<Array>> children;
//   children.push_back(array1);
//   children.push_back(array2);

//   uint8_t bitmap = 6;  // 00000110
//   auto maybeBuffer = AllocateBuffer(1);
//   if (!maybeBuffer.ok()) {
//     FAIL() << "Buffer not created successfully";
//   }
//   std::shared_ptr<Buffer> buffer = *std::move(maybeBuffer);
//   uint8_t* bufferData = buffer->mutable_data();
//   std::memcpy(bufferData, &bitmap, 1);

//   auto array0 = std::make_shared<StructArray>(
//       sharedPtrSchema->field(0)->type()->field(0)->type(), numRows, children, buffer);

//   std::vector<std::shared_ptr<Array>> children2{array0};

//   uint8_t bitmap2 = 12;  // 00001100
//   auto maybeBuffer2 = AllocateBuffer(1);
//   if (!maybeBuffer2.ok()) {
//     FAIL() << "Buffer not created successfully";
//   }
//   std::shared_ptr<Buffer> buffer2 = *std::move(maybeBuffer2);
//   uint8_t* bufferData2 = buffer2->mutable_data();
//   std::memcpy(bufferData2, &bitmap2, 1);

//   auto array = std::make_shared<StructArray>(sharedPtrSchema->field(0)->type(),
//   numRows,
//                                              children2, buffer2);

//   ChunkedArrayVector cv{std::make_shared<ChunkedArray>(array)};
//   std::shared_ptr<Table> table = Table::Make(sharedPtrSchema, cv);

//   std::shared_ptr<io::FileOutputStream> file =
//       std::move(io::FileOutputStream::Open(
//                     "/Users/karlkatzen/Documents/code/orc-files/struct1.orc", false))
//           .ValueOrDie();
//   std::shared_ptr<adapters::orc::ORCWriterOptions> options =
//       std::make_shared<adapters::orc::ORCWriterOptions>(batchSize);
//   std::unique_ptr<adapters::orc::ORCFileWriter> writer;
//   // std::unique_ptr<liborc::OutputStream> out_stream =
//   //     std::unique_ptr<liborc::OutputStream>(static_cast<liborc::OutputStream*>(
//   //         new MemoryOutputStream(DEFAULT_SMALL_MEM_STREAM_SIZE)));
//   ARROW_EXPECT_OK(
//       adapters::orc::ORCFileWriter::Open(sharedPtrSchema, file, options, &writer));
//   ARROW_EXPECT_OK(writer->Write(table));
//   // auto output_mem_stream =
//   // static_cast<MemoryOutputStream*>(writer->ReleaseOutStream());
//   // std::shared_ptr<io::RandomAccessFile> in_stream(
//   //     new io::BufferReader(std::make_shared<Buffer>(
//   //         reinterpret_cast<const uint8_t*>(output_mem_stream->getData()),
//   //         static_cast<int64_t>(output_mem_stream->getLength()))));

//   // std::unique_ptr<adapters::orc::ORCFileReader> reader;
//   // ASSERT_TRUE(
//   //     adapters::orc::ORCFileReader::Open(in_stream, default_memory_pool(),
//   //     &reader).ok());
//   // std::shared_ptr<Table> outputTable;
//   // ARROW_EXPECT_OK(reader->Read(&outputTable));
//   // EXPECT_EQ(outputTable->num_columns(), numCols);
//   // EXPECT_EQ(outputTable->num_rows(), numRows);
//   // EXPECT_TRUE(outputTable->Equals(*table));
// }
// TEST(TestAdapterWriteNested, writeStructChunkedMultibatchParentHasNulls) {
//   std::vector<std::shared_ptr<Field>> xFields0{std::make_shared<Field>("a", utf8()),
//                                                std::make_shared<Field>("b", int32())};
//   std::vector<std::shared_ptr<Field>> xFields{field("struct", struct_(xFields0))};
//   std::shared_ptr<Schema> sharedPtrSchema = std::make_shared<Schema>(xFields);
//   auto sharedPtrArrowType = xFields[0]->type();

//   // int64_t numRows = 100;
//   // int64_t numCols = xFields.size();
//   uint64_t batchSize = 17;

//   StringBuilder builder00, builder01, builder02, builder03, builder04;
//   Int32Builder builder10, builder11, builder12, builder13, builder14;
//   for (int i = 0; i < 50; i++) {
//     if (i % 2) {
//       (void)(builder01.Append("Test " + std::to_string(i)));
//       (void)(builder11.AppendNull());
//     } else {
//       (void)(builder01.AppendNull());
//       (void)(builder11.Append(i));
//     }
//   }
//   for (int i = 50; i < 100; i++) {
//     if (i % 2) {
//       (void)(builder03.Append("Test " + std::to_string(i)));
//       (void)(builder13.AppendNull());
//     } else {
//       (void)(builder03.AppendNull());
//       (void)(builder13.Append(i));
//     }
//   }

//   // Every third entry has null at struct level
//   uint8_t bitmap1[7] = {
//       182, 109, 219, 182,
//       109, 219, 2};  // 10110110 01101101 11011011 10110110 01101101 11011011
//   uint8_t bitmap3[7] = {
//       109, 219, 182, 109,
//       219, 182, 1};  // 01101101 11011011 10110110 01101101 11011011 10110110

//   BufferBuilder builder1, builder3;
//   (void)(builder1.Resize(7));
//   (void)(builder1.Append(bitmap1, 7));
//   std::shared_ptr<arrow::Buffer> bitmapBuffer1;
//   if (!builder1.Finish(&bitmapBuffer1).ok()) {
//     FAIL() << "The offsets buffer can not be constructed!";
//   }
//   (void)(builder3.Resize(7));
//   (void)(builder3.Append(bitmap3, 7));
//   std::shared_ptr<arrow::Buffer> bitmapBuffer3;
//   if (!builder3.Finish(&bitmapBuffer3).ok()) {
//     FAIL() << "The offsets buffer can not be constructed!";
//   }

//   std::shared_ptr<Array> array00, array01, array02, array03, array04;
//   std::shared_ptr<Array> array10, array11, array12, array13, array14;

//   (void)(builder10.Finish(&array10));
//   (void)(builder11.Finish(&array11));
//   (void)(builder12.Finish(&array12));
//   (void)(builder13.Finish(&array13));
//   (void)(builder14.Finish(&array14));
//   (void)(builder00.Finish(&array00));
//   (void)(builder01.Finish(&array01));
//   (void)(builder02.Finish(&array02));
//   (void)(builder03.Finish(&array03));
//   (void)(builder04.Finish(&array04));

//   std::vector<std::shared_ptr<Array>> children0, children1, children2, children3,
//       children4;
//   children0.push_back(array00);
//   children0.push_back(array10);
//   children1.push_back(array01);
//   children1.push_back(array11);
//   children2.push_back(array02);
//   children2.push_back(array12);
//   children3.push_back(array03);
//   children3.push_back(array13);
//   children4.push_back(array04);
//   children4.push_back(array14);

//   auto array0 = std::make_shared<StructArray>(sharedPtrArrowType, 0, children0);
//   auto array1 =
//       std::make_shared<StructArray>(sharedPtrArrowType, 50, children1, bitmapBuffer1);
//   auto array2 = std::make_shared<StructArray>(sharedPtrArrowType, 0, children2);
//   auto array3 =
//       std::make_shared<StructArray>(sharedPtrArrowType, 50, children3, bitmapBuffer3);
//   auto array4 = std::make_shared<StructArray>(sharedPtrArrowType, 0, children4);
//   ArrayVector av;
//   av.push_back(array0);
//   av.push_back(array1);
//   av.push_back(array2);
//   av.push_back(array3);
//   av.push_back(array4);
//   std::shared_ptr<ChunkedArray> carray = std::make_shared<ChunkedArray>(av);

//   ChunkedArrayVector cv{carray};
//   std::shared_ptr<Table> table = Table::Make(sharedPtrSchema, cv);

//   std::shared_ptr<adapters::orc::ORCWriterOptions> options =
//       std::make_shared<adapters::orc::ORCWriterOptions>(batchSize);
//   std::unique_ptr<adapters::orc::ORCFileWriter> writer;
//   // std::unique_ptr<liborc::OutputStream> out_stream =
//   //     std::unique_ptr<liborc::OutputStream>(static_cast<liborc::OutputStream*>(
//   //         new MemoryOutputStream(DEFAULT_SMALL_MEM_STREAM_SIZE)));
//   std::shared_ptr<io::FileOutputStream> file =
//       std::move(io::FileOutputStream::Open(
//                     "/Users/karlkatzen/Documents/code/orc-files/Structtest.orc",
//                     false))
//           .ValueOrDie();
//   ARROW_EXPECT_OK(
//       adapters::orc::ORCFileWriter::Open(sharedPtrSchema, file, options, &writer));
//   ARROW_EXPECT_OK(writer->Write(table));
//   // auto output_mem_stream =
//   // static_cast<MemoryOutputStream*>(writer->ReleaseOutStream());
//   // std::shared_ptr<io::RandomAccessFile> in_stream(
//   //     new io::BufferReader(std::make_shared<Buffer>(
//   //         reinterpret_cast<const uint8_t*>(output_mem_stream->getData()),
//   //         static_cast<int64_t>(output_mem_stream->getLength()))));

//   // std::unique_ptr<adapters::orc::ORCFileReader> reader;
//   // ASSERT_TRUE(
//   //     adapters::orc::ORCFileReader::Open(in_stream, default_memory_pool(),
//   //     &reader).ok());
//   // std::shared_ptr<Table> outputTable;
//   // ARROW_EXPECT_OK(reader->Read(&outputTable));
//   // EXPECT_EQ(outputTable->num_columns(), numCols);
//   // EXPECT_EQ(outputTable->num_rows(), numRows);
//   // EXPECT_TRUE(outputTable->Equals(*table));
// }

// TEST(TestAdapterWriteNested, writeListChunkedMultibatch) {
//   std::vector<std::shared_ptr<Field>> xFields{field("list", list(int32()))};
//   std::shared_ptr<Schema> sharedPtrSchema = std::make_shared<Schema>(xFields);
//   auto sharedPtrArrowType = xFields[0]->type();

//   int64_t numRows = 100;
//   int64_t numCols = xFields.size();
//   uint64_t batchSize = 100;

//   Int32Builder valuesBuilder1, valuesBuilder3;
//   int32_t offset1[51], offset3[51];

//   offset1[0] = 0;
//   for (int i = 0; i < 50; i++) {
//     switch (i % 4) {
//       case 0: {
//         offset1[i + 1] = offset1[i];
//         break;
//       }
//       case 1: {
//         (void)(valuesBuilder1.Append(i - 1));
//         offset1[i + 1] = offset1[i] + 1;
//         break;
//       }
//       case 2: {
//         (void)(valuesBuilder1.AppendNull());
//         (void)(valuesBuilder1.AppendNull());
//         offset1[i + 1] = offset1[i] + 2;
//         break;
//       }
//       default: {
//         (void)(valuesBuilder1.Append(i - 1));
//         (void)(valuesBuilder1.AppendNull());
//         (void)(valuesBuilder1.AppendNull());
//         offset1[i + 1] = offset1[i] + 3;
//       }
//     }
//   }

//   offset3[0] = 0;
//   for (int i = 0; i < 50; i++) {
//     switch ((i + 50) % 4) {
//       case 0: {
//         offset3[i + 1] = offset3[i];
//         break;
//       }
//       case 1: {
//         (void)(valuesBuilder3.Append(i + 50 - 1));
//         offset3[i + 1] = offset3[i] + 1;
//         break;
//       }
//       case 2: {
//         (void)(valuesBuilder3.AppendNull());
//         (void)(valuesBuilder3.AppendNull());
//         offset3[i + 1] = offset3[i] + 2;
//         break;
//       }
//       default: {
//         (void)(valuesBuilder3.Append(i + 50 - 1));
//         (void)(valuesBuilder3.AppendNull());
//         (void)(valuesBuilder3.AppendNull());
//         offset3[i + 1] = offset3[i] + 3;
//       }
//     }
//   }

//   // Every third entry has null at struct level
//   uint8_t bitmap1[7] = {
//       182, 109, 219, 182,
//       109, 219, 2};  // 10110110 01101101 11011011 10110110 01101101 11011011 00000010
//   uint8_t bitmap3[7] = {
//       109, 219, 182, 109,
//       219, 182, 1};  // 01101101 11011011 10110110 01101101 11011011 10110110 00000001

//   BufferBuilder builder1, builder3, offsetsBuilder1, offsetsBuilder3;
//   (void)(builder1.Resize(7));
//   (void)(builder1.Append(bitmap1, 7));
//   std::shared_ptr<arrow::Buffer> bitmapBuffer1;
//   if (!builder1.Finish(&bitmapBuffer1).ok()) {
//     FAIL() << "The offsets buffer can not be constructed!";
//   }
//   (void)(builder3.Resize(7));
//   (void)(builder3.Append(bitmap3, 7));
//   std::shared_ptr<arrow::Buffer> bitmapBuffer3;
//   if (!builder3.Finish(&bitmapBuffer3).ok()) {
//     FAIL() << "The offsets buffer can not be constructed!";
//   }

//   (void)(offsetsBuilder1.Resize(204));
//   (void)(offsetsBuilder1.Append(offset1, 204));
//   std::shared_ptr<arrow::Buffer> offsetsBuffer1;
//   if (!offsetsBuilder1.Finish(&offsetsBuffer1).ok()) {
//     FAIL() << "The offsets buffer can not be constructed!";
//   }
//   (void)(offsetsBuilder3.Resize(204));
//   (void)(offsetsBuilder3.Append(offset3, 204));
//   std::shared_ptr<arrow::Buffer> offsetsBuffer3;
//   if (!offsetsBuilder3.Finish(&offsetsBuffer3).ok()) {
//     FAIL() << "The offsets buffer can not be constructed!";
//   }

//   Int32Builder valuesBuilder0, offsetsBuilder0, valuesBuilder2, offsetsBuilder2,
//       valuesBuilder4, offsetsBuilder4;
//   std::shared_ptr<Array> valuesArray0, offsetsArray0, valuesArray2, offsetsArray2,
//       valuesArray4, offsetsArray4;
//   (void)(valuesBuilder0.Finish(&valuesArray0));
//   (void)(offsetsBuilder0.Append(0));
//   (void)(offsetsBuilder0.Finish(&offsetsArray0));
//   (void)(valuesBuilder2.Finish(&valuesArray2));
//   (void)(offsetsBuilder2.Append(0));
//   (void)(offsetsBuilder2.Finish(&offsetsArray2));
//   (void)(valuesBuilder4.Finish(&valuesArray4));
//   (void)(offsetsBuilder4.Append(0));
//   (void)(offsetsBuilder4.Finish(&offsetsArray4));

//   std::shared_ptr<Array> valuesArray1, valuesArray3;
//   (void)(valuesBuilder1.Finish(&valuesArray1));
//   (void)(valuesBuilder3.Finish(&valuesArray3));

//   std::shared_ptr<ListArray> array0 =
//       ListArray::FromArrays(*offsetsArray0, *valuesArray0).ValueOrDie();
//   std::shared_ptr<ListArray> array2 =
//       ListArray::FromArrays(*offsetsArray2, *valuesArray2).ValueOrDie();
//   std::shared_ptr<ListArray> array4 =
//       ListArray::FromArrays(*offsetsArray4, *valuesArray4).ValueOrDie();
//   auto array1 = std::make_shared<ListArray>(sharedPtrArrowType, 50, offsetsBuffer1,
//                                             valuesArray1, bitmapBuffer1);
//   auto array3 = std::make_shared<ListArray>(sharedPtrArrowType, 50, offsetsBuffer3,
//                                             valuesArray3, bitmapBuffer3);

//   ArrayVector av;
//   av.push_back(array0);
//   av.push_back(array1);
//   av.push_back(array2);
//   av.push_back(array3);
//   av.push_back(array4);
//   std::shared_ptr<ChunkedArray> carray = std::make_shared<ChunkedArray>(av);

//   ChunkedArrayVector cv{carray};
//   std::shared_ptr<Table> table = Table::Make(sharedPtrSchema, cv);

//   std::shared_ptr<adapters::orc::ORCWriterOptions> options =
//       std::make_shared<adapters::orc::ORCWriterOptions>(batchSize);
//   std::unique_ptr<adapters::orc::ORCFileWriter> writer;
//   std::unique_ptr<liborc::OutputStream> out_stream =
//       std::unique_ptr<liborc::OutputStream>(static_cast<liborc::OutputStream*>(
//           new MemoryOutputStream(DEFAULT_SMALL_MEM_STREAM_SIZE)));
//   // std::shared_ptr<io::FileOutputStream> file =
//   //     std::move(io::FileOutputStream::Open(
//   //                   "/Users/karlkatzen/Documents/code/orc-files/Listtest.orc",
//   //                   false))
//   //         .ValueOrDie();
//   ARROW_EXPECT_OK(
//       adapters::orc::ORCFileWriter::Open(sharedPtrSchema, out_stream, options,
//       &writer));
//   ARROW_EXPECT_OK(writer->Write(table));
//   auto output_mem_stream =
//   static_cast<MemoryOutputStream*>(writer->ReleaseOutStream());
//   std::shared_ptr<io::RandomAccessFile> in_stream(
//       new io::BufferReader(std::make_shared<Buffer>(
//           reinterpret_cast<const uint8_t*>(output_mem_stream->getData()),
//           static_cast<int64_t>(output_mem_stream->getLength()))));

//   std::unique_ptr<adapters::orc::ORCFileReader> reader;
//   ASSERT_TRUE(
//       adapters::orc::ORCFileReader::Open(in_stream, default_memory_pool(),
//       &reader).ok());
//   std::shared_ptr<Table> outputTable;
//   ARROW_EXPECT_OK(reader->Read(&outputTable));
//   EXPECT_EQ(outputTable->num_columns(), numCols);
//   EXPECT_EQ(outputTable->num_rows(), numRows);
//   EXPECT_TRUE(outputTable->Equals(*table));
// }

// LargeList
// TEST(TestAdapterWriteNested, writeLargeListChunkedMultibatch) {
//   auto sharedPtrArrowType = large_list(std::make_shared<Field>("a", int32()));
//   DataType* arrowType = sharedPtrArrowType.get();
//   int64_t totalLength = 100;
//   int64_t totalElementLength = 150;
//   int64_t batchSize = 7;
//   Int32Builder valuesBuilder1, valuesBuilder3;
//   int64_t offset1[51], offset3[51];

//   offset1[0] = 0;
//   for (int i = 0; i < 50; i++) {
//     switch (i % 4) {
//       case 0: {
//         offset1[i + 1] = offset1[i];
//         break;
//       }
//       case 1: {
//         (void)(valuesBuilder1.Append(i - 1));
//         offset1[i + 1] = offset1[i] + 1;
//         break;
//       }
//       case 2: {
//         (void)(valuesBuilder1.AppendNull());
//         (void)(valuesBuilder1.AppendNull());
//         offset1[i + 1] = offset1[i] + 2;
//         break;
//       }
//       default: {
//         (void)(valuesBuilder1.Append(i - 1));
//         (void)(valuesBuilder1.AppendNull());
//         (void)(valuesBuilder1.AppendNull());
//         offset1[i + 1] = offset1[i] + 3;
//       }
//     }
//   }

//   offset3[0] = 0;
//   for (int i = 0; i < 50; i++) {
//     switch ((i + 50) % 4) {
//       case 0: {
//         offset3[i + 1] = offset3[i];
//         break;
//       }
//       case 1: {
//         (void)(valuesBuilder3.Append(i + 50 - 1));
//         offset3[i + 1] = offset3[i] + 1;
//         break;
//       }
//       case 2: {
//         (void)(valuesBuilder3.AppendNull());
//         (void)(valuesBuilder3.AppendNull());
//         offset3[i + 1] = offset3[i] + 2;
//         break;
//       }
//       default: {
//         (void)(valuesBuilder3.Append(i + 50 - 1));
//         (void)(valuesBuilder3.AppendNull());
//         (void)(valuesBuilder3.AppendNull());
//         offset3[i + 1] = offset3[i] + 3;
//       }
//     }
//   }

//   // Every third entry has null at struct level
//   uint8_t bitmap1[7] = {
//       182, 109, 219, 182,
//       109, 219, 2};  // 10110110 01101101 11011011 10110110 01101101 11011011
//       00000010
//   uint8_t bitmap3[7] = {
//       109, 219, 182, 109,
//       219, 182, 1};  // 01101101 11011011 10110110 01101101 11011011 10110110
//       00000001

//   BufferBuilder builder1, builder3, offsetsBuilder1, offsetsBuilder3;
//   (void)(builder1.Resize(7));
//   (void)(builder1.Append(bitmap1, 7));
//   std::shared_ptr<arrow::Buffer> bitmapBuffer1;
//   if (!builder1.Finish(&bitmapBuffer1).ok()) {
//     FAIL() << "The offsets buffer can not be constructed!";
//   }
//   (void)(builder3.Resize(7));
//   (void)(builder3.Append(bitmap3, 7));
//   std::shared_ptr<arrow::Buffer> bitmapBuffer3;
//   if (!builder3.Finish(&bitmapBuffer3).ok()) {
//     FAIL() << "The offsets buffer can not be constructed!";
//   }

//   (void)(offsetsBuilder1.Resize(408));
//   (void)(offsetsBuilder1.Append(offset1, 408));
//   std::shared_ptr<arrow::Buffer> offsetsBuffer1;
//   if (!offsetsBuilder1.Finish(&offsetsBuffer1).ok()) {
//     FAIL() << "The offsets buffer can not be constructed!";
//   }
//   (void)(offsetsBuilder3.Resize(408));
//   (void)(offsetsBuilder3.Append(offset3, 408));
//   std::shared_ptr<arrow::Buffer> offsetsBuffer3;
//   if (!offsetsBuilder3.Finish(&offsetsBuffer3).ok()) {
//     FAIL() << "The offsets buffer can not be constructed!";
//   }

//   Int32Builder valuesBuilder0, valuesBuilder2, valuesBuilder4;
//   Int64Builder offsetsBuilder0, offsetsBuilder2, offsetsBuilder4;
//   std::shared_ptr<Array> valuesArray0, offsetsArray0, valuesArray2, offsetsArray2,
//       valuesArray4, offsetsArray4;
//   (void)(valuesBuilder0.Finish(&valuesArray0));
//   (void)(offsetsBuilder0.Append(0));
//   (void)(offsetsBuilder0.Finish(&offsetsArray0));
//   (void)(valuesBuilder2.Finish(&valuesArray2));
//   (void)(offsetsBuilder2.Append(0));
//   (void)(offsetsBuilder2.Finish(&offsetsArray2));
//   (void)(valuesBuilder4.Finish(&valuesArray4));
//   (void)(offsetsBuilder4.Append(0));
//   (void)(offsetsBuilder4.Finish(&offsetsArray4));

//   std::shared_ptr<Array> valuesArray1, valuesArray3;
//   (void)(valuesBuilder1.Finish(&valuesArray1));
//   (void)(valuesBuilder3.Finish(&valuesArray3));

//   std::shared_ptr<LargeListArray> array0 =
//       LargeListArray::FromArrays(*offsetsArray0, *valuesArray0).ValueOrDie();
//   std::shared_ptr<LargeListArray> array2 =
//       LargeListArray::FromArrays(*offsetsArray2, *valuesArray2).ValueOrDie();
//   std::shared_ptr<LargeListArray> array4 =
//       LargeListArray::FromArrays(*offsetsArray4, *valuesArray4).ValueOrDie();
//   auto array1 = std::make_shared<LargeListArray>(sharedPtrArrowType, 50,
//   offsetsBuffer1,
//                                                  valuesArray1, bitmapBuffer1);
//   auto array3 = std::make_shared<LargeListArray>(sharedPtrArrowType, 50,
//   offsetsBuffer3,
//                                                  valuesArray3, bitmapBuffer3);

//   ArrayVector av;
//   av.push_back(array0);
//   av.push_back(array1);
//   av.push_back(array2);
//   av.push_back(array3);
//   av.push_back(array4);
//   std::shared_ptr<ChunkedArray> carray = std::make_shared<ChunkedArray>(av);

//   MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE * 5);
//   ORC_UNIQUE_PTR<liborc::Type> schema(
//       liborc::Type::buildTypeFromString("struct<x:array<a:int>>"));
//   liborc::WriterOptions options;
//   ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream,
//   options); ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch =
//   writer->createRowBatch(batchSize); liborc::StructVectorBatch* root =
//       internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
//   liborc::ListVectorBatch* x =
//       internal::checked_cast<liborc::ListVectorBatch*>(root->fields[0]);
//   liborc::LongVectorBatch* a =
//       internal::checked_cast<liborc::LongVectorBatch*>((x->elements).get());
//   int64_t arrowIndexOffset = 0;
//   int arrowChunkOffset = 0;
//   int64_t resultOffset = 0;
//   int64_t oldValueOffset = 0, valueOffset = 0;
//   while (resultOffset < totalLength - batchSize) {
//     Status st = adapters::orc::FillBatch(arrowType, x, arrowIndexOffset,
//     arrowChunkOffset,
//                                          batchSize, carray.get());
//     if (!st.ok()) {
//       FAIL() << "ORC ColumnBatch not successfully filled";
//     }
//     writer->add(*batch);
//     // RecordProperty("aio-" + std::to_string(resultOffset), arrowIndexOffset);
//     // RecordProperty("aco-" + std::to_string(resultOffset), arrowChunkOffset);
//     for (int64_t i = 0; i < batchSize; i++) {
//       // RecordProperty("xn-res" + std::to_string(i + resultOffset), x->notNull[i]);
//       // RecordProperty("xo-res" + std::to_string(resultOffset) + "-" +
//       //                    std::to_string(i + resultOffset),
//       //                x->offsets[i]);
//       if ((i + resultOffset) % 3) {
//         EXPECT_TRUE(x->notNull[i]);
//       } else {
//         EXPECT_FALSE(x->notNull[i]);
//       }
//       EXPECT_EQ(x->offsets[i], testListOffsetGenerator(i + resultOffset) -
//                                    testListOffsetGenerator(resultOffset));
//     }
//     EXPECT_EQ(x->offsets[batchSize], testListOffsetGenerator(batchSize +
//     resultOffset)
//     -
//                                          testListOffsetGenerator(resultOffset));
//     // RecordProperty("xo-res" + std::to_string(resultOffset) + "-" +
//     //                    std::to_string(batchSize + resultOffset),
//     //                x->offsets[batchSize]);
//     resultOffset = resultOffset + batchSize;
//     oldValueOffset = valueOffset;
//     valueOffset = testListOffsetGenerator(resultOffset);
//     // RecordProperty("vo-res" + std::to_string(resultOffset), oldValueOffset);
//     // RecordProperty("vn-res" + std::to_string(resultOffset), valueOffset);
//     for (int64_t j = 0; j < valueOffset - oldValueOffset; j++) {
//       // RecordProperty("an-res" + std::to_string(j + oldValueOffset),
//       a->notNull[j]);
//       // RecordProperty("av-res" + std::to_string(j + oldValueOffset), a->data[j]);
//       if ((j + oldValueOffset) % 3) {
//         EXPECT_FALSE(a->notNull[j]);
//       } else {
//         EXPECT_TRUE(a->notNull[j]);
//         EXPECT_EQ(a->data[j], (j + oldValueOffset) * 2 / 3);
//       }
//     }
//     EXPECT_EQ(x->numElements, batchSize);
//     EXPECT_EQ(a->numElements, valueOffset - oldValueOffset);
//     EXPECT_TRUE(x->hasNulls);
//     EXPECT_TRUE(a->hasNulls);
//     batch->clear();
//   }
//   Status st = adapters::orc::FillBatch(arrowType, x, arrowIndexOffset,
//   arrowChunkOffset,
//                                        batchSize, carray.get());

//   // RecordProperty("aio-" + std::to_string(resultOffset), arrowIndexOffset);
//   // RecordProperty("aco-" + std::to_string(resultOffset), arrowChunkOffset);
//   if (!st.ok()) {
//     FAIL() << "ORC ColumnBatch not successfully filled";
//   }
//   int64_t lastBatchSize = totalLength - resultOffset;
//   EXPECT_EQ(x->numElements, lastBatchSize);
//   EXPECT_EQ(a->numElements, totalElementLength - valueOffset);
//   EXPECT_TRUE(x->hasNulls);
//   EXPECT_TRUE(a->hasNulls);
//   // RecordProperty("vo-res" + std::to_string(resultOffset), oldValueOffset);
//   // RecordProperty("vn-res" + std::to_string(resultOffset), valueOffset);
//   for (int64_t i = 0; i < lastBatchSize; i++) {
//     // RecordProperty("xn-res" + std::to_string(i + resultOffset), x->notNull[i]);
//     // RecordProperty(
//     //     "xo-res" + std::to_string(resultOffset) + "-" + std::to_string(i +
//     //     resultOffset), x->offsets[i]);
//     if ((i + resultOffset) % 3) {
//       EXPECT_TRUE(x->notNull[i]);
//     } else {
//       EXPECT_FALSE(x->notNull[i]);
//     }
//     EXPECT_EQ(x->offsets[i], testListOffsetGenerator(i + resultOffset) -
//                                  testListOffsetGenerator(resultOffset));
//   }
//   EXPECT_EQ(x->offsets[lastBatchSize],
//             totalElementLength - testListOffsetGenerator(resultOffset));
//   // RecordProperty("xo-res" + std::to_string(resultOffset) + "-" +
//   std::to_string(100),
//   //                x->offsets[lastBatchSize]);
//   oldValueOffset = valueOffset;
//   valueOffset = totalElementLength;
//   for (int64_t j = 0; j < valueOffset - oldValueOffset; j++) {
//     // RecordProperty("an-res" + std::to_string(j + oldValueOffset), a->notNull[j]);
//     // RecordProperty("av-res" + std::to_string(j + oldValueOffset), a->data[j]);
//     if ((j + oldValueOffset) % 3) {
//       EXPECT_FALSE(a->notNull[j]);
//     } else {
//       EXPECT_TRUE(a->notNull[j]);
//       EXPECT_EQ(a->data[j], (j + oldValueOffset) * 2 / 3);
//     }
//   }
//   writer->add(*batch);
//   writer->close();
// }

// FixedSizeList
// writeFixedSizeListZeroEmpty not allowed
// TEST(TestAdapterWriteNested, writeFixedSizeListChunkedMultibatch) {
//   std::vector<std::shared_ptr<Field>> xFields{field("list", fixed_size_list(int32(),
//   3))}; std::shared_ptr<Schema> sharedPtrSchema = std::make_shared<Schema>(xFields);
//   auto sharedPtrArrowType = xFields[0]->type();

//   // int64_t numRows = 100;
//   // int64_t numCols = xFields.size();
//   uint64_t batchSize = 100;

//   Int32Builder valuesBuilder1, valuesBuilder3;

//   for (int i = 0; i < 50; i++) {
//     (void)(valuesBuilder1.Append(i));
//     (void)(valuesBuilder1.AppendNull());
//     (void)(valuesBuilder1.AppendNull());
//   }

//   for (int i = 0; i < 50; i++) {
//     (void)(valuesBuilder3.Append(i + 50));
//     (void)(valuesBuilder3.AppendNull());
//     (void)(valuesBuilder3.AppendNull());
//   }

//   // Every third entry has null at struct level
//   uint8_t bitmap1[7] = {
//       182, 109, 219, 182,
//       109, 219, 2};  // 10110110 01101101 11011011 10110110 01101101 11011011 00000010
//   uint8_t bitmap3[7] = {
//       109, 219, 182, 109,
//       219, 182, 1};  // 01101101 11011011 10110110 01101101 11011011 10110110 00000001

//   BufferBuilder builder1, builder3;
//   (void)(builder1.Resize(7));
//   (void)(builder1.Append(bitmap1, 7));
//   std::shared_ptr<arrow::Buffer> bitmapBuffer1;
//   if (!builder1.Finish(&bitmapBuffer1).ok()) {
//     FAIL() << "The bitmap buffer can not be constructed!";
//   }
//   (void)(builder3.Resize(7));
//   (void)(builder3.Append(bitmap3, 7));
//   std::shared_ptr<arrow::Buffer> bitmapBuffer3;
//   if (!builder3.Finish(&bitmapBuffer3).ok()) {
//     FAIL() << "The bitmap buffer can not be constructed!";
//   }

//   Int32Builder valuesBuilder0, valuesBuilder2, valuesBuilder4;
//   std::shared_ptr<Array> valuesArray0, valuesArray2, valuesArray4;
//   (void)(valuesBuilder0.Finish(&valuesArray0));
//   (void)(valuesBuilder2.Finish(&valuesArray2));
//   (void)(valuesBuilder4.Finish(&valuesArray4));

//   std::shared_ptr<Array> valuesArray1, valuesArray3;
//   (void)(valuesBuilder1.Finish(&valuesArray1));
//   (void)(valuesBuilder3.Finish(&valuesArray3));

//   std::shared_ptr<FixedSizeListArray> array0 =
//       std::static_pointer_cast<FixedSizeListArray>(
//           FixedSizeListArray::FromArrays(valuesArray0, 3).ValueOrDie());
//   std::shared_ptr<FixedSizeListArray> array2 =
//       std::static_pointer_cast<FixedSizeListArray>(
//           FixedSizeListArray::FromArrays(valuesArray2, 3).ValueOrDie());
//   std::shared_ptr<FixedSizeListArray> array4 =
//       std::static_pointer_cast<FixedSizeListArray>(
//           FixedSizeListArray::FromArrays(valuesArray4, 3).ValueOrDie());
//   auto array1 = std::make_shared<FixedSizeListArray>(sharedPtrArrowType, 50,
//   valuesArray1,
//                                                      bitmapBuffer1);
//   auto array3 = std::make_shared<FixedSizeListArray>(sharedPtrArrowType, 50,
//   valuesArray3,
//                                                      bitmapBuffer3);

//   ArrayVector av;
//   av.push_back(array0);
//   av.push_back(array1);
//   av.push_back(array2);
//   av.push_back(array3);
//   av.push_back(array4);
//   std::shared_ptr<ChunkedArray> carray = std::make_shared<ChunkedArray>(av);

//   ChunkedArrayVector cv{carray};
//   std::shared_ptr<Table> table = Table::Make(sharedPtrSchema, cv);

//   std::shared_ptr<adapters::orc::ORCWriterOptions> options =
//       std::make_shared<adapters::orc::ORCWriterOptions>(batchSize);
//   std::unique_ptr<adapters::orc::ORCFileWriter> writer;
//   // std::unique_ptr<liborc::OutputStream> out_stream =
//   //     std::unique_ptr<liborc::OutputStream>(static_cast<liborc::OutputStream*>(
//   //         new MemoryOutputStream(DEFAULT_SMALL_MEM_STREAM_SIZE)));
//   std::shared_ptr<io::FileOutputStream> file =
//       std::move(io::FileOutputStream::Open(
//                     "/Users/karlkatzen/Documents/code/orc-files/Listtest2.orc", false))
//           .ValueOrDie();
//   ARROW_EXPECT_OK(
//       adapters::orc::ORCFileWriter::Open(sharedPtrSchema, file, options, &writer));
//   ARROW_EXPECT_OK(writer->Write(table));
//   // auto output_mem_stream =
//   // static_cast<MemoryOutputStream*>(writer->ReleaseOutStream());
//   // std::shared_ptr<io::RandomAccessFile> in_stream(
//   //     new io::BufferReader(std::make_shared<Buffer>(
//   //         reinterpret_cast<const uint8_t*>(output_mem_stream->getData()),
//   //         static_cast<int64_t>(output_mem_stream->getLength()))));

//   // std::unique_ptr<adapters::orc::ORCFileReader> reader;
//   // ASSERT_TRUE(
//   //     adapters::orc::ORCFileReader::Open(in_stream, default_memory_pool(),
//   //     &reader).ok());
//   // std::shared_ptr<Table> outputTable;
//   // ARROW_EXPECT_OK(reader->Read(&outputTable));
//   // EXPECT_EQ(outputTable->num_columns(), numCols);
//   // EXPECT_EQ(outputTable->num_rows(), numRows);
//   // EXPECT_TRUE(outputTable->Equals(*table));
// }

// Map
// TEST(TestAdapterWriteNested, writeMapChunkedMultibatch) {
//   std::vector<std::shared_ptr<Field>> xFields{field("map", map(utf8(), utf8()))};
//   std::shared_ptr<Schema> sharedPtrSchema = std::make_shared<Schema>(xFields);
//   auto sharedPtrArrowType = xFields[0]->type();

//   int64_t numRows = 100;
//   int64_t numCols = xFields.size();
//   uint64_t batchSize = 100;

//   Int32Builder offsetsBuilder0;
//   StringBuilder keysBuilder0, itemsBuilder0;
//   std::shared_ptr<Array> offsetsArray0, keysArray0, itemsArray0;
//   (void)(keysBuilder0.Finish(&keysArray0));
//   (void)(itemsBuilder0.Finish(&itemsArray0));
//   (void)(offsetsBuilder0.Append(0));
//   (void)(offsetsBuilder0.Finish(&offsetsArray0));

//   std::shared_ptr<MapArray> array0 = std::static_pointer_cast<MapArray>(
//       MapArray::FromArrays(offsetsArray0, keysArray0, itemsArray0).ValueOrDie());

//   Int32Builder offsetsBuilder2;
//   StringBuilder keysBuilder2, itemsBuilder2;
//   std::shared_ptr<Array> offsetsArray2, keysArray2, itemsArray2;
//   (void)(keysBuilder2.Finish(&keysArray2));
//   (void)(itemsBuilder2.Finish(&itemsArray2));
//   (void)(offsetsBuilder2.Append(0));
//   (void)(offsetsBuilder2.Finish(&offsetsArray2));

//   std::shared_ptr<MapArray> array2 = std::static_pointer_cast<MapArray>(
//       MapArray::FromArrays(offsetsArray2, keysArray2, itemsArray2).ValueOrDie());

//   Int32Builder offsetsBuilder4;
//   StringBuilder keysBuilder4, itemsBuilder4;
//   std::shared_ptr<Array> offsetsArray4, keysArray4, itemsArray4;
//   (void)(keysBuilder4.Finish(&keysArray4));
//   (void)(itemsBuilder4.Finish(&itemsArray4));
//   (void)(offsetsBuilder4.Append(0));
//   (void)(offsetsBuilder4.Finish(&offsetsArray4));

//   std::shared_ptr<MapArray> array4 = std::static_pointer_cast<MapArray>(
//       MapArray::FromArrays(offsetsArray4, keysArray4, itemsArray4).ValueOrDie());

//   StringBuilder keysBuilder1, itemsBuilder1, keysBuilder3, itemsBuilder3;
//   int32_t offset1[51], offset3[51];
//   int32_t key = 0;
//   offset1[0] = 0;
//   for (int i = 0; i < 50; i++) {
//     switch (i % 4) {
//       case 0: {
//         offset1[i + 1] = offset1[i];
//         break;
//       }
//       case 1: {
//         (void)(keysBuilder1.Append("Key" + std::to_string(key)));
//         (void)(itemsBuilder1.Append("Item" + std::to_string(i - 1)));
//         key++;
//         offset1[i + 1] = offset1[i] + 1;
//         break;
//       }
//       case 2: {
//         (void)(keysBuilder1.Append("Key" + std::to_string(key)));
//         (void)(keysBuilder1.Append("Key" + std::to_string(key + 1)));
//         (void)(itemsBuilder1.AppendNull());
//         (void)(itemsBuilder1.AppendNull());
//         key += 2;
//         offset1[i + 1] = offset1[i] + 2;
//         break;
//       }
//       default: {
//         (void)(keysBuilder1.Append("Key" + std::to_string(key)));
//         (void)(keysBuilder1.Append("Key" + std::to_string(key + 1)));
//         (void)(keysBuilder1.Append("Key" + std::to_string(key + 2)));
//         (void)(itemsBuilder1.Append("Item" + std::to_string(i - 1)));
//         (void)(itemsBuilder1.AppendNull());
//         (void)(itemsBuilder1.AppendNull());
//         key += 3;
//         offset1[i + 1] = offset1[i] + 3;
//       }
//     }
//   }

//   offset3[0] = 0;
//   for (int i = 0; i < 50; i++) {
//     switch ((i + 50) % 4) {
//       case 0: {
//         offset3[i + 1] = offset3[i];
//         break;
//       }
//       case 1: {
//         (void)(keysBuilder3.Append("Key" + std::to_string(key)));
//         (void)(itemsBuilder3.Append("Item" + std::to_string(i + 50 - 1)));
//         key++;
//         offset3[i + 1] = offset3[i] + 1;
//         break;
//       }
//       case 2: {
//         (void)(keysBuilder3.Append("Key" + std::to_string(key)));
//         (void)(keysBuilder3.Append("Key" + std::to_string(key + 1)));
//         (void)(itemsBuilder3.AppendNull());
//         (void)(itemsBuilder3.AppendNull());
//         key += 2;
//         offset3[i + 1] = offset3[i] + 2;
//         break;
//       }
//       default: {
//         (void)(keysBuilder3.Append("Key" + std::to_string(key)));
//         (void)(keysBuilder3.Append("Key" + std::to_string(key + 1)));
//         (void)(keysBuilder3.Append("Key" + std::to_string(key + 2)));
//         (void)(itemsBuilder3.Append("Item" + std::to_string(i + 50 - 1)));
//         (void)(itemsBuilder3.AppendNull());
//         (void)(itemsBuilder3.AppendNull());
//         key += 3;
//         offset3[i + 1] = offset3[i] + 3;
//       }
//     }
//   }

//   // Every third entry has null at struct level
//   uint8_t bitmap1[7] = {
//       182, 109, 219, 182,
//       109, 219, 2};  // 10110110 01101101 11011011 10110110 01101101 11011011
//       00000010
//   uint8_t bitmap3[7] = {
//       109, 219, 182, 109,
//       219, 182, 1};  // 01101101 11011011 10110110 01101101 11011011 10110110
//       00000001

//   BufferBuilder builder1, builder3, offsetsBuilder1, offsetsBuilder3;
//   (void)(builder1.Resize(7));
//   (void)(builder1.Append(bitmap1, 7));
//   std::shared_ptr<arrow::Buffer> bitmapBuffer1;
//   if (!builder1.Finish(&bitmapBuffer1).ok()) {
//     FAIL() << "The offsets buffer can not be constructed!";
//   }
//   (void)(builder3.Resize(7));
//   (void)(builder3.Append(bitmap3, 7));
//   std::shared_ptr<arrow::Buffer> bitmapBuffer3;
//   if (!builder3.Finish(&bitmapBuffer3).ok()) {
//     FAIL() << "The offsets buffer can not be constructed!";
//   }

//   (void)(offsetsBuilder1.Resize(204));
//   (void)(offsetsBuilder1.Append(offset1, 204));
//   std::shared_ptr<arrow::Buffer> offsetsBuffer1;
//   if (!offsetsBuilder1.Finish(&offsetsBuffer1).ok()) {
//     FAIL() << "The offsets buffer can not be constructed!";
//   }
//   (void)(offsetsBuilder3.Resize(204));
//   (void)(offsetsBuilder3.Append(offset3, 204));
//   std::shared_ptr<arrow::Buffer> offsetsBuffer3;
//   if (!offsetsBuilder3.Finish(&offsetsBuffer3).ok()) {
//     FAIL() << "The offsets buffer can not be constructed!";
//   }

//   std::shared_ptr<Array> keysArray1, keysArray3, itemsArray1, itemsArray3;
//   (void)(keysBuilder1.Finish(&keysArray1));
//   (void)(keysBuilder3.Finish(&keysArray3));
//   (void)(itemsBuilder1.Finish(&itemsArray1));
//   (void)(itemsBuilder3.Finish(&itemsArray3));

//   auto array1 = std::make_shared<MapArray>(sharedPtrArrowType, 50, offsetsBuffer1,
//                                            keysArray1, itemsArray1, bitmapBuffer1);
//   auto array3 = std::make_shared<MapArray>(sharedPtrArrowType, 50, offsetsBuffer3,
//                                            keysArray3, itemsArray3, bitmapBuffer3);

//   ArrayVector av;
//   av.push_back(array0);
//   av.push_back(array1);
//   av.push_back(array2);
//   av.push_back(array3);
//   av.push_back(array4);
//   std::shared_ptr<ChunkedArray> carray = std::make_shared<ChunkedArray>(av);

//   ChunkedArrayVector cv{carray};
//   std::shared_ptr<Table> table = Table::Make(sharedPtrSchema, cv);

//   std::shared_ptr<adapters::orc::ORCWriterOptions> options =
//       std::make_shared<adapters::orc::ORCWriterOptions>(batchSize);
//   std::unique_ptr<adapters::orc::ORCFileWriter> writer;
//   std::unique_ptr<liborc::OutputStream> out_stream =
//       std::unique_ptr<liborc::OutputStream>(static_cast<liborc::OutputStream*>(
//           new MemoryOutputStream(DEFAULT_SMALL_MEM_STREAM_SIZE)));
//   // std::shared_ptr<io::FileOutputStream> file =
//   //     std::move(io::FileOutputStream::Open(
//   //                   "/Users/karlkatzen/Documents/code/orc-files/Maptest.orc",
//   false))
//   //         .ValueOrDie();
//   ARROW_EXPECT_OK(
//       adapters::orc::ORCFileWriter::Open(sharedPtrSchema, out_stream, options,
//       &writer));
//   ARROW_EXPECT_OK(writer->Write(table));
//   auto output_mem_stream =
//   static_cast<MemoryOutputStream*>(writer->ReleaseOutStream());
//   std::shared_ptr<io::RandomAccessFile> in_stream(
//       new io::BufferReader(std::make_shared<Buffer>(
//           reinterpret_cast<const uint8_t*>(output_mem_stream->getData()),
//           static_cast<int64_t>(output_mem_stream->getLength()))));

//   std::unique_ptr<adapters::orc::ORCFileReader> reader;
//   ASSERT_TRUE(
//       adapters::orc::ORCFileReader::Open(in_stream, default_memory_pool(),
//       &reader).ok());
//   std::shared_ptr<Table> outputTable;
//   ARROW_EXPECT_OK(reader->Read(&outputTable));
//   EXPECT_EQ(outputTable->num_columns(), numCols);
//   EXPECT_EQ(outputTable->num_rows(), numRows);
//   EXPECT_TRUE(outputTable->Equals(*table));
//   RecordProperty("o", table->ToString());
//   RecordProperty("n", outputTable->ToString());
// }

// }
}  // namespace arrow
// std::shared_ptr<io::FileOutputStream> file =
//       std::move(io::FileOutputStream::Open(
//                     "/Users/karlkatzen/Documents/code/orc-files/GeneralMixed.orc",
//                     false))
//           .ValueOrDie();
