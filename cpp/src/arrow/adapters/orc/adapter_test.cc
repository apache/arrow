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

#include <string>

#include "arrow/util/decimal.h"
#include "arrow/io/api.h"
#include "arrow/api.h"
#include "arrow/adapters/orc/adapter.h"
#include "arrow/adapters/orc/adapter_util.h"
#include "arrow/type.h"
#include "arrow/array.h"


#include <gtest/gtest.h>
#include <orc/OrcFile.hh>

namespace liborc = orc;

namespace arrow {

constexpr int DEFAULT_MEM_STREAM_SIZE = 100 * 1024 * 1024;

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

TEST(TestAdapter, readIntAndStringFileMultipleStripes) {
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
  auto long_batch = internal::checked_cast<liborc::LongVectorBatch*>(struct_batch->fields[0]);
  auto str_batch = internal::checked_cast<liborc::StringVectorBatch*>(struct_batch->fields[1]);
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
      auto int32_array = std::dynamic_pointer_cast<Int32Array>(record_batch->column(0));
      auto str_array = std::dynamic_pointer_cast<StringArray>(record_batch->column(1));
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
//WriteORC tests

//Bool
TEST(TestAdapter, writeBoolEmpty) {
  BooleanBuilder builder;
  std::shared_ptr<Array> array;
  (void)(builder.Finish(&array));
  MemoryOutputStream mem_stream(DEFAULT_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(liborc::Type::buildTypeFromString("struct<x:boolean>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer =
    createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch =
    writer->createRowBatch(batchSize);
  liborc::StructVectorBatch *root =
    internal::checked_cast<liborc::StructVectorBatch *>(batch.get());
  liborc::LongVectorBatch *x =
    internal::checked_cast<liborc::LongVectorBatch *>(root->fields[0]);
  DataType* arrowType = boolean().get();
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize, array.get());
  if (!st.ok()) {
    FAIL() << "ORC ColumnBatch not successfully filled";
  }
  EXPECT_EQ(x->numElements, 0);
  EXPECT_FALSE(x->hasNulls);
  EXPECT_EQ(arrowOffset, 0);
  EXPECT_EQ(orcOffset, 0);
  writer->add(*batch);
  writer->close();
}
TEST(TestAdapter, writeBoolNoNulls) {
  BooleanBuilder builder;
  (void)(builder.Append(true));
  (void)(builder.Append(false));
  (void)(builder.Append(false));
  (void)(builder.Append(true));
  std::shared_ptr<Array> array;
  (void)(builder.Finish(&array));
  MemoryOutputStream mem_stream(DEFAULT_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(liborc::Type::buildTypeFromString("struct<x:boolean>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer =
    createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch =
    writer->createRowBatch(batchSize);
  liborc::StructVectorBatch *root =
    internal::checked_cast<liborc::StructVectorBatch *>(batch.get());
  liborc::LongVectorBatch *x =
    internal::checked_cast<liborc::LongVectorBatch *>(root->fields[0]);
  DataType* arrowType = boolean().get();
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize, array.get());
  if (!st.ok()) {
    FAIL() << "ORC ColumnBatch not successfully filled";
  }
  EXPECT_EQ(x->numElements, 4);
  EXPECT_FALSE(x->hasNulls);
  EXPECT_EQ(x->data[0], 1);
  EXPECT_EQ(x->data[1], 0);
  EXPECT_EQ(x->data[2], 0);
  EXPECT_EQ(x->data[3], 1);
  EXPECT_EQ(arrowOffset, 4);
  EXPECT_EQ(orcOffset, 4);
  writer->add(*batch);
  writer->close();
}
TEST(TestAdapter, writeBoolAllNulls) {
  BooleanBuilder builder;
  (void)(builder.AppendNull());
  (void)(builder.AppendNull());
  (void)(builder.AppendNull());
  std::shared_ptr<Array> array;
  (void)(builder.Finish(&array));
  MemoryOutputStream mem_stream(DEFAULT_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(liborc::Type::buildTypeFromString("struct<x:boolean>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer =
    createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch =
    writer->createRowBatch(batchSize);
  liborc::StructVectorBatch *root =
    internal::checked_cast<liborc::StructVectorBatch *>(batch.get());
  liborc::LongVectorBatch *x =
    internal::checked_cast<liborc::LongVectorBatch *>(root->fields[0]);
  DataType* arrowType = boolean().get();
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize, array.get());
  if (!st.ok()) {
    FAIL() << "ORC ColumnBatch not successfully filled";
  }
  EXPECT_EQ(x->numElements, 3);
  EXPECT_TRUE(x->hasNulls);
  EXPECT_EQ(x->notNull[0], 0);
  EXPECT_EQ(x->notNull[1], 0);
  EXPECT_EQ(x->notNull[2], 0);
  EXPECT_EQ(arrowOffset, 3);
  EXPECT_EQ(orcOffset, 3);
  writer->add(*batch);
  writer->close();
}
TEST(TestAdapter, writeBooleanMixed) {
  BooleanBuilder builder;
  (void)(builder.AppendNull());
  (void)(builder.Append(true));
  (void)(builder.AppendNull());
  (void)(builder.Append(false));
  (void)(builder.AppendNull());
  std::shared_ptr<Array> array;
  (void)(builder.Finish(&array));
  MemoryOutputStream mem_stream(DEFAULT_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(liborc::Type::buildTypeFromString("struct<x:boolean>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer =
    createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch =
    writer->createRowBatch(batchSize);
  liborc::StructVectorBatch *root =
    internal::checked_cast<liborc::StructVectorBatch *>(batch.get());
  liborc::LongVectorBatch *x =
    internal::checked_cast<liborc::LongVectorBatch *>(root->fields[0]);
  DataType* arrowType = boolean().get();
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize, array.get());
  if (!st.ok()) {
    FAIL() << "ORC ColumnBatch not successfully filled";
  }
  EXPECT_EQ(x->numElements, 5);
  EXPECT_TRUE(x->hasNulls);
  EXPECT_EQ(x->data[1], 1);
  EXPECT_EQ(x->data[3], 0);
  EXPECT_EQ(x->notNull[0], 0);
  EXPECT_EQ(x->notNull[1], 1);
  EXPECT_EQ(x->notNull[2], 0);
  EXPECT_EQ(x->notNull[3], 1);
  EXPECT_EQ(x->notNull[4], 0);
  EXPECT_EQ(arrowOffset, 5);
  EXPECT_EQ(orcOffset, 5);
  writer->add(*batch);
  writer->close();
}

//Int8
TEST(TestAdapter, writeInt8Empty) {
  Int8Builder builder;
  std::shared_ptr<Array> array;
  (void)(builder.Finish(&array));
  MemoryOutputStream mem_stream(DEFAULT_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(liborc::Type::buildTypeFromString("struct<x:tinyint>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer =
    createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch =
    writer->createRowBatch(batchSize);
  liborc::StructVectorBatch *root =
    internal::checked_cast<liborc::StructVectorBatch *>(batch.get());
  liborc::LongVectorBatch *x =
    internal::checked_cast<liborc::LongVectorBatch *>(root->fields[0]);
  DataType* arrowType = int8().get();
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize, array.get());
  if (!st.ok()) {
    FAIL() << "ORC ColumnBatch not successfully filled";
  }
  EXPECT_EQ(x->numElements, 0);
  EXPECT_FALSE(x->hasNulls);
  EXPECT_EQ(arrowOffset, 0);
  EXPECT_EQ(orcOffset, 0);
  writer->add(*batch);
  writer->close();
}
TEST(TestAdapter, writeInt8NoNulls) {
  Int8Builder builder;
  (void)(builder.Append(1));
  (void)(builder.Append(2));
  (void)(builder.Append(3));
  (void)(builder.Append(4));
  std::shared_ptr<Array> array;
  (void)(builder.Finish(&array));
  MemoryOutputStream mem_stream(DEFAULT_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(liborc::Type::buildTypeFromString("struct<x:tinyint>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer =
    createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch =
    writer->createRowBatch(batchSize);
  liborc::StructVectorBatch *root =
    internal::checked_cast<liborc::StructVectorBatch *>(batch.get());
  liborc::LongVectorBatch *x =
    internal::checked_cast<liborc::LongVectorBatch *>(root->fields[0]);
  DataType* arrowType = int8().get();
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize, array.get());
  if (!st.ok()) {
    FAIL() << "ORC ColumnBatch not successfully filled";
  }
  EXPECT_EQ(x->numElements, 4);
  EXPECT_FALSE(x->hasNulls);
  EXPECT_EQ(x->data[0], 1);
  EXPECT_EQ(x->data[1], 2);
  EXPECT_EQ(x->data[2], 3);
  EXPECT_EQ(x->data[3], 4);
  EXPECT_EQ(arrowOffset, 4);
  EXPECT_EQ(orcOffset, 4);
  writer->add(*batch);
  writer->close();
}
TEST(TestAdapter, writeInt8AllNulls) {
  Int8Builder builder;
  (void)(builder.AppendNull());
  (void)(builder.AppendNull());
  (void)(builder.AppendNull());
  std::shared_ptr<Array> array;
  (void)(builder.Finish(&array));
  MemoryOutputStream mem_stream(DEFAULT_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(liborc::Type::buildTypeFromString("struct<x:tinyint>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer =
    createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch =
    writer->createRowBatch(batchSize);
  liborc::StructVectorBatch *root =
    internal::checked_cast<liborc::StructVectorBatch *>(batch.get());
  liborc::LongVectorBatch *x =
    internal::checked_cast<liborc::LongVectorBatch *>(root->fields[0]);
  DataType* arrowType = int8().get();
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize, array.get());
  if (!st.ok()) {
    FAIL() << "ORC ColumnBatch not successfully filled";
  }
  EXPECT_EQ(x->numElements, 3);
  EXPECT_TRUE(x->hasNulls);
  EXPECT_EQ(x->notNull[0], 0);
  EXPECT_EQ(x->notNull[1], 0);
  EXPECT_EQ(x->notNull[2], 0);
  EXPECT_EQ(arrowOffset, 3);
  EXPECT_EQ(orcOffset, 3);
  writer->add(*batch);
  writer->close();
}
TEST(TestAdapter, writeInt8Mixed) {
  Int8Builder builder;
  (void)(builder.AppendNull());
  (void)(builder.Append(1));
  (void)(builder.AppendNull());
  (void)(builder.Append(2));
  (void)(builder.AppendNull());
  std::shared_ptr<Array> array;
  (void)(builder.Finish(&array));
  MemoryOutputStream mem_stream(DEFAULT_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(liborc::Type::buildTypeFromString("struct<x:tinyint>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer =
    createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch =
    writer->createRowBatch(batchSize);
  liborc::StructVectorBatch *root =
    internal::checked_cast<liborc::StructVectorBatch *>(batch.get());
  liborc::LongVectorBatch *x =
    internal::checked_cast<liborc::LongVectorBatch *>(root->fields[0]);
  DataType* arrowType = int8().get();
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize, array.get());
  if (!st.ok()) {
    FAIL() << "ORC ColumnBatch not successfully filled";
  }
  EXPECT_EQ(x->numElements, 5);
  EXPECT_TRUE(x->hasNulls);
  EXPECT_EQ(x->data[1], 1);
  EXPECT_EQ(x->data[3], 2);
  EXPECT_EQ(x->notNull[0], 0);
  EXPECT_EQ(x->notNull[1], 1);
  EXPECT_EQ(x->notNull[2], 0);
  EXPECT_EQ(x->notNull[3], 1);
  EXPECT_EQ(x->notNull[4], 0);
  EXPECT_EQ(arrowOffset, 5);
  EXPECT_EQ(orcOffset, 5);
  writer->add(*batch);
  writer->close();
}

//Int16
TEST(TestAdapter, writeInt16Empty) {
  Int16Builder builder;
  std::shared_ptr<Array> array;
  (void)(builder.Finish(&array));
  MemoryOutputStream mem_stream(DEFAULT_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(liborc::Type::buildTypeFromString("struct<x:smallint>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer =
    createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch =
    writer->createRowBatch(batchSize);
  liborc::StructVectorBatch *root =
    internal::checked_cast<liborc::StructVectorBatch *>(batch.get());
  liborc::LongVectorBatch *x =
    internal::checked_cast<liborc::LongVectorBatch *>(root->fields[0]);
  DataType* arrowType = int16().get();
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize, array.get());
  if (!st.ok()) {
    FAIL() << "ORC ColumnBatch not successfully filled";
  }
  EXPECT_EQ(x->numElements, 0);
  EXPECT_FALSE(x->hasNulls);
  EXPECT_EQ(arrowOffset, 0);
  EXPECT_EQ(orcOffset, 0);
  writer->add(*batch);
  writer->close();
}
TEST(TestAdapter, writeInt16NoNulls) {
  Int16Builder builder;
  (void)(builder.Append(1));
  (void)(builder.Append(2));
  (void)(builder.Append(3));
  (void)(builder.Append(4));
  std::shared_ptr<Array> array;
  (void)(builder.Finish(&array));
  MemoryOutputStream mem_stream(DEFAULT_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(liborc::Type::buildTypeFromString("struct<x:smallint>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer =
    createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch =
    writer->createRowBatch(batchSize);
  liborc::StructVectorBatch *root =
    internal::checked_cast<liborc::StructVectorBatch *>(batch.get());
  liborc::LongVectorBatch *x =
    internal::checked_cast<liborc::LongVectorBatch *>(root->fields[0]);
  DataType* arrowType = int16().get();
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize, array.get());
  if (!st.ok()) {
    FAIL() << "ORC ColumnBatch not successfully filled";
  }
  EXPECT_EQ(x->numElements, 4);
  EXPECT_FALSE(x->hasNulls);
  EXPECT_EQ(x->data[0], 1);
  EXPECT_EQ(x->data[1], 2);
  EXPECT_EQ(x->data[2], 3);
  EXPECT_EQ(x->data[3], 4);
  EXPECT_EQ(arrowOffset, 4);
  EXPECT_EQ(orcOffset, 4);
  writer->add(*batch);
  writer->close();
}
TEST(TestAdapter, writeInt16AllNulls) {
  Int16Builder builder;
  (void)(builder.AppendNull());
  (void)(builder.AppendNull());
  (void)(builder.AppendNull());
  std::shared_ptr<Array> array;
  (void)(builder.Finish(&array));
  MemoryOutputStream mem_stream(DEFAULT_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(liborc::Type::buildTypeFromString("struct<x:smallint>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer =
    createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch =
    writer->createRowBatch(batchSize);
  liborc::StructVectorBatch *root =
    internal::checked_cast<liborc::StructVectorBatch *>(batch.get());
  liborc::LongVectorBatch *x =
    internal::checked_cast<liborc::LongVectorBatch *>(root->fields[0]);
  DataType* arrowType = int16().get();
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize, array.get());
  if (!st.ok()) {
    FAIL() << "ORC ColumnBatch not successfully filled";
  }
  EXPECT_EQ(x->numElements, 3);
  EXPECT_TRUE(x->hasNulls);
  EXPECT_EQ(x->notNull[0], 0);
  EXPECT_EQ(x->notNull[1], 0);
  EXPECT_EQ(x->notNull[2], 0);
  EXPECT_EQ(arrowOffset, 3);
  EXPECT_EQ(orcOffset, 3);
  writer->add(*batch);
  writer->close();
}
TEST(TestAdapter, writeInt16Mixed) {
  Int16Builder builder;
  (void)(builder.AppendNull());
  (void)(builder.Append(1));
  (void)(builder.AppendNull());
  (void)(builder.Append(2));
  (void)(builder.AppendNull());
  std::shared_ptr<Array> array;
  (void)(builder.Finish(&array));
  MemoryOutputStream mem_stream(DEFAULT_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(liborc::Type::buildTypeFromString("struct<x:smallint>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer =
    createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch =
    writer->createRowBatch(batchSize);
  liborc::StructVectorBatch *root =
    internal::checked_cast<liborc::StructVectorBatch *>(batch.get());
  liborc::LongVectorBatch *x =
    internal::checked_cast<liborc::LongVectorBatch *>(root->fields[0]);
  DataType* arrowType = int16().get();
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize, array.get());
  if (!st.ok()) {
    FAIL() << "ORC ColumnBatch not successfully filled";
  }
  EXPECT_EQ(x->numElements, 5);
  EXPECT_TRUE(x->hasNulls);
  EXPECT_EQ(x->data[1], 1);
  EXPECT_EQ(x->data[3], 2);
  EXPECT_EQ(x->notNull[0], 0);
  EXPECT_EQ(x->notNull[1], 1);
  EXPECT_EQ(x->notNull[2], 0);
  EXPECT_EQ(x->notNull[3], 1);
  EXPECT_EQ(x->notNull[4], 0);
  EXPECT_EQ(arrowOffset, 5);
  EXPECT_EQ(orcOffset, 5);
  writer->add(*batch);
  writer->close();
}

//Int32
TEST(TestAdapter, writeInt32Empty) {
  Int32Builder builder;
  std::shared_ptr<Array> array;
  (void)(builder.Finish(&array));
  MemoryOutputStream mem_stream(DEFAULT_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(liborc::Type::buildTypeFromString("struct<x:int>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer =
    createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch =
    writer->createRowBatch(batchSize);
  liborc::StructVectorBatch *root =
    internal::checked_cast<liborc::StructVectorBatch *>(batch.get());
  liborc::LongVectorBatch *x =
    internal::checked_cast<liborc::LongVectorBatch *>(root->fields[0]);
  DataType* arrowType = int32().get();
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize, array.get());
  if (!st.ok()) {
    FAIL() << "ORC ColumnBatch not successfully filled";
  }
  EXPECT_EQ(x->numElements, 0);
  EXPECT_FALSE(x->hasNulls);
  EXPECT_EQ(arrowOffset, 0);
  EXPECT_EQ(orcOffset, 0);
  writer->add(*batch);
  writer->close();
}
TEST(TestAdapter, writeInt32NoNulls) {
  Int32Builder builder;
  (void)(builder.Append(1));
  (void)(builder.Append(2));
  (void)(builder.Append(3));
  (void)(builder.Append(4));
  std::shared_ptr<Array> array;
  (void)(builder.Finish(&array));
  MemoryOutputStream mem_stream(DEFAULT_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(liborc::Type::buildTypeFromString("struct<x:int>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer =
    createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch =
    writer->createRowBatch(batchSize);
  liborc::StructVectorBatch *root =
    internal::checked_cast<liborc::StructVectorBatch *>(batch.get());
  liborc::LongVectorBatch *x =
    internal::checked_cast<liborc::LongVectorBatch *>(root->fields[0]);
  DataType* arrowType = int32().get();
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize, array.get());
  if (!st.ok()) {
    FAIL() << "ORC ColumnBatch not successfully filled";
  }
  EXPECT_EQ(x->numElements, 4);
  EXPECT_FALSE(x->hasNulls);
  EXPECT_EQ(x->data[0], 1);
  EXPECT_EQ(x->data[1], 2);
  EXPECT_EQ(x->data[2], 3);
  EXPECT_EQ(x->data[3], 4);
  EXPECT_EQ(arrowOffset, 4);
  EXPECT_EQ(orcOffset, 4);
  writer->add(*batch);
  writer->close();
}
TEST(TestAdapter, writeInt32AllNulls) {
  Int32Builder builder;
  (void)(builder.AppendNull());
  (void)(builder.AppendNull());
  (void)(builder.AppendNull());
  std::shared_ptr<Array> array;
  (void)(builder.Finish(&array));
  MemoryOutputStream mem_stream(DEFAULT_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(liborc::Type::buildTypeFromString("struct<x:int>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer =
    createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch =
    writer->createRowBatch(batchSize);
  liborc::StructVectorBatch *root =
    internal::checked_cast<liborc::StructVectorBatch *>(batch.get());
  liborc::LongVectorBatch *x =
    internal::checked_cast<liborc::LongVectorBatch *>(root->fields[0]);
  DataType* arrowType = int32().get();
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize, array.get());
  if (!st.ok()) {
    FAIL() << "ORC ColumnBatch not successfully filled";
  }
  EXPECT_EQ(x->numElements, 3);
  EXPECT_TRUE(x->hasNulls);
  EXPECT_EQ(x->notNull[0], 0);
  EXPECT_EQ(x->notNull[1], 0);
  EXPECT_EQ(x->notNull[2], 0);
  EXPECT_EQ(arrowOffset, 3);
  EXPECT_EQ(orcOffset, 3);
  writer->add(*batch);
  writer->close();
}
TEST(TestAdapter, writeInt32Mixed) {
  Int32Builder builder;
  (void)(builder.AppendNull());
  (void)(builder.Append(1));
  (void)(builder.AppendNull());
  (void)(builder.Append(2));
  (void)(builder.AppendNull());
  std::shared_ptr<Array> array;
  (void)(builder.Finish(&array));
  MemoryOutputStream mem_stream(DEFAULT_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(liborc::Type::buildTypeFromString("struct<x:int>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer =
    createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch =
    writer->createRowBatch(batchSize);
  liborc::StructVectorBatch *root =
    internal::checked_cast<liborc::StructVectorBatch *>(batch.get());
  liborc::LongVectorBatch *x =
    internal::checked_cast<liborc::LongVectorBatch *>(root->fields[0]);
  DataType* arrowType = int32().get();
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize, array.get());
  if (!st.ok()) {
    FAIL() << "ORC ColumnBatch not successfully filled";
  }
  EXPECT_EQ(x->numElements, 5);
  EXPECT_TRUE(x->hasNulls);
  EXPECT_EQ(x->data[1], 1);
  EXPECT_EQ(x->data[3], 2);
  EXPECT_EQ(x->notNull[0], 0);
  EXPECT_EQ(x->notNull[1], 1);
  EXPECT_EQ(x->notNull[2], 0);
  EXPECT_EQ(x->notNull[3], 1);
  EXPECT_EQ(x->notNull[4], 0);
  EXPECT_EQ(arrowOffset, 5);
  EXPECT_EQ(orcOffset, 5);
  writer->add(*batch);
  writer->close();
}

//Int64
TEST(TestAdapter, writeInt64Empty) {
  Int64Builder builder;
  std::shared_ptr<Array> array;
  (void)(builder.Finish(&array));
  MemoryOutputStream mem_stream(DEFAULT_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(liborc::Type::buildTypeFromString("struct<x:bigint>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer =
    createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch =
    writer->createRowBatch(batchSize);
  liborc::StructVectorBatch *root =
    internal::checked_cast<liborc::StructVectorBatch *>(batch.get());
  liborc::LongVectorBatch *x =
    internal::checked_cast<liborc::LongVectorBatch *>(root->fields[0]);
  DataType* arrowType = int64().get();
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize, array.get());
  if (!st.ok()) {
    FAIL() << "ORC ColumnBatch not successfully filled";
  }
  EXPECT_EQ(x->numElements, 0);
  EXPECT_FALSE(x->hasNulls);
  EXPECT_EQ(arrowOffset, 0);
  EXPECT_EQ(orcOffset, 0);
  writer->add(*batch);
  writer->close();
}
TEST(TestAdapter, writeInt64NoNulls) {
  Int64Builder builder;
  (void)(builder.Append(1));
  (void)(builder.Append(2));
  (void)(builder.Append(3));
  (void)(builder.Append(4));
  std::shared_ptr<Array> array;
  (void)(builder.Finish(&array));
  MemoryOutputStream mem_stream(DEFAULT_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(liborc::Type::buildTypeFromString("struct<x:bigint>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer =
    createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch =
    writer->createRowBatch(batchSize);
  liborc::StructVectorBatch *root =
    internal::checked_cast<liborc::StructVectorBatch *>(batch.get());
  liborc::LongVectorBatch *x =
    internal::checked_cast<liborc::LongVectorBatch *>(root->fields[0]);
  DataType* arrowType = int64().get();
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize, array.get());
  if (!st.ok()) {
    FAIL() << "ORC ColumnBatch not successfully filled";
  }
  EXPECT_EQ(x->numElements, 4);
  EXPECT_FALSE(x->hasNulls);
  EXPECT_EQ(x->data[0], 1);
  EXPECT_EQ(x->data[1], 2);
  EXPECT_EQ(x->data[2], 3);
  EXPECT_EQ(x->data[3], 4);
  EXPECT_EQ(arrowOffset, 4);
  EXPECT_EQ(orcOffset, 4);
  writer->add(*batch);
  writer->close();
}
TEST(TestAdapter, writeInt64AllNulls) {
  Int64Builder builder;
  (void)(builder.AppendNull());
  (void)(builder.AppendNull());
  (void)(builder.AppendNull());
  std::shared_ptr<Array> array;
  (void)(builder.Finish(&array));
  MemoryOutputStream mem_stream(DEFAULT_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(liborc::Type::buildTypeFromString("struct<x:bigint>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer =
    createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch =
    writer->createRowBatch(batchSize);
  liborc::StructVectorBatch *root =
    internal::checked_cast<liborc::StructVectorBatch *>(batch.get());
  liborc::LongVectorBatch *x =
    internal::checked_cast<liborc::LongVectorBatch *>(root->fields[0]);
  DataType* arrowType = int64().get();
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize, array.get());
  if (!st.ok()) {
    FAIL() << "ORC ColumnBatch not successfully filled";
  }
  EXPECT_EQ(x->numElements, 3);
  EXPECT_TRUE(x->hasNulls);
  EXPECT_EQ(x->notNull[0], 0);
  EXPECT_EQ(x->notNull[1], 0);
  EXPECT_EQ(x->notNull[2], 0);
  EXPECT_EQ(arrowOffset, 3);
  EXPECT_EQ(orcOffset, 3);
  writer->add(*batch);
  writer->close();
}
TEST(TestAdapter, writeInt64Mixed) {
  Int64Builder builder;
  (void)(builder.AppendNull());
  (void)(builder.Append(1));
  (void)(builder.AppendNull());
  (void)(builder.Append(2));
  (void)(builder.AppendNull());
  std::shared_ptr<Array> array;
  (void)(builder.Finish(&array));
  MemoryOutputStream mem_stream(DEFAULT_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(liborc::Type::buildTypeFromString("struct<x:bigint>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer =
    createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch =
    writer->createRowBatch(batchSize);
  liborc::StructVectorBatch *root =
    internal::checked_cast<liborc::StructVectorBatch *>(batch.get());
  liborc::LongVectorBatch *x =
    internal::checked_cast<liborc::LongVectorBatch *>(root->fields[0]);
  DataType* arrowType = int64().get();
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize, array.get());
  if (!st.ok()) {
    FAIL() << "ORC ColumnBatch not successfully filled";
  }
  EXPECT_EQ(x->numElements, 5);
  EXPECT_TRUE(x->hasNulls);
  EXPECT_EQ(x->data[1], 1);
  EXPECT_EQ(x->data[3], 2);
  EXPECT_EQ(x->notNull[0], 0);
  EXPECT_EQ(x->notNull[1], 1);
  EXPECT_EQ(x->notNull[2], 0);
  EXPECT_EQ(x->notNull[3], 1);
  EXPECT_EQ(x->notNull[4], 0);
  EXPECT_EQ(arrowOffset, 5);
  EXPECT_EQ(orcOffset, 5);
  writer->add(*batch);
  writer->close();
}

//Float
TEST(TestAdapter, writeFloatEmpty) {
  FloatBuilder builder;
  std::shared_ptr<Array> array;
  (void)(builder.Finish(&array));
  MemoryOutputStream mem_stream(DEFAULT_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(liborc::Type::buildTypeFromString("struct<x:float>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer =
    createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch =
    writer->createRowBatch(batchSize);
  liborc::StructVectorBatch *root =
    internal::checked_cast<liborc::StructVectorBatch *>(batch.get());
  liborc::DoubleVectorBatch *x =
    internal::checked_cast<liborc::DoubleVectorBatch *>(root->fields[0]);
  DataType* arrowType = float32().get();
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize, array.get());
  if (!st.ok()) {
    FAIL() << "ORC ColumnBatch not successfully filled";
  }
  EXPECT_EQ(x->numElements, 0);
  EXPECT_FALSE(x->hasNulls);
  EXPECT_EQ(arrowOffset, 0);
  EXPECT_EQ(orcOffset, 0);
  writer->add(*batch);
  writer->close();
}
TEST(TestAdapter, writeFloatNoNulls) {
  FloatBuilder builder;
  (void)(builder.Append(1.5));
  (void)(builder.Append(2.6));
  (void)(builder.Append(3.7));
  (void)(builder.Append(4.8));
  std::shared_ptr<Array> array;
  (void)(builder.Finish(&array));
  MemoryOutputStream mem_stream(DEFAULT_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(liborc::Type::buildTypeFromString("struct<x:float>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer =
    createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch =
    writer->createRowBatch(batchSize);
  liborc::StructVectorBatch *root =
    internal::checked_cast<liborc::StructVectorBatch *>(batch.get());
  liborc::DoubleVectorBatch *x =
    internal::checked_cast<liborc::DoubleVectorBatch *>(root->fields[0]);
  DataType* arrowType = float32().get();
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize, array.get());
  if (!st.ok()) {
    FAIL() << "ORC ColumnBatch not successfully filled";
  }
  EXPECT_EQ(x->numElements, 4);
  EXPECT_FALSE(x->hasNulls);
  EXPECT_FLOAT_EQ(x->data[0], 1.5);
  EXPECT_FLOAT_EQ(x->data[1], 2.6);
  EXPECT_FLOAT_EQ(x->data[2], 3.7);
  EXPECT_FLOAT_EQ(x->data[3], 4.8);
  EXPECT_EQ(arrowOffset, 4);
  EXPECT_EQ(orcOffset, 4);
  writer->add(*batch);
  writer->close();
}
TEST(TestAdapter, writeFloatAllNulls) {
  FloatBuilder builder;
  (void)(builder.AppendNull());
  (void)(builder.AppendNull());
  (void)(builder.AppendNull());
  std::shared_ptr<Array> array;
  (void)(builder.Finish(&array));
  MemoryOutputStream mem_stream(DEFAULT_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(liborc::Type::buildTypeFromString("struct<x:float>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer =
    createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch =
    writer->createRowBatch(batchSize);
  liborc::StructVectorBatch *root =
    internal::checked_cast<liborc::StructVectorBatch *>(batch.get());
  liborc::DoubleVectorBatch *x =
    internal::checked_cast<liborc::DoubleVectorBatch *>(root->fields[0]);
  DataType* arrowType = float32().get();
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize, array.get());
  if (!st.ok()) {
    FAIL() << "ORC ColumnBatch not successfully filled";
  }
  EXPECT_EQ(x->numElements, 3);
  EXPECT_TRUE(x->hasNulls);
  EXPECT_EQ(x->notNull[0], 0);
  EXPECT_EQ(x->notNull[1], 0);
  EXPECT_EQ(x->notNull[2], 0);
  EXPECT_EQ(arrowOffset, 3);
  EXPECT_EQ(orcOffset, 3);
  writer->add(*batch);
  writer->close();
}
TEST(TestAdapter, writeFloatMixed) {
  FloatBuilder builder;
  (void)(builder.AppendNull());
  (void)(builder.Append(1.2));
  (void)(builder.AppendNull());
  (void)(builder.Append(2.3));
  (void)(builder.AppendNull());
  std::shared_ptr<Array> array;
  (void)(builder.Finish(&array));
  MemoryOutputStream mem_stream(DEFAULT_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(liborc::Type::buildTypeFromString("struct<x:float>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer =
    createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch =
    writer->createRowBatch(batchSize);
  liborc::StructVectorBatch *root =
    internal::checked_cast<liborc::StructVectorBatch *>(batch.get());
  liborc::DoubleVectorBatch *x =
    internal::checked_cast<liborc::DoubleVectorBatch *>(root->fields[0]);
  DataType* arrowType = float32().get();
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize, array.get());
  if (!st.ok()) {
    FAIL() << "ORC ColumnBatch not successfully filled";
  }
  EXPECT_EQ(x->numElements, 5);
  EXPECT_TRUE(x->hasNulls);
  EXPECT_FLOAT_EQ(x->data[1], 1.2);
  EXPECT_FLOAT_EQ(x->data[3], 2.3);
  EXPECT_EQ(x->notNull[0], 0);
  EXPECT_EQ(x->notNull[1], 1);
  EXPECT_EQ(x->notNull[2], 0);
  EXPECT_EQ(x->notNull[3], 1);
  EXPECT_EQ(x->notNull[4], 0);
  EXPECT_EQ(arrowOffset, 5);
  EXPECT_EQ(orcOffset, 5);
  writer->add(*batch);
  writer->close();
}

//Double
TEST(TestAdapter, writeDoubleEmpty) {
  DoubleBuilder builder;
  std::shared_ptr<Array> array;
  (void)(builder.Finish(&array));
  MemoryOutputStream mem_stream(DEFAULT_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(liborc::Type::buildTypeFromString("struct<x:double>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer =
    createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch =
    writer->createRowBatch(batchSize);
  liborc::StructVectorBatch *root =
    internal::checked_cast<liborc::StructVectorBatch *>(batch.get());
  liborc::DoubleVectorBatch *x =
    internal::checked_cast<liborc::DoubleVectorBatch *>(root->fields[0]);
  DataType* arrowType = float64().get();
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize, array.get());
  if (!st.ok()) {
    FAIL() << "ORC ColumnBatch not successfully filled";
  }
  EXPECT_EQ(x->numElements, 0);
  EXPECT_FALSE(x->hasNulls);
  EXPECT_EQ(arrowOffset, 0);
  EXPECT_EQ(orcOffset, 0);
  writer->add(*batch);
  writer->close();
}
TEST(TestAdapter, writeDoubleNoNulls) {
  DoubleBuilder builder;
  (void)(builder.Append(1.5));
  (void)(builder.Append(2.6));
  (void)(builder.Append(3.7));
  (void)(builder.Append(4.8));
  std::shared_ptr<Array> array;
  (void)(builder.Finish(&array));
  MemoryOutputStream mem_stream(DEFAULT_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(liborc::Type::buildTypeFromString("struct<x:double>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer =
    createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch =
    writer->createRowBatch(batchSize);
  liborc::StructVectorBatch *root =
    internal::checked_cast<liborc::StructVectorBatch *>(batch.get());
  liborc::DoubleVectorBatch *x =
    internal::checked_cast<liborc::DoubleVectorBatch *>(root->fields[0]);
  DataType* arrowType = float64().get();
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize, array.get());
  if (!st.ok()) {
    FAIL() << "ORC ColumnBatch not successfully filled";
  }
  EXPECT_EQ(x->numElements, 4);
  EXPECT_FALSE(x->hasNulls);
  EXPECT_DOUBLE_EQ(x->data[0], 1.5);
  EXPECT_DOUBLE_EQ(x->data[1], 2.6);
  EXPECT_DOUBLE_EQ(x->data[2], 3.7);
  EXPECT_DOUBLE_EQ(x->data[3], 4.8);
  EXPECT_EQ(arrowOffset, 4);
  EXPECT_EQ(orcOffset, 4);
  writer->add(*batch);
  writer->close();
}
TEST(TestAdapter, writeDoubleAllNulls) {
  DoubleBuilder builder;
  (void)(builder.AppendNull());
  (void)(builder.AppendNull());
  (void)(builder.AppendNull());
  std::shared_ptr<Array> array;
  (void)(builder.Finish(&array));
  MemoryOutputStream mem_stream(DEFAULT_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(liborc::Type::buildTypeFromString("struct<x:double>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer =
    createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch =
    writer->createRowBatch(batchSize);
  liborc::StructVectorBatch *root =
    internal::checked_cast<liborc::StructVectorBatch *>(batch.get());
  liborc::DoubleVectorBatch *x =
    internal::checked_cast<liborc::DoubleVectorBatch *>(root->fields[0]);
  DataType* arrowType = float64().get();
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize, array.get());
  if (!st.ok()) {
    FAIL() << "ORC ColumnBatch not successfully filled";
  }
  EXPECT_EQ(x->numElements, 3);
  EXPECT_TRUE(x->hasNulls);
  EXPECT_EQ(x->notNull[0], 0);
  EXPECT_EQ(x->notNull[1], 0);
  EXPECT_EQ(x->notNull[2], 0);
  EXPECT_EQ(arrowOffset, 3);
  EXPECT_EQ(orcOffset, 3);
  writer->add(*batch);
  writer->close();
}
TEST(TestAdapter, writeDoubleMixed) {
  DoubleBuilder builder;
  (void)(builder.AppendNull());
  (void)(builder.Append(1.2));
  (void)(builder.AppendNull());
  (void)(builder.Append(2.3));
  (void)(builder.AppendNull());
  std::shared_ptr<Array> array;
  (void)(builder.Finish(&array));
  MemoryOutputStream mem_stream(DEFAULT_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(liborc::Type::buildTypeFromString("struct<x:double>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer =
    createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch =
    writer->createRowBatch(batchSize);
  liborc::StructVectorBatch *root =
    internal::checked_cast<liborc::StructVectorBatch *>(batch.get());
  liborc::DoubleVectorBatch *x =
    internal::checked_cast<liborc::DoubleVectorBatch *>(root->fields[0]);
  DataType* arrowType = float64().get();
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize, array.get());
  if (!st.ok()) {
    FAIL() << "ORC ColumnBatch not successfully filled";
  }
  EXPECT_EQ(x->numElements, 5);
  EXPECT_TRUE(x->hasNulls);
  EXPECT_DOUBLE_EQ(x->data[1], 1.2);
  EXPECT_DOUBLE_EQ(x->data[3], 2.3);
  EXPECT_EQ(x->notNull[0], 0);
  EXPECT_EQ(x->notNull[1], 1);
  EXPECT_EQ(x->notNull[2], 0);
  EXPECT_EQ(x->notNull[3], 1);
  EXPECT_EQ(x->notNull[4], 0);
  EXPECT_EQ(arrowOffset, 5);
  EXPECT_EQ(orcOffset, 5);
  writer->add(*batch);
  writer->close();
}

//Decimal
// TEST(TestAdapter, writeDecimalEmpty) {
//   Decimal128Builder builder(decimal(38, 6));
//   std::shared_ptr<Array> array;
//   (void)(builder.Finish(&array));
//   MemoryOutputStream mem_stream(DEFAULT_MEM_STREAM_SIZE);
//   ORC_UNIQUE_PTR<liborc::Type> schema(liborc::Type::buildTypeFromString("struct<x:decimal>"));
//   liborc::WriterOptions options;
//   ORC_UNIQUE_PTR<liborc::Writer> writer =
//     createWriter(*schema, &mem_stream, options);
//   uint64_t batchSize = 1024;
//   ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch =
//     writer->createRowBatch(batchSize);
//   liborc::StructVectorBatch *root =
//     internal::checked_cast<liborc::StructVectorBatch *>(batch.get());
//   liborc::Decimal128VectorBatch *x =
//     internal::checked_cast<liborc::Decimal128VectorBatch *>(root->fields[0]);
//   DataType* arrowType = decimal(38, 6).get();
//   int64_t arrowOffset = 0;
//   int64_t orcOffset = 0;
//   Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize, array.get());
//   if (!st.ok()) {
//     FAIL() << "ORC ColumnBatch not successfully filled";
//   }
//   EXPECT_EQ(x->numElements, 0);
//   EXPECT_FALSE(x->hasNulls);
//   EXPECT_EQ(arrowOffset, 0);
//   EXPECT_EQ(orcOffset, 0);
//   writer->add(*batch);
//   writer->close();
// }
// TEST(TestAdapter, writeDecimalNoNulls) {
//   Decimal128Builder builder(decimal(38, 6));
//   (void)(builder.Append(new Decimal128("1.5")));
//   (void)(builder.Append(new Decimal128("2.6")));
//   (void)(builder.Append(new Decimal128("3.7")));
//   (void)(builder.Append(new Decimal128("4.8")));
//   std::shared_ptr<Array> array;
//   (void)(builder.Finish(&array));
//   MemoryOutputStream mem_stream(DEFAULT_MEM_STREAM_SIZE);
//   ORC_UNIQUE_PTR<liborc::Type> schema(liborc::Type::buildTypeFromString("struct<x:decimal>"));
//   liborc::WriterOptions options;
//   ORC_UNIQUE_PTR<liborc::Writer> writer =
//     createWriter(*schema, &mem_stream, options);
//   uint64_t batchSize = 1024;
//   ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch =
//     writer->createRowBatch(batchSize);
//   liborc::StructVectorBatch *root =
//     internal::checked_cast<liborc::StructVectorBatch *>(batch.get());
//   liborc::Decimal128VectorBatch *x =
//     internal::checked_cast<liborc::Decimal128VectorBatch *>(root->fields[0]);
//   DataType* arrowType = decimal(38, 6).get();
//   int64_t arrowOffset = 0;
//   int64_t orcOffset = 0;
//   Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize, array.get());
//   if (!st.ok()) {
//     FAIL() << "ORC ColumnBatch not successfully filled";
//   }
//   EXPECT_EQ(x->numElements, 4);
//   EXPECT_FALSE(x->hasNulls);
//   EXPECT_EQ(new Decimal128(x->values[0].toDecimalString(6)), new Decimal128("1.5")));
//   EXPECT_EQ(new Decimal128(x->values[1].toDecimalString(6)), new Decimal128("2.6")));
//   EXPECT_EQ(new Decimal128(x->values[2].toDecimalString(6)), new Decimal128("3.7")));
//   EXPECT_EQ(new Decimal128(x->values[3].toDecimalString(6)), new Decimal128("4.8")));
//   EXPECT_EQ(arrowOffset, 4);
//   EXPECT_EQ(orcOffset, 4);
//   writer->add(*batch);
//   writer->close();
// }
// TEST(TestAdapter, writeDecimalAllNulls) {
//   Decimal32Builder builder(decimal(38, 6));
//   (void)(builder.AppendNull());
//   (void)(builder.AppendNull());
//   (void)(builder.AppendNull());
//   std::shared_ptr<Array> array;
//   (void)(builder.Finish(&array));
//   MemoryOutputStream mem_stream(DEFAULT_MEM_STREAM_SIZE);
//   ORC_UNIQUE_PTR<liborc::Type> schema(liborc::Type::buildTypeFromString("struct<x:decimal>"));
//   liborc::WriterOptions options;
//   ORC_UNIQUE_PTR<liborc::Writer> writer =
//     createWriter(*schema, &mem_stream, options);
//   uint64_t batchSize = 1024;
//   ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch =
//     writer->createRowBatch(batchSize);
//   liborc::StructVectorBatch *root =
//     internal::checked_cast<liborc::StructVectorBatch *>(batch.get());
//   liborc::Decimal128VectorBatch *x =
//     internal::checked_cast<liborc::Decimal128VectorBatch *>(root->fields[0]);
//   DataType* arrowType = decimal(38, 6).get();
//   int64_t arrowOffset = 0;
//   int64_t orcOffset = 0;
//   Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize, array.get());
//   if (!st.ok()) {
//     FAIL() << "ORC ColumnBatch not successfully filled";
//   }
//   EXPECT_EQ(x->numElements, 3);
//   EXPECT_TRUE(x->hasNulls);
//   EXPECT_EQ(x->notNull[0], 0);
//   EXPECT_EQ(x->notNull[1], 0);
//   EXPECT_EQ(x->notNull[2], 0);
//   EXPECT_EQ(arrowOffset, 3);
//   EXPECT_EQ(orcOffset, 3);
//   writer->add(*batch);
//   writer->close();
// }
// TEST(TestAdapter, writeDecimalMixed) {
//   Decimal128Builder builder(decimal(38, 6));
//   (void)(builder.AppendNull());
//   (void)(builder.Append(new Decimal128("1.2")));
//   (void)(builder.AppendNull());
//   (void)(builder.Append(new Decimal128("2.3")));
//   (void)(builder.AppendNull());
//   std::shared_ptr<Array> array;
//   (void)(builder.Finish(&array));
//   MemoryOutputStream mem_stream(DEFAULT_MEM_STREAM_SIZE);
//   ORC_UNIQUE_PTR<liborc::Type> schema(liborc::Type::buildTypeFromString("struct<x:decimal>"));
//   liborc::WriterOptions options;
//   ORC_UNIQUE_PTR<liborc::Writer> writer =
//     createWriter(*schema, &mem_stream, options);
//   uint64_t batchSize = 1024;
//   ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch =
//     writer->createRowBatch(batchSize);
//   liborc::StructVectorBatch *root =
//     internal::checked_cast<liborc::StructVectorBatch *>(batch.get());
//   liborc::Decimal128VectorBatch *x =
//     internal::checked_cast<liborc::Decimal128VectorBatch *>(root->fields[0]);
//   DataType* arrowType = float64().get();
//   int64_t arrowOffset = 0;
//   int64_t orcOffset = 0;
//   Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize, array.get());
//   if (!st.ok()) {
//     FAIL() << "ORC ColumnBatch not successfully filled";
//   }
//   EXPECT_EQ(x->numElements, 5);
//   EXPECT_TRUE(x->hasNulls);
//   EXPECT_EQ(new Decimal128(x->values[1].toDecimalString(6)), new Decimal128("1.2")));
//   EXPECT_EQ(new Decimal128(x->values[3].toDecimalString(6)), new Decimal128("2.3")));
//   EXPECT_EQ(x->notNull[0], 0);
//   EXPECT_EQ(x->notNull[1], 1);
//   EXPECT_EQ(x->notNull[2], 0);
//   EXPECT_EQ(x->notNull[3], 1);
//   EXPECT_EQ(x->notNull[4], 0);
//   EXPECT_EQ(arrowOffset, 5);
//   EXPECT_EQ(orcOffset, 5);
//   writer->add(*batch);
//   writer->close();
// }

//Binary formats

//String
TEST(TestAdapter, writeStringEmpty) {
  StringBuilder builder;
  std::shared_ptr<Array> array;
  (void)(builder.Finish(&array));
  MemoryOutputStream mem_stream(DEFAULT_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(liborc::Type::buildTypeFromString("struct<x:string>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer =
    createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch =
    writer->createRowBatch(batchSize);
  liborc::StructVectorBatch *root =
    internal::checked_cast<liborc::StructVectorBatch *>(batch.get());
  liborc::StringVectorBatch *x =
    internal::checked_cast<liborc::StringVectorBatch *>(root->fields[0]);
  DataType* arrowType = utf8().get();
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize, array.get());
  if (!st.ok()) {
    FAIL() << "ORC ColumnBatch not successfully filled";
  }
  EXPECT_EQ(x->numElements, 0);
  EXPECT_FALSE(x->hasNulls);
  EXPECT_EQ(arrowOffset, 0);
  EXPECT_EQ(orcOffset, 0);
  writer->add(*batch);
  writer->close();
}
TEST(TestAdapter, writeStringNoNulls) {
  StringBuilder builder;
  (void)(builder.Append("A"));
  (void)(builder.Append("AB"));
  (void)(builder.Append(""));
  (void)(builder.Append("ABCD"));
  std::shared_ptr<Array> array;
  (void)(builder.Finish(&array));
  MemoryOutputStream mem_stream(DEFAULT_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(liborc::Type::buildTypeFromString("struct<x:string>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer =
    createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch =
    writer->createRowBatch(batchSize);
  liborc::StructVectorBatch *root =
    internal::checked_cast<liborc::StructVectorBatch *>(batch.get());
  liborc::StringVectorBatch *x =
    internal::checked_cast<liborc::StringVectorBatch *>(root->fields[0]);
  DataType* arrowType = utf8().get();
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize, array.get());
  if (!st.ok()) {
    FAIL() << "ORC ColumnBatch not successfully filled";
  }
  EXPECT_EQ(x->numElements, 4);
  EXPECT_FALSE(x->hasNulls);
  EXPECT_STREQ(x->data[0], "A");
  EXPECT_STREQ(x->data[1], "AB");
  EXPECT_STREQ(x->data[2], "");
  EXPECT_STREQ(x->data[3], "ABCD");
  EXPECT_EQ(x->length[0], 1);
  EXPECT_EQ(x->length[1], 2);
  EXPECT_EQ(x->length[2], 0);
  EXPECT_EQ(x->length[3], 4);
  EXPECT_EQ(arrowOffset, 4);
  EXPECT_EQ(orcOffset, 4);
  writer->add(*batch);
  writer->close();
}
TEST(TestAdapter, writeStringAllNulls) {
  StringBuilder builder;
  (void)(builder.AppendNull());
  (void)(builder.AppendNull());
  (void)(builder.AppendNull());
  std::shared_ptr<Array> array;
  (void)(builder.Finish(&array));
  MemoryOutputStream mem_stream(DEFAULT_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(liborc::Type::buildTypeFromString("struct<x:string>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer =
    createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch =
    writer->createRowBatch(batchSize);
  liborc::StructVectorBatch *root =
    internal::checked_cast<liborc::StructVectorBatch *>(batch.get());
  liborc::StringVectorBatch *x =
    internal::checked_cast<liborc::StringVectorBatch *>(root->fields[0]);
  DataType* arrowType = utf8().get();
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize, array.get());
  if (!st.ok()) {
    FAIL() << "ORC ColumnBatch not successfully filled";
  }
  EXPECT_EQ(x->numElements, 3);
  EXPECT_TRUE(x->hasNulls);
  EXPECT_EQ(x->notNull[0], 0);
  EXPECT_EQ(x->notNull[1], 0);
  EXPECT_EQ(x->notNull[2], 0);
  EXPECT_EQ(arrowOffset, 3);
  EXPECT_EQ(orcOffset, 3);
  writer->add(*batch);
  writer->close();
}
TEST(TestAdapter, writeStringMixed) {
  StringBuilder builder;
  (void)(builder.AppendNull());
  (void)(builder.Append(""));
  (void)(builder.AppendNull());
  (void)(builder.Append("AB"));
  (void)(builder.AppendNull());
  std::shared_ptr<Array> array;
  (void)(builder.Finish(&array));
  MemoryOutputStream mem_stream(DEFAULT_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(liborc::Type::buildTypeFromString("struct<x:string>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer =
    createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch =
    writer->createRowBatch(batchSize);
  liborc::StructVectorBatch *root =
    internal::checked_cast<liborc::StructVectorBatch *>(batch.get());
  liborc::StringVectorBatch *x =
    internal::checked_cast<liborc::StringVectorBatch *>(root->fields[0]);
  DataType* arrowType = utf8().get();
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize, array.get());
  if (!st.ok()) {
    FAIL() << "ORC ColumnBatch not successfully filled";
  }
  EXPECT_EQ(x->numElements, 5);
  EXPECT_TRUE(x->hasNulls);
  EXPECT_STREQ(x->data[1], "");
  EXPECT_STREQ(x->data[3], "AB");
  EXPECT_EQ(x->length[1], 0);
  EXPECT_EQ(x->length[3], 2);
  EXPECT_EQ(x->notNull[0], 0);
  EXPECT_EQ(x->notNull[1], 1);
  EXPECT_EQ(x->notNull[2], 0);
  EXPECT_EQ(x->notNull[3], 1);
  EXPECT_EQ(x->notNull[4], 0);
  EXPECT_EQ(arrowOffset, 5);
  EXPECT_EQ(orcOffset, 5);
  writer->add(*batch);
  writer->close();
}

//LargeString
TEST(TestAdapter, writeLargeStringEmpty) {
  LargeStringBuilder builder;
  std::shared_ptr<Array> array;
  (void)(builder.Finish(&array));
  MemoryOutputStream mem_stream(DEFAULT_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(liborc::Type::buildTypeFromString("struct<x:string>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer =
    createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch =
    writer->createRowBatch(batchSize);
  liborc::StructVectorBatch *root =
    internal::checked_cast<liborc::StructVectorBatch *>(batch.get());
  liborc::StringVectorBatch *x =
    internal::checked_cast<liborc::StringVectorBatch *>(root->fields[0]);
  DataType* arrowType = large_utf8().get();
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize, array.get());
  if (!st.ok()) {
    FAIL() << "ORC ColumnBatch not successfully filled";
  }
  EXPECT_EQ(x->numElements, 0);
  EXPECT_FALSE(x->hasNulls);
  EXPECT_EQ(arrowOffset, 0);
  EXPECT_EQ(orcOffset, 0);
  writer->add(*batch);
  writer->close();
}
TEST(TestAdapter, writeLargeStringNoNulls) {
  LargeStringBuilder builder;
  (void)(builder.Append("A"));
  (void)(builder.Append("AB"));
  (void)(builder.Append(""));
  (void)(builder.Append("ABCD"));
  std::shared_ptr<Array> array;
  (void)(builder.Finish(&array));
  MemoryOutputStream mem_stream(DEFAULT_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(liborc::Type::buildTypeFromString("struct<x:string>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer =
    createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch =
    writer->createRowBatch(batchSize);
  liborc::StructVectorBatch *root =
    internal::checked_cast<liborc::StructVectorBatch *>(batch.get());
  liborc::StringVectorBatch *x =
    internal::checked_cast<liborc::StringVectorBatch *>(root->fields[0]);
  DataType* arrowType = large_utf8().get();
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize, array.get());
  if (!st.ok()) {
    FAIL() << "ORC ColumnBatch not successfully filled";
  }
  EXPECT_EQ(x->numElements, 4);
  EXPECT_FALSE(x->hasNulls);
  EXPECT_STREQ(x->data[0], "A");
  EXPECT_STREQ(x->data[1], "AB");
  EXPECT_STREQ(x->data[2], "");
  EXPECT_STREQ(x->data[3], "ABCD");
  EXPECT_EQ(x->length[0], 1);
  EXPECT_EQ(x->length[1], 2);
  EXPECT_EQ(x->length[2], 0);
  EXPECT_EQ(x->length[3], 4);
  EXPECT_EQ(arrowOffset, 4);
  EXPECT_EQ(orcOffset, 4);
  writer->add(*batch);
  writer->close();
}
TEST(TestAdapter, writeLargeStringAllNulls) {
  LargeStringBuilder builder;
  (void)(builder.AppendNull());
  (void)(builder.AppendNull());
  (void)(builder.AppendNull());
  std::shared_ptr<Array> array;
  (void)(builder.Finish(&array));
  MemoryOutputStream mem_stream(DEFAULT_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(liborc::Type::buildTypeFromString("struct<x:string>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer =
    createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch =
    writer->createRowBatch(batchSize);
  liborc::StructVectorBatch *root =
    internal::checked_cast<liborc::StructVectorBatch *>(batch.get());
  liborc::StringVectorBatch *x =
    internal::checked_cast<liborc::StringVectorBatch *>(root->fields[0]);
  DataType* arrowType = large_utf8().get();
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize, array.get());
  if (!st.ok()) {
    FAIL() << "ORC ColumnBatch not successfully filled";
  }
  EXPECT_EQ(x->numElements, 3);
  EXPECT_TRUE(x->hasNulls);
  EXPECT_EQ(x->notNull[0], 0);
  EXPECT_EQ(x->notNull[1], 0);
  EXPECT_EQ(x->notNull[2], 0);
  EXPECT_EQ(arrowOffset, 3);
  EXPECT_EQ(orcOffset, 3);
  writer->add(*batch);
  writer->close();
}
TEST(TestAdapter, writeLargeStringMixed) {
  LargeStringBuilder builder;
  (void)(builder.AppendNull());
  (void)(builder.Append(""));
  (void)(builder.AppendNull());
  (void)(builder.Append("AB"));
  (void)(builder.AppendNull());
  std::shared_ptr<Array> array;
  (void)(builder.Finish(&array));
  MemoryOutputStream mem_stream(DEFAULT_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(liborc::Type::buildTypeFromString("struct<x:string>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer =
    createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch =
    writer->createRowBatch(batchSize);
  liborc::StructVectorBatch *root =
    internal::checked_cast<liborc::StructVectorBatch *>(batch.get());
  liborc::StringVectorBatch *x =
    internal::checked_cast<liborc::StringVectorBatch *>(root->fields[0]);
  DataType* arrowType = large_utf8().get();
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize, array.get());
  if (!st.ok()) {
    FAIL() << "ORC ColumnBatch not successfully filled";
  }
  EXPECT_EQ(x->numElements, 5);
  EXPECT_TRUE(x->hasNulls);
  EXPECT_STREQ(x->data[1], "");
  EXPECT_STREQ(x->data[3], "AB");
  EXPECT_EQ(x->length[1], 0);
  EXPECT_EQ(x->length[3], 2);
  EXPECT_EQ(x->notNull[0], 0);
  EXPECT_EQ(x->notNull[1], 1);
  EXPECT_EQ(x->notNull[2], 0);
  EXPECT_EQ(x->notNull[3], 1);
  EXPECT_EQ(x->notNull[4], 0);
  EXPECT_EQ(arrowOffset, 5);
  EXPECT_EQ(orcOffset, 5);
  writer->add(*batch);
  writer->close();
}

//Binary
TEST(TestAdapter, writeBinaryEmpty) {
  BinaryBuilder builder;
  std::shared_ptr<Array> array;
  (void)(builder.Finish(&array));
  MemoryOutputStream mem_stream(DEFAULT_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(liborc::Type::buildTypeFromString("struct<x:binary>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer =
    createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch =
    writer->createRowBatch(batchSize);
  liborc::StructVectorBatch *root =
    internal::checked_cast<liborc::StructVectorBatch *>(batch.get());
  liborc::StringVectorBatch *x =
    internal::checked_cast<liborc::StringVectorBatch *>(root->fields[0]);
  DataType* arrowType = binary().get();
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize, array.get());
  if (!st.ok()) {
    FAIL() << "ORC ColumnBatch not successfully filled";
  }
  EXPECT_EQ(x->numElements, 0);
  EXPECT_FALSE(x->hasNulls);
  EXPECT_EQ(arrowOffset, 0);
  EXPECT_EQ(orcOffset, 0);
  writer->add(*batch);
  writer->close();
}
TEST(TestAdapter, writeBinaryNoNulls) {
  BinaryBuilder builder;
  char a[] = "\xf4", b[] = "\x22\x0e",c[] = "",d[] = "\xff\x66\xbf\x5b";
  (void)(builder.Append(a));
  (void)(builder.Append(b));
  (void)(builder.Append(c));
  (void)(builder.Append(d));
  std::shared_ptr<Array> array;
  (void)(builder.Finish(&array));
  MemoryOutputStream mem_stream(DEFAULT_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(liborc::Type::buildTypeFromString("struct<x:binary>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer =
    createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch =
    writer->createRowBatch(batchSize);
  liborc::StructVectorBatch *root =
    internal::checked_cast<liborc::StructVectorBatch *>(batch.get());
  liborc::StringVectorBatch *x =
    internal::checked_cast<liborc::StringVectorBatch *>(root->fields[0]);
  DataType* arrowType = binary().get();
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize, array.get());
  if (!st.ok()) {
    FAIL() << "ORC ColumnBatch not successfully filled";
  }
  EXPECT_EQ(x->numElements, 4);
  EXPECT_FALSE(x->hasNulls);
  EXPECT_STREQ(x->data[0], a);
  EXPECT_STREQ(x->data[1], b);
  EXPECT_STREQ(x->data[2], c);
  EXPECT_STREQ(x->data[3], d);
  EXPECT_EQ(x->length[0], 1);
  EXPECT_EQ(x->length[1], 2);
  EXPECT_EQ(x->length[2], 0);
  EXPECT_EQ(x->length[3], 4);
  EXPECT_EQ(arrowOffset, 4);
  EXPECT_EQ(orcOffset, 4);
  writer->add(*batch);
  writer->close();
}
TEST(TestAdapter, writeBinaryAllNulls) {
  BinaryBuilder builder;
  (void)(builder.AppendNull());
  (void)(builder.AppendNull());
  (void)(builder.AppendNull());
  std::shared_ptr<Array> array;
  (void)(builder.Finish(&array));
  MemoryOutputStream mem_stream(DEFAULT_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(liborc::Type::buildTypeFromString("struct<x:binary>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer =
    createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch =
    writer->createRowBatch(batchSize);
  liborc::StructVectorBatch *root =
    internal::checked_cast<liborc::StructVectorBatch *>(batch.get());
  liborc::StringVectorBatch *x =
    internal::checked_cast<liborc::StringVectorBatch *>(root->fields[0]);
  DataType* arrowType = binary().get();
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize, array.get());
  if (!st.ok()) {
    FAIL() << "ORC ColumnBatch not successfully filled";
  }
  EXPECT_EQ(x->numElements, 3);
  EXPECT_TRUE(x->hasNulls);
  EXPECT_EQ(x->notNull[0], 0);
  EXPECT_EQ(x->notNull[1], 0);
  EXPECT_EQ(x->notNull[2], 0);
  EXPECT_EQ(arrowOffset, 3);
  EXPECT_EQ(orcOffset, 3);
  writer->add(*batch);
  writer->close();
}
TEST(TestAdapter, writeBinaryMixed) {
  BinaryBuilder builder;
  char a[] = "", b[] = "\xff\xfe";
  (void)(builder.AppendNull());
  (void)(builder.Append(a));
  (void)(builder.AppendNull());
  (void)(builder.Append(b));
  (void)(builder.AppendNull());
  std::shared_ptr<Array> array;
  (void)(builder.Finish(&array));
  MemoryOutputStream mem_stream(DEFAULT_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(liborc::Type::buildTypeFromString("struct<x:binary>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer =
    createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch =
    writer->createRowBatch(batchSize);
  liborc::StructVectorBatch *root =
    internal::checked_cast<liborc::StructVectorBatch *>(batch.get());
  liborc::StringVectorBatch *x =
    internal::checked_cast<liborc::StringVectorBatch *>(root->fields[0]);
  DataType* arrowType = binary().get();
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize, array.get());
  if (!st.ok()) {
    FAIL() << "ORC ColumnBatch not successfully filled";
  }
  EXPECT_EQ(x->numElements, 5);
  EXPECT_TRUE(x->hasNulls);
  EXPECT_STREQ(x->data[1], a);
  EXPECT_STREQ(x->data[3], b);
  EXPECT_EQ(x->length[1], 0);
  EXPECT_EQ(x->length[3], 2);
  EXPECT_EQ(x->notNull[0], 0);
  EXPECT_EQ(x->notNull[1], 1);
  EXPECT_EQ(x->notNull[2], 0);
  EXPECT_EQ(x->notNull[3], 1);
  EXPECT_EQ(x->notNull[4], 0);
  EXPECT_EQ(arrowOffset, 5);
  EXPECT_EQ(orcOffset, 5);
  writer->add(*batch);
  writer->close();
}

//LargeBinary
TEST(TestAdapter, writeLargeBinaryEmpty) {
  LargeBinaryBuilder builder;
  std::shared_ptr<Array> array;
  (void)(builder.Finish(&array));
  MemoryOutputStream mem_stream(DEFAULT_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(liborc::Type::buildTypeFromString("struct<x:binary>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer =
    createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch =
    writer->createRowBatch(batchSize);
  liborc::StructVectorBatch *root =
    internal::checked_cast<liborc::StructVectorBatch *>(batch.get());
  liborc::StringVectorBatch *x =
    internal::checked_cast<liborc::StringVectorBatch *>(root->fields[0]);
  DataType* arrowType = large_binary().get();
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize, array.get());
  if (!st.ok()) {
    FAIL() << "ORC ColumnBatch not successfully filled";
  }
  EXPECT_EQ(x->numElements, 0);
  EXPECT_FALSE(x->hasNulls);
  EXPECT_EQ(arrowOffset, 0);
  EXPECT_EQ(orcOffset, 0);
  writer->add(*batch);
  writer->close();
}
TEST(TestAdapter, writeLargeBinaryNoNulls) {
  LargeBinaryBuilder builder;
  char a[] = "\xf4", b[] = "\x22\x0e",c[] = "",d[] = "\xff\x66\xbf\x5b";
  (void)(builder.Append(a));
  (void)(builder.Append(b));
  (void)(builder.Append(c));
  (void)(builder.Append(d));
  std::shared_ptr<Array> array;
  (void)(builder.Finish(&array));
  MemoryOutputStream mem_stream(DEFAULT_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(liborc::Type::buildTypeFromString("struct<x:binary>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer =
    createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch =
    writer->createRowBatch(batchSize);
  liborc::StructVectorBatch *root =
    internal::checked_cast<liborc::StructVectorBatch *>(batch.get());
  liborc::StringVectorBatch *x =
    internal::checked_cast<liborc::StringVectorBatch *>(root->fields[0]);
  DataType* arrowType = large_binary().get();
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize, array.get());
  if (!st.ok()) {
    FAIL() << "ORC ColumnBatch not successfully filled";
  }
  EXPECT_EQ(x->numElements, 4);
  EXPECT_FALSE(x->hasNulls);
  EXPECT_STREQ(x->data[0], a);
  EXPECT_STREQ(x->data[1], b);
  EXPECT_STREQ(x->data[2], c);
  EXPECT_STREQ(x->data[3], d);
  EXPECT_EQ(x->length[0], 1);
  EXPECT_EQ(x->length[1], 2);
  EXPECT_EQ(x->length[2], 0);
  EXPECT_EQ(x->length[3], 4);
  EXPECT_EQ(arrowOffset, 4);
  EXPECT_EQ(orcOffset, 4);
  writer->add(*batch);
  writer->close();
}
TEST(TestAdapter, writeLargeBinaryAllNulls) {
  LargeBinaryBuilder builder;
  (void)(builder.AppendNull());
  (void)(builder.AppendNull());
  (void)(builder.AppendNull());
  std::shared_ptr<Array> array;
  (void)(builder.Finish(&array));
  MemoryOutputStream mem_stream(DEFAULT_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(liborc::Type::buildTypeFromString("struct<x:binary>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer =
    createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch =
    writer->createRowBatch(batchSize);
  liborc::StructVectorBatch *root =
    internal::checked_cast<liborc::StructVectorBatch *>(batch.get());
  liborc::StringVectorBatch *x =
    internal::checked_cast<liborc::StringVectorBatch *>(root->fields[0]);
  DataType* arrowType = large_binary().get();
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize, array.get());
  if (!st.ok()) {
    FAIL() << "ORC ColumnBatch not successfully filled";
  }
  EXPECT_EQ(x->numElements, 3);
  EXPECT_TRUE(x->hasNulls);
  EXPECT_EQ(x->notNull[0], 0);
  EXPECT_EQ(x->notNull[1], 0);
  EXPECT_EQ(x->notNull[2], 0);
  EXPECT_EQ(arrowOffset, 3);
  EXPECT_EQ(orcOffset, 3);
  writer->add(*batch);
  writer->close();
}
TEST(TestAdapter, writeLargeBinaryMixed) {
  LargeBinaryBuilder builder;
  char a[] = "", b[] = "\xff\xfe";
  (void)(builder.AppendNull());
  (void)(builder.Append(a));
  (void)(builder.AppendNull());
  (void)(builder.Append(b));
  (void)(builder.AppendNull());
  std::shared_ptr<Array> array;
  (void)(builder.Finish(&array));
  MemoryOutputStream mem_stream(DEFAULT_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(liborc::Type::buildTypeFromString("struct<x:binary>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer =
    createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch =
    writer->createRowBatch(batchSize);
  liborc::StructVectorBatch *root =
    internal::checked_cast<liborc::StructVectorBatch *>(batch.get());
  liborc::StringVectorBatch *x =
    internal::checked_cast<liborc::StringVectorBatch *>(root->fields[0]);
  DataType* arrowType = large_binary().get();
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize, array.get());
  if (!st.ok()) {
    FAIL() << "ORC ColumnBatch not successfully filled";
  }
  EXPECT_EQ(x->numElements, 5);
  EXPECT_TRUE(x->hasNulls);
  EXPECT_STREQ(x->data[1], a);
  EXPECT_STREQ(x->data[3], b);
  EXPECT_EQ(x->length[1], 0);
  EXPECT_EQ(x->length[3], 2);
  EXPECT_EQ(x->notNull[0], 0);
  EXPECT_EQ(x->notNull[1], 1);
  EXPECT_EQ(x->notNull[2], 0);
  EXPECT_EQ(x->notNull[3], 1);
  EXPECT_EQ(x->notNull[4], 0);
  EXPECT_EQ(arrowOffset, 5);
  EXPECT_EQ(orcOffset, 5);
  writer->add(*batch);
  writer->close();
}

//FixedSizeBinary
TEST(TestAdapter, writeFixedSizeBinaryEmpty) {
  FixedSizeBinaryBuilder builder(fixed_size_binary(4));
  std::shared_ptr<Array> array;
  (void)(builder.Finish(&array));
  MemoryOutputStream mem_stream(DEFAULT_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(liborc::Type::buildTypeFromString("struct<x:binary>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer =
    createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch =
    writer->createRowBatch(batchSize);
  liborc::StructVectorBatch *root =
    internal::checked_cast<liborc::StructVectorBatch *>(batch.get());
  liborc::StringVectorBatch *x =
    internal::checked_cast<liborc::StringVectorBatch *>(root->fields[0]);
  DataType* arrowType = fixed_size_binary(4).get();
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize, array.get());
  if (!st.ok()) {
    FAIL() << "ORC ColumnBatch not successfully filled";
  }
  EXPECT_EQ(x->numElements, 0);
  EXPECT_FALSE(x->hasNulls);
  EXPECT_EQ(arrowOffset, 0);
  EXPECT_EQ(orcOffset, 0);
  writer->add(*batch);
  writer->close();
}
TEST(TestAdapter, writeFixedSizeBinaryNoNulls) {
  FixedSizeBinaryBuilder builder(fixed_size_binary(4));
  char a[] = "\xf4\xd2\x21\x39", b[] = "\x22\x0e\x09\xaa",c[] = "\x34\x01\x43\x42",d[] = "\xff\x66\xbf\x5b";
  (void)(builder.Append(a));
  (void)(builder.Append(b));
  (void)(builder.Append(c));
  (void)(builder.Append(d));
  std::shared_ptr<Array> array;
  (void)(builder.Finish(&array));
  MemoryOutputStream mem_stream(DEFAULT_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(liborc::Type::buildTypeFromString("struct<x:binary>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer =
    createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch =
    writer->createRowBatch(batchSize);
  liborc::StructVectorBatch *root =
    internal::checked_cast<liborc::StructVectorBatch *>(batch.get());
  liborc::StringVectorBatch *x =
    internal::checked_cast<liborc::StringVectorBatch *>(root->fields[0]);
  DataType* arrowType = fixed_size_binary(4).get();
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize, array.get());
  if (!st.ok()) {
    FAIL() << "ORC ColumnBatch not successfully filled";
  }
  EXPECT_EQ(x->numElements, 4);
  EXPECT_FALSE(x->hasNulls);
  EXPECT_STREQ(x->data[0], a);
  EXPECT_STREQ(x->data[1], b);
  EXPECT_STREQ(x->data[2], c);
  EXPECT_STREQ(x->data[3], d);
  EXPECT_EQ(x->length[0], 4);
  EXPECT_EQ(x->length[1], 4);
  EXPECT_EQ(x->length[2], 4);
  EXPECT_EQ(x->length[3], 4);
  EXPECT_EQ(arrowOffset, 4);
  EXPECT_EQ(orcOffset, 4);
  writer->add(*batch);
  writer->close();
}
TEST(TestAdapter, writeFixedSizeBinaryAllNulls) {
  FixedSizeBinaryBuilder builder(fixed_size_binary(1));
  (void)(builder.AppendNull());
  (void)(builder.AppendNull());
  (void)(builder.AppendNull());
  std::shared_ptr<Array> array;
  (void)(builder.Finish(&array));
  MemoryOutputStream mem_stream(DEFAULT_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(liborc::Type::buildTypeFromString("struct<x:binary>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer =
    createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch =
    writer->createRowBatch(batchSize);
  liborc::StructVectorBatch *root =
    internal::checked_cast<liborc::StructVectorBatch *>(batch.get());
  liborc::StringVectorBatch *x =
    internal::checked_cast<liborc::StringVectorBatch *>(root->fields[0]);
  DataType* arrowType = fixed_size_binary(1).get();
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize, array.get());
  if (!st.ok()) {
    FAIL() << "ORC ColumnBatch not successfully filled";
  }
  EXPECT_EQ(x->numElements, 3);
  EXPECT_TRUE(x->hasNulls);
  EXPECT_EQ(x->notNull[0], 0);
  EXPECT_EQ(x->notNull[1], 0);
  EXPECT_EQ(x->notNull[2], 0);
  EXPECT_EQ(arrowOffset, 3);
  EXPECT_EQ(orcOffset, 3);
  writer->add(*batch);
  writer->close();
}
TEST(TestAdapter, writeFixedSizeBinaryMixed) {
  FixedSizeBinaryBuilder builder(fixed_size_binary(2));
  char a[] = "\xa2\x34", b[] = "\xff\xfe";
  (void)(builder.AppendNull());
  (void)(builder.Append(a));
  (void)(builder.AppendNull());
  (void)(builder.Append(b));
  (void)(builder.AppendNull());
  std::shared_ptr<Array> array;
  (void)(builder.Finish(&array));
  MemoryOutputStream mem_stream(DEFAULT_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(liborc::Type::buildTypeFromString("struct<x:binary>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer =
    createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch =
    writer->createRowBatch(batchSize);
  liborc::StructVectorBatch *root =
    internal::checked_cast<liborc::StructVectorBatch *>(batch.get());
  liborc::StringVectorBatch *x =
    internal::checked_cast<liborc::StringVectorBatch *>(root->fields[0]);
  DataType* arrowType = fixed_size_binary(2).get();
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize, array.get());
  if (!st.ok()) {
    FAIL() << "ORC ColumnBatch not successfully filled";
  }
  EXPECT_EQ(x->numElements, 5);
  EXPECT_TRUE(x->hasNulls);
  EXPECT_STREQ(x->data[1], a);
  EXPECT_STREQ(x->data[3], b);
  EXPECT_EQ(x->length[1], 2);
  EXPECT_EQ(x->length[3], 2);
  EXPECT_EQ(x->notNull[0], 0);
  EXPECT_EQ(x->notNull[1], 1);
  EXPECT_EQ(x->notNull[2], 0);
  EXPECT_EQ(x->notNull[3], 1);
  EXPECT_EQ(x->notNull[4], 0);
  EXPECT_EQ(arrowOffset, 5);
  EXPECT_EQ(orcOffset, 5);
  writer->add(*batch);
  writer->close();
}

}  // namespace arrow
