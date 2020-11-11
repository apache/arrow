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

// //Bool
// TEST(TestAdapterWriteNumerical, writeBoolEmpty) {
//   BooleanBuilder builder;
//   std::shared_ptr<Array> array;
//   (void)(builder.Finish(&array));
//   MemoryOutputStream mem_stream(DEFAULT_MEM_STREAM_SIZE);
//   ORC_UNIQUE_PTR<liborc::Type> schema(liborc::Type::buildTypeFromString("struct<x:boolean>"));
//   liborc::WriterOptions options;
//   ORC_UNIQUE_PTR<liborc::Writer> writer =
//     createWriter(*schema, &mem_stream, options);
//   uint64_t batchSize = 1024;
//   ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch =
//     writer->createRowBatch(batchSize);
//   liborc::StructVectorBatch *root =
//     internal::checked_cast<liborc::StructVectorBatch *>(batch.get());
//   liborc::LongVectorBatch *x =
//     internal::checked_cast<liborc::LongVectorBatch *>(root->fields[0]);
//   DataType* arrowType = boolean().get();
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
// TEST(TestAdapterWriteNumerical, writeBoolNoNulls) {
//   BooleanBuilder builder;
//   (void)(builder.Append(true));
//   (void)(builder.Append(false));
//   (void)(builder.Append(false));
//   (void)(builder.Append(true));
//   std::shared_ptr<Array> array;
//   (void)(builder.Finish(&array));
//   MemoryOutputStream mem_stream(DEFAULT_MEM_STREAM_SIZE);
//   ORC_UNIQUE_PTR<liborc::Type> schema(liborc::Type::buildTypeFromString("struct<x:boolean>"));
//   liborc::WriterOptions options;
//   ORC_UNIQUE_PTR<liborc::Writer> writer =
//     createWriter(*schema, &mem_stream, options);
//   uint64_t batchSize = 1024;
//   ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch =
//     writer->createRowBatch(batchSize);
//   liborc::StructVectorBatch *root =
//     internal::checked_cast<liborc::StructVectorBatch *>(batch.get());
//   liborc::LongVectorBatch *x =
//     internal::checked_cast<liborc::LongVectorBatch *>(root->fields[0]);
//   DataType* arrowType = boolean().get();
//   int64_t arrowOffset = 0;
//   int64_t orcOffset = 0;
//   Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize, array.get());
//   if (!st.ok()) {
//     FAIL() << "ORC ColumnBatch not successfully filled";
//   }
//   EXPECT_EQ(x->numElements, 4);
//   EXPECT_FALSE(x->hasNulls);
//   EXPECT_EQ(x->data[0], 1);
//   EXPECT_EQ(x->data[1], 0);
//   EXPECT_EQ(x->data[2], 0);
//   EXPECT_EQ(x->data[3], 1);
//   EXPECT_EQ(arrowOffset, 4);
//   EXPECT_EQ(orcOffset, 4);
//   writer->add(*batch);
//   writer->close();
// }
// TEST(TestAdapterWriteNumerical, writeBoolAllNulls) {
//   BooleanBuilder builder;
//   (void)(builder.AppendNull());
//   (void)(builder.AppendNull());
//   (void)(builder.AppendNull());
//   std::shared_ptr<Array> array;
//   (void)(builder.Finish(&array));
//   MemoryOutputStream mem_stream(DEFAULT_MEM_STREAM_SIZE);
//   ORC_UNIQUE_PTR<liborc::Type> schema(liborc::Type::buildTypeFromString("struct<x:boolean>"));
//   liborc::WriterOptions options;
//   ORC_UNIQUE_PTR<liborc::Writer> writer =
//     createWriter(*schema, &mem_stream, options);
//   uint64_t batchSize = 1024;
//   ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch =
//     writer->createRowBatch(batchSize);
//   liborc::StructVectorBatch *root =
//     internal::checked_cast<liborc::StructVectorBatch *>(batch.get());
//   liborc::LongVectorBatch *x =
//     internal::checked_cast<liborc::LongVectorBatch *>(root->fields[0]);
//   DataType* arrowType = boolean().get();
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
// TEST(TestAdapterWriteNumerical, writeBooleanMixed) {
//   BooleanBuilder builder;
//   (void)(builder.AppendNull());
//   (void)(builder.Append(true));
//   (void)(builder.AppendNull());
//   (void)(builder.Append(false));
//   (void)(builder.AppendNull());
//   std::shared_ptr<Array> array;
//   (void)(builder.Finish(&array));
//   MemoryOutputStream mem_stream(DEFAULT_MEM_STREAM_SIZE);
//   ORC_UNIQUE_PTR<liborc::Type> schema(liborc::Type::buildTypeFromString("struct<x:boolean>"));
//   liborc::WriterOptions options;
//   ORC_UNIQUE_PTR<liborc::Writer> writer =
//     createWriter(*schema, &mem_stream, options);
//   uint64_t batchSize = 1024;
//   ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch =
//     writer->createRowBatch(batchSize);
//   liborc::StructVectorBatch *root =
//     internal::checked_cast<liborc::StructVectorBatch *>(batch.get());
//   liborc::LongVectorBatch *x =
//     internal::checked_cast<liborc::LongVectorBatch *>(root->fields[0]);
//   DataType* arrowType = boolean().get();
//   int64_t arrowOffset = 0;
//   int64_t orcOffset = 0;
//   Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize, array.get());
//   if (!st.ok()) {
//     FAIL() << "ORC ColumnBatch not successfully filled";
//   }
//   EXPECT_EQ(x->numElements, 5);
//   EXPECT_TRUE(x->hasNulls);
//   EXPECT_EQ(x->data[1], 1);
//   EXPECT_EQ(x->data[3], 0);
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

// //Int8
// TEST(TestAdapterWriteNumerical, writeInt8Empty) {
//   Int8Builder builder;
//   std::shared_ptr<Array> array;
//   (void)(builder.Finish(&array));
//   MemoryOutputStream mem_stream(DEFAULT_MEM_STREAM_SIZE);
//   ORC_UNIQUE_PTR<liborc::Type> schema(liborc::Type::buildTypeFromString("struct<x:tinyint>"));
//   liborc::WriterOptions options;
//   ORC_UNIQUE_PTR<liborc::Writer> writer =
//     createWriter(*schema, &mem_stream, options);
//   uint64_t batchSize = 1024;
//   ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch =
//     writer->createRowBatch(batchSize);
//   liborc::StructVectorBatch *root =
//     internal::checked_cast<liborc::StructVectorBatch *>(batch.get());
//   liborc::LongVectorBatch *x =
//     internal::checked_cast<liborc::LongVectorBatch *>(root->fields[0]);
//   DataType* arrowType = int8().get();
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
// TEST(TestAdapterWriteNumerical, writeInt8NoNulls) {
//   Int8Builder builder;
//   (void)(builder.Append(1));
//   (void)(builder.Append(2));
//   (void)(builder.Append(3));
//   (void)(builder.Append(4));
//   std::shared_ptr<Array> array;
//   (void)(builder.Finish(&array));
//   MemoryOutputStream mem_stream(DEFAULT_MEM_STREAM_SIZE);
//   ORC_UNIQUE_PTR<liborc::Type> schema(liborc::Type::buildTypeFromString("struct<x:tinyint>"));
//   liborc::WriterOptions options;
//   ORC_UNIQUE_PTR<liborc::Writer> writer =
//     createWriter(*schema, &mem_stream, options);
//   uint64_t batchSize = 1024;
//   ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch =
//     writer->createRowBatch(batchSize);
//   liborc::StructVectorBatch *root =
//     internal::checked_cast<liborc::StructVectorBatch *>(batch.get());
//   liborc::LongVectorBatch *x =
//     internal::checked_cast<liborc::LongVectorBatch *>(root->fields[0]);
//   DataType* arrowType = int8().get();
//   int64_t arrowOffset = 0;
//   int64_t orcOffset = 0;
//   Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize, array.get());
//   if (!st.ok()) {
//     FAIL() << "ORC ColumnBatch not successfully filled";
//   }
//   EXPECT_EQ(x->numElements, 4);
//   EXPECT_FALSE(x->hasNulls);
//   EXPECT_EQ(x->data[0], 1);
//   EXPECT_EQ(x->data[1], 2);
//   EXPECT_EQ(x->data[2], 3);
//   EXPECT_EQ(x->data[3], 4);
//   EXPECT_EQ(arrowOffset, 4);
//   EXPECT_EQ(orcOffset, 4);
//   writer->add(*batch);
//   writer->close();
// }
// TEST(TestAdapterWriteNumerical, writeInt8AllNulls) {
//   Int8Builder builder;
//   (void)(builder.AppendNull());
//   (void)(builder.AppendNull());
//   (void)(builder.AppendNull());
//   std::shared_ptr<Array> array;
//   (void)(builder.Finish(&array));
//   MemoryOutputStream mem_stream(DEFAULT_MEM_STREAM_SIZE);
//   ORC_UNIQUE_PTR<liborc::Type> schema(liborc::Type::buildTypeFromString("struct<x:tinyint>"));
//   liborc::WriterOptions options;
//   ORC_UNIQUE_PTR<liborc::Writer> writer =
//     createWriter(*schema, &mem_stream, options);
//   uint64_t batchSize = 1024;
//   ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch =
//     writer->createRowBatch(batchSize);
//   liborc::StructVectorBatch *root =
//     internal::checked_cast<liborc::StructVectorBatch *>(batch.get());
//   liborc::LongVectorBatch *x =
//     internal::checked_cast<liborc::LongVectorBatch *>(root->fields[0]);
//   DataType* arrowType = int8().get();
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
// TEST(TestAdapterWriteNumerical, writeInt8Mixed) {
//   Int8Builder builder;
//   (void)(builder.AppendNull());
//   (void)(builder.Append(1));
//   (void)(builder.AppendNull());
//   (void)(builder.Append(2));
//   (void)(builder.AppendNull());
//   std::shared_ptr<Array> array;
//   (void)(builder.Finish(&array));
//   MemoryOutputStream mem_stream(DEFAULT_MEM_STREAM_SIZE);
//   ORC_UNIQUE_PTR<liborc::Type> schema(liborc::Type::buildTypeFromString("struct<x:tinyint>"));
//   liborc::WriterOptions options;
//   ORC_UNIQUE_PTR<liborc::Writer> writer =
//     createWriter(*schema, &mem_stream, options);
//   uint64_t batchSize = 1024;
//   ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch =
//     writer->createRowBatch(batchSize);
//   liborc::StructVectorBatch *root =
//     internal::checked_cast<liborc::StructVectorBatch *>(batch.get());
//   liborc::LongVectorBatch *x =
//     internal::checked_cast<liborc::LongVectorBatch *>(root->fields[0]);
//   DataType* arrowType = int8().get();
//   int64_t arrowOffset = 0;
//   int64_t orcOffset = 0;
//   Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize, array.get());
//   if (!st.ok()) {
//     FAIL() << "ORC ColumnBatch not successfully filled";
//   }
//   EXPECT_EQ(x->numElements, 5);
//   EXPECT_TRUE(x->hasNulls);
//   EXPECT_EQ(x->data[1], 1);
//   EXPECT_EQ(x->data[3], 2);
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

// //Int16
// TEST(TestAdapterWriteNumerical, writeInt16Empty) {
//   Int16Builder builder;
//   std::shared_ptr<Array> array;
//   (void)(builder.Finish(&array));
//   MemoryOutputStream mem_stream(DEFAULT_MEM_STREAM_SIZE);
//   ORC_UNIQUE_PTR<liborc::Type> schema(liborc::Type::buildTypeFromString("struct<x:smallint>"));
//   liborc::WriterOptions options;
//   ORC_UNIQUE_PTR<liborc::Writer> writer =
//     createWriter(*schema, &mem_stream, options);
//   uint64_t batchSize = 1024;
//   ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch =
//     writer->createRowBatch(batchSize);
//   liborc::StructVectorBatch *root =
//     internal::checked_cast<liborc::StructVectorBatch *>(batch.get());
//   liborc::LongVectorBatch *x =
//     internal::checked_cast<liborc::LongVectorBatch *>(root->fields[0]);
//   DataType* arrowType = int16().get();
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
// TEST(TestAdapterWriteNumerical, writeInt16NoNulls) {
//   Int16Builder builder;
//   (void)(builder.Append(1));
//   (void)(builder.Append(2));
//   (void)(builder.Append(3));
//   (void)(builder.Append(4));
//   std::shared_ptr<Array> array;
//   (void)(builder.Finish(&array));
//   MemoryOutputStream mem_stream(DEFAULT_MEM_STREAM_SIZE);
//   ORC_UNIQUE_PTR<liborc::Type> schema(liborc::Type::buildTypeFromString("struct<x:smallint>"));
//   liborc::WriterOptions options;
//   ORC_UNIQUE_PTR<liborc::Writer> writer =
//     createWriter(*schema, &mem_stream, options);
//   uint64_t batchSize = 1024;
//   ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch =
//     writer->createRowBatch(batchSize);
//   liborc::StructVectorBatch *root =
//     internal::checked_cast<liborc::StructVectorBatch *>(batch.get());
//   liborc::LongVectorBatch *x =
//     internal::checked_cast<liborc::LongVectorBatch *>(root->fields[0]);
//   DataType* arrowType = int16().get();
//   int64_t arrowOffset = 0;
//   int64_t orcOffset = 0;
//   Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize, array.get());
//   if (!st.ok()) {
//     FAIL() << "ORC ColumnBatch not successfully filled";
//   }
//   EXPECT_EQ(x->numElements, 4);
//   EXPECT_FALSE(x->hasNulls);
//   EXPECT_EQ(x->data[0], 1);
//   EXPECT_EQ(x->data[1], 2);
//   EXPECT_EQ(x->data[2], 3);
//   EXPECT_EQ(x->data[3], 4);
//   EXPECT_EQ(arrowOffset, 4);
//   EXPECT_EQ(orcOffset, 4);
//   writer->add(*batch);
//   writer->close();
// }
// TEST(TestAdapterWriteNumerical, writeInt16AllNulls) {
//   Int16Builder builder;
//   (void)(builder.AppendNull());
//   (void)(builder.AppendNull());
//   (void)(builder.AppendNull());
//   std::shared_ptr<Array> array;
//   (void)(builder.Finish(&array));
//   MemoryOutputStream mem_stream(DEFAULT_MEM_STREAM_SIZE);
//   ORC_UNIQUE_PTR<liborc::Type> schema(liborc::Type::buildTypeFromString("struct<x:smallint>"));
//   liborc::WriterOptions options;
//   ORC_UNIQUE_PTR<liborc::Writer> writer =
//     createWriter(*schema, &mem_stream, options);
//   uint64_t batchSize = 1024;
//   ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch =
//     writer->createRowBatch(batchSize);
//   liborc::StructVectorBatch *root =
//     internal::checked_cast<liborc::StructVectorBatch *>(batch.get());
//   liborc::LongVectorBatch *x =
//     internal::checked_cast<liborc::LongVectorBatch *>(root->fields[0]);
//   DataType* arrowType = int16().get();
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
// TEST(TestAdapterWriteNumerical, writeInt16Mixed) {
//   Int16Builder builder;
//   (void)(builder.AppendNull());
//   (void)(builder.Append(1));
//   (void)(builder.AppendNull());
//   (void)(builder.Append(2));
//   (void)(builder.AppendNull());
//   std::shared_ptr<Array> array;
//   (void)(builder.Finish(&array));
//   MemoryOutputStream mem_stream(DEFAULT_MEM_STREAM_SIZE);
//   ORC_UNIQUE_PTR<liborc::Type> schema(liborc::Type::buildTypeFromString("struct<x:smallint>"));
//   liborc::WriterOptions options;
//   ORC_UNIQUE_PTR<liborc::Writer> writer =
//     createWriter(*schema, &mem_stream, options);
//   uint64_t batchSize = 1024;
//   ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch =
//     writer->createRowBatch(batchSize);
//   liborc::StructVectorBatch *root =
//     internal::checked_cast<liborc::StructVectorBatch *>(batch.get());
//   liborc::LongVectorBatch *x =
//     internal::checked_cast<liborc::LongVectorBatch *>(root->fields[0]);
//   DataType* arrowType = int16().get();
//   int64_t arrowOffset = 0;
//   int64_t orcOffset = 0;
//   Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize, array.get());
//   if (!st.ok()) {
//     FAIL() << "ORC ColumnBatch not successfully filled";
//   }
//   EXPECT_EQ(x->numElements, 5);
//   EXPECT_TRUE(x->hasNulls);
//   EXPECT_EQ(x->data[1], 1);
//   EXPECT_EQ(x->data[3], 2);
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

// //Int32
// TEST(TestAdapterWriteNumerical, writeInt32Empty) {
//   Int32Builder builder;
//   std::shared_ptr<Array> array;
//   (void)(builder.Finish(&array));
//   MemoryOutputStream mem_stream(DEFAULT_MEM_STREAM_SIZE);
//   ORC_UNIQUE_PTR<liborc::Type> schema(liborc::Type::buildTypeFromString("struct<x:int>"));
//   liborc::WriterOptions options;
//   ORC_UNIQUE_PTR<liborc::Writer> writer =
//     createWriter(*schema, &mem_stream, options);
//   uint64_t batchSize = 1024;
//   ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch =
//     writer->createRowBatch(batchSize);
//   liborc::StructVectorBatch *root =
//     internal::checked_cast<liborc::StructVectorBatch *>(batch.get());
//   liborc::LongVectorBatch *x =
//     internal::checked_cast<liborc::LongVectorBatch *>(root->fields[0]);
//   DataType* arrowType = int32().get();
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
// TEST(TestAdapterWriteNumerical, writeInt32NoNulls) {
//   Int32Builder builder;
//   (void)(builder.Append(1));
//   (void)(builder.Append(2));
//   (void)(builder.Append(3));
//   (void)(builder.Append(4));
//   std::shared_ptr<Array> array;
//   (void)(builder.Finish(&array));
//   MemoryOutputStream mem_stream(DEFAULT_MEM_STREAM_SIZE);
//   ORC_UNIQUE_PTR<liborc::Type> schema(liborc::Type::buildTypeFromString("struct<x:int>"));
//   liborc::WriterOptions options;
//   ORC_UNIQUE_PTR<liborc::Writer> writer =
//     createWriter(*schema, &mem_stream, options);
//   uint64_t batchSize = 1024;
//   ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch =
//     writer->createRowBatch(batchSize);
//   liborc::StructVectorBatch *root =
//     internal::checked_cast<liborc::StructVectorBatch *>(batch.get());
//   liborc::LongVectorBatch *x =
//     internal::checked_cast<liborc::LongVectorBatch *>(root->fields[0]);
//   DataType* arrowType = int32().get();
//   int64_t arrowOffset = 0;
//   int64_t orcOffset = 0;
//   Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize, array.get());
//   if (!st.ok()) {
//     FAIL() << "ORC ColumnBatch not successfully filled";
//   }
//   EXPECT_EQ(x->numElements, 4);
//   EXPECT_FALSE(x->hasNulls);
//   EXPECT_EQ(x->data[0], 1);
//   EXPECT_EQ(x->data[1], 2);
//   EXPECT_EQ(x->data[2], 3);
//   EXPECT_EQ(x->data[3], 4);
//   EXPECT_EQ(arrowOffset, 4);
//   EXPECT_EQ(orcOffset, 4);
//   writer->add(*batch);
//   writer->close();
// }
// TEST(TestAdapterWriteNumerical, writeInt32AllNulls) {
//   Int32Builder builder;
//   (void)(builder.AppendNull());
//   (void)(builder.AppendNull());
//   (void)(builder.AppendNull());
//   std::shared_ptr<Array> array;
//   (void)(builder.Finish(&array));
//   MemoryOutputStream mem_stream(DEFAULT_MEM_STREAM_SIZE);
//   ORC_UNIQUE_PTR<liborc::Type> schema(liborc::Type::buildTypeFromString("struct<x:int>"));
//   liborc::WriterOptions options;
//   ORC_UNIQUE_PTR<liborc::Writer> writer =
//     createWriter(*schema, &mem_stream, options);
//   uint64_t batchSize = 1024;
//   ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch =
//     writer->createRowBatch(batchSize);
//   liborc::StructVectorBatch *root =
//     internal::checked_cast<liborc::StructVectorBatch *>(batch.get());
//   liborc::LongVectorBatch *x =
//     internal::checked_cast<liborc::LongVectorBatch *>(root->fields[0]);
//   DataType* arrowType = int32().get();
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
// TEST(TestAdapterWriteNumerical, writeInt32Mixed) {
//   Int32Builder builder;
//   (void)(builder.AppendNull());
//   (void)(builder.Append(1));
//   (void)(builder.AppendNull());
//   (void)(builder.Append(2));
//   (void)(builder.AppendNull());
//   std::shared_ptr<Array> array;
//   (void)(builder.Finish(&array));
//   MemoryOutputStream mem_stream(DEFAULT_MEM_STREAM_SIZE);
//   ORC_UNIQUE_PTR<liborc::Type> schema(liborc::Type::buildTypeFromString("struct<x:int>"));
//   liborc::WriterOptions options;
//   ORC_UNIQUE_PTR<liborc::Writer> writer =
//     createWriter(*schema, &mem_stream, options);
//   uint64_t batchSize = 1024;
//   ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch =
//     writer->createRowBatch(batchSize);
//   liborc::StructVectorBatch *root =
//     internal::checked_cast<liborc::StructVectorBatch *>(batch.get());
//   liborc::LongVectorBatch *x =
//     internal::checked_cast<liborc::LongVectorBatch *>(root->fields[0]);
//   DataType* arrowType = int32().get();
//   int64_t arrowOffset = 0;
//   int64_t orcOffset = 0;
//   Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize, array.get());
//   if (!st.ok()) {
//     FAIL() << "ORC ColumnBatch not successfully filled";
//   }
//   EXPECT_EQ(x->numElements, 5);
//   EXPECT_TRUE(x->hasNulls);
//   EXPECT_EQ(x->data[1], 1);
//   EXPECT_EQ(x->data[3], 2);
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

// //Int64
// TEST(TestAdapterWriteNumerical, writeInt64Empty) {
//   Int64Builder builder;
//   std::shared_ptr<Array> array;
//   (void)(builder.Finish(&array));
//   MemoryOutputStream mem_stream(DEFAULT_MEM_STREAM_SIZE);
//   ORC_UNIQUE_PTR<liborc::Type> schema(liborc::Type::buildTypeFromString("struct<x:bigint>"));
//   liborc::WriterOptions options;
//   ORC_UNIQUE_PTR<liborc::Writer> writer =
//     createWriter(*schema, &mem_stream, options);
//   uint64_t batchSize = 1024;
//   ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch =
//     writer->createRowBatch(batchSize);
//   liborc::StructVectorBatch *root =
//     internal::checked_cast<liborc::StructVectorBatch *>(batch.get());
//   liborc::LongVectorBatch *x =
//     internal::checked_cast<liborc::LongVectorBatch *>(root->fields[0]);
//   DataType* arrowType = int64().get();
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
// TEST(TestAdapterWriteNumerical, writeInt64NoNulls) {
//   Int64Builder builder;
//   (void)(builder.Append(1));
//   (void)(builder.Append(2));
//   (void)(builder.Append(3));
//   (void)(builder.Append(4));
//   std::shared_ptr<Array> array;
//   (void)(builder.Finish(&array));
//   MemoryOutputStream mem_stream(DEFAULT_MEM_STREAM_SIZE);
//   ORC_UNIQUE_PTR<liborc::Type> schema(liborc::Type::buildTypeFromString("struct<x:bigint>"));
//   liborc::WriterOptions options;
//   ORC_UNIQUE_PTR<liborc::Writer> writer =
//     createWriter(*schema, &mem_stream, options);
//   uint64_t batchSize = 1024;
//   ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch =
//     writer->createRowBatch(batchSize);
//   liborc::StructVectorBatch *root =
//     internal::checked_cast<liborc::StructVectorBatch *>(batch.get());
//   liborc::LongVectorBatch *x =
//     internal::checked_cast<liborc::LongVectorBatch *>(root->fields[0]);
//   DataType* arrowType = int64().get();
//   int64_t arrowOffset = 0;
//   int64_t orcOffset = 0;
//   Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize, array.get());
//   if (!st.ok()) {
//     FAIL() << "ORC ColumnBatch not successfully filled";
//   }
//   EXPECT_EQ(x->numElements, 4);
//   EXPECT_FALSE(x->hasNulls);
//   EXPECT_EQ(x->data[0], 1);
//   EXPECT_EQ(x->data[1], 2);
//   EXPECT_EQ(x->data[2], 3);
//   EXPECT_EQ(x->data[3], 4);
//   EXPECT_EQ(arrowOffset, 4);
//   EXPECT_EQ(orcOffset, 4);
//   writer->add(*batch);
//   writer->close();
// }
// TEST(TestAdapterWriteNumerical, writeInt64AllNulls) {
//   Int64Builder builder;
//   (void)(builder.AppendNull());
//   (void)(builder.AppendNull());
//   (void)(builder.AppendNull());
//   std::shared_ptr<Array> array;
//   (void)(builder.Finish(&array));
//   MemoryOutputStream mem_stream(DEFAULT_MEM_STREAM_SIZE);
//   ORC_UNIQUE_PTR<liborc::Type> schema(liborc::Type::buildTypeFromString("struct<x:bigint>"));
//   liborc::WriterOptions options;
//   ORC_UNIQUE_PTR<liborc::Writer> writer =
//     createWriter(*schema, &mem_stream, options);
//   uint64_t batchSize = 1024;
//   ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch =
//     writer->createRowBatch(batchSize);
//   liborc::StructVectorBatch *root =
//     internal::checked_cast<liborc::StructVectorBatch *>(batch.get());
//   liborc::LongVectorBatch *x =
//     internal::checked_cast<liborc::LongVectorBatch *>(root->fields[0]);
//   DataType* arrowType = int64().get();
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
// TEST(TestAdapterWriteNumerical, writeInt64Mixed) {
//   Int64Builder builder;
//   (void)(builder.AppendNull());
//   (void)(builder.Append(1));
//   (void)(builder.AppendNull());
//   (void)(builder.Append(2));
//   (void)(builder.AppendNull());
//   std::shared_ptr<Array> array;
//   (void)(builder.Finish(&array));
//   MemoryOutputStream mem_stream(DEFAULT_MEM_STREAM_SIZE);
//   ORC_UNIQUE_PTR<liborc::Type> schema(liborc::Type::buildTypeFromString("struct<x:bigint>"));
//   liborc::WriterOptions options;
//   ORC_UNIQUE_PTR<liborc::Writer> writer =
//     createWriter(*schema, &mem_stream, options);
//   uint64_t batchSize = 1024;
//   ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch =
//     writer->createRowBatch(batchSize);
//   liborc::StructVectorBatch *root =
//     internal::checked_cast<liborc::StructVectorBatch *>(batch.get());
//   liborc::LongVectorBatch *x =
//     internal::checked_cast<liborc::LongVectorBatch *>(root->fields[0]);
//   DataType* arrowType = int64().get();
//   int64_t arrowOffset = 0;
//   int64_t orcOffset = 0;
//   Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize, array.get());
//   if (!st.ok()) {
//     FAIL() << "ORC ColumnBatch not successfully filled";
//   }
//   EXPECT_EQ(x->numElements, 5);
//   EXPECT_TRUE(x->hasNulls);
//   EXPECT_EQ(x->data[1], 1);
//   EXPECT_EQ(x->data[3], 2);
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

// //Float
// TEST(TestAdapterWriteNumerical, writeFloatEmpty) {
//   FloatBuilder builder;
//   std::shared_ptr<Array> array;
//   (void)(builder.Finish(&array));
//   MemoryOutputStream mem_stream(DEFAULT_MEM_STREAM_SIZE);
//   ORC_UNIQUE_PTR<liborc::Type> schema(liborc::Type::buildTypeFromString("struct<x:float>"));
//   liborc::WriterOptions options;
//   ORC_UNIQUE_PTR<liborc::Writer> writer =
//     createWriter(*schema, &mem_stream, options);
//   uint64_t batchSize = 1024;
//   ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch =
//     writer->createRowBatch(batchSize);
//   liborc::StructVectorBatch *root =
//     internal::checked_cast<liborc::StructVectorBatch *>(batch.get());
//   liborc::DoubleVectorBatch *x =
//     internal::checked_cast<liborc::DoubleVectorBatch *>(root->fields[0]);
//   DataType* arrowType = float32().get();
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
// TEST(TestAdapterWriteNumerical, writeFloatNoNulls) {
//   FloatBuilder builder;
//   (void)(builder.Append(1.5));
//   (void)(builder.Append(2.6));
//   (void)(builder.Append(3.7));
//   (void)(builder.Append(4.8));
//   std::shared_ptr<Array> array;
//   (void)(builder.Finish(&array));
//   MemoryOutputStream mem_stream(DEFAULT_MEM_STREAM_SIZE);
//   ORC_UNIQUE_PTR<liborc::Type> schema(liborc::Type::buildTypeFromString("struct<x:float>"));
//   liborc::WriterOptions options;
//   ORC_UNIQUE_PTR<liborc::Writer> writer =
//     createWriter(*schema, &mem_stream, options);
//   uint64_t batchSize = 1024;
//   ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch =
//     writer->createRowBatch(batchSize);
//   liborc::StructVectorBatch *root =
//     internal::checked_cast<liborc::StructVectorBatch *>(batch.get());
//   liborc::DoubleVectorBatch *x =
//     internal::checked_cast<liborc::DoubleVectorBatch *>(root->fields[0]);
//   DataType* arrowType = float32().get();
//   int64_t arrowOffset = 0;
//   int64_t orcOffset = 0;
//   Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize, array.get());
//   if (!st.ok()) {
//     FAIL() << "ORC ColumnBatch not successfully filled";
//   }
//   EXPECT_EQ(x->numElements, 4);
//   EXPECT_FALSE(x->hasNulls);
//   EXPECT_FLOAT_EQ(x->data[0], 1.5);
//   EXPECT_FLOAT_EQ(x->data[1], 2.6);
//   EXPECT_FLOAT_EQ(x->data[2], 3.7);
//   EXPECT_FLOAT_EQ(x->data[3], 4.8);
//   EXPECT_EQ(arrowOffset, 4);
//   EXPECT_EQ(orcOffset, 4);
//   writer->add(*batch);
//   writer->close();
// }
// TEST(TestAdapterWriteNumerical, writeFloatAllNulls) {
//   FloatBuilder builder;
//   (void)(builder.AppendNull());
//   (void)(builder.AppendNull());
//   (void)(builder.AppendNull());
//   std::shared_ptr<Array> array;
//   (void)(builder.Finish(&array));
//   MemoryOutputStream mem_stream(DEFAULT_MEM_STREAM_SIZE);
//   ORC_UNIQUE_PTR<liborc::Type> schema(liborc::Type::buildTypeFromString("struct<x:float>"));
//   liborc::WriterOptions options;
//   ORC_UNIQUE_PTR<liborc::Writer> writer =
//     createWriter(*schema, &mem_stream, options);
//   uint64_t batchSize = 1024;
//   ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch =
//     writer->createRowBatch(batchSize);
//   liborc::StructVectorBatch *root =
//     internal::checked_cast<liborc::StructVectorBatch *>(batch.get());
//   liborc::DoubleVectorBatch *x =
//     internal::checked_cast<liborc::DoubleVectorBatch *>(root->fields[0]);
//   DataType* arrowType = float32().get();
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
// TEST(TestAdapterWriteNumerical, writeFloatMixed) {
//   FloatBuilder builder;
//   (void)(builder.AppendNull());
//   (void)(builder.Append(1.2));
//   (void)(builder.AppendNull());
//   (void)(builder.Append(2.3));
//   (void)(builder.AppendNull());
//   std::shared_ptr<Array> array;
//   (void)(builder.Finish(&array));
//   MemoryOutputStream mem_stream(DEFAULT_MEM_STREAM_SIZE);
//   ORC_UNIQUE_PTR<liborc::Type> schema(liborc::Type::buildTypeFromString("struct<x:float>"));
//   liborc::WriterOptions options;
//   ORC_UNIQUE_PTR<liborc::Writer> writer =
//     createWriter(*schema, &mem_stream, options);
//   uint64_t batchSize = 1024;
//   ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch =
//     writer->createRowBatch(batchSize);
//   liborc::StructVectorBatch *root =
//     internal::checked_cast<liborc::StructVectorBatch *>(batch.get());
//   liborc::DoubleVectorBatch *x =
//     internal::checked_cast<liborc::DoubleVectorBatch *>(root->fields[0]);
//   DataType* arrowType = float32().get();
//   int64_t arrowOffset = 0;
//   int64_t orcOffset = 0;
//   Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize, array.get());
//   if (!st.ok()) {
//     FAIL() << "ORC ColumnBatch not successfully filled";
//   }
//   EXPECT_EQ(x->numElements, 5);
//   EXPECT_TRUE(x->hasNulls);
//   EXPECT_FLOAT_EQ(x->data[1], 1.2);
//   EXPECT_FLOAT_EQ(x->data[3], 2.3);
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

// //Double
// TEST(TestAdapterWriteNumerical, writeDoubleEmpty) {
//   DoubleBuilder builder;
//   std::shared_ptr<Array> array;
//   (void)(builder.Finish(&array));
//   MemoryOutputStream mem_stream(DEFAULT_MEM_STREAM_SIZE);
//   ORC_UNIQUE_PTR<liborc::Type> schema(liborc::Type::buildTypeFromString("struct<x:double>"));
//   liborc::WriterOptions options;
//   ORC_UNIQUE_PTR<liborc::Writer> writer =
//     createWriter(*schema, &mem_stream, options);
//   uint64_t batchSize = 1024;
//   ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch =
//     writer->createRowBatch(batchSize);
//   liborc::StructVectorBatch *root =
//     internal::checked_cast<liborc::StructVectorBatch *>(batch.get());
//   liborc::DoubleVectorBatch *x =
//     internal::checked_cast<liborc::DoubleVectorBatch *>(root->fields[0]);
//   DataType* arrowType = float64().get();
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
// TEST(TestAdapterWriteNumerical, writeDoubleNoNulls) {
//   DoubleBuilder builder;
//   (void)(builder.Append(1.5));
//   (void)(builder.Append(2.6));
//   (void)(builder.Append(3.7));
//   (void)(builder.Append(4.8));
//   std::shared_ptr<Array> array;
//   (void)(builder.Finish(&array));
//   MemoryOutputStream mem_stream(DEFAULT_MEM_STREAM_SIZE);
//   ORC_UNIQUE_PTR<liborc::Type> schema(liborc::Type::buildTypeFromString("struct<x:double>"));
//   liborc::WriterOptions options;
//   ORC_UNIQUE_PTR<liborc::Writer> writer =
//     createWriter(*schema, &mem_stream, options);
//   uint64_t batchSize = 1024;
//   ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch =
//     writer->createRowBatch(batchSize);
//   liborc::StructVectorBatch *root =
//     internal::checked_cast<liborc::StructVectorBatch *>(batch.get());
//   liborc::DoubleVectorBatch *x =
//     internal::checked_cast<liborc::DoubleVectorBatch *>(root->fields[0]);
//   DataType* arrowType = float64().get();
//   int64_t arrowOffset = 0;
//   int64_t orcOffset = 0;
//   Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize, array.get());
//   if (!st.ok()) {
//     FAIL() << "ORC ColumnBatch not successfully filled";
//   }
//   EXPECT_EQ(x->numElements, 4);
//   EXPECT_FALSE(x->hasNulls);
//   EXPECT_DOUBLE_EQ(x->data[0], 1.5);
//   EXPECT_DOUBLE_EQ(x->data[1], 2.6);
//   EXPECT_DOUBLE_EQ(x->data[2], 3.7);
//   EXPECT_DOUBLE_EQ(x->data[3], 4.8);
//   EXPECT_EQ(arrowOffset, 4);
//   EXPECT_EQ(orcOffset, 4);
//   writer->add(*batch);
//   writer->close();
// }
// TEST(TestAdapterWriteNumerical, writeDoubleAllNulls) {
//   DoubleBuilder builder;
//   (void)(builder.AppendNull());
//   (void)(builder.AppendNull());
//   (void)(builder.AppendNull());
//   std::shared_ptr<Array> array;
//   (void)(builder.Finish(&array));
//   MemoryOutputStream mem_stream(DEFAULT_MEM_STREAM_SIZE);
//   ORC_UNIQUE_PTR<liborc::Type> schema(liborc::Type::buildTypeFromString("struct<x:double>"));
//   liborc::WriterOptions options;
//   ORC_UNIQUE_PTR<liborc::Writer> writer =
//     createWriter(*schema, &mem_stream, options);
//   uint64_t batchSize = 1024;
//   ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch =
//     writer->createRowBatch(batchSize);
//   liborc::StructVectorBatch *root =
//     internal::checked_cast<liborc::StructVectorBatch *>(batch.get());
//   liborc::DoubleVectorBatch *x =
//     internal::checked_cast<liborc::DoubleVectorBatch *>(root->fields[0]);
//   DataType* arrowType = float64().get();
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
// TEST(TestAdapterWriteNumerical, writeDoubleMixed) {
//   DoubleBuilder builder;
//   (void)(builder.AppendNull());
//   (void)(builder.Append(1.2));
//   (void)(builder.AppendNull());
//   (void)(builder.Append(2.3));
//   (void)(builder.AppendNull());
//   std::shared_ptr<Array> array;
//   (void)(builder.Finish(&array));
//   MemoryOutputStream mem_stream(DEFAULT_MEM_STREAM_SIZE);
//   ORC_UNIQUE_PTR<liborc::Type> schema(liborc::Type::buildTypeFromString("struct<x:double>"));
//   liborc::WriterOptions options;
//   ORC_UNIQUE_PTR<liborc::Writer> writer =
//     createWriter(*schema, &mem_stream, options);
//   uint64_t batchSize = 1024;
//   ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch =
//     writer->createRowBatch(batchSize);
//   liborc::StructVectorBatch *root =
//     internal::checked_cast<liborc::StructVectorBatch *>(batch.get());
//   liborc::DoubleVectorBatch *x =
//     internal::checked_cast<liborc::DoubleVectorBatch *>(root->fields[0]);
//   DataType* arrowType = float64().get();
//   int64_t arrowOffset = 0;
//   int64_t orcOffset = 0;
//   Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize, array.get());
//   if (!st.ok()) {
//     FAIL() << "ORC ColumnBatch not successfully filled";
//   }
//   EXPECT_EQ(x->numElements, 5);
//   EXPECT_TRUE(x->hasNulls);
//   EXPECT_DOUBLE_EQ(x->data[1], 1.2);
//   EXPECT_DOUBLE_EQ(x->data[3], 2.3);
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

// //Decimal
// // TEST(TestAdapterWriteNumerical, writeDecimalEmpty) {
// //   Decimal128Builder builder(decimal(38, 6));
// //   std::shared_ptr<Array> array;
// //   (void)(builder.Finish(&array));
// //   MemoryOutputStream mem_stream(DEFAULT_MEM_STREAM_SIZE);
// //   ORC_UNIQUE_PTR<liborc::Type> schema(liborc::Type::buildTypeFromString("struct<x:decimal>"));
// //   liborc::WriterOptions options;
// //   ORC_UNIQUE_PTR<liborc::Writer> writer =
// //     createWriter(*schema, &mem_stream, options);
// //   uint64_t batchSize = 1024;
// //   ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch =
// //     writer->createRowBatch(batchSize);
// //   liborc::StructVectorBatch *root =
// //     internal::checked_cast<liborc::StructVectorBatch *>(batch.get());
// //   liborc::Decimal128VectorBatch *x =
// //     internal::checked_cast<liborc::Decimal128VectorBatch *>(root->fields[0]);
// //   DataType* arrowType = decimal(38, 6).get();
// //   int64_t arrowOffset = 0;
// //   int64_t orcOffset = 0;
// //   Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize, array.get());
// //   if (!st.ok()) {
// //     FAIL() << "ORC ColumnBatch not successfully filled";
// //   }
// //   EXPECT_EQ(x->numElements, 0);
// //   EXPECT_FALSE(x->hasNulls);
// //   EXPECT_EQ(arrowOffset, 0);
// //   EXPECT_EQ(orcOffset, 0);
// //   writer->add(*batch);
// //   writer->close();
// // }
// // TEST(TestAdapterWriteNumerical, writeDecimalNoNulls) {
// //   Decimal128Builder builder(decimal(38, 6));
// //   (void)(builder.Append(new Decimal128("1.5")));
// //   (void)(builder.Append(new Decimal128("2.6")));
// //   (void)(builder.Append(new Decimal128("3.7")));
// //   (void)(builder.Append(new Decimal128("4.8")));
// //   std::shared_ptr<Array> array;
// //   (void)(builder.Finish(&array));
// //   MemoryOutputStream mem_stream(DEFAULT_MEM_STREAM_SIZE);
// //   ORC_UNIQUE_PTR<liborc::Type> schema(liborc::Type::buildTypeFromString("struct<x:decimal>"));
// //   liborc::WriterOptions options;
// //   ORC_UNIQUE_PTR<liborc::Writer> writer =
// //     createWriter(*schema, &mem_stream, options);
// //   uint64_t batchSize = 1024;
// //   ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch =
// //     writer->createRowBatch(batchSize);
// //   liborc::StructVectorBatch *root =
// //     internal::checked_cast<liborc::StructVectorBatch *>(batch.get());
// //   liborc::Decimal128VectorBatch *x =
// //     internal::checked_cast<liborc::Decimal128VectorBatch *>(root->fields[0]);
// //   DataType* arrowType = decimal(38, 6).get();
// //   int64_t arrowOffset = 0;
// //   int64_t orcOffset = 0;
// //   Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize, array.get());
// //   if (!st.ok()) {
// //     FAIL() << "ORC ColumnBatch not successfully filled";
// //   }
// //   EXPECT_EQ(x->numElements, 4);
// //   EXPECT_FALSE(x->hasNulls);
// //   EXPECT_EQ(new Decimal128(x->values[0].toDecimalString(6)), new Decimal128("1.5")));
// //   EXPECT_EQ(new Decimal128(x->values[1].toDecimalString(6)), new Decimal128("2.6")));
// //   EXPECT_EQ(new Decimal128(x->values[2].toDecimalString(6)), new Decimal128("3.7")));
// //   EXPECT_EQ(new Decimal128(x->values[3].toDecimalString(6)), new Decimal128("4.8")));
// //   EXPECT_EQ(arrowOffset, 4);
// //   EXPECT_EQ(orcOffset, 4);
// //   writer->add(*batch);
// //   writer->close();
// // }
// // TEST(TestAdapterWriteNumerical, writeDecimalAllNulls) {
// //   Decimal32Builder builder(decimal(38, 6));
// //   (void)(builder.AppendNull());
// //   (void)(builder.AppendNull());
// //   (void)(builder.AppendNull());
// //   std::shared_ptr<Array> array;
// //   (void)(builder.Finish(&array));
// //   MemoryOutputStream mem_stream(DEFAULT_MEM_STREAM_SIZE);
// //   ORC_UNIQUE_PTR<liborc::Type> schema(liborc::Type::buildTypeFromString("struct<x:decimal>"));
// //   liborc::WriterOptions options;
// //   ORC_UNIQUE_PTR<liborc::Writer> writer =
// //     createWriter(*schema, &mem_stream, options);
// //   uint64_t batchSize = 1024;
// //   ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch =
// //     writer->createRowBatch(batchSize);
// //   liborc::StructVectorBatch *root =
// //     internal::checked_cast<liborc::StructVectorBatch *>(batch.get());
// //   liborc::Decimal128VectorBatch *x =
// //     internal::checked_cast<liborc::Decimal128VectorBatch *>(root->fields[0]);
// //   DataType* arrowType = decimal(38, 6).get();
// //   int64_t arrowOffset = 0;
// //   int64_t orcOffset = 0;
// //   Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize, array.get());
// //   if (!st.ok()) {
// //     FAIL() << "ORC ColumnBatch not successfully filled";
// //   }
// //   EXPECT_EQ(x->numElements, 3);
// //   EXPECT_TRUE(x->hasNulls);
// //   EXPECT_EQ(x->notNull[0], 0);
// //   EXPECT_EQ(x->notNull[1], 0);
// //   EXPECT_EQ(x->notNull[2], 0);
// //   EXPECT_EQ(arrowOffset, 3);
// //   EXPECT_EQ(orcOffset, 3);
// //   writer->add(*batch);
// //   writer->close();
// // }
// // TEST(TestAdapterWriteNumerical, writeDecimalMixed) {
// //   Decimal128Builder builder(decimal(38, 6));
// //   (void)(builder.AppendNull());
// //   (void)(builder.Append(new Decimal128("1.2")));
// //   (void)(builder.AppendNull());
// //   (void)(builder.Append(new Decimal128("2.3")));
// //   (void)(builder.AppendNull());
// //   std::shared_ptr<Array> array;
// //   (void)(builder.Finish(&array));
// //   MemoryOutputStream mem_stream(DEFAULT_MEM_STREAM_SIZE);
// //   ORC_UNIQUE_PTR<liborc::Type> schema(liborc::Type::buildTypeFromString("struct<x:decimal>"));
// //   liborc::WriterOptions options;
// //   ORC_UNIQUE_PTR<liborc::Writer> writer =
// //     createWriter(*schema, &mem_stream, options);
// //   uint64_t batchSize = 1024;
// //   ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch =
// //     writer->createRowBatch(batchSize);
// //   liborc::StructVectorBatch *root =
// //     internal::checked_cast<liborc::StructVectorBatch *>(batch.get());
// //   liborc::Decimal128VectorBatch *x =
// //     internal::checked_cast<liborc::Decimal128VectorBatch *>(root->fields[0]);
// //   DataType* arrowType = float64().get();
// //   int64_t arrowOffset = 0;
// //   int64_t orcOffset = 0;
// //   Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize, array.get());
// //   if (!st.ok()) {
// //     FAIL() << "ORC ColumnBatch not successfully filled";
// //   }
// //   EXPECT_EQ(x->numElements, 5);
// //   EXPECT_TRUE(x->hasNulls);
// //   EXPECT_EQ(new Decimal128(x->values[1].toDecimalString(6)), new Decimal128("1.2")));
// //   EXPECT_EQ(new Decimal128(x->values[3].toDecimalString(6)), new Decimal128("2.3")));
// //   EXPECT_EQ(x->notNull[0], 0);
// //   EXPECT_EQ(x->notNull[1], 1);
// //   EXPECT_EQ(x->notNull[2], 0);
// //   EXPECT_EQ(x->notNull[3], 1);
// //   EXPECT_EQ(x->notNull[4], 0);
// //   EXPECT_EQ(arrowOffset, 5);
// //   EXPECT_EQ(orcOffset, 5);
// //   writer->add(*batch);
// //   writer->close();
// // }

// //Binary formats

// //String
// TEST(TestAdapterWriteBinary, writeStringEmpty) {
//   StringBuilder builder;
//   std::shared_ptr<Array> array;
//   (void)(builder.Finish(&array));
//   MemoryOutputStream mem_stream(DEFAULT_MEM_STREAM_SIZE);
//   ORC_UNIQUE_PTR<liborc::Type> schema(liborc::Type::buildTypeFromString("struct<x:string>"));
//   liborc::WriterOptions options;
//   ORC_UNIQUE_PTR<liborc::Writer> writer =
//     createWriter(*schema, &mem_stream, options);
//   uint64_t batchSize = 1024;
//   ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch =
//     writer->createRowBatch(batchSize);
//   liborc::StructVectorBatch *root =
//     internal::checked_cast<liborc::StructVectorBatch *>(batch.get());
//   liborc::StringVectorBatch *x =
//     internal::checked_cast<liborc::StringVectorBatch *>(root->fields[0]);
//   DataType* arrowType = utf8().get();
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
// TEST(TestAdapterWriteBinary, writeStringNoNulls) {
//   StringBuilder builder;
//   (void)(builder.Append("A"));
//   (void)(builder.Append("AB"));
//   (void)(builder.Append(""));
//   (void)(builder.Append("ABCD"));
//   std::shared_ptr<Array> array;
//   (void)(builder.Finish(&array));
//   MemoryOutputStream mem_stream(DEFAULT_MEM_STREAM_SIZE);
//   ORC_UNIQUE_PTR<liborc::Type> schema(liborc::Type::buildTypeFromString("struct<x:string>"));
//   liborc::WriterOptions options;
//   ORC_UNIQUE_PTR<liborc::Writer> writer =
//     createWriter(*schema, &mem_stream, options);
//   uint64_t batchSize = 1024;
//   ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch =
//     writer->createRowBatch(batchSize);
//   liborc::StructVectorBatch *root =
//     internal::checked_cast<liborc::StructVectorBatch *>(batch.get());
//   liborc::StringVectorBatch *x =
//     internal::checked_cast<liborc::StringVectorBatch *>(root->fields[0]);
//   DataType* arrowType = utf8().get();
//   int64_t arrowOffset = 0;
//   int64_t orcOffset = 0;
//   Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize, array.get());
//   if (!st.ok()) {
//     FAIL() << "ORC ColumnBatch not successfully filled";
//   }
//   EXPECT_EQ(x->numElements, 4);
//   EXPECT_FALSE(x->hasNulls);
//   EXPECT_STREQ(x->data[0], "A");
//   EXPECT_STREQ(x->data[1], "AB");
//   EXPECT_STREQ(x->data[2], "");
//   EXPECT_STREQ(x->data[3], "ABCD");
//   EXPECT_EQ(x->length[0], 1);
//   EXPECT_EQ(x->length[1], 2);
//   EXPECT_EQ(x->length[2], 0);
//   EXPECT_EQ(x->length[3], 4);
//   EXPECT_EQ(arrowOffset, 4);
//   EXPECT_EQ(orcOffset, 4);
//   writer->add(*batch);
//   writer->close();
// }
// TEST(TestAdapterWriteBinary, writeStringAllNulls) {
//   StringBuilder builder;
//   (void)(builder.AppendNull());
//   (void)(builder.AppendNull());
//   (void)(builder.AppendNull());
//   std::shared_ptr<Array> array;
//   (void)(builder.Finish(&array));
//   MemoryOutputStream mem_stream(DEFAULT_MEM_STREAM_SIZE);
//   ORC_UNIQUE_PTR<liborc::Type> schema(liborc::Type::buildTypeFromString("struct<x:string>"));
//   liborc::WriterOptions options;
//   ORC_UNIQUE_PTR<liborc::Writer> writer =
//     createWriter(*schema, &mem_stream, options);
//   uint64_t batchSize = 1024;
//   ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch =
//     writer->createRowBatch(batchSize);
//   liborc::StructVectorBatch *root =
//     internal::checked_cast<liborc::StructVectorBatch *>(batch.get());
//   liborc::StringVectorBatch *x =
//     internal::checked_cast<liborc::StringVectorBatch *>(root->fields[0]);
//   DataType* arrowType = utf8().get();
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
// TEST(TestAdapterWriteBinary, writeStringMixed) {
//   StringBuilder builder;
//   (void)(builder.AppendNull());
//   (void)(builder.Append(""));
//   (void)(builder.AppendNull());
//   (void)(builder.Append("AB"));
//   (void)(builder.AppendNull());
//   std::shared_ptr<Array> array;
//   (void)(builder.Finish(&array));
//   MemoryOutputStream mem_stream(DEFAULT_MEM_STREAM_SIZE);
//   ORC_UNIQUE_PTR<liborc::Type> schema(liborc::Type::buildTypeFromString("struct<x:string>"));
//   liborc::WriterOptions options;
//   ORC_UNIQUE_PTR<liborc::Writer> writer =
//     createWriter(*schema, &mem_stream, options);
//   uint64_t batchSize = 1024;
//   ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch =
//     writer->createRowBatch(batchSize);
//   liborc::StructVectorBatch *root =
//     internal::checked_cast<liborc::StructVectorBatch *>(batch.get());
//   liborc::StringVectorBatch *x =
//     internal::checked_cast<liborc::StringVectorBatch *>(root->fields[0]);
//   DataType* arrowType = utf8().get();
//   int64_t arrowOffset = 0;
//   int64_t orcOffset = 0;
//   Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize, array.get());
//   if (!st.ok()) {
//     FAIL() << "ORC ColumnBatch not successfully filled";
//   }
//   EXPECT_EQ(x->numElements, 5);
//   EXPECT_TRUE(x->hasNulls);
//   EXPECT_STREQ(x->data[1], "");
//   EXPECT_STREQ(x->data[3], "AB");
//   EXPECT_EQ(x->length[1], 0);
//   EXPECT_EQ(x->length[3], 2);
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

// //LargeString
// TEST(TestAdapterWriteBinary, writeLargeStringEmpty) {
//   LargeStringBuilder builder;
//   std::shared_ptr<Array> array;
//   (void)(builder.Finish(&array));
//   MemoryOutputStream mem_stream(DEFAULT_MEM_STREAM_SIZE);
//   ORC_UNIQUE_PTR<liborc::Type> schema(liborc::Type::buildTypeFromString("struct<x:string>"));
//   liborc::WriterOptions options;
//   ORC_UNIQUE_PTR<liborc::Writer> writer =
//     createWriter(*schema, &mem_stream, options);
//   uint64_t batchSize = 1024;
//   ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch =
//     writer->createRowBatch(batchSize);
//   liborc::StructVectorBatch *root =
//     internal::checked_cast<liborc::StructVectorBatch *>(batch.get());
//   liborc::StringVectorBatch *x =
//     internal::checked_cast<liborc::StringVectorBatch *>(root->fields[0]);
//   DataType* arrowType = large_utf8().get();
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
// TEST(TestAdapterWriteBinary, writeLargeStringNoNulls) {
//   LargeStringBuilder builder;
//   (void)(builder.Append("A"));
//   (void)(builder.Append("AB"));
//   (void)(builder.Append(""));
//   (void)(builder.Append("ABCD"));
//   std::shared_ptr<Array> array;
//   (void)(builder.Finish(&array));
//   MemoryOutputStream mem_stream(DEFAULT_MEM_STREAM_SIZE);
//   ORC_UNIQUE_PTR<liborc::Type> schema(liborc::Type::buildTypeFromString("struct<x:string>"));
//   liborc::WriterOptions options;
//   ORC_UNIQUE_PTR<liborc::Writer> writer =
//     createWriter(*schema, &mem_stream, options);
//   uint64_t batchSize = 1024;
//   ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch =
//     writer->createRowBatch(batchSize);
//   liborc::StructVectorBatch *root =
//     internal::checked_cast<liborc::StructVectorBatch *>(batch.get());
//   liborc::StringVectorBatch *x =
//     internal::checked_cast<liborc::StringVectorBatch *>(root->fields[0]);
//   DataType* arrowType = large_utf8().get();
//   int64_t arrowOffset = 0;
//   int64_t orcOffset = 0;
//   Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize, array.get());
//   if (!st.ok()) {
//     FAIL() << "ORC ColumnBatch not successfully filled";
//   }
//   EXPECT_EQ(x->numElements, 4);
//   EXPECT_FALSE(x->hasNulls);
//   EXPECT_STREQ(x->data[0], "A");
//   EXPECT_STREQ(x->data[1], "AB");
//   EXPECT_STREQ(x->data[2], "");
//   EXPECT_STREQ(x->data[3], "ABCD");
//   EXPECT_EQ(x->length[0], 1);
//   EXPECT_EQ(x->length[1], 2);
//   EXPECT_EQ(x->length[2], 0);
//   EXPECT_EQ(x->length[3], 4);
//   EXPECT_EQ(arrowOffset, 4);
//   EXPECT_EQ(orcOffset, 4);
//   writer->add(*batch);
//   writer->close();
// }
// TEST(TestAdapterWriteBinary, writeLargeStringAllNulls) {
//   LargeStringBuilder builder;
//   (void)(builder.AppendNull());
//   (void)(builder.AppendNull());
//   (void)(builder.AppendNull());
//   std::shared_ptr<Array> array;
//   (void)(builder.Finish(&array));
//   MemoryOutputStream mem_stream(DEFAULT_MEM_STREAM_SIZE);
//   ORC_UNIQUE_PTR<liborc::Type> schema(liborc::Type::buildTypeFromString("struct<x:string>"));
//   liborc::WriterOptions options;
//   ORC_UNIQUE_PTR<liborc::Writer> writer =
//     createWriter(*schema, &mem_stream, options);
//   uint64_t batchSize = 1024;
//   ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch =
//     writer->createRowBatch(batchSize);
//   liborc::StructVectorBatch *root =
//     internal::checked_cast<liborc::StructVectorBatch *>(batch.get());
//   liborc::StringVectorBatch *x =
//     internal::checked_cast<liborc::StringVectorBatch *>(root->fields[0]);
//   DataType* arrowType = large_utf8().get();
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
// TEST(TestAdapterWriteBinary, writeLargeStringMixed) {
//   LargeStringBuilder builder;
//   (void)(builder.AppendNull());
//   (void)(builder.Append(""));
//   (void)(builder.AppendNull());
//   (void)(builder.Append("AB"));
//   (void)(builder.AppendNull());
//   std::shared_ptr<Array> array;
//   (void)(builder.Finish(&array));
//   MemoryOutputStream mem_stream(DEFAULT_MEM_STREAM_SIZE);
//   ORC_UNIQUE_PTR<liborc::Type> schema(liborc::Type::buildTypeFromString("struct<x:string>"));
//   liborc::WriterOptions options;
//   ORC_UNIQUE_PTR<liborc::Writer> writer =
//     createWriter(*schema, &mem_stream, options);
//   uint64_t batchSize = 1024;
//   ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch =
//     writer->createRowBatch(batchSize);
//   liborc::StructVectorBatch *root =
//     internal::checked_cast<liborc::StructVectorBatch *>(batch.get());
//   liborc::StringVectorBatch *x =
//     internal::checked_cast<liborc::StringVectorBatch *>(root->fields[0]);
//   DataType* arrowType = large_utf8().get();
//   int64_t arrowOffset = 0;
//   int64_t orcOffset = 0;
//   Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize, array.get());
//   if (!st.ok()) {
//     FAIL() << "ORC ColumnBatch not successfully filled";
//   }
//   EXPECT_EQ(x->numElements, 5);
//   EXPECT_TRUE(x->hasNulls);
//   EXPECT_STREQ(x->data[1], "");
//   EXPECT_STREQ(x->data[3], "AB");
//   EXPECT_EQ(x->length[1], 0);
//   EXPECT_EQ(x->length[3], 2);
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

// //Binary
// TEST(TestAdapterWriteBinary, writeBinaryEmpty) {
//   BinaryBuilder builder;
//   std::shared_ptr<Array> array;
//   (void)(builder.Finish(&array));
//   MemoryOutputStream mem_stream(DEFAULT_MEM_STREAM_SIZE);
//   ORC_UNIQUE_PTR<liborc::Type> schema(liborc::Type::buildTypeFromString("struct<x:binary>"));
//   liborc::WriterOptions options;
//   ORC_UNIQUE_PTR<liborc::Writer> writer =
//     createWriter(*schema, &mem_stream, options);
//   uint64_t batchSize = 1024;
//   ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch =
//     writer->createRowBatch(batchSize);
//   liborc::StructVectorBatch *root =
//     internal::checked_cast<liborc::StructVectorBatch *>(batch.get());
//   liborc::StringVectorBatch *x =
//     internal::checked_cast<liborc::StringVectorBatch *>(root->fields[0]);
//   DataType* arrowType = binary().get();
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
// TEST(TestAdapterWriteBinary, writeBinaryNoNulls) {
//   BinaryBuilder builder;
//   char a[] = "\xf4", b[] = "\x22\x0e",c[] = "",d[] = "\xff\x66\xbf\x5b";
//   (void)(builder.Append(a));
//   (void)(builder.Append(b));
//   (void)(builder.Append(c));
//   (void)(builder.Append(d));
//   std::shared_ptr<Array> array;
//   (void)(builder.Finish(&array));
//   MemoryOutputStream mem_stream(DEFAULT_MEM_STREAM_SIZE);
//   ORC_UNIQUE_PTR<liborc::Type> schema(liborc::Type::buildTypeFromString("struct<x:binary>"));
//   liborc::WriterOptions options;
//   ORC_UNIQUE_PTR<liborc::Writer> writer =
//     createWriter(*schema, &mem_stream, options);
//   uint64_t batchSize = 1024;
//   ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch =
//     writer->createRowBatch(batchSize);
//   liborc::StructVectorBatch *root =
//     internal::checked_cast<liborc::StructVectorBatch *>(batch.get());
//   liborc::StringVectorBatch *x =
//     internal::checked_cast<liborc::StringVectorBatch *>(root->fields[0]);
//   DataType* arrowType = binary().get();
//   int64_t arrowOffset = 0;
//   int64_t orcOffset = 0;
//   Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize, array.get());
//   if (!st.ok()) {
//     FAIL() << "ORC ColumnBatch not successfully filled";
//   }
//   EXPECT_EQ(x->numElements, 4);
//   EXPECT_FALSE(x->hasNulls);
//   EXPECT_STREQ(x->data[0], a);
//   EXPECT_STREQ(x->data[1], b);
//   EXPECT_STREQ(x->data[2], c);
//   EXPECT_STREQ(x->data[3], d);
//   EXPECT_EQ(x->length[0], 1);
//   EXPECT_EQ(x->length[1], 2);
//   EXPECT_EQ(x->length[2], 0);
//   EXPECT_EQ(x->length[3], 4);
//   EXPECT_EQ(arrowOffset, 4);
//   EXPECT_EQ(orcOffset, 4);
//   writer->add(*batch);
//   writer->close();
// }
// TEST(TestAdapterWriteBinary, writeBinaryAllNulls) {
//   BinaryBuilder builder;
//   (void)(builder.AppendNull());
//   (void)(builder.AppendNull());
//   (void)(builder.AppendNull());
//   std::shared_ptr<Array> array;
//   (void)(builder.Finish(&array));
//   MemoryOutputStream mem_stream(DEFAULT_MEM_STREAM_SIZE);
//   ORC_UNIQUE_PTR<liborc::Type> schema(liborc::Type::buildTypeFromString("struct<x:binary>"));
//   liborc::WriterOptions options;
//   ORC_UNIQUE_PTR<liborc::Writer> writer =
//     createWriter(*schema, &mem_stream, options);
//   uint64_t batchSize = 1024;
//   ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch =
//     writer->createRowBatch(batchSize);
//   liborc::StructVectorBatch *root =
//     internal::checked_cast<liborc::StructVectorBatch *>(batch.get());
//   liborc::StringVectorBatch *x =
//     internal::checked_cast<liborc::StringVectorBatch *>(root->fields[0]);
//   DataType* arrowType = binary().get();
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
// TEST(TestAdapterWriteBinary, writeBinaryMixed) {
//   BinaryBuilder builder;
//   char a[] = "", b[] = "\xff\xfe";
//   (void)(builder.AppendNull());
//   (void)(builder.Append(a));
//   (void)(builder.AppendNull());
//   (void)(builder.Append(b));
//   (void)(builder.AppendNull());
//   std::shared_ptr<Array> array;
//   (void)(builder.Finish(&array));
//   MemoryOutputStream mem_stream(DEFAULT_MEM_STREAM_SIZE);
//   ORC_UNIQUE_PTR<liborc::Type> schema(liborc::Type::buildTypeFromString("struct<x:binary>"));
//   liborc::WriterOptions options;
//   ORC_UNIQUE_PTR<liborc::Writer> writer =
//     createWriter(*schema, &mem_stream, options);
//   uint64_t batchSize = 1024;
//   ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch =
//     writer->createRowBatch(batchSize);
//   liborc::StructVectorBatch *root =
//     internal::checked_cast<liborc::StructVectorBatch *>(batch.get());
//   liborc::StringVectorBatch *x =
//     internal::checked_cast<liborc::StringVectorBatch *>(root->fields[0]);
//   DataType* arrowType = binary().get();
//   int64_t arrowOffset = 0;
//   int64_t orcOffset = 0;
//   Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize, array.get());
//   if (!st.ok()) {
//     FAIL() << "ORC ColumnBatch not successfully filled";
//   }
//   EXPECT_EQ(x->numElements, 5);
//   EXPECT_TRUE(x->hasNulls);
//   EXPECT_STREQ(x->data[1], a);
//   EXPECT_STREQ(x->data[3], b);
//   EXPECT_EQ(x->length[1], 0);
//   EXPECT_EQ(x->length[3], 2);
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

// //LargeBinary
// TEST(TestAdapterWriteBinary, writeLargeBinaryEmpty) {
//   LargeBinaryBuilder builder;
//   std::shared_ptr<Array> array;
//   (void)(builder.Finish(&array));
//   MemoryOutputStream mem_stream(DEFAULT_MEM_STREAM_SIZE);
//   ORC_UNIQUE_PTR<liborc::Type> schema(liborc::Type::buildTypeFromString("struct<x:binary>"));
//   liborc::WriterOptions options;
//   ORC_UNIQUE_PTR<liborc::Writer> writer =
//     createWriter(*schema, &mem_stream, options);
//   uint64_t batchSize = 1024;
//   ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch =
//     writer->createRowBatch(batchSize);
//   liborc::StructVectorBatch *root =
//     internal::checked_cast<liborc::StructVectorBatch *>(batch.get());
//   liborc::StringVectorBatch *x =
//     internal::checked_cast<liborc::StringVectorBatch *>(root->fields[0]);
//   DataType* arrowType = large_binary().get();
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
// TEST(TestAdapterWriteBinary, writeLargeBinaryNoNulls) {
//   LargeBinaryBuilder builder;
//   char a[] = "\xf4", b[] = "\x22\x0e",c[] = "",d[] = "\xff\x66\xbf\x5b";
//   (void)(builder.Append(a));
//   (void)(builder.Append(b));
//   (void)(builder.Append(c));
//   (void)(builder.Append(d));
//   std::shared_ptr<Array> array;
//   (void)(builder.Finish(&array));
//   MemoryOutputStream mem_stream(DEFAULT_MEM_STREAM_SIZE);
//   ORC_UNIQUE_PTR<liborc::Type> schema(liborc::Type::buildTypeFromString("struct<x:binary>"));
//   liborc::WriterOptions options;
//   ORC_UNIQUE_PTR<liborc::Writer> writer =
//     createWriter(*schema, &mem_stream, options);
//   uint64_t batchSize = 1024;
//   ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch =
//     writer->createRowBatch(batchSize);
//   liborc::StructVectorBatch *root =
//     internal::checked_cast<liborc::StructVectorBatch *>(batch.get());
//   liborc::StringVectorBatch *x =
//     internal::checked_cast<liborc::StringVectorBatch *>(root->fields[0]);
//   DataType* arrowType = large_binary().get();
//   int64_t arrowOffset = 0;
//   int64_t orcOffset = 0;
//   Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize, array.get());
//   if (!st.ok()) {
//     FAIL() << "ORC ColumnBatch not successfully filled";
//   }
//   EXPECT_EQ(x->numElements, 4);
//   EXPECT_FALSE(x->hasNulls);
//   EXPECT_STREQ(x->data[0], a);
//   EXPECT_STREQ(x->data[1], b);
//   EXPECT_STREQ(x->data[2], c);
//   EXPECT_STREQ(x->data[3], d);
//   EXPECT_EQ(x->length[0], 1);
//   EXPECT_EQ(x->length[1], 2);
//   EXPECT_EQ(x->length[2], 0);
//   EXPECT_EQ(x->length[3], 4);
//   EXPECT_EQ(arrowOffset, 4);
//   EXPECT_EQ(orcOffset, 4);
//   writer->add(*batch);
//   writer->close();
// }
// TEST(TestAdapterWriteBinary, writeLargeBinaryAllNulls) {
//   LargeBinaryBuilder builder;
//   (void)(builder.AppendNull());
//   (void)(builder.AppendNull());
//   (void)(builder.AppendNull());
//   std::shared_ptr<Array> array;
//   (void)(builder.Finish(&array));
//   MemoryOutputStream mem_stream(DEFAULT_MEM_STREAM_SIZE);
//   ORC_UNIQUE_PTR<liborc::Type> schema(liborc::Type::buildTypeFromString("struct<x:binary>"));
//   liborc::WriterOptions options;
//   ORC_UNIQUE_PTR<liborc::Writer> writer =
//     createWriter(*schema, &mem_stream, options);
//   uint64_t batchSize = 1024;
//   ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch =
//     writer->createRowBatch(batchSize);
//   liborc::StructVectorBatch *root =
//     internal::checked_cast<liborc::StructVectorBatch *>(batch.get());
//   liborc::StringVectorBatch *x =
//     internal::checked_cast<liborc::StringVectorBatch *>(root->fields[0]);
//   DataType* arrowType = large_binary().get();
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
// TEST(TestAdapterWriteBinary, writeLargeBinaryMixed) {
//   LargeBinaryBuilder builder;
//   char a[] = "", b[] = "\xff\xfe";
//   (void)(builder.AppendNull());
//   (void)(builder.Append(a));
//   (void)(builder.AppendNull());
//   (void)(builder.Append(b));
//   (void)(builder.AppendNull());
//   std::shared_ptr<Array> array;
//   (void)(builder.Finish(&array));
//   MemoryOutputStream mem_stream(DEFAULT_MEM_STREAM_SIZE);
//   ORC_UNIQUE_PTR<liborc::Type> schema(liborc::Type::buildTypeFromString("struct<x:binary>"));
//   liborc::WriterOptions options;
//   ORC_UNIQUE_PTR<liborc::Writer> writer =
//     createWriter(*schema, &mem_stream, options);
//   uint64_t batchSize = 1024;
//   ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch =
//     writer->createRowBatch(batchSize);
//   liborc::StructVectorBatch *root =
//     internal::checked_cast<liborc::StructVectorBatch *>(batch.get());
//   liborc::StringVectorBatch *x =
//     internal::checked_cast<liborc::StringVectorBatch *>(root->fields[0]);
//   DataType* arrowType = large_binary().get();
//   int64_t arrowOffset = 0;
//   int64_t orcOffset = 0;
//   Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize, array.get());
//   if (!st.ok()) {
//     FAIL() << "ORC ColumnBatch not successfully filled";
//   }
//   EXPECT_EQ(x->numElements, 5);
//   EXPECT_TRUE(x->hasNulls);
//   EXPECT_STREQ(x->data[1], a);
//   EXPECT_STREQ(x->data[3], b);
//   EXPECT_EQ(x->length[1], 0);
//   EXPECT_EQ(x->length[3], 2);
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

// //FixedSizeBinary
// TEST(TestAdapterWriteBinary, writeFixedSizeBinaryEmpty) {
//   FixedSizeBinaryBuilder builder(fixed_size_binary(4));
//   std::shared_ptr<Array> array;
//   (void)(builder.Finish(&array));
//   MemoryOutputStream mem_stream(DEFAULT_MEM_STREAM_SIZE);
//   ORC_UNIQUE_PTR<liborc::Type> schema(liborc::Type::buildTypeFromString("struct<x:binary>"));
//   liborc::WriterOptions options;
//   ORC_UNIQUE_PTR<liborc::Writer> writer =
//     createWriter(*schema, &mem_stream, options);
//   uint64_t batchSize = 1024;
//   ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch =
//     writer->createRowBatch(batchSize);
//   liborc::StructVectorBatch *root =
//     internal::checked_cast<liborc::StructVectorBatch *>(batch.get());
//   liborc::StringVectorBatch *x =
//     internal::checked_cast<liborc::StringVectorBatch *>(root->fields[0]);
//   DataType* arrowType = fixed_size_binary(4).get();
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
// TEST(TestAdapterWriteBinary, writeFixedSizeBinaryZeroEmpty) {
//   FixedSizeBinaryBuilder builder(fixed_size_binary(0));
//   std::shared_ptr<Array> array;
//   (void)(builder.Finish(&array));
//   MemoryOutputStream mem_stream(DEFAULT_MEM_STREAM_SIZE);
//   ORC_UNIQUE_PTR<liborc::Type> schema(liborc::Type::buildTypeFromString("struct<x:binary>"));
//   liborc::WriterOptions options;
//   ORC_UNIQUE_PTR<liborc::Writer> writer =
//     createWriter(*schema, &mem_stream, options);
//   uint64_t batchSize = 1024;
//   ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch =
//     writer->createRowBatch(batchSize);
//   liborc::StructVectorBatch *root =
//     internal::checked_cast<liborc::StructVectorBatch *>(batch.get());
//   liborc::StringVectorBatch *x =
//     internal::checked_cast<liborc::StringVectorBatch *>(root->fields[0]);
//   DataType* arrowType = fixed_size_binary(0).get();
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
// TEST(TestAdapterWriteBinary, writeFixedSizeBinaryZeroNoNulls) {
//   FixedSizeBinaryBuilder builder(fixed_size_binary(0));
//   std::shared_ptr<Array> array;
//   char a[] = "", b[] = "";
//   (void)(builder.Append(a));
//   (void)(builder.Append(b));
//   (void)(builder.Finish(&array));
//   MemoryOutputStream mem_stream(DEFAULT_MEM_STREAM_SIZE);
//   ORC_UNIQUE_PTR<liborc::Type> schema(liborc::Type::buildTypeFromString("struct<x:binary>"));
//   liborc::WriterOptions options;
//   ORC_UNIQUE_PTR<liborc::Writer> writer =
//     createWriter(*schema, &mem_stream, options);
//   uint64_t batchSize = 1024;
//   ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch =
//     writer->createRowBatch(batchSize);
//   liborc::StructVectorBatch *root =
//     internal::checked_cast<liborc::StructVectorBatch *>(batch.get());
//   liborc::StringVectorBatch *x =
//     internal::checked_cast<liborc::StringVectorBatch *>(root->fields[0]);
//   DataType* arrowType = fixed_size_binary(0).get();
//   int64_t arrowOffset = 0;
//   int64_t orcOffset = 0;
//   Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize, array.get());
//   if (!st.ok()) {
//     FAIL() << "ORC ColumnBatch not successfully filled";
//   }
//   EXPECT_EQ(x->numElements, 2);
//   EXPECT_FALSE(x->hasNulls);
//   EXPECT_EQ(x->length[0], 0);
//   EXPECT_EQ(x->length[1], 0);
//   EXPECT_EQ(arrowOffset, 2);
//   EXPECT_EQ(orcOffset, 2);
//   writer->add(*batch);
//   writer->close();
// }
// TEST(TestAdapterWriteBinary, writeFixedSizeBinaryZeroMixed) {
//   FixedSizeBinaryBuilder builder(fixed_size_binary(0));
//   std::shared_ptr<Array> array;
//   char a[] = "", b[] = "";
//   (void)(builder.Append(a));
//   (void)(builder.Append(b));
//   (void)(builder.AppendNull());
//   (void)(builder.Finish(&array));
//   MemoryOutputStream mem_stream(DEFAULT_MEM_STREAM_SIZE);
//   ORC_UNIQUE_PTR<liborc::Type> schema(liborc::Type::buildTypeFromString("struct<x:binary>"));
//   liborc::WriterOptions options;
//   ORC_UNIQUE_PTR<liborc::Writer> writer =
//     createWriter(*schema, &mem_stream, options);
//   uint64_t batchSize = 1024;
//   ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch =
//     writer->createRowBatch(batchSize);
//   liborc::StructVectorBatch *root =
//     internal::checked_cast<liborc::StructVectorBatch *>(batch.get());
//   liborc::StringVectorBatch *x =
//     internal::checked_cast<liborc::StringVectorBatch *>(root->fields[0]);
//   DataType* arrowType = fixed_size_binary(0).get();
//   int64_t arrowOffset = 0;
//   int64_t orcOffset = 0;
//   Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize, array.get());
//   if (!st.ok()) {
//     FAIL() << "ORC ColumnBatch not successfully filled";
//   }
//   EXPECT_EQ(x->numElements, 3);
//   EXPECT_TRUE(x->hasNulls);
//   EXPECT_EQ(x->notNull[0], 1);
//   EXPECT_EQ(x->notNull[1], 1);
//   EXPECT_EQ(x->notNull[2], 0);

//   EXPECT_EQ(x->length[0], 0);
//   EXPECT_EQ(x->length[1], 0);
//   EXPECT_EQ(x->length[2], 0);

//   EXPECT_EQ(arrowOffset, 3);
//   EXPECT_EQ(orcOffset, 3);
//   writer->add(*batch);
//   writer->close();
// }
// TEST(TestAdapterWriteBinary, writeFixedSizeBinaryZeroAllNulls) {
//   FixedSizeBinaryBuilder builder(fixed_size_binary(0));
//   std::shared_ptr<Array> array;
//   (void)(builder.AppendNull());
//   (void)(builder.AppendNull());
//   (void)(builder.AppendNull());
//   (void)(builder.Finish(&array));
//   MemoryOutputStream mem_stream(DEFAULT_MEM_STREAM_SIZE);
//   ORC_UNIQUE_PTR<liborc::Type> schema(liborc::Type::buildTypeFromString("struct<x:binary>"));
//   liborc::WriterOptions options;
//   ORC_UNIQUE_PTR<liborc::Writer> writer =
//     createWriter(*schema, &mem_stream, options);
//   uint64_t batchSize = 1024;
//   ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch =
//     writer->createRowBatch(batchSize);
//   liborc::StructVectorBatch *root =
//     internal::checked_cast<liborc::StructVectorBatch *>(batch.get());
//   liborc::StringVectorBatch *x =
//     internal::checked_cast<liborc::StringVectorBatch *>(root->fields[0]);
//   DataType* arrowType = fixed_size_binary(0).get();
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

//   EXPECT_EQ(x->length[0], 0);
//   EXPECT_EQ(x->length[1], 0);
//   EXPECT_EQ(x->length[2], 0);

//   EXPECT_EQ(arrowOffset, 3);
//   EXPECT_EQ(orcOffset, 3);
//   writer->add(*batch);
//   writer->close();
// }
// TEST(TestAdapterWriteBinary, writeFixedSizeBinaryNoNulls) {
//   FixedSizeBinaryBuilder builder(fixed_size_binary(4));
//   char a[] = "\xf4\xd2\x21\x39", b[] = "\x22\x0e\x09\xaa",c[] = "\x34\x01\x43\x42",d[] = "\xff\x66\xbf\x5b";
//   (void)(builder.Append(a));
//   (void)(builder.Append(b));
//   (void)(builder.Append(c));
//   (void)(builder.Append(d));
//   std::shared_ptr<Array> array;
//   (void)(builder.Finish(&array));
//   MemoryOutputStream mem_stream(DEFAULT_MEM_STREAM_SIZE);
//   ORC_UNIQUE_PTR<liborc::Type> schema(liborc::Type::buildTypeFromString("struct<x:binary>"));
//   liborc::WriterOptions options;
//   ORC_UNIQUE_PTR<liborc::Writer> writer =
//     createWriter(*schema, &mem_stream, options);
//   uint64_t batchSize = 1024;
//   ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch =
//     writer->createRowBatch(batchSize);
//   liborc::StructVectorBatch *root =
//     internal::checked_cast<liborc::StructVectorBatch *>(batch.get());
//   liborc::StringVectorBatch *x =
//     internal::checked_cast<liborc::StringVectorBatch *>(root->fields[0]);
//   DataType* arrowType = fixed_size_binary(4).get();
//   int64_t arrowOffset = 0;
//   int64_t orcOffset = 0;
//   Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize, array.get());
//   if (!st.ok()) {
//     FAIL() << "ORC ColumnBatch not successfully filled";
//   }
//   EXPECT_EQ(x->numElements, 4);
//   EXPECT_FALSE(x->hasNulls);
//   EXPECT_STREQ(x->data[0], a);
//   EXPECT_STREQ(x->data[1], b);
//   EXPECT_STREQ(x->data[2], c);
//   EXPECT_STREQ(x->data[3], d);
//   EXPECT_EQ(x->length[0], 4);
//   EXPECT_EQ(x->length[1], 4);
//   EXPECT_EQ(x->length[2], 4);
//   EXPECT_EQ(x->length[3], 4);
//   EXPECT_EQ(arrowOffset, 4);
//   EXPECT_EQ(orcOffset, 4);
//   writer->add(*batch);
//   writer->close();
// }
// TEST(TestAdapterWriteBinary, writeFixedSizeBinaryAllNulls) {
//   FixedSizeBinaryBuilder builder(fixed_size_binary(1));
//   (void)(builder.AppendNull());
//   (void)(builder.AppendNull());
//   (void)(builder.AppendNull());
//   std::shared_ptr<Array> array;
//   (void)(builder.Finish(&array));
//   MemoryOutputStream mem_stream(DEFAULT_MEM_STREAM_SIZE);
//   ORC_UNIQUE_PTR<liborc::Type> schema(liborc::Type::buildTypeFromString("struct<x:binary>"));
//   liborc::WriterOptions options;
//   ORC_UNIQUE_PTR<liborc::Writer> writer =
//     createWriter(*schema, &mem_stream, options);
//   uint64_t batchSize = 1024;
//   ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch =
//     writer->createRowBatch(batchSize);
//   liborc::StructVectorBatch *root =
//     internal::checked_cast<liborc::StructVectorBatch *>(batch.get());
//   liborc::StringVectorBatch *x =
//     internal::checked_cast<liborc::StringVectorBatch *>(root->fields[0]);
//   DataType* arrowType = fixed_size_binary(1).get();
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
// TEST(TestAdapterWriteBinary, writeFixedSizeBinaryMixed) {
//   FixedSizeBinaryBuilder builder(fixed_size_binary(2));
//   char a[] = "\xa2\x34", b[] = "\xff\xfe";
//   (void)(builder.AppendNull());
//   (void)(builder.Append(a));
//   (void)(builder.AppendNull());
//   (void)(builder.Append(b));
//   (void)(builder.AppendNull());
//   std::shared_ptr<Array> array;
//   (void)(builder.Finish(&array));
//   MemoryOutputStream mem_stream(DEFAULT_MEM_STREAM_SIZE);
//   ORC_UNIQUE_PTR<liborc::Type> schema(liborc::Type::buildTypeFromString("struct<x:binary>"));
//   liborc::WriterOptions options;
//   ORC_UNIQUE_PTR<liborc::Writer> writer =
//     createWriter(*schema, &mem_stream, options);
//   uint64_t batchSize = 1024;
//   ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch =
//     writer->createRowBatch(batchSize);
//   liborc::StructVectorBatch *root =
//     internal::checked_cast<liborc::StructVectorBatch *>(batch.get());
//   liborc::StringVectorBatch *x =
//     internal::checked_cast<liborc::StringVectorBatch *>(root->fields[0]);
//   DataType* arrowType = fixed_size_binary(2).get();
//   int64_t arrowOffset = 0;
//   int64_t orcOffset = 0;
//   Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize, array.get());
//   if (!st.ok()) {
//     FAIL() << "ORC ColumnBatch not successfully filled";
//   }
//   EXPECT_EQ(x->numElements, 5);
//   EXPECT_TRUE(x->hasNulls);
//   EXPECT_STREQ(x->data[1], a);
//   EXPECT_STREQ(x->data[3], b);
//   EXPECT_EQ(x->length[1], 2);
//   EXPECT_EQ(x->length[3], 2);
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

// //Nested types
// //Struct
// TEST(TestAdapterWriteNested, writeStructEmpty) {
//   std::vector<std::shared_ptr<Field>> xFields;
//   xFields.push_back(std::make_shared<Field>("a", utf8()));
//   xFields.push_back(std::make_shared<Field>("b", int32()));
//   auto sharedPtrArrowType = struct_(xFields);
//   DataType* arrowType = sharedPtrArrowType.get();

//   StringBuilder builder1;
//   Int32Builder builder2;
//   std::shared_ptr<Array> array1;
//   (void)(builder1.Finish(&array1));
//   std::shared_ptr<Array> array2;
//   (void)(builder2.Finish(&array2));

//   std::vector<std::shared_ptr<Array>> children;
//   children.push_back(array1);
//   children.push_back(array2);

//   auto array = std::make_shared<StructArray>(sharedPtrArrowType, 0, children);


//   MemoryOutputStream mem_stream(DEFAULT_MEM_STREAM_SIZE);
//   ORC_UNIQUE_PTR<liborc::Type> schema(liborc::Type::buildTypeFromString("struct<x:struct<a:string,b:int>>"));
//   liborc::WriterOptions options;
//   ORC_UNIQUE_PTR<liborc::Writer> writer =
//     createWriter(*schema, &mem_stream, options);
//   uint64_t batchSize = 1024;
//   ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch =
//     writer->createRowBatch(batchSize);
//   liborc::StructVectorBatch *root =
//     internal::checked_cast<liborc::StructVectorBatch *>(batch.get());
//   liborc::StructVectorBatch *x =
//     internal::checked_cast<liborc::StructVectorBatch *>(root->fields[0]);
//   liborc::StringVectorBatch *a =
//     internal::checked_cast<liborc::StringVectorBatch *>(x->fields[0]);
//   liborc::LongVectorBatch *b =
//     internal::checked_cast<liborc::LongVectorBatch *>(x->fields[1]);
//   int64_t arrowOffset = 0;
//   int64_t orcOffset = 0;
//   Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize, array.get());
//   if (!st.ok()) {
//     FAIL() << "ORC ColumnBatch not successfully filled";
//   }
//   EXPECT_EQ(x->numElements, 0);
//   EXPECT_FALSE(x->hasNulls);
//   EXPECT_EQ(a->numElements, 0);
//   EXPECT_FALSE(a->hasNulls);
//   EXPECT_EQ(b->numElements, 0);
//   EXPECT_FALSE(b->hasNulls);

//   EXPECT_EQ(arrowOffset, 0);
//   EXPECT_EQ(orcOffset, 0);
//   writer->add(*batch);
//   writer->close();
// }
// TEST(TestAdapterWriteNested, writeStructNoNulls) {

//   std::vector<std::shared_ptr<Field>> xFields;
//   xFields.push_back(std::make_shared<Field>("a", utf8()));
//   xFields.push_back(std::make_shared<Field>("b", int32()));
//   auto sharedPtrArrowType = struct_(xFields);
//   DataType* arrowType = sharedPtrArrowType.get();

//   StringBuilder builder1;
//   (void)(builder1.Append("A"));
//   (void)(builder1.Append("AB"));
//   (void)(builder1.Append(""));
//   (void)(builder1.Append("ABCD"));

//   Int32Builder builder2;
//   (void)(builder2.Append(3));
//   (void)(builder2.Append(-12));
//   (void)(builder2.Append(25));
//   (void)(builder2.Append(76));

//   std::shared_ptr<Array> array1;
//   (void)(builder1.Finish(&array1));
//   std::shared_ptr<Array> array2;
//   (void)(builder2.Finish(&array2));

//   std::vector<std::shared_ptr<Array>> children;
//   children.push_back(array1);
//   children.push_back(array2);

//   auto array = std::make_shared<StructArray>(sharedPtrArrowType, 4, children);

//   MemoryOutputStream mem_stream(DEFAULT_MEM_STREAM_SIZE);
//   ORC_UNIQUE_PTR<liborc::Type> schema(liborc::Type::buildTypeFromString("struct<x:struct<a:string,b:int>>"));
//   liborc::WriterOptions options;
//   ORC_UNIQUE_PTR<liborc::Writer> writer =
//     createWriter(*schema, &mem_stream, options);
//   uint64_t batchSize = 1024;
//   ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch =
//     writer->createRowBatch(batchSize);
//   liborc::StructVectorBatch *root =
//     internal::checked_cast<liborc::StructVectorBatch *>(batch.get());
//   liborc::StructVectorBatch *x =
//     internal::checked_cast<liborc::StructVectorBatch *>(root->fields[0]);
//   liborc::StringVectorBatch *a =
//     internal::checked_cast<liborc::StringVectorBatch *>(x->fields[0]);
//   liborc::LongVectorBatch *b =
//     internal::checked_cast<liborc::LongVectorBatch *>(x->fields[1]);
//   int64_t arrowOffset = 0;
//   int64_t orcOffset = 0;
//   Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize, array.get());
//   if (!st.ok()) {
//     FAIL() << "ORC ColumnBatch not successfully filled";
//   }
//   EXPECT_EQ(x->numElements, 4);
//   EXPECT_FALSE(x->hasNulls);
//   EXPECT_EQ(a->numElements, 4);
//   EXPECT_FALSE(a->hasNulls);
//   EXPECT_EQ(b->numElements, 4);
//   EXPECT_FALSE(b->hasNulls);

//   EXPECT_STREQ(a->data[0], "A");
//   EXPECT_STREQ(a->data[1], "AB");
//   EXPECT_STREQ(a->data[2], "");
//   EXPECT_STREQ(a->data[3], "ABCD");
//   EXPECT_EQ(a->length[0], 1);
//   EXPECT_EQ(a->length[1], 2);
//   EXPECT_EQ(a->length[2], 0);
//   EXPECT_EQ(a->length[3], 4);

//   EXPECT_EQ(b->data[0], 3);
//   EXPECT_EQ(b->data[1], -12);
//   EXPECT_EQ(b->data[2], 25);
//   EXPECT_EQ(b->data[3], 76);

//   EXPECT_EQ(arrowOffset, 4);
//   EXPECT_EQ(orcOffset, 4);
//   writer->add(*batch);
//   writer->close();
// }
// TEST(TestAdapterWriteNested, writeStructMixed1) {

//   std::vector<std::shared_ptr<Field>> xFields;
//   xFields.push_back(std::make_shared<Field>("a", utf8()));
//   xFields.push_back(std::make_shared<Field>("b", int32()));
//   auto sharedPtrArrowType = struct_(xFields);
//   DataType* arrowType = sharedPtrArrowType.get();

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

//   auto array = std::make_shared<StructArray>(sharedPtrArrowType, 4, children);

//   MemoryOutputStream mem_stream(DEFAULT_MEM_STREAM_SIZE);
//   ORC_UNIQUE_PTR<liborc::Type> schema(liborc::Type::buildTypeFromString("struct<x:struct<a:string,b:int>>"));
//   liborc::WriterOptions options;
//   ORC_UNIQUE_PTR<liborc::Writer> writer =
//     createWriter(*schema, &mem_stream, options);
//   uint64_t batchSize = 1024;
//   ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch =
//     writer->createRowBatch(batchSize);
//   liborc::StructVectorBatch *root =
//     internal::checked_cast<liborc::StructVectorBatch *>(batch.get());
//   liborc::StructVectorBatch *x =
//     internal::checked_cast<liborc::StructVectorBatch *>(root->fields[0]);
//   liborc::StringVectorBatch *a =
//     internal::checked_cast<liborc::StringVectorBatch *>(x->fields[0]);
//   liborc::LongVectorBatch *b =
//     internal::checked_cast<liborc::LongVectorBatch *>(x->fields[1]);
//   int64_t arrowOffset = 0;
//   int64_t orcOffset = 0;
//   Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize, array.get());
//   if (!st.ok()) {
//     FAIL() << "ORC ColumnBatch not successfully filled";
//   }
//   EXPECT_EQ(x->numElements, 4);
//   EXPECT_FALSE(x->hasNulls);
//   EXPECT_EQ(a->numElements, 4);
//   EXPECT_TRUE(a->hasNulls);
//   EXPECT_EQ(b->numElements, 4);
//   EXPECT_TRUE(b->hasNulls);

//   EXPECT_STREQ(a->data[0], "A");
//   EXPECT_STREQ(a->data[1], "AB");
//   EXPECT_EQ(a->length[0], 1);
//   EXPECT_EQ(a->length[1], 2);
//   EXPECT_EQ(a->notNull[0], 1);
//   EXPECT_EQ(a->notNull[1], 1);
//   EXPECT_EQ(a->notNull[2], 0);
//   EXPECT_EQ(a->notNull[3], 0);

//   EXPECT_EQ(b->data[0], 3);
//   EXPECT_EQ(b->data[2], 25);
//   EXPECT_EQ(b->notNull[0], 1);
//   EXPECT_EQ(b->notNull[1], 0);
//   EXPECT_EQ(b->notNull[2], 1);
//   EXPECT_EQ(b->notNull[3], 0);

//   EXPECT_EQ(arrowOffset, 4);
//   EXPECT_EQ(orcOffset, 4);
//   writer->add(*batch);
//   writer->close();
// }
// TEST(TestAdapterWriteNested, writeStructMixed2) {
//   std::vector<std::shared_ptr<Field>> xFields;
//   xFields.push_back(std::make_shared<Field>("a", utf8()));
//   xFields.push_back(std::make_shared<Field>("b", int32()));
//   auto sharedPtrArrowType = struct_(xFields);
//   DataType* arrowType = sharedPtrArrowType.get();

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

//   uint8_t bitmap = 14;//00001110
//   auto maybeBuffer = AllocateBuffer(1);
//   if (!maybeBuffer.ok()){
//     FAIL() << "Buffer not created successfully";
//   }
//   std::shared_ptr<Buffer> buffer = *std::move(maybeBuffer);
//   uint8_t* bufferData = buffer->mutable_data();
//   std::memcpy(bufferData, &bitmap, 1);

//   auto array = std::make_shared<StructArray>(sharedPtrArrowType, 4, children, buffer);

//   MemoryOutputStream mem_stream(DEFAULT_MEM_STREAM_SIZE);
//   ORC_UNIQUE_PTR<liborc::Type> schema(liborc::Type::buildTypeFromString("struct<x:struct<a:string,b:int>>"));
//   liborc::WriterOptions options;
//   ORC_UNIQUE_PTR<liborc::Writer> writer =
//     createWriter(*schema, &mem_stream, options);
//   uint64_t batchSize = 1024;
//   ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch =
//     writer->createRowBatch(batchSize);
//   liborc::StructVectorBatch *root =
//     internal::checked_cast<liborc::StructVectorBatch *>(batch.get());
//   liborc::StructVectorBatch *x =
//     internal::checked_cast<liborc::StructVectorBatch *>(root->fields[0]);
//   liborc::StringVectorBatch *a =
//     internal::checked_cast<liborc::StringVectorBatch *>(x->fields[0]);
//   liborc::LongVectorBatch *b =
//     internal::checked_cast<liborc::LongVectorBatch *>(x->fields[1]);
//   int64_t arrowOffset = 0;
//   int64_t orcOffset = 0;
//   Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize, array.get());
//   if (!st.ok()) {
//     FAIL() << "ORC ColumnBatch not successfully filled";
//   }
//   EXPECT_EQ(x->numElements, 4);
//   EXPECT_TRUE(x->hasNulls);
//   EXPECT_EQ(a->numElements, 4);
//   EXPECT_TRUE(a->hasNulls);
//   EXPECT_EQ(b->numElements, 4);
//   EXPECT_TRUE(b->hasNulls);

//   EXPECT_EQ(x->notNull[0], 0);
//   EXPECT_EQ(x->notNull[1], 1);
//   EXPECT_EQ(x->notNull[2], 1);
//   EXPECT_EQ(x->notNull[3], 1);

//   EXPECT_STREQ(a->data[0], "A");
//   EXPECT_STREQ(a->data[1], "AB");
//   EXPECT_EQ(a->length[0], 1);
//   EXPECT_EQ(a->length[1], 2);
//   EXPECT_EQ(a->notNull[0], 1);
//   EXPECT_EQ(a->notNull[1], 1);
//   EXPECT_EQ(a->notNull[2], 0);
//   EXPECT_EQ(a->notNull[3], 0);

//   EXPECT_EQ(b->data[0], 3);
//   EXPECT_EQ(b->data[2], 25);
//   EXPECT_EQ(b->notNull[0], 1);
//   EXPECT_EQ(b->notNull[1], 0);
//   EXPECT_EQ(b->notNull[2], 1);
//   EXPECT_EQ(b->notNull[3], 0);

//   EXPECT_EQ(arrowOffset, 4);
//   EXPECT_EQ(orcOffset, 4);
//   writer->add(*batch);
//   writer->close();
// }
// TEST(TestAdapterWriteNested, writeStructMixed3) {
//   std::vector<std::shared_ptr<Field>> xFields;
//   xFields.push_back(std::make_shared<Field>("a", utf8()));
//   xFields.push_back(std::make_shared<Field>("b", int32()));
//   auto sharedPtrArrowType = struct_(xFields);
//   DataType* arrowType = sharedPtrArrowType.get();

//   StringBuilder builder1;
//   (void)(builder1.AppendNull());
//   (void)(builder1.AppendNull());
//   (void)(builder1.AppendNull());
//   (void)(builder1.AppendNull());

//   Int32Builder builder2;
//   (void)(builder2.AppendNull());
//   (void)(builder2.AppendNull());
//   (void)(builder2.AppendNull());
//   (void)(builder2.AppendNull());

//   std::shared_ptr<Array> array1;
//   (void)(builder1.Finish(&array1));
//   std::shared_ptr<Array> array2;
//   (void)(builder2.Finish(&array2));

//   std::vector<std::shared_ptr<Array>> children;
//   children.push_back(array1);
//   children.push_back(array2);

//   uint8_t bitmap = 14;//00001110
//   auto maybeBuffer = AllocateBuffer(1);
//   if (!maybeBuffer.ok()){
//     FAIL() << "Buffer not created successfully";
//   }
//   std::shared_ptr<Buffer> buffer = *std::move(maybeBuffer);
//   uint8_t* bufferData = buffer->mutable_data();
//   std::memcpy(bufferData, &bitmap, 1);

//   auto array = std::make_shared<StructArray>(sharedPtrArrowType, 4, children, buffer);

//   MemoryOutputStream mem_stream(DEFAULT_MEM_STREAM_SIZE);
//   ORC_UNIQUE_PTR<liborc::Type> schema(liborc::Type::buildTypeFromString("struct<x:struct<a:string,b:int>>"));
//   liborc::WriterOptions options;
//   ORC_UNIQUE_PTR<liborc::Writer> writer =
//     createWriter(*schema, &mem_stream, options);
//   uint64_t batchSize = 1024;
//   ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch =
//     writer->createRowBatch(batchSize);
//   liborc::StructVectorBatch *root =
//     internal::checked_cast<liborc::StructVectorBatch *>(batch.get());
//   liborc::StructVectorBatch *x =
//     internal::checked_cast<liborc::StructVectorBatch *>(root->fields[0]);
//   liborc::StringVectorBatch *a =
//     internal::checked_cast<liborc::StringVectorBatch *>(x->fields[0]);
//   liborc::LongVectorBatch *b =
//     internal::checked_cast<liborc::LongVectorBatch *>(x->fields[1]);
//   int64_t arrowOffset = 0;
//   int64_t orcOffset = 0;
//   Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize, array.get());
//   if (!st.ok()) {
//     FAIL() << "ORC ColumnBatch not successfully filled";
//   }
//   EXPECT_EQ(x->numElements, 4);
//   EXPECT_TRUE(x->hasNulls);
//   EXPECT_EQ(a->numElements, 4);
//   EXPECT_TRUE(a->hasNulls);
//   EXPECT_EQ(b->numElements, 4);
//   EXPECT_TRUE(b->hasNulls);

//   EXPECT_EQ(x->notNull[0], 0);
//   EXPECT_EQ(x->notNull[1], 1);
//   EXPECT_EQ(x->notNull[2], 1);
//   EXPECT_EQ(x->notNull[3], 1);

//   EXPECT_EQ(a->notNull[0], 0);
//   EXPECT_EQ(a->notNull[1], 0);
//   EXPECT_EQ(a->notNull[2], 0);
//   EXPECT_EQ(a->notNull[3], 0);

//   EXPECT_EQ(b->notNull[0], 0);
//   EXPECT_EQ(b->notNull[1], 0);
//   EXPECT_EQ(b->notNull[2], 0);
//   EXPECT_EQ(b->notNull[3], 0);

//   EXPECT_EQ(arrowOffset, 4);
//   EXPECT_EQ(orcOffset, 4);
//   writer->add(*batch);
//   writer->close();
// }

// //List
// TEST(TestAdapterWriteNested, writeListNoNulls) {
//   auto sharedPtrArrowType = list(std::make_shared<Field>("a", int32()));
//   DataType* arrowType = sharedPtrArrowType.get();

//   Int32Builder valuesBuilder, offsetsBuilder;
//   std::shared_ptr<Array> valuesArray, offsetsArray;
//   (void)(valuesBuilder.Append(1));
//   (void)(valuesBuilder.Append(2));
//   (void)(valuesBuilder.Append(3));
//   (void)(valuesBuilder.Append(4));
//   (void)(valuesBuilder.Append(5));
//   (void)(valuesBuilder.Append(6));
//   (void)(valuesBuilder.Append(7));
//   (void)(valuesBuilder.Append(8));
//   (void)(valuesBuilder.Append(9));
//   (void)(valuesBuilder.Append(10));
//   (void)(valuesBuilder.Finish(&valuesArray));
//   (void)(offsetsBuilder.Append(0));
//   (void)(offsetsBuilder.Append(1));
//   (void)(offsetsBuilder.Append(3));
//   (void)(offsetsBuilder.Append(6));
//   (void)(offsetsBuilder.Append(10));
//   (void)(offsetsBuilder.Finish(&offsetsArray));

//   std::shared_ptr<ListArray> array;

//   array = ListArray::FromArrays(*offsetsArray, *valuesArray).ValueOrDie();

//   MemoryOutputStream mem_stream(DEFAULT_MEM_STREAM_SIZE);
//   ORC_UNIQUE_PTR<liborc::Type> schema(liborc::Type::buildTypeFromString("struct<x:array<a:int>>"));
//   liborc::WriterOptions options;
//   ORC_UNIQUE_PTR<liborc::Writer> writer =
//     createWriter(*schema, &mem_stream, options);
//   uint64_t batchSize = 1024;
//   ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch =
//     writer->createRowBatch(batchSize);
//   liborc::StructVectorBatch *root =
//     internal::checked_cast<liborc::StructVectorBatch *>(batch.get());
//   liborc::ListVectorBatch *x =
//     internal::checked_cast<liborc::ListVectorBatch *>(root->fields[0]);
//   liborc::LongVectorBatch *a =
//     internal::checked_cast<liborc::LongVectorBatch *>((x->elements).get());
//   int64_t arrowOffset = 0;
//   int64_t orcOffset = 0;
//   Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize, array.get());
//   if (!st.ok()) {
//     FAIL() << "ORC ColumnBatch not successfully filled";
//   }

//   EXPECT_EQ(x->numElements, 4);
//   EXPECT_FALSE(x->hasNulls);
//   EXPECT_EQ(a->numElements, 10);
//   EXPECT_FALSE(a->hasNulls);

//   EXPECT_EQ(x->offsets[0], 0);
//   EXPECT_EQ(x->offsets[1], 1);
//   EXPECT_EQ(x->offsets[2], 3);
//   EXPECT_EQ(x->offsets[3], 6);
//   EXPECT_EQ(x->offsets[4], 10);

//   EXPECT_EQ(a->data[0], 1);
//   EXPECT_EQ(a->data[1], 2);
//   EXPECT_EQ(a->data[2], 3);
//   EXPECT_EQ(a->data[3], 4);
//   EXPECT_EQ(a->data[4], 5);
//   EXPECT_EQ(a->data[5], 6);
//   EXPECT_EQ(a->data[6], 7);
//   EXPECT_EQ(a->data[7], 8);
//   EXPECT_EQ(a->data[8], 9);
//   EXPECT_EQ(a->data[9], 10);

//   EXPECT_EQ(arrowOffset, 4);
//   EXPECT_EQ(orcOffset, 4);
//   writer->add(*batch);
//   writer->close();
// }

// TEST(TestAdapterWriteNested, writeListMixed1) {
//   auto sharedPtrArrowType = list(std::make_shared<Field>("a", int32()));
//   DataType* arrowType = sharedPtrArrowType.get();

//   Int32Builder valuesBuilder, offsetsBuilder;
//   std::shared_ptr<Array> valuesArray, offsetsArray;
//   (void)(valuesBuilder.AppendNull());
//   (void)(valuesBuilder.Append(2));
//   (void)(valuesBuilder.Append(3));
//   (void)(valuesBuilder.Append(4));
//   (void)(valuesBuilder.AppendNull());
//   (void)(valuesBuilder.Append(6));
//   (void)(valuesBuilder.AppendNull());
//   (void)(valuesBuilder.Append(8));
//   (void)(valuesBuilder.Append(9));
//   (void)(valuesBuilder.AppendNull());
//   (void)(valuesBuilder.Finish(&valuesArray));
//   (void)(offsetsBuilder.Append(0));
//   (void)(offsetsBuilder.Append(1));
//   (void)(offsetsBuilder.Append(3));
//   (void)(offsetsBuilder.Append(6));
//   (void)(offsetsBuilder.Append(10));
//   (void)(offsetsBuilder.Finish(&offsetsArray));

//   std::shared_ptr<ListArray> array;

//   array = ListArray::FromArrays(*offsetsArray, *valuesArray).ValueOrDie();

//   MemoryOutputStream mem_stream(DEFAULT_MEM_STREAM_SIZE);
//   ORC_UNIQUE_PTR<liborc::Type> schema(liborc::Type::buildTypeFromString("struct<x:array<a:int>>"));
//   liborc::WriterOptions options;
//   ORC_UNIQUE_PTR<liborc::Writer> writer =
//     createWriter(*schema, &mem_stream, options);
//   uint64_t batchSize = 1024;
//   ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch =
//     writer->createRowBatch(batchSize);
//   liborc::StructVectorBatch *root =
//     internal::checked_cast<liborc::StructVectorBatch *>(batch.get());
//   liborc::ListVectorBatch *x =
//     internal::checked_cast<liborc::ListVectorBatch *>(root->fields[0]);
//   liborc::LongVectorBatch *a =
//     internal::checked_cast<liborc::LongVectorBatch *>((x->elements).get());
//   int64_t arrowOffset = 0;
//   int64_t orcOffset = 0;
//   Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize, array.get());
//   if (!st.ok()) {
//     FAIL() << "ORC ColumnBatch not successfully filled";
//   }

//   EXPECT_EQ(x->numElements, 4);
//   EXPECT_FALSE(x->hasNulls);
//   EXPECT_EQ(a->numElements, 10);
//   EXPECT_TRUE(a->hasNulls);

//   EXPECT_EQ(x->offsets[0], 0);
//   EXPECT_EQ(x->offsets[1], 1);
//   EXPECT_EQ(x->offsets[2], 3);
//   EXPECT_EQ(x->offsets[3], 6);
//   EXPECT_EQ(x->offsets[4], 10);

//   EXPECT_EQ(a->notNull[0], 0);
//   EXPECT_EQ(a->notNull[1], 1);
//   EXPECT_EQ(a->notNull[2], 1);
//   EXPECT_EQ(a->notNull[3], 1);
//   EXPECT_EQ(a->notNull[4], 0);
//   EXPECT_EQ(a->notNull[5], 1);
//   EXPECT_EQ(a->notNull[6], 0);
//   EXPECT_EQ(a->notNull[7], 1);
//   EXPECT_EQ(a->notNull[8], 1);
//   EXPECT_EQ(a->notNull[9], 0);

//   EXPECT_EQ(a->data[1], 2);
//   EXPECT_EQ(a->data[2], 3);
//   EXPECT_EQ(a->data[3], 4);
//   EXPECT_EQ(a->data[5], 6);
//   EXPECT_EQ(a->data[7], 8);
//   EXPECT_EQ(a->data[8], 9);

//   EXPECT_EQ(arrowOffset, 4);
//   EXPECT_EQ(orcOffset, 4);
//   writer->add(*batch);
//   writer->close();
// }

// TEST(TestAdapterWriteNested, writeListMixed2) {
//   auto sharedPtrArrowType = list(std::make_shared<Field>("a", int32()));
//   DataType* arrowType = sharedPtrArrowType.get();

//   Int32Builder valuesBuilder, offsetsBuilder;

//   int32_t offsets[5] = {0, 1, 3, 6, 10};

//   BufferBuilder builder;
//   (void)(builder.Resize(20));
//   (void)(builder.Append(offsets, 20));
//   std::shared_ptr<arrow::Buffer> offsetsBuffer;
//   if (!builder.Finish(&offsetsBuffer).ok()) {
//     FAIL() << "The offsets buffer can not be constructed!";
//   }

//   std::shared_ptr<Array> valuesArray;
//   (void)(valuesBuilder.AppendNull());
//   (void)(valuesBuilder.Append(2));
//   (void)(valuesBuilder.Append(3));
//   (void)(valuesBuilder.Append(4));
//   (void)(valuesBuilder.AppendNull());
//   (void)(valuesBuilder.Append(6));
//   (void)(valuesBuilder.AppendNull());
//   (void)(valuesBuilder.Append(8));
//   (void)(valuesBuilder.Append(9));
//   (void)(valuesBuilder.AppendNull());
//   (void)(valuesBuilder.Finish(&valuesArray));

//   uint8_t bitmap = 13;//00001101
//   auto maybeBuffer = AllocateBuffer(1);
//   if (!maybeBuffer.ok()){
//     FAIL() << "Buffer not created successfully";
//   }
//   std::shared_ptr<Buffer> bitmapBuffer = *std::move(maybeBuffer);
//   uint8_t* bufferData = bitmapBuffer->mutable_data();
//   std::memcpy(bufferData, &bitmap, 1);

//   auto array = std::make_shared<ListArray>(sharedPtrArrowType, 4, offsetsBuffer, valuesArray, bitmapBuffer);

//   MemoryOutputStream mem_stream(DEFAULT_MEM_STREAM_SIZE);
//   ORC_UNIQUE_PTR<liborc::Type> schema(liborc::Type::buildTypeFromString("struct<x:array<a:int>>"));
//   liborc::WriterOptions options;
//   ORC_UNIQUE_PTR<liborc::Writer> writer =
//     createWriter(*schema, &mem_stream, options);
//   uint64_t batchSize = 1024;
//   ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch =
//     writer->createRowBatch(batchSize);
//   liborc::StructVectorBatch *root =
//     internal::checked_cast<liborc::StructVectorBatch *>(batch.get());
//   liborc::ListVectorBatch *x =
//     internal::checked_cast<liborc::ListVectorBatch *>(root->fields[0]);
//   liborc::LongVectorBatch *a =
//     internal::checked_cast<liborc::LongVectorBatch *>((x->elements).get());
//   int64_t arrowOffset = 0;
//   int64_t orcOffset = 0;
//   Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize, array.get());
//   if (!st.ok()) {
//     FAIL() << "ORC ColumnBatch not successfully filled";
//   }

//   EXPECT_EQ(x->numElements, 4);
//   EXPECT_TRUE(x->hasNulls);
//   EXPECT_EQ(a->numElements, 10);
//   EXPECT_TRUE(a->hasNulls);

//   EXPECT_EQ(x->notNull[0], 1);
//   EXPECT_EQ(x->notNull[1], 0);
//   EXPECT_EQ(x->notNull[2], 1);
//   EXPECT_EQ(x->notNull[3], 1);

//   EXPECT_EQ(x->offsets[0], 0);
//   EXPECT_EQ(x->offsets[1], 1);
//   EXPECT_EQ(x->offsets[2], 3);
//   EXPECT_EQ(x->offsets[3], 6);
//   EXPECT_EQ(x->offsets[4], 10);

//   EXPECT_EQ(a->notNull[0], 0);
//   EXPECT_EQ(a->notNull[1], 1);
//   EXPECT_EQ(a->notNull[2], 1);
//   EXPECT_EQ(a->notNull[3], 1);
//   EXPECT_EQ(a->notNull[4], 0);
//   EXPECT_EQ(a->notNull[5], 1);
//   EXPECT_EQ(a->notNull[6], 0);
//   EXPECT_EQ(a->notNull[7], 1);
//   EXPECT_EQ(a->notNull[8], 1);
//   EXPECT_EQ(a->notNull[9], 0);

//   EXPECT_EQ(a->data[1], 2);
//   EXPECT_EQ(a->data[2], 3);
//   EXPECT_EQ(a->data[3], 4);
//   EXPECT_EQ(a->data[5], 6);
//   EXPECT_EQ(a->data[7], 8);
//   EXPECT_EQ(a->data[8], 9);

//   EXPECT_EQ(arrowOffset, 4);
//   EXPECT_EQ(orcOffset, 4);
//   writer->add(*batch);
//   writer->close();
// }

// TEST(TestAdapterWriteNested, writeListMixed3) {
//   auto sharedPtrArrowType = list(std::make_shared<Field>("a", int32()));
//   DataType* arrowType = sharedPtrArrowType.get();

//   Int32Builder valuesBuilder, offsetsBuilder;

//   int32_t offsets[5] = {0, 1, 3, 3, 7};

//   BufferBuilder builder;
//   (void)(builder.Resize(20));
//   (void)(builder.Append(offsets, 20));
//   std::shared_ptr<arrow::Buffer> offsetsBuffer;
//   if (!builder.Finish(&offsetsBuffer).ok()) {
//     FAIL() << "The offsets buffer can not be constructed!";
//   }

//   std::shared_ptr<Array> valuesArray;
//   (void)(valuesBuilder.AppendNull());
//   (void)(valuesBuilder.Append(2));
//   (void)(valuesBuilder.Append(3));
//   (void)(valuesBuilder.AppendNull());
//   (void)(valuesBuilder.Append(8));
//   (void)(valuesBuilder.Append(9));
//   (void)(valuesBuilder.AppendNull());
//   (void)(valuesBuilder.Finish(&valuesArray));

//   uint8_t bitmap = 13;//00001101
//   auto maybeBuffer = AllocateBuffer(1);
//   if (!maybeBuffer.ok()){
//     FAIL() << "Buffer not created successfully";
//   }
//   std::shared_ptr<Buffer> bitmapBuffer = *std::move(maybeBuffer);
//   uint8_t* bufferData = bitmapBuffer->mutable_data();
//   std::memcpy(bufferData, &bitmap, 1);

//   auto array = std::make_shared<ListArray>(sharedPtrArrowType, 4, offsetsBuffer, valuesArray, bitmapBuffer);

//   MemoryOutputStream mem_stream(DEFAULT_MEM_STREAM_SIZE);
//   ORC_UNIQUE_PTR<liborc::Type> schema(liborc::Type::buildTypeFromString("struct<x:array<a:int>>"));
//   liborc::WriterOptions options;
//   ORC_UNIQUE_PTR<liborc::Writer> writer =
//     createWriter(*schema, &mem_stream, options);
//   uint64_t batchSize = 1024;
//   ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch =
//     writer->createRowBatch(batchSize);
//   liborc::StructVectorBatch *root =
//     internal::checked_cast<liborc::StructVectorBatch *>(batch.get());
//   liborc::ListVectorBatch *x =
//     internal::checked_cast<liborc::ListVectorBatch *>(root->fields[0]);
//   liborc::LongVectorBatch *a =
//     internal::checked_cast<liborc::LongVectorBatch *>((x->elements).get());
//   int64_t arrowOffset = 0;
//   int64_t orcOffset = 0;
//   Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize, array.get());
//   if (!st.ok()) {
//     FAIL() << "ORC ColumnBatch not successfully filled";
//   }

//   EXPECT_EQ(x->numElements, 4);
//   EXPECT_TRUE(x->hasNulls);
//   EXPECT_EQ(a->numElements, 7);
//   EXPECT_TRUE(a->hasNulls);

//   EXPECT_EQ(x->notNull[0], 1);
//   EXPECT_EQ(x->notNull[1], 0);
//   EXPECT_EQ(x->notNull[2], 1);
//   EXPECT_EQ(x->notNull[3], 1);

//   EXPECT_EQ(x->offsets[0], 0);
//   EXPECT_EQ(x->offsets[1], 1);
//   EXPECT_EQ(x->offsets[2], 3);
//   EXPECT_EQ(x->offsets[3], 3);
//   EXPECT_EQ(x->offsets[4], 7);

//   EXPECT_EQ(a->notNull[0], 0);
//   EXPECT_EQ(a->notNull[1], 1);
//   EXPECT_EQ(a->notNull[2], 1);
//   EXPECT_EQ(a->notNull[3], 0);
//   EXPECT_EQ(a->notNull[4], 1);
//   EXPECT_EQ(a->notNull[5], 1);
//   EXPECT_EQ(a->notNull[6], 0);

//   EXPECT_EQ(a->data[1], 2);
//   EXPECT_EQ(a->data[2], 3);
//   EXPECT_EQ(a->data[4], 8);
//   EXPECT_EQ(a->data[5], 9);

//   EXPECT_EQ(arrowOffset, 4);
//   EXPECT_EQ(orcOffset, 4);
//   writer->add(*batch);
//   writer->close();
// }

// //LargeList
// TEST(TestAdapterWriteNested, writeLargeListNoNulls) {
//   auto sharedPtrArrowType = large_list(std::make_shared<Field>("a", int32()));
//   DataType* arrowType = sharedPtrArrowType.get();

//   Int32Builder valuesBuilder; 
//   Int64Builder offsetsBuilder;
//   std::shared_ptr<Array> valuesArray, offsetsArray;
//   (void)(valuesBuilder.Append(1));
//   (void)(valuesBuilder.Append(2));
//   (void)(valuesBuilder.Append(3));
//   (void)(valuesBuilder.Append(4));
//   (void)(valuesBuilder.Append(5));
//   (void)(valuesBuilder.Append(6));
//   (void)(valuesBuilder.Append(7));
//   (void)(valuesBuilder.Append(8));
//   (void)(valuesBuilder.Append(9));
//   (void)(valuesBuilder.Append(10));
//   (void)(valuesBuilder.Finish(&valuesArray));
//   (void)(offsetsBuilder.Append(0));
//   (void)(offsetsBuilder.Append(1));
//   (void)(offsetsBuilder.Append(3));
//   (void)(offsetsBuilder.Append(6));
//   (void)(offsetsBuilder.Append(10));
//   (void)(offsetsBuilder.Finish(&offsetsArray));

//   std::shared_ptr<LargeListArray> array;

//   array = LargeListArray::FromArrays(*offsetsArray, *valuesArray).ValueOrDie();

//   MemoryOutputStream mem_stream(DEFAULT_MEM_STREAM_SIZE);
//   ORC_UNIQUE_PTR<liborc::Type> schema(liborc::Type::buildTypeFromString("struct<x:array<a:int>>"));
//   liborc::WriterOptions options;
//   ORC_UNIQUE_PTR<liborc::Writer> writer =
//     createWriter(*schema, &mem_stream, options);
//   uint64_t batchSize = 1024;
//   ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch =
//     writer->createRowBatch(batchSize);
//   liborc::StructVectorBatch *root =
//     internal::checked_cast<liborc::StructVectorBatch *>(batch.get());
//   liborc::ListVectorBatch *x =
//     internal::checked_cast<liborc::ListVectorBatch *>(root->fields[0]);
//   liborc::LongVectorBatch *a =
//     internal::checked_cast<liborc::LongVectorBatch *>((x->elements).get());
//   int64_t arrowOffset = 0;
//   int64_t orcOffset = 0;
//   Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize, array.get());
//   if (!st.ok()) {
//     FAIL() << "ORC ColumnBatch not successfully filled";
//   }

//   EXPECT_EQ(x->numElements, 4);
//   EXPECT_FALSE(x->hasNulls);
//   EXPECT_EQ(a->numElements, 10);
//   EXPECT_FALSE(a->hasNulls);

//   EXPECT_EQ(x->offsets[0], 0);
//   EXPECT_EQ(x->offsets[1], 1);
//   EXPECT_EQ(x->offsets[2], 3);
//   EXPECT_EQ(x->offsets[3], 6);
//   EXPECT_EQ(x->offsets[4], 10);

//   EXPECT_EQ(a->data[0], 1);
//   EXPECT_EQ(a->data[1], 2);
//   EXPECT_EQ(a->data[2], 3);
//   EXPECT_EQ(a->data[3], 4);
//   EXPECT_EQ(a->data[4], 5);
//   EXPECT_EQ(a->data[5], 6);
//   EXPECT_EQ(a->data[6], 7);
//   EXPECT_EQ(a->data[7], 8);
//   EXPECT_EQ(a->data[8], 9);
//   EXPECT_EQ(a->data[9], 10);

//   EXPECT_EQ(arrowOffset, 4);
//   EXPECT_EQ(orcOffset, 4);
//   writer->add(*batch);
//   writer->close();
// }

// TEST(TestAdapterWriteNested, writeLargeListMixed1) {
//   auto sharedPtrArrowType = large_list(std::make_shared<Field>("a", int32()));
//   DataType* arrowType = sharedPtrArrowType.get();

//   Int32Builder valuesBuilder;
//   Int64Builder offsetsBuilder;
//   std::shared_ptr<Array> valuesArray, offsetsArray;
//   (void)(valuesBuilder.AppendNull());
//   (void)(valuesBuilder.Append(2));
//   (void)(valuesBuilder.Append(3));
//   (void)(valuesBuilder.Append(4));
//   (void)(valuesBuilder.AppendNull());
//   (void)(valuesBuilder.Append(6));
//   (void)(valuesBuilder.AppendNull());
//   (void)(valuesBuilder.Append(8));
//   (void)(valuesBuilder.Append(9));
//   (void)(valuesBuilder.AppendNull());
//   (void)(valuesBuilder.Finish(&valuesArray));
//   (void)(offsetsBuilder.Append(0));
//   (void)(offsetsBuilder.Append(1));
//   (void)(offsetsBuilder.Append(3));
//   (void)(offsetsBuilder.Append(6));
//   (void)(offsetsBuilder.Append(10));
//   (void)(offsetsBuilder.Finish(&offsetsArray));

//   std::shared_ptr<LargeListArray> array;

//   array = LargeListArray::FromArrays(*offsetsArray, *valuesArray).ValueOrDie();

//   MemoryOutputStream mem_stream(DEFAULT_MEM_STREAM_SIZE);
//   ORC_UNIQUE_PTR<liborc::Type> schema(liborc::Type::buildTypeFromString("struct<x:array<a:int>>"));
//   liborc::WriterOptions options;
//   ORC_UNIQUE_PTR<liborc::Writer> writer =
//     createWriter(*schema, &mem_stream, options);
//   uint64_t batchSize = 1024;
//   ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch =
//     writer->createRowBatch(batchSize);
//   liborc::StructVectorBatch *root =
//     internal::checked_cast<liborc::StructVectorBatch *>(batch.get());
//   liborc::ListVectorBatch *x =
//     internal::checked_cast<liborc::ListVectorBatch *>(root->fields[0]);
//   liborc::LongVectorBatch *a =
//     internal::checked_cast<liborc::LongVectorBatch *>((x->elements).get());
//   int64_t arrowOffset = 0;
//   int64_t orcOffset = 0;
//   Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize, array.get());
//   if (!st.ok()) {
//     FAIL() << "ORC ColumnBatch not successfully filled";
//   }

//   EXPECT_EQ(x->numElements, 4);
//   EXPECT_FALSE(x->hasNulls);
//   EXPECT_EQ(a->numElements, 10);
//   EXPECT_TRUE(a->hasNulls);

//   EXPECT_EQ(x->offsets[0], 0);
//   EXPECT_EQ(x->offsets[1], 1);
//   EXPECT_EQ(x->offsets[2], 3);
//   EXPECT_EQ(x->offsets[3], 6);
//   EXPECT_EQ(x->offsets[4], 10);

//   EXPECT_EQ(a->notNull[0], 0);
//   EXPECT_EQ(a->notNull[1], 1);
//   EXPECT_EQ(a->notNull[2], 1);
//   EXPECT_EQ(a->notNull[3], 1);
//   EXPECT_EQ(a->notNull[4], 0);
//   EXPECT_EQ(a->notNull[5], 1);
//   EXPECT_EQ(a->notNull[6], 0);
//   EXPECT_EQ(a->notNull[7], 1);
//   EXPECT_EQ(a->notNull[8], 1);
//   EXPECT_EQ(a->notNull[9], 0);

//   EXPECT_EQ(a->data[1], 2);
//   EXPECT_EQ(a->data[2], 3);
//   EXPECT_EQ(a->data[3], 4);
//   EXPECT_EQ(a->data[5], 6);
//   EXPECT_EQ(a->data[7], 8);
//   EXPECT_EQ(a->data[8], 9);

//   EXPECT_EQ(arrowOffset, 4);
//   EXPECT_EQ(orcOffset, 4);
//   writer->add(*batch);
//   writer->close();
// }

// TEST(TestAdapterWriteNested, writeLargeListMixed2) {
//   auto sharedPtrArrowType = large_list(std::make_shared<Field>("a", int32()));
//   DataType* arrowType = sharedPtrArrowType.get();

//   Int32Builder valuesBuilder;
//   Int64Builder offsetsBuilder;
//   int64_t offsets[5] = {0, 1, 3, 6, 10};

//   BufferBuilder builder;
//   (void)(builder.Resize(40));
//   (void)(builder.Append(offsets, 40));
//   std::shared_ptr<arrow::Buffer> offsetsBuffer;
//   if (!builder.Finish(&offsetsBuffer).ok()) {
//     FAIL() << "The offsets buffer can not be constructed!";
//   }

//   std::shared_ptr<Array> valuesArray;
//   (void)(valuesBuilder.AppendNull());
//   (void)(valuesBuilder.Append(2));
//   (void)(valuesBuilder.Append(3));
//   (void)(valuesBuilder.Append(4));
//   (void)(valuesBuilder.AppendNull());
//   (void)(valuesBuilder.Append(6));
//   (void)(valuesBuilder.AppendNull());
//   (void)(valuesBuilder.Append(8));
//   (void)(valuesBuilder.Append(9));
//   (void)(valuesBuilder.AppendNull());
//   (void)(valuesBuilder.Finish(&valuesArray));

//   uint8_t bitmap = 13;//00001101
//   auto maybeBuffer = AllocateBuffer(1);
//   if (!maybeBuffer.ok()){
//     FAIL() << "Buffer not created successfully";
//   }
//   std::shared_ptr<Buffer> bitmapBuffer = *std::move(maybeBuffer);
//   uint8_t* bufferData = bitmapBuffer->mutable_data();
//   std::memcpy(bufferData, &bitmap, 1);

//   auto array = std::make_shared<LargeListArray>(sharedPtrArrowType, 4, offsetsBuffer, valuesArray, bitmapBuffer);

//   MemoryOutputStream mem_stream(DEFAULT_MEM_STREAM_SIZE);
//   ORC_UNIQUE_PTR<liborc::Type> schema(liborc::Type::buildTypeFromString("struct<x:array<a:int>>"));
//   liborc::WriterOptions options;
//   ORC_UNIQUE_PTR<liborc::Writer> writer =
//     createWriter(*schema, &mem_stream, options);
//   uint64_t batchSize = 1024;
//   ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch =
//     writer->createRowBatch(batchSize);
//   liborc::StructVectorBatch *root =
//     internal::checked_cast<liborc::StructVectorBatch *>(batch.get());
//   liborc::ListVectorBatch *x =
//     internal::checked_cast<liborc::ListVectorBatch *>(root->fields[0]);
//   liborc::LongVectorBatch *a =
//     internal::checked_cast<liborc::LongVectorBatch *>((x->elements).get());
//   int64_t arrowOffset = 0;
//   int64_t orcOffset = 0;
//   Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize, array.get());
//   if (!st.ok()) {
//     FAIL() << "ORC ColumnBatch not successfully filled";
//   }

//   EXPECT_EQ(x->numElements, 4);
//   EXPECT_TRUE(x->hasNulls);
//   EXPECT_EQ(a->numElements, 10);
//   EXPECT_TRUE(a->hasNulls);

//   EXPECT_EQ(x->notNull[0], 1);
//   EXPECT_EQ(x->notNull[1], 0);
//   EXPECT_EQ(x->notNull[2], 1);
//   EXPECT_EQ(x->notNull[3], 1);

//   EXPECT_EQ(x->offsets[0], 0);
//   EXPECT_EQ(x->offsets[1], 1);
//   EXPECT_EQ(x->offsets[2], 3);
//   EXPECT_EQ(x->offsets[3], 6);
//   EXPECT_EQ(x->offsets[4], 10);

//   EXPECT_EQ(a->notNull[0], 0);
//   EXPECT_EQ(a->notNull[1], 1);
//   EXPECT_EQ(a->notNull[2], 1);
//   EXPECT_EQ(a->notNull[3], 1);
//   EXPECT_EQ(a->notNull[4], 0);
//   EXPECT_EQ(a->notNull[5], 1);
//   EXPECT_EQ(a->notNull[6], 0);
//   EXPECT_EQ(a->notNull[7], 1);
//   EXPECT_EQ(a->notNull[8], 1);
//   EXPECT_EQ(a->notNull[9], 0);

//   EXPECT_EQ(a->data[1], 2);
//   EXPECT_EQ(a->data[2], 3);
//   EXPECT_EQ(a->data[3], 4);
//   EXPECT_EQ(a->data[5], 6);
//   EXPECT_EQ(a->data[7], 8);
//   EXPECT_EQ(a->data[8], 9);

//   EXPECT_EQ(arrowOffset, 4);
//   EXPECT_EQ(orcOffset, 4);
//   writer->add(*batch);
//   writer->close();
// }

// TEST(TestAdapterWriteNested, writeLargeListMixed3) {
//   auto sharedPtrArrowType = large_list(std::make_shared<Field>("a", int32()));
//   DataType* arrowType = sharedPtrArrowType.get();

//   Int32Builder valuesBuilder;
//   Int64Builder offsetsBuilder;
//   int64_t offsets[5] = {0, 1, 3, 3, 7};

//   BufferBuilder builder;
//   (void)(builder.Resize(40));
//   (void)(builder.Append(offsets, 40));
//   std::shared_ptr<arrow::Buffer> offsetsBuffer;
//   if (!builder.Finish(&offsetsBuffer).ok()) {
//     FAIL() << "The offsets buffer can not be constructed!";
//   }

//   std::shared_ptr<Array> valuesArray;
//   (void)(valuesBuilder.AppendNull());
//   (void)(valuesBuilder.Append(2));
//   (void)(valuesBuilder.Append(3));
//   (void)(valuesBuilder.AppendNull());
//   (void)(valuesBuilder.Append(8));
//   (void)(valuesBuilder.Append(9));
//   (void)(valuesBuilder.AppendNull());
//   (void)(valuesBuilder.Finish(&valuesArray));

//   uint8_t bitmap = 13;//00001101
//   auto maybeBuffer = AllocateBuffer(1);
//   if (!maybeBuffer.ok()){
//     FAIL() << "Buffer not created successfully";
//   }
//   std::shared_ptr<Buffer> bitmapBuffer = *std::move(maybeBuffer);
//   uint8_t* bufferData = bitmapBuffer->mutable_data();
//   std::memcpy(bufferData, &bitmap, 1);

//   auto array = std::make_shared<LargeListArray>(sharedPtrArrowType, 4, offsetsBuffer, valuesArray, bitmapBuffer);

//   MemoryOutputStream mem_stream(DEFAULT_MEM_STREAM_SIZE);
//   ORC_UNIQUE_PTR<liborc::Type> schema(liborc::Type::buildTypeFromString("struct<x:array<a:int>>"));
//   liborc::WriterOptions options;
//   ORC_UNIQUE_PTR<liborc::Writer> writer =
//     createWriter(*schema, &mem_stream, options);
//   uint64_t batchSize = 1024;
//   ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch =
//     writer->createRowBatch(batchSize);
//   liborc::StructVectorBatch *root =
//     internal::checked_cast<liborc::StructVectorBatch *>(batch.get());
//   liborc::ListVectorBatch *x =
//     internal::checked_cast<liborc::ListVectorBatch *>(root->fields[0]);
//   liborc::LongVectorBatch *a =
//     internal::checked_cast<liborc::LongVectorBatch *>((x->elements).get());
//   int64_t arrowOffset = 0;
//   int64_t orcOffset = 0;
//   Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize, array.get());
//   if (!st.ok()) {
//     FAIL() << "ORC ColumnBatch not successfully filled";
//   }

//   EXPECT_EQ(x->numElements, 4);
//   EXPECT_TRUE(x->hasNulls);
//   EXPECT_EQ(a->numElements, 7);
//   EXPECT_TRUE(a->hasNulls);

//   EXPECT_EQ(x->notNull[0], 1);
//   EXPECT_EQ(x->notNull[1], 0);
//   EXPECT_EQ(x->notNull[2], 1);
//   EXPECT_EQ(x->notNull[3], 1);

//   EXPECT_EQ(x->offsets[0], 0);
//   EXPECT_EQ(x->offsets[1], 1);
//   EXPECT_EQ(x->offsets[2], 3);
//   EXPECT_EQ(x->offsets[3], 3);
//   EXPECT_EQ(x->offsets[4], 7);

//   EXPECT_EQ(a->notNull[0], 0);
//   EXPECT_EQ(a->notNull[1], 1);
//   EXPECT_EQ(a->notNull[2], 1);
//   EXPECT_EQ(a->notNull[3], 0);
//   EXPECT_EQ(a->notNull[4], 1);
//   EXPECT_EQ(a->notNull[5], 1);
//   EXPECT_EQ(a->notNull[6], 0);

//   EXPECT_EQ(a->data[1], 2);
//   EXPECT_EQ(a->data[2], 3);
//   EXPECT_EQ(a->data[4], 8);
//   EXPECT_EQ(a->data[5], 9);

//   EXPECT_EQ(arrowOffset, 4);
//   EXPECT_EQ(orcOffset, 4);
//   writer->add(*batch);
//   writer->close();
// }

// //FixedSizeList
// TEST(TestAdapterWriteNested, writeFixedSizeListEmpty) {
//   auto sharedPtrArrowType = fixed_size_list(std::make_shared<Field>("a", int32()), 3);
//   DataType* arrowType = sharedPtrArrowType.get();

//   Int32Builder valuesBuilder;
//   std::shared_ptr<Array> valuesArray;
//   (void)(valuesBuilder.Finish(&valuesArray));

//   std::shared_ptr<FixedSizeListArray> array = std::static_pointer_cast<FixedSizeListArray>(FixedSizeListArray::FromArrays(valuesArray, 3).ValueOrDie());

//   MemoryOutputStream mem_stream(DEFAULT_MEM_STREAM_SIZE);
//   ORC_UNIQUE_PTR<liborc::Type> schema(liborc::Type::buildTypeFromString("struct<x:array<a:int>>"));
//   liborc::WriterOptions options;
//   ORC_UNIQUE_PTR<liborc::Writer> writer =
//     createWriter(*schema, &mem_stream, options);
//   uint64_t batchSize = 1024;
//   ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch =
//     writer->createRowBatch(batchSize);
//   liborc::StructVectorBatch *root =
//     internal::checked_cast<liborc::StructVectorBatch *>(batch.get());
//   liborc::ListVectorBatch *x =
//     internal::checked_cast<liborc::ListVectorBatch *>(root->fields[0]);
//   liborc::LongVectorBatch *a =
//     internal::checked_cast<liborc::LongVectorBatch *>((x->elements).get());
//   int64_t arrowOffset = 0;
//   int64_t orcOffset = 0;
//   Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize, array.get());
//   if (!st.ok()) {
//     FAIL() << "ORC ColumnBatch not successfully filled";
//   }

//   EXPECT_EQ(x->numElements, 0);
//   EXPECT_FALSE(x->hasNulls);
//   EXPECT_EQ(a->numElements, 0);
//   EXPECT_FALSE(a->hasNulls);

//   EXPECT_EQ(x->offsets[0], 0);

//   EXPECT_EQ(arrowOffset, 0);
//   EXPECT_EQ(orcOffset, 0);
//   writer->add(*batch);
//   writer->close();
// }

// TEST(TestAdapterWriteNested, writeFixedSizeListAllNulls) {
//   auto sharedPtrArrowType = fixed_size_list(std::make_shared<Field>("a", int32()), 3);
//   DataType* arrowType = sharedPtrArrowType.get();

//   Int32Builder valuesBuilder;
//   std::shared_ptr<Array> valuesArray;
//   (void)(valuesBuilder.AppendNull());
//   (void)(valuesBuilder.AppendNull());
//   (void)(valuesBuilder.AppendNull());
//   (void)(valuesBuilder.AppendNull());
//   (void)(valuesBuilder.AppendNull());
//   (void)(valuesBuilder.AppendNull());
//   (void)(valuesBuilder.Finish(&valuesArray));

//   std::shared_ptr<FixedSizeListArray> array = std::static_pointer_cast<FixedSizeListArray>(FixedSizeListArray::FromArrays(valuesArray, 3).ValueOrDie());

//   MemoryOutputStream mem_stream(DEFAULT_MEM_STREAM_SIZE);
//   ORC_UNIQUE_PTR<liborc::Type> schema(liborc::Type::buildTypeFromString("struct<x:array<a:int>>"));
//   liborc::WriterOptions options;
//   ORC_UNIQUE_PTR<liborc::Writer> writer =
//     createWriter(*schema, &mem_stream, options);
//   uint64_t batchSize = 1024;
//   ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch =
//     writer->createRowBatch(batchSize);
//   liborc::StructVectorBatch *root =
//     internal::checked_cast<liborc::StructVectorBatch *>(batch.get());
//   liborc::ListVectorBatch *x =
//     internal::checked_cast<liborc::ListVectorBatch *>(root->fields[0]);
//   liborc::LongVectorBatch *a =
//     internal::checked_cast<liborc::LongVectorBatch *>((x->elements).get());
//   int64_t arrowOffset = 0;
//   int64_t orcOffset = 0;
//   Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize, array.get());
//   if (!st.ok()) {
//     FAIL() << "ORC ColumnBatch not successfully filled";
//   }

//   EXPECT_EQ(x->numElements, 2);
//   EXPECT_FALSE(x->hasNulls);
//   EXPECT_EQ(a->numElements, 6);
//   EXPECT_TRUE(a->hasNulls);

//   EXPECT_EQ(x->offsets[0], 0);
//   EXPECT_EQ(x->offsets[1], 3);
//   EXPECT_EQ(x->offsets[2], 6);

//   EXPECT_EQ(a->notNull[0], 0);
//   EXPECT_EQ(a->notNull[1], 0);
//   EXPECT_EQ(a->notNull[2], 0);
//   EXPECT_EQ(a->notNull[3], 0);
//   EXPECT_EQ(a->notNull[4], 0);
//   EXPECT_EQ(a->notNull[5], 0);

//   EXPECT_EQ(arrowOffset, 2);
//   EXPECT_EQ(orcOffset, 2);
//   writer->add(*batch);
//   writer->close();
// }

// TEST(TestAdapterWriteNested, writeFixedSizeListZeroLength) {
//   auto sharedPtrArrowType = fixed_size_list(std::make_shared<Field>("a", int32()), 0);
//   DataType* arrowType = sharedPtrArrowType.get();

//   Int32Builder valuesBuilder;
//   std::shared_ptr<Array> valuesArray;
//   (void)(valuesBuilder.Finish(&valuesArray));

//   auto array = std::make_shared<FixedSizeListArray>(sharedPtrArrowType, 3, valuesArray);

//   MemoryOutputStream mem_stream(DEFAULT_MEM_STREAM_SIZE);
//   ORC_UNIQUE_PTR<liborc::Type> schema(liborc::Type::buildTypeFromString("struct<x:array<a:int>>"));
//   liborc::WriterOptions options;
//   ORC_UNIQUE_PTR<liborc::Writer> writer =
//     createWriter(*schema, &mem_stream, options);
//   uint64_t batchSize = 1024;
//   ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch =
//     writer->createRowBatch(batchSize);
//   liborc::StructVectorBatch *root =
//     internal::checked_cast<liborc::StructVectorBatch *>(batch.get());
//   liborc::ListVectorBatch *x =
//     internal::checked_cast<liborc::ListVectorBatch *>(root->fields[0]);
//   liborc::LongVectorBatch *a =
//     internal::checked_cast<liborc::LongVectorBatch *>((x->elements).get());
//   int64_t arrowOffset = 0;
//   int64_t orcOffset = 0;
//   Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize, array.get());
//   if (!st.ok()) {
//     FAIL() << "ORC ColumnBatch not successfully filled";
//   }

//   EXPECT_EQ(x->numElements, 3);
//   EXPECT_FALSE(x->hasNulls);
//   EXPECT_EQ(a->numElements, 0);
//   EXPECT_FALSE(a->hasNulls);

//   EXPECT_EQ(x->offsets[0], 0);

//   EXPECT_EQ(arrowOffset, 3);
//   EXPECT_EQ(orcOffset, 3);
//   writer->add(*batch);
//   writer->close();
// }

// TEST(TestAdapterWriteNested, writeFixedSizeListNoNulls) {
//   auto sharedPtrArrowType = fixed_size_list(std::make_shared<Field>("a", int32()), 3);
//   DataType* arrowType = sharedPtrArrowType.get();

//   Int32Builder valuesBuilder;
//   std::shared_ptr<Array> valuesArray;
//   (void)(valuesBuilder.Append(1));
//   (void)(valuesBuilder.Append(2));
//   (void)(valuesBuilder.Append(3));
//   (void)(valuesBuilder.Append(4));
//   (void)(valuesBuilder.Append(5));
//   (void)(valuesBuilder.Append(6));
//   (void)(valuesBuilder.Append(7));
//   (void)(valuesBuilder.Append(8));
//   (void)(valuesBuilder.Append(9));
//   (void)(valuesBuilder.Append(10));
//   (void)(valuesBuilder.Append(11));
//   (void)(valuesBuilder.Append(12));
//   (void)(valuesBuilder.Finish(&valuesArray));

//   std::shared_ptr<FixedSizeListArray> array = std::static_pointer_cast<FixedSizeListArray>(FixedSizeListArray::FromArrays(valuesArray, 3).ValueOrDie());

//   MemoryOutputStream mem_stream(DEFAULT_MEM_STREAM_SIZE);
//   ORC_UNIQUE_PTR<liborc::Type> schema(liborc::Type::buildTypeFromString("struct<x:array<a:int>>"));
//   liborc::WriterOptions options;
//   ORC_UNIQUE_PTR<liborc::Writer> writer =
//     createWriter(*schema, &mem_stream, options);
//   uint64_t batchSize = 1024;
//   ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch =
//     writer->createRowBatch(batchSize);
//   liborc::StructVectorBatch *root =
//     internal::checked_cast<liborc::StructVectorBatch *>(batch.get());
//   liborc::ListVectorBatch *x =
//     internal::checked_cast<liborc::ListVectorBatch *>(root->fields[0]);
//   liborc::LongVectorBatch *a =
//     internal::checked_cast<liborc::LongVectorBatch *>((x->elements).get());
//   int64_t arrowOffset = 0;
//   int64_t orcOffset = 0;
//   Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize, array.get());
//   if (!st.ok()) {
//     FAIL() << "ORC ColumnBatch not successfully filled";
//   }

//   EXPECT_EQ(x->numElements, 4);
//   EXPECT_FALSE(x->hasNulls);
//   EXPECT_EQ(a->numElements, 12);
//   EXPECT_FALSE(a->hasNulls);

//   EXPECT_EQ(x->offsets[0], 0);
//   EXPECT_EQ(x->offsets[1], 3);
//   EXPECT_EQ(x->offsets[2], 6);
//   EXPECT_EQ(x->offsets[3], 9);
//   EXPECT_EQ(x->offsets[4], 12);

//   EXPECT_EQ(a->data[0], 1);
//   EXPECT_EQ(a->data[1], 2);
//   EXPECT_EQ(a->data[2], 3);
//   EXPECT_EQ(a->data[3], 4);
//   EXPECT_EQ(a->data[4], 5);
//   EXPECT_EQ(a->data[5], 6);
//   EXPECT_EQ(a->data[6], 7);
//   EXPECT_EQ(a->data[7], 8);
//   EXPECT_EQ(a->data[8], 9);
//   EXPECT_EQ(a->data[9], 10);
//   EXPECT_EQ(a->data[10], 11);
//   EXPECT_EQ(a->data[11], 12);

//   EXPECT_EQ(arrowOffset, 4);
//   EXPECT_EQ(orcOffset, 4);
//   writer->add(*batch);
//   writer->close();
// }

// TEST(TestAdapterWriteNested, writeFixedSizeListMixed1) {
//   auto sharedPtrArrowType = fixed_size_list(std::make_shared<Field>("a", int32()), 3);
//   DataType* arrowType = sharedPtrArrowType.get();

//   Int32Builder valuesBuilder;
//   std::shared_ptr<Array> valuesArray;
//   (void)(valuesBuilder.AppendNull());
//   (void)(valuesBuilder.Append(2));
//   (void)(valuesBuilder.Append(3));
//   (void)(valuesBuilder.Append(4));
//   (void)(valuesBuilder.AppendNull());
//   (void)(valuesBuilder.Append(6));
//   (void)(valuesBuilder.AppendNull());
//   (void)(valuesBuilder.Append(8));
//   (void)(valuesBuilder.Append(9));
//   (void)(valuesBuilder.AppendNull());
//   (void)(valuesBuilder.Append(11));
//   (void)(valuesBuilder.Append(12));
//   (void)(valuesBuilder.Finish(&valuesArray));

//   std::shared_ptr<FixedSizeListArray> array = std::static_pointer_cast<FixedSizeListArray>(FixedSizeListArray::FromArrays(valuesArray, 3).ValueOrDie());

//   MemoryOutputStream mem_stream(DEFAULT_MEM_STREAM_SIZE);
//   ORC_UNIQUE_PTR<liborc::Type> schema(liborc::Type::buildTypeFromString("struct<x:array<a:int>>"));
//   liborc::WriterOptions options;
//   ORC_UNIQUE_PTR<liborc::Writer> writer =
//     createWriter(*schema, &mem_stream, options);
//   uint64_t batchSize = 1024;
//   ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch =
//     writer->createRowBatch(batchSize);
//   liborc::StructVectorBatch *root =
//     internal::checked_cast<liborc::StructVectorBatch *>(batch.get());
//   liborc::ListVectorBatch *x =
//     internal::checked_cast<liborc::ListVectorBatch *>(root->fields[0]);
//   liborc::LongVectorBatch *a =
//     internal::checked_cast<liborc::LongVectorBatch *>((x->elements).get());
//   int64_t arrowOffset = 0;
//   int64_t orcOffset = 0;
//   Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize, array.get());
//   if (!st.ok()) {
//     FAIL() << "ORC ColumnBatch not successfully filled";
//   }

//   EXPECT_EQ(x->numElements, 4);
//   EXPECT_FALSE(x->hasNulls);
//   EXPECT_EQ(a->numElements, 12);
//   EXPECT_TRUE(a->hasNulls);

//   EXPECT_EQ(x->offsets[0], 0);
//   EXPECT_EQ(x->offsets[1], 3);
//   EXPECT_EQ(x->offsets[2], 6);
//   EXPECT_EQ(x->offsets[3], 9);
//   EXPECT_EQ(x->offsets[4], 12);

//   EXPECT_EQ(a->notNull[0], 0);
//   EXPECT_EQ(a->notNull[1], 1);
//   EXPECT_EQ(a->notNull[2], 1);
//   EXPECT_EQ(a->notNull[3], 1);
//   EXPECT_EQ(a->notNull[4], 0);
//   EXPECT_EQ(a->notNull[5], 1);
//   EXPECT_EQ(a->notNull[6], 0);
//   EXPECT_EQ(a->notNull[7], 1);
//   EXPECT_EQ(a->notNull[8], 1);
//   EXPECT_EQ(a->notNull[9], 0);
//   EXPECT_EQ(a->notNull[10], 1);
//   EXPECT_EQ(a->notNull[11], 1);

//   EXPECT_EQ(a->data[1], 2);
//   EXPECT_EQ(a->data[2], 3);
//   EXPECT_EQ(a->data[3], 4);
//   EXPECT_EQ(a->data[5], 6);
//   EXPECT_EQ(a->data[7], 8);
//   EXPECT_EQ(a->data[8], 9);
//   EXPECT_EQ(a->data[10], 11);
//   EXPECT_EQ(a->data[11], 12);

//   EXPECT_EQ(arrowOffset, 4);
//   EXPECT_EQ(orcOffset, 4);
//   writer->add(*batch);
//   writer->close();
// }

// TEST(TestAdapterWriteNested, writeFixedSizeListMixed2) {
//   auto sharedPtrArrowType = fixed_size_list(std::make_shared<Field>("a", int32()), 3);
//   DataType* arrowType = sharedPtrArrowType.get();

//   Int32Builder valuesBuilder;

//   std::shared_ptr<Array> valuesArray;
//   (void)(valuesBuilder.AppendNull());
//   (void)(valuesBuilder.Append(2));
//   (void)(valuesBuilder.Append(3));
//   (void)(valuesBuilder.Append(4));
//   (void)(valuesBuilder.AppendNull());
//   (void)(valuesBuilder.Append(6));
//   (void)(valuesBuilder.AppendNull());
//   (void)(valuesBuilder.Append(8));
//   (void)(valuesBuilder.Append(9));
//   (void)(valuesBuilder.AppendNull());
//   (void)(valuesBuilder.Append(11));
//   (void)(valuesBuilder.Append(12));
//   (void)(valuesBuilder.Finish(&valuesArray));

//   uint8_t bitmap = 13;//00001101
//   auto maybeBuffer = AllocateBuffer(1);
//   if (!maybeBuffer.ok()){
//     FAIL() << "Buffer not created successfully";
//   }
//   std::shared_ptr<Buffer> bitmapBuffer = *std::move(maybeBuffer);
//   uint8_t* bufferData = bitmapBuffer->mutable_data();
//   std::memcpy(bufferData, &bitmap, 1);

//   auto array = std::make_shared<FixedSizeListArray>(sharedPtrArrowType, 4, valuesArray, bitmapBuffer);

//   MemoryOutputStream mem_stream(DEFAULT_MEM_STREAM_SIZE);
//   ORC_UNIQUE_PTR<liborc::Type> schema(liborc::Type::buildTypeFromString("struct<x:array<a:int>>"));
//   liborc::WriterOptions options;
//   ORC_UNIQUE_PTR<liborc::Writer> writer =
//     createWriter(*schema, &mem_stream, options);
//   uint64_t batchSize = 1024;
//   ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch =
//     writer->createRowBatch(batchSize);
//   liborc::StructVectorBatch *root =
//     internal::checked_cast<liborc::StructVectorBatch *>(batch.get());
//   liborc::ListVectorBatch *x =
//     internal::checked_cast<liborc::ListVectorBatch *>(root->fields[0]);
//   liborc::LongVectorBatch *a =
//     internal::checked_cast<liborc::LongVectorBatch *>((x->elements).get());
//   int64_t arrowOffset = 0;
//   int64_t orcOffset = 0;
//   Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize, array.get());
//   if (!st.ok()) {
//     FAIL() << "ORC ColumnBatch not successfully filled";
//   }

//   EXPECT_EQ(x->numElements, 4);
//   EXPECT_TRUE(x->hasNulls);
//   EXPECT_EQ(a->numElements, 12);
//   EXPECT_TRUE(a->hasNulls);

//   EXPECT_EQ(x->notNull[0], 1);
//   EXPECT_EQ(x->notNull[1], 0);
//   EXPECT_EQ(x->notNull[2], 1);
//   EXPECT_EQ(x->notNull[3], 1);

//   EXPECT_EQ(x->offsets[0], 0);
//   EXPECT_EQ(x->offsets[1], 3);
//   EXPECT_EQ(x->offsets[2], 6);
//   EXPECT_EQ(x->offsets[3], 9);
//   EXPECT_EQ(x->offsets[4], 12);

//   EXPECT_EQ(a->notNull[0], 0);
//   EXPECT_EQ(a->notNull[1], 1);
//   EXPECT_EQ(a->notNull[2], 1);
//   EXPECT_EQ(a->notNull[3], 1);
//   EXPECT_EQ(a->notNull[4], 0);
//   EXPECT_EQ(a->notNull[5], 1);
//   EXPECT_EQ(a->notNull[6], 0);
//   EXPECT_EQ(a->notNull[7], 1);
//   EXPECT_EQ(a->notNull[8], 1);
//   EXPECT_EQ(a->notNull[9], 0);
//   EXPECT_EQ(a->notNull[10], 1);
//   EXPECT_EQ(a->notNull[11], 1);

//   EXPECT_EQ(a->data[1], 2);
//   EXPECT_EQ(a->data[2], 3);
//   EXPECT_EQ(a->data[3], 4);
//   EXPECT_EQ(a->data[5], 6);
//   EXPECT_EQ(a->data[7], 8);
//   EXPECT_EQ(a->data[8], 9);
//   EXPECT_EQ(a->data[10], 11);
//   EXPECT_EQ(a->data[11], 12);

//   EXPECT_EQ(arrowOffset, 4);
//   EXPECT_EQ(orcOffset, 4);
//   writer->add(*batch);
//   writer->close();
// }

// //Map
// TEST(TestAdapterWriteNested, writeMapEmpty) {
//   auto sharedPtrArrowType = map(utf8(), utf8());
//   DataType* arrowType = sharedPtrArrowType.get();

//   Int32Builder offsetsBuilder;
//   StringBuilder keysBuilder, itemsBuilder;
//   std::shared_ptr<Array> offsetsArray, keysArray, itemsArray;
//   (void)(keysBuilder.Finish(&keysArray));
//   (void)(itemsBuilder.Finish(&itemsArray));
//   (void)(offsetsBuilder.Append(0));
//   (void)(offsetsBuilder.Finish(&offsetsArray));

//   std::shared_ptr<MapArray> array = std::static_pointer_cast<MapArray>(MapArray::FromArrays(offsetsArray, keysArray, itemsArray).ValueOrDie());

//   MemoryOutputStream mem_stream(DEFAULT_MEM_STREAM_SIZE);
//   ORC_UNIQUE_PTR<liborc::Type> schema(liborc::Type::buildTypeFromString("struct<x:map<a:string,b:string>>"));
//   liborc::WriterOptions options;
//   ORC_UNIQUE_PTR<liborc::Writer> writer =
//     createWriter(*schema, &mem_stream, options);
//   uint64_t batchSize = 1024;
//   ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch =
//     writer->createRowBatch(batchSize);
//   liborc::StructVectorBatch *root =
//     internal::checked_cast<liborc::StructVectorBatch *>(batch.get());
//   liborc::MapVectorBatch *x =
//     internal::checked_cast<liborc::MapVectorBatch *>(root->fields[0]);
//   liborc::StringVectorBatch *a =
//     internal::checked_cast<liborc::StringVectorBatch *>((x->keys).get());
//   liborc::StringVectorBatch *b =
//     internal::checked_cast<liborc::StringVectorBatch *>((x->elements).get());
//   int64_t arrowOffset = 0;
//   int64_t orcOffset = 0;
//   Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize, array.get());
//   if (!st.ok()) {
//     FAIL() << "ORC ColumnBatch not successfully filled";
//   }

//   EXPECT_EQ(x->numElements, 0);
//   EXPECT_FALSE(x->hasNulls);
//   EXPECT_EQ(a->numElements, 0);
//   EXPECT_FALSE(a->hasNulls);
//   EXPECT_EQ(b->numElements, 0);
//   EXPECT_FALSE(b->hasNulls);

//   EXPECT_EQ(x->offsets[0], 0);

//   EXPECT_EQ(arrowOffset, 0);
//   EXPECT_EQ(orcOffset, 0);
//   writer->add(*batch);
//   writer->close();
// }

// TEST(TestAdapterWriteNested, writeMapNoNulls) {
//   auto sharedPtrArrowType = map(utf8(), utf8());
//   DataType* arrowType = sharedPtrArrowType.get();

//   Int32Builder offsetsBuilder;
//   StringBuilder keysBuilder, itemsBuilder;
//   std::shared_ptr<Array> offsetsArray, keysArray, itemsArray;
//   (void)(keysBuilder.Append("AB"));
//   (void)(keysBuilder.Append(""));
//   (void)(keysBuilder.Append("CDE"));
//   (void)(keysBuilder.Append("F C"));
//   (void)(keysBuilder.Append("AA"));
//   (void)(keysBuilder.Finish(&keysArray));
//   (void)(itemsBuilder.Append("CM"));
//   (void)(itemsBuilder.Append("wdw"));
//   (void)(itemsBuilder.Append("dsw7"));
//   (void)(itemsBuilder.Append("872"));
//   (void)(itemsBuilder.Append("dww"));
//   (void)(itemsBuilder.Finish(&itemsArray));
//   (void)(offsetsBuilder.Append(0));
//   (void)(offsetsBuilder.Append(1));
//   (void)(offsetsBuilder.Append(3));
//   (void)(offsetsBuilder.Append(5));
//   (void)(offsetsBuilder.Finish(&offsetsArray));

//   std::shared_ptr<MapArray> array = std::static_pointer_cast<MapArray>(MapArray::FromArrays(offsetsArray, keysArray, itemsArray).ValueOrDie());

//   MemoryOutputStream mem_stream(DEFAULT_MEM_STREAM_SIZE);
//   ORC_UNIQUE_PTR<liborc::Type> schema(liborc::Type::buildTypeFromString("struct<x:map<a:string,b:string>>"));
//   liborc::WriterOptions options;
//   ORC_UNIQUE_PTR<liborc::Writer> writer =
//     createWriter(*schema, &mem_stream, options);
//   uint64_t batchSize = 1024;
//   ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch =
//     writer->createRowBatch(batchSize);
//   liborc::StructVectorBatch *root =
//     internal::checked_cast<liborc::StructVectorBatch *>(batch.get());
//   liborc::MapVectorBatch *x =
//     internal::checked_cast<liborc::MapVectorBatch *>(root->fields[0]);
//   liborc::StringVectorBatch *a =
//     internal::checked_cast<liborc::StringVectorBatch *>((x->keys).get());
//   liborc::StringVectorBatch *b =
//     internal::checked_cast<liborc::StringVectorBatch *>((x->elements).get());
//   int64_t arrowOffset = 0;
//   int64_t orcOffset = 0;
//   Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize, array.get());
//   if (!st.ok()) {
//     FAIL() << "ORC ColumnBatch not successfully filled";
//   }

//   EXPECT_EQ(x->numElements, 3);
//   EXPECT_FALSE(x->hasNulls);
//   EXPECT_EQ(a->numElements, 5);
//   EXPECT_FALSE(a->hasNulls);
//   EXPECT_EQ(b->numElements, 5);
//   EXPECT_FALSE(b->hasNulls);

//   EXPECT_EQ(x->offsets[0], 0);
//   EXPECT_EQ(x->offsets[1], 1);
//   EXPECT_EQ(x->offsets[2], 3);
//   EXPECT_EQ(x->offsets[3], 5);

//   EXPECT_EQ(a->length[0], 2);
//   EXPECT_EQ(a->length[1], 0);
//   EXPECT_EQ(a->length[2], 3);
//   EXPECT_EQ(a->length[3], 3);
//   EXPECT_EQ(a->length[4], 2);
//   EXPECT_EQ(b->length[0], 2);
//   EXPECT_EQ(b->length[1], 3);
//   EXPECT_EQ(b->length[2], 4);
//   EXPECT_EQ(b->length[3], 3);
//   EXPECT_EQ(b->length[4], 3);

//   EXPECT_STREQ(a->data[0], "AB");
//   EXPECT_STREQ(a->data[1], "");
//   EXPECT_STREQ(a->data[2], "CDE");
//   EXPECT_STREQ(a->data[3], "F C");
//   EXPECT_STREQ(a->data[4], "AA");
//   EXPECT_STREQ(b->data[0], "CM");
//   EXPECT_STREQ(b->data[1], "wdw");
//   EXPECT_STREQ(b->data[2], "dsw7");
//   EXPECT_STREQ(b->data[3], "872");
//   EXPECT_STREQ(b->data[4], "dww");

//   EXPECT_EQ(arrowOffset, 3);
//   EXPECT_EQ(orcOffset, 3);
//   writer->add(*batch);
//   writer->close();
// }

// TEST(TestAdapterWriteNested, writeMapMixed1) {
//   auto sharedPtrArrowType = map(utf8(), utf8());
//   DataType* arrowType = sharedPtrArrowType.get();

//   Int32Builder offsetsBuilder;
//   StringBuilder keysBuilder, itemsBuilder;
//   std::shared_ptr<Array> offsetsArray, keysArray, itemsArray;
//   (void)(keysBuilder.Append("AB"));
//   (void)(keysBuilder.Append(""));
//   (void)(keysBuilder.Append("CDE"));
//   (void)(keysBuilder.Append("F C"));
//   (void)(keysBuilder.Append("AA"));
//   (void)(keysBuilder.Finish(&keysArray));
//   (void)(itemsBuilder.AppendNull());
//   (void)(itemsBuilder.Append("wdw"));
//   (void)(itemsBuilder.Append("dsw7"));
//   (void)(itemsBuilder.Append("872"));
//   (void)(itemsBuilder.AppendNull());
//   (void)(itemsBuilder.Finish(&itemsArray));
//   (void)(offsetsBuilder.Append(0));
//   (void)(offsetsBuilder.Append(1));
//   (void)(offsetsBuilder.Append(3));
//   (void)(offsetsBuilder.Append(5));
//   (void)(offsetsBuilder.Finish(&offsetsArray));

//   std::shared_ptr<MapArray> array = std::static_pointer_cast<MapArray>(MapArray::FromArrays(offsetsArray, keysArray, itemsArray).ValueOrDie());

//   MemoryOutputStream mem_stream(DEFAULT_MEM_STREAM_SIZE);
//   ORC_UNIQUE_PTR<liborc::Type> schema(liborc::Type::buildTypeFromString("struct<x:map<a:string,b:string>>"));
//   liborc::WriterOptions options;
//   ORC_UNIQUE_PTR<liborc::Writer> writer =
//     createWriter(*schema, &mem_stream, options);
//   uint64_t batchSize = 1024;
//   ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch =
//     writer->createRowBatch(batchSize);
//   liborc::StructVectorBatch *root =
//     internal::checked_cast<liborc::StructVectorBatch *>(batch.get());
//   liborc::MapVectorBatch *x =
//     internal::checked_cast<liborc::MapVectorBatch *>(root->fields[0]);
//   liborc::StringVectorBatch *a =
//     internal::checked_cast<liborc::StringVectorBatch *>((x->keys).get());
//   liborc::StringVectorBatch *b =
//     internal::checked_cast<liborc::StringVectorBatch *>((x->elements).get());
//   int64_t arrowOffset = 0;
//   int64_t orcOffset = 0;
//   Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize, array.get());
//   if (!st.ok()) {
//     FAIL() << "ORC ColumnBatch not successfully filled";
//   }

//   EXPECT_EQ(x->numElements, 3);
//   EXPECT_FALSE(x->hasNulls);
//   EXPECT_EQ(a->numElements, 5);
//   EXPECT_FALSE(a->hasNulls);
//   EXPECT_EQ(b->numElements, 5);
//   EXPECT_TRUE(b->hasNulls);

//   EXPECT_EQ(b->notNull[0], 0);
//   EXPECT_EQ(b->notNull[1], 1);
//   EXPECT_EQ(b->notNull[2], 1);
//   EXPECT_EQ(b->notNull[3], 1);
//   EXPECT_EQ(b->notNull[4], 0);

//   EXPECT_EQ(x->offsets[0], 0);
//   EXPECT_EQ(x->offsets[1], 1);
//   EXPECT_EQ(x->offsets[2], 3);
//   EXPECT_EQ(x->offsets[3], 5);

//   EXPECT_EQ(a->length[0], 2);
//   EXPECT_EQ(a->length[1], 0);
//   EXPECT_EQ(a->length[2], 3);
//   EXPECT_EQ(a->length[3], 3);
//   EXPECT_EQ(a->length[4], 2);
//   EXPECT_EQ(b->length[1], 3);
//   EXPECT_EQ(b->length[2], 4);
//   EXPECT_EQ(b->length[3], 3);

//   EXPECT_STREQ(a->data[0], "AB");
//   EXPECT_STREQ(a->data[1], "");
//   EXPECT_STREQ(a->data[2], "CDE");
//   EXPECT_STREQ(a->data[3], "F C");
//   EXPECT_STREQ(a->data[4], "AA");
//   EXPECT_STREQ(b->data[1], "wdw");
//   EXPECT_STREQ(b->data[2], "dsw7");
//   EXPECT_STREQ(b->data[3], "872");

//   EXPECT_EQ(arrowOffset, 3);
//   EXPECT_EQ(orcOffset, 3);
//   writer->add(*batch);
//   writer->close();
// }

// TEST(TestAdapterWriteNested, writeMapMixed2) {
//   auto sharedPtrArrowType = map(utf8(), utf8());
//   DataType* arrowType = sharedPtrArrowType.get();

//   StringBuilder keysBuilder, itemsBuilder;
//   std::shared_ptr<Array> keysArray, itemsArray;
  
//   int32_t offsets[4] = {0, 1, 3, 5};

//   BufferBuilder builder;
//   (void)(builder.Resize(16));
//   (void)(builder.Append(offsets, 16));
//   std::shared_ptr<arrow::Buffer> offsetsBuffer;
//   if (!builder.Finish(&offsetsBuffer).ok()) {
//     FAIL() << "The offsets buffer can not be constructed!";
//   }

//   (void)(keysBuilder.Append("AB"));
//   (void)(keysBuilder.Append(""));
//   (void)(keysBuilder.Append("CDE"));
//   (void)(keysBuilder.Append("F C"));
//   (void)(keysBuilder.Append("AA"));
//   (void)(keysBuilder.Finish(&keysArray));
//   (void)(itemsBuilder.AppendNull());
//   (void)(itemsBuilder.Append("wdw"));
//   (void)(itemsBuilder.Append("dsw7"));
//   (void)(itemsBuilder.Append("872"));
//   (void)(itemsBuilder.AppendNull());
//   (void)(itemsBuilder.Finish(&itemsArray));

//   uint8_t bitmap = 5;//00000101
//   auto maybeBuffer = AllocateBuffer(1);
//   if (!maybeBuffer.ok()){
//     FAIL() << "Buffer not created successfully";
//   }
//   std::shared_ptr<Buffer> bitmapBuffer = *std::move(maybeBuffer);
//   uint8_t* bufferData = bitmapBuffer->mutable_data();
//   std::memcpy(bufferData, &bitmap, 1);

//   auto array = std::make_shared<MapArray>(sharedPtrArrowType, 3, offsetsBuffer, keysArray, itemsArray, bitmapBuffer);

//   MemoryOutputStream mem_stream(DEFAULT_MEM_STREAM_SIZE);
//   ORC_UNIQUE_PTR<liborc::Type> schema(liborc::Type::buildTypeFromString("struct<x:map<a:string,b:string>>"));
//   liborc::WriterOptions options;
//   ORC_UNIQUE_PTR<liborc::Writer> writer =
//     createWriter(*schema, &mem_stream, options);
//   uint64_t batchSize = 1024;
//   ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch =
//     writer->createRowBatch(batchSize);
//   liborc::StructVectorBatch *root =
//     internal::checked_cast<liborc::StructVectorBatch *>(batch.get());
//   liborc::MapVectorBatch *x =
//     internal::checked_cast<liborc::MapVectorBatch *>(root->fields[0]);
//   liborc::StringVectorBatch *a =
//     internal::checked_cast<liborc::StringVectorBatch *>((x->keys).get());
//   liborc::StringVectorBatch *b =
//     internal::checked_cast<liborc::StringVectorBatch *>((x->elements).get());
//   int64_t arrowOffset = 0;
//   int64_t orcOffset = 0;
//   Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize, array.get());
//   if (!st.ok()) {
//     FAIL() << "ORC ColumnBatch not successfully filled";
//   }

//   EXPECT_EQ(x->numElements, 3);
//   EXPECT_TRUE(x->hasNulls);
//   EXPECT_EQ(a->numElements, 5);
//   EXPECT_FALSE(a->hasNulls);
//   EXPECT_EQ(b->numElements, 5);
//   EXPECT_TRUE(b->hasNulls);

//   EXPECT_EQ(x->notNull[0], 1);
//   EXPECT_EQ(x->notNull[1], 0);
//   EXPECT_EQ(x->notNull[2], 1);
//   EXPECT_EQ(b->notNull[0], 0);
//   EXPECT_EQ(b->notNull[1], 1);
//   EXPECT_EQ(b->notNull[2], 1);
//   EXPECT_EQ(b->notNull[3], 1);
//   EXPECT_EQ(b->notNull[4], 0);

//   EXPECT_EQ(x->offsets[0], 0);
//   EXPECT_EQ(x->offsets[1], 1);
//   EXPECT_EQ(x->offsets[2], 3);
//   EXPECT_EQ(x->offsets[3], 5);

//   EXPECT_EQ(a->length[0], 2);
//   EXPECT_EQ(a->length[1], 0);
//   EXPECT_EQ(a->length[2], 3);
//   EXPECT_EQ(a->length[3], 3);
//   EXPECT_EQ(a->length[4], 2);
//   EXPECT_EQ(b->length[1], 3);
//   EXPECT_EQ(b->length[2], 4);
//   EXPECT_EQ(b->length[3], 3);

//   EXPECT_STREQ(a->data[0], "AB");
//   EXPECT_STREQ(a->data[1], "");
//   EXPECT_STREQ(a->data[2], "CDE");
//   EXPECT_STREQ(a->data[3], "F C");
//   EXPECT_STREQ(a->data[4], "AA");
//   EXPECT_STREQ(b->data[1], "wdw");
//   EXPECT_STREQ(b->data[2], "dsw7");
//   EXPECT_STREQ(b->data[3], "872");

//   EXPECT_EQ(arrowOffset, 3);
//   EXPECT_EQ(orcOffset, 3);
//   writer->add(*batch);
//   writer->close();
// }

// TEST(TestAdapterWriteNested, writeMapMixed3) {
//   auto sharedPtrArrowType = map(utf8(), utf8());
//   DataType* arrowType = sharedPtrArrowType.get();

//   StringBuilder keysBuilder, itemsBuilder;
//   std::shared_ptr<Array> keysArray, itemsArray;
  
//   int32_t offsets[4] = {0, 1, 3, 3};

//   BufferBuilder builder;
//   (void)(builder.Resize(16));
//   (void)(builder.Append(offsets, 16));
//   std::shared_ptr<arrow::Buffer> offsetsBuffer;
//   if (!builder.Finish(&offsetsBuffer).ok()) {
//     FAIL() << "The offsets buffer can not be constructed!";
//   }

//   (void)(keysBuilder.Append("AB"));
//   (void)(keysBuilder.Append(""));
//   (void)(keysBuilder.Append("CDE"));
//   (void)(keysBuilder.Finish(&keysArray));
//   (void)(itemsBuilder.AppendNull());
//   (void)(itemsBuilder.Append("wdw"));
//   (void)(itemsBuilder.Append("dsw7"));
//   (void)(itemsBuilder.Finish(&itemsArray));

//   uint8_t bitmap = 5;//00000101
//   auto maybeBuffer = AllocateBuffer(1);
//   if (!maybeBuffer.ok()){
//     FAIL() << "Buffer not created successfully";
//   }
//   std::shared_ptr<Buffer> bitmapBuffer = *std::move(maybeBuffer);
//   uint8_t* bufferData = bitmapBuffer->mutable_data();
//   std::memcpy(bufferData, &bitmap, 1);

//   auto array = std::make_shared<MapArray>(sharedPtrArrowType, 3, offsetsBuffer, keysArray, itemsArray, bitmapBuffer);

//   MemoryOutputStream mem_stream(DEFAULT_MEM_STREAM_SIZE);
//   ORC_UNIQUE_PTR<liborc::Type> schema(liborc::Type::buildTypeFromString("struct<x:map<a:string,b:string>>"));
//   liborc::WriterOptions options;
//   ORC_UNIQUE_PTR<liborc::Writer> writer =
//     createWriter(*schema, &mem_stream, options);
//   uint64_t batchSize = 1024;
//   ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch =
//     writer->createRowBatch(batchSize);
//   liborc::StructVectorBatch *root =
//     internal::checked_cast<liborc::StructVectorBatch *>(batch.get());
//   liborc::MapVectorBatch *x =
//     internal::checked_cast<liborc::MapVectorBatch *>(root->fields[0]);
//   liborc::StringVectorBatch *a =
//     internal::checked_cast<liborc::StringVectorBatch *>((x->keys).get());
//   liborc::StringVectorBatch *b =
//     internal::checked_cast<liborc::StringVectorBatch *>((x->elements).get());
//   int64_t arrowOffset = 0;
//   int64_t orcOffset = 0;
//   Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize, array.get());
//   if (!st.ok()) {
//     FAIL() << "ORC ColumnBatch not successfully filled";
//   }

//   EXPECT_EQ(x->numElements, 3);
//   EXPECT_TRUE(x->hasNulls);
//   EXPECT_EQ(a->numElements, 3);
//   EXPECT_FALSE(a->hasNulls);
//   EXPECT_EQ(b->numElements, 3);
//   EXPECT_TRUE(b->hasNulls);

//   EXPECT_EQ(x->notNull[0], 1);
//   EXPECT_EQ(x->notNull[1], 0);
//   EXPECT_EQ(x->notNull[2], 1);
//   EXPECT_EQ(b->notNull[0], 0);
//   EXPECT_EQ(b->notNull[1], 1);
//   EXPECT_EQ(b->notNull[2], 1);

//   EXPECT_EQ(x->offsets[0], 0);
//   EXPECT_EQ(x->offsets[1], 1);
//   EXPECT_EQ(x->offsets[2], 3);
//   EXPECT_EQ(x->offsets[3], 3);

//   EXPECT_EQ(a->length[0], 2);
//   EXPECT_EQ(a->length[1], 0);
//   EXPECT_EQ(a->length[2], 3);
//   EXPECT_EQ(b->length[1], 3);
//   EXPECT_EQ(b->length[2], 4);

//   EXPECT_STREQ(a->data[0], "AB");
//   EXPECT_STREQ(a->data[1], "");
//   EXPECT_STREQ(a->data[2], "CDE");
//   EXPECT_STREQ(b->data[1], "wdw");
//   EXPECT_STREQ(b->data[2], "dsw7");

//   EXPECT_EQ(arrowOffset, 3);
//   EXPECT_EQ(orcOffset, 3);
//   writer->add(*batch);
//   writer->close();
// }

//DenseUnion
TEST(TestAdapterWriteNested, writeDenseUnionEmpty) {

  std::vector<std::shared_ptr<Field>> xFields;
  xFields.push_back(std::make_shared<Field>("a", utf8()));
  xFields.push_back(std::make_shared<Field>("b", int32()));
  auto sharedPtrArrowType = dense_union(xFields);
  DataType* arrowType = sharedPtrArrowType.get();

  StringBuilder builder1;
  Int32Builder builder2;
  std::shared_ptr<Array> array1;
  (void)(builder1.Finish(&array1));
  std::shared_ptr<Array> array2;
  (void)(builder2.Finish(&array2));

  std::vector<std::shared_ptr<Array>> children;
  children.push_back(array1);
  children.push_back(array2);

  int8_t type_ids[2] = {0, 1};

  BufferBuilder builder;
  (void)(builder.Resize(2));
  (void)(builder.Append(type_ids, 2));
  std::shared_ptr<arrow::Buffer> type_idsBuffer;
  if (!builder.Finish(&type_idsBuffer).ok()) {
    FAIL() << "The type_ids buffer can not be constructed!";
  }

  auto array = std::make_shared<DenseUnionArray>(sharedPtrArrowType, 0, children, type_idsBuffer);

  MemoryOutputStream mem_stream(DEFAULT_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(liborc::Type::buildTypeFromString("struct<x:uniontype<a:string,b:int>>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer =
    createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch =
    writer->createRowBatch(batchSize);
  liborc::StructVectorBatch *root =
    internal::checked_cast<liborc::StructVectorBatch *>(batch.get());
  liborc::UnionVectorBatch *x =
    internal::checked_cast<liborc::UnionVectorBatch *>(root->fields[0]);
  liborc::StringVectorBatch *a =
    internal::checked_cast<liborc::StringVectorBatch *>(x->children[0]);
  liborc::LongVectorBatch *b =
    internal::checked_cast<liborc::LongVectorBatch *>(x->children[1]);
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize, array.get());
  if (!st.ok()) {
    FAIL() << "ORC ColumnBatch not successfully filled";
  }
  EXPECT_EQ(x->numElements, 0);
  EXPECT_FALSE(x->hasNulls);
  EXPECT_EQ(a->numElements, 0);
  EXPECT_FALSE(a->hasNulls);
  EXPECT_EQ(b->numElements, 0);
  EXPECT_FALSE(b->hasNulls);

  EXPECT_EQ(arrowOffset, 0);
  EXPECT_EQ(orcOffset, 0);
  writer->add(*batch);
  writer->close();
}
TEST(TestAdapterWriteNested, writeDenseUnionNoNulls) {
  std::vector<std::shared_ptr<Field>> xFields;
  xFields.push_back(std::make_shared<Field>("a", utf8()));
  xFields.push_back(std::make_shared<Field>("b", int32()));
  auto sharedPtrArrowType = dense_union(xFields);
  DataType* arrowType = sharedPtrArrowType.get();

  StringBuilder builder1;
  (void)(builder1.Append("A"));
  (void)(builder1.Append("AB"));
  (void)(builder1.Append(""));
  (void)(builder1.Append("ABCD"));

  Int32Builder builder2;
  (void)(builder2.Append(3));
  (void)(builder2.Append(-12));
  (void)(builder2.Append(25));
  (void)(builder2.Append(76));

  std::shared_ptr<Array> array1;
  (void)(builder1.Finish(&array1));
  std::shared_ptr<Array> array2;
  (void)(builder2.Finish(&array2));

  std::vector<std::shared_ptr<Array>> children;
  children.push_back(array1);
  children.push_back(array2);

  int8_t type_ids[8] = {0, 1, 1, 0, 0, 0, 1, 1};
  int32_t valueOffsets[8] = {0, 0, 1, 1, 2, 3, 2, 3};

  BufferBuilder builder;
  (void)(builder.Resize(8));
  (void)(builder.Append(type_ids, 8));
  std::shared_ptr<arrow::Buffer> type_idsBuffer;
  if (!builder.Finish(&type_idsBuffer).ok()) {
    FAIL() << "The type_ids buffer can not be constructed!";
  }

  BufferBuilder valueOffsetsBuilder;
  (void)(valueOffsetsBuilder.Resize(32));
  (void)(valueOffsetsBuilder.Append(valueOffsets, 32));
  std::shared_ptr<arrow::Buffer> valueOffsetsBuffer;
  if (!valueOffsetsBuilder.Finish(&valueOffsetsBuffer).ok()) {
    FAIL() << "The value_offsets buffer can not be constructed!";
  }

  auto array = std::make_shared<DenseUnionArray>(sharedPtrArrowType, 8, children, type_idsBuffer, valueOffsetsBuffer);

  MemoryOutputStream mem_stream(DEFAULT_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(liborc::Type::buildTypeFromString("struct<x:uniontype<a:string,b:int>>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer =
    createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch =
    writer->createRowBatch(batchSize);
  liborc::StructVectorBatch *root =
    internal::checked_cast<liborc::StructVectorBatch *>(batch.get());
  liborc::UnionVectorBatch *x =
    internal::checked_cast<liborc::UnionVectorBatch *>(root->fields[0]);
  liborc::StringVectorBatch *a =
    internal::checked_cast<liborc::StringVectorBatch *>(x->children[0]);
  liborc::LongVectorBatch *b =
    internal::checked_cast<liborc::LongVectorBatch *>(x->children[1]);
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize, array.get());
  if (!st.ok()) {
    FAIL() << "ORC ColumnBatch not successfully filled";
  }
  EXPECT_EQ(x->numElements, 8);
  EXPECT_FALSE(x->hasNulls);
  EXPECT_EQ(a->numElements, 4);
  EXPECT_FALSE(a->hasNulls);
  EXPECT_EQ(b->numElements, 4);
  EXPECT_FALSE(b->hasNulls);

  EXPECT_EQ(x->offsets[0], 0);
  EXPECT_EQ(x->offsets[1], 0);
  EXPECT_EQ(x->offsets[2], 1);
  EXPECT_EQ(x->offsets[3], 1);
  EXPECT_EQ(x->offsets[4], 2);
  EXPECT_EQ(x->offsets[5], 3);
  EXPECT_EQ(x->offsets[6], 2);
  EXPECT_EQ(x->offsets[7], 3);

  EXPECT_STREQ(a->data[0], "A");
  EXPECT_STREQ(a->data[1], "AB");
  EXPECT_STREQ(a->data[2], "");
  EXPECT_STREQ(a->data[3], "ABCD");
  EXPECT_EQ(a->length[0], 1);
  EXPECT_EQ(a->length[1], 2);
  EXPECT_EQ(a->length[2], 0);
  EXPECT_EQ(a->length[3], 4);

  EXPECT_EQ(b->data[0], 3);
  EXPECT_EQ(b->data[1], -12);
  EXPECT_EQ(b->data[2], 25);
  EXPECT_EQ(b->data[3], 76);

  EXPECT_EQ(arrowOffset, 4);
  EXPECT_EQ(orcOffset, 4);
  writer->add(*batch);
  writer->close();
}
TEST(TestAdapterWriteNested, writeDenseUnionAllNulls) {
  std::vector<std::shared_ptr<Field>> xFields;
  xFields.push_back(std::make_shared<Field>("a", utf8()));
  xFields.push_back(std::make_shared<Field>("b", int32()));
  auto sharedPtrArrowType = dense_union(xFields);
  DataType* arrowType = sharedPtrArrowType.get();

  StringBuilder builder1;
  (void)(builder1.AppendNull());
  (void)(builder1.AppendNull());
  (void)(builder1.AppendNull());
  (void)(builder1.AppendNull());

  Int32Builder builder2;
  (void)(builder2.AppendNull());
  (void)(builder2.AppendNull());
  (void)(builder2.AppendNull());
  (void)(builder2.AppendNull());

  std::shared_ptr<Array> array1;
  (void)(builder1.Finish(&array1));
  std::shared_ptr<Array> array2;
  (void)(builder2.Finish(&array2));

  std::vector<std::shared_ptr<Array>> children;
  children.push_back(array1);
  children.push_back(array2);

  int8_t type_ids[8] = {0, 0, 0, 0, 1, 1, 1, 1};
  int32_t valueOffsets[8] = {0, 1, 2, 3, 0, 1, 2, 3};

  BufferBuilder builder;
  (void)(builder.Resize(8));
  (void)(builder.Append(type_ids, 8));
  std::shared_ptr<arrow::Buffer> type_idsBuffer;
  if (!builder.Finish(&type_idsBuffer).ok()) {
    FAIL() << "The type_ids buffer can not be constructed!";
  }

  BufferBuilder valueOffsetsBuilder;
  (void)(valueOffsetsBuilder.Resize(32));
  (void)(valueOffsetsBuilder.Append(valueOffsets, 32));
  std::shared_ptr<arrow::Buffer> valueOffsetsBuffer;
  if (!valueOffsetsBuilder.Finish(&valueOffsetsBuffer).ok()) {
    FAIL() << "The value_offsets buffer can not be constructed!";
  }

  auto array = std::make_shared<DenseUnionArray>(sharedPtrArrowType, 8, children, type_idsBuffer, valueOffsetsBuffer);

  MemoryOutputStream mem_stream(DEFAULT_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(liborc::Type::buildTypeFromString("struct<x:uniontype<a:string,b:int>>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer =
    createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch =
    writer->createRowBatch(batchSize);
  liborc::StructVectorBatch *root =
    internal::checked_cast<liborc::StructVectorBatch *>(batch.get());
  liborc::UnionVectorBatch *x =
    internal::checked_cast<liborc::UnionVectorBatch *>(root->fields[0]);
  liborc::StringVectorBatch *a =
    internal::checked_cast<liborc::StringVectorBatch *>(x->children[0]);
  liborc::LongVectorBatch *b =
    internal::checked_cast<liborc::LongVectorBatch *>(x->children[1]);
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize, array.get());
  if (!st.ok()) {
    FAIL() << "ORC ColumnBatch not successfully filled";
  }
  EXPECT_EQ(x->numElements, 8);
  EXPECT_FALSE(x->hasNulls);
  EXPECT_EQ(a->numElements, 4);
  EXPECT_TRUE(a->hasNulls);
  EXPECT_EQ(b->numElements, 4);
  EXPECT_TRUE(b->hasNulls);

  EXPECT_EQ(a->notNull[0], 0);
  EXPECT_EQ(a->notNull[1], 0);
  EXPECT_EQ(a->notNull[2], 0);
  EXPECT_EQ(a->notNull[3], 0);
  EXPECT_EQ(b->notNull[0], 0);
  EXPECT_EQ(b->notNull[1], 0);
  EXPECT_EQ(b->notNull[2], 0);
  EXPECT_EQ(b->notNull[3], 0);

  EXPECT_EQ(x->offsets[0], 0);
  EXPECT_EQ(x->offsets[1], 1);
  EXPECT_EQ(x->offsets[2], 2);
  EXPECT_EQ(x->offsets[3], 3);
  EXPECT_EQ(x->offsets[4], 0);
  EXPECT_EQ(x->offsets[5], 1);
  EXPECT_EQ(x->offsets[6], 2);
  EXPECT_EQ(x->offsets[7], 3);

  EXPECT_EQ(arrowOffset, 4);
  EXPECT_EQ(orcOffset, 4);
  writer->add(*batch);
  writer->close();
}
TEST(TestAdapterWriteNested, writeDenseUnionMixed1) {
  std::vector<std::shared_ptr<Field>> xFields;
  xFields.push_back(std::make_shared<Field>("a", utf8()));
  xFields.push_back(std::make_shared<Field>("b", int32()));
  auto sharedPtrArrowType = dense_union(xFields);
  DataType* arrowType = sharedPtrArrowType.get();

  StringBuilder builder1;
  (void)(builder1.Append("A"));
  (void)(builder1.AppendNull());
  (void)(builder1.Append(""));
  (void)(builder1.Append("ABCD"));

  Int32Builder builder2;
  (void)(builder2.Append(3));
  (void)(builder2.Append(-12));
  (void)(builder2.Append(25));
  (void)(builder2.AppendNull());

  std::shared_ptr<Array> array1;
  (void)(builder1.Finish(&array1));
  std::shared_ptr<Array> array2;
  (void)(builder2.Finish(&array2));

  std::vector<std::shared_ptr<Array>> children;
  children.push_back(array1);
  children.push_back(array2);

  int8_t type_ids[8] = {0, 0, 0, 1, 1, 1, 1, 0};
  int32_t valueOffsets[8] = {0, 1, 2, 0, 1, 2, 3, 3};

  BufferBuilder builder;
  (void)(builder.Resize(8));
  (void)(builder.Append(type_ids, 8));
  std::shared_ptr<arrow::Buffer> type_idsBuffer;
  if (!builder.Finish(&type_idsBuffer).ok()) {
    FAIL() << "The type_ids buffer can not be constructed!";
  }

  BufferBuilder valueOffsetsBuilder;
  (void)(valueOffsetsBuilder.Resize(32));
  (void)(valueOffsetsBuilder.Append(valueOffsets, 32));
  std::shared_ptr<arrow::Buffer> valueOffsetsBuffer;
  if (!valueOffsetsBuilder.Finish(&valueOffsetsBuffer).ok()) {
    FAIL() << "The value_offsets buffer can not be constructed!";
  }

  auto array = std::make_shared<DenseUnionArray>(sharedPtrArrowType, 8, children, type_idsBuffer, valueOffsetsBuffer);

  MemoryOutputStream mem_stream(DEFAULT_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(liborc::Type::buildTypeFromString("struct<x:uniontype<a:string,b:int>>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer =
    createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch =
    writer->createRowBatch(batchSize);
  liborc::StructVectorBatch *root =
    internal::checked_cast<liborc::StructVectorBatch *>(batch.get());
  liborc::UnionVectorBatch *x =
    internal::checked_cast<liborc::UnionVectorBatch *>(root->fields[0]);
  liborc::StringVectorBatch *a =
    internal::checked_cast<liborc::StringVectorBatch *>(x->children[0]);
  liborc::LongVectorBatch *b =
    internal::checked_cast<liborc::LongVectorBatch *>(x->children[1]);
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize, array.get());
  if (!st.ok()) {
    FAIL() << "ORC ColumnBatch not successfully filled";
  }
  EXPECT_EQ(x->numElements, 8);
  EXPECT_FALSE(x->hasNulls);
  EXPECT_EQ(a->numElements, 4);
  EXPECT_TRUE(a->hasNulls);
  EXPECT_EQ(b->numElements, 4);
  EXPECT_TRUE(b->hasNulls);

  EXPECT_EQ(a->notNull[0], 1);
  EXPECT_EQ(a->notNull[1], 0);
  EXPECT_EQ(a->notNull[2], 1);
  EXPECT_EQ(a->notNull[3], 1);
  EXPECT_EQ(b->notNull[0], 1);
  EXPECT_EQ(b->notNull[1], 1);
  EXPECT_EQ(b->notNull[2], 1);
  EXPECT_EQ(b->notNull[3], 0);

  EXPECT_EQ(x->offsets[0], 0);
  EXPECT_EQ(x->offsets[1], 1);
  EXPECT_EQ(x->offsets[2], 2);
  EXPECT_EQ(x->offsets[3], 0);
  EXPECT_EQ(x->offsets[4], 1);
  EXPECT_EQ(x->offsets[5], 2);
  EXPECT_EQ(x->offsets[6], 3);
  EXPECT_EQ(x->offsets[7], 3);

  EXPECT_STREQ(a->data[0], "A");
  EXPECT_STREQ(a->data[2], "");
  EXPECT_STREQ(a->data[3], "ABCD");
  EXPECT_EQ(a->length[0], 1);
  EXPECT_EQ(a->length[2], 0);
  EXPECT_EQ(a->length[3], 4);

  EXPECT_EQ(b->data[0], 3);
  EXPECT_EQ(b->data[1], -12);
  EXPECT_EQ(b->data[2], 25);

  EXPECT_EQ(arrowOffset, 4);
  EXPECT_EQ(orcOffset, 4);
  writer->add(*batch);
  writer->close();
}
TEST(TestAdapterWriteNested, writeDenseUnionMixed2) {
  std::vector<std::shared_ptr<Field>> xFields;
  xFields.push_back(std::make_shared<Field>("a", utf8()));
  xFields.push_back(std::make_shared<Field>("b", int32()));
  xFields.push_back(std::make_shared<Field>("c", float64()));
  auto sharedPtrArrowType = dense_union(xFields);
  DataType* arrowType = sharedPtrArrowType.get();

  StringBuilder builder1;
  (void)(builder1.Append("A"));
  (void)(builder1.AppendNull());
  (void)(builder1.Append(""));
  (void)(builder1.Append("ABCD"));

  Int32Builder builder2;
  (void)(builder2.Append(3));
  (void)(builder2.Append(-12));
  (void)(builder2.Append(25));
  (void)(builder2.AppendNull());

  DoubleBuilder builder3;
  (void)(builder3.Append(3.7));
  (void)(builder3.Append(-12.5));
  (void)(builder3.Append(25.4));
  (void)(builder3.AppendNull());

  std::shared_ptr<Array> array1;
  (void)(builder1.Finish(&array1));
  std::shared_ptr<Array> array2;
  (void)(builder2.Finish(&array2));
  std::shared_ptr<Array> array3;
  (void)(builder3.Finish(&array3));

  std::vector<std::shared_ptr<Array>> children;
  children.push_back(array1);
  children.push_back(array2);
  children.push_back(array3);

  int8_t type_ids[12] = {0, 0, 2, 0, 1, 1, 2, 1, 1, 0, 2, 2};
  int32_t valueOffsets[12] = {0, 1, 0, 2, 0, 1, 1, 2, 3, 3, 2, 3};

  BufferBuilder builder;
  (void)(builder.Resize(12));
  (void)(builder.Append(type_ids, 12));
  std::shared_ptr<arrow::Buffer> type_idsBuffer;
  if (!builder.Finish(&type_idsBuffer).ok()) {
    FAIL() << "The type_ids buffer can not be constructed!";
  }

  BufferBuilder valueOffsetsBuilder;
  (void)(valueOffsetsBuilder.Resize(48));
  (void)(valueOffsetsBuilder.Append(valueOffsets, 48));
  std::shared_ptr<arrow::Buffer> valueOffsetsBuffer;
  if (!valueOffsetsBuilder.Finish(&valueOffsetsBuffer).ok()) {
    FAIL() << "The value_offsets buffer can not be constructed!";
  }

  auto array = std::make_shared<DenseUnionArray>(sharedPtrArrowType, 12, children, type_idsBuffer, valueOffsetsBuffer);

  MemoryOutputStream mem_stream(DEFAULT_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(liborc::Type::buildTypeFromString("struct<x:uniontype<a:string,b:int,c:double>>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer =
    createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch =
    writer->createRowBatch(batchSize);
  liborc::StructVectorBatch *root =
    internal::checked_cast<liborc::StructVectorBatch *>(batch.get());
  liborc::UnionVectorBatch *x =
    internal::checked_cast<liborc::UnionVectorBatch *>(root->fields[0]);
  liborc::StringVectorBatch *a =
    internal::checked_cast<liborc::StringVectorBatch *>(x->children[0]);
  liborc::LongVectorBatch *b =
    internal::checked_cast<liborc::LongVectorBatch *>(x->children[1]);
  liborc::DoubleVectorBatch *c =
    internal::checked_cast<liborc::DoubleVectorBatch *>(x->children[2]);
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize, array.get());
  if (!st.ok()) {
    FAIL() << "ORC ColumnBatch not successfully filled";
  }
  EXPECT_EQ(x->numElements, 12);
  EXPECT_FALSE(x->hasNulls);
  EXPECT_EQ(a->numElements, 4);
  EXPECT_TRUE(a->hasNulls);
  EXPECT_EQ(b->numElements, 4);
  EXPECT_TRUE(b->hasNulls);
  EXPECT_EQ(c->numElements, 4);
  EXPECT_TRUE(c->hasNulls);

  EXPECT_EQ(a->notNull[0], 1);
  EXPECT_EQ(a->notNull[1], 0);
  EXPECT_EQ(a->notNull[2], 1);
  EXPECT_EQ(a->notNull[3], 1);
  EXPECT_EQ(b->notNull[0], 1);
  EXPECT_EQ(b->notNull[1], 1);
  EXPECT_EQ(b->notNull[2], 1);
  EXPECT_EQ(b->notNull[3], 0);
  EXPECT_EQ(c->notNull[0], 1);
  EXPECT_EQ(c->notNull[1], 1);
  EXPECT_EQ(c->notNull[2], 1);
  EXPECT_EQ(c->notNull[3], 0);

  EXPECT_EQ(x->offsets[0], 0);
  EXPECT_EQ(x->offsets[1], 1);
  EXPECT_EQ(x->offsets[2], 0);
  EXPECT_EQ(x->offsets[3], 2);
  EXPECT_EQ(x->offsets[4], 0);
  EXPECT_EQ(x->offsets[5], 1);
  EXPECT_EQ(x->offsets[6], 1);
  EXPECT_EQ(x->offsets[7], 2);
  EXPECT_EQ(x->offsets[8], 3);
  EXPECT_EQ(x->offsets[9], 3);
  EXPECT_EQ(x->offsets[10], 2);
  EXPECT_EQ(x->offsets[11], 3);

  EXPECT_STREQ(a->data[0], "A");
  EXPECT_STREQ(a->data[2], "");
  EXPECT_STREQ(a->data[3], "ABCD");
  EXPECT_EQ(a->length[0], 1);
  EXPECT_EQ(a->length[2], 0);
  EXPECT_EQ(a->length[3], 4);

  EXPECT_EQ(b->data[0], 3);
  EXPECT_EQ(b->data[1], -12);
  EXPECT_EQ(b->data[2], 25);

  EXPECT_DOUBLE_EQ(c->data[0], 3.7);
  EXPECT_DOUBLE_EQ(c->data[1], -12.5);
  EXPECT_DOUBLE_EQ(c->data[2], 25.4);

  EXPECT_EQ(arrowOffset, 4);
  EXPECT_EQ(orcOffset, 4);
  writer->add(*batch);
  writer->close();
}

}  // namespace arrow
