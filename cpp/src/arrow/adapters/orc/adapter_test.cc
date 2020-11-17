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
#include "arrow/type.h"
#include "arrow/util/decimal.h"

namespace liborc = orc;

namespace arrow {

constexpr int DEFAULT_MEM_STREAM_SIZE = 10 * 1024 * 1024;
constexpr int DEFAULT_SMALL_MEM_STREAM_SIZE = 16384;

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

class MemoryOutputStreamV2 : public adapters::orc::ArrowOutputFile {
 public:
  explicit MemoryOutputStreamV2(ssize_t capacity)
      : ArrowOutputFile(nullptr), data_(capacity) {}

  void write(const void* buf, size_t size) {
    memcpy(data_.data() + get_length(), buf, size);
    set_length(get_length() + size);
  }

  const std::string& getName() const {
    static const std::string filename("MemoryOutputStreamV2");
    return filename;
  }

  const char* getData() const { return data_.data(); }

  void close() {}

  void reset() { set_length(0); }

 private:
  std::vector<char> data_;
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
// TEST(TestAdapterWriteNumerical, writeBoolEmpty0) {
//   BooleanBuilder builder;
//   std::shared_ptr<Array> array;
//   (void)(builder.Finish(&array));
//   std::shared_ptr<Table> table = std::make_shared<Table>({array},{std::String("a")});
//   MemoryOutputStreamV2 file(DEFAULT_SMALL_MEM_STREAM_SIZE);
//   std::unique_ptr<adapters::orc::ORCFileWriter>* writer;
//   ORCFileWriter::Open(table->schema().get(),
//                     const std::shared_ptr<io::FileOutputStream>& file,
//                     std::shared_ptr<liborc::WriterOptions> options,
//                     std::shared_ptr<ArrowWriterOptions> arrow_options,
//                     std::unique_ptr<ORCFileWriter>* writer
//                     )
// }
// Bool
TEST(TestAdapterWriteNumerical, writeBoolEmpty) {
  BooleanBuilder builder;
  std::shared_ptr<Array> array;
  (void)(builder.Finish(&array));
  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:boolean>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::LongVectorBatch* x =
      internal::checked_cast<liborc::LongVectorBatch*>(root->fields[0]);
  DataType* arrowType = boolean().get();
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize,
                                       array.get());
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
TEST(TestAdapterWriteNumerical, writeBoolNoNulls) {
  BooleanBuilder builder;
  (void)(builder.Append(true));
  (void)(builder.Append(false));
  (void)(builder.Append(false));
  (void)(builder.Append(true));
  std::shared_ptr<Array> array;
  (void)(builder.Finish(&array));
  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:boolean>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::LongVectorBatch* x =
      internal::checked_cast<liborc::LongVectorBatch*>(root->fields[0]);
  DataType* arrowType = boolean().get();
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize,
                                       array.get());
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
TEST(TestAdapterWriteNumerical, writeBoolAllNulls) {
  BooleanBuilder builder;
  (void)(builder.AppendNull());
  (void)(builder.AppendNull());
  (void)(builder.AppendNull());
  std::shared_ptr<Array> array;
  (void)(builder.Finish(&array));
  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:boolean>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::LongVectorBatch* x =
      internal::checked_cast<liborc::LongVectorBatch*>(root->fields[0]);
  DataType* arrowType = boolean().get();
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize,
                                       array.get());
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
TEST(TestAdapterWriteNumerical, writeBooleanMixed) {
  BooleanBuilder builder;
  (void)(builder.AppendNull());
  (void)(builder.Append(true));
  (void)(builder.AppendNull());
  (void)(builder.Append(false));
  (void)(builder.AppendNull());
  std::shared_ptr<Array> array;
  (void)(builder.Finish(&array));
  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:boolean>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::LongVectorBatch* x =
      internal::checked_cast<liborc::LongVectorBatch*>(root->fields[0]);
  DataType* arrowType = boolean().get();
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize,
                                       array.get());
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
TEST(TestAdapterWriteNumerical, writeBoolChunkedEmpty) {
  ArrayVector av;
  std::shared_ptr<ChunkedArray> carray = std::make_shared<ChunkedArray>(av, boolean());
  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:boolean>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::LongVectorBatch* x =
      internal::checked_cast<liborc::LongVectorBatch*>(root->fields[0]);
  DataType* arrowType = boolean().get();
  int64_t arrowIndexOffset = 0;
  int arrowChunkOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowIndexOffset, arrowChunkOffset,
                                       batchSize, carray.get());
  if (!st.ok()) {
    FAIL() << "ORC ColumnBatch not successfully filled";
  }
  EXPECT_EQ(x->numElements, 0);
  EXPECT_FALSE(x->hasNulls);
  EXPECT_EQ(arrowIndexOffset, 0);
  EXPECT_EQ(arrowChunkOffset, 0);
  writer->add(*batch);
  writer->close();
}
TEST(TestAdapterWriteNumerical, writeBooleanChunkedMixed) {
  BooleanBuilder builder0, builder1, builder2, builder3, builder4;
  (void)(builder1.AppendNull());
  (void)(builder1.Append(true));
  (void)(builder2.AppendNull());
  (void)(builder2.Append(false));
  (void)(builder3.AppendNull());
  std::shared_ptr<Array> array0, array1, array2, array3, array4;
  (void)(builder0.Finish(&array0));
  (void)(builder1.Finish(&array1));
  (void)(builder2.Finish(&array2));
  (void)(builder3.Finish(&array3));
  (void)(builder4.Finish(&array4));
  ArrayVector av;
  av.push_back(array0);
  av.push_back(array1);
  av.push_back(array2);
  av.push_back(array3);
  av.push_back(array4);
  std::shared_ptr<ChunkedArray> carray = std::make_shared<ChunkedArray>(av);
  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:boolean>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::LongVectorBatch* x =
      internal::checked_cast<liborc::LongVectorBatch*>(root->fields[0]);
  DataType* arrowType = boolean().get();
  int64_t arrowIndexOffset = 0;
  int arrowChunkOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowIndexOffset, arrowChunkOffset,
                                       batchSize, carray.get());
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
  EXPECT_EQ(arrowIndexOffset, 0);
  EXPECT_EQ(arrowChunkOffset, 5);
  writer->add(*batch);
  writer->close();
}
TEST(TestAdapterWriteNumerical, writeBooleanChunkedMultibatch) {
  uint64_t totalLength = 100;
  BooleanBuilder builder0, builder1, builder2, builder3, builder4;
  for (int i = 0; i < 50; i++) {
    if (i % 2)
      (void)(builder1.Append(true));
    else
      (void)(builder1.AppendNull());
  }
  for (int i = 50; i < 100; i++) {
    if (i % 2)
      (void)(builder3.Append(true));
    else
      (void)(builder3.AppendNull());
  }
  std::shared_ptr<Array> array0, array1, array2, array3, array4;
  (void)(builder0.Finish(&array0));
  (void)(builder1.Finish(&array1));
  (void)(builder2.Finish(&array2));
  (void)(builder3.Finish(&array3));
  (void)(builder4.Finish(&array4));
  ArrayVector av;
  av.push_back(array0);
  av.push_back(array1);
  av.push_back(array2);
  av.push_back(array3);
  av.push_back(array4);
  std::shared_ptr<ChunkedArray> carray = std::make_shared<ChunkedArray>(av);
  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:boolean>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 74;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::LongVectorBatch* x =
      internal::checked_cast<liborc::LongVectorBatch*>(root->fields[0]);
  DataType* arrowType = boolean().get();
  int64_t arrowIndexOffset = 0;
  int arrowChunkOffset = 0;
  uint64_t resultOffset = 0;
  while (resultOffset < totalLength - batchSize) {
    Status st = adapters::orc::FillBatch(arrowType, x, arrowIndexOffset, arrowChunkOffset,
                                         batchSize, carray.get());
    if (!st.ok()) {
      FAIL() << "ORC ColumnBatch not successfully filled";
    }
    writer->add(*batch);
    EXPECT_EQ(x->numElements, batchSize);
    EXPECT_TRUE(x->hasNulls);
    for (uint64_t i = 0; i < batchSize; i++) {
      if ((i + resultOffset) % 2) {
        EXPECT_TRUE(x->notNull[i]);
        EXPECT_EQ(x->data[i], 1);
      } else
        EXPECT_FALSE(x->notNull[i]);
    }
    resultOffset = resultOffset + batchSize;
    batch->clear();
  }
  Status st = adapters::orc::FillBatch(arrowType, x, arrowIndexOffset, arrowChunkOffset,
                                       batchSize, carray.get());
  uint64_t lastBatchSize = totalLength - resultOffset;
  EXPECT_EQ(x->numElements, lastBatchSize);
  EXPECT_TRUE(x->hasNulls);
  for (uint64_t i = 0; i < lastBatchSize; i++) {
    if ((i + resultOffset) % 2) {
      EXPECT_TRUE(x->notNull[i]);
      EXPECT_EQ(x->data[i], 1);
    } else
      EXPECT_FALSE(x->notNull[i]);
  }
  writer->add(*batch);
  writer->close();
}

// Int8
TEST(TestAdapterWriteNumerical, writeInt8Empty) {
  Int8Builder builder;
  std::shared_ptr<Array> array;
  (void)(builder.Finish(&array));
  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:tinyint>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::LongVectorBatch* x =
      internal::checked_cast<liborc::LongVectorBatch*>(root->fields[0]);
  DataType* arrowType = int8().get();
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize,
                                       array.get());
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
TEST(TestAdapterWriteNumerical, writeInt8NoNulls) {
  Int8Builder builder;
  (void)(builder.Append(1));
  (void)(builder.Append(2));
  (void)(builder.Append(3));
  (void)(builder.Append(4));
  std::shared_ptr<Array> array;
  (void)(builder.Finish(&array));
  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:tinyint>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::LongVectorBatch* x =
      internal::checked_cast<liborc::LongVectorBatch*>(root->fields[0]);
  DataType* arrowType = int8().get();
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize,
                                       array.get());
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
TEST(TestAdapterWriteNumerical, writeInt8NoNulls2) {
  Int8Builder builder;
  (void)(builder.Append(1));
  (void)(builder.Append(2));
  (void)(builder.Append(3));
  (void)(builder.Append(4));
  std::shared_ptr<Array> array;
  (void)(builder.Finish(&array));
  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:tinyint>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::LongVectorBatch* x =
      internal::checked_cast<liborc::LongVectorBatch*>(root->fields[0]);
  DataType* arrowType = int8().get();
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  for (uint64_t i = 0; i < 4; i++) {
    Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize,
                                         array.get());
    if (!st.ok()) {
      FAIL() << "ORC ColumnBatch not successfully filled";
    }
    EXPECT_EQ(x->numElements, 1);
    EXPECT_FALSE(x->hasNulls);
    EXPECT_EQ(x->data[0], i + 1);
    EXPECT_EQ(arrowOffset, i + 1);
    EXPECT_EQ(orcOffset, 1);
    orcOffset = 0;
    writer->add(*batch);
    batch->clear();
  }
  writer->close();
}
TEST(TestAdapterWriteNumerical, writeInt8NoNulls3) {
  Int8Builder builder;
  (void)(builder.Append(1));
  (void)(builder.Append(2));
  (void)(builder.Append(3));
  (void)(builder.Append(4));
  std::shared_ptr<Array> array;
  (void)(builder.Finish(&array));
  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:tinyint>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 3;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::LongVectorBatch* x =
      internal::checked_cast<liborc::LongVectorBatch*>(root->fields[0]);
  DataType* arrowType = int8().get();
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize,
                                       array.get());
  if (!st.ok()) {
    FAIL() << "ORC ColumnBatch not successfully filled";
  }
  EXPECT_EQ(x->numElements, 3);
  EXPECT_FALSE(x->hasNulls);
  EXPECT_EQ(x->data[0], 1);
  EXPECT_EQ(x->data[1], 2);
  EXPECT_EQ(x->data[2], 3);
  EXPECT_EQ(arrowOffset, 3);
  EXPECT_EQ(orcOffset, 3);
  orcOffset = 0;
  writer->add(*batch);
  batch->clear();
  st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize,
                                array.get());
  if (!st.ok()) {
    FAIL() << "ORC ColumnBatch not successfully filled";
  }
  EXPECT_EQ(x->numElements, 1);
  EXPECT_FALSE(x->hasNulls);
  EXPECT_EQ(x->data[0], 4);
  EXPECT_EQ(arrowOffset, 4);
  EXPECT_EQ(orcOffset, 1);
  writer->add(*batch);
  batch->clear();
  writer->close();
}
TEST(TestAdapterWriteNumerical, writeInt8AllNulls) {
  Int8Builder builder;
  (void)(builder.AppendNull());
  (void)(builder.AppendNull());
  (void)(builder.AppendNull());
  std::shared_ptr<Array> array;
  (void)(builder.Finish(&array));
  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:tinyint>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::LongVectorBatch* x =
      internal::checked_cast<liborc::LongVectorBatch*>(root->fields[0]);
  DataType* arrowType = int8().get();
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize,
                                       array.get());
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
TEST(TestAdapterWriteNumerical, writeInt8Mixed) {
  Int8Builder builder;
  (void)(builder.AppendNull());
  (void)(builder.Append(1));
  (void)(builder.AppendNull());
  (void)(builder.Append(2));
  (void)(builder.AppendNull());
  std::shared_ptr<Array> array;
  (void)(builder.Finish(&array));
  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:tinyint>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::LongVectorBatch* x =
      internal::checked_cast<liborc::LongVectorBatch*>(root->fields[0]);
  DataType* arrowType = int8().get();
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize,
                                       array.get());
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
TEST(TestAdapterWriteNumerical, writeInt8ChunkedEmpty) {
  ArrayVector av;
  std::shared_ptr<ChunkedArray> carray = std::make_shared<ChunkedArray>(av, int8());
  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:tinyint>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::LongVectorBatch* x =
      internal::checked_cast<liborc::LongVectorBatch*>(root->fields[0]);
  DataType* arrowType = int8().get();
  int64_t arrowIndexOffset = 0;
  int arrowChunkOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowIndexOffset, arrowChunkOffset,
                                       batchSize, carray.get());
  if (!st.ok()) {
    FAIL() << "ORC ColumnBatch not successfully filled";
  }
  EXPECT_EQ(x->numElements, 0);
  EXPECT_FALSE(x->hasNulls);
  EXPECT_EQ(arrowIndexOffset, 0);
  EXPECT_EQ(arrowChunkOffset, 0);
  writer->add(*batch);
  writer->close();
}
TEST(TestAdapterWriteNumerical, writeInt8ChunkedMixed) {
  Int8Builder builder0, builder1, builder2, builder3, builder4;
  (void)(builder1.AppendNull());
  (void)(builder1.Append(1));
  (void)(builder2.AppendNull());
  (void)(builder2.Append(2));
  (void)(builder3.AppendNull());
  std::shared_ptr<Array> array0, array1, array2, array3, array4;
  (void)(builder0.Finish(&array0));
  (void)(builder1.Finish(&array1));
  (void)(builder2.Finish(&array2));
  (void)(builder3.Finish(&array3));
  (void)(builder4.Finish(&array4));
  ArrayVector av;
  av.push_back(array0);
  av.push_back(array1);
  av.push_back(array2);
  av.push_back(array3);
  av.push_back(array4);
  std::shared_ptr<ChunkedArray> carray = std::make_shared<ChunkedArray>(av);
  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:tinyint>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::LongVectorBatch* x =
      internal::checked_cast<liborc::LongVectorBatch*>(root->fields[0]);
  DataType* arrowType = int8().get();
  int64_t arrowIndexOffset = 0;
  int arrowChunkOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowIndexOffset, arrowChunkOffset,
                                       batchSize, carray.get());
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
  EXPECT_EQ(arrowIndexOffset, 0);
  EXPECT_EQ(arrowChunkOffset, 5);
  writer->add(*batch);
  writer->close();
}
TEST(TestAdapterWriteNumerical, writeInt8ChunkedMultibatch) {
  uint64_t totalLength = 100;
  Int8Builder builder0, builder1, builder2, builder3, builder4;
  for (int i = 0; i < 50; i++) {
    if (i % 2)
      (void)(builder1.Append(i));
    else
      (void)(builder1.AppendNull());
  }
  for (int i = 50; i < 100; i++) {
    if (i % 2)
      (void)(builder3.Append(i));
    else
      (void)(builder3.AppendNull());
  }
  std::shared_ptr<Array> array0, array1, array2, array3, array4;
  (void)(builder0.Finish(&array0));
  (void)(builder1.Finish(&array1));
  (void)(builder2.Finish(&array2));
  (void)(builder3.Finish(&array3));
  (void)(builder4.Finish(&array4));
  ArrayVector av;
  av.push_back(array0);
  av.push_back(array1);
  av.push_back(array2);
  av.push_back(array3);
  av.push_back(array4);
  std::shared_ptr<ChunkedArray> carray = std::make_shared<ChunkedArray>(av);
  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:tinyint>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 7;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::LongVectorBatch* x =
      internal::checked_cast<liborc::LongVectorBatch*>(root->fields[0]);
  DataType* arrowType = int8().get();
  int64_t arrowIndexOffset = 0;
  int arrowChunkOffset = 0;
  uint64_t resultOffset = 0;
  while (resultOffset < totalLength - batchSize) {
    Status st = adapters::orc::FillBatch(arrowType, x, arrowIndexOffset, arrowChunkOffset,
                                         batchSize, carray.get());
    if (!st.ok()) {
      FAIL() << "ORC ColumnBatch not successfully filled";
    }
    writer->add(*batch);
    EXPECT_EQ(x->numElements, batchSize);
    EXPECT_TRUE(x->hasNulls);
    for (uint64_t i = 0; i < batchSize; i++) {
      if ((i + resultOffset) % 2) {
        EXPECT_TRUE(x->notNull[i]);
        EXPECT_EQ(x->data[i], i + resultOffset);
      } else
        EXPECT_FALSE(x->notNull[i]);
    }
    resultOffset = resultOffset + batchSize;
    batch->clear();
  }
  Status st = adapters::orc::FillBatch(arrowType, x, arrowIndexOffset, arrowChunkOffset,
                                       batchSize, carray.get());
  uint64_t lastBatchSize = totalLength - resultOffset;
  EXPECT_EQ(x->numElements, lastBatchSize);
  EXPECT_TRUE(x->hasNulls);
  for (uint64_t i = 0; i < lastBatchSize; i++) {
    if ((i + resultOffset) % 2) {
      EXPECT_TRUE(x->notNull[i]);
      EXPECT_EQ(x->data[i], i + resultOffset);
    } else
      EXPECT_FALSE(x->notNull[i]);
  }
  writer->add(*batch);
  writer->close();
}

// Int16
TEST(TestAdapterWriteNumerical, writeInt16Empty) {
  Int16Builder builder;
  std::shared_ptr<Array> array;
  (void)(builder.Finish(&array));
  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:smallint>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::LongVectorBatch* x =
      internal::checked_cast<liborc::LongVectorBatch*>(root->fields[0]);
  DataType* arrowType = int16().get();
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize,
                                       array.get());
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
TEST(TestAdapterWriteNumerical, writeInt16NoNulls) {
  Int16Builder builder;
  (void)(builder.Append(1));
  (void)(builder.Append(2));
  (void)(builder.Append(3));
  (void)(builder.Append(4));
  std::shared_ptr<Array> array;
  (void)(builder.Finish(&array));
  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:smallint>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::LongVectorBatch* x =
      internal::checked_cast<liborc::LongVectorBatch*>(root->fields[0]);
  DataType* arrowType = int16().get();
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize,
                                       array.get());
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
TEST(TestAdapterWriteNumerical, writeInt16AllNulls) {
  Int16Builder builder;
  (void)(builder.AppendNull());
  (void)(builder.AppendNull());
  (void)(builder.AppendNull());
  std::shared_ptr<Array> array;
  (void)(builder.Finish(&array));
  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:smallint>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::LongVectorBatch* x =
      internal::checked_cast<liborc::LongVectorBatch*>(root->fields[0]);
  DataType* arrowType = int16().get();
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize,
                                       array.get());
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
TEST(TestAdapterWriteNumerical, writeInt16Mixed) {
  Int16Builder builder;
  (void)(builder.AppendNull());
  (void)(builder.Append(1));
  (void)(builder.AppendNull());
  (void)(builder.Append(2));
  (void)(builder.AppendNull());
  std::shared_ptr<Array> array;
  (void)(builder.Finish(&array));
  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:smallint>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::LongVectorBatch* x =
      internal::checked_cast<liborc::LongVectorBatch*>(root->fields[0]);
  DataType* arrowType = int16().get();
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize,
                                       array.get());
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
TEST(TestAdapterWriteNumerical, writeInt16ChunkedEmpty) {
  ArrayVector av;
  std::shared_ptr<ChunkedArray> carray = std::make_shared<ChunkedArray>(av, int16());
  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:smallint>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::LongVectorBatch* x =
      internal::checked_cast<liborc::LongVectorBatch*>(root->fields[0]);
  DataType* arrowType = int16().get();
  int64_t arrowIndexOffset = 0;
  int arrowChunkOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowIndexOffset, arrowChunkOffset,
                                       batchSize, carray.get());
  if (!st.ok()) {
    FAIL() << "ORC ColumnBatch not successfully filled";
  }
  EXPECT_EQ(x->numElements, 0);
  EXPECT_FALSE(x->hasNulls);
  EXPECT_EQ(arrowIndexOffset, 0);
  EXPECT_EQ(arrowChunkOffset, 0);
  writer->add(*batch);
  writer->close();
}
TEST(TestAdapterWriteNumerical, writeInt16ChunkedMixed) {
  Int16Builder builder0, builder1, builder2, builder3, builder4;
  (void)(builder1.AppendNull());
  (void)(builder1.Append(1));
  (void)(builder2.AppendNull());
  (void)(builder2.Append(2));
  (void)(builder3.AppendNull());
  std::shared_ptr<Array> array0, array1, array2, array3, array4;
  (void)(builder0.Finish(&array0));
  (void)(builder1.Finish(&array1));
  (void)(builder2.Finish(&array2));
  (void)(builder3.Finish(&array3));
  (void)(builder4.Finish(&array4));
  ArrayVector av;
  av.push_back(array0);
  av.push_back(array1);
  av.push_back(array2);
  av.push_back(array3);
  av.push_back(array4);
  std::shared_ptr<ChunkedArray> carray = std::make_shared<ChunkedArray>(av);
  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:smallint>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::LongVectorBatch* x =
      internal::checked_cast<liborc::LongVectorBatch*>(root->fields[0]);
  DataType* arrowType = int16().get();
  int64_t arrowIndexOffset = 0;
  int arrowChunkOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowIndexOffset, arrowChunkOffset,
                                       batchSize, carray.get());
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
  EXPECT_EQ(arrowIndexOffset, 0);
  EXPECT_EQ(arrowChunkOffset, 5);
  writer->add(*batch);
  writer->close();
}
TEST(TestAdapterWriteNumerical, writeInt16ChunkedMultibatch) {
  uint64_t totalLength = 100;
  Int16Builder builder0, builder1, builder2, builder3, builder4;
  for (int i = 0; i < 50; i++) {
    if (i % 2)
      (void)(builder1.Append(i));
    else
      (void)(builder1.AppendNull());
  }
  for (int i = 50; i < 100; i++) {
    if (i % 2)
      (void)(builder3.Append(i));
    else
      (void)(builder3.AppendNull());
  }
  std::shared_ptr<Array> array0, array1, array2, array3, array4;
  (void)(builder0.Finish(&array0));
  (void)(builder1.Finish(&array1));
  (void)(builder2.Finish(&array2));
  (void)(builder3.Finish(&array3));
  (void)(builder4.Finish(&array4));
  ArrayVector av;
  av.push_back(array0);
  av.push_back(array1);
  av.push_back(array2);
  av.push_back(array3);
  av.push_back(array4);
  std::shared_ptr<ChunkedArray> carray = std::make_shared<ChunkedArray>(av);
  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:smallint>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 12;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::LongVectorBatch* x =
      internal::checked_cast<liborc::LongVectorBatch*>(root->fields[0]);
  DataType* arrowType = int16().get();
  int64_t arrowIndexOffset = 0;
  int arrowChunkOffset = 0;
  uint64_t resultOffset = 0;
  while (resultOffset < totalLength - batchSize) {
    Status st = adapters::orc::FillBatch(arrowType, x, arrowIndexOffset, arrowChunkOffset,
                                         batchSize, carray.get());
    if (!st.ok()) {
      FAIL() << "ORC ColumnBatch not successfully filled";
    }
    writer->add(*batch);
    EXPECT_EQ(x->numElements, batchSize);
    EXPECT_TRUE(x->hasNulls);
    for (uint64_t i = 0; i < batchSize; i++) {
      if ((i + resultOffset) % 2) {
        EXPECT_TRUE(x->notNull[i]);
        EXPECT_EQ(x->data[i], i + resultOffset);
      } else
        EXPECT_FALSE(x->notNull[i]);
    }
    resultOffset = resultOffset + batchSize;
    batch->clear();
  }
  Status st = adapters::orc::FillBatch(arrowType, x, arrowIndexOffset, arrowChunkOffset,
                                       batchSize, carray.get());
  uint64_t lastBatchSize = totalLength - resultOffset;
  EXPECT_EQ(x->numElements, lastBatchSize);
  EXPECT_TRUE(x->hasNulls);
  for (uint64_t i = 0; i < lastBatchSize; i++) {
    if ((i + resultOffset) % 2) {
      EXPECT_TRUE(x->notNull[i]);
      EXPECT_EQ(x->data[i], i + resultOffset);
    } else
      EXPECT_FALSE(x->notNull[i]);
  }
  writer->add(*batch);
  writer->close();
}

// Int32
TEST(TestAdapterWriteNumerical, writeInt32Empty) {
  Int32Builder builder;
  std::shared_ptr<Array> array;
  (void)(builder.Finish(&array));
  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(liborc::Type::buildTypeFromString("struct<x:int>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::LongVectorBatch* x =
      internal::checked_cast<liborc::LongVectorBatch*>(root->fields[0]);
  DataType* arrowType = int32().get();
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize,
                                       array.get());
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
TEST(TestAdapterWriteNumerical, writeInt32NoNulls) {
  Int32Builder builder;
  (void)(builder.Append(1));
  (void)(builder.Append(2));
  (void)(builder.Append(3));
  (void)(builder.Append(4));
  std::shared_ptr<Array> array;
  (void)(builder.Finish(&array));
  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(liborc::Type::buildTypeFromString("struct<x:int>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::LongVectorBatch* x =
      internal::checked_cast<liborc::LongVectorBatch*>(root->fields[0]);
  DataType* arrowType = int32().get();
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize,
                                       array.get());
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
TEST(TestAdapterWriteNumerical, writeInt32AllNulls) {
  Int32Builder builder;
  (void)(builder.AppendNull());
  (void)(builder.AppendNull());
  (void)(builder.AppendNull());
  std::shared_ptr<Array> array;
  (void)(builder.Finish(&array));
  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(liborc::Type::buildTypeFromString("struct<x:int>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::LongVectorBatch* x =
      internal::checked_cast<liborc::LongVectorBatch*>(root->fields[0]);
  DataType* arrowType = int32().get();
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize,
                                       array.get());
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
TEST(TestAdapterWriteNumerical, writeInt32Mixed) {
  Int32Builder builder;
  (void)(builder.AppendNull());
  (void)(builder.Append(1));
  (void)(builder.AppendNull());
  (void)(builder.Append(2));
  (void)(builder.AppendNull());
  std::shared_ptr<Array> array;
  (void)(builder.Finish(&array));
  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(liborc::Type::buildTypeFromString("struct<x:int>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::LongVectorBatch* x =
      internal::checked_cast<liborc::LongVectorBatch*>(root->fields[0]);
  DataType* arrowType = int32().get();
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize,
                                       array.get());
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
TEST(TestAdapterWriteNumerical, writeInt32ChunkedEmpty) {
  ArrayVector av;
  std::shared_ptr<ChunkedArray> carray = std::make_shared<ChunkedArray>(av, int32());
  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(liborc::Type::buildTypeFromString("struct<x:int>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::LongVectorBatch* x =
      internal::checked_cast<liborc::LongVectorBatch*>(root->fields[0]);
  DataType* arrowType = int32().get();
  int64_t arrowIndexOffset = 0;
  int arrowChunkOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowIndexOffset, arrowChunkOffset,
                                       batchSize, carray.get());
  if (!st.ok()) {
    FAIL() << "ORC ColumnBatch not successfully filled";
  }
  EXPECT_EQ(x->numElements, 0);
  EXPECT_FALSE(x->hasNulls);
  EXPECT_EQ(arrowIndexOffset, 0);
  EXPECT_EQ(arrowChunkOffset, 0);
  writer->add(*batch);
  writer->close();
}
TEST(TestAdapterWriteNumerical, writeInt32ChunkedMixed) {
  Int32Builder builder0, builder1, builder2, builder3, builder4;
  (void)(builder1.AppendNull());
  (void)(builder1.Append(1));
  (void)(builder2.AppendNull());
  (void)(builder2.Append(2));
  (void)(builder3.AppendNull());
  std::shared_ptr<Array> array0, array1, array2, array3, array4;
  (void)(builder0.Finish(&array0));
  (void)(builder1.Finish(&array1));
  (void)(builder2.Finish(&array2));
  (void)(builder3.Finish(&array3));
  (void)(builder4.Finish(&array4));
  ArrayVector av;
  av.push_back(array0);
  av.push_back(array1);
  av.push_back(array2);
  av.push_back(array3);
  av.push_back(array4);
  std::shared_ptr<ChunkedArray> carray = std::make_shared<ChunkedArray>(av);
  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(liborc::Type::buildTypeFromString("struct<x:int>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::LongVectorBatch* x =
      internal::checked_cast<liborc::LongVectorBatch*>(root->fields[0]);
  DataType* arrowType = int32().get();
  int64_t arrowIndexOffset = 0;
  int arrowChunkOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowIndexOffset, arrowChunkOffset,
                                       batchSize, carray.get());
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
  EXPECT_EQ(arrowIndexOffset, 0);
  EXPECT_EQ(arrowChunkOffset, 5);
  writer->add(*batch);
  writer->close();
}
TEST(TestAdapterWriteNumerical, writeInt32ChunkedMultibatch) {
  uint64_t totalLength = 100;
  Int32Builder builder0, builder1, builder2, builder3, builder4;
  for (int i = 0; i < 50; i++) {
    if (i % 2)
      (void)(builder1.Append(i));
    else
      (void)(builder1.AppendNull());
  }
  for (int i = 50; i < 100; i++) {
    if (i % 2)
      (void)(builder3.Append(i));
    else
      (void)(builder3.AppendNull());
  }
  std::shared_ptr<Array> array0, array1, array2, array3, array4;
  (void)(builder0.Finish(&array0));
  (void)(builder1.Finish(&array1));
  (void)(builder2.Finish(&array2));
  (void)(builder3.Finish(&array3));
  (void)(builder4.Finish(&array4));
  ArrayVector av;
  av.push_back(array0);
  av.push_back(array1);
  av.push_back(array2);
  av.push_back(array3);
  av.push_back(array4);
  std::shared_ptr<ChunkedArray> carray = std::make_shared<ChunkedArray>(av);
  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(liborc::Type::buildTypeFromString("struct<x:int>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 99;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::LongVectorBatch* x =
      internal::checked_cast<liborc::LongVectorBatch*>(root->fields[0]);
  DataType* arrowType = int32().get();
  int64_t arrowIndexOffset = 0;
  int arrowChunkOffset = 0;
  uint64_t resultOffset = 0;
  while (resultOffset < totalLength - batchSize) {
    Status st = adapters::orc::FillBatch(arrowType, x, arrowIndexOffset, arrowChunkOffset,
                                         batchSize, carray.get());
    if (!st.ok()) {
      FAIL() << "ORC ColumnBatch not successfully filled";
    }
    writer->add(*batch);
    EXPECT_EQ(x->numElements, batchSize);
    EXPECT_TRUE(x->hasNulls);
    for (uint64_t i = 0; i < batchSize; i++) {
      if ((i + resultOffset) % 2) {
        EXPECT_TRUE(x->notNull[i]);
        EXPECT_EQ(x->data[i], i + resultOffset);
      } else
        EXPECT_FALSE(x->notNull[i]);
    }
    resultOffset = resultOffset + batchSize;
    batch->clear();
  }
  Status st = adapters::orc::FillBatch(arrowType, x, arrowIndexOffset, arrowChunkOffset,
                                       batchSize, carray.get());
  uint64_t lastBatchSize = totalLength - resultOffset;
  EXPECT_EQ(x->numElements, lastBatchSize);
  EXPECT_TRUE(x->hasNulls);
  for (uint64_t i = 0; i < lastBatchSize; i++) {
    if ((i + resultOffset) % 2) {
      EXPECT_TRUE(x->notNull[i]);
      EXPECT_EQ(x->data[i], i + resultOffset);
    } else
      EXPECT_FALSE(x->notNull[i]);
  }
  writer->add(*batch);
  writer->close();
}

// Int64
TEST(TestAdapterWriteNumerical, writeInt64Empty) {
  Int64Builder builder;
  std::shared_ptr<Array> array;
  (void)(builder.Finish(&array));
  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:bigint>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::LongVectorBatch* x =
      internal::checked_cast<liborc::LongVectorBatch*>(root->fields[0]);
  DataType* arrowType = int64().get();
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize,
                                       array.get());
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
TEST(TestAdapterWriteNumerical, writeInt64NoNulls) {
  Int64Builder builder;
  (void)(builder.Append(1));
  (void)(builder.Append(2));
  (void)(builder.Append(3));
  (void)(builder.Append(4));
  std::shared_ptr<Array> array;
  (void)(builder.Finish(&array));
  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:bigint>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::LongVectorBatch* x =
      internal::checked_cast<liborc::LongVectorBatch*>(root->fields[0]);
  DataType* arrowType = int64().get();
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize,
                                       array.get());
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
TEST(TestAdapterWriteNumerical, writeInt64AllNulls) {
  Int64Builder builder;
  (void)(builder.AppendNull());
  (void)(builder.AppendNull());
  (void)(builder.AppendNull());
  std::shared_ptr<Array> array;
  (void)(builder.Finish(&array));
  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:bigint>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::LongVectorBatch* x =
      internal::checked_cast<liborc::LongVectorBatch*>(root->fields[0]);
  DataType* arrowType = int64().get();
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize,
                                       array.get());
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
TEST(TestAdapterWriteNumerical, writeInt64Mixed) {
  Int64Builder builder;
  (void)(builder.AppendNull());
  (void)(builder.Append(1));
  (void)(builder.AppendNull());
  (void)(builder.Append(2));
  (void)(builder.AppendNull());
  std::shared_ptr<Array> array;
  (void)(builder.Finish(&array));
  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:bigint>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::LongVectorBatch* x =
      internal::checked_cast<liborc::LongVectorBatch*>(root->fields[0]);
  DataType* arrowType = int64().get();
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize,
                                       array.get());
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
TEST(TestAdapterWriteNumerical, writeInt64ChunkedEmpty) {
  ArrayVector av;
  std::shared_ptr<ChunkedArray> carray = std::make_shared<ChunkedArray>(av, int64());
  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:bigint>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::LongVectorBatch* x =
      internal::checked_cast<liborc::LongVectorBatch*>(root->fields[0]);
  DataType* arrowType = int64().get();
  int64_t arrowIndexOffset = 0;
  int arrowChunkOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowIndexOffset, arrowChunkOffset,
                                       batchSize, carray.get());
  if (!st.ok()) {
    FAIL() << "ORC ColumnBatch not successfully filled";
  }
  EXPECT_EQ(x->numElements, 0);
  EXPECT_FALSE(x->hasNulls);
  EXPECT_EQ(arrowIndexOffset, 0);
  EXPECT_EQ(arrowChunkOffset, 0);
  writer->add(*batch);
  writer->close();
}
TEST(TestAdapterWriteNumerical, writeInt64ChunkedMixed) {
  Int64Builder builder0, builder1, builder2, builder3, builder4;
  (void)(builder1.AppendNull());
  (void)(builder1.Append(1));
  (void)(builder2.AppendNull());
  (void)(builder2.Append(2));
  (void)(builder3.AppendNull());
  std::shared_ptr<Array> array0, array1, array2, array3, array4;
  (void)(builder0.Finish(&array0));
  (void)(builder1.Finish(&array1));
  (void)(builder2.Finish(&array2));
  (void)(builder3.Finish(&array3));
  (void)(builder4.Finish(&array4));
  ArrayVector av;
  av.push_back(array0);
  av.push_back(array1);
  av.push_back(array2);
  av.push_back(array3);
  av.push_back(array4);
  std::shared_ptr<ChunkedArray> carray = std::make_shared<ChunkedArray>(av);
  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:bigint>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::LongVectorBatch* x =
      internal::checked_cast<liborc::LongVectorBatch*>(root->fields[0]);
  DataType* arrowType = int64().get();
  int64_t arrowIndexOffset = 0;
  int arrowChunkOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowIndexOffset, arrowChunkOffset,
                                       batchSize, carray.get());
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
  EXPECT_EQ(arrowIndexOffset, 0);
  EXPECT_EQ(arrowChunkOffset, 5);
  writer->add(*batch);
  writer->close();
}
TEST(TestAdapterWriteNumerical, writeInt64ChunkedMultibatch) {
  uint64_t totalLength = 100;
  Int64Builder builder0, builder1, builder2, builder3, builder4;
  for (int i = 0; i < 50; i++) {
    if (i % 2)
      (void)(builder1.Append(i));
    else
      (void)(builder1.AppendNull());
  }
  for (int i = 50; i < 100; i++) {
    if (i % 2)
      (void)(builder3.Append(i));
    else
      (void)(builder3.AppendNull());
  }
  std::shared_ptr<Array> array0, array1, array2, array3, array4;
  (void)(builder0.Finish(&array0));
  (void)(builder1.Finish(&array1));
  (void)(builder2.Finish(&array2));
  (void)(builder3.Finish(&array3));
  (void)(builder4.Finish(&array4));
  ArrayVector av;
  av.push_back(array0);
  av.push_back(array1);
  av.push_back(array2);
  av.push_back(array3);
  av.push_back(array4);
  std::shared_ptr<ChunkedArray> carray = std::make_shared<ChunkedArray>(av);
  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:bigint>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 100;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::LongVectorBatch* x =
      internal::checked_cast<liborc::LongVectorBatch*>(root->fields[0]);
  DataType* arrowType = int64().get();
  int64_t arrowIndexOffset = 0;
  int arrowChunkOffset = 0;
  uint64_t resultOffset = 0;
  while (resultOffset < totalLength - batchSize) {
    Status st = adapters::orc::FillBatch(arrowType, x, arrowIndexOffset, arrowChunkOffset,
                                         batchSize, carray.get());
    if (!st.ok()) {
      FAIL() << "ORC ColumnBatch not successfully filled";
    }
    writer->add(*batch);
    EXPECT_EQ(x->numElements, batchSize);
    EXPECT_TRUE(x->hasNulls);
    for (uint64_t i = 0; i < batchSize; i++) {
      if ((i + resultOffset) % 2) {
        EXPECT_TRUE(x->notNull[i]);
        EXPECT_EQ(x->data[i], i + resultOffset);
      } else
        EXPECT_FALSE(x->notNull[i]);
    }
    resultOffset = resultOffset + batchSize;
    batch->clear();
  }
  Status st = adapters::orc::FillBatch(arrowType, x, arrowIndexOffset, arrowChunkOffset,
                                       batchSize, carray.get());
  uint64_t lastBatchSize = totalLength - resultOffset;
  EXPECT_EQ(x->numElements, lastBatchSize);
  EXPECT_TRUE(x->hasNulls);
  for (uint64_t i = 0; i < lastBatchSize; i++) {
    if ((i + resultOffset) % 2) {
      EXPECT_TRUE(x->notNull[i]);
      EXPECT_EQ(x->data[i], i + resultOffset);
    } else
      EXPECT_FALSE(x->notNull[i]);
  }
  writer->add(*batch);
  writer->close();
}

// Float
TEST(TestAdapterWriteNumerical, writeFloatEmpty) {
  FloatBuilder builder;
  std::shared_ptr<Array> array;
  (void)(builder.Finish(&array));
  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:float>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::DoubleVectorBatch* x =
      internal::checked_cast<liborc::DoubleVectorBatch*>(root->fields[0]);
  DataType* arrowType = float32().get();
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize,
                                       array.get());
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
TEST(TestAdapterWriteNumerical, writeFloatNoNulls) {
  FloatBuilder builder;
  (void)(builder.Append(1.5));
  (void)(builder.Append(2.6));
  (void)(builder.Append(3.7));
  (void)(builder.Append(4.8));
  std::shared_ptr<Array> array;
  (void)(builder.Finish(&array));
  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:float>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::DoubleVectorBatch* x =
      internal::checked_cast<liborc::DoubleVectorBatch*>(root->fields[0]);
  DataType* arrowType = float32().get();
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize,
                                       array.get());
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
TEST(TestAdapterWriteNumerical, writeFloatAllNulls) {
  FloatBuilder builder;
  (void)(builder.AppendNull());
  (void)(builder.AppendNull());
  (void)(builder.AppendNull());
  std::shared_ptr<Array> array;
  (void)(builder.Finish(&array));
  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:float>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::DoubleVectorBatch* x =
      internal::checked_cast<liborc::DoubleVectorBatch*>(root->fields[0]);
  DataType* arrowType = float32().get();
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize,
                                       array.get());
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
TEST(TestAdapterWriteNumerical, writeFloatMixed) {
  FloatBuilder builder;
  (void)(builder.AppendNull());
  (void)(builder.Append(1.2));
  (void)(builder.AppendNull());
  (void)(builder.Append(2.3));
  (void)(builder.AppendNull());
  std::shared_ptr<Array> array;
  (void)(builder.Finish(&array));
  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:float>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::DoubleVectorBatch* x =
      internal::checked_cast<liborc::DoubleVectorBatch*>(root->fields[0]);
  DataType* arrowType = float32().get();
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize,
                                       array.get());
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
TEST(TestAdapterWriteNumerical, writeFloatChunkedEmpty) {
  ArrayVector av;
  std::shared_ptr<ChunkedArray> carray = std::make_shared<ChunkedArray>(av, float32());
  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:float>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::DoubleVectorBatch* x =
      internal::checked_cast<liborc::DoubleVectorBatch*>(root->fields[0]);
  DataType* arrowType = float32().get();
  int64_t arrowIndexOffset = 0;
  int arrowChunkOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowIndexOffset, arrowChunkOffset,
                                       batchSize, carray.get());
  if (!st.ok()) {
    FAIL() << "ORC ColumnBatch not successfully filled";
  }
  EXPECT_EQ(x->numElements, 0);
  EXPECT_FALSE(x->hasNulls);
  EXPECT_EQ(arrowIndexOffset, 0);
  EXPECT_EQ(arrowChunkOffset, 0);
  writer->add(*batch);
  writer->close();
}
TEST(TestAdapterWriteNumerical, writeFloatChunkedMixed) {
  FloatBuilder builder0, builder1, builder2, builder3, builder4;
  (void)(builder1.AppendNull());
  (void)(builder1.Append(1.2));
  (void)(builder2.AppendNull());
  (void)(builder2.Append(2.3));
  (void)(builder3.AppendNull());
  std::shared_ptr<Array> array0, array1, array2, array3, array4;
  (void)(builder0.Finish(&array0));
  (void)(builder1.Finish(&array1));
  (void)(builder2.Finish(&array2));
  (void)(builder3.Finish(&array3));
  (void)(builder4.Finish(&array4));
  ArrayVector av;
  av.push_back(array0);
  av.push_back(array1);
  av.push_back(array2);
  av.push_back(array3);
  av.push_back(array4);
  std::shared_ptr<ChunkedArray> carray = std::make_shared<ChunkedArray>(av);
  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:float>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::DoubleVectorBatch* x =
      internal::checked_cast<liborc::DoubleVectorBatch*>(root->fields[0]);
  DataType* arrowType = float32().get();
  int64_t arrowIndexOffset = 0;
  int arrowChunkOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowIndexOffset, arrowChunkOffset,
                                       batchSize, carray.get());
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
  EXPECT_EQ(arrowIndexOffset, 0);
  EXPECT_EQ(arrowChunkOffset, 5);
  writer->add(*batch);
  writer->close();
}
TEST(TestAdapterWriteNumerical, writeFloatChunkedMultibatch) {
  uint64_t totalLength = 100;
  FloatBuilder builder0, builder1, builder2, builder3, builder4;
  for (int i = 0; i < 50; i++) {
    if (i % 2)
      (void)(builder1.Append(i + 0.5));
    else
      (void)(builder1.AppendNull());
  }
  for (int i = 50; i < 100; i++) {
    if (i % 2)
      (void)(builder3.Append(i + 0.5));
    else
      (void)(builder3.AppendNull());
  }
  std::shared_ptr<Array> array0, array1, array2, array3, array4;
  (void)(builder0.Finish(&array0));
  (void)(builder1.Finish(&array1));
  (void)(builder2.Finish(&array2));
  (void)(builder3.Finish(&array3));
  (void)(builder4.Finish(&array4));
  ArrayVector av;
  av.push_back(array0);
  av.push_back(array1);
  av.push_back(array2);
  av.push_back(array3);
  av.push_back(array4);
  std::shared_ptr<ChunkedArray> carray = std::make_shared<ChunkedArray>(av);
  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:float>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 100;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::DoubleVectorBatch* x =
      internal::checked_cast<liborc::DoubleVectorBatch*>(root->fields[0]);
  DataType* arrowType = float32().get();
  int64_t arrowIndexOffset = 0;
  int arrowChunkOffset = 0;
  uint64_t resultOffset = 0;
  while (resultOffset < totalLength - batchSize) {
    Status st = adapters::orc::FillBatch(arrowType, x, arrowIndexOffset, arrowChunkOffset,
                                         batchSize, carray.get());
    if (!st.ok()) {
      FAIL() << "ORC ColumnBatch not successfully filled";
    }
    writer->add(*batch);
    EXPECT_EQ(x->numElements, batchSize);
    EXPECT_TRUE(x->hasNulls);
    for (uint64_t i = 0; i < batchSize; i++) {
      if ((i + resultOffset) % 2) {
        EXPECT_TRUE(x->notNull[i]);
        EXPECT_FLOAT_EQ(x->data[i], i + resultOffset + 0.5);
      } else
        EXPECT_FALSE(x->notNull[i]);
    }
    resultOffset = resultOffset + batchSize;
    batch->clear();
  }
  Status st = adapters::orc::FillBatch(arrowType, x, arrowIndexOffset, arrowChunkOffset,
                                       batchSize, carray.get());
  uint64_t lastBatchSize = totalLength - resultOffset;
  EXPECT_EQ(x->numElements, lastBatchSize);
  EXPECT_TRUE(x->hasNulls);
  for (uint64_t i = 0; i < lastBatchSize; i++) {
    if ((i + resultOffset) % 2) {
      EXPECT_TRUE(x->notNull[i]);
      EXPECT_FLOAT_EQ(x->data[i], i + resultOffset + 0.5);
    } else
      EXPECT_FALSE(x->notNull[i]);
  }
  writer->add(*batch);
  writer->close();
}

// Double
TEST(TestAdapterWriteNumerical, writeDoubleEmpty) {
  DoubleBuilder builder;
  std::shared_ptr<Array> array;
  (void)(builder.Finish(&array));
  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:double>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::DoubleVectorBatch* x =
      internal::checked_cast<liborc::DoubleVectorBatch*>(root->fields[0]);
  DataType* arrowType = float64().get();
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize,
                                       array.get());
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
TEST(TestAdapterWriteNumerical, writeDoubleNoNulls) {
  DoubleBuilder builder;
  (void)(builder.Append(1.5));
  (void)(builder.Append(2.6));
  (void)(builder.Append(3.7));
  (void)(builder.Append(4.8));
  std::shared_ptr<Array> array;
  (void)(builder.Finish(&array));
  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:double>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::DoubleVectorBatch* x =
      internal::checked_cast<liborc::DoubleVectorBatch*>(root->fields[0]);
  DataType* arrowType = float64().get();
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize,
                                       array.get());
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
TEST(TestAdapterWriteNumerical, writeDoubleAllNulls) {
  DoubleBuilder builder;
  (void)(builder.AppendNull());
  (void)(builder.AppendNull());
  (void)(builder.AppendNull());
  std::shared_ptr<Array> array;
  (void)(builder.Finish(&array));
  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:double>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::DoubleVectorBatch* x =
      internal::checked_cast<liborc::DoubleVectorBatch*>(root->fields[0]);
  DataType* arrowType = float64().get();
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize,
                                       array.get());
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
TEST(TestAdapterWriteNumerical, writeDoubleMixed) {
  DoubleBuilder builder;
  (void)(builder.AppendNull());
  (void)(builder.Append(1.2));
  (void)(builder.AppendNull());
  (void)(builder.Append(2.3));
  (void)(builder.AppendNull());
  std::shared_ptr<Array> array;
  (void)(builder.Finish(&array));
  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:double>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::DoubleVectorBatch* x =
      internal::checked_cast<liborc::DoubleVectorBatch*>(root->fields[0]);
  DataType* arrowType = float64().get();
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize,
                                       array.get());
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
TEST(TestAdapterWriteNumerical, writeDoubleChunkedEmpty) {
  ArrayVector av;
  std::shared_ptr<ChunkedArray> carray = std::make_shared<ChunkedArray>(av, float64());
  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:double>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::DoubleVectorBatch* x =
      internal::checked_cast<liborc::DoubleVectorBatch*>(root->fields[0]);
  DataType* arrowType = float64().get();
  int64_t arrowIndexOffset = 0;
  int arrowChunkOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowIndexOffset, arrowChunkOffset,
                                       batchSize, carray.get());
  if (!st.ok()) {
    FAIL() << "ORC ColumnBatch not successfully filled";
  }
  EXPECT_EQ(x->numElements, 0);
  EXPECT_FALSE(x->hasNulls);
  EXPECT_EQ(arrowIndexOffset, 0);
  EXPECT_EQ(arrowChunkOffset, 0);
  writer->add(*batch);
  writer->close();
}
TEST(TestAdapterWriteNumerical, writeDoubleChunkedMixed) {
  DoubleBuilder builder0, builder1, builder2, builder3, builder4;
  (void)(builder1.AppendNull());
  (void)(builder1.Append(1.2));
  (void)(builder2.AppendNull());
  (void)(builder2.Append(2.3));
  (void)(builder3.AppendNull());
  std::shared_ptr<Array> array0, array1, array2, array3, array4;
  (void)(builder0.Finish(&array0));
  (void)(builder1.Finish(&array1));
  (void)(builder2.Finish(&array2));
  (void)(builder3.Finish(&array3));
  (void)(builder4.Finish(&array4));
  ArrayVector av;
  av.push_back(array0);
  av.push_back(array1);
  av.push_back(array2);
  av.push_back(array3);
  av.push_back(array4);
  std::shared_ptr<ChunkedArray> carray = std::make_shared<ChunkedArray>(av);
  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:double>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::DoubleVectorBatch* x =
      internal::checked_cast<liborc::DoubleVectorBatch*>(root->fields[0]);
  DataType* arrowType = float64().get();
  int64_t arrowIndexOffset = 0;
  int arrowChunkOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowIndexOffset, arrowChunkOffset,
                                       batchSize, carray.get());
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
  EXPECT_EQ(arrowIndexOffset, 0);
  EXPECT_EQ(arrowChunkOffset, 5);
  writer->add(*batch);
  writer->close();
}
TEST(TestAdapterWriteNumerical, writeDoubleChunkedMultibatch) {
  uint64_t totalLength = 100;
  DoubleBuilder builder0, builder1, builder2, builder3, builder4;
  for (int i = 0; i < 50; i++) {
    if (i % 2)
      (void)(builder1.Append(i + 0.5));
    else
      (void)(builder1.AppendNull());
  }
  for (int i = 50; i < 100; i++) {
    if (i % 2)
      (void)(builder3.Append(i + 0.5));
    else
      (void)(builder3.AppendNull());
  }
  std::shared_ptr<Array> array0, array1, array2, array3, array4;
  (void)(builder0.Finish(&array0));
  (void)(builder1.Finish(&array1));
  (void)(builder2.Finish(&array2));
  (void)(builder3.Finish(&array3));
  (void)(builder4.Finish(&array4));
  ArrayVector av;
  av.push_back(array0);
  av.push_back(array1);
  av.push_back(array2);
  av.push_back(array3);
  av.push_back(array4);
  std::shared_ptr<ChunkedArray> carray = std::make_shared<ChunkedArray>(av);
  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:double>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 100;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::DoubleVectorBatch* x =
      internal::checked_cast<liborc::DoubleVectorBatch*>(root->fields[0]);
  DataType* arrowType = float64().get();
  int64_t arrowIndexOffset = 0;
  int arrowChunkOffset = 0;
  uint64_t resultOffset = 0;
  while (resultOffset < totalLength - batchSize) {
    Status st = adapters::orc::FillBatch(arrowType, x, arrowIndexOffset, arrowChunkOffset,
                                         batchSize, carray.get());
    if (!st.ok()) {
      FAIL() << "ORC ColumnBatch not successfully filled";
    }
    writer->add(*batch);
    EXPECT_EQ(x->numElements, batchSize);
    EXPECT_TRUE(x->hasNulls);
    for (uint64_t i = 0; i < batchSize; i++) {
      if ((i + resultOffset) % 2) {
        EXPECT_TRUE(x->notNull[i]);
        EXPECT_DOUBLE_EQ(x->data[i], i + resultOffset + 0.5);
      } else
        EXPECT_FALSE(x->notNull[i]);
    }
    resultOffset = resultOffset + batchSize;
    batch->clear();
  }
  Status st = adapters::orc::FillBatch(arrowType, x, arrowIndexOffset, arrowChunkOffset,
                                       batchSize, carray.get());
  uint64_t lastBatchSize = totalLength - resultOffset;
  EXPECT_EQ(x->numElements, lastBatchSize);
  EXPECT_TRUE(x->hasNulls);
  for (uint64_t i = 0; i < lastBatchSize; i++) {
    if ((i + resultOffset) % 2) {
      EXPECT_TRUE(x->notNull[i]);
      EXPECT_DOUBLE_EQ(x->data[i], i + resultOffset + 0.5);
    } else
      EXPECT_FALSE(x->notNull[i]);
  }
  writer->add(*batch);
  writer->close();
}

// Decimal
TEST(TestAdapterWriteNumerical, writeDecimal128Empty) {
  Decimal128Builder builder(decimal(38, 6));
  std::shared_ptr<Decimal128Array> array;
  (void)(builder.Finish(&array));
  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:decimal(38,6)>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::Decimal128VectorBatch* x =
      internal::checked_cast<liborc::Decimal128VectorBatch*>(root->fields[0]);
  DataType* arrowType = decimal(38, 6).get();
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize,
                                       array.get());
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
TEST(TestAdapterWriteNumerical, writeDecimal128EmptyZero) {
  Decimal128Builder builder(decimal(38, 0));
  std::shared_ptr<Decimal128Array> array;
  (void)(builder.Finish(&array));
  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:decimal(38,0)>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::Decimal128VectorBatch* x =
      internal::checked_cast<liborc::Decimal128VectorBatch*>(root->fields[0]);
  DataType* arrowType = decimal(38, 0).get();
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize,
                                       array.get());
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
TEST(TestAdapterWriteNumerical, writeDecimal128NoNulls) {
  Decimal128Builder builder(decimal(38, 6));
  (void)(builder.Append(Decimal128("1.500000")));
  (void)(builder.Append(Decimal128("2.600000")));
  (void)(builder.Append(Decimal128("3.700000")));
  (void)(builder.Append(Decimal128("4.860000")));
  std::shared_ptr<Array> array;
  (void)(builder.Finish(&array));
  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:decimal(38,6)>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::Decimal128VectorBatch* x =
      internal::checked_cast<liborc::Decimal128VectorBatch*>(root->fields[0]);
  DataType* arrowType = decimal(38, 6).get();
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize,
                                       array.get());
  if (!st.ok()) {
    FAIL() << "ORC ColumnBatch not successfully filled";
  }
  EXPECT_EQ(x->numElements, 4);
  EXPECT_FALSE(x->hasNulls);
  EXPECT_EQ(Decimal128(x->values[0].toDecimalString(6)), Decimal128("1.500000"));
  EXPECT_EQ(Decimal128(x->values[1].toDecimalString(6)), Decimal128("2.600000"));
  EXPECT_EQ(Decimal128(x->values[2].toDecimalString(6)), Decimal128("3.700000"));
  EXPECT_EQ(Decimal128(x->values[3].toDecimalString(6)), Decimal128("4.860000"));
  EXPECT_EQ(arrowOffset, 4);
  EXPECT_EQ(orcOffset, 4);
  writer->add(*batch);
  writer->close();
}
TEST(TestAdapterWriteNumerical, writeDecimal128NoNullsZero) {
  Decimal128Builder builder(decimal(38, 0));
  (void)(builder.Append(Decimal128("15")));
  (void)(builder.Append(Decimal128("-26")));
  (void)(builder.Append(Decimal128("37")));
  (void)(builder.Append(Decimal128("-48")));
  std::shared_ptr<Array> array;
  (void)(builder.Finish(&array));
  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:decimal(38,0)>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::Decimal128VectorBatch* x =
      internal::checked_cast<liborc::Decimal128VectorBatch*>(root->fields[0]);
  DataType* arrowType = decimal(38, 0).get();
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize,
                                       array.get());
  if (!st.ok()) {
    FAIL() << "ORC ColumnBatch not successfully filled";
  }
  EXPECT_EQ(x->numElements, 4);
  EXPECT_FALSE(x->hasNulls);
  EXPECT_EQ(Decimal128(x->values[0].toDecimalString(0)), Decimal128("15"));
  EXPECT_EQ(Decimal128(x->values[1].toDecimalString(0)), Decimal128("-26"));
  EXPECT_EQ(Decimal128(x->values[2].toDecimalString(0)), Decimal128("37"));
  EXPECT_EQ(Decimal128(x->values[3].toDecimalString(0)), Decimal128("-48"));
  EXPECT_EQ(arrowOffset, 4);
  EXPECT_EQ(orcOffset, 4);
  writer->add(*batch);
  writer->close();
}
TEST(TestAdapterWriteNumerical, writeDecimal128AllNulls) {
  Decimal128Builder builder(decimal(38, 2));
  (void)(builder.AppendNull());
  (void)(builder.AppendNull());
  (void)(builder.AppendNull());
  std::shared_ptr<Array> array;
  (void)(builder.Finish(&array));
  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:decimal(38,2)>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::Decimal128VectorBatch* x =
      internal::checked_cast<liborc::Decimal128VectorBatch*>(root->fields[0]);
  DataType* arrowType = decimal(38, 2).get();
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize,
                                       array.get());
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
TEST(TestAdapterWriteNumerical, writeDecimal128AllNullsZero) {
  Decimal128Builder builder(decimal(38, 0));
  (void)(builder.AppendNull());
  (void)(builder.AppendNull());
  (void)(builder.AppendNull());
  std::shared_ptr<Array> array;
  (void)(builder.Finish(&array));
  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:decimal(38,0)>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::Decimal128VectorBatch* x =
      internal::checked_cast<liborc::Decimal128VectorBatch*>(root->fields[0]);
  DataType* arrowType = decimal(38, 0).get();
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize,
                                       array.get());
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
TEST(TestAdapterWriteNumerical, writeDecimal128Mixed) {
  Decimal128Builder builder(decimal(38, 4));
  (void)(builder.AppendNull());
  (void)(builder.Append(Decimal128("-541.2000")));
  (void)(builder.AppendNull());
  (void)(builder.Append(Decimal128("2.3000")));
  (void)(builder.AppendNull());
  std::shared_ptr<Array> array;
  (void)(builder.Finish(&array));
  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:decimal(38,4)>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::Decimal128VectorBatch* x =
      internal::checked_cast<liborc::Decimal128VectorBatch*>(root->fields[0]);
  DataType* arrowType = decimal(38, 4).get();
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize,
                                       array.get());
  if (!st.ok()) {
    FAIL() << "ORC ColumnBatch not successfully filled";
  }
  EXPECT_EQ(x->numElements, 5);
  EXPECT_TRUE(x->hasNulls);
  EXPECT_EQ(Decimal128(x->values[1].toDecimalString(4)), Decimal128("-541.2000"));
  EXPECT_EQ(Decimal128(x->values[3].toDecimalString(4)), Decimal128("2.3000"));
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
TEST(TestAdapterWriteNumerical, writeDecimal128MixedZero) {
  Decimal128Builder builder(decimal(38, 0));
  (void)(builder.AppendNull());
  (void)(builder.Append(Decimal128("-5412")));
  (void)(builder.AppendNull());
  (void)(builder.Append(Decimal128("23")));
  (void)(builder.AppendNull());
  std::shared_ptr<Array> array;
  (void)(builder.Finish(&array));
  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:decimal(38,0)>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::Decimal128VectorBatch* x =
      internal::checked_cast<liborc::Decimal128VectorBatch*>(root->fields[0]);
  DataType* arrowType = decimal(38, 0).get();
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize,
                                       array.get());
  if (!st.ok()) {
    FAIL() << "ORC ColumnBatch not successfully filled";
  }
  EXPECT_EQ(x->numElements, 5);
  EXPECT_TRUE(x->hasNulls);
  EXPECT_EQ(Decimal128(x->values[1].toDecimalString(0)), Decimal128("-5412"));
  EXPECT_EQ(Decimal128(x->values[3].toDecimalString(0)), Decimal128("23"));
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
TEST(TestAdapterWriteNumerical, writeDecimal128ChunkedEmpty) {
  ArrayVector av;
  std::shared_ptr<ChunkedArray> carray =
      std::make_shared<ChunkedArray>(av, decimal(38, 4));
  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:decimal(38,4)>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::Decimal128VectorBatch* x =
      internal::checked_cast<liborc::Decimal128VectorBatch*>(root->fields[0]);
  DataType* arrowType = decimal(38, 4).get();
  int64_t arrowIndexOffset = 0;
  int arrowChunkOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowIndexOffset, arrowChunkOffset,
                                       batchSize, carray.get());
  if (!st.ok()) {
    FAIL() << "ORC ColumnBatch not successfully filled";
  }
  EXPECT_EQ(x->numElements, 0);
  EXPECT_FALSE(x->hasNulls);
  EXPECT_EQ(arrowIndexOffset, 0);
  EXPECT_EQ(arrowChunkOffset, 0);
  writer->add(*batch);
  writer->close();
}
TEST(TestAdapterWriteNumerical, writeDecimal128ChunkedEmptyZero) {
  ArrayVector av;
  std::shared_ptr<ChunkedArray> carray =
      std::make_shared<ChunkedArray>(av, decimal(38, 0));
  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:decimal(38,0)>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::Decimal128VectorBatch* x =
      internal::checked_cast<liborc::Decimal128VectorBatch*>(root->fields[0]);
  DataType* arrowType = decimal(38, 0).get();
  int64_t arrowIndexOffset = 0;
  int arrowChunkOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowIndexOffset, arrowChunkOffset,
                                       batchSize, carray.get());
  if (!st.ok()) {
    FAIL() << "ORC ColumnBatch not successfully filled";
  }
  EXPECT_EQ(x->numElements, 0);
  EXPECT_FALSE(x->hasNulls);
  EXPECT_EQ(arrowIndexOffset, 0);
  EXPECT_EQ(arrowChunkOffset, 0);
  writer->add(*batch);
  writer->close();
}
TEST(TestAdapterWriteNumerical, writeDecimal128ChunkedMixed) {
  Decimal128Builder builder0(decimal(38, 6)), builder1(decimal(38, 6)),
      builder2(decimal(38, 6)), builder3(decimal(38, 6)), builder4(decimal(38, 6));
  (void)(builder1.AppendNull());
  (void)(builder1.Append(Decimal128("-541.200005")));
  (void)(builder2.AppendNull());
  (void)(builder2.Append(Decimal128("2.300007")));
  (void)(builder3.AppendNull());
  std::shared_ptr<Array> array0, array1, array2, array3, array4;
  (void)(builder0.Finish(&array0));
  (void)(builder1.Finish(&array1));
  (void)(builder2.Finish(&array2));
  (void)(builder3.Finish(&array3));
  (void)(builder4.Finish(&array4));
  ArrayVector av;
  av.push_back(array0);
  av.push_back(array1);
  av.push_back(array2);
  av.push_back(array3);
  av.push_back(array4);
  std::shared_ptr<ChunkedArray> carray = std::make_shared<ChunkedArray>(av);
  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:decimal(38,6)>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::Decimal128VectorBatch* x =
      internal::checked_cast<liborc::Decimal128VectorBatch*>(root->fields[0]);
  DataType* arrowType = decimal(38, 6).get();
  int64_t arrowIndexOffset = 0;
  int arrowChunkOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowIndexOffset, arrowChunkOffset,
                                       batchSize, carray.get());
  if (!st.ok()) {
    FAIL() << "ORC ColumnBatch not successfully filled";
  }
  EXPECT_EQ(x->numElements, 5);
  EXPECT_TRUE(x->hasNulls);
  EXPECT_EQ(Decimal128(x->values[1].toDecimalString(6)), Decimal128("-541.200005"));
  EXPECT_EQ(Decimal128(x->values[3].toDecimalString(6)), Decimal128("2.300007"));
  EXPECT_EQ(x->notNull[0], 0);
  EXPECT_EQ(x->notNull[1], 1);
  EXPECT_EQ(x->notNull[2], 0);
  EXPECT_EQ(x->notNull[3], 1);
  EXPECT_EQ(x->notNull[4], 0);
  EXPECT_EQ(arrowIndexOffset, 0);
  EXPECT_EQ(arrowChunkOffset, 5);
  writer->add(*batch);
  writer->close();
}
TEST(TestAdapterWriteNumerical, writeDecimal128ChunkedMixedZero) {
  Decimal128Builder builder0(decimal(38, 0)), builder1(decimal(38, 0)),
      builder2(decimal(38, 0)), builder3(decimal(38, 0)), builder4(decimal(38, 0));
  (void)(builder1.AppendNull());
  (void)(builder1.Append(Decimal128("-541")));
  (void)(builder2.AppendNull());
  (void)(builder2.Append(Decimal128("2")));
  (void)(builder3.AppendNull());
  std::shared_ptr<Array> array0, array1, array2, array3, array4;
  (void)(builder0.Finish(&array0));
  (void)(builder1.Finish(&array1));
  (void)(builder2.Finish(&array2));
  (void)(builder3.Finish(&array3));
  (void)(builder4.Finish(&array4));
  ArrayVector av;
  av.push_back(array0);
  av.push_back(array1);
  av.push_back(array2);
  av.push_back(array3);
  av.push_back(array4);
  std::shared_ptr<ChunkedArray> carray = std::make_shared<ChunkedArray>(av);
  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:decimal(38,0)>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::Decimal128VectorBatch* x =
      internal::checked_cast<liborc::Decimal128VectorBatch*>(root->fields[0]);
  DataType* arrowType = decimal(38, 0).get();
  int64_t arrowIndexOffset = 0;
  int arrowChunkOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowIndexOffset, arrowChunkOffset,
                                       batchSize, carray.get());
  if (!st.ok()) {
    FAIL() << "ORC ColumnBatch not successfully filled";
  }
  EXPECT_EQ(x->numElements, 5);
  EXPECT_TRUE(x->hasNulls);
  EXPECT_EQ(Decimal128(x->values[1].toDecimalString(0)), Decimal128("-541"));
  EXPECT_EQ(Decimal128(x->values[3].toDecimalString(0)), Decimal128("2"));
  EXPECT_EQ(x->notNull[0], 0);
  EXPECT_EQ(x->notNull[1], 1);
  EXPECT_EQ(x->notNull[2], 0);
  EXPECT_EQ(x->notNull[3], 1);
  EXPECT_EQ(x->notNull[4], 0);
  EXPECT_EQ(arrowIndexOffset, 0);
  EXPECT_EQ(arrowChunkOffset, 5);
  writer->add(*batch);
  writer->close();
}
TEST(TestAdapterWriteNumerical, writeDecimal128ChunkedMultibatch) {
  uint64_t totalLength = 100;
  Decimal128Builder builder0(decimal(38, 6)), builder1(decimal(38, 6)),
      builder2(decimal(38, 6)), builder3(decimal(38, 6)), builder4(decimal(38, 6));
  for (int i = 0; i < 50; i++) {
    if (i % 2)
      (void)(builder1.Append(Decimal128(std::to_string(i) + ".567891")));
    else
      (void)(builder1.AppendNull());
  }
  for (int i = 50; i < 100; i++) {
    if (i % 2)
      (void)(builder3.Append(Decimal128(std::to_string(i) + ".567891")));
    else
      (void)(builder3.AppendNull());
  }
  std::shared_ptr<Array> array0, array1, array2, array3, array4;
  (void)(builder0.Finish(&array0));
  (void)(builder1.Finish(&array1));
  (void)(builder2.Finish(&array2));
  (void)(builder3.Finish(&array3));
  (void)(builder4.Finish(&array4));
  ArrayVector av;
  av.push_back(array0);
  av.push_back(array1);
  av.push_back(array2);
  av.push_back(array3);
  av.push_back(array4);
  std::shared_ptr<ChunkedArray> carray = std::make_shared<ChunkedArray>(av);
  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:decimal(38,6)>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 100;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::Decimal128VectorBatch* x =
      internal::checked_cast<liborc::Decimal128VectorBatch*>(root->fields[0]);
  DataType* arrowType = decimal(38, 6).get();
  int64_t arrowIndexOffset = 0;
  int arrowChunkOffset = 0;
  uint64_t resultOffset = 0;
  while (resultOffset < totalLength - batchSize) {
    Status st = adapters::orc::FillBatch(arrowType, x, arrowIndexOffset, arrowChunkOffset,
                                         batchSize, carray.get());
    if (!st.ok()) {
      FAIL() << "ORC ColumnBatch not successfully filled";
    }
    writer->add(*batch);
    EXPECT_EQ(x->numElements, batchSize);
    EXPECT_TRUE(x->hasNulls);
    for (uint64_t i = 0; i < batchSize; i++) {
      if ((i + resultOffset) % 2) {
        EXPECT_TRUE(x->notNull[i]);
        EXPECT_EQ(Decimal128(x->values[i].toDecimalString(6)),
                  Decimal128(std::to_string(i + resultOffset) + ".567891"));
      } else
        EXPECT_FALSE(x->notNull[i]);
    }
    resultOffset = resultOffset + batchSize;
    batch->clear();
  }
  Status st = adapters::orc::FillBatch(arrowType, x, arrowIndexOffset, arrowChunkOffset,
                                       batchSize, carray.get());
  uint64_t lastBatchSize = totalLength - resultOffset;
  EXPECT_EQ(x->numElements, lastBatchSize);
  EXPECT_TRUE(x->hasNulls);
  for (uint64_t i = 0; i < lastBatchSize; i++) {
    if ((i + resultOffset) % 2) {
      EXPECT_TRUE(x->notNull[i]);
      EXPECT_EQ(Decimal128(x->values[i].toDecimalString(6)),
                Decimal128(std::to_string(i + resultOffset) + ".567891"));
    } else
      EXPECT_FALSE(x->notNull[i]);
  }
  writer->add(*batch);
  writer->close();
}
TEST(TestAdapterWriteNumerical, writeDecimal128ChunkedMultibatchZero) {
  uint64_t totalLength = 100;
  Decimal128Builder builder0(decimal(38, 0)), builder1(decimal(38, 0)),
      builder2(decimal(38, 0)), builder3(decimal(38, 0)), builder4(decimal(38, 0));
  for (int i = 0; i < 50; i++) {
    if (i % 2)
      (void)(builder1.Append(Decimal128(std::to_string(i))));
    else
      (void)(builder1.AppendNull());
  }
  for (int i = 50; i < 100; i++) {
    if (i % 2)
      (void)(builder3.Append(Decimal128(std::to_string(i))));
    else
      (void)(builder3.AppendNull());
  }
  std::shared_ptr<Array> array0, array1, array2, array3, array4;
  (void)(builder0.Finish(&array0));
  (void)(builder1.Finish(&array1));
  (void)(builder2.Finish(&array2));
  (void)(builder3.Finish(&array3));
  (void)(builder4.Finish(&array4));
  ArrayVector av;
  av.push_back(array0);
  av.push_back(array1);
  av.push_back(array2);
  av.push_back(array3);
  av.push_back(array4);
  std::shared_ptr<ChunkedArray> carray = std::make_shared<ChunkedArray>(av);
  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:decimal(38,0)>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 100;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::Decimal128VectorBatch* x =
      internal::checked_cast<liborc::Decimal128VectorBatch*>(root->fields[0]);
  DataType* arrowType = decimal(38, 0).get();
  int64_t arrowIndexOffset = 0;
  int arrowChunkOffset = 0;
  uint64_t resultOffset = 0;
  while (resultOffset < totalLength - batchSize) {
    Status st = adapters::orc::FillBatch(arrowType, x, arrowIndexOffset, arrowChunkOffset,
                                         batchSize, carray.get());
    if (!st.ok()) {
      FAIL() << "ORC ColumnBatch not successfully filled";
    }
    writer->add(*batch);
    EXPECT_EQ(x->numElements, batchSize);
    EXPECT_TRUE(x->hasNulls);
    for (uint64_t i = 0; i < batchSize; i++) {
      if ((i + resultOffset) % 2) {
        EXPECT_TRUE(x->notNull[i]);
        EXPECT_EQ(Decimal128(x->values[i].toDecimalString(0)),
                  Decimal128(std::to_string(i + resultOffset)));
      } else
        EXPECT_FALSE(x->notNull[i]);
    }
    resultOffset = resultOffset + batchSize;
    batch->clear();
  }
  Status st = adapters::orc::FillBatch(arrowType, x, arrowIndexOffset, arrowChunkOffset,
                                       batchSize, carray.get());
  uint64_t lastBatchSize = totalLength - resultOffset;
  EXPECT_EQ(x->numElements, lastBatchSize);
  EXPECT_TRUE(x->hasNulls);
  for (uint64_t i = 0; i < lastBatchSize; i++) {
    if ((i + resultOffset) % 2) {
      EXPECT_TRUE(x->notNull[i]);
      EXPECT_EQ(Decimal128(x->values[i].toDecimalString(0)),
                Decimal128(std::to_string(i + resultOffset)));
    } else
      EXPECT_FALSE(x->notNull[i]);
  }
  writer->add(*batch);
  writer->close();
}

// Binary formats

// String
TEST(TestAdapterWriteBinary, writeStringEmpty) {
  StringBuilder builder;
  std::shared_ptr<Array> array;
  (void)(builder.Finish(&array));
  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:string>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::StringVectorBatch* x =
      internal::checked_cast<liborc::StringVectorBatch*>(root->fields[0]);
  DataType* arrowType = utf8().get();
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize,
                                       array.get());
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
TEST(TestAdapterWriteBinary, writeStringNoNulls) {
  StringBuilder builder;
  (void)(builder.Append("A"));
  (void)(builder.Append("AB"));
  (void)(builder.Append(""));
  (void)(builder.Append("ABCD"));
  std::shared_ptr<Array> array;
  (void)(builder.Finish(&array));
  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:string>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::StringVectorBatch* x =
      internal::checked_cast<liborc::StringVectorBatch*>(root->fields[0]);
  DataType* arrowType = utf8().get();
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize,
                                       array.get());
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
TEST(TestAdapterWriteBinary, writeStringAllNulls) {
  StringBuilder builder;
  (void)(builder.AppendNull());
  (void)(builder.AppendNull());
  (void)(builder.AppendNull());
  std::shared_ptr<Array> array;
  (void)(builder.Finish(&array));
  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:string>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::StringVectorBatch* x =
      internal::checked_cast<liborc::StringVectorBatch*>(root->fields[0]);
  DataType* arrowType = utf8().get();
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize,
                                       array.get());
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
TEST(TestAdapterWriteBinary, writeStringMixed) {
  StringBuilder builder;
  (void)(builder.AppendNull());
  (void)(builder.Append(""));
  (void)(builder.AppendNull());
  (void)(builder.Append("AB"));
  (void)(builder.AppendNull());
  std::shared_ptr<Array> array;
  (void)(builder.Finish(&array));
  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:string>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::StringVectorBatch* x =
      internal::checked_cast<liborc::StringVectorBatch*>(root->fields[0]);
  DataType* arrowType = utf8().get();
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize,
                                       array.get());
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

// LargeString
TEST(TestAdapterWriteBinary, writeLargeStringEmpty) {
  LargeStringBuilder builder;
  std::shared_ptr<Array> array;
  (void)(builder.Finish(&array));
  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:string>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::StringVectorBatch* x =
      internal::checked_cast<liborc::StringVectorBatch*>(root->fields[0]);
  DataType* arrowType = large_utf8().get();
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize,
                                       array.get());
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
TEST(TestAdapterWriteBinary, writeLargeStringNoNulls) {
  LargeStringBuilder builder;
  (void)(builder.Append("A"));
  (void)(builder.Append("AB"));
  (void)(builder.Append(""));
  (void)(builder.Append("ABCD"));
  std::shared_ptr<Array> array;
  (void)(builder.Finish(&array));
  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:string>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::StringVectorBatch* x =
      internal::checked_cast<liborc::StringVectorBatch*>(root->fields[0]);
  DataType* arrowType = large_utf8().get();
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize,
                                       array.get());
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
TEST(TestAdapterWriteBinary, writeLargeStringAllNulls) {
  LargeStringBuilder builder;
  (void)(builder.AppendNull());
  (void)(builder.AppendNull());
  (void)(builder.AppendNull());
  std::shared_ptr<Array> array;
  (void)(builder.Finish(&array));
  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:string>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::StringVectorBatch* x =
      internal::checked_cast<liborc::StringVectorBatch*>(root->fields[0]);
  DataType* arrowType = large_utf8().get();
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize,
                                       array.get());
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
TEST(TestAdapterWriteBinary, writeLargeStringMixed) {
  LargeStringBuilder builder;
  (void)(builder.AppendNull());
  (void)(builder.Append(""));
  (void)(builder.AppendNull());
  (void)(builder.Append("AB"));
  (void)(builder.AppendNull());
  std::shared_ptr<Array> array;
  (void)(builder.Finish(&array));
  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:string>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::StringVectorBatch* x =
      internal::checked_cast<liborc::StringVectorBatch*>(root->fields[0]);
  DataType* arrowType = large_utf8().get();
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize,
                                       array.get());
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

// Binary
TEST(TestAdapterWriteBinary, writeBinaryEmpty) {
  BinaryBuilder builder;
  std::shared_ptr<Array> array;
  (void)(builder.Finish(&array));
  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:binary>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::StringVectorBatch* x =
      internal::checked_cast<liborc::StringVectorBatch*>(root->fields[0]);
  DataType* arrowType = binary().get();
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize,
                                       array.get());
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
TEST(TestAdapterWriteBinary, writeBinaryNoNulls) {
  BinaryBuilder builder;
  char a[] = "\xf4", b[] = "\x22\x0e", c[] = "", d[] = "\xff\x66\xbf\x5b";
  (void)(builder.Append(a));
  (void)(builder.Append(b));
  (void)(builder.Append(c));
  (void)(builder.Append(d));
  std::shared_ptr<Array> array;
  (void)(builder.Finish(&array));
  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:binary>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::StringVectorBatch* x =
      internal::checked_cast<liborc::StringVectorBatch*>(root->fields[0]);
  DataType* arrowType = binary().get();
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize,
                                       array.get());
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
TEST(TestAdapterWriteBinary, writeBinaryAllNulls) {
  BinaryBuilder builder;
  (void)(builder.AppendNull());
  (void)(builder.AppendNull());
  (void)(builder.AppendNull());
  std::shared_ptr<Array> array;
  (void)(builder.Finish(&array));
  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:binary>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::StringVectorBatch* x =
      internal::checked_cast<liborc::StringVectorBatch*>(root->fields[0]);
  DataType* arrowType = binary().get();
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize,
                                       array.get());
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
TEST(TestAdapterWriteBinary, writeBinaryMixed) {
  BinaryBuilder builder;
  char a[] = "", b[] = "\xff\xfe";
  (void)(builder.AppendNull());
  (void)(builder.Append(a));
  (void)(builder.AppendNull());
  (void)(builder.Append(b));
  (void)(builder.AppendNull());
  std::shared_ptr<Array> array;
  (void)(builder.Finish(&array));
  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:binary>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::StringVectorBatch* x =
      internal::checked_cast<liborc::StringVectorBatch*>(root->fields[0]);
  DataType* arrowType = binary().get();
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize,
                                       array.get());
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

// LargeBinary
TEST(TestAdapterWriteBinary, writeLargeBinaryEmpty) {
  LargeBinaryBuilder builder;
  std::shared_ptr<Array> array;
  (void)(builder.Finish(&array));
  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:binary>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::StringVectorBatch* x =
      internal::checked_cast<liborc::StringVectorBatch*>(root->fields[0]);
  DataType* arrowType = large_binary().get();
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize,
                                       array.get());
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
TEST(TestAdapterWriteBinary, writeLargeBinaryNoNulls) {
  LargeBinaryBuilder builder;
  char a[] = "\xf4", b[] = "\x22\x0e", c[] = "", d[] = "\xff\x66\xbf\x5b";
  (void)(builder.Append(a));
  (void)(builder.Append(b));
  (void)(builder.Append(c));
  (void)(builder.Append(d));
  std::shared_ptr<Array> array;
  (void)(builder.Finish(&array));
  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:binary>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::StringVectorBatch* x =
      internal::checked_cast<liborc::StringVectorBatch*>(root->fields[0]);
  DataType* arrowType = large_binary().get();
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize,
                                       array.get());
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
TEST(TestAdapterWriteBinary, writeLargeBinaryAllNulls) {
  LargeBinaryBuilder builder;
  (void)(builder.AppendNull());
  (void)(builder.AppendNull());
  (void)(builder.AppendNull());
  std::shared_ptr<Array> array;
  (void)(builder.Finish(&array));
  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:binary>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::StringVectorBatch* x =
      internal::checked_cast<liborc::StringVectorBatch*>(root->fields[0]);
  DataType* arrowType = large_binary().get();
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize,
                                       array.get());
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
TEST(TestAdapterWriteBinary, writeLargeBinaryMixed) {
  LargeBinaryBuilder builder;
  char a[] = "", b[] = "\xff\xfe";
  (void)(builder.AppendNull());
  (void)(builder.Append(a));
  (void)(builder.AppendNull());
  (void)(builder.Append(b));
  (void)(builder.AppendNull());
  std::shared_ptr<Array> array;
  (void)(builder.Finish(&array));
  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:binary>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::StringVectorBatch* x =
      internal::checked_cast<liborc::StringVectorBatch*>(root->fields[0]);
  DataType* arrowType = large_binary().get();
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize,
                                       array.get());
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

// FixedSizeBinary
TEST(TestAdapterWriteBinary, writeFixedSizeBinaryEmpty) {
  FixedSizeBinaryBuilder builder(fixed_size_binary(4));
  std::shared_ptr<Array> array;
  (void)(builder.Finish(&array));
  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:binary>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::StringVectorBatch* x =
      internal::checked_cast<liborc::StringVectorBatch*>(root->fields[0]);
  DataType* arrowType = fixed_size_binary(4).get();
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize,
                                       array.get());
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
TEST(TestAdapterWriteBinary, writeFixedSizeBinaryZeroEmpty) {
  FixedSizeBinaryBuilder builder(fixed_size_binary(0));
  std::shared_ptr<Array> array;
  (void)(builder.Finish(&array));
  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:binary>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::StringVectorBatch* x =
      internal::checked_cast<liborc::StringVectorBatch*>(root->fields[0]);
  DataType* arrowType = fixed_size_binary(0).get();
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize,
                                       array.get());
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
TEST(TestAdapterWriteBinary, writeFixedSizeBinaryZeroNoNulls) {
  FixedSizeBinaryBuilder builder(fixed_size_binary(0));
  std::shared_ptr<Array> array;
  char a[] = "", b[] = "";
  (void)(builder.Append(a));
  (void)(builder.Append(b));
  (void)(builder.Finish(&array));
  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:binary>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::StringVectorBatch* x =
      internal::checked_cast<liborc::StringVectorBatch*>(root->fields[0]);
  DataType* arrowType = fixed_size_binary(0).get();
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize,
                                       array.get());
  if (!st.ok()) {
    FAIL() << "ORC ColumnBatch not successfully filled";
  }
  EXPECT_EQ(x->numElements, 2);
  EXPECT_FALSE(x->hasNulls);
  EXPECT_EQ(x->length[0], 0);
  EXPECT_EQ(x->length[1], 0);
  EXPECT_EQ(arrowOffset, 2);
  EXPECT_EQ(orcOffset, 2);
  writer->add(*batch);
  writer->close();
}
TEST(TestAdapterWriteBinary, writeFixedSizeBinaryZeroMixed) {
  FixedSizeBinaryBuilder builder(fixed_size_binary(0));
  std::shared_ptr<Array> array;
  char a[] = "", b[] = "";
  (void)(builder.Append(a));
  (void)(builder.Append(b));
  (void)(builder.AppendNull());
  (void)(builder.Finish(&array));
  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:binary>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::StringVectorBatch* x =
      internal::checked_cast<liborc::StringVectorBatch*>(root->fields[0]);
  DataType* arrowType = fixed_size_binary(0).get();
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize,
                                       array.get());
  if (!st.ok()) {
    FAIL() << "ORC ColumnBatch not successfully filled";
  }
  EXPECT_EQ(x->numElements, 3);
  EXPECT_TRUE(x->hasNulls);
  EXPECT_EQ(x->notNull[0], 1);
  EXPECT_EQ(x->notNull[1], 1);
  EXPECT_EQ(x->notNull[2], 0);

  EXPECT_EQ(x->length[0], 0);
  EXPECT_EQ(x->length[1], 0);
  EXPECT_EQ(x->length[2], 0);

  EXPECT_EQ(arrowOffset, 3);
  EXPECT_EQ(orcOffset, 3);
  writer->add(*batch);
  writer->close();
}
TEST(TestAdapterWriteBinary, writeFixedSizeBinaryZeroAllNulls) {
  FixedSizeBinaryBuilder builder(fixed_size_binary(0));
  std::shared_ptr<Array> array;
  (void)(builder.AppendNull());
  (void)(builder.AppendNull());
  (void)(builder.AppendNull());
  (void)(builder.Finish(&array));
  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:binary>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::StringVectorBatch* x =
      internal::checked_cast<liborc::StringVectorBatch*>(root->fields[0]);
  DataType* arrowType = fixed_size_binary(0).get();
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize,
                                       array.get());
  if (!st.ok()) {
    FAIL() << "ORC ColumnBatch not successfully filled";
  }
  EXPECT_EQ(x->numElements, 3);
  EXPECT_TRUE(x->hasNulls);
  EXPECT_EQ(x->notNull[0], 0);
  EXPECT_EQ(x->notNull[1], 0);
  EXPECT_EQ(x->notNull[2], 0);

  EXPECT_EQ(x->length[0], 0);
  EXPECT_EQ(x->length[1], 0);
  EXPECT_EQ(x->length[2], 0);

  EXPECT_EQ(arrowOffset, 3);
  EXPECT_EQ(orcOffset, 3);
  writer->add(*batch);
  writer->close();
}
TEST(TestAdapterWriteBinary, writeFixedSizeBinaryNoNulls) {
  FixedSizeBinaryBuilder builder(fixed_size_binary(4));
  char a[] = "\xf4\xd2\x21\x39", b[] = "\x22\x0e\x09\xaa", c[] = "\x34\x01\x43\x42",
       d[] = "\xff\x66\xbf\x5b";
  (void)(builder.Append(a));
  (void)(builder.Append(b));
  (void)(builder.Append(c));
  (void)(builder.Append(d));
  std::shared_ptr<Array> array;
  (void)(builder.Finish(&array));
  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:binary>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::StringVectorBatch* x =
      internal::checked_cast<liborc::StringVectorBatch*>(root->fields[0]);
  DataType* arrowType = fixed_size_binary(4).get();
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize,
                                       array.get());
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
TEST(TestAdapterWriteBinary, writeFixedSizeBinaryAllNulls) {
  FixedSizeBinaryBuilder builder(fixed_size_binary(1));
  (void)(builder.AppendNull());
  (void)(builder.AppendNull());
  (void)(builder.AppendNull());
  std::shared_ptr<Array> array;
  (void)(builder.Finish(&array));
  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:binary>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::StringVectorBatch* x =
      internal::checked_cast<liborc::StringVectorBatch*>(root->fields[0]);
  DataType* arrowType = fixed_size_binary(1).get();
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize,
                                       array.get());
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
TEST(TestAdapterWriteBinary, writeFixedSizeBinaryMixed) {
  FixedSizeBinaryBuilder builder(fixed_size_binary(2));
  char a[] = "\xa2\x34", b[] = "\xff\xfe";
  (void)(builder.AppendNull());
  (void)(builder.Append(a));
  (void)(builder.AppendNull());
  (void)(builder.Append(b));
  (void)(builder.AppendNull());
  std::shared_ptr<Array> array;
  (void)(builder.Finish(&array));
  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:binary>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::StringVectorBatch* x =
      internal::checked_cast<liborc::StringVectorBatch*>(root->fields[0]);
  DataType* arrowType = fixed_size_binary(2).get();
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize,
                                       array.get());
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

// Nested types
// Struct
TEST(TestAdapterWriteNested, writeStructEmpty) {
  std::vector<std::shared_ptr<Field>> xFields;
  xFields.push_back(std::make_shared<Field>("a", utf8()));
  xFields.push_back(std::make_shared<Field>("b", int32()));
  auto sharedPtrArrowType = struct_(xFields);
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

  auto array = std::make_shared<StructArray>(sharedPtrArrowType, 0, children);

  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:struct<a:string,b:int>>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::StructVectorBatch* x =
      internal::checked_cast<liborc::StructVectorBatch*>(root->fields[0]);
  liborc::StringVectorBatch* a =
      internal::checked_cast<liborc::StringVectorBatch*>(x->fields[0]);
  liborc::LongVectorBatch* b =
      internal::checked_cast<liborc::LongVectorBatch*>(x->fields[1]);
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize,
                                       array.get());
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
TEST(TestAdapterWriteNested, writeStructNoNulls) {
  std::vector<std::shared_ptr<Field>> xFields;
  xFields.push_back(std::make_shared<Field>("a", utf8()));
  xFields.push_back(std::make_shared<Field>("b", int32()));
  auto sharedPtrArrowType = struct_(xFields);
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

  auto array = std::make_shared<StructArray>(sharedPtrArrowType, 4, children);

  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:struct<a:string,b:int>>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::StructVectorBatch* x =
      internal::checked_cast<liborc::StructVectorBatch*>(root->fields[0]);
  liborc::StringVectorBatch* a =
      internal::checked_cast<liborc::StringVectorBatch*>(x->fields[0]);
  liborc::LongVectorBatch* b =
      internal::checked_cast<liborc::LongVectorBatch*>(x->fields[1]);
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize,
                                       array.get());
  if (!st.ok()) {
    FAIL() << "ORC ColumnBatch not successfully filled";
  }
  EXPECT_EQ(x->numElements, 4);
  EXPECT_FALSE(x->hasNulls);
  EXPECT_EQ(a->numElements, 4);
  EXPECT_FALSE(a->hasNulls);
  EXPECT_EQ(b->numElements, 4);
  EXPECT_FALSE(b->hasNulls);

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
TEST(TestAdapterWriteNested, writeStructMixed1) {
  std::vector<std::shared_ptr<Field>> xFields;
  xFields.push_back(std::make_shared<Field>("a", utf8()));
  xFields.push_back(std::make_shared<Field>("b", int32()));
  auto sharedPtrArrowType = struct_(xFields);
  DataType* arrowType = sharedPtrArrowType.get();

  StringBuilder builder1;
  (void)(builder1.Append("A"));
  (void)(builder1.Append("AB"));
  (void)(builder1.AppendNull());
  (void)(builder1.AppendNull());

  Int32Builder builder2;
  (void)(builder2.Append(3));
  (void)(builder2.AppendNull());
  (void)(builder2.Append(25));
  (void)(builder2.AppendNull());

  std::shared_ptr<Array> array1;
  (void)(builder1.Finish(&array1));
  std::shared_ptr<Array> array2;
  (void)(builder2.Finish(&array2));

  std::vector<std::shared_ptr<Array>> children;
  children.push_back(array1);
  children.push_back(array2);

  auto array = std::make_shared<StructArray>(sharedPtrArrowType, 4, children);

  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:struct<a:string,b:int>>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::StructVectorBatch* x =
      internal::checked_cast<liborc::StructVectorBatch*>(root->fields[0]);
  liborc::StringVectorBatch* a =
      internal::checked_cast<liborc::StringVectorBatch*>(x->fields[0]);
  liborc::LongVectorBatch* b =
      internal::checked_cast<liborc::LongVectorBatch*>(x->fields[1]);
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize,
                                       array.get());
  if (!st.ok()) {
    FAIL() << "ORC ColumnBatch not successfully filled";
  }
  EXPECT_EQ(x->numElements, 4);
  EXPECT_FALSE(x->hasNulls);
  EXPECT_EQ(a->numElements, 4);
  EXPECT_TRUE(a->hasNulls);
  EXPECT_EQ(b->numElements, 4);
  EXPECT_TRUE(b->hasNulls);

  EXPECT_STREQ(a->data[0], "A");
  EXPECT_STREQ(a->data[1], "AB");
  EXPECT_EQ(a->length[0], 1);
  EXPECT_EQ(a->length[1], 2);
  EXPECT_EQ(a->notNull[0], 1);
  EXPECT_EQ(a->notNull[1], 1);
  EXPECT_EQ(a->notNull[2], 0);
  EXPECT_EQ(a->notNull[3], 0);

  EXPECT_EQ(b->data[0], 3);
  EXPECT_EQ(b->data[2], 25);
  EXPECT_EQ(b->notNull[0], 1);
  EXPECT_EQ(b->notNull[1], 0);
  EXPECT_EQ(b->notNull[2], 1);
  EXPECT_EQ(b->notNull[3], 0);

  EXPECT_EQ(arrowOffset, 4);
  EXPECT_EQ(orcOffset, 4);
  writer->add(*batch);
  writer->close();
}
TEST(TestAdapterWriteNested, writeStructMixed2) {
  std::vector<std::shared_ptr<Field>> xFields;
  xFields.push_back(std::make_shared<Field>("a", utf8()));
  xFields.push_back(std::make_shared<Field>("b", int32()));
  auto sharedPtrArrowType = struct_(xFields);
  DataType* arrowType = sharedPtrArrowType.get();

  StringBuilder builder1;
  (void)(builder1.Append("A"));
  (void)(builder1.Append("AB"));
  (void)(builder1.AppendNull());
  (void)(builder1.AppendNull());

  Int32Builder builder2;
  (void)(builder2.Append(3));
  (void)(builder2.AppendNull());
  (void)(builder2.Append(25));
  (void)(builder2.AppendNull());

  std::shared_ptr<Array> array1;
  (void)(builder1.Finish(&array1));
  std::shared_ptr<Array> array2;
  (void)(builder2.Finish(&array2));

  std::vector<std::shared_ptr<Array>> children;
  children.push_back(array1);
  children.push_back(array2);

  uint8_t bitmap = 14;  // 00001110
  auto maybeBuffer = AllocateBuffer(1);
  if (!maybeBuffer.ok()) {
    FAIL() << "Buffer not created successfully";
  }
  std::shared_ptr<Buffer> buffer = *std::move(maybeBuffer);
  uint8_t* bufferData = buffer->mutable_data();
  std::memcpy(bufferData, &bitmap, 1);

  auto array = std::make_shared<StructArray>(sharedPtrArrowType, 4, children, buffer);

  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:struct<a:string,b:int>>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::StructVectorBatch* x =
      internal::checked_cast<liborc::StructVectorBatch*>(root->fields[0]);
  liborc::StringVectorBatch* a =
      internal::checked_cast<liborc::StringVectorBatch*>(x->fields[0]);
  liborc::LongVectorBatch* b =
      internal::checked_cast<liborc::LongVectorBatch*>(x->fields[1]);
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize,
                                       array.get());
  if (!st.ok()) {
    FAIL() << "ORC ColumnBatch not successfully filled";
  }
  EXPECT_EQ(x->numElements, 4);
  EXPECT_TRUE(x->hasNulls);
  EXPECT_EQ(a->numElements, 4);
  EXPECT_TRUE(a->hasNulls);
  EXPECT_EQ(b->numElements, 4);
  EXPECT_TRUE(b->hasNulls);

  EXPECT_EQ(x->notNull[0], 0);
  EXPECT_EQ(x->notNull[1], 1);
  EXPECT_EQ(x->notNull[2], 1);
  EXPECT_EQ(x->notNull[3], 1);

  EXPECT_STREQ(a->data[0], "A");
  EXPECT_STREQ(a->data[1], "AB");
  EXPECT_EQ(a->length[0], 1);
  EXPECT_EQ(a->length[1], 2);
  EXPECT_EQ(a->notNull[0], 1);
  EXPECT_EQ(a->notNull[1], 1);
  EXPECT_EQ(a->notNull[2], 0);
  EXPECT_EQ(a->notNull[3], 0);

  EXPECT_EQ(b->data[0], 3);
  EXPECT_EQ(b->data[2], 25);
  EXPECT_EQ(b->notNull[0], 1);
  EXPECT_EQ(b->notNull[1], 0);
  EXPECT_EQ(b->notNull[2], 1);
  EXPECT_EQ(b->notNull[3], 0);

  EXPECT_EQ(arrowOffset, 4);
  EXPECT_EQ(orcOffset, 4);
  writer->add(*batch);
  writer->close();
}
TEST(TestAdapterWriteNested, writeStructMixed3) {
  std::vector<std::shared_ptr<Field>> xFields;
  xFields.push_back(std::make_shared<Field>("a", utf8()));
  xFields.push_back(std::make_shared<Field>("b", int32()));
  auto sharedPtrArrowType = struct_(xFields);
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

  uint8_t bitmap = 14;  // 00001110
  auto maybeBuffer = AllocateBuffer(1);
  if (!maybeBuffer.ok()) {
    FAIL() << "Buffer not created successfully";
  }
  std::shared_ptr<Buffer> buffer = *std::move(maybeBuffer);
  uint8_t* bufferData = buffer->mutable_data();
  std::memcpy(bufferData, &bitmap, 1);

  auto array = std::make_shared<StructArray>(sharedPtrArrowType, 4, children, buffer);

  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:struct<a:string,b:int>>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::StructVectorBatch* x =
      internal::checked_cast<liborc::StructVectorBatch*>(root->fields[0]);
  liborc::StringVectorBatch* a =
      internal::checked_cast<liborc::StringVectorBatch*>(x->fields[0]);
  liborc::LongVectorBatch* b =
      internal::checked_cast<liborc::LongVectorBatch*>(x->fields[1]);
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize,
                                       array.get());
  if (!st.ok()) {
    FAIL() << "ORC ColumnBatch not successfully filled";
  }
  EXPECT_EQ(x->numElements, 4);
  EXPECT_TRUE(x->hasNulls);
  EXPECT_EQ(a->numElements, 4);
  EXPECT_TRUE(a->hasNulls);
  EXPECT_EQ(b->numElements, 4);
  EXPECT_TRUE(b->hasNulls);

  EXPECT_EQ(x->notNull[0], 0);
  EXPECT_EQ(x->notNull[1], 1);
  EXPECT_EQ(x->notNull[2], 1);
  EXPECT_EQ(x->notNull[3], 1);

  EXPECT_EQ(a->notNull[0], 0);
  EXPECT_EQ(a->notNull[1], 0);
  EXPECT_EQ(a->notNull[2], 0);
  EXPECT_EQ(a->notNull[3], 0);

  EXPECT_EQ(b->notNull[0], 0);
  EXPECT_EQ(b->notNull[1], 0);
  EXPECT_EQ(b->notNull[2], 0);
  EXPECT_EQ(b->notNull[3], 0);

  EXPECT_EQ(arrowOffset, 4);
  EXPECT_EQ(orcOffset, 4);
  writer->add(*batch);
  writer->close();
}

// List
TEST(TestAdapterWriteNested, writeListNoNulls) {
  auto sharedPtrArrowType = list(std::make_shared<Field>("a", int32()));
  DataType* arrowType = sharedPtrArrowType.get();

  Int32Builder valuesBuilder, offsetsBuilder;
  std::shared_ptr<Array> valuesArray, offsetsArray;
  (void)(valuesBuilder.Append(1));
  (void)(valuesBuilder.Append(2));
  (void)(valuesBuilder.Append(3));
  (void)(valuesBuilder.Append(4));
  (void)(valuesBuilder.Append(5));
  (void)(valuesBuilder.Append(6));
  (void)(valuesBuilder.Append(7));
  (void)(valuesBuilder.Append(8));
  (void)(valuesBuilder.Append(9));
  (void)(valuesBuilder.Append(10));
  (void)(valuesBuilder.Finish(&valuesArray));
  (void)(offsetsBuilder.Append(0));
  (void)(offsetsBuilder.Append(1));
  (void)(offsetsBuilder.Append(3));
  (void)(offsetsBuilder.Append(6));
  (void)(offsetsBuilder.Append(10));
  (void)(offsetsBuilder.Finish(&offsetsArray));

  std::shared_ptr<ListArray> array;

  array = ListArray::FromArrays(*offsetsArray, *valuesArray).ValueOrDie();

  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:array<a:int>>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::ListVectorBatch* x =
      internal::checked_cast<liborc::ListVectorBatch*>(root->fields[0]);
  liborc::LongVectorBatch* a =
      internal::checked_cast<liborc::LongVectorBatch*>((x->elements).get());
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize,
                                       array.get());
  if (!st.ok()) {
    FAIL() << "ORC ColumnBatch not successfully filled";
  }

  EXPECT_EQ(x->numElements, 4);
  EXPECT_FALSE(x->hasNulls);
  EXPECT_EQ(a->numElements, 10);
  EXPECT_FALSE(a->hasNulls);

  EXPECT_EQ(x->offsets[0], 0);
  EXPECT_EQ(x->offsets[1], 1);
  EXPECT_EQ(x->offsets[2], 3);
  EXPECT_EQ(x->offsets[3], 6);
  EXPECT_EQ(x->offsets[4], 10);

  EXPECT_EQ(a->data[0], 1);
  EXPECT_EQ(a->data[1], 2);
  EXPECT_EQ(a->data[2], 3);
  EXPECT_EQ(a->data[3], 4);
  EXPECT_EQ(a->data[4], 5);
  EXPECT_EQ(a->data[5], 6);
  EXPECT_EQ(a->data[6], 7);
  EXPECT_EQ(a->data[7], 8);
  EXPECT_EQ(a->data[8], 9);
  EXPECT_EQ(a->data[9], 10);

  EXPECT_EQ(arrowOffset, 4);
  EXPECT_EQ(orcOffset, 4);
  writer->add(*batch);
  writer->close();
}
TEST(TestAdapterWriteNested, writeListMixed1) {
  auto sharedPtrArrowType = list(std::make_shared<Field>("a", int32()));
  DataType* arrowType = sharedPtrArrowType.get();

  Int32Builder valuesBuilder, offsetsBuilder;
  std::shared_ptr<Array> valuesArray, offsetsArray;
  (void)(valuesBuilder.AppendNull());
  (void)(valuesBuilder.Append(2));
  (void)(valuesBuilder.Append(3));
  (void)(valuesBuilder.Append(4));
  (void)(valuesBuilder.AppendNull());
  (void)(valuesBuilder.Append(6));
  (void)(valuesBuilder.AppendNull());
  (void)(valuesBuilder.Append(8));
  (void)(valuesBuilder.Append(9));
  (void)(valuesBuilder.AppendNull());
  (void)(valuesBuilder.Finish(&valuesArray));
  (void)(offsetsBuilder.Append(0));
  (void)(offsetsBuilder.Append(1));
  (void)(offsetsBuilder.Append(3));
  (void)(offsetsBuilder.Append(6));
  (void)(offsetsBuilder.Append(10));
  (void)(offsetsBuilder.Finish(&offsetsArray));

  std::shared_ptr<ListArray> array;

  array = ListArray::FromArrays(*offsetsArray, *valuesArray).ValueOrDie();

  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:array<a:int>>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::ListVectorBatch* x =
      internal::checked_cast<liborc::ListVectorBatch*>(root->fields[0]);
  liborc::LongVectorBatch* a =
      internal::checked_cast<liborc::LongVectorBatch*>((x->elements).get());
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize,
                                       array.get());
  if (!st.ok()) {
    FAIL() << "ORC ColumnBatch not successfully filled";
  }

  EXPECT_EQ(x->numElements, 4);
  EXPECT_FALSE(x->hasNulls);
  EXPECT_EQ(a->numElements, 10);
  EXPECT_TRUE(a->hasNulls);

  EXPECT_EQ(x->offsets[0], 0);
  EXPECT_EQ(x->offsets[1], 1);
  EXPECT_EQ(x->offsets[2], 3);
  EXPECT_EQ(x->offsets[3], 6);
  EXPECT_EQ(x->offsets[4], 10);

  EXPECT_EQ(a->notNull[0], 0);
  EXPECT_EQ(a->notNull[1], 1);
  EXPECT_EQ(a->notNull[2], 1);
  EXPECT_EQ(a->notNull[3], 1);
  EXPECT_EQ(a->notNull[4], 0);
  EXPECT_EQ(a->notNull[5], 1);
  EXPECT_EQ(a->notNull[6], 0);
  EXPECT_EQ(a->notNull[7], 1);
  EXPECT_EQ(a->notNull[8], 1);
  EXPECT_EQ(a->notNull[9], 0);

  EXPECT_EQ(a->data[1], 2);
  EXPECT_EQ(a->data[2], 3);
  EXPECT_EQ(a->data[3], 4);
  EXPECT_EQ(a->data[5], 6);
  EXPECT_EQ(a->data[7], 8);
  EXPECT_EQ(a->data[8], 9);

  EXPECT_EQ(arrowOffset, 4);
  EXPECT_EQ(orcOffset, 4);
  writer->add(*batch);
  writer->close();
}
TEST(TestAdapterWriteNested, writeListMixed2) {
  auto sharedPtrArrowType = list(std::make_shared<Field>("a", int32()));
  DataType* arrowType = sharedPtrArrowType.get();

  Int32Builder valuesBuilder, offsetsBuilder;

  int32_t offsets[5] = {0, 1, 3, 6, 10};

  BufferBuilder builder;
  (void)(builder.Resize(20));
  (void)(builder.Append(offsets, 20));
  std::shared_ptr<arrow::Buffer> offsetsBuffer;
  if (!builder.Finish(&offsetsBuffer).ok()) {
    FAIL() << "The offsets buffer can not be constructed!";
  }

  std::shared_ptr<Array> valuesArray;
  (void)(valuesBuilder.AppendNull());
  (void)(valuesBuilder.Append(2));
  (void)(valuesBuilder.Append(3));
  (void)(valuesBuilder.Append(4));
  (void)(valuesBuilder.AppendNull());
  (void)(valuesBuilder.Append(6));
  (void)(valuesBuilder.AppendNull());
  (void)(valuesBuilder.Append(8));
  (void)(valuesBuilder.Append(9));
  (void)(valuesBuilder.AppendNull());
  (void)(valuesBuilder.Finish(&valuesArray));

  uint8_t bitmap = 13;  // 00001101
  auto maybeBuffer = AllocateBuffer(1);
  if (!maybeBuffer.ok()) {
    FAIL() << "Buffer not created successfully";
  }
  std::shared_ptr<Buffer> bitmapBuffer = *std::move(maybeBuffer);
  uint8_t* bufferData = bitmapBuffer->mutable_data();
  std::memcpy(bufferData, &bitmap, 1);

  auto array = std::make_shared<ListArray>(sharedPtrArrowType, 4, offsetsBuffer,
                                           valuesArray, bitmapBuffer);

  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:array<a:int>>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::ListVectorBatch* x =
      internal::checked_cast<liborc::ListVectorBatch*>(root->fields[0]);
  liborc::LongVectorBatch* a =
      internal::checked_cast<liborc::LongVectorBatch*>((x->elements).get());
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize,
                                       array.get());
  if (!st.ok()) {
    FAIL() << "ORC ColumnBatch not successfully filled";
  }

  EXPECT_EQ(x->numElements, 4);
  EXPECT_TRUE(x->hasNulls);
  EXPECT_EQ(a->numElements, 10);
  EXPECT_TRUE(a->hasNulls);

  EXPECT_EQ(x->notNull[0], 1);
  EXPECT_EQ(x->notNull[1], 0);
  EXPECT_EQ(x->notNull[2], 1);
  EXPECT_EQ(x->notNull[3], 1);

  EXPECT_EQ(x->offsets[0], 0);
  EXPECT_EQ(x->offsets[1], 1);
  EXPECT_EQ(x->offsets[2], 3);
  EXPECT_EQ(x->offsets[3], 6);
  EXPECT_EQ(x->offsets[4], 10);

  EXPECT_EQ(a->notNull[0], 0);
  EXPECT_EQ(a->notNull[1], 1);
  EXPECT_EQ(a->notNull[2], 1);
  EXPECT_EQ(a->notNull[3], 1);
  EXPECT_EQ(a->notNull[4], 0);
  EXPECT_EQ(a->notNull[5], 1);
  EXPECT_EQ(a->notNull[6], 0);
  EXPECT_EQ(a->notNull[7], 1);
  EXPECT_EQ(a->notNull[8], 1);
  EXPECT_EQ(a->notNull[9], 0);

  EXPECT_EQ(a->data[1], 2);
  EXPECT_EQ(a->data[2], 3);
  EXPECT_EQ(a->data[3], 4);
  EXPECT_EQ(a->data[5], 6);
  EXPECT_EQ(a->data[7], 8);
  EXPECT_EQ(a->data[8], 9);

  EXPECT_EQ(arrowOffset, 4);
  EXPECT_EQ(orcOffset, 4);
  writer->add(*batch);
  writer->close();
}
TEST(TestAdapterWriteNested, writeListMixed3) {
  auto sharedPtrArrowType = list(std::make_shared<Field>("a", int32()));
  DataType* arrowType = sharedPtrArrowType.get();

  Int32Builder valuesBuilder, offsetsBuilder;

  int32_t offsets[5] = {0, 1, 3, 3, 7};

  BufferBuilder builder;
  (void)(builder.Resize(20));
  (void)(builder.Append(offsets, 20));
  std::shared_ptr<arrow::Buffer> offsetsBuffer;
  if (!builder.Finish(&offsetsBuffer).ok()) {
    FAIL() << "The offsets buffer can not be constructed!";
  }

  std::shared_ptr<Array> valuesArray;
  (void)(valuesBuilder.AppendNull());
  (void)(valuesBuilder.Append(2));
  (void)(valuesBuilder.Append(3));
  (void)(valuesBuilder.AppendNull());
  (void)(valuesBuilder.Append(8));
  (void)(valuesBuilder.Append(9));
  (void)(valuesBuilder.AppendNull());
  (void)(valuesBuilder.Finish(&valuesArray));

  uint8_t bitmap = 13;  // 00001101
  auto maybeBuffer = AllocateBuffer(1);
  if (!maybeBuffer.ok()) {
    FAIL() << "Buffer not created successfully";
  }
  std::shared_ptr<Buffer> bitmapBuffer = *std::move(maybeBuffer);
  uint8_t* bufferData = bitmapBuffer->mutable_data();
  std::memcpy(bufferData, &bitmap, 1);

  auto array = std::make_shared<ListArray>(sharedPtrArrowType, 4, offsetsBuffer,
                                           valuesArray, bitmapBuffer);

  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:array<a:int>>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::ListVectorBatch* x =
      internal::checked_cast<liborc::ListVectorBatch*>(root->fields[0]);
  liborc::LongVectorBatch* a =
      internal::checked_cast<liborc::LongVectorBatch*>((x->elements).get());
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize,
                                       array.get());
  if (!st.ok()) {
    FAIL() << "ORC ColumnBatch not successfully filled";
  }

  EXPECT_EQ(x->numElements, 4);
  EXPECT_TRUE(x->hasNulls);
  EXPECT_EQ(a->numElements, 7);
  EXPECT_TRUE(a->hasNulls);

  EXPECT_EQ(x->notNull[0], 1);
  EXPECT_EQ(x->notNull[1], 0);
  EXPECT_EQ(x->notNull[2], 1);
  EXPECT_EQ(x->notNull[3], 1);

  EXPECT_EQ(x->offsets[0], 0);
  EXPECT_EQ(x->offsets[1], 1);
  EXPECT_EQ(x->offsets[2], 3);
  EXPECT_EQ(x->offsets[3], 3);
  EXPECT_EQ(x->offsets[4], 7);

  EXPECT_EQ(a->notNull[0], 0);
  EXPECT_EQ(a->notNull[1], 1);
  EXPECT_EQ(a->notNull[2], 1);
  EXPECT_EQ(a->notNull[3], 0);
  EXPECT_EQ(a->notNull[4], 1);
  EXPECT_EQ(a->notNull[5], 1);
  EXPECT_EQ(a->notNull[6], 0);

  EXPECT_EQ(a->data[1], 2);
  EXPECT_EQ(a->data[2], 3);
  EXPECT_EQ(a->data[4], 8);
  EXPECT_EQ(a->data[5], 9);

  EXPECT_EQ(arrowOffset, 4);
  EXPECT_EQ(orcOffset, 4);
  writer->add(*batch);
  writer->close();
}

// LargeList
TEST(TestAdapterWriteNested, writeLargeListNoNulls) {
  auto sharedPtrArrowType = large_list(std::make_shared<Field>("a", int32()));
  DataType* arrowType = sharedPtrArrowType.get();

  Int32Builder valuesBuilder;
  Int64Builder offsetsBuilder;
  std::shared_ptr<Array> valuesArray, offsetsArray;
  (void)(valuesBuilder.Append(1));
  (void)(valuesBuilder.Append(2));
  (void)(valuesBuilder.Append(3));
  (void)(valuesBuilder.Append(4));
  (void)(valuesBuilder.Append(5));
  (void)(valuesBuilder.Append(6));
  (void)(valuesBuilder.Append(7));
  (void)(valuesBuilder.Append(8));
  (void)(valuesBuilder.Append(9));
  (void)(valuesBuilder.Append(10));
  (void)(valuesBuilder.Finish(&valuesArray));
  (void)(offsetsBuilder.Append(0));
  (void)(offsetsBuilder.Append(1));
  (void)(offsetsBuilder.Append(3));
  (void)(offsetsBuilder.Append(6));
  (void)(offsetsBuilder.Append(10));
  (void)(offsetsBuilder.Finish(&offsetsArray));

  std::shared_ptr<LargeListArray> array;

  array = LargeListArray::FromArrays(*offsetsArray, *valuesArray).ValueOrDie();

  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:array<a:int>>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::ListVectorBatch* x =
      internal::checked_cast<liborc::ListVectorBatch*>(root->fields[0]);
  liborc::LongVectorBatch* a =
      internal::checked_cast<liborc::LongVectorBatch*>((x->elements).get());
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize,
                                       array.get());
  if (!st.ok()) {
    FAIL() << "ORC ColumnBatch not successfully filled";
  }

  EXPECT_EQ(x->numElements, 4);
  EXPECT_FALSE(x->hasNulls);
  EXPECT_EQ(a->numElements, 10);
  EXPECT_FALSE(a->hasNulls);

  EXPECT_EQ(x->offsets[0], 0);
  EXPECT_EQ(x->offsets[1], 1);
  EXPECT_EQ(x->offsets[2], 3);
  EXPECT_EQ(x->offsets[3], 6);
  EXPECT_EQ(x->offsets[4], 10);

  EXPECT_EQ(a->data[0], 1);
  EXPECT_EQ(a->data[1], 2);
  EXPECT_EQ(a->data[2], 3);
  EXPECT_EQ(a->data[3], 4);
  EXPECT_EQ(a->data[4], 5);
  EXPECT_EQ(a->data[5], 6);
  EXPECT_EQ(a->data[6], 7);
  EXPECT_EQ(a->data[7], 8);
  EXPECT_EQ(a->data[8], 9);
  EXPECT_EQ(a->data[9], 10);

  EXPECT_EQ(arrowOffset, 4);
  EXPECT_EQ(orcOffset, 4);
  writer->add(*batch);
  writer->close();
}
TEST(TestAdapterWriteNested, writeLargeListMixed1) {
  auto sharedPtrArrowType = large_list(std::make_shared<Field>("a", int32()));
  DataType* arrowType = sharedPtrArrowType.get();

  Int32Builder valuesBuilder;
  Int64Builder offsetsBuilder;
  std::shared_ptr<Array> valuesArray, offsetsArray;
  (void)(valuesBuilder.AppendNull());
  (void)(valuesBuilder.Append(2));
  (void)(valuesBuilder.Append(3));
  (void)(valuesBuilder.Append(4));
  (void)(valuesBuilder.AppendNull());
  (void)(valuesBuilder.Append(6));
  (void)(valuesBuilder.AppendNull());
  (void)(valuesBuilder.Append(8));
  (void)(valuesBuilder.Append(9));
  (void)(valuesBuilder.AppendNull());
  (void)(valuesBuilder.Finish(&valuesArray));
  (void)(offsetsBuilder.Append(0));
  (void)(offsetsBuilder.Append(1));
  (void)(offsetsBuilder.Append(3));
  (void)(offsetsBuilder.Append(6));
  (void)(offsetsBuilder.Append(10));
  (void)(offsetsBuilder.Finish(&offsetsArray));

  std::shared_ptr<LargeListArray> array;

  array = LargeListArray::FromArrays(*offsetsArray, *valuesArray).ValueOrDie();

  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:array<a:int>>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::ListVectorBatch* x =
      internal::checked_cast<liborc::ListVectorBatch*>(root->fields[0]);
  liborc::LongVectorBatch* a =
      internal::checked_cast<liborc::LongVectorBatch*>((x->elements).get());
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize,
                                       array.get());
  if (!st.ok()) {
    FAIL() << "ORC ColumnBatch not successfully filled";
  }

  EXPECT_EQ(x->numElements, 4);
  EXPECT_FALSE(x->hasNulls);
  EXPECT_EQ(a->numElements, 10);
  EXPECT_TRUE(a->hasNulls);

  EXPECT_EQ(x->offsets[0], 0);
  EXPECT_EQ(x->offsets[1], 1);
  EXPECT_EQ(x->offsets[2], 3);
  EXPECT_EQ(x->offsets[3], 6);
  EXPECT_EQ(x->offsets[4], 10);

  EXPECT_EQ(a->notNull[0], 0);
  EXPECT_EQ(a->notNull[1], 1);
  EXPECT_EQ(a->notNull[2], 1);
  EXPECT_EQ(a->notNull[3], 1);
  EXPECT_EQ(a->notNull[4], 0);
  EXPECT_EQ(a->notNull[5], 1);
  EXPECT_EQ(a->notNull[6], 0);
  EXPECT_EQ(a->notNull[7], 1);
  EXPECT_EQ(a->notNull[8], 1);
  EXPECT_EQ(a->notNull[9], 0);

  EXPECT_EQ(a->data[1], 2);
  EXPECT_EQ(a->data[2], 3);
  EXPECT_EQ(a->data[3], 4);
  EXPECT_EQ(a->data[5], 6);
  EXPECT_EQ(a->data[7], 8);
  EXPECT_EQ(a->data[8], 9);

  EXPECT_EQ(arrowOffset, 4);
  EXPECT_EQ(orcOffset, 4);
  writer->add(*batch);
  writer->close();
}
TEST(TestAdapterWriteNested, writeLargeListMixed2) {
  auto sharedPtrArrowType = large_list(std::make_shared<Field>("a", int32()));
  DataType* arrowType = sharedPtrArrowType.get();

  Int32Builder valuesBuilder;
  Int64Builder offsetsBuilder;
  int64_t offsets[5] = {0, 1, 3, 6, 10};

  BufferBuilder builder;
  (void)(builder.Resize(40));
  (void)(builder.Append(offsets, 40));
  std::shared_ptr<arrow::Buffer> offsetsBuffer;
  if (!builder.Finish(&offsetsBuffer).ok()) {
    FAIL() << "The offsets buffer can not be constructed!";
  }

  std::shared_ptr<Array> valuesArray;
  (void)(valuesBuilder.AppendNull());
  (void)(valuesBuilder.Append(2));
  (void)(valuesBuilder.Append(3));
  (void)(valuesBuilder.Append(4));
  (void)(valuesBuilder.AppendNull());
  (void)(valuesBuilder.Append(6));
  (void)(valuesBuilder.AppendNull());
  (void)(valuesBuilder.Append(8));
  (void)(valuesBuilder.Append(9));
  (void)(valuesBuilder.AppendNull());
  (void)(valuesBuilder.Finish(&valuesArray));

  uint8_t bitmap = 13;  // 00001101
  auto maybeBuffer = AllocateBuffer(1);
  if (!maybeBuffer.ok()) {
    FAIL() << "Buffer not created successfully";
  }
  std::shared_ptr<Buffer> bitmapBuffer = *std::move(maybeBuffer);
  uint8_t* bufferData = bitmapBuffer->mutable_data();
  std::memcpy(bufferData, &bitmap, 1);

  auto array = std::make_shared<LargeListArray>(sharedPtrArrowType, 4, offsetsBuffer,
                                                valuesArray, bitmapBuffer);

  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:array<a:int>>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::ListVectorBatch* x =
      internal::checked_cast<liborc::ListVectorBatch*>(root->fields[0]);
  liborc::LongVectorBatch* a =
      internal::checked_cast<liborc::LongVectorBatch*>((x->elements).get());
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize,
                                       array.get());
  if (!st.ok()) {
    FAIL() << "ORC ColumnBatch not successfully filled";
  }

  EXPECT_EQ(x->numElements, 4);
  EXPECT_TRUE(x->hasNulls);
  EXPECT_EQ(a->numElements, 10);
  EXPECT_TRUE(a->hasNulls);

  EXPECT_EQ(x->notNull[0], 1);
  EXPECT_EQ(x->notNull[1], 0);
  EXPECT_EQ(x->notNull[2], 1);
  EXPECT_EQ(x->notNull[3], 1);

  EXPECT_EQ(x->offsets[0], 0);
  EXPECT_EQ(x->offsets[1], 1);
  EXPECT_EQ(x->offsets[2], 3);
  EXPECT_EQ(x->offsets[3], 6);
  EXPECT_EQ(x->offsets[4], 10);

  EXPECT_EQ(a->notNull[0], 0);
  EXPECT_EQ(a->notNull[1], 1);
  EXPECT_EQ(a->notNull[2], 1);
  EXPECT_EQ(a->notNull[3], 1);
  EXPECT_EQ(a->notNull[4], 0);
  EXPECT_EQ(a->notNull[5], 1);
  EXPECT_EQ(a->notNull[6], 0);
  EXPECT_EQ(a->notNull[7], 1);
  EXPECT_EQ(a->notNull[8], 1);
  EXPECT_EQ(a->notNull[9], 0);

  EXPECT_EQ(a->data[1], 2);
  EXPECT_EQ(a->data[2], 3);
  EXPECT_EQ(a->data[3], 4);
  EXPECT_EQ(a->data[5], 6);
  EXPECT_EQ(a->data[7], 8);
  EXPECT_EQ(a->data[8], 9);

  EXPECT_EQ(arrowOffset, 4);
  EXPECT_EQ(orcOffset, 4);
  writer->add(*batch);
  writer->close();
}
TEST(TestAdapterWriteNested, writeLargeListMixed3) {
  auto sharedPtrArrowType = large_list(std::make_shared<Field>("a", int32()));
  DataType* arrowType = sharedPtrArrowType.get();

  Int32Builder valuesBuilder;
  Int64Builder offsetsBuilder;
  int64_t offsets[5] = {0, 1, 3, 3, 7};

  BufferBuilder builder;
  (void)(builder.Resize(40));
  (void)(builder.Append(offsets, 40));
  std::shared_ptr<arrow::Buffer> offsetsBuffer;
  if (!builder.Finish(&offsetsBuffer).ok()) {
    FAIL() << "The offsets buffer can not be constructed!";
  }

  std::shared_ptr<Array> valuesArray;
  (void)(valuesBuilder.AppendNull());
  (void)(valuesBuilder.Append(2));
  (void)(valuesBuilder.Append(3));
  (void)(valuesBuilder.AppendNull());
  (void)(valuesBuilder.Append(8));
  (void)(valuesBuilder.Append(9));
  (void)(valuesBuilder.AppendNull());
  (void)(valuesBuilder.Finish(&valuesArray));

  uint8_t bitmap = 13;  // 00001101
  auto maybeBuffer = AllocateBuffer(1);
  if (!maybeBuffer.ok()) {
    FAIL() << "Buffer not created successfully";
  }
  std::shared_ptr<Buffer> bitmapBuffer = *std::move(maybeBuffer);
  uint8_t* bufferData = bitmapBuffer->mutable_data();
  std::memcpy(bufferData, &bitmap, 1);

  auto array = std::make_shared<LargeListArray>(sharedPtrArrowType, 4, offsetsBuffer,
                                                valuesArray, bitmapBuffer);

  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:array<a:int>>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::ListVectorBatch* x =
      internal::checked_cast<liborc::ListVectorBatch*>(root->fields[0]);
  liborc::LongVectorBatch* a =
      internal::checked_cast<liborc::LongVectorBatch*>((x->elements).get());
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize,
                                       array.get());
  if (!st.ok()) {
    FAIL() << "ORC ColumnBatch not successfully filled";
  }

  EXPECT_EQ(x->numElements, 4);
  EXPECT_TRUE(x->hasNulls);
  EXPECT_EQ(a->numElements, 7);
  EXPECT_TRUE(a->hasNulls);

  EXPECT_EQ(x->notNull[0], 1);
  EXPECT_EQ(x->notNull[1], 0);
  EXPECT_EQ(x->notNull[2], 1);
  EXPECT_EQ(x->notNull[3], 1);

  EXPECT_EQ(x->offsets[0], 0);
  EXPECT_EQ(x->offsets[1], 1);
  EXPECT_EQ(x->offsets[2], 3);
  EXPECT_EQ(x->offsets[3], 3);
  EXPECT_EQ(x->offsets[4], 7);

  EXPECT_EQ(a->notNull[0], 0);
  EXPECT_EQ(a->notNull[1], 1);
  EXPECT_EQ(a->notNull[2], 1);
  EXPECT_EQ(a->notNull[3], 0);
  EXPECT_EQ(a->notNull[4], 1);
  EXPECT_EQ(a->notNull[5], 1);
  EXPECT_EQ(a->notNull[6], 0);

  EXPECT_EQ(a->data[1], 2);
  EXPECT_EQ(a->data[2], 3);
  EXPECT_EQ(a->data[4], 8);
  EXPECT_EQ(a->data[5], 9);

  EXPECT_EQ(arrowOffset, 4);
  EXPECT_EQ(orcOffset, 4);
  writer->add(*batch);
  writer->close();
}

// FixedSizeList
TEST(TestAdapterWriteNested, writeFixedSizeListEmpty) {
  auto sharedPtrArrowType = fixed_size_list(std::make_shared<Field>("a", int32()), 3);
  DataType* arrowType = sharedPtrArrowType.get();

  Int32Builder valuesBuilder;
  std::shared_ptr<Array> valuesArray;
  (void)(valuesBuilder.Finish(&valuesArray));

  std::shared_ptr<FixedSizeListArray> array =
      std::static_pointer_cast<FixedSizeListArray>(
          FixedSizeListArray::FromArrays(valuesArray, 3).ValueOrDie());

  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:array<a:int>>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::ListVectorBatch* x =
      internal::checked_cast<liborc::ListVectorBatch*>(root->fields[0]);
  liborc::LongVectorBatch* a =
      internal::checked_cast<liborc::LongVectorBatch*>((x->elements).get());
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize,
                                       array.get());
  if (!st.ok()) {
    FAIL() << "ORC ColumnBatch not successfully filled";
  }

  EXPECT_EQ(x->numElements, 0);
  EXPECT_FALSE(x->hasNulls);
  EXPECT_EQ(a->numElements, 0);
  EXPECT_FALSE(a->hasNulls);

  EXPECT_EQ(x->offsets[0], 0);

  EXPECT_EQ(arrowOffset, 0);
  EXPECT_EQ(orcOffset, 0);
  writer->add(*batch);
  writer->close();
}
TEST(TestAdapterWriteNested, writeFixedSizeListAllNulls) {
  auto sharedPtrArrowType = fixed_size_list(std::make_shared<Field>("a", int32()), 3);
  DataType* arrowType = sharedPtrArrowType.get();

  Int32Builder valuesBuilder;
  std::shared_ptr<Array> valuesArray;
  (void)(valuesBuilder.AppendNull());
  (void)(valuesBuilder.AppendNull());
  (void)(valuesBuilder.AppendNull());
  (void)(valuesBuilder.AppendNull());
  (void)(valuesBuilder.AppendNull());
  (void)(valuesBuilder.AppendNull());
  (void)(valuesBuilder.Finish(&valuesArray));

  std::shared_ptr<FixedSizeListArray> array =
      std::static_pointer_cast<FixedSizeListArray>(
          FixedSizeListArray::FromArrays(valuesArray, 3).ValueOrDie());

  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:array<a:int>>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::ListVectorBatch* x =
      internal::checked_cast<liborc::ListVectorBatch*>(root->fields[0]);
  liborc::LongVectorBatch* a =
      internal::checked_cast<liborc::LongVectorBatch*>((x->elements).get());
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize,
                                       array.get());
  if (!st.ok()) {
    FAIL() << "ORC ColumnBatch not successfully filled";
  }

  EXPECT_EQ(x->numElements, 2);
  EXPECT_FALSE(x->hasNulls);
  EXPECT_EQ(a->numElements, 6);
  EXPECT_TRUE(a->hasNulls);

  EXPECT_EQ(x->offsets[0], 0);
  EXPECT_EQ(x->offsets[1], 3);
  EXPECT_EQ(x->offsets[2], 6);

  EXPECT_EQ(a->notNull[0], 0);
  EXPECT_EQ(a->notNull[1], 0);
  EXPECT_EQ(a->notNull[2], 0);
  EXPECT_EQ(a->notNull[3], 0);
  EXPECT_EQ(a->notNull[4], 0);
  EXPECT_EQ(a->notNull[5], 0);

  EXPECT_EQ(arrowOffset, 2);
  EXPECT_EQ(orcOffset, 2);
  writer->add(*batch);
  writer->close();
}
TEST(TestAdapterWriteNested, writeFixedSizeListZeroLength) {
  auto sharedPtrArrowType = fixed_size_list(std::make_shared<Field>("a", int32()), 0);
  DataType* arrowType = sharedPtrArrowType.get();

  Int32Builder valuesBuilder;
  std::shared_ptr<Array> valuesArray;
  (void)(valuesBuilder.Finish(&valuesArray));

  auto array = std::make_shared<FixedSizeListArray>(sharedPtrArrowType, 3, valuesArray);

  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:array<a:int>>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::ListVectorBatch* x =
      internal::checked_cast<liborc::ListVectorBatch*>(root->fields[0]);
  liborc::LongVectorBatch* a =
      internal::checked_cast<liborc::LongVectorBatch*>((x->elements).get());
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize,
                                       array.get());
  if (!st.ok()) {
    FAIL() << "ORC ColumnBatch not successfully filled";
  }

  EXPECT_EQ(x->numElements, 3);
  EXPECT_FALSE(x->hasNulls);
  EXPECT_EQ(a->numElements, 0);
  EXPECT_FALSE(a->hasNulls);

  EXPECT_EQ(x->offsets[0], 0);

  EXPECT_EQ(arrowOffset, 3);
  EXPECT_EQ(orcOffset, 3);
  writer->add(*batch);
  writer->close();
}
TEST(TestAdapterWriteNested, writeFixedSizeListNoNulls) {
  auto sharedPtrArrowType = fixed_size_list(std::make_shared<Field>("a", int32()), 3);
  DataType* arrowType = sharedPtrArrowType.get();

  Int32Builder valuesBuilder;
  std::shared_ptr<Array> valuesArray;
  (void)(valuesBuilder.Append(1));
  (void)(valuesBuilder.Append(2));
  (void)(valuesBuilder.Append(3));
  (void)(valuesBuilder.Append(4));
  (void)(valuesBuilder.Append(5));
  (void)(valuesBuilder.Append(6));
  (void)(valuesBuilder.Append(7));
  (void)(valuesBuilder.Append(8));
  (void)(valuesBuilder.Append(9));
  (void)(valuesBuilder.Append(10));
  (void)(valuesBuilder.Append(11));
  (void)(valuesBuilder.Append(12));
  (void)(valuesBuilder.Finish(&valuesArray));

  std::shared_ptr<FixedSizeListArray> array =
      std::static_pointer_cast<FixedSizeListArray>(
          FixedSizeListArray::FromArrays(valuesArray, 3).ValueOrDie());

  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:array<a:int>>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::ListVectorBatch* x =
      internal::checked_cast<liborc::ListVectorBatch*>(root->fields[0]);
  liborc::LongVectorBatch* a =
      internal::checked_cast<liborc::LongVectorBatch*>((x->elements).get());
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize,
                                       array.get());
  if (!st.ok()) {
    FAIL() << "ORC ColumnBatch not successfully filled";
  }

  EXPECT_EQ(x->numElements, 4);
  EXPECT_FALSE(x->hasNulls);
  EXPECT_EQ(a->numElements, 12);
  EXPECT_FALSE(a->hasNulls);

  EXPECT_EQ(x->offsets[0], 0);
  EXPECT_EQ(x->offsets[1], 3);
  EXPECT_EQ(x->offsets[2], 6);
  EXPECT_EQ(x->offsets[3], 9);
  EXPECT_EQ(x->offsets[4], 12);

  EXPECT_EQ(a->data[0], 1);
  EXPECT_EQ(a->data[1], 2);
  EXPECT_EQ(a->data[2], 3);
  EXPECT_EQ(a->data[3], 4);
  EXPECT_EQ(a->data[4], 5);
  EXPECT_EQ(a->data[5], 6);
  EXPECT_EQ(a->data[6], 7);
  EXPECT_EQ(a->data[7], 8);
  EXPECT_EQ(a->data[8], 9);
  EXPECT_EQ(a->data[9], 10);
  EXPECT_EQ(a->data[10], 11);
  EXPECT_EQ(a->data[11], 12);

  EXPECT_EQ(arrowOffset, 4);
  EXPECT_EQ(orcOffset, 4);
  writer->add(*batch);
  writer->close();
}
TEST(TestAdapterWriteNested, writeFixedSizeListMixed1) {
  auto sharedPtrArrowType = fixed_size_list(std::make_shared<Field>("a", int32()), 3);
  DataType* arrowType = sharedPtrArrowType.get();

  Int32Builder valuesBuilder;
  std::shared_ptr<Array> valuesArray;
  (void)(valuesBuilder.AppendNull());
  (void)(valuesBuilder.Append(2));
  (void)(valuesBuilder.Append(3));
  (void)(valuesBuilder.Append(4));
  (void)(valuesBuilder.AppendNull());
  (void)(valuesBuilder.Append(6));
  (void)(valuesBuilder.AppendNull());
  (void)(valuesBuilder.Append(8));
  (void)(valuesBuilder.Append(9));
  (void)(valuesBuilder.AppendNull());
  (void)(valuesBuilder.Append(11));
  (void)(valuesBuilder.Append(12));
  (void)(valuesBuilder.Finish(&valuesArray));

  std::shared_ptr<FixedSizeListArray> array =
      std::static_pointer_cast<FixedSizeListArray>(
          FixedSizeListArray::FromArrays(valuesArray, 3).ValueOrDie());

  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:array<a:int>>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::ListVectorBatch* x =
      internal::checked_cast<liborc::ListVectorBatch*>(root->fields[0]);
  liborc::LongVectorBatch* a =
      internal::checked_cast<liborc::LongVectorBatch*>((x->elements).get());
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize,
                                       array.get());
  if (!st.ok()) {
    FAIL() << "ORC ColumnBatch not successfully filled";
  }

  EXPECT_EQ(x->numElements, 4);
  EXPECT_FALSE(x->hasNulls);
  EXPECT_EQ(a->numElements, 12);
  EXPECT_TRUE(a->hasNulls);

  EXPECT_EQ(x->offsets[0], 0);
  EXPECT_EQ(x->offsets[1], 3);
  EXPECT_EQ(x->offsets[2], 6);
  EXPECT_EQ(x->offsets[3], 9);
  EXPECT_EQ(x->offsets[4], 12);

  EXPECT_EQ(a->notNull[0], 0);
  EXPECT_EQ(a->notNull[1], 1);
  EXPECT_EQ(a->notNull[2], 1);
  EXPECT_EQ(a->notNull[3], 1);
  EXPECT_EQ(a->notNull[4], 0);
  EXPECT_EQ(a->notNull[5], 1);
  EXPECT_EQ(a->notNull[6], 0);
  EXPECT_EQ(a->notNull[7], 1);
  EXPECT_EQ(a->notNull[8], 1);
  EXPECT_EQ(a->notNull[9], 0);
  EXPECT_EQ(a->notNull[10], 1);
  EXPECT_EQ(a->notNull[11], 1);

  EXPECT_EQ(a->data[1], 2);
  EXPECT_EQ(a->data[2], 3);
  EXPECT_EQ(a->data[3], 4);
  EXPECT_EQ(a->data[5], 6);
  EXPECT_EQ(a->data[7], 8);
  EXPECT_EQ(a->data[8], 9);
  EXPECT_EQ(a->data[10], 11);
  EXPECT_EQ(a->data[11], 12);

  EXPECT_EQ(arrowOffset, 4);
  EXPECT_EQ(orcOffset, 4);
  writer->add(*batch);
  writer->close();
}
TEST(TestAdapterWriteNested, writeFixedSizeListMixed2) {
  auto sharedPtrArrowType = fixed_size_list(std::make_shared<Field>("a", int32()), 3);
  DataType* arrowType = sharedPtrArrowType.get();

  Int32Builder valuesBuilder;

  std::shared_ptr<Array> valuesArray;
  (void)(valuesBuilder.AppendNull());
  (void)(valuesBuilder.Append(2));
  (void)(valuesBuilder.Append(3));
  (void)(valuesBuilder.Append(4));
  (void)(valuesBuilder.AppendNull());
  (void)(valuesBuilder.Append(6));
  (void)(valuesBuilder.AppendNull());
  (void)(valuesBuilder.Append(8));
  (void)(valuesBuilder.Append(9));
  (void)(valuesBuilder.AppendNull());
  (void)(valuesBuilder.Append(11));
  (void)(valuesBuilder.Append(12));
  (void)(valuesBuilder.Finish(&valuesArray));

  uint8_t bitmap = 13;  // 00001101
  auto maybeBuffer = AllocateBuffer(1);
  if (!maybeBuffer.ok()) {
    FAIL() << "Buffer not created successfully";
  }
  std::shared_ptr<Buffer> bitmapBuffer = *std::move(maybeBuffer);
  uint8_t* bufferData = bitmapBuffer->mutable_data();
  std::memcpy(bufferData, &bitmap, 1);

  auto array = std::make_shared<FixedSizeListArray>(sharedPtrArrowType, 4, valuesArray,
                                                    bitmapBuffer);

  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:array<a:int>>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::ListVectorBatch* x =
      internal::checked_cast<liborc::ListVectorBatch*>(root->fields[0]);
  liborc::LongVectorBatch* a =
      internal::checked_cast<liborc::LongVectorBatch*>((x->elements).get());
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize,
                                       array.get());
  if (!st.ok()) {
    FAIL() << "ORC ColumnBatch not successfully filled";
  }

  EXPECT_EQ(x->numElements, 4);
  EXPECT_TRUE(x->hasNulls);
  EXPECT_EQ(a->numElements, 12);
  EXPECT_TRUE(a->hasNulls);

  EXPECT_EQ(x->notNull[0], 1);
  EXPECT_EQ(x->notNull[1], 0);
  EXPECT_EQ(x->notNull[2], 1);
  EXPECT_EQ(x->notNull[3], 1);

  EXPECT_EQ(x->offsets[0], 0);
  EXPECT_EQ(x->offsets[1], 3);
  EXPECT_EQ(x->offsets[2], 6);
  EXPECT_EQ(x->offsets[3], 9);
  EXPECT_EQ(x->offsets[4], 12);

  EXPECT_EQ(a->notNull[0], 0);
  EXPECT_EQ(a->notNull[1], 1);
  EXPECT_EQ(a->notNull[2], 1);
  EXPECT_EQ(a->notNull[3], 1);
  EXPECT_EQ(a->notNull[4], 0);
  EXPECT_EQ(a->notNull[5], 1);
  EXPECT_EQ(a->notNull[6], 0);
  EXPECT_EQ(a->notNull[7], 1);
  EXPECT_EQ(a->notNull[8], 1);
  EXPECT_EQ(a->notNull[9], 0);
  EXPECT_EQ(a->notNull[10], 1);
  EXPECT_EQ(a->notNull[11], 1);

  EXPECT_EQ(a->data[1], 2);
  EXPECT_EQ(a->data[2], 3);
  EXPECT_EQ(a->data[3], 4);
  EXPECT_EQ(a->data[5], 6);
  EXPECT_EQ(a->data[7], 8);
  EXPECT_EQ(a->data[8], 9);
  EXPECT_EQ(a->data[10], 11);
  EXPECT_EQ(a->data[11], 12);

  EXPECT_EQ(arrowOffset, 4);
  EXPECT_EQ(orcOffset, 4);
  writer->add(*batch);
  writer->close();
}

// Map
TEST(TestAdapterWriteNested, writeMapEmpty) {
  auto sharedPtrArrowType = map(utf8(), utf8());
  DataType* arrowType = sharedPtrArrowType.get();

  Int32Builder offsetsBuilder;
  StringBuilder keysBuilder, itemsBuilder;
  std::shared_ptr<Array> offsetsArray, keysArray, itemsArray;
  (void)(keysBuilder.Finish(&keysArray));
  (void)(itemsBuilder.Finish(&itemsArray));
  (void)(offsetsBuilder.Append(0));
  (void)(offsetsBuilder.Finish(&offsetsArray));

  std::shared_ptr<MapArray> array = std::static_pointer_cast<MapArray>(
      MapArray::FromArrays(offsetsArray, keysArray, itemsArray).ValueOrDie());

  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:map<a:string,b:string>>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::MapVectorBatch* x =
      internal::checked_cast<liborc::MapVectorBatch*>(root->fields[0]);
  liborc::StringVectorBatch* a =
      internal::checked_cast<liborc::StringVectorBatch*>((x->keys).get());
  liborc::StringVectorBatch* b =
      internal::checked_cast<liborc::StringVectorBatch*>((x->elements).get());
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize,
                                       array.get());
  if (!st.ok()) {
    FAIL() << "ORC ColumnBatch not successfully filled";
  }

  EXPECT_EQ(x->numElements, 0);
  EXPECT_FALSE(x->hasNulls);
  EXPECT_EQ(a->numElements, 0);
  EXPECT_FALSE(a->hasNulls);
  EXPECT_EQ(b->numElements, 0);
  EXPECT_FALSE(b->hasNulls);

  EXPECT_EQ(x->offsets[0], 0);

  EXPECT_EQ(arrowOffset, 0);
  EXPECT_EQ(orcOffset, 0);
  writer->add(*batch);
  writer->close();
}
TEST(TestAdapterWriteNested, writeMapNoNulls) {
  auto sharedPtrArrowType = map(utf8(), utf8());
  DataType* arrowType = sharedPtrArrowType.get();

  Int32Builder offsetsBuilder;
  StringBuilder keysBuilder, itemsBuilder;
  std::shared_ptr<Array> offsetsArray, keysArray, itemsArray;
  (void)(keysBuilder.Append("AB"));
  (void)(keysBuilder.Append(""));
  (void)(keysBuilder.Append("CDE"));
  (void)(keysBuilder.Append("F C"));
  (void)(keysBuilder.Append("AA"));
  (void)(keysBuilder.Finish(&keysArray));
  (void)(itemsBuilder.Append("CM"));
  (void)(itemsBuilder.Append("wdw"));
  (void)(itemsBuilder.Append("dsw7"));
  (void)(itemsBuilder.Append("872"));
  (void)(itemsBuilder.Append("dww"));
  (void)(itemsBuilder.Finish(&itemsArray));
  (void)(offsetsBuilder.Append(0));
  (void)(offsetsBuilder.Append(1));
  (void)(offsetsBuilder.Append(3));
  (void)(offsetsBuilder.Append(5));
  (void)(offsetsBuilder.Finish(&offsetsArray));

  std::shared_ptr<MapArray> array = std::static_pointer_cast<MapArray>(
      MapArray::FromArrays(offsetsArray, keysArray, itemsArray).ValueOrDie());

  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:map<a:string,b:string>>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::MapVectorBatch* x =
      internal::checked_cast<liborc::MapVectorBatch*>(root->fields[0]);
  liborc::StringVectorBatch* a =
      internal::checked_cast<liborc::StringVectorBatch*>((x->keys).get());
  liborc::StringVectorBatch* b =
      internal::checked_cast<liborc::StringVectorBatch*>((x->elements).get());
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize,
                                       array.get());
  if (!st.ok()) {
    FAIL() << "ORC ColumnBatch not successfully filled";
  }

  EXPECT_EQ(x->numElements, 3);
  EXPECT_FALSE(x->hasNulls);
  EXPECT_EQ(a->numElements, 5);
  EXPECT_FALSE(a->hasNulls);
  EXPECT_EQ(b->numElements, 5);
  EXPECT_FALSE(b->hasNulls);

  EXPECT_EQ(x->offsets[0], 0);
  EXPECT_EQ(x->offsets[1], 1);
  EXPECT_EQ(x->offsets[2], 3);
  EXPECT_EQ(x->offsets[3], 5);

  EXPECT_EQ(a->length[0], 2);
  EXPECT_EQ(a->length[1], 0);
  EXPECT_EQ(a->length[2], 3);
  EXPECT_EQ(a->length[3], 3);
  EXPECT_EQ(a->length[4], 2);
  EXPECT_EQ(b->length[0], 2);
  EXPECT_EQ(b->length[1], 3);
  EXPECT_EQ(b->length[2], 4);
  EXPECT_EQ(b->length[3], 3);
  EXPECT_EQ(b->length[4], 3);

  EXPECT_STREQ(a->data[0], "AB");
  EXPECT_STREQ(a->data[1], "");
  EXPECT_STREQ(a->data[2], "CDE");
  EXPECT_STREQ(a->data[3], "F C");
  EXPECT_STREQ(a->data[4], "AA");
  EXPECT_STREQ(b->data[0], "CM");
  EXPECT_STREQ(b->data[1], "wdw");
  EXPECT_STREQ(b->data[2], "dsw7");
  EXPECT_STREQ(b->data[3], "872");
  EXPECT_STREQ(b->data[4], "dww");

  EXPECT_EQ(arrowOffset, 3);
  EXPECT_EQ(orcOffset, 3);
  writer->add(*batch);
  writer->close();
}
TEST(TestAdapterWriteNested, writeMapMixed1) {
  auto sharedPtrArrowType = map(utf8(), utf8());
  DataType* arrowType = sharedPtrArrowType.get();

  Int32Builder offsetsBuilder;
  StringBuilder keysBuilder, itemsBuilder;
  std::shared_ptr<Array> offsetsArray, keysArray, itemsArray;
  (void)(keysBuilder.Append("AB"));
  (void)(keysBuilder.Append(""));
  (void)(keysBuilder.Append("CDE"));
  (void)(keysBuilder.Append("F C"));
  (void)(keysBuilder.Append("AA"));
  (void)(keysBuilder.Finish(&keysArray));
  (void)(itemsBuilder.AppendNull());
  (void)(itemsBuilder.Append("wdw"));
  (void)(itemsBuilder.Append("dsw7"));
  (void)(itemsBuilder.Append("872"));
  (void)(itemsBuilder.AppendNull());
  (void)(itemsBuilder.Finish(&itemsArray));
  (void)(offsetsBuilder.Append(0));
  (void)(offsetsBuilder.Append(1));
  (void)(offsetsBuilder.Append(3));
  (void)(offsetsBuilder.Append(5));
  (void)(offsetsBuilder.Finish(&offsetsArray));

  std::shared_ptr<MapArray> array = std::static_pointer_cast<MapArray>(
      MapArray::FromArrays(offsetsArray, keysArray, itemsArray).ValueOrDie());

  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:map<a:string,b:string>>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::MapVectorBatch* x =
      internal::checked_cast<liborc::MapVectorBatch*>(root->fields[0]);
  liborc::StringVectorBatch* a =
      internal::checked_cast<liborc::StringVectorBatch*>((x->keys).get());
  liborc::StringVectorBatch* b =
      internal::checked_cast<liborc::StringVectorBatch*>((x->elements).get());
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize,
                                       array.get());
  if (!st.ok()) {
    FAIL() << "ORC ColumnBatch not successfully filled";
  }

  EXPECT_EQ(x->numElements, 3);
  EXPECT_FALSE(x->hasNulls);
  EXPECT_EQ(a->numElements, 5);
  EXPECT_FALSE(a->hasNulls);
  EXPECT_EQ(b->numElements, 5);
  EXPECT_TRUE(b->hasNulls);

  EXPECT_EQ(b->notNull[0], 0);
  EXPECT_EQ(b->notNull[1], 1);
  EXPECT_EQ(b->notNull[2], 1);
  EXPECT_EQ(b->notNull[3], 1);
  EXPECT_EQ(b->notNull[4], 0);

  EXPECT_EQ(x->offsets[0], 0);
  EXPECT_EQ(x->offsets[1], 1);
  EXPECT_EQ(x->offsets[2], 3);
  EXPECT_EQ(x->offsets[3], 5);

  EXPECT_EQ(a->length[0], 2);
  EXPECT_EQ(a->length[1], 0);
  EXPECT_EQ(a->length[2], 3);
  EXPECT_EQ(a->length[3], 3);
  EXPECT_EQ(a->length[4], 2);
  EXPECT_EQ(b->length[1], 3);
  EXPECT_EQ(b->length[2], 4);
  EXPECT_EQ(b->length[3], 3);

  EXPECT_STREQ(a->data[0], "AB");
  EXPECT_STREQ(a->data[1], "");
  EXPECT_STREQ(a->data[2], "CDE");
  EXPECT_STREQ(a->data[3], "F C");
  EXPECT_STREQ(a->data[4], "AA");
  EXPECT_STREQ(b->data[1], "wdw");
  EXPECT_STREQ(b->data[2], "dsw7");
  EXPECT_STREQ(b->data[3], "872");

  EXPECT_EQ(arrowOffset, 3);
  EXPECT_EQ(orcOffset, 3);
  writer->add(*batch);
  writer->close();
}
TEST(TestAdapterWriteNested, writeMapMixed2) {
  auto sharedPtrArrowType = map(utf8(), utf8());
  DataType* arrowType = sharedPtrArrowType.get();

  StringBuilder keysBuilder, itemsBuilder;
  std::shared_ptr<Array> keysArray, itemsArray;

  int32_t offsets[4] = {0, 1, 3, 5};

  BufferBuilder builder;
  (void)(builder.Resize(16));
  (void)(builder.Append(offsets, 16));
  std::shared_ptr<arrow::Buffer> offsetsBuffer;
  if (!builder.Finish(&offsetsBuffer).ok()) {
    FAIL() << "The offsets buffer can not be constructed!";
  }

  (void)(keysBuilder.Append("AB"));
  (void)(keysBuilder.Append(""));
  (void)(keysBuilder.Append("CDE"));
  (void)(keysBuilder.Append("F C"));
  (void)(keysBuilder.Append("AA"));
  (void)(keysBuilder.Finish(&keysArray));
  (void)(itemsBuilder.AppendNull());
  (void)(itemsBuilder.Append("wdw"));
  (void)(itemsBuilder.Append("dsw7"));
  (void)(itemsBuilder.Append("872"));
  (void)(itemsBuilder.AppendNull());
  (void)(itemsBuilder.Finish(&itemsArray));

  uint8_t bitmap = 5;  // 00000101
  auto maybeBuffer = AllocateBuffer(1);
  if (!maybeBuffer.ok()) {
    FAIL() << "Buffer not created successfully";
  }
  std::shared_ptr<Buffer> bitmapBuffer = *std::move(maybeBuffer);
  uint8_t* bufferData = bitmapBuffer->mutable_data();
  std::memcpy(bufferData, &bitmap, 1);

  auto array = std::make_shared<MapArray>(sharedPtrArrowType, 3, offsetsBuffer, keysArray,
                                          itemsArray, bitmapBuffer);

  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:map<a:string,b:string>>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::MapVectorBatch* x =
      internal::checked_cast<liborc::MapVectorBatch*>(root->fields[0]);
  liborc::StringVectorBatch* a =
      internal::checked_cast<liborc::StringVectorBatch*>((x->keys).get());
  liborc::StringVectorBatch* b =
      internal::checked_cast<liborc::StringVectorBatch*>((x->elements).get());
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize,
                                       array.get());
  if (!st.ok()) {
    FAIL() << "ORC ColumnBatch not successfully filled";
  }

  EXPECT_EQ(x->numElements, 3);
  EXPECT_TRUE(x->hasNulls);
  EXPECT_EQ(a->numElements, 5);
  EXPECT_FALSE(a->hasNulls);
  EXPECT_EQ(b->numElements, 5);
  EXPECT_TRUE(b->hasNulls);

  EXPECT_EQ(x->notNull[0], 1);
  EXPECT_EQ(x->notNull[1], 0);
  EXPECT_EQ(x->notNull[2], 1);
  EXPECT_EQ(b->notNull[0], 0);
  EXPECT_EQ(b->notNull[1], 1);
  EXPECT_EQ(b->notNull[2], 1);
  EXPECT_EQ(b->notNull[3], 1);
  EXPECT_EQ(b->notNull[4], 0);

  EXPECT_EQ(x->offsets[0], 0);
  EXPECT_EQ(x->offsets[1], 1);
  EXPECT_EQ(x->offsets[2], 3);
  EXPECT_EQ(x->offsets[3], 5);

  EXPECT_EQ(a->length[0], 2);
  EXPECT_EQ(a->length[1], 0);
  EXPECT_EQ(a->length[2], 3);
  EXPECT_EQ(a->length[3], 3);
  EXPECT_EQ(a->length[4], 2);
  EXPECT_EQ(b->length[1], 3);
  EXPECT_EQ(b->length[2], 4);
  EXPECT_EQ(b->length[3], 3);

  EXPECT_STREQ(a->data[0], "AB");
  EXPECT_STREQ(a->data[1], "");
  EXPECT_STREQ(a->data[2], "CDE");
  EXPECT_STREQ(a->data[3], "F C");
  EXPECT_STREQ(a->data[4], "AA");
  EXPECT_STREQ(b->data[1], "wdw");
  EXPECT_STREQ(b->data[2], "dsw7");
  EXPECT_STREQ(b->data[3], "872");

  EXPECT_EQ(arrowOffset, 3);
  EXPECT_EQ(orcOffset, 3);
  writer->add(*batch);
  writer->close();
}
TEST(TestAdapterWriteNested, writeMapMixed3) {
  auto sharedPtrArrowType = map(utf8(), utf8());
  DataType* arrowType = sharedPtrArrowType.get();

  StringBuilder keysBuilder, itemsBuilder;
  std::shared_ptr<Array> keysArray, itemsArray;

  int32_t offsets[4] = {0, 1, 3, 3};

  BufferBuilder builder;
  (void)(builder.Resize(16));
  (void)(builder.Append(offsets, 16));
  std::shared_ptr<arrow::Buffer> offsetsBuffer;
  if (!builder.Finish(&offsetsBuffer).ok()) {
    FAIL() << "The offsets buffer can not be constructed!";
  }

  (void)(keysBuilder.Append("AB"));
  (void)(keysBuilder.Append(""));
  (void)(keysBuilder.Append("CDE"));
  (void)(keysBuilder.Finish(&keysArray));
  (void)(itemsBuilder.AppendNull());
  (void)(itemsBuilder.Append("wdw"));
  (void)(itemsBuilder.Append("dsw7"));
  (void)(itemsBuilder.Finish(&itemsArray));

  uint8_t bitmap = 5;  // 00000101
  auto maybeBuffer = AllocateBuffer(1);
  if (!maybeBuffer.ok()) {
    FAIL() << "Buffer not created successfully";
  }
  std::shared_ptr<Buffer> bitmapBuffer = *std::move(maybeBuffer);
  uint8_t* bufferData = bitmapBuffer->mutable_data();
  std::memcpy(bufferData, &bitmap, 1);

  auto array = std::make_shared<MapArray>(sharedPtrArrowType, 3, offsetsBuffer, keysArray,
                                          itemsArray, bitmapBuffer);

  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:map<a:string,b:string>>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::MapVectorBatch* x =
      internal::checked_cast<liborc::MapVectorBatch*>(root->fields[0]);
  liborc::StringVectorBatch* a =
      internal::checked_cast<liborc::StringVectorBatch*>((x->keys).get());
  liborc::StringVectorBatch* b =
      internal::checked_cast<liborc::StringVectorBatch*>((x->elements).get());
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize,
                                       array.get());
  if (!st.ok()) {
    FAIL() << "ORC ColumnBatch not successfully filled";
  }

  EXPECT_EQ(x->numElements, 3);
  EXPECT_TRUE(x->hasNulls);
  EXPECT_EQ(a->numElements, 3);
  EXPECT_FALSE(a->hasNulls);
  EXPECT_EQ(b->numElements, 3);
  EXPECT_TRUE(b->hasNulls);

  EXPECT_EQ(x->notNull[0], 1);
  EXPECT_EQ(x->notNull[1], 0);
  EXPECT_EQ(x->notNull[2], 1);
  EXPECT_EQ(b->notNull[0], 0);
  EXPECT_EQ(b->notNull[1], 1);
  EXPECT_EQ(b->notNull[2], 1);

  EXPECT_EQ(x->offsets[0], 0);
  EXPECT_EQ(x->offsets[1], 1);
  EXPECT_EQ(x->offsets[2], 3);
  EXPECT_EQ(x->offsets[3], 3);

  EXPECT_EQ(a->length[0], 2);
  EXPECT_EQ(a->length[1], 0);
  EXPECT_EQ(a->length[2], 3);
  EXPECT_EQ(b->length[1], 3);
  EXPECT_EQ(b->length[2], 4);

  EXPECT_STREQ(a->data[0], "AB");
  EXPECT_STREQ(a->data[1], "");
  EXPECT_STREQ(a->data[2], "CDE");
  EXPECT_STREQ(b->data[1], "wdw");
  EXPECT_STREQ(b->data[2], "dsw7");

  EXPECT_EQ(arrowOffset, 3);
  EXPECT_EQ(orcOffset, 3);
  writer->add(*batch);
  writer->close();
}

// DenseUnion
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

  auto array =
      std::make_shared<DenseUnionArray>(sharedPtrArrowType, 0, children, type_idsBuffer);

  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:uniontype<a:string,b:int>>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::UnionVectorBatch* x =
      internal::checked_cast<liborc::UnionVectorBatch*>(root->fields[0]);
  liborc::StringVectorBatch* a =
      internal::checked_cast<liborc::StringVectorBatch*>(x->children[0]);
  liborc::LongVectorBatch* b =
      internal::checked_cast<liborc::LongVectorBatch*>(x->children[1]);
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize,
                                       array.get());
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

  auto array = std::make_shared<DenseUnionArray>(sharedPtrArrowType, 8, children,
                                                 type_idsBuffer, valueOffsetsBuffer);

  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:uniontype<a:string,b:int>>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::UnionVectorBatch* x =
      internal::checked_cast<liborc::UnionVectorBatch*>(root->fields[0]);
  liborc::StringVectorBatch* a =
      internal::checked_cast<liborc::StringVectorBatch*>(x->children[0]);
  liborc::LongVectorBatch* b =
      internal::checked_cast<liborc::LongVectorBatch*>(x->children[1]);
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize,
                                       array.get());
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
  EXPECT_EQ(x->offsets[1], 1);
  EXPECT_EQ(x->offsets[2], 2);
  EXPECT_EQ(x->offsets[3], 3);
  EXPECT_EQ(x->offsets[4], 4);
  EXPECT_EQ(x->offsets[5], 5);
  EXPECT_EQ(x->offsets[6], 6);
  EXPECT_EQ(x->offsets[7], 7);

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

  auto array = std::make_shared<DenseUnionArray>(sharedPtrArrowType, 8, children,
                                                 type_idsBuffer, valueOffsetsBuffer);

  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:uniontype<a:string,b:int>>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::UnionVectorBatch* x =
      internal::checked_cast<liborc::UnionVectorBatch*>(root->fields[0]);
  liborc::StringVectorBatch* a =
      internal::checked_cast<liborc::StringVectorBatch*>(x->children[0]);
  liborc::LongVectorBatch* b =
      internal::checked_cast<liborc::LongVectorBatch*>(x->children[1]);
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize,
                                       array.get());
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
  EXPECT_EQ(x->offsets[4], 4);
  EXPECT_EQ(x->offsets[5], 5);
  EXPECT_EQ(x->offsets[6], 6);
  EXPECT_EQ(x->offsets[7], 7);

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

  auto array = std::make_shared<DenseUnionArray>(sharedPtrArrowType, 8, children,
                                                 type_idsBuffer, valueOffsetsBuffer);

  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:uniontype<a:string,b:int>>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::UnionVectorBatch* x =
      internal::checked_cast<liborc::UnionVectorBatch*>(root->fields[0]);
  liborc::StringVectorBatch* a =
      internal::checked_cast<liborc::StringVectorBatch*>(x->children[0]);
  liborc::LongVectorBatch* b =
      internal::checked_cast<liborc::LongVectorBatch*>(x->children[1]);
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize,
                                       array.get());
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
  EXPECT_EQ(x->offsets[3], 3);
  EXPECT_EQ(x->offsets[4], 4);
  EXPECT_EQ(x->offsets[5], 5);
  EXPECT_EQ(x->offsets[6], 6);
  EXPECT_EQ(x->offsets[7], 7);

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

  auto array = std::make_shared<DenseUnionArray>(sharedPtrArrowType, 12, children,
                                                 type_idsBuffer, valueOffsetsBuffer);

  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:uniontype<a:string,b:int,c:double>>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::UnionVectorBatch* x =
      internal::checked_cast<liborc::UnionVectorBatch*>(root->fields[0]);
  liborc::StringVectorBatch* a =
      internal::checked_cast<liborc::StringVectorBatch*>(x->children[0]);
  liborc::LongVectorBatch* b =
      internal::checked_cast<liborc::LongVectorBatch*>(x->children[1]);
  liborc::DoubleVectorBatch* c =
      internal::checked_cast<liborc::DoubleVectorBatch*>(x->children[2]);
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize,
                                       array.get());
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
  EXPECT_EQ(x->offsets[2], 2);
  EXPECT_EQ(x->offsets[3], 3);
  EXPECT_EQ(x->offsets[4], 4);
  EXPECT_EQ(x->offsets[5], 5);
  EXPECT_EQ(x->offsets[6], 6);
  EXPECT_EQ(x->offsets[7], 7);
  EXPECT_EQ(x->offsets[8], 8);
  EXPECT_EQ(x->offsets[9], 9);
  EXPECT_EQ(x->offsets[10], 10);
  EXPECT_EQ(x->offsets[11], 11);

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

// SparseUnion
TEST(TestAdapterWriteNested, writeSparseUnionEmpty) {
  std::vector<std::shared_ptr<Field>> xFields;
  xFields.push_back(std::make_shared<Field>("a", utf8()));
  xFields.push_back(std::make_shared<Field>("b", int32()));
  auto sharedPtrArrowType = sparse_union(xFields);
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

  auto array =
      std::make_shared<SparseUnionArray>(sharedPtrArrowType, 0, children, type_idsBuffer);

  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:uniontype<a:string,b:int>>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::UnionVectorBatch* x =
      internal::checked_cast<liborc::UnionVectorBatch*>(root->fields[0]);
  liborc::StringVectorBatch* a =
      internal::checked_cast<liborc::StringVectorBatch*>(x->children[0]);
  liborc::LongVectorBatch* b =
      internal::checked_cast<liborc::LongVectorBatch*>(x->children[1]);
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize,
                                       array.get());
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
TEST(TestAdapterWriteNested, writeSparseUnionNoNulls) {
  std::vector<std::shared_ptr<Field>> xFields;
  xFields.push_back(std::make_shared<Field>("a", utf8()));
  xFields.push_back(std::make_shared<Field>("b", int32()));
  auto sharedPtrArrowType = sparse_union(xFields);
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

  BufferBuilder builder;
  (void)(builder.Resize(8));
  (void)(builder.Append(type_ids, 8));
  std::shared_ptr<arrow::Buffer> type_idsBuffer;
  if (!builder.Finish(&type_idsBuffer).ok()) {
    FAIL() << "The type_ids buffer can not be constructed!";
  }

  auto array =
      std::make_shared<SparseUnionArray>(sharedPtrArrowType, 8, children, type_idsBuffer);

  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:uniontype<a:string,b:int>>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::UnionVectorBatch* x =
      internal::checked_cast<liborc::UnionVectorBatch*>(root->fields[0]);
  liborc::StringVectorBatch* a =
      internal::checked_cast<liborc::StringVectorBatch*>(x->children[0]);
  liborc::LongVectorBatch* b =
      internal::checked_cast<liborc::LongVectorBatch*>(x->children[1]);
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize,
                                       array.get());
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
  EXPECT_EQ(x->offsets[1], 1);
  EXPECT_EQ(x->offsets[2], 2);
  EXPECT_EQ(x->offsets[3], 3);
  EXPECT_EQ(x->offsets[4], 4);
  EXPECT_EQ(x->offsets[5], 5);
  EXPECT_EQ(x->offsets[6], 6);
  EXPECT_EQ(x->offsets[7], 7);

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
TEST(TestAdapterWriteNested, writeSparseUnionAllNulls) {
  std::vector<std::shared_ptr<Field>> xFields;
  xFields.push_back(std::make_shared<Field>("a", utf8()));
  xFields.push_back(std::make_shared<Field>("b", int32()));
  auto sharedPtrArrowType = sparse_union(xFields);
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

  BufferBuilder builder;
  (void)(builder.Resize(8));
  (void)(builder.Append(type_ids, 8));
  std::shared_ptr<arrow::Buffer> type_idsBuffer;
  if (!builder.Finish(&type_idsBuffer).ok()) {
    FAIL() << "The type_ids buffer can not be constructed!";
  }

  auto array =
      std::make_shared<SparseUnionArray>(sharedPtrArrowType, 8, children, type_idsBuffer);

  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:uniontype<a:string,b:int>>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::UnionVectorBatch* x =
      internal::checked_cast<liborc::UnionVectorBatch*>(root->fields[0]);
  liborc::StringVectorBatch* a =
      internal::checked_cast<liborc::StringVectorBatch*>(x->children[0]);
  liborc::LongVectorBatch* b =
      internal::checked_cast<liborc::LongVectorBatch*>(x->children[1]);
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize,
                                       array.get());
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
  EXPECT_EQ(x->offsets[4], 4);
  EXPECT_EQ(x->offsets[5], 5);
  EXPECT_EQ(x->offsets[6], 6);
  EXPECT_EQ(x->offsets[7], 7);

  EXPECT_EQ(arrowOffset, 4);
  EXPECT_EQ(orcOffset, 4);
  writer->add(*batch);
  writer->close();
}
TEST(TestAdapterWriteNested, writeSparseUnionMixed1) {
  std::vector<std::shared_ptr<Field>> xFields;
  xFields.push_back(std::make_shared<Field>("a", utf8()));
  xFields.push_back(std::make_shared<Field>("b", int32()));
  auto sharedPtrArrowType = sparse_union(xFields);
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

  BufferBuilder builder;
  (void)(builder.Resize(8));
  (void)(builder.Append(type_ids, 8));
  std::shared_ptr<arrow::Buffer> type_idsBuffer;
  if (!builder.Finish(&type_idsBuffer).ok()) {
    FAIL() << "The type_ids buffer can not be constructed!";
  }

  auto array =
      std::make_shared<SparseUnionArray>(sharedPtrArrowType, 8, children, type_idsBuffer);

  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:uniontype<a:string,b:int>>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::UnionVectorBatch* x =
      internal::checked_cast<liborc::UnionVectorBatch*>(root->fields[0]);
  liborc::StringVectorBatch* a =
      internal::checked_cast<liborc::StringVectorBatch*>(x->children[0]);
  liborc::LongVectorBatch* b =
      internal::checked_cast<liborc::LongVectorBatch*>(x->children[1]);
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize,
                                       array.get());
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
  EXPECT_EQ(x->offsets[3], 3);
  EXPECT_EQ(x->offsets[4], 4);
  EXPECT_EQ(x->offsets[5], 5);
  EXPECT_EQ(x->offsets[6], 6);
  EXPECT_EQ(x->offsets[7], 7);

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
TEST(TestAdapterWriteNested, writeSparseUnionMixed2) {
  std::vector<std::shared_ptr<Field>> xFields;
  xFields.push_back(std::make_shared<Field>("a", utf8()));
  xFields.push_back(std::make_shared<Field>("b", int32()));
  xFields.push_back(std::make_shared<Field>("c", float64()));
  auto sharedPtrArrowType = sparse_union(xFields);
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

  BufferBuilder builder;
  (void)(builder.Resize(12));
  (void)(builder.Append(type_ids, 12));
  std::shared_ptr<arrow::Buffer> type_idsBuffer;
  if (!builder.Finish(&type_idsBuffer).ok()) {
    FAIL() << "The type_ids buffer can not be constructed!";
  }

  auto array = std::make_shared<SparseUnionArray>(sharedPtrArrowType, 12, children,
                                                  type_idsBuffer);

  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:uniontype<a:string,b:int,c:double>>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::UnionVectorBatch* x =
      internal::checked_cast<liborc::UnionVectorBatch*>(root->fields[0]);
  liborc::StringVectorBatch* a =
      internal::checked_cast<liborc::StringVectorBatch*>(x->children[0]);
  liborc::LongVectorBatch* b =
      internal::checked_cast<liborc::LongVectorBatch*>(x->children[1]);
  liborc::DoubleVectorBatch* c =
      internal::checked_cast<liborc::DoubleVectorBatch*>(x->children[2]);
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize,
                                       array.get());
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
  EXPECT_EQ(x->offsets[2], 2);
  EXPECT_EQ(x->offsets[3], 3);
  EXPECT_EQ(x->offsets[4], 4);
  EXPECT_EQ(x->offsets[5], 5);
  EXPECT_EQ(x->offsets[6], 6);
  EXPECT_EQ(x->offsets[7], 7);
  EXPECT_EQ(x->offsets[8], 8);
  EXPECT_EQ(x->offsets[9], 9);
  EXPECT_EQ(x->offsets[10], 10);
  EXPECT_EQ(x->offsets[11], 11);

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

// Time-related
// Date32
TEST(TestAdapterWriteTime, writeDate32Empty) {
  Date32Builder builder;
  std::shared_ptr<Array> array;
  (void)(builder.Finish(&array));
  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:date>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::LongVectorBatch* x =
      internal::checked_cast<liborc::LongVectorBatch*>(root->fields[0]);
  DataType* arrowType = date32().get();
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize,
                                       array.get());
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
TEST(TestAdapterWriteTime, writeDate32NoNulls) {
  Date32Builder builder;
  (void)(builder.Append(1));
  (void)(builder.Append(-2));
  (void)(builder.Append(18582));  // Nov 16th 2020
  (void)(builder.Append(-4));
  std::shared_ptr<Array> array;
  (void)(builder.Finish(&array));
  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:date>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::LongVectorBatch* x =
      internal::checked_cast<liborc::LongVectorBatch*>(root->fields[0]);
  DataType* arrowType = date32().get();
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize,
                                       array.get());
  if (!st.ok()) {
    FAIL() << "ORC ColumnBatch not successfully filled";
  }
  EXPECT_EQ(x->numElements, 4);
  EXPECT_FALSE(x->hasNulls);
  EXPECT_EQ(x->data[0], 1);
  EXPECT_EQ(x->data[1], -2);
  EXPECT_EQ(x->data[2], 18582);
  EXPECT_EQ(x->data[3], -4);
  EXPECT_EQ(arrowOffset, 4);
  EXPECT_EQ(orcOffset, 4);
  writer->add(*batch);
  writer->close();
}
TEST(TestAdapterWriteTime, writeDate32AllNulls) {
  Date32Builder builder;
  (void)(builder.AppendNull());
  (void)(builder.AppendNull());
  (void)(builder.AppendNull());
  std::shared_ptr<Array> array;
  (void)(builder.Finish(&array));
  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:date>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::LongVectorBatch* x =
      internal::checked_cast<liborc::LongVectorBatch*>(root->fields[0]);
  DataType* arrowType = date32().get();
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize,
                                       array.get());
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
TEST(TestAdapterWriteTime, writeDate32Mixed) {
  Date32Builder builder;
  (void)(builder.AppendNull());
  (void)(builder.Append(18581));
  (void)(builder.AppendNull());
  (void)(builder.Append(18583));
  (void)(builder.AppendNull());
  std::shared_ptr<Array> array;
  (void)(builder.Finish(&array));
  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:date>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::LongVectorBatch* x =
      internal::checked_cast<liborc::LongVectorBatch*>(root->fields[0]);
  DataType* arrowType = date32().get();
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize,
                                       array.get());
  if (!st.ok()) {
    FAIL() << "ORC ColumnBatch not successfully filled";
  }
  EXPECT_EQ(x->numElements, 5);
  EXPECT_TRUE(x->hasNulls);
  EXPECT_EQ(x->data[1], 18581);
  EXPECT_EQ(x->data[3], 18583);
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
TEST(TestAdapterWriteTime, writeDate32ChunkedEmpty) {
  ArrayVector av;
  std::shared_ptr<ChunkedArray> carray = std::make_shared<ChunkedArray>(av, date32());
  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:date>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::LongVectorBatch* x =
      internal::checked_cast<liborc::LongVectorBatch*>(root->fields[0]);
  DataType* arrowType = date32().get();
  int64_t arrowIndexOffset = 0;
  int arrowChunkOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowIndexOffset, arrowChunkOffset,
                                       batchSize, carray.get());
  if (!st.ok()) {
    FAIL() << "ORC ColumnBatch not successfully filled";
  }
  EXPECT_EQ(x->numElements, 0);
  EXPECT_FALSE(x->hasNulls);
  EXPECT_EQ(arrowIndexOffset, 0);
  EXPECT_EQ(arrowChunkOffset, 0);
  writer->add(*batch);
  writer->close();
}
TEST(TestAdapterWriteTime, writeDate32ChunkedMixed) {
  Date32Builder builder0, builder1, builder2, builder3, builder4;
  (void)(builder1.AppendNull());
  (void)(builder1.Append(18585));
  (void)(builder2.AppendNull());
  (void)(builder2.Append(18473));
  (void)(builder3.AppendNull());
  std::shared_ptr<Array> array0, array1, array2, array3, array4;
  (void)(builder0.Finish(&array0));
  (void)(builder1.Finish(&array1));
  (void)(builder2.Finish(&array2));
  (void)(builder3.Finish(&array3));
  (void)(builder4.Finish(&array4));
  ArrayVector av;
  av.push_back(array0);
  av.push_back(array1);
  av.push_back(array2);
  av.push_back(array3);
  av.push_back(array4);
  std::shared_ptr<ChunkedArray> carray = std::make_shared<ChunkedArray>(av);
  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:date>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::LongVectorBatch* x =
      internal::checked_cast<liborc::LongVectorBatch*>(root->fields[0]);
  DataType* arrowType = date32().get();
  int64_t arrowIndexOffset = 0;
  int arrowChunkOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowIndexOffset, arrowChunkOffset,
                                       batchSize, carray.get());
  if (!st.ok()) {
    FAIL() << "ORC ColumnBatch not successfully filled";
  }
  EXPECT_EQ(x->numElements, 5);
  EXPECT_TRUE(x->hasNulls);
  EXPECT_EQ(x->data[1], 18585);
  EXPECT_EQ(x->data[3], 18473);
  EXPECT_EQ(x->notNull[0], 0);
  EXPECT_EQ(x->notNull[1], 1);
  EXPECT_EQ(x->notNull[2], 0);
  EXPECT_EQ(x->notNull[3], 1);
  EXPECT_EQ(x->notNull[4], 0);
  EXPECT_EQ(arrowIndexOffset, 0);
  EXPECT_EQ(arrowChunkOffset, 5);
  writer->add(*batch);
  writer->close();
}

// Date64
TEST(TestAdapterWriteTime, writeDate64Empty) {
  Date64Builder builder;
  std::shared_ptr<Array> array;
  (void)(builder.Finish(&array));
  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:timestamp>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::TimestampVectorBatch* x =
      internal::checked_cast<liborc::TimestampVectorBatch*>(root->fields[0]);
  DataType* arrowType = date64().get();
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize,
                                       array.get());
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
TEST(TestAdapterWriteTime, writeDate64NoNulls) {
  Date64Builder builder;
  (void)(builder.Append(1605547708000));
  (void)(builder.Append(1605547718500));
  (void)(builder.Append(1605547720800));
  (void)(builder.Append(1605547748500));
  std::shared_ptr<Array> array;
  (void)(builder.Finish(&array));
  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:timestamp>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::TimestampVectorBatch* x =
      internal::checked_cast<liborc::TimestampVectorBatch*>(root->fields[0]);
  DataType* arrowType = date64().get();
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize,
                                       array.get());
  if (!st.ok()) {
    FAIL() << "ORC ColumnBatch not successfully filled";
  }
  EXPECT_EQ(x->numElements, 4);
  EXPECT_FALSE(x->hasNulls);
  EXPECT_EQ(x->data[0], 1605547708);
  EXPECT_EQ(x->data[1], 1605547718);
  EXPECT_EQ(x->data[2], 1605547720);
  EXPECT_EQ(x->data[3], 1605547748);
  EXPECT_EQ(x->nanoseconds[0], 0);
  EXPECT_EQ(x->nanoseconds[1], 500000000);
  EXPECT_EQ(x->nanoseconds[2], 800000000);
  EXPECT_EQ(x->nanoseconds[3], 500000000);
  EXPECT_EQ(arrowOffset, 4);
  EXPECT_EQ(orcOffset, 4);
  writer->add(*batch);
  writer->close();
}
TEST(TestAdapterWriteTime, writeDate64AllNulls) {
  Date64Builder builder;
  (void)(builder.AppendNull());
  (void)(builder.AppendNull());
  (void)(builder.AppendNull());
  std::shared_ptr<Array> array;
  (void)(builder.Finish(&array));
  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:timestamp>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::TimestampVectorBatch* x =
      internal::checked_cast<liborc::TimestampVectorBatch*>(root->fields[0]);
  DataType* arrowType = date64().get();
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize,
                                       array.get());
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
TEST(TestAdapterWriteTime, writeDate64Mixed) {
  Date64Builder builder;
  (void)(builder.AppendNull());
  (void)(builder.Append(1605547718500));
  (void)(builder.AppendNull());
  (void)(builder.Append(1605547770009));
  (void)(builder.AppendNull());
  std::shared_ptr<Array> array;
  (void)(builder.Finish(&array));
  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:timestamp>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::TimestampVectorBatch* x =
      internal::checked_cast<liborc::TimestampVectorBatch*>(root->fields[0]);
  DataType* arrowType = date64().get();
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize,
                                       array.get());
  if (!st.ok()) {
    FAIL() << "ORC ColumnBatch not successfully filled";
  }
  EXPECT_EQ(x->numElements, 5);
  EXPECT_TRUE(x->hasNulls);
  EXPECT_EQ(x->data[1], 1605547718);
  EXPECT_EQ(x->data[3], 1605547770);
  EXPECT_EQ(x->nanoseconds[1], 500000000);
  EXPECT_EQ(x->nanoseconds[3], 9000000);
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
TEST(TestAdapterWriteTime, writeDate64ChunkedEmpty) {
  ArrayVector av;
  std::shared_ptr<ChunkedArray> carray = std::make_shared<ChunkedArray>(av, date64());
  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:timestamp>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::TimestampVectorBatch* x =
      internal::checked_cast<liborc::TimestampVectorBatch*>(root->fields[0]);
  DataType* arrowType = date64().get();
  int64_t arrowIndexOffset = 0;
  int arrowChunkOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowIndexOffset, arrowChunkOffset,
                                       batchSize, carray.get());
  if (!st.ok()) {
    FAIL() << "ORC ColumnBatch not successfully filled";
  }
  EXPECT_EQ(x->numElements, 0);
  EXPECT_FALSE(x->hasNulls);
  EXPECT_EQ(arrowIndexOffset, 0);
  EXPECT_EQ(arrowChunkOffset, 0);
  writer->add(*batch);
  writer->close();
}
TEST(TestAdapterWriteTime, writeDate64ChunkedMixed) {
  Date64Builder builder0, builder1, builder2, builder3, builder4;
  (void)(builder1.AppendNull());
  (void)(builder1.Append(1605547718999));
  (void)(builder2.AppendNull());
  (void)(builder2.Append(1605947718900));
  (void)(builder3.AppendNull());
  std::shared_ptr<Array> array0, array1, array2, array3, array4;
  (void)(builder0.Finish(&array0));
  (void)(builder1.Finish(&array1));
  (void)(builder2.Finish(&array2));
  (void)(builder3.Finish(&array3));
  (void)(builder4.Finish(&array4));
  ArrayVector av;
  av.push_back(array0);
  av.push_back(array1);
  av.push_back(array2);
  av.push_back(array3);
  av.push_back(array4);
  std::shared_ptr<ChunkedArray> carray = std::make_shared<ChunkedArray>(av);
  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:timestamp>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::TimestampVectorBatch* x =
      internal::checked_cast<liborc::TimestampVectorBatch*>(root->fields[0]);
  DataType* arrowType = date64().get();
  int64_t arrowIndexOffset = 0;
  int arrowChunkOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowIndexOffset, arrowChunkOffset,
                                       batchSize, carray.get());
  if (!st.ok()) {
    FAIL() << "ORC ColumnBatch not successfully filled";
  }
  EXPECT_EQ(x->numElements, 5);
  EXPECT_TRUE(x->hasNulls);
  EXPECT_EQ(x->data[1], 1605547718);
  EXPECT_EQ(x->data[3], 1605947718);
  EXPECT_EQ(x->nanoseconds[1], 999000000);
  EXPECT_EQ(x->nanoseconds[3], 900000000);
  EXPECT_EQ(x->notNull[0], 0);
  EXPECT_EQ(x->notNull[1], 1);
  EXPECT_EQ(x->notNull[2], 0);
  EXPECT_EQ(x->notNull[3], 1);
  EXPECT_EQ(x->notNull[4], 0);
  EXPECT_EQ(arrowIndexOffset, 0);
  EXPECT_EQ(arrowChunkOffset, 5);
  writer->add(*batch);
  writer->close();
}

// Timestamp
TEST(TestAdapterWriteTime, writeTimestampEmpty) {
  TimestampBuilder builder(timestamp(TimeUnit::type::SECOND), default_memory_pool());
  std::shared_ptr<Array> array;
  (void)(builder.Finish(&array));
  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:timestamp>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::TimestampVectorBatch* x =
      internal::checked_cast<liborc::TimestampVectorBatch*>(root->fields[0]);
  DataType* arrowType = timestamp(TimeUnit::type::SECOND).get();
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize,
                                       array.get());
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
TEST(TestAdapterWriteTime, writeTimestampNoNulls) {
  TimestampBuilder builder(timestamp(TimeUnit::type::MICRO), default_memory_pool());
  (void)(builder.Append(1605547708000000));
  (void)(builder.Append(1605547718500060));
  (void)(builder.Append(1605547720800009));
  (void)(builder.Append(1605547748999999));
  std::shared_ptr<Array> array;
  (void)(builder.Finish(&array));
  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:timestamp>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::TimestampVectorBatch* x =
      internal::checked_cast<liborc::TimestampVectorBatch*>(root->fields[0]);
  DataType* arrowType = timestamp(TimeUnit::type::MICRO).get();
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize,
                                       array.get());
  if (!st.ok()) {
    FAIL() << "ORC ColumnBatch not successfully filled";
  }
  EXPECT_EQ(x->numElements, 4);
  EXPECT_FALSE(x->hasNulls);
  EXPECT_EQ(x->data[0], 1605547708);
  EXPECT_EQ(x->data[1], 1605547718);
  EXPECT_EQ(x->data[2], 1605547720);
  EXPECT_EQ(x->data[3], 1605547748);
  EXPECT_EQ(x->nanoseconds[0], 0);
  EXPECT_EQ(x->nanoseconds[1], 500060000);
  EXPECT_EQ(x->nanoseconds[2], 800009000);
  EXPECT_EQ(x->nanoseconds[3], 999999000);
  EXPECT_EQ(arrowOffset, 4);
  EXPECT_EQ(orcOffset, 4);
  writer->add(*batch);
  writer->close();
}
TEST(TestAdapterWriteTime, writeTimestampAllNulls) {
  TimestampBuilder builder(timestamp(TimeUnit::type::MILLI), default_memory_pool());
  (void)(builder.AppendNull());
  (void)(builder.AppendNull());
  (void)(builder.AppendNull());
  std::shared_ptr<Array> array;
  (void)(builder.Finish(&array));
  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:timestamp>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::TimestampVectorBatch* x =
      internal::checked_cast<liborc::TimestampVectorBatch*>(root->fields[0]);
  DataType* arrowType = timestamp(TimeUnit::type::MILLI).get();
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize,
                                       array.get());
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
TEST(TestAdapterWriteTime, writeTimestampMixed) {
  TimestampBuilder builder(timestamp(TimeUnit::type::NANO), default_memory_pool());
  (void)(builder.AppendNull());
  (void)(builder.Append(1605547718500999999));
  (void)(builder.AppendNull());
  (void)(builder.Append(1605547770009999999));
  (void)(builder.AppendNull());
  std::shared_ptr<Array> array;
  (void)(builder.Finish(&array));
  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:timestamp>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::TimestampVectorBatch* x =
      internal::checked_cast<liborc::TimestampVectorBatch*>(root->fields[0]);
  DataType* arrowType = timestamp(TimeUnit::type::NANO).get();
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize,
                                       array.get());
  if (!st.ok()) {
    FAIL() << "ORC ColumnBatch not successfully filled";
  }
  EXPECT_EQ(x->numElements, 5);
  EXPECT_TRUE(x->hasNulls);
  EXPECT_EQ(x->data[1], 1605547718);
  EXPECT_EQ(x->data[3], 1605547770);
  EXPECT_EQ(x->nanoseconds[1], 500999999);
  EXPECT_EQ(x->nanoseconds[3], 9999999);
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
TEST(TestAdapterWriteTime, writeTimestampChunkedEmpty) {
  ArrayVector av;
  std::shared_ptr<ChunkedArray> carray =
      std::make_shared<ChunkedArray>(av, timestamp(TimeUnit::type::NANO));
  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:timestamp>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::TimestampVectorBatch* x =
      internal::checked_cast<liborc::TimestampVectorBatch*>(root->fields[0]);
  DataType* arrowType = timestamp(TimeUnit::type::NANO).get();
  int64_t arrowIndexOffset = 0;
  int arrowChunkOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowIndexOffset, arrowChunkOffset,
                                       batchSize, carray.get());
  if (!st.ok()) {
    FAIL() << "ORC ColumnBatch not successfully filled";
  }
  EXPECT_EQ(x->numElements, 0);
  EXPECT_FALSE(x->hasNulls);
  EXPECT_EQ(arrowIndexOffset, 0);
  EXPECT_EQ(arrowChunkOffset, 0);
  writer->add(*batch);
  writer->close();
}
TEST(TestAdapterWriteTime, writeTimestampChunkedMixedSecond) {
  TimestampBuilder builder0(timestamp(TimeUnit::type::SECOND), default_memory_pool()),
      builder1(timestamp(TimeUnit::type::SECOND), default_memory_pool()),
      builder2(timestamp(TimeUnit::type::SECOND), default_memory_pool()),
      builder3(timestamp(TimeUnit::type::SECOND), default_memory_pool()),
      builder4(timestamp(TimeUnit::type::SECOND), default_memory_pool());
  (void)(builder1.AppendNull());
  (void)(builder1.Append(1605547718));
  (void)(builder2.AppendNull());
  (void)(builder2.Append(1605947718));
  (void)(builder3.AppendNull());
  std::shared_ptr<Array> array0, array1, array2, array3, array4;
  (void)(builder0.Finish(&array0));
  (void)(builder1.Finish(&array1));
  (void)(builder2.Finish(&array2));
  (void)(builder3.Finish(&array3));
  (void)(builder4.Finish(&array4));
  ArrayVector av;
  av.push_back(array0);
  av.push_back(array1);
  av.push_back(array2);
  av.push_back(array3);
  av.push_back(array4);
  std::shared_ptr<ChunkedArray> carray = std::make_shared<ChunkedArray>(av);
  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:timestamp>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::TimestampVectorBatch* x =
      internal::checked_cast<liborc::TimestampVectorBatch*>(root->fields[0]);
  DataType* arrowType = timestamp(TimeUnit::type::SECOND).get();
  int64_t arrowIndexOffset = 0;
  int arrowChunkOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowIndexOffset, arrowChunkOffset,
                                       batchSize, carray.get());
  if (!st.ok()) {
    FAIL() << "ORC ColumnBatch not successfully filled";
  }
  EXPECT_EQ(x->numElements, 5);
  EXPECT_TRUE(x->hasNulls);
  EXPECT_EQ(x->data[1], 1605547718);
  EXPECT_EQ(x->data[3], 1605947718);
  EXPECT_EQ(x->nanoseconds[1], 0);
  EXPECT_EQ(x->nanoseconds[3], 0);
  EXPECT_EQ(x->notNull[0], 0);
  EXPECT_EQ(x->notNull[1], 1);
  EXPECT_EQ(x->notNull[2], 0);
  EXPECT_EQ(x->notNull[3], 1);
  EXPECT_EQ(x->notNull[4], 0);
  EXPECT_EQ(arrowIndexOffset, 0);
  EXPECT_EQ(arrowChunkOffset, 5);
  writer->add(*batch);
  writer->close();
}
TEST(TestAdapterWriteTime, writeTimestampChunkedMixedMilli) {
  TimestampBuilder builder0(timestamp(TimeUnit::type::MILLI), default_memory_pool()),
      builder1(timestamp(TimeUnit::type::MILLI), default_memory_pool()),
      builder2(timestamp(TimeUnit::type::MILLI), default_memory_pool()),
      builder3(timestamp(TimeUnit::type::MILLI), default_memory_pool()),
      builder4(timestamp(TimeUnit::type::MILLI), default_memory_pool());
  (void)(builder1.AppendNull());
  (void)(builder1.Append(1605547718999));
  (void)(builder2.AppendNull());
  (void)(builder2.Append(1605947718900));
  (void)(builder3.AppendNull());
  std::shared_ptr<Array> array0, array1, array2, array3, array4;
  (void)(builder0.Finish(&array0));
  (void)(builder1.Finish(&array1));
  (void)(builder2.Finish(&array2));
  (void)(builder3.Finish(&array3));
  (void)(builder4.Finish(&array4));
  ArrayVector av;
  av.push_back(array0);
  av.push_back(array1);
  av.push_back(array2);
  av.push_back(array3);
  av.push_back(array4);
  std::shared_ptr<ChunkedArray> carray = std::make_shared<ChunkedArray>(av);
  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:timestamp>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::TimestampVectorBatch* x =
      internal::checked_cast<liborc::TimestampVectorBatch*>(root->fields[0]);
  DataType* arrowType = timestamp(TimeUnit::type::MILLI).get();
  int64_t arrowIndexOffset = 0;
  int arrowChunkOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowIndexOffset, arrowChunkOffset,
                                       batchSize, carray.get());
  if (!st.ok()) {
    FAIL() << "ORC ColumnBatch not successfully filled";
  }
  EXPECT_EQ(x->numElements, 5);
  EXPECT_TRUE(x->hasNulls);
  EXPECT_EQ(x->data[1], 1605547718);
  EXPECT_EQ(x->data[3], 1605947718);
  EXPECT_EQ(x->nanoseconds[1], 999000000);
  EXPECT_EQ(x->nanoseconds[3], 900000000);
  EXPECT_EQ(x->notNull[0], 0);
  EXPECT_EQ(x->notNull[1], 1);
  EXPECT_EQ(x->notNull[2], 0);
  EXPECT_EQ(x->notNull[3], 1);
  EXPECT_EQ(x->notNull[4], 0);
  EXPECT_EQ(arrowIndexOffset, 0);
  EXPECT_EQ(arrowChunkOffset, 5);
  writer->add(*batch);
  writer->close();
}
TEST(TestAdapterWriteTime, writeTimestampChunkedMixedMicro) {
  TimestampBuilder builder0(timestamp(TimeUnit::type::MICRO), default_memory_pool()),
      builder1(timestamp(TimeUnit::type::MICRO), default_memory_pool()),
      builder2(timestamp(TimeUnit::type::MICRO), default_memory_pool()),
      builder3(timestamp(TimeUnit::type::MICRO), default_memory_pool()),
      builder4(timestamp(TimeUnit::type::MICRO), default_memory_pool());
  (void)(builder1.AppendNull());
  (void)(builder1.Append(1605547718999999));
  (void)(builder2.AppendNull());
  (void)(builder2.Append(1605947718900900));
  (void)(builder3.AppendNull());
  std::shared_ptr<Array> array0, array1, array2, array3, array4;
  (void)(builder0.Finish(&array0));
  (void)(builder1.Finish(&array1));
  (void)(builder2.Finish(&array2));
  (void)(builder3.Finish(&array3));
  (void)(builder4.Finish(&array4));
  ArrayVector av;
  av.push_back(array0);
  av.push_back(array1);
  av.push_back(array2);
  av.push_back(array3);
  av.push_back(array4);
  std::shared_ptr<ChunkedArray> carray = std::make_shared<ChunkedArray>(av);
  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:timestamp>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::TimestampVectorBatch* x =
      internal::checked_cast<liborc::TimestampVectorBatch*>(root->fields[0]);
  DataType* arrowType = timestamp(TimeUnit::type::MICRO).get();
  int64_t arrowIndexOffset = 0;
  int arrowChunkOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowIndexOffset, arrowChunkOffset,
                                       batchSize, carray.get());
  if (!st.ok()) {
    FAIL() << "ORC ColumnBatch not successfully filled";
  }
  EXPECT_EQ(x->numElements, 5);
  EXPECT_TRUE(x->hasNulls);
  EXPECT_EQ(x->data[1], 1605547718);
  EXPECT_EQ(x->data[3], 1605947718);
  EXPECT_EQ(x->nanoseconds[1], 999999000);
  EXPECT_EQ(x->nanoseconds[3], 900900000);
  EXPECT_EQ(x->notNull[0], 0);
  EXPECT_EQ(x->notNull[1], 1);
  EXPECT_EQ(x->notNull[2], 0);
  EXPECT_EQ(x->notNull[3], 1);
  EXPECT_EQ(x->notNull[4], 0);
  EXPECT_EQ(arrowIndexOffset, 0);
  EXPECT_EQ(arrowChunkOffset, 5);
  writer->add(*batch);
  writer->close();
}
TEST(TestAdapterWriteTime, writeTimestampChunkedMixedNano) {
  TimestampBuilder builder0(timestamp(TimeUnit::type::NANO), default_memory_pool()),
      builder1(timestamp(TimeUnit::type::NANO), default_memory_pool()),
      builder2(timestamp(TimeUnit::type::NANO), default_memory_pool()),
      builder3(timestamp(TimeUnit::type::NANO), default_memory_pool()),
      builder4(timestamp(TimeUnit::type::NANO), default_memory_pool());
  (void)(builder1.AppendNull());
  (void)(builder1.Append(1605547718999999999));
  (void)(builder2.AppendNull());
  (void)(builder2.Append(1605947718900900900));
  (void)(builder3.AppendNull());
  std::shared_ptr<Array> array0, array1, array2, array3, array4;
  (void)(builder0.Finish(&array0));
  (void)(builder1.Finish(&array1));
  (void)(builder2.Finish(&array2));
  (void)(builder3.Finish(&array3));
  (void)(builder4.Finish(&array4));
  ArrayVector av;
  av.push_back(array0);
  av.push_back(array1);
  av.push_back(array2);
  av.push_back(array3);
  av.push_back(array4);
  std::shared_ptr<ChunkedArray> carray = std::make_shared<ChunkedArray>(av);
  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:timestamp>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::TimestampVectorBatch* x =
      internal::checked_cast<liborc::TimestampVectorBatch*>(root->fields[0]);
  DataType* arrowType = timestamp(TimeUnit::type::NANO).get();
  int64_t arrowIndexOffset = 0;
  int arrowChunkOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowIndexOffset, arrowChunkOffset,
                                       batchSize, carray.get());
  if (!st.ok()) {
    FAIL() << "ORC ColumnBatch not successfully filled";
  }
  EXPECT_EQ(x->numElements, 5);
  EXPECT_TRUE(x->hasNulls);
  EXPECT_EQ(x->data[1], 1605547718);
  EXPECT_EQ(x->data[3], 1605947718);
  EXPECT_EQ(x->nanoseconds[1], 999999999);
  EXPECT_EQ(x->nanoseconds[3], 900900900);
  EXPECT_EQ(x->notNull[0], 0);
  EXPECT_EQ(x->notNull[1], 1);
  EXPECT_EQ(x->notNull[2], 0);
  EXPECT_EQ(x->notNull[3], 1);
  EXPECT_EQ(x->notNull[4], 0);
  EXPECT_EQ(arrowIndexOffset, 0);
  EXPECT_EQ(arrowChunkOffset, 5);
  writer->add(*batch);
  writer->close();
}
TEST(TestAdapterWriteTime, writeTimestampChunkedMixedMicroNewYork) {
  TimestampBuilder builder0(timestamp(TimeUnit::type::MICRO, "America/New_York"),
                            default_memory_pool()),
      builder1(timestamp(TimeUnit::type::MICRO), default_memory_pool()),
      builder2(timestamp(TimeUnit::type::MICRO), default_memory_pool()),
      builder3(timestamp(TimeUnit::type::MICRO), default_memory_pool()),
      builder4(timestamp(TimeUnit::type::MICRO), default_memory_pool());
  (void)(builder1.AppendNull());
  (void)(builder1.Append(1601547718999999));  // Daylight saving time on
  (void)(builder2.AppendNull());
  (void)(builder2.Append(1605947718900900));  // Daylight saving time off
  (void)(builder3.AppendNull());
  std::shared_ptr<Array> array0, array1, array2, array3, array4;
  (void)(builder0.Finish(&array0));
  (void)(builder1.Finish(&array1));
  (void)(builder2.Finish(&array2));
  (void)(builder3.Finish(&array3));
  (void)(builder4.Finish(&array4));
  ArrayVector av;
  av.push_back(array0);
  av.push_back(array1);
  av.push_back(array2);
  av.push_back(array3);
  av.push_back(array4);
  std::shared_ptr<ChunkedArray> carray = std::make_shared<ChunkedArray>(av);
  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:timestamp>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::TimestampVectorBatch* x =
      internal::checked_cast<liborc::TimestampVectorBatch*>(root->fields[0]);
  DataType* arrowType = timestamp(TimeUnit::type::MICRO, "America/New_York").get();
  int64_t arrowIndexOffset = 0;
  int arrowChunkOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowIndexOffset, arrowChunkOffset,
                                       batchSize, carray.get());
  if (!st.ok()) {
    FAIL() << "ORC ColumnBatch not successfully filled";
  }
  EXPECT_EQ(x->numElements, 5);
  EXPECT_TRUE(x->hasNulls);
  EXPECT_EQ(x->data[1], 1601547718);
  EXPECT_EQ(x->data[3], 1605947718);
  EXPECT_EQ(x->nanoseconds[1], 999999000);
  EXPECT_EQ(x->nanoseconds[3], 900900000);
  EXPECT_EQ(x->notNull[0], 0);
  EXPECT_EQ(x->notNull[1], 1);
  EXPECT_EQ(x->notNull[2], 0);
  EXPECT_EQ(x->notNull[3], 1);
  EXPECT_EQ(x->notNull[4], 0);
  EXPECT_EQ(arrowIndexOffset, 0);
  EXPECT_EQ(arrowChunkOffset, 5);
  writer->add(*batch);
  writer->close();
}
}  // namespace arrow
