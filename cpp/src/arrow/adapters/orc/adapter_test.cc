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
constexpr int DEFAULT_SMALL_MEM_STREAM_SIZE = 16384;

// Status MOpen(Schema* schema, const std::string file_name,
//              const std::shared_ptr<liborc::WriterOptions>& options,
//              const std::shared_ptr<adapters::orc::ArrowWriterOptions>& arrow_options) {
//   std::unique_ptr<liborc::Writer> liborc_writer;
//   std::unique_ptr<liborc::Type> orcSchema;
//   ORC_UNIQUE_PTR<liborc::OutputStream> outStream = liborc::writeLocalFile(file_name);
//   ORC_THROW_NOT_OK(adapters::orc::GetORCType(schema, &orcSchema));
//   try {
//     writer_ = createWriter(*orcSchema, outStream.get(), *options);
//   } catch (const liborc::ParseError& e) {
//     return Status::IOError(e.what());
//   }
//   schema_ = schema;
//   arrow_options_ = arrow_options;
//   num_cols_ = schema->num_fields();
//   return Status::OK();
// }

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

// Used in testing of FillBatch functions for union types
// begin is included, end is not
void testDenseUnionStringOffsetGenerator(int32_t begin, int32_t end, int32_t* out) {
  out[0] = 0;
  out[1] = 0;
  int32_t res;
  for (int32_t i = begin; i < end; i++) {
    res = i % 5;
    if (res >= 3)
      out[1]++;
    else
      out[0]++;
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

// Arrow2ORC type converter tests

TEST(TestAdapterWriteConverter, typeBool) {
  DataType* type = boolean().get();
  ORC_UNIQUE_PTR<liborc::Type> out;
  (void)(adapters::orc::GetORCType(type, &out));
  EXPECT_EQ(out->getKind(), liborc::TypeKind::BOOLEAN);
}
TEST(TestAdapterWriteConverter, typeInt8) {
  DataType* type = int8().get();
  ORC_UNIQUE_PTR<liborc::Type> out;
  (void)(adapters::orc::GetORCType(type, &out));
  EXPECT_EQ(out->getKind(), liborc::TypeKind::BYTE);
}
TEST(TestAdapterWriteConverter, typeInt16) {
  DataType* type = int16().get();
  ORC_UNIQUE_PTR<liborc::Type> out;
  (void)(adapters::orc::GetORCType(type, &out));
  EXPECT_EQ(out->getKind(), liborc::TypeKind::SHORT);
}
TEST(TestAdapterWriteConverter, typeInt32) {
  DataType* type = int32().get();
  ORC_UNIQUE_PTR<liborc::Type> out;
  (void)(adapters::orc::GetORCType(type, &out));
  EXPECT_EQ(out->getKind(), liborc::TypeKind::INT);
}
TEST(TestAdapterWriteConverter, typeInt64) {
  DataType* type = int64().get();
  ORC_UNIQUE_PTR<liborc::Type> out;
  (void)(adapters::orc::GetORCType(type, &out));
  EXPECT_EQ(out->getKind(), liborc::TypeKind::LONG);
}
TEST(TestAdapterWriteConverter, typeFloat) {
  DataType* type = float32().get();
  ORC_UNIQUE_PTR<liborc::Type> out;
  (void)(adapters::orc::GetORCType(type, &out));
  EXPECT_EQ(out->getKind(), liborc::TypeKind::FLOAT);
}
TEST(TestAdapterWriteConverter, typeDouble) {
  DataType* type = float64().get();
  ORC_UNIQUE_PTR<liborc::Type> out;
  (void)(adapters::orc::GetORCType(type, &out));
  EXPECT_EQ(out->getKind(), liborc::TypeKind::DOUBLE);
}
TEST(TestAdapterWriteConverter, typeString) {
  DataType* type = utf8().get();
  ORC_UNIQUE_PTR<liborc::Type> out;
  (void)(adapters::orc::GetORCType(type, &out));
  EXPECT_EQ(out->getKind(), liborc::TypeKind::STRING);
}
TEST(TestAdapterWriteConverter, typeLargeString) {
  DataType* type = large_utf8().get();
  ORC_UNIQUE_PTR<liborc::Type> out;
  (void)(adapters::orc::GetORCType(type, &out));
  EXPECT_EQ(out->getKind(), liborc::TypeKind::STRING);
}
TEST(TestAdapterWriteConverter, typeBinary) {
  DataType* type = binary().get();
  ORC_UNIQUE_PTR<liborc::Type> out;
  (void)(adapters::orc::GetORCType(type, &out));
  EXPECT_EQ(out->getKind(), liborc::TypeKind::BINARY);
}
TEST(TestAdapterWriteConverter, typeLargeBinary) {
  DataType* type = large_binary().get();
  ORC_UNIQUE_PTR<liborc::Type> out;
  (void)(adapters::orc::GetORCType(type, &out));
  EXPECT_EQ(out->getKind(), liborc::TypeKind::BINARY);
}
TEST(TestAdapterWriteConverter, typeFixedSizeBinary) {
  DataType* type = fixed_size_binary(3).get();
  ORC_UNIQUE_PTR<liborc::Type> out;
  (void)(adapters::orc::GetORCType(type, &out));
  EXPECT_EQ(out->getKind(), liborc::TypeKind::BINARY);
}
TEST(TestAdapterWriteConverter, typeFixedSizeBinaryZero) {
  DataType* type = fixed_size_binary(0).get();
  ORC_UNIQUE_PTR<liborc::Type> out;
  (void)(adapters::orc::GetORCType(type, &out));
  EXPECT_EQ(out->getKind(), liborc::TypeKind::BINARY);
}
TEST(TestAdapterWriteConverter, typeDate32) {
  DataType* type = date32().get();
  ORC_UNIQUE_PTR<liborc::Type> out;
  (void)(adapters::orc::GetORCType(type, &out));
  EXPECT_EQ(out->getKind(), liborc::TypeKind::DATE);
}
TEST(TestAdapterWriteConverter, typeDate64) {
  DataType* type = date64().get();
  ORC_UNIQUE_PTR<liborc::Type> out;
  (void)(adapters::orc::GetORCType(type, &out));
  EXPECT_EQ(out->getKind(), liborc::TypeKind::TIMESTAMP);
}
TEST(TestAdapterWriteConverter, typeTimestampSecond) {
  DataType* type = timestamp(TimeUnit::type::SECOND).get();
  ORC_UNIQUE_PTR<liborc::Type> out;
  (void)(adapters::orc::GetORCType(type, &out));
  EXPECT_EQ(out->getKind(), liborc::TypeKind::TIMESTAMP);
}
TEST(TestAdapterWriteConverter, typeTimestampMilli) {
  DataType* type = timestamp(TimeUnit::type::MILLI).get();
  ORC_UNIQUE_PTR<liborc::Type> out;
  (void)(adapters::orc::GetORCType(type, &out));
  EXPECT_EQ(out->getKind(), liborc::TypeKind::TIMESTAMP);
}
TEST(TestAdapterWriteConverter, typeTimestampMicro) {
  DataType* type = timestamp(TimeUnit::type::MICRO).get();
  ORC_UNIQUE_PTR<liborc::Type> out;
  (void)(adapters::orc::GetORCType(type, &out));
  EXPECT_EQ(out->getKind(), liborc::TypeKind::TIMESTAMP);
}
TEST(TestAdapterWriteConverter, typeTimestampNano) {
  DataType* type = timestamp(TimeUnit::type::NANO).get();
  ORC_UNIQUE_PTR<liborc::Type> out;
  (void)(adapters::orc::GetORCType(type, &out));
  EXPECT_EQ(out->getKind(), liborc::TypeKind::TIMESTAMP);
}
TEST(TestAdapterWriteConverter, typeDecimal) {
  DataType* type = decimal(32, 5).get();
  ORC_UNIQUE_PTR<liborc::Type> out;
  (void)(adapters::orc::GetORCType(type, &out));
  EXPECT_EQ(out->getKind(), liborc::TypeKind::DECIMAL);
  EXPECT_EQ(out->getPrecision(), 32);
  EXPECT_EQ(out->getScale(), 5);
}
TEST(TestAdapterWriteConverter, typeList) {
  auto sharedPtrArrowType = list(std::make_shared<Field>("a", int32()));
  DataType* type = sharedPtrArrowType.get();
  ORC_UNIQUE_PTR<liborc::Type> out;
  (void)(adapters::orc::GetORCType(type, &out));
  EXPECT_EQ(out->getSubtypeCount(), 1);
  EXPECT_EQ(out->getKind(), liborc::TypeKind::LIST);
  EXPECT_EQ(out->getSubtype(0)->getKind(), liborc::TypeKind::INT);
}
TEST(TestAdapterWriteConverter, typeLargeList) {
  auto sharedPtrArrowType = large_list(std::make_shared<Field>("a", int32()));
  DataType* type = sharedPtrArrowType.get();
  ORC_UNIQUE_PTR<liborc::Type> out;
  (void)(adapters::orc::GetORCType(type, &out));
  EXPECT_EQ(out->getSubtypeCount(), 1);
  EXPECT_EQ(out->getKind(), liborc::TypeKind::LIST);
  EXPECT_EQ(out->getSubtype(0)->getKind(), liborc::TypeKind::INT);
}
TEST(TestAdapterWriteConverter, typeFixedSizeList) {
  auto sharedPtrArrowType = fixed_size_list(std::make_shared<Field>("a", int32()), 3);
  DataType* type = sharedPtrArrowType.get();
  ORC_UNIQUE_PTR<liborc::Type> out;
  (void)(adapters::orc::GetORCType(type, &out));
  EXPECT_EQ(out->getSubtypeCount(), 1);
  EXPECT_EQ(out->getKind(), liborc::TypeKind::LIST);
  EXPECT_EQ(out->getSubtype(0)->getKind(), liborc::TypeKind::INT);
}
TEST(TestAdapterWriteConverter, typeFixedSizeListZero) {
  auto sharedPtrArrowType = fixed_size_list(std::make_shared<Field>("a", int32()), 0);
  DataType* type = sharedPtrArrowType.get();
  ORC_UNIQUE_PTR<liborc::Type> out;
  (void)(adapters::orc::GetORCType(type, &out));
  EXPECT_EQ(out->getSubtypeCount(), 1);
  EXPECT_EQ(out->getKind(), liborc::TypeKind::LIST);
  EXPECT_EQ(out->getSubtype(0)->getKind(), liborc::TypeKind::INT);
}
TEST(TestAdapterWriteConverter, typeStructTrivial) {
  std::vector<std::shared_ptr<Field>> xFields;
  auto sharedPtrArrowType = struct_(xFields);
  DataType* type = sharedPtrArrowType.get();
  ORC_UNIQUE_PTR<liborc::Type> out;
  (void)(adapters::orc::GetORCType(type, &out));
  EXPECT_EQ(out->getSubtypeCount(), 0);
  EXPECT_EQ(out->getKind(), liborc::TypeKind::STRUCT);
}
TEST(TestAdapterWriteConverter, typeStructSingleton) {
  std::vector<std::shared_ptr<Field>> xFields;
  xFields.push_back(std::make_shared<Field>("a", utf8()));
  auto sharedPtrArrowType = struct_(xFields);
  DataType* type = sharedPtrArrowType.get();
  ORC_UNIQUE_PTR<liborc::Type> out;
  (void)(adapters::orc::GetORCType(type, &out));
  EXPECT_EQ(out->getSubtypeCount(), 1);
  EXPECT_EQ(out->getKind(), liborc::TypeKind::STRUCT);
  EXPECT_EQ(out->getFieldName(0), "a");
  EXPECT_EQ(out->getSubtype(0)->getKind(), liborc::TypeKind::STRING);
}
TEST(TestAdapterWriteConverter, typeStruct) {
  std::vector<std::shared_ptr<Field>> xFields;
  xFields.push_back(std::make_shared<Field>("a", utf8()));
  xFields.push_back(std::make_shared<Field>("b", int32()));
  auto sharedPtrArrowType = struct_(xFields);
  DataType* type = sharedPtrArrowType.get();
  ORC_UNIQUE_PTR<liborc::Type> out;
  (void)(adapters::orc::GetORCType(type, &out));
  EXPECT_EQ(out->getSubtypeCount(), 2);
  EXPECT_EQ(out->getKind(), liborc::TypeKind::STRUCT);
  EXPECT_EQ(out->getFieldName(0), "a");
  EXPECT_EQ(out->getFieldName(1), "b");
  EXPECT_EQ(out->getSubtype(0)->getKind(), liborc::TypeKind::STRING);
  EXPECT_EQ(out->getSubtype(1)->getKind(), liborc::TypeKind::INT);
}
TEST(TestAdapterWriteConverter, typeMap) {
  auto sharedPtrArrowType = map(utf8(), int32());
  DataType* type = sharedPtrArrowType.get();
  ORC_UNIQUE_PTR<liborc::Type> out;
  (void)(adapters::orc::GetORCType(type, &out));
  EXPECT_EQ(out->getSubtypeCount(), 2);
  EXPECT_EQ(out->getKind(), liborc::TypeKind::MAP);
  EXPECT_EQ(out->getSubtype(0)->getKind(), liborc::TypeKind::STRING);
  EXPECT_EQ(out->getSubtype(1)->getKind(), liborc::TypeKind::INT);
}
TEST(TestAdapterWriteConverter, typeDenseUnionTrivial) {
  std::vector<std::shared_ptr<Field>> xFields;
  auto sharedPtrArrowType = dense_union(xFields);
  DataType* type = sharedPtrArrowType.get();
  ORC_UNIQUE_PTR<liborc::Type> out;
  (void)(adapters::orc::GetORCType(type, &out));
  EXPECT_EQ(out->getSubtypeCount(), 0);
  EXPECT_EQ(out->getKind(), liborc::TypeKind::UNION);
}
TEST(TestAdapterWriteConverter, typeDenseUnionSingleton) {
  std::vector<std::shared_ptr<Field>> xFields;
  xFields.push_back(std::make_shared<Field>("a", utf8()));
  auto sharedPtrArrowType = dense_union(xFields);
  DataType* type = sharedPtrArrowType.get();
  ORC_UNIQUE_PTR<liborc::Type> out;
  (void)(adapters::orc::GetORCType(type, &out));
  EXPECT_EQ(out->getSubtypeCount(), 1);
  EXPECT_EQ(out->getKind(), liborc::TypeKind::UNION);
  EXPECT_EQ(out->getSubtype(0)->getKind(), liborc::TypeKind::STRING);
}
TEST(TestAdapterWriteConverter, typeDenseUnion) {
  std::vector<std::shared_ptr<Field>> xFields;
  xFields.push_back(std::make_shared<Field>("a", utf8()));
  xFields.push_back(std::make_shared<Field>("b", int32()));
  auto sharedPtrArrowType = dense_union(xFields);
  DataType* type = sharedPtrArrowType.get();
  ORC_UNIQUE_PTR<liborc::Type> out;
  (void)(adapters::orc::GetORCType(type, &out));
  EXPECT_EQ(out->getSubtypeCount(), 2);
  EXPECT_EQ(out->getKind(), liborc::TypeKind::UNION);
  EXPECT_EQ(out->getSubtype(0)->getKind(), liborc::TypeKind::STRING);
  EXPECT_EQ(out->getSubtype(1)->getKind(), liborc::TypeKind::INT);
}
TEST(TestAdapterWriteConverter, typeSparseUnionTrivial) {
  std::vector<std::shared_ptr<Field>> xFields;
  auto sharedPtrArrowType = sparse_union(xFields);
  DataType* type = sharedPtrArrowType.get();
  ORC_UNIQUE_PTR<liborc::Type> out;
  (void)(adapters::orc::GetORCType(type, &out));
  EXPECT_EQ(out->getSubtypeCount(), 0);
  EXPECT_EQ(out->getKind(), liborc::TypeKind::UNION);
}
TEST(TestAdapterWriteConverter, typeSparseUnionSingleton) {
  std::vector<std::shared_ptr<Field>> xFields;
  xFields.push_back(std::make_shared<Field>("b", int32()));
  auto sharedPtrArrowType = sparse_union(xFields);
  DataType* type = sharedPtrArrowType.get();
  ORC_UNIQUE_PTR<liborc::Type> out;
  (void)(adapters::orc::GetORCType(type, &out));
  EXPECT_EQ(out->getSubtypeCount(), 1);
  EXPECT_EQ(out->getKind(), liborc::TypeKind::UNION);
  EXPECT_EQ(out->getSubtype(0)->getKind(), liborc::TypeKind::INT);
}
TEST(TestAdapterWriteConverter, typeSparseUnion) {
  std::vector<std::shared_ptr<Field>> xFields;
  xFields.push_back(std::make_shared<Field>("a", utf8()));
  xFields.push_back(std::make_shared<Field>("b", int32()));
  auto sharedPtrArrowType = sparse_union(xFields);
  DataType* type = sharedPtrArrowType.get();
  ORC_UNIQUE_PTR<liborc::Type> out;
  (void)(adapters::orc::GetORCType(type, &out));
  EXPECT_EQ(out->getSubtypeCount(), 2);
  EXPECT_EQ(out->getKind(), liborc::TypeKind::UNION);
  EXPECT_EQ(out->getSubtype(0)->getKind(), liborc::TypeKind::STRING);
  EXPECT_EQ(out->getSubtype(1)->getKind(), liborc::TypeKind::INT);
}
TEST(TestAdapterWriteConverter, typeListOfList) {
  auto sharedPtrArrowSubtype = list(std::make_shared<Field>("a", int32()));
  auto sharedPtrArrowType = list(std::make_shared<Field>("a", sharedPtrArrowSubtype));
  DataType* type = sharedPtrArrowType.get();
  ORC_UNIQUE_PTR<liborc::Type> out;
  (void)(adapters::orc::GetORCType(type, &out));
  EXPECT_EQ(out->getSubtypeCount(), 1);
  EXPECT_EQ(out->getKind(), liborc::TypeKind::LIST);
  EXPECT_EQ(out->getSubtype(0)->getSubtypeCount(), 1);
  EXPECT_EQ(out->getSubtype(0)->getKind(), liborc::TypeKind::LIST);
  EXPECT_EQ(out->getSubtype(0)->getSubtype(0)->getKind(), liborc::TypeKind::INT);
}
TEST(TestAdapterWriteConverter, typeListOfMap) {
  auto sharedPtrArrowSubtype = map(utf8(), int32());
  auto sharedPtrArrowType = list(std::make_shared<Field>("a", sharedPtrArrowSubtype));
  DataType* type = sharedPtrArrowType.get();
  ORC_UNIQUE_PTR<liborc::Type> out;
  (void)(adapters::orc::GetORCType(type, &out));
  EXPECT_EQ(out->getSubtypeCount(), 1);
  EXPECT_EQ(out->getKind(), liborc::TypeKind::LIST);
  EXPECT_EQ(out->getSubtype(0)->getSubtypeCount(), 2);
  EXPECT_EQ(out->getSubtype(0)->getKind(), liborc::TypeKind::MAP);
  EXPECT_EQ(out->getSubtype(0)->getSubtype(0)->getKind(), liborc::TypeKind::STRING);
  EXPECT_EQ(out->getSubtype(0)->getSubtype(1)->getKind(), liborc::TypeKind::INT);
}
TEST(TestAdapterWriteConverter, typeListOfStructOfLists) {
  auto sharedPtrArrowSubsubtype0 = list(std::make_shared<Field>("a", int8()));
  auto sharedPtrArrowSubsubtype1 = list(std::make_shared<Field>("b", float64()));
  auto sharedPtrArrowSubsubtype2 = list(std::make_shared<Field>("c", date32()));
  std::vector<std::shared_ptr<Field>> xFields;
  xFields.push_back(std::make_shared<Field>("a", sharedPtrArrowSubsubtype0));
  xFields.push_back(std::make_shared<Field>("b", sharedPtrArrowSubsubtype1));
  xFields.push_back(std::make_shared<Field>("c", sharedPtrArrowSubsubtype2));
  auto sharedPtrArrowSubtype = struct_(xFields);
  auto sharedPtrArrowType = list(std::make_shared<Field>("x", sharedPtrArrowSubtype));
  DataType* type = sharedPtrArrowType.get();
  ORC_UNIQUE_PTR<liborc::Type> out;
  (void)(adapters::orc::GetORCType(type, &out));
  EXPECT_EQ(out->getSubtypeCount(), 1);
  EXPECT_EQ(out->getKind(), liborc::TypeKind::LIST);
  EXPECT_EQ(out->getSubtype(0)->getSubtypeCount(), 3);
  EXPECT_EQ(out->getSubtype(0)->getKind(), liborc::TypeKind::STRUCT);
  EXPECT_EQ(out->getSubtype(0)->getSubtype(0)->getSubtypeCount(), 1);
  EXPECT_EQ(out->getSubtype(0)->getSubtype(0)->getKind(), liborc::TypeKind::LIST);
  EXPECT_EQ(out->getSubtype(0)->getSubtype(1)->getSubtypeCount(), 1);
  EXPECT_EQ(out->getSubtype(0)->getSubtype(1)->getKind(), liborc::TypeKind::LIST);
  EXPECT_EQ(out->getSubtype(0)->getSubtype(2)->getSubtypeCount(), 1);
  EXPECT_EQ(out->getSubtype(0)->getSubtype(2)->getKind(), liborc::TypeKind::LIST);
  EXPECT_EQ(out->getSubtype(0)->getSubtype(0)->getSubtype(0)->getKind(),
            liborc::TypeKind::BYTE);
  EXPECT_EQ(out->getSubtype(0)->getSubtype(1)->getSubtype(0)->getKind(),
            liborc::TypeKind::DOUBLE);
  EXPECT_EQ(out->getSubtype(0)->getSubtype(2)->getSubtype(0)->getKind(),
            liborc::TypeKind::DATE);
}
TEST(TestAdapterWriteConverter, schemaTrivial) {
  std::vector<std::shared_ptr<Field>> xFields;
  std::shared_ptr<Schema> sharedPtrSchema = std::make_shared<Schema>(xFields);
  ORC_UNIQUE_PTR<liborc::Type> out;
  (void)(adapters::orc::GetORCType(sharedPtrSchema.get(), &out));
  EXPECT_EQ(out->getSubtypeCount(), 0);
  EXPECT_EQ(out->getKind(), liborc::TypeKind::STRUCT);
}
TEST(TestAdapterWriteConverter, schemaSingleton) {
  std::vector<std::shared_ptr<Field>> xFields;
  xFields.push_back(std::make_shared<Field>("a", utf8()));
  std::shared_ptr<Schema> sharedPtrSchema = std::make_shared<Schema>(xFields);
  ORC_UNIQUE_PTR<liborc::Type> out;
  (void)(adapters::orc::GetORCType(sharedPtrSchema.get(), &out));
  EXPECT_EQ(out->getSubtypeCount(), 1);
  EXPECT_EQ(out->getKind(), liborc::TypeKind::STRUCT);
  EXPECT_EQ(out->getFieldName(0), "a");
  EXPECT_EQ(out->getSubtype(0)->getKind(), liborc::TypeKind::STRING);
}
TEST(TestAdapterWriteConverter, schemaMixed1) {
  auto sharedPtrArrowSubsubtype0 = list(std::make_shared<Field>("a", large_utf8()));
  auto sharedPtrArrowSubtype0 =
      list(std::make_shared<Field>("a", sharedPtrArrowSubsubtype0));
  auto sharedPtrArrowSubtype1 = list(std::make_shared<Field>("b", decimal(30, 4)));
  auto sharedPtrArrowSubtype2 =
      list(std::make_shared<Field>("c", timestamp(TimeUnit::type::MICRO)));
  std::vector<std::shared_ptr<Field>> xFields;
  xFields.push_back(std::make_shared<Field>("a", sharedPtrArrowSubtype0));
  xFields.push_back(std::make_shared<Field>("b", sharedPtrArrowSubtype1));
  xFields.push_back(std::make_shared<Field>("c", sharedPtrArrowSubtype2));
  xFields.push_back(std::make_shared<Field>("d", boolean()));
  xFields.push_back(std::make_shared<Field>("e", fixed_size_binary(5)));
  std::shared_ptr<Schema> sharedPtrSchema = std::make_shared<Schema>(xFields);
  ORC_UNIQUE_PTR<liborc::Type> out;
  (void)(adapters::orc::GetORCType(sharedPtrSchema.get(), &out));
  EXPECT_EQ(out->getSubtypeCount(), 5);
  EXPECT_EQ(out->getKind(), liborc::TypeKind::STRUCT);
  EXPECT_EQ(out->getFieldName(0), "a");
  EXPECT_EQ(out->getFieldName(1), "b");
  EXPECT_EQ(out->getFieldName(2), "c");
  EXPECT_EQ(out->getFieldName(3), "d");
  EXPECT_EQ(out->getFieldName(4), "e");
  EXPECT_EQ(out->getSubtype(0)->getSubtypeCount(), 1);
  EXPECT_EQ(out->getSubtype(0)->getKind(), liborc::TypeKind::LIST);
  EXPECT_EQ(out->getSubtype(1)->getSubtypeCount(), 1);
  EXPECT_EQ(out->getSubtype(1)->getKind(), liborc::TypeKind::LIST);
  EXPECT_EQ(out->getSubtype(2)->getSubtypeCount(), 1);
  EXPECT_EQ(out->getSubtype(2)->getKind(), liborc::TypeKind::LIST);
  EXPECT_EQ(out->getSubtype(3)->getKind(), liborc::TypeKind::BOOLEAN);
  EXPECT_EQ(out->getSubtype(4)->getKind(), liborc::TypeKind::BINARY);
  EXPECT_EQ(out->getSubtype(0)->getSubtype(0)->getSubtypeCount(), 1);
  EXPECT_EQ(out->getSubtype(0)->getSubtype(0)->getKind(), liborc::TypeKind::LIST);
  EXPECT_EQ(out->getSubtype(1)->getSubtype(0)->getKind(), liborc::TypeKind::DECIMAL);
  EXPECT_EQ(out->getSubtype(2)->getSubtype(0)->getKind(), liborc::TypeKind::TIMESTAMP);
  EXPECT_EQ(out->getSubtype(0)->getSubtype(0)->getSubtype(0)->getKind(),
            liborc::TypeKind::STRING);
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

// Numeric

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
  root->numElements = x->numElements;
  writer->add(*batch);
  writer->close();
  std::shared_ptr<io::RandomAccessFile> in_stream(new io::BufferReader(
      std::make_shared<Buffer>(reinterpret_cast<const uint8_t*>(mem_stream.getData()),
                               static_cast<int64_t>(mem_stream.getLength()))));

  std::unique_ptr<adapters::orc::ORCFileReader> reader;
  ASSERT_TRUE(
      adapters::orc::ORCFileReader::Open(in_stream, default_memory_pool(), &reader).ok());
  std::shared_ptr<Table> outputTable;
  ARROW_EXPECT_OK(reader->Read(&outputTable));
  auto schemaOut = outputTable->schema();
  EXPECT_EQ(outputTable->num_columns(), 1);
  EXPECT_EQ(outputTable->num_rows(), 4);
  EXPECT_TRUE(schemaOut->field(0)->type()->Equals(*arrowType));
  EXPECT_TRUE(outputTable->column(0)->chunk(0)->Equals(*array));
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
      } else {
        EXPECT_FALSE(x->notNull[i]);
      }
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
    } else {
      EXPECT_FALSE(x->notNull[i]);
    }
  }
  writer->add(*batch);
  writer->close();
  RecordProperty("length", mem_stream.getLength());
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
      } else {
        EXPECT_FALSE(x->notNull[i]);
      }
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
    } else {
      EXPECT_FALSE(x->notNull[i]);
    }
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
  uint64_t batchSize = 120;
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
  uint64_t batchSize = 35;
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
      } else {
        EXPECT_FALSE(x->notNull[i]);
      }
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
    } else {
      EXPECT_FALSE(x->notNull[i]);
    }
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
      } else {
        EXPECT_FALSE(x->notNull[i]);
      }
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
    } else {
      EXPECT_FALSE(x->notNull[i]);
    }
  }
  writer->add(*batch);
  writer->close();
}
TEST(TestAdapterWriteNumerical, writeInt32ChunkedMultibatchFromList) {
  auto sharedPtrArrowType = list(std::make_shared<Field>("a", int32()));
  // DataType* arrowType = sharedPtrArrowType.get();

  uint64_t totalLength = 150;
  Int32Builder valuesBuilder1, valuesBuilder3;
  int32_t offset1[51], offset3[51];

  offset1[0] = 0;
  for (int i = 0; i < 50; i++) {
    switch (i % 4) {
      case 0: {
        offset1[i + 1] = offset1[i];
        break;
      }
      case 1: {
        (void)(valuesBuilder1.Append(i - 1));
        offset1[i + 1] = offset1[i] + 1;
        break;
      }
      case 2: {
        (void)(valuesBuilder1.AppendNull());
        (void)(valuesBuilder1.AppendNull());
        offset1[i + 1] = offset1[i] + 2;
        break;
      }
      default: {
        (void)(valuesBuilder1.Append(i - 1));
        (void)(valuesBuilder1.AppendNull());
        (void)(valuesBuilder1.AppendNull());
        offset1[i + 1] = offset1[i] + 3;
      }
    }
  }

  offset3[0] = 0;
  for (int i = 0; i < 50; i++) {
    switch ((i + 50) % 4) {
      case 0: {
        offset3[i + 1] = offset3[i];
        break;
      }
      case 1: {
        (void)(valuesBuilder3.Append(i + 50 - 1));
        offset3[i + 1] = offset3[i] + 1;
        break;
      }
      case 2: {
        (void)(valuesBuilder3.AppendNull());
        (void)(valuesBuilder3.AppendNull());
        offset3[i + 1] = offset3[i] + 2;
        break;
      }
      default: {
        (void)(valuesBuilder3.Append(i + 50 - 1));
        (void)(valuesBuilder3.AppendNull());
        (void)(valuesBuilder3.AppendNull());
        offset3[i + 1] = offset3[i] + 3;
      }
    }
  }

  // Every third entry has null at struct level
  uint8_t bitmap1[7] = {
      182, 109, 219, 182,
      109, 219, 2};  // 10110110 01101101 11011011 10110110 01101101 11011011 00000010
  uint8_t bitmap3[7] = {
      109, 219, 182, 109,
      219, 182, 1};  // 01101101 11011011 10110110 01101101 11011011 10110110 00000001

  BufferBuilder builder1, builder3, offsetsBuilder1, offsetsBuilder3;
  (void)(builder1.Resize(7));
  (void)(builder1.Append(bitmap1, 7));
  std::shared_ptr<arrow::Buffer> bitmapBuffer1;
  if (!builder1.Finish(&bitmapBuffer1).ok()) {
    FAIL() << "The offsets buffer can not be constructed!";
  }
  (void)(builder3.Resize(7));
  (void)(builder3.Append(bitmap3, 7));
  std::shared_ptr<arrow::Buffer> bitmapBuffer3;
  if (!builder3.Finish(&bitmapBuffer3).ok()) {
    FAIL() << "The offsets buffer can not be constructed!";
  }

  (void)(offsetsBuilder1.Resize(204));
  (void)(offsetsBuilder1.Append(offset1, 204));
  std::shared_ptr<arrow::Buffer> offsetsBuffer1;
  if (!offsetsBuilder1.Finish(&offsetsBuffer1).ok()) {
    FAIL() << "The offsets buffer can not be constructed!";
  }
  (void)(offsetsBuilder3.Resize(204));
  (void)(offsetsBuilder3.Append(offset3, 204));
  std::shared_ptr<arrow::Buffer> offsetsBuffer3;
  if (!offsetsBuilder3.Finish(&offsetsBuffer3).ok()) {
    FAIL() << "The offsets buffer can not be constructed!";
  }

  Int32Builder valuesBuilder0, offsetsBuilder0, valuesBuilder2, offsetsBuilder2,
      valuesBuilder4, offsetsBuilder4;
  std::shared_ptr<Array> valuesArray0, offsetsArray0, valuesArray2, offsetsArray2,
      valuesArray4, offsetsArray4;
  (void)(valuesBuilder0.Finish(&valuesArray0));
  (void)(offsetsBuilder0.Append(0));
  (void)(offsetsBuilder0.Finish(&offsetsArray0));
  (void)(valuesBuilder2.Finish(&valuesArray2));
  (void)(offsetsBuilder2.Append(0));
  (void)(offsetsBuilder2.Finish(&offsetsArray2));
  (void)(valuesBuilder4.Finish(&valuesArray4));
  (void)(offsetsBuilder4.Append(0));
  (void)(offsetsBuilder4.Finish(&offsetsArray4));

  std::shared_ptr<Array> valuesArray1, valuesArray3;
  (void)(valuesBuilder1.Finish(&valuesArray1));
  (void)(valuesBuilder3.Finish(&valuesArray3));

  std::shared_ptr<ListArray> array0 =
      ListArray::FromArrays(*offsetsArray0, *valuesArray0).ValueOrDie();
  std::shared_ptr<ListArray> array2 =
      ListArray::FromArrays(*offsetsArray2, *valuesArray2).ValueOrDie();
  std::shared_ptr<ListArray> array4 =
      ListArray::FromArrays(*offsetsArray4, *valuesArray4).ValueOrDie();
  auto array1 = std::make_shared<ListArray>(sharedPtrArrowType, 50, offsetsBuffer1,
                                            valuesArray1, bitmapBuffer1);
  auto array3 = std::make_shared<ListArray>(sharedPtrArrowType, 50, offsetsBuffer3,
                                            valuesArray3, bitmapBuffer3);

  ArrayVector av;
  av.push_back(valuesArray0);
  av.push_back(valuesArray1);
  av.push_back(valuesArray2);
  av.push_back(valuesArray3);
  av.push_back(valuesArray4);
  std::shared_ptr<ChunkedArray> carray = std::make_shared<ChunkedArray>(av);

  // RecordProperty("l", carray->length());
  // RecordProperty("l0", carray->chunk(0)->length());
  // RecordProperty("l1", carray->chunk(1)->length());
  // RecordProperty("l2", carray->chunk(2)->length());
  // RecordProperty("l3", carray->chunk(3)->length());
  // RecordProperty("l4", carray->chunk(4)->length());

  // auto a1 = std::static_pointer_cast<Int32Array>(carray->chunk(1));
  // RecordProperty("l1", a1->length());
  // auto a3 = std::static_pointer_cast<Int32Array>(carray->chunk(3));
  // RecordProperty("l3", a3->length());

  // for (int i = 0; i < 73; i++) {
  //   RecordProperty("v" + std::to_string(i), a1->Value(i));
  //   RecordProperty("an" + std::to_string(i), a1->IsNull(i));
  // }
  // for (int i = 0; i < 77; i++) {
  //   RecordProperty("v" + std::to_string(i + 73), a3->Value(i));
  //   RecordProperty("an" + std::to_string(i + 73), a3->IsNull(i));
  // }

  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(liborc::Type::buildTypeFromString("struct<a:int>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 100;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::LongVectorBatch* x =
      internal::checked_cast<liborc::LongVectorBatch*>(root->fields[0]);
  int64_t arrowIndexOffset = 0;
  int arrowChunkOffset = 0;
  uint64_t resultOffset = 0;
  while (resultOffset < totalLength - batchSize) {
    Status st = adapters::orc::FillBatch(int32().get(), x, arrowIndexOffset,
                                         arrowChunkOffset, batchSize, carray.get());
    if (!st.ok()) {
      FAIL() << "ORC ColumnBatch not successfully filled";
    }
    writer->add(*batch);

    for (uint64_t i = 0; i < batchSize; i++) {
      if ((i + resultOffset) % 3) {
        EXPECT_FALSE(x->notNull[i]);
      } else {
        EXPECT_TRUE(x->notNull[i]);
        EXPECT_EQ(x->data[i], (i + resultOffset) / 3 * 2);
      }
    }
    EXPECT_EQ(x->numElements, batchSize);
    EXPECT_TRUE(x->hasNulls);
    resultOffset = resultOffset + batchSize;
    batch->clear();
  }
  Status st = adapters::orc::FillBatch(int32().get(), x, arrowIndexOffset,
                                       arrowChunkOffset, batchSize, carray.get());
  if (!st.ok()) {
    FAIL() << "ORC ColumnBatch not successfully filled";
  }
  uint64_t lastBatchSize = totalLength - resultOffset;
  EXPECT_EQ(x->numElements, lastBatchSize);
  EXPECT_TRUE(x->hasNulls);
  for (uint64_t i = 0; i < lastBatchSize; i++) {
    if ((i + resultOffset) % 3) {
      EXPECT_FALSE(x->notNull[i]);
    } else {
      EXPECT_TRUE(x->notNull[i]);
      EXPECT_EQ(x->data[i], (i + resultOffset) / 3 * 2);
    }
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
  RecordProperty("length", mem_stream.getLength());
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
  uint64_t batchSize = 29;
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
      } else {
        EXPECT_FALSE(x->notNull[i]);
      }
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
    } else {
      EXPECT_FALSE(x->notNull[i]);
    }
  }
  writer->add(*batch);
  writer->close();
  RecordProperty("length", mem_stream.getLength());
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
  uint64_t batchSize = 1;
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
      } else {
        EXPECT_FALSE(x->notNull[i]);
      }
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
    } else {
      EXPECT_FALSE(x->notNull[i]);
    }
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
  uint64_t batchSize = 18;
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
    root->numElements = x->numElements;
    writer->add(*batch);
    EXPECT_EQ(x->numElements, batchSize);
    EXPECT_TRUE(x->hasNulls);
    for (uint64_t i = 0; i < batchSize; i++) {
      if ((i + resultOffset) % 2) {
        EXPECT_TRUE(x->notNull[i]);
        EXPECT_DOUBLE_EQ(x->data[i], i + resultOffset + 0.5);
      } else {
        EXPECT_FALSE(x->notNull[i]);
      }
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
    } else {
      EXPECT_FALSE(x->notNull[i]);
    }
  }
  root->numElements = x->numElements;
  writer->add(*batch);
  writer->close();
  std::shared_ptr<io::RandomAccessFile> in_stream(new io::BufferReader(
      std::make_shared<Buffer>(reinterpret_cast<const uint8_t*>(mem_stream.getData()),
                               static_cast<int64_t>(mem_stream.getLength()))));

  std::unique_ptr<adapters::orc::ORCFileReader> reader;
  ASSERT_TRUE(
      adapters::orc::ORCFileReader::Open(in_stream, default_memory_pool(), &reader).ok());
  std::shared_ptr<Table> outputTable;
  ARROW_EXPECT_OK(reader->Read(&outputTable));
  auto schemaOut = outputTable->schema();
  EXPECT_EQ(outputTable->num_columns(), 1);
  EXPECT_EQ(outputTable->num_rows(), 100);
  EXPECT_TRUE(schemaOut->field(0)->type()->Equals(*(carray->type())));
  EXPECT_TRUE(outputTable->column(0)->Equals(*carray));
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
  MemoryOutputStream mem_stream(5 * DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:decimal(38,6)>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 6;
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
    root->numElements = x->numElements;
    writer->add(*batch);
    EXPECT_EQ(x->numElements, batchSize);
    EXPECT_TRUE(x->hasNulls);
    for (uint64_t i = 0; i < batchSize; i++) {
      if ((i + resultOffset) % 2) {
        EXPECT_TRUE(x->notNull[i]);
        EXPECT_EQ(Decimal128(x->values[i].toDecimalString(6)),
                  Decimal128(std::to_string(i + resultOffset) + ".567891"));
      } else {
        EXPECT_FALSE(x->notNull[i]);
      }
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
    } else {
      EXPECT_FALSE(x->notNull[i]);
    }
  }
  root->numElements = x->numElements;
  writer->add(*batch);
  writer->close();
  std::shared_ptr<io::RandomAccessFile> in_stream(new io::BufferReader(
      std::make_shared<Buffer>(reinterpret_cast<const uint8_t*>(mem_stream.getData()),
                               static_cast<int64_t>(mem_stream.getLength()))));

  std::unique_ptr<adapters::orc::ORCFileReader> reader;
  ASSERT_TRUE(
      adapters::orc::ORCFileReader::Open(in_stream, default_memory_pool(), &reader).ok());
  std::shared_ptr<Table> outputTable;
  ARROW_EXPECT_OK(reader->Read(&outputTable));
  auto schemaOut = outputTable->schema();
  EXPECT_EQ(outputTable->num_columns(), 1);
  EXPECT_EQ(outputTable->num_rows(), 100);
  auto outputType = std::static_pointer_cast<Decimal128Type>(schemaOut->field(0)->type());
  EXPECT_EQ(outputType->precision(), 38);
  EXPECT_EQ(outputType->scale(), 6);
  RecordProperty("new", schemaOut->field(0)->type()->ToString());
  RecordProperty("old", carray->type()->ToString());
  EXPECT_TRUE(outputTable->column(0)->Equals(*carray));
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
  MemoryOutputStream mem_stream(5 * DEFAULT_SMALL_MEM_STREAM_SIZE);
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
    root->numElements = x->numElements;
    writer->add(*batch);
    EXPECT_EQ(x->numElements, batchSize);
    EXPECT_TRUE(x->hasNulls);
    for (uint64_t i = 0; i < batchSize; i++) {
      if ((i + resultOffset) % 2) {
        EXPECT_TRUE(x->notNull[i]);
        EXPECT_EQ(Decimal128(x->values[i].toDecimalString(0)),
                  Decimal128(std::to_string(i + resultOffset)));
      } else {
        EXPECT_FALSE(x->notNull[i]);
      }
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
    } else {
      EXPECT_FALSE(x->notNull[i]);
    }
  }
  root->numElements = x->numElements;
  writer->add(*batch);
  writer->close();
  std::shared_ptr<io::RandomAccessFile> in_stream(new io::BufferReader(
      std::make_shared<Buffer>(reinterpret_cast<const uint8_t*>(mem_stream.getData()),
                               static_cast<int64_t>(mem_stream.getLength()))));
  std::unique_ptr<adapters::orc::ORCFileReader> reader;
  ASSERT_TRUE(
      adapters::orc::ORCFileReader::Open(in_stream, default_memory_pool(), &reader).ok());
  std::shared_ptr<Table> outputTable;
  ARROW_EXPECT_OK(reader->Read(&outputTable));
  auto schemaOut = outputTable->schema();
  EXPECT_EQ(outputTable->num_columns(), 1);
  EXPECT_EQ(outputTable->num_rows(), 100);
  auto outputType = std::static_pointer_cast<Decimal128Type>(schemaOut->field(0)->type());
  EXPECT_EQ(outputType->precision(), 38);
  EXPECT_EQ(outputType->scale(), 0);
  RecordProperty("cat", (int64_t)arrowType);
  RecordProperty("new", schemaOut->field(0)->type()->ToString());
  RecordProperty("old", carray->type()->ToString());
  EXPECT_TRUE(schemaOut->field(0)->type()->Equals(*(carray->type())));
  EXPECT_TRUE(outputTable->column(0)->Equals(*carray));
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
TEST(TestAdapterWriteTime, writeDate32ChunkedMultibatch) {
  uint64_t totalLength = 100;
  Date32Builder builder0, builder1, builder2, builder3, builder4;
  for (int i = 0; i < 50; i++) {
    if (i % 2)
      (void)(builder1.Append(18584 + i));
    else
      (void)(builder1.AppendNull());
  }
  for (int i = 50; i < 100; i++) {
    if (i % 2)
      (void)(builder3.Append(18584 + i));
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
      liborc::Type::buildTypeFromString("struct<x:date>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 50;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::LongVectorBatch* x =
      internal::checked_cast<liborc::LongVectorBatch*>(root->fields[0]);
  DataType* arrowType = date32().get();
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
        EXPECT_EQ(x->data[i], i + resultOffset + 18584);
      } else {
        EXPECT_FALSE(x->notNull[i]);
      }
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
      EXPECT_EQ(x->data[i], i + resultOffset + 18584);
    } else {
      EXPECT_FALSE(x->notNull[i]);
    }
  }
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
TEST(TestAdapterWriteTime, writeDate64ChunkedMultibatch) {
  uint64_t totalLength = 100;
  Date64Builder builder0, builder1, builder2, builder3, builder4;
  for (int i = 0; i < 50; i++) {
    if (i % 2)
      (void)(builder1.Append(1605758461552 + i));
    else
      (void)(builder1.AppendNull());
  }
  for (int i = 50; i < 100; i++) {
    if (i % 2)
      (void)(builder3.Append(1605758461552 + i));
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
      liborc::Type::buildTypeFromString("struct<x:timestamp>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 35;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::TimestampVectorBatch* x =
      internal::checked_cast<liborc::TimestampVectorBatch*>(root->fields[0]);
  DataType* arrowType = date64().get();
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
        EXPECT_EQ(x->data[i], 1605758461);
        EXPECT_EQ(x->nanoseconds[i], (i + resultOffset + 552) * 1000000);
      } else {
        EXPECT_FALSE(x->notNull[i]);
      }
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
      EXPECT_EQ(x->data[i], 1605758461);
      EXPECT_EQ(x->nanoseconds[i], (i + resultOffset + 552) * 1000000);
    } else {
      EXPECT_FALSE(x->notNull[i]);
    }
  }
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
TEST(TestAdapterWriteTime, writeTimestampChunkedMultibatchSecond) {
  uint64_t totalLength = 100;
  TimestampBuilder builder0(timestamp(TimeUnit::type::SECOND), default_memory_pool()),
      builder1(timestamp(TimeUnit::type::SECOND), default_memory_pool()),
      builder2(timestamp(TimeUnit::type::SECOND), default_memory_pool()),
      builder3(timestamp(TimeUnit::type::SECOND), default_memory_pool()),
      builder4(timestamp(TimeUnit::type::SECOND), default_memory_pool());
  for (int i = 0; i < 50; i++) {
    if (i % 2)
      (void)(builder1.Append(1605758461 + i));
    else
      (void)(builder1.AppendNull());
  }
  for (int i = 50; i < 100; i++) {
    if (i % 2)
      (void)(builder3.Append(1605758461 + i));
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
      liborc::Type::buildTypeFromString("struct<x:timestamp>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::TimestampVectorBatch* x =
      internal::checked_cast<liborc::TimestampVectorBatch*>(root->fields[0]);
  DataType* arrowType = timestamp(TimeUnit::type::SECOND).get();
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
        EXPECT_EQ(x->data[i], i + resultOffset + 1605758461);
        EXPECT_EQ(x->nanoseconds[i], 0);
      } else {
        EXPECT_FALSE(x->notNull[i]);
      }
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
      EXPECT_EQ(x->data[i], i + resultOffset + 1605758461);
      EXPECT_EQ(x->nanoseconds[i], 0);
    } else {
      EXPECT_FALSE(x->notNull[i]);
    }
  }
  writer->add(*batch);
  writer->close();
}
TEST(TestAdapterWriteTime, writeTimestampChunkedMultibatchMilli) {
  uint64_t totalLength = 100;
  TimestampBuilder builder0(timestamp(TimeUnit::type::MILLI), default_memory_pool()),
      builder1(timestamp(TimeUnit::type::MILLI), default_memory_pool()),
      builder2(timestamp(TimeUnit::type::MILLI), default_memory_pool()),
      builder3(timestamp(TimeUnit::type::MILLI), default_memory_pool()),
      builder4(timestamp(TimeUnit::type::MILLI), default_memory_pool());
  for (int i = 0; i < 50; i++) {
    if (i % 2)
      (void)(builder1.Append(1605758461398 + i));
    else
      (void)(builder1.AppendNull());
  }
  for (int i = 50; i < 100; i++) {
    if (i % 2)
      (void)(builder3.Append(1605758461398 + i));
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
      liborc::Type::buildTypeFromString("struct<x:timestamp>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::TimestampVectorBatch* x =
      internal::checked_cast<liborc::TimestampVectorBatch*>(root->fields[0]);
  DataType* arrowType = timestamp(TimeUnit::type::MILLI).get();
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
        EXPECT_EQ(x->data[i], 1605758461);
        EXPECT_EQ(x->nanoseconds[i], (i + resultOffset + 398) * 1000000);
      } else {
        EXPECT_FALSE(x->notNull[i]);
      }
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
      EXPECT_EQ(x->data[i], 1605758461);
      EXPECT_EQ(x->nanoseconds[i], (i + resultOffset + 398) * 1000000);
    } else {
      EXPECT_FALSE(x->notNull[i]);
    }
  }
  writer->add(*batch);
  writer->close();
}
TEST(TestAdapterWriteTime, writeTimestampChunkedMultibatchMicro) {
  uint64_t totalLength = 100;
  TimestampBuilder builder0(timestamp(TimeUnit::type::MICRO), default_memory_pool()),
      builder1(timestamp(TimeUnit::type::MICRO), default_memory_pool()),
      builder2(timestamp(TimeUnit::type::MICRO), default_memory_pool()),
      builder3(timestamp(TimeUnit::type::MICRO), default_memory_pool()),
      builder4(timestamp(TimeUnit::type::MICRO), default_memory_pool());
  for (int i = 0; i < 50; i++) {
    if (i % 2)
      (void)(builder1.Append(1605758461398538 + i));
    else
      (void)(builder1.AppendNull());
  }
  for (int i = 50; i < 100; i++) {
    if (i % 2)
      (void)(builder3.Append(1605758461398538 + i));
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
      liborc::Type::buildTypeFromString("struct<x:timestamp>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::TimestampVectorBatch* x =
      internal::checked_cast<liborc::TimestampVectorBatch*>(root->fields[0]);
  DataType* arrowType = timestamp(TimeUnit::type::MICRO).get();
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
        EXPECT_EQ(x->data[i], 1605758461);
        EXPECT_EQ(x->nanoseconds[i], (i + resultOffset + 398538) * 1000);
      } else {
        EXPECT_FALSE(x->notNull[i]);
      }
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
      EXPECT_EQ(x->data[i], 1605758461);
      EXPECT_EQ(x->nanoseconds[i], (i + resultOffset + 398538) * 1000);
    } else {
      EXPECT_FALSE(x->notNull[i]);
    }
  }
  writer->add(*batch);
  writer->close();
}
// TEST(TestAdapterWriteTime, writeTimestampChunkedMultibatchNano) {
//   uint64_t totalLength = 100;
//   TimestampBuilder builder0(timestamp(TimeUnit::type::NANO), default_memory_pool()),
//       builder1(timestamp(TimeUnit::type::NANO), default_memory_pool()),
//       builder2(timestamp(TimeUnit::type::NANO), default_memory_pool()),
//       builder3(timestamp(TimeUnit::type::NANO), default_memory_pool()),
//       builder4(timestamp(TimeUnit::type::NANO), default_memory_pool());
//   for (int i = 0; i < 50; i++) {
//     if (i % 2)
//       (void)(builder1.Append(1605758461398038293 + i));
//     else
//       (void)(builder1.AppendNull());
//   }
//   for (int i = 50; i < 100; i++) {
//     if (i % 2)
//       (void)(builder3.Append(1605758461398038293 + i));
//     else
//       (void)(builder3.AppendNull());
//   }
//   std::shared_ptr<Array> array0, array1, array2, array3, array4;
//   (void)(builder0.Finish(&array0));
//   (void)(builder1.Finish(&array1));
//   (void)(builder2.Finish(&array2));
//   (void)(builder3.Finish(&array3));
//   (void)(builder4.Finish(&array4));
//   ArrayVector av;
//   av.push_back(array0);
//   av.push_back(array1);
//   av.push_back(array2);
//   av.push_back(array3);
//   av.push_back(array4);
//   std::shared_ptr<ChunkedArray> carray = std::make_shared<ChunkedArray>(av);
//   MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
//   ORC_UNIQUE_PTR<liborc::Type> schema(
//       liborc::Type::buildTypeFromString("struct<x:timestamp>"));
//   liborc::WriterOptions options;
//   ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
//   uint64_t batchSize = 10;
//   ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
//   liborc::StructVectorBatch* root =
//       internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
//   liborc::TimestampVectorBatch* x =
//       internal::checked_cast<liborc::TimestampVectorBatch*>(root->fields[0]);
//   DataType* arrowType = timestamp(TimeUnit::type::NANO).get();
//   int64_t arrowIndexOffset = 0;
//   int arrowChunkOffset = 0;
//   uint64_t resultOffset = 0;
//   while (resultOffset < totalLength - batchSize) {
//     Status st = adapters::orc::FillBatch(arrowType, x, arrowIndexOffset,
//     arrowChunkOffset,
//                                          batchSize, carray.get());
//     if (!st.ok()) {
//       FAIL() << "ORC ColumnBatch not successfully filled";
//     }
//     root->numElements = x->numElements;
//     writer->add(*batch);
//     EXPECT_EQ(x->numElements, batchSize);
//     EXPECT_TRUE(x->hasNulls);
//     for (uint64_t i = 0; i < batchSize; i++) {
//       if ((i + resultOffset) % 2) {
//         EXPECT_TRUE(x->notNull[i]);
//         EXPECT_EQ(x->data[i], 1605758461);
//         EXPECT_EQ(x->nanoseconds[i], i + resultOffset + 398038293);
//       } else {
//         EXPECT_FALSE(x->notNull[i]);
//       }
//     }
//     resultOffset = resultOffset + batchSize;
//     batch->clear();
//   }
//   Status st = adapters::orc::FillBatch(arrowType, x, arrowIndexOffset,
//   arrowChunkOffset,
//                                        batchSize, carray.get());
//   uint64_t lastBatchSize = totalLength - resultOffset;
//   EXPECT_EQ(x->numElements, lastBatchSize);
//   EXPECT_TRUE(x->hasNulls);
//   for (uint64_t i = 0; i < lastBatchSize; i++) {
//     if ((i + resultOffset) % 2) {
//       EXPECT_TRUE(x->notNull[i]);
//       EXPECT_EQ(x->data[i], 1605758461);
//       EXPECT_EQ(x->nanoseconds[i], i + resultOffset + 398038293);
//     } else {
//       EXPECT_FALSE(x->notNull[i]);
//     }
//   }
//   root->numElements = x->numElements;
//   writer->add(*batch);
//   writer->close();
//   // std::shared_ptr<io::RandomAccessFile> in_stream(new io::BufferReader(
//   //     std::make_shared<Buffer>(reinterpret_cast<const
//   uint8_t*>(mem_stream.getData()),
//   //                              static_cast<int64_t>(mem_stream.getLength()))));

//   // std::unique_ptr<adapters::orc::ORCFileReader> reader;
//   // ASSERT_TRUE(
//   //     adapters::orc::ORCFileReader::Open(in_stream, default_memory_pool(),
//   //     &reader).ok());
//   // std::shared_ptr<Table> outputTable;
//   // ARROW_EXPECT_OK(reader->Read(&outputTable));
//   // auto schemaOut = outputTable->schema();
//   // EXPECT_EQ(outputTable->num_columns(), 1);
//   // EXPECT_EQ(outputTable->num_rows(), 100);
//   // EXPECT_TRUE(schemaOut->field(0)->type()->Equals(*arrowType));
//   // EXPECT_TRUE(outputTable->column(0)->Equals(*carray));
// }

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
TEST(TestAdapterWriteBinary, writeStringChunkedEmpty) {
  ArrayVector av;
  std::shared_ptr<ChunkedArray> carray = std::make_shared<ChunkedArray>(av, utf8());
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
  int64_t arrowIndexOffset = 0;
  int arrowChunkOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowIndexOffset, arrowChunkOffset,
                                       batchSize, carray.get());
  if (!st.ok()) {
    FAIL() << "ORC ColumnBatch not successfully filled";
  }
  EXPECT_EQ(x->numElements, 0);
  EXPECT_FALSE(x->hasNulls);
  writer->add(*batch);
  writer->close();
}
TEST(TestAdapterWriteBinary, writeStringChunkedMixed) {
  StringBuilder builder0, builder1, builder2, builder3, builder4;
  (void)(builder1.AppendNull());
  (void)(builder1.Append(""));
  (void)(builder2.AppendNull());
  (void)(builder2.Append("AB"));
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
  int64_t arrowIndexOffset = 0;
  int arrowChunkOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowIndexOffset, arrowChunkOffset,
                                       batchSize, carray.get());
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
  writer->add(*batch);
  writer->close();
}
TEST(TestAdapterWriteBinary, writeStringChunkedMultibatch) {
  uint64_t totalLength = 1000;
  StringBuilder builder0, builder1, builder2, builder3, builder4;
  for (int i = 0; i < 500; i++) {
    if (i % 2) {
      (void)(builder1.Append(("Test " + std::to_string(i)).c_str()));
      // RecordProperty("value" + std::to_string(i), ("Test " +
      // std::to_string(i)).c_str());
    } else {
      (void)(builder1.AppendNull());
    }
  }
  for (int i = 500; i < 1000; i++) {
    if (i % 2) {
      (void)(builder3.Append(("Test " + std::to_string(i)).c_str()));
    } else {
      (void)(builder3.AppendNull());
    }
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
      liborc::Type::buildTypeFromString("struct<x:string>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::StringVectorBatch* x =
      internal::checked_cast<liborc::StringVectorBatch*>(root->fields[0]);
  DataType* arrowType = utf8().get();
  int64_t arrowIndexOffset = 0;
  int arrowChunkOffset = 0;
  uint64_t resultOffset = 0;
  while (resultOffset < totalLength - batchSize) {
    Status st = adapters::orc::FillBatch(arrowType, x, arrowIndexOffset, arrowChunkOffset,
                                         batchSize, carray.get());
    if (!st.ok()) {
      FAIL() << "ORC ColumnBatch not successfully filled";
    }
    root->numElements = x->numElements;
    writer->add(*batch);
    EXPECT_EQ(x->numElements, batchSize);
    EXPECT_TRUE(x->hasNulls);
    for (uint64_t i = 0; i < batchSize; i++) {
      if ((i + resultOffset) % 2) {
        EXPECT_TRUE(x->notNull[i]);
        EXPECT_STREQ(x->data[i], ("Test " + std::to_string(i + resultOffset)).c_str());
        if (i + resultOffset >= 100)
          EXPECT_EQ(x->length[i], 8);
        else if (i + resultOffset >= 10)
          EXPECT_EQ(x->length[i], 7);
        else
          EXPECT_EQ(x->length[i], 6);
      } else {
        EXPECT_FALSE(x->notNull[i]);
      }
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
      EXPECT_STREQ(x->data[i], ("Test " + std::to_string(i + resultOffset)).c_str());
      if (i + resultOffset >= 100)
        EXPECT_EQ(x->length[i], 8);
      else if (i + resultOffset >= 10)
        EXPECT_EQ(x->length[i], 7);
      else
        EXPECT_EQ(x->length[i], 6);
    } else {
      EXPECT_FALSE(x->notNull[i]);
    }
  }
  root->numElements = x->numElements;
  writer->add(*batch);
  writer->close();
  std::shared_ptr<io::RandomAccessFile> in_stream(new io::BufferReader(
      std::make_shared<Buffer>(reinterpret_cast<const uint8_t*>(mem_stream.getData()),
                               static_cast<int64_t>(mem_stream.getLength()))));

  std::unique_ptr<adapters::orc::ORCFileReader> reader;
  ASSERT_TRUE(
      adapters::orc::ORCFileReader::Open(in_stream, default_memory_pool(), &reader).ok());
  std::shared_ptr<Table> outputTable;
  ARROW_EXPECT_OK(reader->Read(&outputTable));
  auto schemaOut = outputTable->schema();
  EXPECT_EQ(outputTable->num_columns(), 1);
  EXPECT_EQ(outputTable->num_rows(), 1000);
  EXPECT_TRUE(schemaOut->field(0)->type()->Equals(*(carray->type())));
  EXPECT_TRUE(outputTable->column(0)->Equals(*carray));
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
TEST(TestAdapterWriteBinary, writeLargeStringChunkedEmpty) {
  ArrayVector av;
  std::shared_ptr<ChunkedArray> carray = std::make_shared<ChunkedArray>(av, large_utf8());
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
  int64_t arrowIndexOffset = 0;
  int arrowChunkOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowIndexOffset, arrowChunkOffset,
                                       batchSize, carray.get());
  if (!st.ok()) {
    FAIL() << "ORC ColumnBatch not successfully filled";
  }
  EXPECT_EQ(x->numElements, 0);
  EXPECT_FALSE(x->hasNulls);
  writer->add(*batch);
  writer->close();
}
TEST(TestAdapterWriteBinary, writeLargeStringChunkedMixed) {
  LargeStringBuilder builder0, builder1, builder2, builder3, builder4;
  (void)(builder1.AppendNull());
  (void)(builder1.Append(""));
  (void)(builder2.AppendNull());
  (void)(builder2.Append("AB"));
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
  int64_t arrowIndexOffset = 0;
  int arrowChunkOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowIndexOffset, arrowChunkOffset,
                                       batchSize, carray.get());
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
  writer->add(*batch);
  writer->close();
}
TEST(TestAdapterWriteBinary, writeLargeStringChunkedMultibatch) {
  uint64_t totalLength = 1000;
  LargeStringBuilder builder0, builder1, builder2, builder3, builder4;
  for (int i = 0; i < 500; i++) {
    if (i % 2) {
      (void)(builder1.Append(("Test " + std::to_string(i)).c_str()));
    } else {
      (void)(builder1.AppendNull());
    }
  }
  for (int i = 500; i < 1000; i++) {
    if (i % 2) {
      (void)(builder3.Append(("Test " + std::to_string(i)).c_str()));
    } else {
      (void)(builder3.AppendNull());
    }
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
      liborc::Type::buildTypeFromString("struct<x:string>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 17;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::StringVectorBatch* x =
      internal::checked_cast<liborc::StringVectorBatch*>(root->fields[0]);
  DataType* arrowType = large_utf8().get();
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
        EXPECT_STREQ(x->data[i], ("Test " + std::to_string(i + resultOffset)).c_str());
        if (i + resultOffset >= 100)
          EXPECT_EQ(x->length[i], 8);
        else if (i + resultOffset >= 10)
          EXPECT_EQ(x->length[i], 7);
        else
          EXPECT_EQ(x->length[i], 6);
      } else {
        EXPECT_FALSE(x->notNull[i]);
      }
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
      EXPECT_STREQ(x->data[i], ("Test " + std::to_string(i + resultOffset)).c_str());
      if (i + resultOffset >= 100)
        EXPECT_EQ(x->length[i], 8);
      else if (i + resultOffset >= 10)
        EXPECT_EQ(x->length[i], 7);
      else
        EXPECT_EQ(x->length[i], 6);
    } else {
      EXPECT_FALSE(x->notNull[i]);
    }
  }
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
  char a[] = "\x00", b[] = "\x00\x0e", c[] = "", d[] = "\xff\x00\xbf\x5b";
  (void)(builder.Append(a, 1));
  (void)(builder.Append(b, 2));
  (void)(builder.Append(c, 0));
  (void)(builder.Append(d, 4));
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
  EXPECT_EQ(x->data[0][0], '\x00');
  EXPECT_EQ(x->data[1][0], '\x00');
  EXPECT_EQ(x->data[1][1], '\x0e');
  EXPECT_EQ(x->data[3][0], '\xff');
  EXPECT_EQ(x->data[3][1], '\x00');
  EXPECT_EQ(x->data[3][2], '\xbf');
  EXPECT_EQ(x->data[3][3], '\x5b');
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
  (void)(builder.Append(a, 0));
  (void)(builder.AppendNull());
  (void)(builder.Append(b, 2));
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
  EXPECT_EQ(x->data[3][0], '\xff');
  EXPECT_EQ(x->data[3][1], '\xfe');
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
TEST(TestAdapterWriteBinary, writeBinaryChunkedEmpty) {
  ArrayVector av;
  std::shared_ptr<ChunkedArray> carray = std::make_shared<ChunkedArray>(av, binary());
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
  int64_t arrowIndexOffset = 0;
  int arrowChunkOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowIndexOffset, arrowChunkOffset,
                                       batchSize, carray.get());
  if (!st.ok()) {
    FAIL() << "ORC ColumnBatch not successfully filled";
  }
  EXPECT_EQ(x->numElements, 0);
  EXPECT_FALSE(x->hasNulls);
  writer->add(*batch);
  writer->close();
}
TEST(TestAdapterWriteBinary, writeBinaryChunkedMixed) {
  BinaryBuilder builder0, builder1, builder2, builder3, builder4;
  char a[] = "", b[] = "\x00\xfe";
  (void)(builder1.AppendNull());
  (void)(builder1.Append(a, 0));
  (void)(builder2.AppendNull());
  (void)(builder2.Append(b, 2));
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
  int64_t arrowIndexOffset = 0;
  int arrowChunkOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowIndexOffset, arrowChunkOffset,
                                       batchSize, carray.get());
  if (!st.ok()) {
    FAIL() << "ORC ColumnBatch not successfully filled";
  }
  EXPECT_EQ(x->numElements, 5);
  EXPECT_TRUE(x->hasNulls);
  EXPECT_EQ(x->data[3][0], '\x00');
  EXPECT_EQ(x->data[3][1], '\xfe');
  EXPECT_EQ(x->length[1], 0);
  EXPECT_EQ(x->length[3], 2);
  EXPECT_EQ(x->notNull[0], 0);
  EXPECT_EQ(x->notNull[1], 1);
  EXPECT_EQ(x->notNull[2], 0);
  EXPECT_EQ(x->notNull[3], 1);
  EXPECT_EQ(x->notNull[4], 0);
  writer->add(*batch);
  writer->close();
}
TEST(TestAdapterWriteBinary, writeBinaryChunkedMultibatch) {
  uint64_t totalLength = 100;
  BinaryBuilder builder0, builder1, builder2, builder3, builder4;
  char a[2];
  for (char i = 0; i < 50; i++) {
    if (i % 2) {
      a[0] = i;
      a[1] = i;
      (void)(builder1.Append(a, 2));
    } else {
      (void)(builder1.AppendNull());
    }
  }
  for (char i = 50; i < 100; i++) {
    if (i % 2) {
      a[0] = i;
      a[1] = i;
      (void)(builder3.Append(a, 2));
    } else {
      (void)(builder3.AppendNull());
    }
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
      liborc::Type::buildTypeFromString("struct<x:binary>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 29;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::StringVectorBatch* x =
      internal::checked_cast<liborc::StringVectorBatch*>(root->fields[0]);
  DataType* arrowType = binary().get();
  int64_t arrowIndexOffset = 0;
  int arrowChunkOffset = 0;
  uint64_t resultOffset = 0;
  while (resultOffset < totalLength - batchSize) {
    Status st = adapters::orc::FillBatch(arrowType, x, arrowIndexOffset, arrowChunkOffset,
                                         batchSize, carray.get());
    if (!st.ok()) {
      FAIL() << "ORC ColumnBatch not successfully filled";
    }
    root->numElements = x->numElements;
    writer->add(*batch);
    EXPECT_EQ(x->numElements, batchSize);
    EXPECT_TRUE(x->hasNulls);
    for (uint64_t i = 0; i < batchSize; i++) {
      if ((i + resultOffset) % 2) {
        a[0] = i + resultOffset;
        a[1] = i + resultOffset;
        EXPECT_TRUE(x->notNull[i]);
        EXPECT_EQ(x->data[i][0], (char)(i + resultOffset));
        EXPECT_EQ(x->data[i][1], (char)(i + resultOffset));
        EXPECT_EQ(x->length[i], 2);
      } else {
        EXPECT_FALSE(x->notNull[i]);
      }
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
      a[0] = i + resultOffset;
      a[1] = i + resultOffset;
      EXPECT_TRUE(x->notNull[i]);
      EXPECT_EQ(x->data[i][0], (char)(i + resultOffset));
      EXPECT_EQ(x->data[i][1], (char)(i + resultOffset));
      EXPECT_EQ(x->length[i], 2);
    } else {
      EXPECT_FALSE(x->notNull[i]);
    }
  }
  root->numElements = x->numElements;
  writer->add(*batch);
  writer->close();
  std::shared_ptr<io::RandomAccessFile> in_stream(new io::BufferReader(
      std::make_shared<Buffer>(reinterpret_cast<const uint8_t*>(mem_stream.getData()),
                               static_cast<int64_t>(mem_stream.getLength()))));

  std::unique_ptr<adapters::orc::ORCFileReader> reader;
  ASSERT_TRUE(
      adapters::orc::ORCFileReader::Open(in_stream, default_memory_pool(), &reader).ok());
  std::shared_ptr<Table> outputTable;
  ARROW_EXPECT_OK(reader->Read(&outputTable));
  auto schemaOut = outputTable->schema();
  EXPECT_EQ(outputTable->num_columns(), 1);
  EXPECT_EQ(outputTable->num_rows(), 100);
  EXPECT_TRUE(schemaOut->field(0)->type()->Equals(*(carray->type())));
  EXPECT_TRUE(outputTable->column(0)->Equals(*carray));
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
  char a[] = "\x00", b[] = "\x00\x0e", c[] = "", d[] = "\xff\x00\xbf\x5b";
  (void)(builder.Append(a, 1));
  (void)(builder.Append(b, 2));
  (void)(builder.Append(c, 0));
  (void)(builder.Append(d, 4));
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
  EXPECT_EQ(x->data[0][0], '\x00');
  EXPECT_EQ(x->data[1][0], '\x00');
  EXPECT_EQ(x->data[1][1], '\x0e');
  EXPECT_EQ(x->data[3][0], '\xff');
  EXPECT_EQ(x->data[3][1], '\x00');
  EXPECT_EQ(x->data[3][2], '\xbf');
  EXPECT_EQ(x->data[3][3], '\x5b');
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
  (void)(builder.Append(a, 0));
  (void)(builder.AppendNull());
  (void)(builder.Append(b, 2));
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
  EXPECT_EQ(x->data[3][0], '\xff');
  EXPECT_EQ(x->data[3][1], '\xfe');
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
TEST(TestAdapterWriteBinary, writeLargeBinaryChunkedEmpty) {
  ArrayVector av;
  std::shared_ptr<ChunkedArray> carray =
      std::make_shared<ChunkedArray>(av, large_binary());
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
  int64_t arrowIndexOffset = 0;
  int arrowChunkOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowIndexOffset, arrowChunkOffset,
                                       batchSize, carray.get());
  if (!st.ok()) {
    FAIL() << "ORC ColumnBatch not successfully filled";
  }
  EXPECT_EQ(x->numElements, 0);
  EXPECT_FALSE(x->hasNulls);
  writer->add(*batch);
  writer->close();
}
TEST(TestAdapterWriteBinary, writeLargeBinaryChunkedMixed) {
  LargeBinaryBuilder builder0, builder1, builder2, builder3, builder4;
  char a[] = "", b[] = "\x00\xfe";
  (void)(builder1.AppendNull());
  (void)(builder1.Append(a, 0));
  (void)(builder2.AppendNull());
  (void)(builder2.Append(b, 2));
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
  int64_t arrowIndexOffset = 0;
  int arrowChunkOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowIndexOffset, arrowChunkOffset,
                                       batchSize, carray.get());
  if (!st.ok()) {
    FAIL() << "ORC ColumnBatch not successfully filled";
  }
  EXPECT_EQ(x->numElements, 5);
  EXPECT_TRUE(x->hasNulls);
  EXPECT_EQ(x->data[3][0], '\x00');
  EXPECT_EQ(x->data[3][1], '\xfe');
  EXPECT_EQ(x->length[1], 0);
  EXPECT_EQ(x->length[3], 2);
  EXPECT_EQ(x->notNull[0], 0);
  EXPECT_EQ(x->notNull[1], 1);
  EXPECT_EQ(x->notNull[2], 0);
  EXPECT_EQ(x->notNull[3], 1);
  EXPECT_EQ(x->notNull[4], 0);
  writer->add(*batch);
  writer->close();
}
TEST(TestAdapterWriteBinary, writeLargeBinaryChunkedMultibatch) {
  uint64_t totalLength = 100;
  LargeBinaryBuilder builder0, builder1, builder2, builder3, builder4;
  char a[2];
  for (char i = 0; i < 50; i++) {
    if (i % 2) {
      a[0] = i;
      a[1] = i;
      (void)(builder1.Append(a, 2));
    } else {
      (void)(builder1.AppendNull());
    }
  }
  for (char i = 50; i < 100; i++) {
    if (i % 2) {
      a[0] = i;
      a[1] = i;
      (void)(builder3.Append(a, 2));
    } else {
      (void)(builder3.AppendNull());
    }
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
      liborc::Type::buildTypeFromString("struct<x:binary>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 29;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::StringVectorBatch* x =
      internal::checked_cast<liborc::StringVectorBatch*>(root->fields[0]);
  DataType* arrowType = large_binary().get();
  int64_t arrowIndexOffset = 0;
  int arrowChunkOffset = 0;
  uint64_t resultOffset = 0;
  while (resultOffset < totalLength - batchSize) {
    Status st = adapters::orc::FillBatch(arrowType, x, arrowIndexOffset, arrowChunkOffset,
                                         batchSize, carray.get());
    if (!st.ok()) {
      FAIL() << "ORC ColumnBatch not successfully filled";
    }
    root->numElements = x->numElements;
    writer->add(*batch);
    EXPECT_EQ(x->numElements, batchSize);
    EXPECT_TRUE(x->hasNulls);
    for (uint64_t i = 0; i < batchSize; i++) {
      if ((i + resultOffset) % 2) {
        a[0] = i + resultOffset;
        a[1] = i + resultOffset;
        EXPECT_TRUE(x->notNull[i]);
        EXPECT_EQ(x->data[i][0], (char)(i + resultOffset));
        EXPECT_EQ(x->data[i][1], (char)(i + resultOffset));
        EXPECT_EQ(x->length[i], 2);
      } else {
        EXPECT_FALSE(x->notNull[i]);
      }
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
      a[0] = i + resultOffset;
      a[1] = i + resultOffset;
      EXPECT_TRUE(x->notNull[i]);
      EXPECT_EQ(x->data[i][0], (char)(i + resultOffset));
      EXPECT_EQ(x->data[i][1], (char)(i + resultOffset));
      EXPECT_EQ(x->length[i], 2);
    } else {
      EXPECT_FALSE(x->notNull[i]);
    }
  }
  root->numElements = x->numElements;
  writer->add(*batch);
  writer->close();
  std::shared_ptr<io::RandomAccessFile> in_stream(new io::BufferReader(
      std::make_shared<Buffer>(reinterpret_cast<const uint8_t*>(mem_stream.getData()),
                               static_cast<int64_t>(mem_stream.getLength()))));

  std::unique_ptr<adapters::orc::ORCFileReader> reader;
  ASSERT_TRUE(
      adapters::orc::ORCFileReader::Open(in_stream, default_memory_pool(), &reader).ok());
  std::shared_ptr<Table> outputTable;
  ARROW_EXPECT_OK(reader->Read(&outputTable));
  auto schemaOut = outputTable->schema();
  EXPECT_EQ(outputTable->num_columns(), 1);
  EXPECT_EQ(outputTable->num_rows(), 100);
  // EXPECT_TRUE(schemaOut->field(0)->type()->Equals(*(carray->type())));
  // EXPECT_TRUE(outputTable->column(0)->Equals(*carray));
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
  char a[] = "\xf4\x00\x21\x39", b[] = "\x22\x0e\x00\xaa", c[] = "\x00\x01\x43\x42",
       d[] = "\xff\x66\xbf\x00";
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
  EXPECT_EQ(x->data[0][0], '\xf4');
  EXPECT_EQ(x->data[0][1], '\x00');
  EXPECT_EQ(x->data[0][2], '\x21');
  EXPECT_EQ(x->data[0][3], '\x39');
  EXPECT_EQ(x->data[1][0], '\x22');
  EXPECT_EQ(x->data[1][1], '\x0e');
  EXPECT_EQ(x->data[1][2], '\x00');
  EXPECT_EQ(x->data[1][3], '\xaa');
  EXPECT_EQ(x->data[2][0], '\x00');
  EXPECT_EQ(x->data[2][1], '\x01');
  EXPECT_EQ(x->data[2][2], '\x43');
  EXPECT_EQ(x->data[2][3], '\x42');
  EXPECT_EQ(x->data[3][0], '\xff');
  EXPECT_EQ(x->data[3][1], '\x66');
  EXPECT_EQ(x->data[3][2], '\xbf');
  EXPECT_EQ(x->data[3][3], '\x00');
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
  char a[] = "\x00\xff", b[] = "\xff\x00";
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
  EXPECT_EQ(x->data[1][0], '\x00');
  EXPECT_EQ(x->data[1][1], '\xff');
  EXPECT_EQ(x->data[3][0], '\xff');
  EXPECT_EQ(x->data[3][1], '\x00');
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
TEST(TestAdapterWriteBinary, writeFixedSizeBinaryChunkedEmpty) {
  ArrayVector av;
  std::shared_ptr<ChunkedArray> carray =
      std::make_shared<ChunkedArray>(av, fixed_size_binary(2));
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
  int64_t arrowIndexOffset = 0;
  int arrowChunkOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowIndexOffset, arrowChunkOffset,
                                       batchSize, carray.get());
  if (!st.ok()) {
    FAIL() << "ORC ColumnBatch not successfully filled";
  }
  EXPECT_EQ(x->numElements, 0);
  EXPECT_FALSE(x->hasNulls);
  writer->add(*batch);
  writer->close();
}
TEST(TestAdapterWriteBinary, writeFixedSizeBinaryZeroChunkedEmpty) {
  ArrayVector av;
  std::shared_ptr<ChunkedArray> carray =
      std::make_shared<ChunkedArray>(av, fixed_size_binary(0));
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
  int64_t arrowIndexOffset = 0;
  int arrowChunkOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowIndexOffset, arrowChunkOffset,
                                       batchSize, carray.get());
  if (!st.ok()) {
    FAIL() << "ORC ColumnBatch not successfully filled";
  }
  EXPECT_EQ(x->numElements, 0);
  EXPECT_FALSE(x->hasNulls);
  writer->add(*batch);
  writer->close();
}
TEST(TestAdapterWriteBinary, writeFixedSizeBinaryChunkedMixed) {
  FixedSizeBinaryBuilder builder0(fixed_size_binary(2)), builder1(fixed_size_binary(2)),
      builder2(fixed_size_binary(2)), builder3(fixed_size_binary(2)),
      builder4(fixed_size_binary(2));
  char a[] = "\x00\x00", b[] = "\x00\xfe";
  (void)(builder1.AppendNull());
  (void)(builder1.Append(a));
  (void)(builder2.AppendNull());
  (void)(builder2.Append(b));
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
  int64_t arrowIndexOffset = 0;
  int arrowChunkOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowIndexOffset, arrowChunkOffset,
                                       batchSize, carray.get());
  if (!st.ok()) {
    FAIL() << "ORC ColumnBatch not successfully filled";
  }
  EXPECT_EQ(x->numElements, 5);
  EXPECT_TRUE(x->hasNulls);
  EXPECT_EQ(x->data[1][0], '\x00');
  EXPECT_EQ(x->data[1][1], '\x00');
  EXPECT_EQ(x->data[3][0], '\x00');
  EXPECT_EQ(x->data[3][1], '\xfe');
  EXPECT_EQ(x->length[1], 2);
  EXPECT_EQ(x->length[3], 2);
  EXPECT_EQ(x->notNull[0], 0);
  EXPECT_EQ(x->notNull[1], 1);
  EXPECT_EQ(x->notNull[2], 0);
  EXPECT_EQ(x->notNull[3], 1);
  EXPECT_EQ(x->notNull[4], 0);
  writer->add(*batch);
  writer->close();
}
TEST(TestAdapterWriteBinary, writeFixedSizeBinaryZeroChunkedMixed) {
  FixedSizeBinaryBuilder builder0(fixed_size_binary(0)), builder1(fixed_size_binary(0)),
      builder2(fixed_size_binary(0)), builder3(fixed_size_binary(0)),
      builder4(fixed_size_binary(0));
  char a[] = "", b[] = "";
  (void)(builder1.AppendNull());
  (void)(builder1.Append(a));
  (void)(builder2.AppendNull());
  (void)(builder2.Append(b));
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
  int64_t arrowIndexOffset = 0;
  int arrowChunkOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowIndexOffset, arrowChunkOffset,
                                       batchSize, carray.get());
  if (!st.ok()) {
    FAIL() << "ORC ColumnBatch not successfully filled";
  }
  EXPECT_EQ(x->numElements, 5);
  EXPECT_TRUE(x->hasNulls);
  EXPECT_EQ(x->length[1], 0);
  EXPECT_EQ(x->length[3], 0);
  EXPECT_EQ(x->notNull[0], 0);
  EXPECT_EQ(x->notNull[1], 1);
  EXPECT_EQ(x->notNull[2], 0);
  EXPECT_EQ(x->notNull[3], 1);
  EXPECT_EQ(x->notNull[4], 0);
  writer->add(*batch);
  writer->close();
}
TEST(TestAdapterWriteBinary, writeFixedSizeBinaryChunkedMultibatch) {
  uint64_t totalLength = 100;
  FixedSizeBinaryBuilder builder0(fixed_size_binary(2)), builder1(fixed_size_binary(2)),
      builder2(fixed_size_binary(2)), builder3(fixed_size_binary(2)),
      builder4(fixed_size_binary(2));
  char a[2];
  for (char i = 0; i < 50; i++) {
    if (i % 2) {
      a[0] = i;
      a[1] = i;
      (void)(builder1.Append(a));
    } else {
      (void)(builder1.AppendNull());
    }
  }
  for (char i = 50; i < 100; i++) {
    if (i % 2) {
      a[0] = i;
      a[1] = i;
      (void)(builder3.Append(a));
    } else {
      (void)(builder3.AppendNull());
    }
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
      liborc::Type::buildTypeFromString("struct<x:binary>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 100;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::StringVectorBatch* x =
      internal::checked_cast<liborc::StringVectorBatch*>(root->fields[0]);
  DataType* arrowType = fixed_size_binary(2).get();
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
        a[0] = i + resultOffset;
        a[1] = i + resultOffset;
        EXPECT_TRUE(x->notNull[i]);
        EXPECT_EQ(x->data[i][0], (char)(i + resultOffset));
        EXPECT_EQ(x->data[i][1], (char)(i + resultOffset));
        EXPECT_EQ(x->length[i], 2);
      } else {
        EXPECT_FALSE(x->notNull[i]);
      }
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
      a[0] = i + resultOffset;
      a[1] = i + resultOffset;
      EXPECT_TRUE(x->notNull[i]);
      EXPECT_EQ(x->data[i][0], (char)(i + resultOffset));
      EXPECT_EQ(x->data[i][1], (char)(i + resultOffset));
      EXPECT_EQ(x->length[i], 2);
    } else {
      EXPECT_FALSE(x->notNull[i]);
    }
  }
  writer->add(*batch);
  writer->close();
}
TEST(TestAdapterWriteBinary, writeFixedSizeBinaryZeroChunkedMultibatch) {
  uint64_t totalLength = 100;
  FixedSizeBinaryBuilder builder0(fixed_size_binary(0)), builder1(fixed_size_binary(0)),
      builder2(fixed_size_binary(0)), builder3(fixed_size_binary(0)),
      builder4(fixed_size_binary(0));
  for (char i = 0; i < 50; i++) {
    if (i % 2) {
      (void)(builder1.Append(""));
    } else {
      (void)(builder1.AppendNull());
    }
  }
  for (char i = 50; i < 100; i++) {
    if (i % 2) {
      (void)(builder3.Append(""));
    } else {
      (void)(builder3.AppendNull());
    }
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
      liborc::Type::buildTypeFromString("struct<x:binary>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::StringVectorBatch* x =
      internal::checked_cast<liborc::StringVectorBatch*>(root->fields[0]);
  DataType* arrowType = fixed_size_binary(0).get();
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
        EXPECT_EQ(x->length[i], 0);
      } else {
        EXPECT_FALSE(x->notNull[i]);
      }
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
      EXPECT_EQ(x->length[i], 0);
    } else {
      EXPECT_FALSE(x->notNull[i]);
    }
  }
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
TEST(TestAdapterWriteNested, writeStructMixedParentNoNulls) {
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
TEST(TestAdapterWriteNested, writeStructMixedParentHasNulls) {
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
TEST(TestAdapterWriteNested, writeStructAllNullsParentHasNulls) {
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
TEST(TestAdapterWriteNested, writeStructChunkedEmpty) {
  std::vector<std::shared_ptr<Field>> xFields;
  xFields.push_back(std::make_shared<Field>("a", utf8()));
  xFields.push_back(std::make_shared<Field>("b", int32()));
  auto sharedPtrArrowType = struct_(xFields);
  ArrayVector av;
  std::shared_ptr<ChunkedArray> carray =
      std::make_shared<ChunkedArray>(av, sharedPtrArrowType);
  DataType* arrowType = sharedPtrArrowType.get();

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
  int64_t arrowIndexOffset = 0;
  int arrowChunkOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowIndexOffset, arrowChunkOffset,
                                       batchSize, carray.get());
  if (!st.ok()) {
    FAIL() << "ORC ColumnBatch not successfully filled";
  }
  EXPECT_EQ(x->numElements, 0);
  EXPECT_FALSE(x->hasNulls);
  EXPECT_EQ(a->numElements, 0);
  EXPECT_FALSE(a->hasNulls);
  EXPECT_EQ(b->numElements, 0);
  EXPECT_FALSE(b->hasNulls);

  writer->add(*batch);
  writer->close();
}
TEST(TestAdapterWriteNested, writeStructChunkedMixed) {
  std::vector<std::shared_ptr<Field>> xFields;
  xFields.push_back(std::make_shared<Field>("a", utf8()));
  xFields.push_back(std::make_shared<Field>("b", int32()));
  auto sharedPtrArrowType = struct_(xFields);
  DataType* arrowType = sharedPtrArrowType.get();

  StringBuilder builder10, builder11, builder12, builder13, builder14;
  (void)(builder11.Append("A"));
  (void)(builder11.Append("AB"));
  (void)(builder13.AppendNull());
  (void)(builder13.AppendNull());

  Int32Builder builder20, builder21, builder22, builder23, builder24;
  (void)(builder21.Append(3));
  (void)(builder21.AppendNull());
  (void)(builder23.Append(25));
  (void)(builder23.AppendNull());

  std::shared_ptr<Array> array10, array11, array12, array13, array14;
  std::shared_ptr<Array> array20, array21, array22, array23, array24;

  (void)(builder10.Finish(&array10));
  (void)(builder11.Finish(&array11));
  (void)(builder12.Finish(&array12));
  (void)(builder13.Finish(&array13));
  (void)(builder14.Finish(&array14));
  (void)(builder20.Finish(&array20));
  (void)(builder21.Finish(&array21));
  (void)(builder22.Finish(&array22));
  (void)(builder23.Finish(&array23));
  (void)(builder24.Finish(&array24));

  std::vector<std::shared_ptr<Array>> children0, children1, children2, children3,
      children4;
  children0.push_back(array10);
  children0.push_back(array20);
  children1.push_back(array11);
  children1.push_back(array21);
  children2.push_back(array12);
  children2.push_back(array22);
  children3.push_back(array13);
  children3.push_back(array23);
  children4.push_back(array14);
  children4.push_back(array24);

  auto array0 = std::make_shared<StructArray>(sharedPtrArrowType, 0, children0);
  auto array2 = std::make_shared<StructArray>(sharedPtrArrowType, 0, children2);
  auto array4 = std::make_shared<StructArray>(sharedPtrArrowType, 0, children4);

  uint8_t bitmap1 = 2;  // 00000010
  auto maybeBuffer1 = AllocateBuffer(1);
  if (!maybeBuffer1.ok()) {
    FAIL() << "Buffer not created successfully";
  }
  std::shared_ptr<Buffer> buffer1 = *std::move(maybeBuffer1);
  uint8_t* bufferData1 = buffer1->mutable_data();
  std::memcpy(bufferData1, &bitmap1, 1);

  auto array1 = std::make_shared<StructArray>(sharedPtrArrowType, 2, children1, buffer1);

  uint8_t bitmap3 = 1;  // 00000001
  auto maybeBuffer3 = AllocateBuffer(1);
  if (!maybeBuffer3.ok()) {
    FAIL() << "Buffer not created successfully";
  }
  std::shared_ptr<Buffer> buffer3 = *std::move(maybeBuffer3);
  uint8_t* bufferData3 = buffer3->mutable_data();
  std::memcpy(bufferData3, &bitmap3, 1);

  auto array3 = std::make_shared<StructArray>(sharedPtrArrowType, 2, children3, buffer3);

  ArrayVector av;
  av.push_back(array0);
  av.push_back(array1);
  av.push_back(array2);
  av.push_back(array3);
  av.push_back(array4);
  std::shared_ptr<ChunkedArray> carray = std::make_shared<ChunkedArray>(av);

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
  int64_t arrowIndexOffset = 0;
  int arrowChunkOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowIndexOffset, arrowChunkOffset,
                                       batchSize, carray.get());
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
  EXPECT_EQ(x->notNull[3], 0);

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

  writer->add(*batch);
  writer->close();
}
TEST(TestAdapterWriteNested, writeStructChunkedMultibatchParentNoNulls) {
  std::vector<std::shared_ptr<Field>> xFields;
  xFields.push_back(std::make_shared<Field>("a", utf8()));
  xFields.push_back(std::make_shared<Field>("b", int32()));
  auto sharedPtrArrowType = struct_(xFields);
  DataType* arrowType = sharedPtrArrowType.get();

  uint64_t totalLength = 100;
  StringBuilder builder00, builder01, builder02, builder03, builder04;
  Int32Builder builder10, builder11, builder12, builder13, builder14;
  for (int i = 0; i < 50; i++) {
    if (i % 2) {
      (void)(builder01.Append("Test " + std::to_string(i)));
      (void)(builder11.AppendNull());
    } else {
      (void)(builder01.AppendNull());
      (void)(builder11.Append(i));
    }
  }
  for (int i = 50; i < 100; i++) {
    if (i % 2) {
      (void)(builder03.Append("Test " + std::to_string(i)));
      (void)(builder13.AppendNull());
    } else {
      (void)(builder03.AppendNull());
      (void)(builder13.Append(i));
    }
  }

  std::shared_ptr<Array> array00, array01, array02, array03, array04;
  std::shared_ptr<Array> array10, array11, array12, array13, array14;

  (void)(builder10.Finish(&array10));
  (void)(builder11.Finish(&array11));
  (void)(builder12.Finish(&array12));
  (void)(builder13.Finish(&array13));
  (void)(builder14.Finish(&array14));
  (void)(builder00.Finish(&array00));
  (void)(builder01.Finish(&array01));
  (void)(builder02.Finish(&array02));
  (void)(builder03.Finish(&array03));
  (void)(builder04.Finish(&array04));

  std::vector<std::shared_ptr<Array>> children0, children1, children2, children3,
      children4;
  children0.push_back(array00);
  children0.push_back(array10);
  children1.push_back(array01);
  children1.push_back(array11);
  children2.push_back(array02);
  children2.push_back(array12);
  children3.push_back(array03);
  children3.push_back(array13);
  children4.push_back(array04);
  children4.push_back(array14);

  auto array0 = std::make_shared<StructArray>(sharedPtrArrowType, 0, children0);
  auto array1 = std::make_shared<StructArray>(sharedPtrArrowType, 50, children1);
  auto array2 = std::make_shared<StructArray>(sharedPtrArrowType, 0, children2);
  auto array3 = std::make_shared<StructArray>(sharedPtrArrowType, 50, children3);
  auto array4 = std::make_shared<StructArray>(sharedPtrArrowType, 0, children4);
  ArrayVector av;
  av.push_back(array0);
  av.push_back(array1);
  av.push_back(array2);
  av.push_back(array3);
  av.push_back(array4);
  std::shared_ptr<ChunkedArray> carray = std::make_shared<ChunkedArray>(av);

  // RecordProperty("l", carray->length());
  // RecordProperty("l0", carray->chunk(0)->length());
  // RecordProperty("l1", carray->chunk(1)->length());
  // RecordProperty("l2", carray->chunk(2)->length());
  // RecordProperty("l3", carray->chunk(3)->length());
  // RecordProperty("l4", carray->chunk(4)->length());

  // auto a1 = std::static_pointer_cast<StructArray>(carray->chunk(1));
  // auto a10 = std::static_pointer_cast<StringArray>(a1->field(0));
  // auto a11 = std::static_pointer_cast<Int32Array>(a1->field(1));
  // RecordProperty("l10", a10->length());
  // RecordProperty("l11", a11->length());
  // auto a3 = std::static_pointer_cast<StructArray>(carray->chunk(3));
  // auto a30 = std::static_pointer_cast<StringArray>(a3->field(0));
  // auto a31 = std::static_pointer_cast<Int32Array>(a3->field(1));
  // RecordProperty("l30", a30->length());
  // RecordProperty("l31", a31->length());

  // for (int i = 0; i < 50; i++) {
  //   if (i % 2) {
  //     RecordProperty("s" + std::to_string(i), a10->GetString(i));
  //     RecordProperty("an" + std::to_string(i), a10->IsNull(i));
  //     RecordProperty("bn" + std::to_string(i), a11->IsNull(i));
  //   } else {
  //     RecordProperty("i" + std::to_string(i), a11->Value(i));
  //     RecordProperty("an" + std::to_string(i), a10->IsNull(i));
  //     RecordProperty("bn" + std::to_string(i), a11->IsNull(i));
  //   }
  // }
  // for (int i = 0; i < 50; i++) {
  //   if (i % 2) {
  //     RecordProperty("s" + std::to_string(50 + i), a30->GetString(i));
  //     RecordProperty("an" + std::to_string(50 + i), a30->IsNull(i));
  //     RecordProperty("bn" + std::to_string(50 + i), a31->IsNull(i));
  //   } else {
  //     RecordProperty("i" + std::to_string(50 + i), a31->Value(i));
  //     RecordProperty("an" + std::to_string(50 + i), a30->IsNull(i));
  //     RecordProperty("bn" + std::to_string(50 + i), a31->IsNull(i));
  //   }
  // }

  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:struct<a:string,b:int>>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 25;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::StructVectorBatch* x =
      internal::checked_cast<liborc::StructVectorBatch*>(root->fields[0]);
  liborc::StringVectorBatch* a =
      internal::checked_cast<liborc::StringVectorBatch*>(x->fields[0]);
  liborc::LongVectorBatch* b =
      internal::checked_cast<liborc::LongVectorBatch*>(x->fields[1]);
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
    EXPECT_EQ(a->numElements, batchSize);
    EXPECT_EQ(b->numElements, batchSize);
    EXPECT_FALSE(x->hasNulls);
    EXPECT_TRUE(a->hasNulls);
    EXPECT_TRUE(b->hasNulls);
    for (uint64_t i = 0; i < batchSize; i++) {
      if ((i + resultOffset) % 2) {
        EXPECT_TRUE(a->notNull[i]);
        EXPECT_FALSE(b->notNull[i]);
        EXPECT_EQ(a->data[i], "Test " + std::to_string(i + resultOffset));
        if ((i + resultOffset) >= 10)
          EXPECT_EQ(a->length[i], 7);
        else
          EXPECT_EQ(a->length[i], 6);
      } else {
        EXPECT_FALSE(a->notNull[i]);
        EXPECT_TRUE(b->notNull[i]);
        EXPECT_EQ(b->data[i], i + resultOffset);
      }
    }
    resultOffset = resultOffset + batchSize;
    batch->clear();
  }
  Status st = adapters::orc::FillBatch(arrowType, x, arrowIndexOffset, arrowChunkOffset,
                                       batchSize, carray.get());
  uint64_t lastBatchSize = totalLength - resultOffset;
  EXPECT_EQ(x->numElements, lastBatchSize);
  EXPECT_EQ(a->numElements, lastBatchSize);
  EXPECT_EQ(b->numElements, lastBatchSize);
  EXPECT_FALSE(x->hasNulls);
  EXPECT_TRUE(a->hasNulls);
  EXPECT_TRUE(b->hasNulls);
  for (uint64_t i = 0; i < lastBatchSize; i++) {
    if ((i + resultOffset) % 2) {
      EXPECT_TRUE(a->notNull[i]);
      EXPECT_FALSE(b->notNull[i]);
      EXPECT_EQ(a->data[i], "Test " + std::to_string(i + resultOffset));
      if ((i + resultOffset) >= 10)
        EXPECT_EQ(a->length[i], 7);
      else
        EXPECT_EQ(a->length[i], 6);
    } else {
      EXPECT_FALSE(a->notNull[i]);
      EXPECT_TRUE(b->notNull[i]);
      EXPECT_EQ(b->data[i], i + resultOffset);
    }
  }
  writer->add(*batch);
  writer->close();
}
TEST(TestAdapterWriteNested, writeStructChunkedMultibatchParentHasNulls) {
  std::vector<std::shared_ptr<Field>> xFields;
  xFields.push_back(std::make_shared<Field>("a", utf8()));
  xFields.push_back(std::make_shared<Field>("b", int32()));
  auto sharedPtrArrowType = struct_(xFields);
  DataType* arrowType = sharedPtrArrowType.get();

  uint64_t totalLength = 100;
  StringBuilder builder00, builder01, builder02, builder03, builder04;
  Int32Builder builder10, builder11, builder12, builder13, builder14;
  for (int i = 0; i < 50; i++) {
    if (i % 2) {
      (void)(builder01.Append("Test " + std::to_string(i)));
      (void)(builder11.AppendNull());
    } else {
      (void)(builder01.AppendNull());
      (void)(builder11.Append(i));
    }
  }
  for (int i = 50; i < 100; i++) {
    if (i % 2) {
      (void)(builder03.Append("Test " + std::to_string(i)));
      (void)(builder13.AppendNull());
    } else {
      (void)(builder03.AppendNull());
      (void)(builder13.Append(i));
    }
  }

  // Every third entry has null at struct level
  uint8_t bitmap1[7] = {
      182, 109, 219, 182,
      109, 219, 2};  // 10110110 01101101 11011011 10110110 01101101 11011011 00000010
  uint8_t bitmap3[7] = {
      109, 219, 182, 109,
      219, 182, 1};  // 01101101 11011011 10110110 01101101 11011011 10110110 00000001

  BufferBuilder builder1, builder3;
  (void)(builder1.Resize(7));
  (void)(builder1.Append(bitmap1, 7));
  std::shared_ptr<arrow::Buffer> bitmapBuffer1;
  if (!builder1.Finish(&bitmapBuffer1).ok()) {
    FAIL() << "The offsets buffer can not be constructed!";
  }
  (void)(builder3.Resize(7));
  (void)(builder3.Append(bitmap3, 7));
  std::shared_ptr<arrow::Buffer> bitmapBuffer3;
  if (!builder3.Finish(&bitmapBuffer3).ok()) {
    FAIL() << "The offsets buffer can not be constructed!";
  }

  std::shared_ptr<Array> array00, array01, array02, array03, array04;
  std::shared_ptr<Array> array10, array11, array12, array13, array14;

  (void)(builder10.Finish(&array10));
  (void)(builder11.Finish(&array11));
  (void)(builder12.Finish(&array12));
  (void)(builder13.Finish(&array13));
  (void)(builder14.Finish(&array14));
  (void)(builder00.Finish(&array00));
  (void)(builder01.Finish(&array01));
  (void)(builder02.Finish(&array02));
  (void)(builder03.Finish(&array03));
  (void)(builder04.Finish(&array04));

  std::vector<std::shared_ptr<Array>> children0, children1, children2, children3,
      children4;
  children0.push_back(array00);
  children0.push_back(array10);
  children1.push_back(array01);
  children1.push_back(array11);
  children2.push_back(array02);
  children2.push_back(array12);
  children3.push_back(array03);
  children3.push_back(array13);
  children4.push_back(array04);
  children4.push_back(array14);

  auto array0 = std::make_shared<StructArray>(sharedPtrArrowType, 0, children0);
  auto array1 =
      std::make_shared<StructArray>(sharedPtrArrowType, 50, children1, bitmapBuffer1);
  auto array2 = std::make_shared<StructArray>(sharedPtrArrowType, 0, children2);
  auto array3 =
      std::make_shared<StructArray>(sharedPtrArrowType, 50, children3, bitmapBuffer3);
  auto array4 = std::make_shared<StructArray>(sharedPtrArrowType, 0, children4);
  ArrayVector av;
  av.push_back(array0);
  av.push_back(array1);
  av.push_back(array2);
  av.push_back(array3);
  av.push_back(array4);
  std::shared_ptr<ChunkedArray> carray = std::make_shared<ChunkedArray>(av);

  // RecordProperty("l", carray->length());
  // RecordProperty("l0", carray->chunk(0)->length());
  // RecordProperty("l1", carray->chunk(1)->length());
  // RecordProperty("l2", carray->chunk(2)->length());
  // RecordProperty("l3", carray->chunk(3)->length());
  // RecordProperty("l4", carray->chunk(4)->length());

  // auto a1 = std::static_pointer_cast<StructArray>(carray->chunk(1));
  // auto a10 = std::static_pointer_cast<StringArray>(a1->field(0));
  // auto a11 = std::static_pointer_cast<Int32Array>(a1->field(1));
  // RecordProperty("l10", a10->length());
  // RecordProperty("l11", a11->length());
  // auto a3 = std::static_pointer_cast<StructArray>(carray->chunk(3));
  // auto a30 = std::static_pointer_cast<StringArray>(a3->field(0));
  // auto a31 = std::static_pointer_cast<Int32Array>(a3->field(1));
  // RecordProperty("l30", a30->length());
  // RecordProperty("l31", a31->length());

  // for (int i = 0; i < 50; i++) {
  //   RecordProperty("xn" + std::to_string(i), a1->IsNull(i));
  //   if (i % 2) {
  //     RecordProperty("s" + std::to_string(i), a10->GetString(i));
  //     RecordProperty("an" + std::to_string(i), a10->IsNull(i));
  //     RecordProperty("bn" + std::to_string(i), a11->IsNull(i));
  //   } else {
  //     RecordProperty("i" + std::to_string(i), a11->Value(i));
  //     RecordProperty("an" + std::to_string(i), a10->IsNull(i));
  //     RecordProperty("bn" + std::to_string(i), a11->IsNull(i));
  //   }
  // }
  // for (int i = 0; i < 50; i++) {
  //   RecordProperty("xn" + std::to_string(50 + i), a3->IsNull(i));
  //   if (i % 2) {
  //     RecordProperty("s" + std::to_string(50 + i), a30->GetString(i));
  //     RecordProperty("an" + std::to_string(50 + i), a30->IsNull(i));
  //     RecordProperty("bn" + std::to_string(50 + i), a31->IsNull(i));
  //   } else {
  //     RecordProperty("i" + std::to_string(50 + i), a31->Value(i));
  //     RecordProperty("an" + std::to_string(50 + i), a30->IsNull(i));
  //     RecordProperty("bn" + std::to_string(50 + i), a31->IsNull(i));
  //   }
  // }

  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:struct<a:string,b:int>>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 29;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::StructVectorBatch* x =
      internal::checked_cast<liborc::StructVectorBatch*>(root->fields[0]);
  liborc::StringVectorBatch* a =
      internal::checked_cast<liborc::StringVectorBatch*>(x->fields[0]);
  liborc::LongVectorBatch* b =
      internal::checked_cast<liborc::LongVectorBatch*>(x->fields[1]);
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
    EXPECT_EQ(a->numElements, batchSize);
    EXPECT_EQ(b->numElements, batchSize);
    EXPECT_TRUE(x->hasNulls);
    EXPECT_TRUE(a->hasNulls);
    EXPECT_TRUE(b->hasNulls);
    for (uint64_t i = 0; i < batchSize; i++) {
      if ((i + resultOffset) % 2) {
        EXPECT_TRUE(a->notNull[i]);
        EXPECT_FALSE(b->notNull[i]);
        EXPECT_EQ(a->data[i], "Test " + std::to_string(i + resultOffset));
        if ((i + resultOffset) >= 10)
          EXPECT_EQ(a->length[i], 7);
        else
          EXPECT_EQ(a->length[i], 6);
      } else {
        EXPECT_FALSE(a->notNull[i]);
        EXPECT_TRUE(b->notNull[i]);
        EXPECT_EQ(b->data[i], i + resultOffset);
      }
      if ((i + resultOffset) % 3) {
        EXPECT_TRUE(x->notNull[i]);
      } else {
        EXPECT_FALSE(x->notNull[i]);
      }
    }
    resultOffset = resultOffset + batchSize;
    batch->clear();
  }
  Status st = adapters::orc::FillBatch(arrowType, x, arrowIndexOffset, arrowChunkOffset,
                                       batchSize, carray.get());
  uint64_t lastBatchSize = totalLength - resultOffset;
  EXPECT_EQ(x->numElements, lastBatchSize);
  EXPECT_EQ(a->numElements, lastBatchSize);
  EXPECT_EQ(b->numElements, lastBatchSize);
  EXPECT_TRUE(x->hasNulls);
  EXPECT_TRUE(a->hasNulls);
  EXPECT_TRUE(b->hasNulls);
  for (uint64_t i = 0; i < lastBatchSize; i++) {
    if ((i + resultOffset) % 2) {
      EXPECT_TRUE(a->notNull[i]);
      EXPECT_FALSE(b->notNull[i]);
      EXPECT_EQ(a->data[i], "Test " + std::to_string(i + resultOffset));
      if ((i + resultOffset) >= 10)
        EXPECT_EQ(a->length[i], 7);
      else
        EXPECT_EQ(a->length[i], 6);
    } else {
      EXPECT_FALSE(a->notNull[i]);
      EXPECT_TRUE(b->notNull[i]);
      EXPECT_EQ(b->data[i], i + resultOffset);
    }
    if ((i + resultOffset) % 3) {
      EXPECT_TRUE(x->notNull[i]);
    } else {
      EXPECT_FALSE(x->notNull[i]);
    }
  }
  writer->add(*batch);
  writer->close();
}

// List
TEST(TestAdapterWriteNested, writeListEmpty) {
  auto sharedPtrArrowType = list(std::make_shared<Field>("a", int32()));
  DataType* arrowType = sharedPtrArrowType.get();

  Int32Builder valuesBuilder, offsetsBuilder;
  std::shared_ptr<Array> valuesArray, offsetsArray;
  (void)(valuesBuilder.Finish(&valuesArray));
  (void)(offsetsBuilder.Append(0));
  (void)(offsetsBuilder.Finish(&offsetsArray));

  std::shared_ptr<ListArray> array =
      ListArray::FromArrays(*offsetsArray, *valuesArray).ValueOrDie();

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

  EXPECT_EQ(arrowOffset, 0);
  EXPECT_EQ(orcOffset, 0);
  writer->add(*batch);
  writer->close();
}
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

  std::shared_ptr<ListArray> array =
      ListArray::FromArrays(*offsetsArray, *valuesArray).ValueOrDie();

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
TEST(TestAdapterWriteNested, writeListNoNulls2) {
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

  std::shared_ptr<ListArray> array =
      ListArray::FromArrays(*offsetsArray, *valuesArray).ValueOrDie();

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
  int64_t arrowOffset = 2;
  int64_t orcOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize,
                                       array.get());
  if (!st.ok()) {
    FAIL() << "ORC ColumnBatch not successfully filled";
  }

  EXPECT_EQ(x->numElements, 2);
  EXPECT_FALSE(x->hasNulls);
  EXPECT_EQ(a->numElements, 7);
  EXPECT_FALSE(a->hasNulls);

  EXPECT_EQ(x->offsets[0], 0);
  EXPECT_EQ(x->offsets[1], 3);
  EXPECT_EQ(x->offsets[2], 7);

  EXPECT_EQ(a->data[0], 4);
  EXPECT_EQ(a->data[1], 5);
  EXPECT_EQ(a->data[2], 6);
  EXPECT_EQ(a->data[3], 7);
  EXPECT_EQ(a->data[4], 8);
  EXPECT_EQ(a->data[5], 9);
  EXPECT_EQ(a->data[6], 10);

  // EXPECT_EQ(arrowOffset, 4);
  // EXPECT_EQ(orcOffset, 4);
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

  std::shared_ptr<ListArray> array =
      ListArray::FromArrays(*offsetsArray, *valuesArray).ValueOrDie();

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
TEST(TestAdapterWriteNested, writeListMixed4) {
  auto sharedPtrArrowType = list(std::make_shared<Field>("a", int32()));
  DataType* arrowType = sharedPtrArrowType.get();
  Int32Builder valuesBuilder;
  int32_t offset[101];

  offset[0] = 0;
  for (int i = 0; i < 100; i++) {
    switch (i % 4) {
      case 0: {
        offset[i + 1] = offset[i];
        break;
      }
      case 1: {
        (void)(valuesBuilder.Append(i - 1));
        offset[i + 1] = offset[i] + 1;
        break;
      }
      case 2: {
        (void)(valuesBuilder.AppendNull());
        (void)(valuesBuilder.AppendNull());
        offset[i + 1] = offset[i] + 2;
        break;
      }
      default: {
        (void)(valuesBuilder.Append(i - 1));
        (void)(valuesBuilder.AppendNull());
        (void)(valuesBuilder.AppendNull());
        offset[i + 1] = offset[i] + 3;
      }
    }
  }
  // Every third entry has null at struct level
  uint8_t bitmap[13] = {182, 109, 219, 182, 109, 219, 182, 109, 219, 182, 109, 219, 6};
  // 10110110 01101101 11011011 10110110 01101101 11011011
  // 10110110 01101101 11011011 10110110 01101101 11011011
  // 00000110
  int64_t batchSize = 100;

  BufferBuilder builder, offsetsBuilder;
  (void)(builder.Resize(13));
  (void)(builder.Append(bitmap, 13));
  std::shared_ptr<arrow::Buffer> bitmapBuffer;
  if (!builder.Finish(&bitmapBuffer).ok()) {
    FAIL() << "The offsets buffer can not be constructed!";
  }

  (void)(offsetsBuilder.Resize(404));
  (void)(offsetsBuilder.Append(offset, 404));
  std::shared_ptr<arrow::Buffer> offsetsBuffer;
  if (!offsetsBuilder.Finish(&offsetsBuffer).ok()) {
    FAIL() << "The offsets buffer can not be constructed!";
  }

  std::shared_ptr<Array> valuesArray;
  (void)(valuesBuilder.Finish(&valuesArray));

  auto array = std::make_shared<ListArray>(sharedPtrArrowType, 100, offsetsBuffer,
                                           valuesArray, bitmapBuffer);

  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE * 5);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:array<a:int>>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
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

  writer->add(*batch);
  for (uint64_t i = 0; i < 100; i++) {
    if (i % 3) {
      EXPECT_TRUE(x->notNull[i]);
    } else {
      EXPECT_FALSE(x->notNull[i]);
    }
    int64_t offset_ = testListOffsetGenerator(i);
    EXPECT_EQ(x->offsets[i], offset_);
  }
  EXPECT_EQ(x->offsets[100], 150);
  for (uint64_t j = 0; j < 150; j++) {
    if (j % 3) {
      EXPECT_FALSE(a->notNull[j]);
    } else {
      EXPECT_TRUE(a->notNull[j]);
      EXPECT_EQ(a->data[j], j * 2 / 3);
    }
  }
  EXPECT_EQ(x->numElements, batchSize);
  EXPECT_EQ(a->numElements, 150);
  EXPECT_TRUE(x->hasNulls);
  EXPECT_TRUE(a->hasNulls);
  batch->clear();
  writer->close();
}
TEST(TestAdapterWriteNested, writeListChunkedEmpty) {
  auto sharedPtrArrowType = list(std::make_shared<Field>("a", int32()));
  ArrayVector av;
  std::shared_ptr<ChunkedArray> carray =
      std::make_shared<ChunkedArray>(av, sharedPtrArrowType);
  DataType* arrowType = sharedPtrArrowType.get();

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
  int64_t arrowIndexOffset = 0;
  int arrowChunkOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowIndexOffset, arrowChunkOffset,
                                       batchSize, carray.get());
  if (!st.ok()) {
    FAIL() << "ORC ColumnBatch not successfully filled";
  }
  EXPECT_EQ(x->numElements, 0);
  EXPECT_FALSE(x->hasNulls);
  EXPECT_EQ(a->numElements, 0);
  EXPECT_FALSE(a->hasNulls);

  writer->add(*batch);
  writer->close();
}
TEST(TestAdapterWriteNested, writeListChunkedMixed) {
  auto sharedPtrArrowType = list(std::make_shared<Field>("a", int32()));
  DataType* arrowType = sharedPtrArrowType.get();

  Int32Builder valuesBuilder0, offsetsBuilder0, valuesBuilder2, offsetsBuilder2,
      valuesBuilder4, offsetsBuilder4;
  std::shared_ptr<Array> valuesArray0, offsetsArray0, valuesArray2, offsetsArray2,
      valuesArray4, offsetsArray4;
  (void)(valuesBuilder0.Finish(&valuesArray0));
  (void)(offsetsBuilder0.Append(0));
  (void)(offsetsBuilder0.Finish(&offsetsArray0));
  (void)(valuesBuilder2.Finish(&valuesArray2));
  (void)(offsetsBuilder2.Append(0));
  (void)(offsetsBuilder2.Finish(&offsetsArray2));
  (void)(valuesBuilder4.Finish(&valuesArray4));
  (void)(offsetsBuilder4.Append(0));
  (void)(offsetsBuilder4.Finish(&offsetsArray4));

  std::shared_ptr<ListArray> array0 =
      ListArray::FromArrays(*offsetsArray0, *valuesArray0).ValueOrDie();
  std::shared_ptr<ListArray> array2 =
      ListArray::FromArrays(*offsetsArray2, *valuesArray2).ValueOrDie();
  std::shared_ptr<ListArray> array4 =
      ListArray::FromArrays(*offsetsArray4, *valuesArray4).ValueOrDie();

  Int32Builder valuesBuilder1, offsetsBuilder1, valuesBuilder3, offsetsBuilder3;

  int32_t offsets1[3] = {0, 1, 3};

  BufferBuilder builder1;
  (void)(builder1.Resize(12));
  (void)(builder1.Append(offsets1, 12));
  std::shared_ptr<arrow::Buffer> offsetsBuffer1;
  if (!builder1.Finish(&offsetsBuffer1).ok()) {
    FAIL() << "The offsets buffer can not be constructed!";
  }

  int32_t offsets3[3] = {0, 0, 4};

  BufferBuilder builder3;
  (void)(builder3.Resize(12));
  (void)(builder3.Append(offsets3, 12));
  std::shared_ptr<arrow::Buffer> offsetsBuffer3;
  if (!builder3.Finish(&offsetsBuffer3).ok()) {
    FAIL() << "The offsets buffer can not be constructed!";
  }

  std::shared_ptr<Array> valuesArray1, valuesArray3;
  (void)(valuesBuilder1.AppendNull());
  (void)(valuesBuilder1.Append(2));
  (void)(valuesBuilder1.Append(3));
  (void)(valuesBuilder1.Finish(&valuesArray1));
  (void)(valuesBuilder3.AppendNull());
  (void)(valuesBuilder3.Append(8));
  (void)(valuesBuilder3.Append(9));
  (void)(valuesBuilder3.AppendNull());
  (void)(valuesBuilder3.Finish(&valuesArray3));

  uint8_t bitmap1 = 1;  // 00000001
  auto maybeBuffer1 = AllocateBuffer(1);
  if (!maybeBuffer1.ok()) {
    FAIL() << "Buffer not created successfully";
  }
  std::shared_ptr<Buffer> bitmapBuffer1 = *std::move(maybeBuffer1);
  uint8_t* bufferData1 = bitmapBuffer1->mutable_data();
  std::memcpy(bufferData1, &bitmap1, 1);

  uint8_t bitmap3 = 3;  // 00000011
  auto maybeBuffer3 = AllocateBuffer(1);
  if (!maybeBuffer3.ok()) {
    FAIL() << "Buffer not created successfully";
  }
  std::shared_ptr<Buffer> bitmapBuffer3 = *std::move(maybeBuffer3);
  uint8_t* bufferData3 = bitmapBuffer3->mutable_data();
  std::memcpy(bufferData3, &bitmap3, 1);

  auto array1 = std::make_shared<ListArray>(sharedPtrArrowType, 2, offsetsBuffer1,
                                            valuesArray1, bitmapBuffer1);
  auto array3 = std::make_shared<ListArray>(sharedPtrArrowType, 2, offsetsBuffer3,
                                            valuesArray3, bitmapBuffer3);

  ArrayVector av;
  av.push_back(array0);
  av.push_back(array1);
  av.push_back(array2);
  av.push_back(array3);
  av.push_back(array4);
  std::shared_ptr<ChunkedArray> carray = std::make_shared<ChunkedArray>(av);

  // RecordProperty("l", carray->length());
  // RecordProperty("l0", carray->chunk(0)->length());
  // RecordProperty("l1", carray->chunk(1)->length());
  // RecordProperty("l2", carray->chunk(2)->length());
  // RecordProperty("l3", carray->chunk(3)->length());
  // RecordProperty("l4", carray->chunk(4)->length());

  // auto a1 = std::static_pointer_cast<ListArray>(carray->chunk(1));
  // auto a11 = std::static_pointer_cast<Int32Array>(a1->values());
  // RecordProperty("l11", a11->length());
  // auto a3 = std::static_pointer_cast<ListArray>(carray->chunk(3));
  // auto a31 = std::static_pointer_cast<Int32Array>(a3->values());
  // RecordProperty("l31", a31->length());

  // for (int i = 0; i < 2; i++) {
  //   RecordProperty("xn" + std::to_string(i), a1->IsNull(i));
  // }
  // for (int i = 0; i < 2; i++) {
  //   RecordProperty("xn" + std::to_string(i + 2), a3->IsNull(i));
  // }
  // for (int i = 0; i < 3; i++) {
  //   RecordProperty("v" + std::to_string(i), a11->Value(i));
  //   RecordProperty("an" + std::to_string(i), a11->IsNull(i));
  // }
  // for (int i = 0; i < 4; i++) {
  //   RecordProperty("v" + std::to_string(i + 3), a31->Value(i));
  //   RecordProperty("an" + std::to_string(i + 3), a31->IsNull(i));
  // }

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
  int64_t arrowIndexOffset = 0;
  int arrowChunkOffset = 0;

  Status st = adapters::orc::FillBatch(arrowType, x, arrowIndexOffset, arrowChunkOffset,
                                       batchSize, carray.get());
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

  writer->add(*batch);
  writer->close();
}
TEST(TestAdapterWriteNested, writeListMultibatch) {
  auto sharedPtrArrowType = list(std::make_shared<Field>("a", int32()));
  DataType* arrowType = sharedPtrArrowType.get();
  int64_t totalLength = 100;
  int64_t totalElementLength = 150;
  Int32Builder valuesBuilder;
  int32_t offset[101];

  offset[0] = 0;
  for (int i = 0; i < 100; i++) {
    switch (i % 4) {
      case 0: {
        offset[i + 1] = offset[i];
        break;
      }
      case 1: {
        (void)(valuesBuilder.Append(i - 1));
        offset[i + 1] = offset[i] + 1;
        break;
      }
      case 2: {
        (void)(valuesBuilder.AppendNull());
        (void)(valuesBuilder.AppendNull());
        offset[i + 1] = offset[i] + 2;
        break;
      }
      default: {
        (void)(valuesBuilder.Append(i - 1));
        (void)(valuesBuilder.AppendNull());
        (void)(valuesBuilder.AppendNull());
        offset[i + 1] = offset[i] + 3;
      }
    }
  }
  // Every third entry has null at struct level
  uint8_t bitmap[13] = {182, 109, 219, 182, 109, 219, 182, 109, 219, 182, 109, 219, 6};
  // 10110110 01101101 11011011 10110110 01101101 11011011
  // 10110110 01101101 11011011 10110110 01101101 11011011
  // 00000110
  int64_t batchSize = 1;

  BufferBuilder builder, offsetsBuilder;
  (void)(builder.Resize(13));
  (void)(builder.Append(bitmap, 13));
  std::shared_ptr<arrow::Buffer> bitmapBuffer;
  if (!builder.Finish(&bitmapBuffer).ok()) {
    FAIL() << "The offsets buffer can not be constructed!";
  }

  (void)(offsetsBuilder.Resize(404));
  (void)(offsetsBuilder.Append(offset, 404));
  std::shared_ptr<arrow::Buffer> offsetsBuffer;
  if (!offsetsBuilder.Finish(&offsetsBuffer).ok()) {
    FAIL() << "The offsets buffer can not be constructed!";
  }

  std::shared_ptr<Array> valuesArray;
  (void)(valuesBuilder.Finish(&valuesArray));

  auto array = std::make_shared<ListArray>(sharedPtrArrowType, 100, offsetsBuffer,
                                           valuesArray, bitmapBuffer);

  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE * 5);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:array<a:int>>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::ListVectorBatch* x =
      internal::checked_cast<liborc::ListVectorBatch*>(root->fields[0]);
  liborc::LongVectorBatch* a =
      internal::checked_cast<liborc::LongVectorBatch*>((x->elements).get());
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  int64_t resultOffset = 0;
  int64_t oldValueOffset = 0, valueOffset = 0;
  while (resultOffset < totalLength - batchSize) {
    Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize,
                                         array.get());
    orcOffset = 0;
    if (!st.ok()) {
      FAIL() << "ORC ColumnBatch not successfully filled";
    }
    writer->add(*batch);
    // RecordProperty("aro-" + std::to_string(resultOffset), arrowOffset);
    // RecordProperty("orco-" + std::to_string(resultOffset), orcOffset);
    for (int64_t i = 0; i < batchSize; i++) {
      // RecordProperty("xn-res" + std::to_string(i + resultOffset), x->notNull[i]);
      // RecordProperty("xo-res" + std::to_string(resultOffset) + "-" +
      //                    std::to_string(i + resultOffset),
      //                x->offsets[i]);
      if ((i + resultOffset) % 3) {
        EXPECT_TRUE(x->notNull[i]);
      } else {
        EXPECT_FALSE(x->notNull[i]);
      }
      EXPECT_EQ(x->offsets[i], testListOffsetGenerator(i + resultOffset) -
                                   testListOffsetGenerator(resultOffset));
    }
    EXPECT_EQ(x->offsets[batchSize], testListOffsetGenerator(batchSize + resultOffset) -
                                         testListOffsetGenerator(resultOffset));
    // RecordProperty("xo-res" + std::to_string(resultOffset) + "-" +
    //                    std::to_string(batchSize + resultOffset),
    //                x->offsets[batchSize]);
    resultOffset = resultOffset + batchSize;
    oldValueOffset = valueOffset;
    valueOffset = testListOffsetGenerator(resultOffset);
    // RecordProperty("vo-res" + std::to_string(resultOffset), oldValueOffset);
    // RecordProperty("vn-res" + std::to_string(resultOffset), valueOffset);
    for (int64_t j = 0; j < valueOffset - oldValueOffset; j++) {
      // RecordProperty("an-res" + std::to_string(j + oldValueOffset), a->notNull[j]);
      // RecordProperty("av-res" + std::to_string(j + oldValueOffset), a->data[j]);
      if ((j + oldValueOffset) % 3) {
        EXPECT_FALSE(a->notNull[j]);
      } else {
        EXPECT_TRUE(a->notNull[j]);
        EXPECT_EQ(a->data[j], (j + oldValueOffset) * 2 / 3);
      }
    }
    EXPECT_EQ(x->numElements, batchSize);
    EXPECT_EQ(a->numElements, valueOffset - oldValueOffset);
    EXPECT_TRUE(x->hasNulls);
    EXPECT_TRUE(a->hasNulls);
    batch->clear();
  }
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize,
                                       array.get());

  // RecordProperty("aro-" + std::to_string(resultOffset), arrowOffset);
  // RecordProperty("orco-" + std::to_string(resultOffset), orcOffset);
  if (!st.ok()) {
    FAIL() << "ORC ColumnBatch not successfully filled";
  }
  int64_t lastBatchSize = totalLength - resultOffset;
  EXPECT_EQ(x->numElements, lastBatchSize);
  EXPECT_EQ(a->numElements, totalElementLength - valueOffset);
  EXPECT_TRUE(x->hasNulls);
  EXPECT_TRUE(a->hasNulls);
  // RecordProperty("vo-res" + std::to_string(resultOffset), oldValueOffset);
  // RecordProperty("vn-res" + std::to_string(resultOffset), valueOffset);
  for (int64_t i = 0; i < lastBatchSize; i++) {
    // RecordProperty("xn-res" + std::to_string(i + resultOffset), x->notNull[i]);
    // RecordProperty(
    //     "xo-res" + std::to_string(resultOffset) + "-" + std::to_string(i +
    //     resultOffset), x->offsets[i]);
    if ((i + resultOffset) % 3) {
      EXPECT_TRUE(x->notNull[i]);
    } else {
      EXPECT_FALSE(x->notNull[i]);
    }
    EXPECT_EQ(x->offsets[i], testListOffsetGenerator(i + resultOffset) -
                                 testListOffsetGenerator(resultOffset));
  }
  EXPECT_EQ(x->offsets[lastBatchSize],
            totalElementLength - testListOffsetGenerator(resultOffset));
  // RecordProperty("xo-res" + std::to_string(resultOffset) + "-" + std::to_string(100),
  //                x->offsets[lastBatchSize]);
  oldValueOffset = valueOffset;
  valueOffset = totalElementLength;
  for (int64_t j = 0; j < valueOffset - oldValueOffset; j++) {
    // RecordProperty("an-res" + std::to_string(j + oldValueOffset), a->notNull[j]);
    // RecordProperty("av-res" + std::to_string(j + oldValueOffset), a->data[j]);
    if ((j + oldValueOffset) % 3) {
      EXPECT_FALSE(a->notNull[j]);
    } else {
      EXPECT_TRUE(a->notNull[j]);
      EXPECT_EQ(a->data[j], (j + oldValueOffset) * 2 / 3);
    }
  }
  writer->add(*batch);
  writer->close();
}  // namespace arrow
TEST(TestAdapterWriteNested, writeListChunkedMultibatch) {
  auto sharedPtrArrowType = list(std::make_shared<Field>("a", int32()));
  DataType* arrowType = sharedPtrArrowType.get();
  int64_t totalLength = 100;
  int64_t totalElementLength = 150;
  int64_t batchSize = 75;
  Int32Builder valuesBuilder1, valuesBuilder3;
  int32_t offset1[51], offset3[51];

  offset1[0] = 0;
  for (int i = 0; i < 50; i++) {
    switch (i % 4) {
      case 0: {
        offset1[i + 1] = offset1[i];
        break;
      }
      case 1: {
        (void)(valuesBuilder1.Append(i - 1));
        offset1[i + 1] = offset1[i] + 1;
        break;
      }
      case 2: {
        (void)(valuesBuilder1.AppendNull());
        (void)(valuesBuilder1.AppendNull());
        offset1[i + 1] = offset1[i] + 2;
        break;
      }
      default: {
        (void)(valuesBuilder1.Append(i - 1));
        (void)(valuesBuilder1.AppendNull());
        (void)(valuesBuilder1.AppendNull());
        offset1[i + 1] = offset1[i] + 3;
      }
    }
  }

  offset3[0] = 0;
  for (int i = 0; i < 50; i++) {
    switch ((i + 50) % 4) {
      case 0: {
        offset3[i + 1] = offset3[i];
        break;
      }
      case 1: {
        (void)(valuesBuilder3.Append(i + 50 - 1));
        offset3[i + 1] = offset3[i] + 1;
        break;
      }
      case 2: {
        (void)(valuesBuilder3.AppendNull());
        (void)(valuesBuilder3.AppendNull());
        offset3[i + 1] = offset3[i] + 2;
        break;
      }
      default: {
        (void)(valuesBuilder3.Append(i + 50 - 1));
        (void)(valuesBuilder3.AppendNull());
        (void)(valuesBuilder3.AppendNull());
        offset3[i + 1] = offset3[i] + 3;
      }
    }
  }

  // Every third entry has null at struct level
  uint8_t bitmap1[7] = {
      182, 109, 219, 182,
      109, 219, 2};  // 10110110 01101101 11011011 10110110 01101101 11011011 00000010
  uint8_t bitmap3[7] = {
      109, 219, 182, 109,
      219, 182, 1};  // 01101101 11011011 10110110 01101101 11011011 10110110 00000001

  BufferBuilder builder1, builder3, offsetsBuilder1, offsetsBuilder3;
  (void)(builder1.Resize(7));
  (void)(builder1.Append(bitmap1, 7));
  std::shared_ptr<arrow::Buffer> bitmapBuffer1;
  if (!builder1.Finish(&bitmapBuffer1).ok()) {
    FAIL() << "The offsets buffer can not be constructed!";
  }
  (void)(builder3.Resize(7));
  (void)(builder3.Append(bitmap3, 7));
  std::shared_ptr<arrow::Buffer> bitmapBuffer3;
  if (!builder3.Finish(&bitmapBuffer3).ok()) {
    FAIL() << "The offsets buffer can not be constructed!";
  }

  (void)(offsetsBuilder1.Resize(204));
  (void)(offsetsBuilder1.Append(offset1, 204));
  std::shared_ptr<arrow::Buffer> offsetsBuffer1;
  if (!offsetsBuilder1.Finish(&offsetsBuffer1).ok()) {
    FAIL() << "The offsets buffer can not be constructed!";
  }
  (void)(offsetsBuilder3.Resize(204));
  (void)(offsetsBuilder3.Append(offset3, 204));
  std::shared_ptr<arrow::Buffer> offsetsBuffer3;
  if (!offsetsBuilder3.Finish(&offsetsBuffer3).ok()) {
    FAIL() << "The offsets buffer can not be constructed!";
  }

  Int32Builder valuesBuilder0, offsetsBuilder0, valuesBuilder2, offsetsBuilder2,
      valuesBuilder4, offsetsBuilder4;
  std::shared_ptr<Array> valuesArray0, offsetsArray0, valuesArray2, offsetsArray2,
      valuesArray4, offsetsArray4;
  (void)(valuesBuilder0.Finish(&valuesArray0));
  (void)(offsetsBuilder0.Append(0));
  (void)(offsetsBuilder0.Finish(&offsetsArray0));
  (void)(valuesBuilder2.Finish(&valuesArray2));
  (void)(offsetsBuilder2.Append(0));
  (void)(offsetsBuilder2.Finish(&offsetsArray2));
  (void)(valuesBuilder4.Finish(&valuesArray4));
  (void)(offsetsBuilder4.Append(0));
  (void)(offsetsBuilder4.Finish(&offsetsArray4));

  std::shared_ptr<Array> valuesArray1, valuesArray3;
  (void)(valuesBuilder1.Finish(&valuesArray1));
  (void)(valuesBuilder3.Finish(&valuesArray3));

  std::shared_ptr<ListArray> array0 =
      ListArray::FromArrays(*offsetsArray0, *valuesArray0).ValueOrDie();
  std::shared_ptr<ListArray> array2 =
      ListArray::FromArrays(*offsetsArray2, *valuesArray2).ValueOrDie();
  std::shared_ptr<ListArray> array4 =
      ListArray::FromArrays(*offsetsArray4, *valuesArray4).ValueOrDie();
  auto array1 = std::make_shared<ListArray>(sharedPtrArrowType, 50, offsetsBuffer1,
                                            valuesArray1, bitmapBuffer1);
  auto array3 = std::make_shared<ListArray>(sharedPtrArrowType, 50, offsetsBuffer3,
                                            valuesArray3, bitmapBuffer3);

  ArrayVector av;
  av.push_back(array0);
  av.push_back(array1);
  av.push_back(array2);
  av.push_back(array3);
  av.push_back(array4);
  std::shared_ptr<ChunkedArray> carray = std::make_shared<ChunkedArray>(av);

  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE * 5);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:array<a:int>>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::ListVectorBatch* x =
      internal::checked_cast<liborc::ListVectorBatch*>(root->fields[0]);
  liborc::LongVectorBatch* a =
      internal::checked_cast<liborc::LongVectorBatch*>((x->elements).get());
  int64_t arrowIndexOffset = 0;
  int arrowChunkOffset = 0;
  int64_t resultOffset = 0;
  int64_t oldValueOffset = 0, valueOffset = 0;
  while (resultOffset < totalLength - batchSize) {
    Status st = adapters::orc::FillBatch(arrowType, x, arrowIndexOffset, arrowChunkOffset,
                                         batchSize, carray.get());
    if (!st.ok()) {
      FAIL() << "ORC ColumnBatch not successfully filled";
    }
    writer->add(*batch);
    // RecordProperty("aio-" + std::to_string(resultOffset), arrowIndexOffset);
    // RecordProperty("aco-" + std::to_string(resultOffset), arrowChunkOffset);
    for (int64_t i = 0; i < batchSize; i++) {
      // RecordProperty("xn-res" + std::to_string(i + resultOffset), x->notNull[i]);
      // RecordProperty("xo-res" + std::to_string(resultOffset) + "-" +
      //                    std::to_string(i + resultOffset),
      //                x->offsets[i]);
      if ((i + resultOffset) % 3) {
        EXPECT_TRUE(x->notNull[i]);
      } else {
        EXPECT_FALSE(x->notNull[i]);
      }
      EXPECT_EQ(x->offsets[i], testListOffsetGenerator(i + resultOffset) -
                                   testListOffsetGenerator(resultOffset));
    }
    EXPECT_EQ(x->offsets[batchSize], testListOffsetGenerator(batchSize + resultOffset) -
                                         testListOffsetGenerator(resultOffset));
    // RecordProperty("xo-res" + std::to_string(resultOffset) + "-" +
    //                    std::to_string(batchSize + resultOffset),
    //                x->offsets[batchSize]);
    resultOffset = resultOffset + batchSize;
    oldValueOffset = valueOffset;
    valueOffset = testListOffsetGenerator(resultOffset);
    // RecordProperty("vo-res" + std::to_string(resultOffset), oldValueOffset);
    // RecordProperty("vn-res" + std::to_string(resultOffset), valueOffset);
    for (int64_t j = 0; j < valueOffset - oldValueOffset; j++) {
      // RecordProperty("an-res" + std::to_string(j + oldValueOffset), a->notNull[j]);
      // RecordProperty("av-res" + std::to_string(j + oldValueOffset), a->data[j]);
      if ((j + oldValueOffset) % 3) {
        EXPECT_FALSE(a->notNull[j]);
      } else {
        EXPECT_TRUE(a->notNull[j]);
        EXPECT_EQ(a->data[j], (j + oldValueOffset) * 2 / 3);
      }
    }
    EXPECT_EQ(x->numElements, batchSize);
    EXPECT_EQ(a->numElements, valueOffset - oldValueOffset);
    EXPECT_TRUE(x->hasNulls);
    EXPECT_TRUE(a->hasNulls);
    batch->clear();
  }
  Status st = adapters::orc::FillBatch(arrowType, x, arrowIndexOffset, arrowChunkOffset,
                                       batchSize, carray.get());

  // RecordProperty("aio-" + std::to_string(resultOffset), arrowIndexOffset);
  // RecordProperty("aco-" + std::to_string(resultOffset), arrowChunkOffset);
  if (!st.ok()) {
    FAIL() << "ORC ColumnBatch not successfully filled";
  }
  int64_t lastBatchSize = totalLength - resultOffset;
  EXPECT_EQ(x->numElements, lastBatchSize);
  EXPECT_EQ(a->numElements, totalElementLength - valueOffset);
  EXPECT_TRUE(x->hasNulls);
  EXPECT_TRUE(a->hasNulls);
  // RecordProperty("vo-res" + std::to_string(resultOffset), oldValueOffset);
  // RecordProperty("vn-res" + std::to_string(resultOffset), valueOffset);
  for (int64_t i = 0; i < lastBatchSize; i++) {
    // RecordProperty("xn-res" + std::to_string(i + resultOffset), x->notNull[i]);
    // RecordProperty(
    //     "xo-res" + std::to_string(resultOffset) + "-" + std::to_string(i +
    //     resultOffset), x->offsets[i]);
    if ((i + resultOffset) % 3) {
      EXPECT_TRUE(x->notNull[i]);
    } else {
      EXPECT_FALSE(x->notNull[i]);
    }
    EXPECT_EQ(x->offsets[i], testListOffsetGenerator(i + resultOffset) -
                                 testListOffsetGenerator(resultOffset));
  }
  EXPECT_EQ(x->offsets[lastBatchSize],
            totalElementLength - testListOffsetGenerator(resultOffset));
  // RecordProperty("xo-res" + std::to_string(resultOffset) + "-" + std::to_string(100),
  //                x->offsets[lastBatchSize]);
  oldValueOffset = valueOffset;
  valueOffset = totalElementLength;
  for (int64_t j = 0; j < valueOffset - oldValueOffset; j++) {
    // RecordProperty("an-res" + std::to_string(j + oldValueOffset), a->notNull[j]);
    // RecordProperty("av-res" + std::to_string(j + oldValueOffset), a->data[j]);
    if ((j + oldValueOffset) % 3) {
      EXPECT_FALSE(a->notNull[j]);
    } else {
      EXPECT_TRUE(a->notNull[j]);
      EXPECT_EQ(a->data[j], (j + oldValueOffset) * 2 / 3);
    }
  }
  writer->add(*batch);
  writer->close();
}

// LargeList
TEST(TestAdapterWriteNested, writeLargeListEmpty) {
  auto sharedPtrArrowType = large_list(std::make_shared<Field>("a", int32()));
  DataType* arrowType = sharedPtrArrowType.get();

  Int32Builder valuesBuilder;
  Int64Builder offsetsBuilder;
  std::shared_ptr<Array> valuesArray, offsetsArray;
  (void)(valuesBuilder.Finish(&valuesArray));
  (void)(offsetsBuilder.Append(0));
  (void)(offsetsBuilder.Finish(&offsetsArray));

  std::shared_ptr<LargeListArray> array =
      LargeListArray::FromArrays(*offsetsArray, *valuesArray).ValueOrDie();

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

  EXPECT_EQ(arrowOffset, 0);
  EXPECT_EQ(orcOffset, 0);
  writer->add(*batch);
  writer->close();
}
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
TEST(TestAdapterWriteNested, writeLargeListMixed4) {
  auto sharedPtrArrowType = large_list(std::make_shared<Field>("a", int32()));
  DataType* arrowType = sharedPtrArrowType.get();
  Int32Builder valuesBuilder;
  int64_t offset[101];

  offset[0] = 0;
  for (int i = 0; i < 100; i++) {
    switch (i % 4) {
      case 0: {
        offset[i + 1] = offset[i];
        break;
      }
      case 1: {
        (void)(valuesBuilder.Append(i - 1));
        offset[i + 1] = offset[i] + 1;
        break;
      }
      case 2: {
        (void)(valuesBuilder.AppendNull());
        (void)(valuesBuilder.AppendNull());
        offset[i + 1] = offset[i] + 2;
        break;
      }
      default: {
        (void)(valuesBuilder.Append(i - 1));
        (void)(valuesBuilder.AppendNull());
        (void)(valuesBuilder.AppendNull());
        offset[i + 1] = offset[i] + 3;
      }
    }
  }
  // Every third entry has null at struct level
  uint8_t bitmap[13] = {182, 109, 219, 182, 109, 219, 182, 109, 219, 182, 109, 219, 6};
  // 10110110 01101101 11011011 10110110 01101101 11011011
  // 10110110 01101101 11011011 10110110 01101101 11011011
  // 00000110
  int64_t batchSize = 100;

  BufferBuilder builder, offsetsBuilder;
  (void)(builder.Resize(13));
  (void)(builder.Append(bitmap, 13));
  std::shared_ptr<arrow::Buffer> bitmapBuffer;
  if (!builder.Finish(&bitmapBuffer).ok()) {
    FAIL() << "The offsets buffer can not be constructed!";
  }

  (void)(offsetsBuilder.Resize(808));
  (void)(offsetsBuilder.Append(offset, 808));
  std::shared_ptr<arrow::Buffer> offsetsBuffer;
  if (!offsetsBuilder.Finish(&offsetsBuffer).ok()) {
    FAIL() << "The offsets buffer can not be constructed!";
  }

  std::shared_ptr<Array> valuesArray;
  (void)(valuesBuilder.Finish(&valuesArray));

  auto array = std::make_shared<LargeListArray>(sharedPtrArrowType, 100, offsetsBuffer,
                                                valuesArray, bitmapBuffer);

  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE * 5);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:array<a:int>>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
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

  writer->add(*batch);
  for (uint64_t i = 0; i < 100; i++) {
    if (i % 3) {
      EXPECT_TRUE(x->notNull[i]);
    } else {
      EXPECT_FALSE(x->notNull[i]);
    }
    int64_t offset_ = testListOffsetGenerator(i);
    EXPECT_EQ(x->offsets[i], offset_);
  }
  EXPECT_EQ(x->offsets[100], 150);
  for (uint64_t j = 0; j < 150; j++) {
    if (j % 3) {
      EXPECT_FALSE(a->notNull[j]);
    } else {
      EXPECT_TRUE(a->notNull[j]);
      EXPECT_EQ(a->data[j], j * 2 / 3);
    }
  }
  EXPECT_EQ(x->numElements, batchSize);
  EXPECT_EQ(a->numElements, 150);
  EXPECT_TRUE(x->hasNulls);
  EXPECT_TRUE(a->hasNulls);
  batch->clear();
  writer->close();
}
TEST(TestAdapterWriteNested, writeLargeListChunkedEmpty) {
  auto sharedPtrArrowType = large_list(std::make_shared<Field>("a", int32()));
  ArrayVector av;
  std::shared_ptr<ChunkedArray> carray =
      std::make_shared<ChunkedArray>(av, sharedPtrArrowType);
  DataType* arrowType = sharedPtrArrowType.get();

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
  int64_t arrowIndexOffset = 0;
  int arrowChunkOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowIndexOffset, arrowChunkOffset,
                                       batchSize, carray.get());
  if (!st.ok()) {
    FAIL() << "ORC ColumnBatch not successfully filled";
  }
  EXPECT_EQ(x->numElements, 0);
  EXPECT_FALSE(x->hasNulls);
  EXPECT_EQ(a->numElements, 0);
  EXPECT_FALSE(a->hasNulls);

  writer->add(*batch);
  writer->close();
}
TEST(TestAdapterWriteNested, writeLargeListChunkedMixed) {
  auto sharedPtrArrowType = large_list(std::make_shared<Field>("a", int32()));
  DataType* arrowType = sharedPtrArrowType.get();

  Int32Builder valuesBuilder0, valuesBuilder2, valuesBuilder4;
  Int64Builder offsetsBuilder0, offsetsBuilder2, offsetsBuilder4;
  std::shared_ptr<Array> valuesArray0, offsetsArray0, valuesArray2, offsetsArray2,
      valuesArray4, offsetsArray4;
  (void)(valuesBuilder0.Finish(&valuesArray0));
  (void)(offsetsBuilder0.Append(0));
  (void)(offsetsBuilder0.Finish(&offsetsArray0));
  (void)(valuesBuilder2.Finish(&valuesArray2));
  (void)(offsetsBuilder2.Append(0));
  (void)(offsetsBuilder2.Finish(&offsetsArray2));
  (void)(valuesBuilder4.Finish(&valuesArray4));
  (void)(offsetsBuilder4.Append(0));
  (void)(offsetsBuilder4.Finish(&offsetsArray4));

  std::shared_ptr<LargeListArray> array0 =
      LargeListArray::FromArrays(*offsetsArray0, *valuesArray0).ValueOrDie();
  std::shared_ptr<LargeListArray> array2 =
      LargeListArray::FromArrays(*offsetsArray2, *valuesArray2).ValueOrDie();
  std::shared_ptr<LargeListArray> array4 =
      LargeListArray::FromArrays(*offsetsArray4, *valuesArray4).ValueOrDie();

  Int32Builder valuesBuilder1, valuesBuilder3;
  Int64Builder offsetsBuilder1, offsetsBuilder3;
  int64_t offsets1[3] = {0, 1, 3};

  BufferBuilder builder1;
  (void)(builder1.Resize(24));
  (void)(builder1.Append(offsets1, 24));
  std::shared_ptr<arrow::Buffer> offsetsBuffer1;
  if (!builder1.Finish(&offsetsBuffer1).ok()) {
    FAIL() << "The offsets buffer can not be constructed!";
  }

  int64_t offsets3[3] = {0, 0, 4};

  BufferBuilder builder3;
  (void)(builder3.Resize(24));
  (void)(builder3.Append(offsets3, 24));
  std::shared_ptr<arrow::Buffer> offsetsBuffer3;
  if (!builder3.Finish(&offsetsBuffer3).ok()) {
    FAIL() << "The offsets buffer can not be constructed!";
  }

  std::shared_ptr<Array> valuesArray1, valuesArray3;
  (void)(valuesBuilder1.AppendNull());
  (void)(valuesBuilder1.Append(2));
  (void)(valuesBuilder1.Append(3));
  (void)(valuesBuilder1.Finish(&valuesArray1));
  (void)(valuesBuilder3.AppendNull());
  (void)(valuesBuilder3.Append(8));
  (void)(valuesBuilder3.Append(9));
  (void)(valuesBuilder3.AppendNull());
  (void)(valuesBuilder3.Finish(&valuesArray3));

  uint8_t bitmap1 = 1;  // 00000001
  auto maybeBuffer1 = AllocateBuffer(1);
  if (!maybeBuffer1.ok()) {
    FAIL() << "Buffer not created successfully";
  }
  std::shared_ptr<Buffer> bitmapBuffer1 = *std::move(maybeBuffer1);
  uint8_t* bufferData1 = bitmapBuffer1->mutable_data();
  std::memcpy(bufferData1, &bitmap1, 1);

  uint8_t bitmap3 = 3;  // 00000011
  auto maybeBuffer3 = AllocateBuffer(1);
  if (!maybeBuffer3.ok()) {
    FAIL() << "Buffer not created successfully";
  }
  std::shared_ptr<Buffer> bitmapBuffer3 = *std::move(maybeBuffer3);
  uint8_t* bufferData3 = bitmapBuffer3->mutable_data();
  std::memcpy(bufferData3, &bitmap3, 1);

  auto array1 = std::make_shared<LargeListArray>(sharedPtrArrowType, 2, offsetsBuffer1,
                                                 valuesArray1, bitmapBuffer1);
  auto array3 = std::make_shared<LargeListArray>(sharedPtrArrowType, 2, offsetsBuffer3,
                                                 valuesArray3, bitmapBuffer3);

  ArrayVector av;
  av.push_back(array0);
  av.push_back(array1);
  av.push_back(array2);
  av.push_back(array3);
  av.push_back(array4);
  std::shared_ptr<ChunkedArray> carray = std::make_shared<ChunkedArray>(av);

  // RecordProperty("l", carray->length());
  // RecordProperty("l0", carray->chunk(0)->length());
  // RecordProperty("l1", carray->chunk(1)->length());
  // RecordProperty("l2", carray->chunk(2)->length());
  // RecordProperty("l3", carray->chunk(3)->length());
  // RecordProperty("l4", carray->chunk(4)->length());

  // auto a1 = std::static_pointer_cast<ListArray>(carray->chunk(1));
  // auto a11 = std::static_pointer_cast<Int32Array>(a1->values());
  // RecordProperty("l11", a11->length());
  // auto a3 = std::static_pointer_cast<ListArray>(carray->chunk(3));
  // auto a31 = std::static_pointer_cast<Int32Array>(a3->values());
  // RecordProperty("l31", a31->length());

  // for (int i = 0; i < 2; i++) {
  //   RecordProperty("xn" + std::to_string(i), a1->IsNull(i));
  // }
  // for (int i = 0; i < 2; i++) {
  //   RecordProperty("xn" + std::to_string(i + 2), a3->IsNull(i));
  // }
  // for (int i = 0; i < 3; i++) {
  //   RecordProperty("v" + std::to_string(i), a11->Value(i));
  //   RecordProperty("an" + std::to_string(i), a11->IsNull(i));
  // }
  // for (int i = 0; i < 4; i++) {
  //   RecordProperty("v" + std::to_string(i + 3), a31->Value(i));
  //   RecordProperty("an" + std::to_string(i + 3), a31->IsNull(i));
  // }

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
  int64_t arrowIndexOffset = 0;
  int arrowChunkOffset = 0;

  Status st = adapters::orc::FillBatch(arrowType, x, arrowIndexOffset, arrowChunkOffset,
                                       batchSize, carray.get());
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

  writer->add(*batch);
  writer->close();
}
TEST(TestAdapterWriteNested, writeLargeListMultibatch) {
  auto sharedPtrArrowType = large_list(std::make_shared<Field>("a", int32()));
  DataType* arrowType = sharedPtrArrowType.get();
  int64_t totalLength = 100;
  int64_t totalElementLength = 150;
  Int32Builder valuesBuilder;
  int64_t offset[101];

  offset[0] = 0;
  for (int i = 0; i < 100; i++) {
    switch (i % 4) {
      case 0: {
        offset[i + 1] = offset[i];
        break;
      }
      case 1: {
        (void)(valuesBuilder.Append(i - 1));
        offset[i + 1] = offset[i] + 1;
        break;
      }
      case 2: {
        (void)(valuesBuilder.AppendNull());
        (void)(valuesBuilder.AppendNull());
        offset[i + 1] = offset[i] + 2;
        break;
      }
      default: {
        (void)(valuesBuilder.Append(i - 1));
        (void)(valuesBuilder.AppendNull());
        (void)(valuesBuilder.AppendNull());
        offset[i + 1] = offset[i] + 3;
      }
    }
  }
  // Every third entry has null at struct level
  uint8_t bitmap[13] = {182, 109, 219, 182, 109, 219, 182, 109, 219, 182, 109, 219, 6};
  // 10110110 01101101 11011011 10110110 01101101 11011011
  // 10110110 01101101 11011011 10110110 01101101 11011011
  // 00000110
  int64_t batchSize = 1;

  BufferBuilder builder, offsetsBuilder;
  (void)(builder.Resize(13));
  (void)(builder.Append(bitmap, 13));
  std::shared_ptr<arrow::Buffer> bitmapBuffer;
  if (!builder.Finish(&bitmapBuffer).ok()) {
    FAIL() << "The offsets buffer can not be constructed!";
  }

  (void)(offsetsBuilder.Resize(808));
  (void)(offsetsBuilder.Append(offset, 808));
  std::shared_ptr<arrow::Buffer> offsetsBuffer;
  if (!offsetsBuilder.Finish(&offsetsBuffer).ok()) {
    FAIL() << "The offsets buffer can not be constructed!";
  }

  std::shared_ptr<Array> valuesArray;
  (void)(valuesBuilder.Finish(&valuesArray));

  auto array = std::make_shared<LargeListArray>(sharedPtrArrowType, 100, offsetsBuffer,
                                                valuesArray, bitmapBuffer);

  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE * 5);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:array<a:int>>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::ListVectorBatch* x =
      internal::checked_cast<liborc::ListVectorBatch*>(root->fields[0]);
  liborc::LongVectorBatch* a =
      internal::checked_cast<liborc::LongVectorBatch*>((x->elements).get());
  int64_t arrowOffset = 0;
  int64_t orcOffset = 0;
  int64_t resultOffset = 0;
  int64_t oldValueOffset = 0, valueOffset = 0;
  while (resultOffset < totalLength - batchSize) {
    Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize,
                                         array.get());
    orcOffset = 0;
    if (!st.ok()) {
      FAIL() << "ORC ColumnBatch not successfully filled";
    }
    writer->add(*batch);
    // RecordProperty("aro-" + std::to_string(resultOffset), arrowOffset);
    // RecordProperty("orco-" + std::to_string(resultOffset), orcOffset);
    for (int64_t i = 0; i < batchSize; i++) {
      // RecordProperty("xn-res" + std::to_string(i + resultOffset), x->notNull[i]);
      // RecordProperty("xo-res" + std::to_string(resultOffset) + "-" +
      //                    std::to_string(i + resultOffset),
      //                x->offsets[i]);
      if ((i + resultOffset) % 3) {
        EXPECT_TRUE(x->notNull[i]);
      } else {
        EXPECT_FALSE(x->notNull[i]);
      }
      EXPECT_EQ(x->offsets[i], testListOffsetGenerator(i + resultOffset) -
                                   testListOffsetGenerator(resultOffset));
    }
    EXPECT_EQ(x->offsets[batchSize], testListOffsetGenerator(batchSize + resultOffset) -
                                         testListOffsetGenerator(resultOffset));
    // RecordProperty("xo-res" + std::to_string(resultOffset) + "-" +
    //                    std::to_string(batchSize + resultOffset),
    //                x->offsets[batchSize]);
    resultOffset = resultOffset + batchSize;
    oldValueOffset = valueOffset;
    valueOffset = testListOffsetGenerator(resultOffset);
    // RecordProperty("vo-res" + std::to_string(resultOffset), oldValueOffset);
    // RecordProperty("vn-res" + std::to_string(resultOffset), valueOffset);
    for (int64_t j = 0; j < valueOffset - oldValueOffset; j++) {
      // RecordProperty("an-res" + std::to_string(j + oldValueOffset), a->notNull[j]);
      // RecordProperty("av-res" + std::to_string(j + oldValueOffset), a->data[j]);
      if ((j + oldValueOffset) % 3) {
        EXPECT_FALSE(a->notNull[j]);
      } else {
        EXPECT_TRUE(a->notNull[j]);
        EXPECT_EQ(a->data[j], (j + oldValueOffset) * 2 / 3);
      }
    }
    EXPECT_EQ(x->numElements, batchSize);
    EXPECT_EQ(a->numElements, valueOffset - oldValueOffset);
    EXPECT_TRUE(x->hasNulls);
    EXPECT_TRUE(a->hasNulls);
    batch->clear();
  }
  Status st = adapters::orc::FillBatch(arrowType, x, arrowOffset, orcOffset, batchSize,
                                       array.get());

  // RecordProperty("aro-" + std::to_string(resultOffset), arrowOffset);
  // RecordProperty("orco-" + std::to_string(resultOffset), orcOffset);
  if (!st.ok()) {
    FAIL() << "ORC ColumnBatch not successfully filled";
  }
  int64_t lastBatchSize = totalLength - resultOffset;
  EXPECT_EQ(x->numElements, lastBatchSize);
  EXPECT_EQ(a->numElements, totalElementLength - valueOffset);
  EXPECT_TRUE(x->hasNulls);
  EXPECT_TRUE(a->hasNulls);
  // RecordProperty("vo-res" + std::to_string(resultOffset), oldValueOffset);
  // RecordProperty("vn-res" + std::to_string(resultOffset), valueOffset);
  for (int64_t i = 0; i < lastBatchSize; i++) {
    // RecordProperty("xn-res" + std::to_string(i + resultOffset), x->notNull[i]);
    // RecordProperty(
    //     "xo-res" + std::to_string(resultOffset) + "-" + std::to_string(i +
    //     resultOffset), x->offsets[i]);
    if ((i + resultOffset) % 3) {
      EXPECT_TRUE(x->notNull[i]);
    } else {
      EXPECT_FALSE(x->notNull[i]);
    }
    EXPECT_EQ(x->offsets[i], testListOffsetGenerator(i + resultOffset) -
                                 testListOffsetGenerator(resultOffset));
  }
  EXPECT_EQ(x->offsets[lastBatchSize],
            totalElementLength - testListOffsetGenerator(resultOffset));
  // RecordProperty("xo-res" + std::to_string(resultOffset) + "-" + std::to_string(100),
  //                x->offsets[lastBatchSize]);
  oldValueOffset = valueOffset;
  valueOffset = totalElementLength;
  for (int64_t j = 0; j < valueOffset - oldValueOffset; j++) {
    // RecordProperty("an-res" + std::to_string(j + oldValueOffset), a->notNull[j]);
    // RecordProperty("av-res" + std::to_string(j + oldValueOffset), a->data[j]);
    if ((j + oldValueOffset) % 3) {
      EXPECT_FALSE(a->notNull[j]);
    } else {
      EXPECT_TRUE(a->notNull[j]);
      EXPECT_EQ(a->data[j], (j + oldValueOffset) * 2 / 3);
    }
  }
  writer->add(*batch);
  writer->close();
}  // namespace arrow
TEST(TestAdapterWriteNested, writeLargeListChunkedMultibatch) {
  auto sharedPtrArrowType = large_list(std::make_shared<Field>("a", int32()));
  DataType* arrowType = sharedPtrArrowType.get();
  int64_t totalLength = 100;
  int64_t totalElementLength = 150;
  int64_t batchSize = 7;
  Int32Builder valuesBuilder1, valuesBuilder3;
  int64_t offset1[51], offset3[51];

  offset1[0] = 0;
  for (int i = 0; i < 50; i++) {
    switch (i % 4) {
      case 0: {
        offset1[i + 1] = offset1[i];
        break;
      }
      case 1: {
        (void)(valuesBuilder1.Append(i - 1));
        offset1[i + 1] = offset1[i] + 1;
        break;
      }
      case 2: {
        (void)(valuesBuilder1.AppendNull());
        (void)(valuesBuilder1.AppendNull());
        offset1[i + 1] = offset1[i] + 2;
        break;
      }
      default: {
        (void)(valuesBuilder1.Append(i - 1));
        (void)(valuesBuilder1.AppendNull());
        (void)(valuesBuilder1.AppendNull());
        offset1[i + 1] = offset1[i] + 3;
      }
    }
  }

  offset3[0] = 0;
  for (int i = 0; i < 50; i++) {
    switch ((i + 50) % 4) {
      case 0: {
        offset3[i + 1] = offset3[i];
        break;
      }
      case 1: {
        (void)(valuesBuilder3.Append(i + 50 - 1));
        offset3[i + 1] = offset3[i] + 1;
        break;
      }
      case 2: {
        (void)(valuesBuilder3.AppendNull());
        (void)(valuesBuilder3.AppendNull());
        offset3[i + 1] = offset3[i] + 2;
        break;
      }
      default: {
        (void)(valuesBuilder3.Append(i + 50 - 1));
        (void)(valuesBuilder3.AppendNull());
        (void)(valuesBuilder3.AppendNull());
        offset3[i + 1] = offset3[i] + 3;
      }
    }
  }

  // Every third entry has null at struct level
  uint8_t bitmap1[7] = {
      182, 109, 219, 182,
      109, 219, 2};  // 10110110 01101101 11011011 10110110 01101101 11011011 00000010
  uint8_t bitmap3[7] = {
      109, 219, 182, 109,
      219, 182, 1};  // 01101101 11011011 10110110 01101101 11011011 10110110 00000001

  BufferBuilder builder1, builder3, offsetsBuilder1, offsetsBuilder3;
  (void)(builder1.Resize(7));
  (void)(builder1.Append(bitmap1, 7));
  std::shared_ptr<arrow::Buffer> bitmapBuffer1;
  if (!builder1.Finish(&bitmapBuffer1).ok()) {
    FAIL() << "The offsets buffer can not be constructed!";
  }
  (void)(builder3.Resize(7));
  (void)(builder3.Append(bitmap3, 7));
  std::shared_ptr<arrow::Buffer> bitmapBuffer3;
  if (!builder3.Finish(&bitmapBuffer3).ok()) {
    FAIL() << "The offsets buffer can not be constructed!";
  }

  (void)(offsetsBuilder1.Resize(408));
  (void)(offsetsBuilder1.Append(offset1, 408));
  std::shared_ptr<arrow::Buffer> offsetsBuffer1;
  if (!offsetsBuilder1.Finish(&offsetsBuffer1).ok()) {
    FAIL() << "The offsets buffer can not be constructed!";
  }
  (void)(offsetsBuilder3.Resize(408));
  (void)(offsetsBuilder3.Append(offset3, 408));
  std::shared_ptr<arrow::Buffer> offsetsBuffer3;
  if (!offsetsBuilder3.Finish(&offsetsBuffer3).ok()) {
    FAIL() << "The offsets buffer can not be constructed!";
  }

  Int32Builder valuesBuilder0, valuesBuilder2, valuesBuilder4;
  Int64Builder offsetsBuilder0, offsetsBuilder2, offsetsBuilder4;
  std::shared_ptr<Array> valuesArray0, offsetsArray0, valuesArray2, offsetsArray2,
      valuesArray4, offsetsArray4;
  (void)(valuesBuilder0.Finish(&valuesArray0));
  (void)(offsetsBuilder0.Append(0));
  (void)(offsetsBuilder0.Finish(&offsetsArray0));
  (void)(valuesBuilder2.Finish(&valuesArray2));
  (void)(offsetsBuilder2.Append(0));
  (void)(offsetsBuilder2.Finish(&offsetsArray2));
  (void)(valuesBuilder4.Finish(&valuesArray4));
  (void)(offsetsBuilder4.Append(0));
  (void)(offsetsBuilder4.Finish(&offsetsArray4));

  std::shared_ptr<Array> valuesArray1, valuesArray3;
  (void)(valuesBuilder1.Finish(&valuesArray1));
  (void)(valuesBuilder3.Finish(&valuesArray3));

  std::shared_ptr<LargeListArray> array0 =
      LargeListArray::FromArrays(*offsetsArray0, *valuesArray0).ValueOrDie();
  std::shared_ptr<LargeListArray> array2 =
      LargeListArray::FromArrays(*offsetsArray2, *valuesArray2).ValueOrDie();
  std::shared_ptr<LargeListArray> array4 =
      LargeListArray::FromArrays(*offsetsArray4, *valuesArray4).ValueOrDie();
  auto array1 = std::make_shared<LargeListArray>(sharedPtrArrowType, 50, offsetsBuffer1,
                                                 valuesArray1, bitmapBuffer1);
  auto array3 = std::make_shared<LargeListArray>(sharedPtrArrowType, 50, offsetsBuffer3,
                                                 valuesArray3, bitmapBuffer3);

  ArrayVector av;
  av.push_back(array0);
  av.push_back(array1);
  av.push_back(array2);
  av.push_back(array3);
  av.push_back(array4);
  std::shared_ptr<ChunkedArray> carray = std::make_shared<ChunkedArray>(av);

  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE * 5);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:array<a:int>>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::ListVectorBatch* x =
      internal::checked_cast<liborc::ListVectorBatch*>(root->fields[0]);
  liborc::LongVectorBatch* a =
      internal::checked_cast<liborc::LongVectorBatch*>((x->elements).get());
  int64_t arrowIndexOffset = 0;
  int arrowChunkOffset = 0;
  int64_t resultOffset = 0;
  int64_t oldValueOffset = 0, valueOffset = 0;
  while (resultOffset < totalLength - batchSize) {
    Status st = adapters::orc::FillBatch(arrowType, x, arrowIndexOffset, arrowChunkOffset,
                                         batchSize, carray.get());
    if (!st.ok()) {
      FAIL() << "ORC ColumnBatch not successfully filled";
    }
    writer->add(*batch);
    // RecordProperty("aio-" + std::to_string(resultOffset), arrowIndexOffset);
    // RecordProperty("aco-" + std::to_string(resultOffset), arrowChunkOffset);
    for (int64_t i = 0; i < batchSize; i++) {
      // RecordProperty("xn-res" + std::to_string(i + resultOffset), x->notNull[i]);
      // RecordProperty("xo-res" + std::to_string(resultOffset) + "-" +
      //                    std::to_string(i + resultOffset),
      //                x->offsets[i]);
      if ((i + resultOffset) % 3) {
        EXPECT_TRUE(x->notNull[i]);
      } else {
        EXPECT_FALSE(x->notNull[i]);
      }
      EXPECT_EQ(x->offsets[i], testListOffsetGenerator(i + resultOffset) -
                                   testListOffsetGenerator(resultOffset));
    }
    EXPECT_EQ(x->offsets[batchSize], testListOffsetGenerator(batchSize + resultOffset) -
                                         testListOffsetGenerator(resultOffset));
    // RecordProperty("xo-res" + std::to_string(resultOffset) + "-" +
    //                    std::to_string(batchSize + resultOffset),
    //                x->offsets[batchSize]);
    resultOffset = resultOffset + batchSize;
    oldValueOffset = valueOffset;
    valueOffset = testListOffsetGenerator(resultOffset);
    // RecordProperty("vo-res" + std::to_string(resultOffset), oldValueOffset);
    // RecordProperty("vn-res" + std::to_string(resultOffset), valueOffset);
    for (int64_t j = 0; j < valueOffset - oldValueOffset; j++) {
      // RecordProperty("an-res" + std::to_string(j + oldValueOffset), a->notNull[j]);
      // RecordProperty("av-res" + std::to_string(j + oldValueOffset), a->data[j]);
      if ((j + oldValueOffset) % 3) {
        EXPECT_FALSE(a->notNull[j]);
      } else {
        EXPECT_TRUE(a->notNull[j]);
        EXPECT_EQ(a->data[j], (j + oldValueOffset) * 2 / 3);
      }
    }
    EXPECT_EQ(x->numElements, batchSize);
    EXPECT_EQ(a->numElements, valueOffset - oldValueOffset);
    EXPECT_TRUE(x->hasNulls);
    EXPECT_TRUE(a->hasNulls);
    batch->clear();
  }
  Status st = adapters::orc::FillBatch(arrowType, x, arrowIndexOffset, arrowChunkOffset,
                                       batchSize, carray.get());

  // RecordProperty("aio-" + std::to_string(resultOffset), arrowIndexOffset);
  // RecordProperty("aco-" + std::to_string(resultOffset), arrowChunkOffset);
  if (!st.ok()) {
    FAIL() << "ORC ColumnBatch not successfully filled";
  }
  int64_t lastBatchSize = totalLength - resultOffset;
  EXPECT_EQ(x->numElements, lastBatchSize);
  EXPECT_EQ(a->numElements, totalElementLength - valueOffset);
  EXPECT_TRUE(x->hasNulls);
  EXPECT_TRUE(a->hasNulls);
  // RecordProperty("vo-res" + std::to_string(resultOffset), oldValueOffset);
  // RecordProperty("vn-res" + std::to_string(resultOffset), valueOffset);
  for (int64_t i = 0; i < lastBatchSize; i++) {
    // RecordProperty("xn-res" + std::to_string(i + resultOffset), x->notNull[i]);
    // RecordProperty(
    //     "xo-res" + std::to_string(resultOffset) + "-" + std::to_string(i +
    //     resultOffset), x->offsets[i]);
    if ((i + resultOffset) % 3) {
      EXPECT_TRUE(x->notNull[i]);
    } else {
      EXPECT_FALSE(x->notNull[i]);
    }
    EXPECT_EQ(x->offsets[i], testListOffsetGenerator(i + resultOffset) -
                                 testListOffsetGenerator(resultOffset));
  }
  EXPECT_EQ(x->offsets[lastBatchSize],
            totalElementLength - testListOffsetGenerator(resultOffset));
  // RecordProperty("xo-res" + std::to_string(resultOffset) + "-" + std::to_string(100),
  //                x->offsets[lastBatchSize]);
  oldValueOffset = valueOffset;
  valueOffset = totalElementLength;
  for (int64_t j = 0; j < valueOffset - oldValueOffset; j++) {
    // RecordProperty("an-res" + std::to_string(j + oldValueOffset), a->notNull[j]);
    // RecordProperty("av-res" + std::to_string(j + oldValueOffset), a->data[j]);
    if ((j + oldValueOffset) % 3) {
      EXPECT_FALSE(a->notNull[j]);
    } else {
      EXPECT_TRUE(a->notNull[j]);
      EXPECT_EQ(a->data[j], (j + oldValueOffset) * 2 / 3);
    }
  }
  writer->add(*batch);
  writer->close();
}

// FixedSizeList
// writeFixedSizeListZeroEmpty not allowed
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
TEST(TestAdapterWriteNested, writeFixedSizeListChunkedEmpty) {
  auto sharedPtrArrowType = fixed_size_list(std::make_shared<Field>("a", int32()), 3);
  ArrayVector av;
  std::shared_ptr<ChunkedArray> carray =
      std::make_shared<ChunkedArray>(av, sharedPtrArrowType);
  DataType* arrowType = sharedPtrArrowType.get();

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
  int64_t arrowIndexOffset = 0;
  int arrowChunkOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowIndexOffset, arrowChunkOffset,
                                       batchSize, carray.get());
  if (!st.ok()) {
    FAIL() << "ORC ColumnBatch not successfully filled";
  }
  EXPECT_EQ(x->numElements, 0);
  EXPECT_FALSE(x->hasNulls);
  EXPECT_EQ(a->numElements, 0);
  EXPECT_FALSE(a->hasNulls);

  writer->add(*batch);
  writer->close();
}
TEST(TestAdapterWriteNested, writeFixedSizeListZeroChunkedEmpty) {
  auto sharedPtrArrowType = fixed_size_list(std::make_shared<Field>("a", int32()), 0);
  ArrayVector av;
  std::shared_ptr<ChunkedArray> carray =
      std::make_shared<ChunkedArray>(av, sharedPtrArrowType);
  DataType* arrowType = sharedPtrArrowType.get();

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
  int64_t arrowIndexOffset = 0;
  int arrowChunkOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowIndexOffset, arrowChunkOffset,
                                       batchSize, carray.get());
  if (!st.ok()) {
    FAIL() << "ORC ColumnBatch not successfully filled";
  }
  EXPECT_EQ(x->numElements, 0);
  EXPECT_FALSE(x->hasNulls);
  EXPECT_EQ(a->numElements, 0);
  EXPECT_FALSE(a->hasNulls);

  writer->add(*batch);
  writer->close();
}
TEST(TestAdapterWriteNested, writeFixedSizeListChunkedMixed) {
  auto sharedPtrArrowType = fixed_size_list(std::make_shared<Field>("a", int32()), 3);
  DataType* arrowType = sharedPtrArrowType.get();

  Int32Builder valuesBuilder0, valuesBuilder2, valuesBuilder4;
  std::shared_ptr<Array> valuesArray0, valuesArray2, valuesArray4;
  (void)(valuesBuilder0.Finish(&valuesArray0));
  (void)(valuesBuilder2.Finish(&valuesArray2));
  (void)(valuesBuilder4.Finish(&valuesArray4));

  std::shared_ptr<FixedSizeListArray> array0 =
      std::static_pointer_cast<FixedSizeListArray>(
          FixedSizeListArray::FromArrays(valuesArray0, 3).ValueOrDie());
  std::shared_ptr<FixedSizeListArray> array2 =
      std::static_pointer_cast<FixedSizeListArray>(
          FixedSizeListArray::FromArrays(valuesArray2, 3).ValueOrDie());
  std::shared_ptr<FixedSizeListArray> array4 =
      std::static_pointer_cast<FixedSizeListArray>(
          FixedSizeListArray::FromArrays(valuesArray4, 3).ValueOrDie());

  Int32Builder valuesBuilder1, valuesBuilder3;

  std::shared_ptr<Array> valuesArray1, valuesArray3;
  (void)(valuesBuilder1.AppendNull());
  (void)(valuesBuilder1.Append(2));
  (void)(valuesBuilder1.Append(3));
  (void)(valuesBuilder1.AppendNull());
  (void)(valuesBuilder1.Append(5));
  (void)(valuesBuilder1.Append(6));
  (void)(valuesBuilder1.Finish(&valuesArray1));
  (void)(valuesBuilder3.AppendNull());
  (void)(valuesBuilder3.Append(8));
  (void)(valuesBuilder3.Append(9));
  (void)(valuesBuilder3.AppendNull());
  (void)(valuesBuilder3.Append(11));
  (void)(valuesBuilder3.Append(12));
  (void)(valuesBuilder3.Finish(&valuesArray3));

  uint8_t bitmap1 = 1;  // 00000001
  auto maybeBuffer1 = AllocateBuffer(1);
  if (!maybeBuffer1.ok()) {
    FAIL() << "Buffer not created successfully";
  }
  std::shared_ptr<Buffer> bitmapBuffer1 = *std::move(maybeBuffer1);
  uint8_t* bufferData1 = bitmapBuffer1->mutable_data();
  std::memcpy(bufferData1, &bitmap1, 1);

  uint8_t bitmap3 = 3;  // 00000011
  auto maybeBuffer3 = AllocateBuffer(1);
  if (!maybeBuffer3.ok()) {
    FAIL() << "Buffer not created successfully";
  }
  std::shared_ptr<Buffer> bitmapBuffer3 = *std::move(maybeBuffer3);
  uint8_t* bufferData3 = bitmapBuffer3->mutable_data();
  std::memcpy(bufferData3, &bitmap3, 1);

  auto array1 = std::make_shared<FixedSizeListArray>(sharedPtrArrowType, 2, valuesArray1,
                                                     bitmapBuffer1);
  auto array3 = std::make_shared<FixedSizeListArray>(sharedPtrArrowType, 2, valuesArray3,
                                                     bitmapBuffer3);

  ArrayVector av;
  av.push_back(array0);
  av.push_back(array1);
  av.push_back(array2);
  av.push_back(array3);
  av.push_back(array4);
  std::shared_ptr<ChunkedArray> carray = std::make_shared<ChunkedArray>(av);

  // RecordProperty("l", carray->length());
  // RecordProperty("l0", carray->chunk(0)->length());
  // RecordProperty("l1", carray->chunk(1)->length());
  // RecordProperty("l2", carray->chunk(2)->length());
  // RecordProperty("l3", carray->chunk(3)->length());
  // RecordProperty("l4", carray->chunk(4)->length());

  // auto a1 = std::static_pointer_cast<ListArray>(carray->chunk(1));
  // auto a11 = std::static_pointer_cast<Int32Array>(a1->values());
  // RecordProperty("l11", a11->length());
  // auto a3 = std::static_pointer_cast<ListArray>(carray->chunk(3));
  // auto a31 = std::static_pointer_cast<Int32Array>(a3->values());
  // RecordProperty("l31", a31->length());

  // for (int i = 0; i < 2; i++) {
  //   RecordProperty("xn" + std::to_string(i), a1->IsNull(i));
  // }
  // for (int i = 0; i < 2; i++) {
  //   RecordProperty("xn" + std::to_string(i + 2), a3->IsNull(i));
  // }
  // for (int i = 0; i < 3; i++) {
  //   RecordProperty("v" + std::to_string(i), a11->Value(i));
  //   RecordProperty("an" + std::to_string(i), a11->IsNull(i));
  // }
  // for (int i = 0; i < 4; i++) {
  //   RecordProperty("v" + std::to_string(i + 3), a31->Value(i));
  //   RecordProperty("an" + std::to_string(i + 3), a31->IsNull(i));
  // }

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
  int64_t arrowIndexOffset = 0;
  int arrowChunkOffset = 0;

  Status st = adapters::orc::FillBatch(arrowType, x, arrowIndexOffset, arrowChunkOffset,
                                       batchSize, carray.get());
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
  EXPECT_EQ(a->notNull[3], 0);
  EXPECT_EQ(a->notNull[4], 1);
  EXPECT_EQ(a->notNull[5], 1);
  EXPECT_EQ(a->notNull[6], 0);
  EXPECT_EQ(a->notNull[7], 1);
  EXPECT_EQ(a->notNull[8], 1);
  EXPECT_EQ(a->notNull[9], 0);
  EXPECT_EQ(a->notNull[10], 1);
  EXPECT_EQ(a->notNull[11], 1);

  EXPECT_EQ(a->data[1], 2);
  EXPECT_EQ(a->data[2], 3);
  EXPECT_EQ(a->data[4], 5);
  EXPECT_EQ(a->data[5], 6);
  EXPECT_EQ(a->data[7], 8);
  EXPECT_EQ(a->data[8], 9);
  EXPECT_EQ(a->data[10], 11);
  EXPECT_EQ(a->data[11], 12);

  writer->add(*batch);
  writer->close();
}
TEST(TestAdapterWriteNested, writeFixedSizeListChunkedMultibatch) {
  auto sharedPtrArrowType = fixed_size_list(std::make_shared<Field>("a", int32()), 3);
  DataType* arrowType = sharedPtrArrowType.get();
  int64_t totalLength = 100;
  int64_t batchSize = 67;
  Int32Builder valuesBuilder1, valuesBuilder3;

  for (int i = 0; i < 50; i++) {
    (void)(valuesBuilder1.Append(i));
    (void)(valuesBuilder1.AppendNull());
    (void)(valuesBuilder1.AppendNull());
  }

  for (int i = 0; i < 50; i++) {
    (void)(valuesBuilder3.Append(i + 50));
    (void)(valuesBuilder3.AppendNull());
    (void)(valuesBuilder3.AppendNull());
  }

  // Every third entry has null at struct level
  uint8_t bitmap1[7] = {
      182, 109, 219, 182,
      109, 219, 2};  // 10110110 01101101 11011011 10110110 01101101 11011011 00000010
  uint8_t bitmap3[7] = {
      109, 219, 182, 109,
      219, 182, 1};  // 01101101 11011011 10110110 01101101 11011011 10110110 00000001

  BufferBuilder builder1, builder3;
  (void)(builder1.Resize(7));
  (void)(builder1.Append(bitmap1, 7));
  std::shared_ptr<arrow::Buffer> bitmapBuffer1;
  if (!builder1.Finish(&bitmapBuffer1).ok()) {
    FAIL() << "The bitmap buffer can not be constructed!";
  }
  (void)(builder3.Resize(7));
  (void)(builder3.Append(bitmap3, 7));
  std::shared_ptr<arrow::Buffer> bitmapBuffer3;
  if (!builder3.Finish(&bitmapBuffer3).ok()) {
    FAIL() << "The bitmap buffer can not be constructed!";
  }

  Int32Builder valuesBuilder0, valuesBuilder2, valuesBuilder4;
  std::shared_ptr<Array> valuesArray0, valuesArray2, valuesArray4;
  (void)(valuesBuilder0.Finish(&valuesArray0));
  (void)(valuesBuilder2.Finish(&valuesArray2));
  (void)(valuesBuilder4.Finish(&valuesArray4));

  std::shared_ptr<Array> valuesArray1, valuesArray3;
  (void)(valuesBuilder1.Finish(&valuesArray1));
  (void)(valuesBuilder3.Finish(&valuesArray3));

  std::shared_ptr<FixedSizeListArray> array0 =
      std::static_pointer_cast<FixedSizeListArray>(
          FixedSizeListArray::FromArrays(valuesArray0, 3).ValueOrDie());
  std::shared_ptr<FixedSizeListArray> array2 =
      std::static_pointer_cast<FixedSizeListArray>(
          FixedSizeListArray::FromArrays(valuesArray2, 3).ValueOrDie());
  std::shared_ptr<FixedSizeListArray> array4 =
      std::static_pointer_cast<FixedSizeListArray>(
          FixedSizeListArray::FromArrays(valuesArray4, 3).ValueOrDie());
  auto array1 = std::make_shared<FixedSizeListArray>(sharedPtrArrowType, 50, valuesArray1,
                                                     bitmapBuffer1);
  auto array3 = std::make_shared<FixedSizeListArray>(sharedPtrArrowType, 50, valuesArray3,
                                                     bitmapBuffer3);

  ArrayVector av;
  av.push_back(array0);
  av.push_back(array1);
  av.push_back(array2);
  av.push_back(array3);
  av.push_back(array4);
  std::shared_ptr<ChunkedArray> carray = std::make_shared<ChunkedArray>(av);

  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE * 5);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:array<a:int>>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::ListVectorBatch* x =
      internal::checked_cast<liborc::ListVectorBatch*>(root->fields[0]);
  liborc::LongVectorBatch* a =
      internal::checked_cast<liborc::LongVectorBatch*>((x->elements).get());
  int64_t arrowIndexOffset = 0;
  int arrowChunkOffset = 0;
  int64_t resultOffset = 0;
  int64_t oldValueOffset = 0, valueOffset = 0;
  while (resultOffset < totalLength - batchSize) {
    Status st = adapters::orc::FillBatch(arrowType, x, arrowIndexOffset, arrowChunkOffset,
                                         batchSize, carray.get());
    if (!st.ok()) {
      FAIL() << "ORC ColumnBatch not successfully filled";
    }
    writer->add(*batch);
    // RecordProperty("aio-" + std::to_string(resultOffset), arrowIndexOffset);
    // RecordProperty("aco-" + std::to_string(resultOffset), arrowChunkOffset);
    for (int64_t i = 0; i < batchSize; i++) {
      // RecordProperty("xn-res" + std::to_string(i + resultOffset), x->notNull[i]);
      // RecordProperty("xo-res" + std::to_string(resultOffset) + "-" +
      //                    std::to_string(i + resultOffset),
      //                x->offsets[i]);
      if ((i + resultOffset) % 3) {
        EXPECT_TRUE(x->notNull[i]);
      } else {
        EXPECT_FALSE(x->notNull[i]);
      }
      EXPECT_EQ(x->offsets[i], 3 * i);
    }
    EXPECT_EQ(x->offsets[batchSize], 3 * batchSize);
    // RecordProperty("xo-res" + std::to_string(resultOffset) + "-" +
    //                    std::to_string(batchSize + resultOffset),
    //                x->offsets[batchSize]);
    resultOffset = resultOffset + batchSize;
    oldValueOffset = valueOffset;
    valueOffset = 3 * resultOffset;
    // RecordProperty("vo-res" + std::to_string(resultOffset), oldValueOffset);
    // RecordProperty("vn-res" + std::to_string(resultOffset), valueOffset);
    for (int64_t j = 0; j < valueOffset - oldValueOffset; j++) {
      // RecordProperty("an-res" + std::to_string(j + oldValueOffset), a->notNull[j]);
      // RecordProperty("av-res" + std::to_string(j + oldValueOffset), a->data[j]);
      if ((j + oldValueOffset) % 3) {
        EXPECT_FALSE(a->notNull[j]);
      } else {
        EXPECT_TRUE(a->notNull[j]);
        EXPECT_EQ(a->data[j], (j + oldValueOffset) / 3);
      }
    }
    EXPECT_EQ(x->numElements, batchSize);
    EXPECT_EQ(a->numElements, valueOffset - oldValueOffset);
    EXPECT_TRUE(x->hasNulls);
    EXPECT_TRUE(a->hasNulls);
    batch->clear();
  }
  Status st = adapters::orc::FillBatch(arrowType, x, arrowIndexOffset, arrowChunkOffset,
                                       batchSize, carray.get());

  // RecordProperty("aio-" + std::to_string(resultOffset), arrowIndexOffset);
  // RecordProperty("aco-" + std::to_string(resultOffset), arrowChunkOffset);
  if (!st.ok()) {
    FAIL() << "ORC ColumnBatch not successfully filled";
  }
  int64_t lastBatchSize = totalLength - resultOffset;
  EXPECT_EQ(x->numElements, lastBatchSize);
  EXPECT_EQ(a->numElements, 3 * totalLength - valueOffset);
  EXPECT_TRUE(x->hasNulls);
  EXPECT_TRUE(a->hasNulls);
  // RecordProperty("vo-res" + std::to_string(resultOffset), oldValueOffset);
  // RecordProperty("vn-res" + std::to_string(resultOffset), valueOffset);
  for (int64_t i = 0; i < lastBatchSize; i++) {
    // RecordProperty("xn-res" + std::to_string(i + resultOffset), x->notNull[i]);
    // RecordProperty(
    //     "xo-res" + std::to_string(resultOffset) + "-" + std::to_string(i +
    //     resultOffset), x->offsets[i]);
    if ((i + resultOffset) % 3) {
      EXPECT_TRUE(x->notNull[i]);
    } else {
      EXPECT_FALSE(x->notNull[i]);
    }
    EXPECT_EQ(x->offsets[i], 3 * i);
  }
  EXPECT_EQ(x->offsets[lastBatchSize], 3 * lastBatchSize);
  // RecordProperty("xo-res" + std::to_string(resultOffset) + "-" + std::to_string(100),
  //                x->offsets[lastBatchSize]);
  oldValueOffset = valueOffset;
  valueOffset = 3 * totalLength;
  for (int64_t j = 0; j < valueOffset - oldValueOffset; j++) {
    // RecordProperty("an-res" + std::to_string(j + oldValueOffset), a->notNull[j]);
    // RecordProperty("av-res" + std::to_string(j + oldValueOffset), a->data[j]);
    if ((j + oldValueOffset) % 3) {
      EXPECT_FALSE(a->notNull[j]);
    } else {
      EXPECT_TRUE(a->notNull[j]);
      EXPECT_EQ(a->data[j], (j + oldValueOffset) / 3);
    }
  }
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
TEST(TestAdapterWriteNested, writeMapChunkedEmpty) {
  auto sharedPtrArrowType = map(utf8(), utf8());
  ArrayVector av;
  std::shared_ptr<ChunkedArray> carray =
      std::make_shared<ChunkedArray>(av, sharedPtrArrowType);
  DataType* arrowType = sharedPtrArrowType.get();

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
  int64_t arrowIndexOffset = 0;
  int arrowChunkOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowIndexOffset, arrowChunkOffset,
                                       batchSize, carray.get());
  if (!st.ok()) {
    FAIL() << "ORC ColumnBatch not successfully filled";
  }
  EXPECT_EQ(x->numElements, 0);
  EXPECT_FALSE(x->hasNulls);
  EXPECT_EQ(a->numElements, 0);
  EXPECT_FALSE(a->hasNulls);
  EXPECT_EQ(b->numElements, 0);
  EXPECT_FALSE(b->hasNulls);

  writer->add(*batch);
  writer->close();
}
TEST(TestAdapterWriteNested, writeMapChunkedMixed) {
  auto sharedPtrArrowType = map(utf8(), utf8());
  DataType* arrowType = sharedPtrArrowType.get();

  Int32Builder offsetsBuilder0;
  StringBuilder keysBuilder0, itemsBuilder0;
  std::shared_ptr<Array> offsetsArray0, keysArray0, itemsArray0;
  (void)(keysBuilder0.Finish(&keysArray0));
  (void)(itemsBuilder0.Finish(&itemsArray0));
  (void)(offsetsBuilder0.Append(0));
  (void)(offsetsBuilder0.Finish(&offsetsArray0));

  std::shared_ptr<MapArray> array0 = std::static_pointer_cast<MapArray>(
      MapArray::FromArrays(offsetsArray0, keysArray0, itemsArray0).ValueOrDie());

  Int32Builder offsetsBuilder2;
  StringBuilder keysBuilder2, itemsBuilder2;
  std::shared_ptr<Array> offsetsArray2, keysArray2, itemsArray2;
  (void)(keysBuilder2.Finish(&keysArray2));
  (void)(itemsBuilder2.Finish(&itemsArray2));
  (void)(offsetsBuilder2.Append(0));
  (void)(offsetsBuilder2.Finish(&offsetsArray2));

  std::shared_ptr<MapArray> array2 = std::static_pointer_cast<MapArray>(
      MapArray::FromArrays(offsetsArray2, keysArray2, itemsArray2).ValueOrDie());

  Int32Builder offsetsBuilder4;
  StringBuilder keysBuilder4, itemsBuilder4;
  std::shared_ptr<Array> offsetsArray4, keysArray4, itemsArray4;
  (void)(keysBuilder4.Finish(&keysArray4));
  (void)(itemsBuilder4.Finish(&itemsArray4));
  (void)(offsetsBuilder4.Append(0));
  (void)(offsetsBuilder4.Finish(&offsetsArray4));

  std::shared_ptr<MapArray> array4 = std::static_pointer_cast<MapArray>(
      MapArray::FromArrays(offsetsArray4, keysArray4, itemsArray4).ValueOrDie());

  StringBuilder keysBuilder1, itemsBuilder1;
  std::shared_ptr<Array> keysArray1, itemsArray1;

  int32_t offsets1[4] = {0, 1, 3, 3};

  BufferBuilder builder1;
  (void)(builder1.Resize(16));
  (void)(builder1.Append(offsets1, 16));
  std::shared_ptr<arrow::Buffer> offsetsBuffer1;
  if (!builder1.Finish(&offsetsBuffer1).ok()) {
    FAIL() << "The offsets buffer can not be constructed!";
  }

  (void)(keysBuilder1.Append("AB"));
  (void)(keysBuilder1.Append(""));
  (void)(keysBuilder1.Append("AB"));
  (void)(keysBuilder1.Finish(&keysArray1));
  (void)(itemsBuilder1.AppendNull());
  (void)(itemsBuilder1.Append("wdw"));
  (void)(itemsBuilder1.Append("dsw7"));
  (void)(itemsBuilder1.Finish(&itemsArray1));

  uint8_t bitmap1 = 5;  // 00000101
  auto maybeBuffer1 = AllocateBuffer(1);
  if (!maybeBuffer1.ok()) {
    FAIL() << "Buffer not created successfully";
  }
  std::shared_ptr<Buffer> bitmapBuffer1 = *std::move(maybeBuffer1);
  uint8_t* bufferData1 = bitmapBuffer1->mutable_data();
  std::memcpy(bufferData1, &bitmap1, 1);

  auto array1 = std::make_shared<MapArray>(sharedPtrArrowType, 3, offsetsBuffer1,
                                           keysArray1, itemsArray1, bitmapBuffer1);

  StringBuilder keysBuilder3, itemsBuilder3;
  std::shared_ptr<Array> keysArray3, itemsArray3;

  int32_t offsets3[4] = {0, 0, 4, 4};

  BufferBuilder builder3;
  (void)(builder3.Resize(16));
  (void)(builder3.Append(offsets3, 16));
  std::shared_ptr<arrow::Buffer> offsetsBuffer3;
  if (!builder3.Finish(&offsetsBuffer3).ok()) {
    FAIL() << "The offsets buffer can not be constructed!";
  }

  (void)(keysBuilder3.Append("BC"));
  (void)(keysBuilder3.Append(""));
  (void)(keysBuilder3.Append("CED"));
  (void)(keysBuilder3.Append("F"));
  (void)(keysBuilder3.Finish(&keysArray3));
  (void)(itemsBuilder3.AppendNull());
  (void)(itemsBuilder3.Append("wwd"));
  (void)(itemsBuilder3.Append("ds7w"));
  (void)(itemsBuilder3.Append("uv"));
  (void)(itemsBuilder3.Finish(&itemsArray3));

  uint8_t bitmap3 = 3;  // 00000011
  auto maybeBuffer3 = AllocateBuffer(1);
  if (!maybeBuffer3.ok()) {
    FAIL() << "Buffer not created successfully";
  }
  std::shared_ptr<Buffer> bitmapBuffer3 = *std::move(maybeBuffer3);
  uint8_t* bufferData3 = bitmapBuffer3->mutable_data();
  std::memcpy(bufferData3, &bitmap3, 1);

  auto array3 = std::make_shared<MapArray>(sharedPtrArrowType, 3, offsetsBuffer3,
                                           keysArray3, itemsArray3, bitmapBuffer3);

  ArrayVector av;
  av.push_back(array0);
  av.push_back(array1);
  av.push_back(array2);
  av.push_back(array3);
  av.push_back(array4);
  std::shared_ptr<ChunkedArray> carray = std::make_shared<ChunkedArray>(av);

  // RecordProperty("l", carray->length());
  // RecordProperty("l0", carray->chunk(0)->length());
  // RecordProperty("l1", carray->chunk(1)->length());
  // RecordProperty("l2", carray->chunk(2)->length());
  // RecordProperty("l3", carray->chunk(3)->length());
  // RecordProperty("l4", carray->chunk(4)->length());

  // auto a1 = std::static_pointer_cast<ListArray>(carray->chunk(1));
  // auto a11 = std::static_pointer_cast<Int32Array>(a1->values());
  // RecordProperty("l11", a11->length());
  // auto a3 = std::static_pointer_cast<MapArray>(carray->chunk(3));
  // auto a31 = std::static_pointer_cast<Int32Array>(a3->values());
  // RecordProperty("l31", a31->length());

  // for (int i = 0; i < 2; i++) {
  //   RecordProperty("xn" + std::to_string(i), a1->IsNull(i));
  // }
  // for (int i = 0; i < 2; i++) {
  //   RecordProperty("xn" + std::to_string(i + 2), a3->IsNull(i));
  // }
  // for (int i = 0; i < 3; i++) {
  //   RecordProperty("v" + std::to_string(i), a11->Value(i));
  //   RecordProperty("an" + std::to_string(i), a11->IsNull(i));
  // }
  // for (int i = 0; i < 4; i++) {
  //   RecordProperty("v" + std::to_string(i + 3), a31->Value(i));
  //   RecordProperty("an" + std::to_string(i + 3), a31->IsNull(i));
  // }

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
  int64_t arrowIndexOffset = 0;
  int arrowChunkOffset = 0;

  Status st = adapters::orc::FillBatch(arrowType, x, arrowIndexOffset, arrowChunkOffset,
                                       batchSize, carray.get());
  if (!st.ok()) {
    FAIL() << "ORC ColumnBatch not successfully filled";
  }

  EXPECT_EQ(x->numElements, 6);
  EXPECT_TRUE(x->hasNulls);
  EXPECT_EQ(a->numElements, 7);
  EXPECT_FALSE(a->hasNulls);
  EXPECT_EQ(b->numElements, 7);
  EXPECT_TRUE(b->hasNulls);

  EXPECT_EQ(x->notNull[0], 1);
  EXPECT_EQ(x->notNull[1], 0);
  EXPECT_EQ(x->notNull[2], 1);
  EXPECT_EQ(x->notNull[3], 1);
  EXPECT_EQ(x->notNull[4], 1);
  EXPECT_EQ(x->notNull[5], 0);
  EXPECT_EQ(b->notNull[0], 0);
  EXPECT_EQ(b->notNull[1], 1);
  EXPECT_EQ(b->notNull[2], 1);
  EXPECT_EQ(b->notNull[3], 0);
  EXPECT_EQ(b->notNull[4], 1);
  EXPECT_EQ(b->notNull[5], 1);
  EXPECT_EQ(b->notNull[6], 1);

  EXPECT_EQ(x->offsets[0], 0);
  EXPECT_EQ(x->offsets[1], 1);
  EXPECT_EQ(x->offsets[2], 3);
  EXPECT_EQ(x->offsets[3], 3);
  EXPECT_EQ(x->offsets[4], 3);
  EXPECT_EQ(x->offsets[5], 7);
  EXPECT_EQ(x->offsets[6], 7);

  EXPECT_EQ(a->length[0], 2);
  EXPECT_EQ(a->length[1], 0);
  EXPECT_EQ(a->length[2], 2);
  EXPECT_EQ(a->length[3], 2);
  EXPECT_EQ(a->length[4], 0);
  EXPECT_EQ(a->length[5], 3);
  EXPECT_EQ(a->length[6], 1);
  EXPECT_EQ(b->length[1], 3);
  EXPECT_EQ(b->length[2], 4);
  EXPECT_EQ(b->length[4], 3);
  EXPECT_EQ(b->length[5], 4);
  EXPECT_EQ(b->length[6], 2);

  EXPECT_STREQ(a->data[0], "AB");
  EXPECT_STREQ(a->data[1], "");
  EXPECT_STREQ(a->data[2], "AB");
  EXPECT_STREQ(a->data[3], "BC");
  EXPECT_STREQ(a->data[4], "");
  EXPECT_STREQ(a->data[5], "CED");
  EXPECT_STREQ(a->data[6], "F");
  EXPECT_STREQ(b->data[1], "wdw");
  EXPECT_STREQ(b->data[2], "dsw7");
  EXPECT_STREQ(b->data[4], "wwd");
  EXPECT_STREQ(b->data[5], "ds7w");
  EXPECT_STREQ(b->data[6], "uv");
}
TEST(TestAdapterWriteNested, writeMapChunkedMultibatch) {
  auto sharedPtrArrowType = map(utf8(), utf8());
  DataType* arrowType = sharedPtrArrowType.get();
  int64_t totalLength = 100;
  int64_t totalElementLength = 150;
  int64_t batchSize = 14;

  Int32Builder offsetsBuilder0;
  StringBuilder keysBuilder0, itemsBuilder0;
  std::shared_ptr<Array> offsetsArray0, keysArray0, itemsArray0;
  (void)(keysBuilder0.Finish(&keysArray0));
  (void)(itemsBuilder0.Finish(&itemsArray0));
  (void)(offsetsBuilder0.Append(0));
  (void)(offsetsBuilder0.Finish(&offsetsArray0));

  std::shared_ptr<MapArray> array0 = std::static_pointer_cast<MapArray>(
      MapArray::FromArrays(offsetsArray0, keysArray0, itemsArray0).ValueOrDie());

  Int32Builder offsetsBuilder2;
  StringBuilder keysBuilder2, itemsBuilder2;
  std::shared_ptr<Array> offsetsArray2, keysArray2, itemsArray2;
  (void)(keysBuilder2.Finish(&keysArray2));
  (void)(itemsBuilder2.Finish(&itemsArray2));
  (void)(offsetsBuilder2.Append(0));
  (void)(offsetsBuilder2.Finish(&offsetsArray2));

  std::shared_ptr<MapArray> array2 = std::static_pointer_cast<MapArray>(
      MapArray::FromArrays(offsetsArray2, keysArray2, itemsArray2).ValueOrDie());

  Int32Builder offsetsBuilder4;
  StringBuilder keysBuilder4, itemsBuilder4;
  std::shared_ptr<Array> offsetsArray4, keysArray4, itemsArray4;
  (void)(keysBuilder4.Finish(&keysArray4));
  (void)(itemsBuilder4.Finish(&itemsArray4));
  (void)(offsetsBuilder4.Append(0));
  (void)(offsetsBuilder4.Finish(&offsetsArray4));

  std::shared_ptr<MapArray> array4 = std::static_pointer_cast<MapArray>(
      MapArray::FromArrays(offsetsArray4, keysArray4, itemsArray4).ValueOrDie());

  StringBuilder keysBuilder1, itemsBuilder1, keysBuilder3, itemsBuilder3;
  int32_t offset1[51], offset3[51];
  int32_t key = 0;
  offset1[0] = 0;
  for (int i = 0; i < 50; i++) {
    switch (i % 4) {
      case 0: {
        offset1[i + 1] = offset1[i];
        break;
      }
      case 1: {
        (void)(keysBuilder1.Append("Key" + std::to_string(key)));
        (void)(itemsBuilder1.Append("Item" + std::to_string(i - 1)));
        key++;
        offset1[i + 1] = offset1[i] + 1;
        break;
      }
      case 2: {
        (void)(keysBuilder1.Append("Key" + std::to_string(key)));
        (void)(keysBuilder1.Append("Key" + std::to_string(key + 1)));
        (void)(itemsBuilder1.AppendNull());
        (void)(itemsBuilder1.AppendNull());
        key += 2;
        offset1[i + 1] = offset1[i] + 2;
        break;
      }
      default: {
        (void)(keysBuilder1.Append("Key" + std::to_string(key)));
        (void)(keysBuilder1.Append("Key" + std::to_string(key + 1)));
        (void)(keysBuilder1.Append("Key" + std::to_string(key + 2)));
        (void)(itemsBuilder1.Append("Item" + std::to_string(i - 1)));
        (void)(itemsBuilder1.AppendNull());
        (void)(itemsBuilder1.AppendNull());
        key += 3;
        offset1[i + 1] = offset1[i] + 3;
      }
    }
  }

  offset3[0] = 0;
  for (int i = 0; i < 50; i++) {
    switch ((i + 50) % 4) {
      case 0: {
        offset3[i + 1] = offset3[i];
        break;
      }
      case 1: {
        (void)(keysBuilder3.Append("Key" + std::to_string(key)));
        (void)(itemsBuilder3.Append("Item" + std::to_string(i + 50 - 1)));
        key++;
        offset3[i + 1] = offset3[i] + 1;
        break;
      }
      case 2: {
        (void)(keysBuilder3.Append("Key" + std::to_string(key)));
        (void)(keysBuilder3.Append("Key" + std::to_string(key + 1)));
        (void)(itemsBuilder3.AppendNull());
        (void)(itemsBuilder3.AppendNull());
        key += 2;
        offset3[i + 1] = offset3[i] + 2;
        break;
      }
      default: {
        (void)(keysBuilder3.Append("Key" + std::to_string(key)));
        (void)(keysBuilder3.Append("Key" + std::to_string(key + 1)));
        (void)(keysBuilder3.Append("Key" + std::to_string(key + 2)));
        (void)(itemsBuilder3.Append("Item" + std::to_string(i + 50 - 1)));
        (void)(itemsBuilder3.AppendNull());
        (void)(itemsBuilder3.AppendNull());
        key += 3;
        offset3[i + 1] = offset3[i] + 3;
      }
    }
  }

  // Every third entry has null at struct level
  uint8_t bitmap1[7] = {
      182, 109, 219, 182,
      109, 219, 2};  // 10110110 01101101 11011011 10110110 01101101 11011011 00000010
  uint8_t bitmap3[7] = {
      109, 219, 182, 109,
      219, 182, 1};  // 01101101 11011011 10110110 01101101 11011011 10110110 00000001

  BufferBuilder builder1, builder3, offsetsBuilder1, offsetsBuilder3;
  (void)(builder1.Resize(7));
  (void)(builder1.Append(bitmap1, 7));
  std::shared_ptr<arrow::Buffer> bitmapBuffer1;
  if (!builder1.Finish(&bitmapBuffer1).ok()) {
    FAIL() << "The offsets buffer can not be constructed!";
  }
  (void)(builder3.Resize(7));
  (void)(builder3.Append(bitmap3, 7));
  std::shared_ptr<arrow::Buffer> bitmapBuffer3;
  if (!builder3.Finish(&bitmapBuffer3).ok()) {
    FAIL() << "The offsets buffer can not be constructed!";
  }

  (void)(offsetsBuilder1.Resize(204));
  (void)(offsetsBuilder1.Append(offset1, 204));
  std::shared_ptr<arrow::Buffer> offsetsBuffer1;
  if (!offsetsBuilder1.Finish(&offsetsBuffer1).ok()) {
    FAIL() << "The offsets buffer can not be constructed!";
  }
  (void)(offsetsBuilder3.Resize(204));
  (void)(offsetsBuilder3.Append(offset3, 204));
  std::shared_ptr<arrow::Buffer> offsetsBuffer3;
  if (!offsetsBuilder3.Finish(&offsetsBuffer3).ok()) {
    FAIL() << "The offsets buffer can not be constructed!";
  }

  std::shared_ptr<Array> keysArray1, keysArray3, itemsArray1, itemsArray3;
  (void)(keysBuilder1.Finish(&keysArray1));
  (void)(keysBuilder3.Finish(&keysArray3));
  (void)(itemsBuilder1.Finish(&itemsArray1));
  (void)(itemsBuilder3.Finish(&itemsArray3));

  auto array1 = std::make_shared<MapArray>(sharedPtrArrowType, 50, offsetsBuffer1,
                                           keysArray1, itemsArray1, bitmapBuffer1);
  auto array3 = std::make_shared<MapArray>(sharedPtrArrowType, 50, offsetsBuffer3,
                                           keysArray3, itemsArray3, bitmapBuffer3);

  ArrayVector av;
  av.push_back(array0);
  av.push_back(array1);
  av.push_back(array2);
  av.push_back(array3);
  av.push_back(array4);
  std::shared_ptr<ChunkedArray> carray = std::make_shared<ChunkedArray>(av);

  // RecordProperty("l", carray->length());
  // RecordProperty("l0", carray->chunk(0)->length());
  // RecordProperty("l1", carray->chunk(1)->length());
  // RecordProperty("l2", carray->chunk(2)->length());
  // RecordProperty("l3", carray->chunk(3)->length());
  // RecordProperty("l4", carray->chunk(4)->length());

  // auto a1 = std::static_pointer_cast<MapArray>(carray->chunk(1));
  // auto a1k = std::static_pointer_cast<StringArray>(a1->keys());
  // auto a1i = std::static_pointer_cast<StringArray>(a1->items());
  // RecordProperty("l1k", a1k->length());
  // RecordProperty("l1i", a1i->length());
  // auto a3 = std::static_pointer_cast<MapArray>(carray->chunk(3));
  // auto a3k = std::static_pointer_cast<StringArray>(a3->keys());
  // auto a3i = std::static_pointer_cast<StringArray>(a3->items());
  // RecordProperty("l3k", a3k->length());
  // RecordProperty("l3i", a3i->length());

  // for (int i = 0; i < 50; i++) {
  //   RecordProperty("xn" + std::to_string(i), a1->IsNull(i));
  //   RecordProperty("xo" + std::to_string(i), a1->value_offset(i));
  // }
  // for (int i = 0; i < 50; i++) {
  //   RecordProperty("xn" + std::to_string(i + 50), a3->IsNull(i));
  //   RecordProperty("xo" + std::to_string(i + 50), a3->value_offset(i));
  // }
  // for (int i = 0; i < 73; i++) {
  //   RecordProperty("av" + std::to_string(i), a1k->GetString(i));
  //   RecordProperty("an" + std::to_string(i), a1k->IsNull(i));
  //   RecordProperty("bv" + std::to_string(i), a1i->GetString(i));
  //   RecordProperty("bn" + std::to_string(i), a1i->IsNull(i));
  // }
  // for (int i = 0; i < 77; i++) {
  //   RecordProperty("av" + std::to_string(i + 73), a3k->GetString(i));
  //   RecordProperty("an" + std::to_string(i + 73), a3k->IsNull(i));
  //   RecordProperty("bv" + std::to_string(i + 73), a3i->GetString(i));
  //   RecordProperty("bn" + std::to_string(i + 73), a3i->IsNull(i));
  // }

  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE * 10);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:map<a:string,b:string>>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::MapVectorBatch* x =
      internal::checked_cast<liborc::MapVectorBatch*>(root->fields[0]);
  liborc::StringVectorBatch* a =
      internal::checked_cast<liborc::StringVectorBatch*>((x->keys).get());
  liborc::StringVectorBatch* b =
      internal::checked_cast<liborc::StringVectorBatch*>((x->elements).get());
  int64_t arrowIndexOffset = 0;
  int arrowChunkOffset = 0;
  int64_t resultOffset = 0;
  int64_t oldValueOffset = 0, valueOffset = 0;
  while (resultOffset < totalLength - batchSize) {
    Status st = adapters::orc::FillBatch(arrowType, x, arrowIndexOffset, arrowChunkOffset,
                                         batchSize, carray.get());
    if (!st.ok()) {
      FAIL() << "ORC ColumnBatch not successfully filled";
    }
    writer->add(*batch);
    // RecordProperty("aio-" + std::to_string(resultOffset), arrowIndexOffset);
    // RecordProperty("aco-" + std::to_string(resultOffset), arrowChunkOffset);
    for (int64_t i = 0; i < batchSize; i++) {
      // RecordProperty("xn-res" + std::to_string(i + resultOffset), x->notNull[i]);
      // RecordProperty("xo-res" + std::to_string(resultOffset) + "-" +
      //                    std::to_string(i + resultOffset),
      //                x->offsets[i]);
      if ((i + resultOffset) % 3) {
        EXPECT_TRUE(x->notNull[i]);
      } else {
        EXPECT_FALSE(x->notNull[i]);
      }
      EXPECT_EQ(x->offsets[i], testListOffsetGenerator(i + resultOffset) -
                                   testListOffsetGenerator(resultOffset));
    }
    EXPECT_EQ(x->offsets[batchSize], testListOffsetGenerator(batchSize + resultOffset) -
                                         testListOffsetGenerator(resultOffset));
    // RecordProperty("xo-res" + std::to_string(resultOffset) + "-" +
    //                    std::to_string(batchSize + resultOffset),
    //                x->offsets[batchSize]);
    resultOffset = resultOffset + batchSize;
    oldValueOffset = valueOffset;
    valueOffset = testListOffsetGenerator(resultOffset);
    // RecordProperty("vo-res" + std::to_string(resultOffset), oldValueOffset);
    // RecordProperty("vn-res" + std::to_string(resultOffset), valueOffset);
    for (int64_t j = 0; j < valueOffset - oldValueOffset; j++) {
      // RecordProperty("an-res" + std::to_string(j + oldValueOffset), a->notNull[j]);
      // RecordProperty("av-res" + std::to_string(j + oldValueOffset), a->data[j]);
      EXPECT_EQ(a->data[j], "Key" + std::to_string(j + oldValueOffset));
      if (j + oldValueOffset >= 100) {
        EXPECT_EQ(a->length[j], 6);
      } else if (j + oldValueOffset >= 10) {
        EXPECT_EQ(a->length[j], 5);
      } else {
        EXPECT_EQ(a->length[j], 4);
      }
      if ((j + oldValueOffset) % 3) {
        EXPECT_FALSE(b->notNull[j]);
      } else {
        EXPECT_TRUE(b->notNull[j]);
        EXPECT_EQ(b->data[j], "Item" + std::to_string((j + oldValueOffset) * 2 / 3));
        if ((j + oldValueOffset) >= 15) {
          EXPECT_EQ(b->length[j], 6);
        } else {
          EXPECT_EQ(b->length[j], 5);
        }
      }
    }
    EXPECT_EQ(x->numElements, batchSize);
    EXPECT_EQ(a->numElements, valueOffset - oldValueOffset);
    EXPECT_EQ(b->numElements, valueOffset - oldValueOffset);
    EXPECT_TRUE(x->hasNulls);
    EXPECT_FALSE(a->hasNulls);
    EXPECT_TRUE(b->hasNulls);
    batch->clear();
  }
  Status st = adapters::orc::FillBatch(arrowType, x, arrowIndexOffset, arrowChunkOffset,
                                       batchSize, carray.get());

  // RecordProperty("aio-" + std::to_string(resultOffset), arrowIndexOffset);
  // RecordProperty("aco-" + std::to_string(resultOffset), arrowChunkOffset);
  if (!st.ok()) {
    FAIL() << "ORC ColumnBatch not successfully filled";
  }
  int64_t lastBatchSize = totalLength - resultOffset;
  EXPECT_EQ(x->numElements, lastBatchSize);
  EXPECT_EQ(a->numElements, totalElementLength - valueOffset);
  EXPECT_EQ(b->numElements, totalElementLength - valueOffset);
  EXPECT_TRUE(x->hasNulls);
  EXPECT_FALSE(a->hasNulls);
  EXPECT_TRUE(b->hasNulls);
  // RecordProperty("vo-res" + std::to_string(resultOffset), oldValueOffset);
  // RecordProperty("vn-res" + std::to_string(resultOffset), valueOffset);
  for (int64_t i = 0; i < lastBatchSize; i++) {
    // RecordProperty("xn-res" + std::to_string(i + resultOffset), x->notNull[i]);
    // RecordProperty(
    //     "xo-res" + std::to_string(resultOffset) + "-" + std::to_string(i +
    //     resultOffset), x->offsets[i]);
    if ((i + resultOffset) % 3) {
      EXPECT_TRUE(x->notNull[i]);
    } else {
      EXPECT_FALSE(x->notNull[i]);
    }
    EXPECT_EQ(x->offsets[i], testListOffsetGenerator(i + resultOffset) -
                                 testListOffsetGenerator(resultOffset));
  }
  EXPECT_EQ(x->offsets[lastBatchSize],
            totalElementLength - testListOffsetGenerator(resultOffset));
  // RecordProperty("xo-res" + std::to_string(resultOffset) + "-" + std::to_string(100),
  //                x->offsets[lastBatchSize]);
  oldValueOffset = valueOffset;
  valueOffset = totalElementLength;
  for (int64_t j = 0; j < valueOffset - oldValueOffset; j++) {
    // RecordProperty("an-res" + std::to_string(j + oldValueOffset), a->notNull[j]);
    // RecordProperty("av-res" + std::to_string(j + oldValueOffset), a->data[j]);
    EXPECT_EQ(a->data[j], "Key" + std::to_string(j + oldValueOffset));
    if (j + oldValueOffset >= 100) {
      EXPECT_EQ(a->length[j], 6);
    } else if (j + oldValueOffset >= 10) {
      EXPECT_EQ(a->length[j], 5);
    } else {
      EXPECT_EQ(a->length[j], 4);
    }
    if ((j + oldValueOffset) % 3) {
      EXPECT_FALSE(b->notNull[j]);
    } else {
      EXPECT_TRUE(b->notNull[j]);
      EXPECT_EQ(b->data[j], "Item" + std::to_string((j + oldValueOffset) * 2 / 3));
      if ((j + oldValueOffset) >= 15) {
        EXPECT_EQ(b->length[j], 6);
      } else {
        EXPECT_EQ(b->length[j], 5);
      }
    }
  }
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

  auto array =
      std::make_shared<DenseUnionArray>(sharedPtrArrowType, 0, children, NULLPTR);

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
TEST(TestAdapterWriteNested, writeDenseUnionNoNulls1) {
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

  EXPECT_EQ(x->tags[0], 0);
  EXPECT_EQ(x->tags[1], 1);
  EXPECT_EQ(x->tags[2], 1);
  EXPECT_EQ(x->tags[3], 0);
  EXPECT_EQ(x->tags[4], 0);
  EXPECT_EQ(x->tags[5], 0);
  EXPECT_EQ(x->tags[6], 1);
  EXPECT_EQ(x->tags[7], 1);

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

  EXPECT_EQ(arrowOffset, 8);
  EXPECT_EQ(orcOffset, 8);
  writer->add(*batch);
  writer->close();
}
TEST(TestAdapterWriteNested, writeDenseUnionNoNulls2) {
  std::vector<std::shared_ptr<Field>> xFields;
  xFields.push_back(std::make_shared<Field>("a", utf8()));
  xFields.push_back(std::make_shared<Field>("b", int32()));
  auto sharedPtrArrowType = dense_union(xFields);
  DataType* arrowType = sharedPtrArrowType.get();

  StringBuilder builder1;
  (void)(builder1.Append("A"));
  (void)(builder1.Append("AB"));
  (void)(builder1.Append(""));

  Int32Builder builder2;
  (void)(builder2.Append(7));
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

  int8_t type_ids[8] = {1, 1, 1, 0, 0, 0, 1, 1};
  int32_t valueOffsets[8] = {0, 1, 2, 0, 1, 2, 3, 4};

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
  EXPECT_EQ(a->numElements, 3);
  EXPECT_FALSE(a->hasNulls);
  EXPECT_EQ(b->numElements, 5);
  EXPECT_FALSE(b->hasNulls);

  EXPECT_EQ(x->offsets[0], 0);
  EXPECT_EQ(x->offsets[1], 1);
  EXPECT_EQ(x->offsets[2], 2);
  EXPECT_EQ(x->offsets[3], 3);
  EXPECT_EQ(x->offsets[4], 4);
  EXPECT_EQ(x->offsets[5], 5);
  EXPECT_EQ(x->offsets[6], 6);
  EXPECT_EQ(x->offsets[7], 7);

  EXPECT_EQ(x->tags[0], 1);
  EXPECT_EQ(x->tags[1], 1);
  EXPECT_EQ(x->tags[2], 1);
  EXPECT_EQ(x->tags[3], 0);
  EXPECT_EQ(x->tags[4], 0);
  EXPECT_EQ(x->tags[5], 0);
  EXPECT_EQ(x->tags[6], 1);
  EXPECT_EQ(x->tags[7], 1);

  EXPECT_STREQ(a->data[0], "A");
  EXPECT_STREQ(a->data[1], "AB");
  EXPECT_STREQ(a->data[2], "");
  EXPECT_EQ(a->length[0], 1);
  EXPECT_EQ(a->length[1], 2);
  EXPECT_EQ(a->length[2], 0);

  EXPECT_EQ(b->data[0], 7);
  EXPECT_EQ(b->data[1], 3);
  EXPECT_EQ(b->data[2], -12);
  EXPECT_EQ(b->data[3], 25);
  EXPECT_EQ(b->data[4], 76);

  EXPECT_EQ(arrowOffset, 8);
  EXPECT_EQ(orcOffset, 8);
  writer->add(*batch);
  writer->close();
}
TEST(TestAdapterWriteNested, writeDenseUnionNoNulls3) {
  std::vector<std::shared_ptr<Field>> xFields;
  xFields.push_back(std::make_shared<Field>("a", utf8()));
  xFields.push_back(std::make_shared<Field>("b", int32()));
  auto sharedPtrArrowType = dense_union(xFields);
  DataType* arrowType = sharedPtrArrowType.get();

  StringBuilder builder1;

  Int32Builder builder2;
  (void)(builder2.Append(7));
  (void)(builder2.Append(3));
  (void)(builder2.Append(-12));
  (void)(builder2.Append(25));
  (void)(builder2.Append(76));
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

  int8_t type_ids[8] = {1, 1, 1, 1, 1, 1, 1, 1};
  int32_t valueOffsets[8] = {0, 1, 2, 3, 4, 5, 6, 7};

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
  EXPECT_EQ(a->numElements, 0);
  EXPECT_FALSE(a->hasNulls);
  EXPECT_EQ(b->numElements, 8);
  EXPECT_FALSE(b->hasNulls);

  EXPECT_EQ(x->offsets[0], 0);
  EXPECT_EQ(x->offsets[1], 1);
  EXPECT_EQ(x->offsets[2], 2);
  EXPECT_EQ(x->offsets[3], 3);
  EXPECT_EQ(x->offsets[4], 4);
  EXPECT_EQ(x->offsets[5], 5);
  EXPECT_EQ(x->offsets[6], 6);
  EXPECT_EQ(x->offsets[7], 7);

  EXPECT_EQ(x->tags[0], 1);
  EXPECT_EQ(x->tags[1], 1);
  EXPECT_EQ(x->tags[2], 1);
  EXPECT_EQ(x->tags[3], 1);
  EXPECT_EQ(x->tags[4], 1);
  EXPECT_EQ(x->tags[5], 1);
  EXPECT_EQ(x->tags[6], 1);
  EXPECT_EQ(x->tags[7], 1);

  EXPECT_EQ(b->data[0], 7);
  EXPECT_EQ(b->data[1], 3);
  EXPECT_EQ(b->data[2], -12);
  EXPECT_EQ(b->data[3], 25);
  EXPECT_EQ(b->data[4], 76);
  EXPECT_EQ(b->data[5], -12);
  EXPECT_EQ(b->data[6], 25);
  EXPECT_EQ(b->data[7], 76);

  EXPECT_EQ(arrowOffset, 8);
  EXPECT_EQ(orcOffset, 8);
  writer->add(*batch);
  writer->close();
}
TEST(TestAdapterWriteNested, writeDenseUnionAllNulls1) {
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

  EXPECT_EQ(x->tags[0], 0);
  EXPECT_EQ(x->tags[1], 0);
  EXPECT_EQ(x->tags[2], 0);
  EXPECT_EQ(x->tags[3], 0);
  EXPECT_EQ(x->tags[4], 1);
  EXPECT_EQ(x->tags[5], 1);
  EXPECT_EQ(x->tags[6], 1);
  EXPECT_EQ(x->tags[7], 1);

  EXPECT_EQ(arrowOffset, 8);
  EXPECT_EQ(orcOffset, 8);
  writer->add(*batch);
  writer->close();
}
TEST(TestAdapterWriteNested, writeDenseUnionAllNulls2) {
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
  (void)(builder1.AppendNull());
  (void)(builder1.AppendNull());
  (void)(builder1.AppendNull());
  (void)(builder1.AppendNull());

  Int32Builder builder2;

  std::shared_ptr<Array> array1;
  (void)(builder1.Finish(&array1));
  std::shared_ptr<Array> array2;
  (void)(builder2.Finish(&array2));

  std::vector<std::shared_ptr<Array>> children;
  children.push_back(array1);
  children.push_back(array2);

  int8_t type_ids[8] = {0, 0, 0, 0, 0, 0, 0, 0};
  int32_t valueOffsets[8] = {0, 1, 2, 3, 4, 5, 6, 7};

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
  EXPECT_EQ(a->numElements, 8);
  EXPECT_TRUE(a->hasNulls);
  EXPECT_EQ(b->numElements, 0);
  EXPECT_FALSE(b->hasNulls);

  EXPECT_EQ(a->notNull[0], 0);
  EXPECT_EQ(a->notNull[1], 0);
  EXPECT_EQ(a->notNull[2], 0);
  EXPECT_EQ(a->notNull[3], 0);
  EXPECT_EQ(a->notNull[4], 0);
  EXPECT_EQ(a->notNull[5], 0);
  EXPECT_EQ(a->notNull[6], 0);
  EXPECT_EQ(a->notNull[7], 0);

  EXPECT_EQ(x->offsets[0], 0);
  EXPECT_EQ(x->offsets[1], 1);
  EXPECT_EQ(x->offsets[2], 2);
  EXPECT_EQ(x->offsets[3], 3);
  EXPECT_EQ(x->offsets[4], 4);
  EXPECT_EQ(x->offsets[5], 5);
  EXPECT_EQ(x->offsets[6], 6);
  EXPECT_EQ(x->offsets[7], 7);

  EXPECT_EQ(x->tags[0], 0);
  EXPECT_EQ(x->tags[1], 0);
  EXPECT_EQ(x->tags[2], 0);
  EXPECT_EQ(x->tags[3], 0);
  EXPECT_EQ(x->tags[4], 0);
  EXPECT_EQ(x->tags[5], 0);
  EXPECT_EQ(x->tags[6], 0);
  EXPECT_EQ(x->tags[7], 0);

  EXPECT_EQ(arrowOffset, 8);
  EXPECT_EQ(orcOffset, 8);
  writer->add(*batch);
  writer->close();
}
TEST(TestAdapterWriteNested, writeDenseUnionAllNulls3) {
  std::vector<std::shared_ptr<Field>> xFields;
  xFields.push_back(std::make_shared<Field>("a", utf8()));
  xFields.push_back(std::make_shared<Field>("b", int32()));
  auto sharedPtrArrowType = dense_union(xFields);
  DataType* arrowType = sharedPtrArrowType.get();

  StringBuilder builder1;
  (void)(builder1.AppendNull());

  Int32Builder builder2;
  (void)(builder2.AppendNull());
  (void)(builder2.AppendNull());
  (void)(builder2.AppendNull());
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

  int8_t type_ids[8] = {1, 1, 1, 0, 1, 1, 1, 1};
  int32_t valueOffsets[8] = {0, 1, 2, 0, 3, 4, 5, 6};

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
  EXPECT_EQ(a->numElements, 1);
  EXPECT_TRUE(a->hasNulls);
  EXPECT_EQ(b->numElements, 7);
  EXPECT_TRUE(b->hasNulls);

  EXPECT_EQ(a->notNull[0], 0);
  EXPECT_EQ(b->notNull[0], 0);
  EXPECT_EQ(b->notNull[1], 0);
  EXPECT_EQ(b->notNull[2], 0);
  EXPECT_EQ(b->notNull[3], 0);
  EXPECT_EQ(b->notNull[4], 0);
  EXPECT_EQ(b->notNull[5], 0);
  EXPECT_EQ(b->notNull[6], 0);

  EXPECT_EQ(x->offsets[0], 0);
  EXPECT_EQ(x->offsets[1], 1);
  EXPECT_EQ(x->offsets[2], 2);
  EXPECT_EQ(x->offsets[3], 3);
  EXPECT_EQ(x->offsets[4], 4);
  EXPECT_EQ(x->offsets[5], 5);
  EXPECT_EQ(x->offsets[6], 6);
  EXPECT_EQ(x->offsets[7], 7);

  EXPECT_EQ(x->tags[0], 1);
  EXPECT_EQ(x->tags[1], 1);
  EXPECT_EQ(x->tags[2], 1);
  EXPECT_EQ(x->tags[3], 0);
  EXPECT_EQ(x->tags[4], 1);
  EXPECT_EQ(x->tags[5], 1);
  EXPECT_EQ(x->tags[6], 1);
  EXPECT_EQ(x->tags[7], 1);

  EXPECT_EQ(arrowOffset, 8);
  EXPECT_EQ(orcOffset, 8);
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

  int8_t type_ids[8] = {1, 1, 1, 0, 0, 0, 0, 1};
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

  EXPECT_EQ(x->tags[0], 1);
  EXPECT_EQ(x->tags[1], 1);
  EXPECT_EQ(x->tags[2], 1);
  EXPECT_EQ(x->tags[3], 0);
  EXPECT_EQ(x->tags[4], 0);
  EXPECT_EQ(x->tags[5], 0);
  EXPECT_EQ(x->tags[6], 0);
  EXPECT_EQ(x->tags[7], 1);

  EXPECT_STREQ(a->data[0], "A");
  EXPECT_STREQ(a->data[2], "");
  EXPECT_STREQ(a->data[3], "ABCD");
  EXPECT_EQ(a->length[0], 1);
  EXPECT_EQ(a->length[2], 0);
  EXPECT_EQ(a->length[3], 4);

  EXPECT_EQ(b->data[0], 3);
  EXPECT_EQ(b->data[1], -12);
  EXPECT_EQ(b->data[2], 25);

  EXPECT_EQ(arrowOffset, 8);
  EXPECT_EQ(orcOffset, 8);
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

  EXPECT_EQ(x->tags[0], 0);
  EXPECT_EQ(x->tags[1], 0);
  EXPECT_EQ(x->tags[2], 2);
  EXPECT_EQ(x->tags[3], 0);
  EXPECT_EQ(x->tags[4], 1);
  EXPECT_EQ(x->tags[5], 1);
  EXPECT_EQ(x->tags[6], 2);
  EXPECT_EQ(x->tags[7], 1);
  EXPECT_EQ(x->tags[8], 1);
  EXPECT_EQ(x->tags[9], 0);
  EXPECT_EQ(x->tags[10], 2);
  EXPECT_EQ(x->tags[11], 2);

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

  EXPECT_EQ(arrowOffset, 12);
  EXPECT_EQ(orcOffset, 12);
  writer->add(*batch);
  writer->close();
}
TEST(TestAdapterWriteNested, writeDenseUnionMixed3) {
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
  (void)(builder1.Append("AA"));
  (void)(builder1.Append(""));
  (void)(builder1.AppendNull());

  Int32Builder builder2;
  (void)(builder2.Append(3));
  (void)(builder2.Append(25));
  (void)(builder2.AppendNull());

  DoubleBuilder builder3;
  (void)(builder3.Append(3.7));
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

  int8_t type_ids[12] = {0, 0, 0, 0, 1, 1, 2, 1, 0, 0, 2, 0};
  int32_t valueOffsets[12] = {0, 1, 2, 3, 0, 1, 0, 2, 4, 5, 1, 6};

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
  EXPECT_EQ(a->numElements, 7);
  EXPECT_TRUE(a->hasNulls);
  EXPECT_EQ(b->numElements, 3);
  EXPECT_TRUE(b->hasNulls);
  EXPECT_EQ(c->numElements, 2);
  EXPECT_TRUE(c->hasNulls);

  EXPECT_EQ(a->notNull[0], 1);
  EXPECT_EQ(a->notNull[1], 0);
  EXPECT_EQ(a->notNull[2], 1);
  EXPECT_EQ(a->notNull[3], 1);
  EXPECT_EQ(a->notNull[4], 1);
  EXPECT_EQ(a->notNull[5], 1);
  EXPECT_EQ(a->notNull[6], 0);
  EXPECT_EQ(b->notNull[0], 1);
  EXPECT_EQ(b->notNull[1], 1);
  EXPECT_EQ(b->notNull[2], 0);
  EXPECT_EQ(c->notNull[0], 1);
  EXPECT_EQ(c->notNull[1], 0);

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

  EXPECT_EQ(x->tags[0], 0);
  EXPECT_EQ(x->tags[1], 0);
  EXPECT_EQ(x->tags[2], 0);
  EXPECT_EQ(x->tags[3], 0);
  EXPECT_EQ(x->tags[4], 1);
  EXPECT_EQ(x->tags[5], 1);
  EXPECT_EQ(x->tags[6], 2);
  EXPECT_EQ(x->tags[7], 1);
  EXPECT_EQ(x->tags[8], 0);
  EXPECT_EQ(x->tags[9], 0);
  EXPECT_EQ(x->tags[10], 2);
  EXPECT_EQ(x->tags[11], 0);

  EXPECT_STREQ(a->data[0], "A");
  EXPECT_STREQ(a->data[2], "");
  EXPECT_STREQ(a->data[3], "ABCD");
  EXPECT_STREQ(a->data[4], "AA");
  EXPECT_STREQ(a->data[5], "");
  EXPECT_EQ(a->length[0], 1);
  EXPECT_EQ(a->length[2], 0);
  EXPECT_EQ(a->length[3], 4);
  EXPECT_EQ(a->length[4], 2);
  EXPECT_EQ(a->length[5], 0);

  EXPECT_EQ(b->data[0], 3);
  EXPECT_EQ(b->data[1], 25);

  EXPECT_DOUBLE_EQ(c->data[0], 3.7);

  EXPECT_EQ(arrowOffset, 12);
  EXPECT_EQ(orcOffset, 12);
  writer->add(*batch);
  writer->close();
}
TEST(TestAdapterWriteNested, writeDenseUnionChunkedEmpty) {
  std::vector<std::shared_ptr<Field>> xFields;
  xFields.push_back(std::make_shared<Field>("a", utf8()));
  xFields.push_back(std::make_shared<Field>("b", int32()));
  auto sharedPtrArrowType = dense_union(xFields);
  ArrayVector av;
  std::shared_ptr<ChunkedArray> carray =
      std::make_shared<ChunkedArray>(av, sharedPtrArrowType);
  DataType* arrowType = sharedPtrArrowType.get();

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
  int64_t arrowIndexOffset = 0;
  int arrowChunkOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowIndexOffset, arrowChunkOffset,
                                       batchSize, carray.get());
  if (!st.ok()) {
    FAIL() << "ORC ColumnBatch not successfully filled";
  }
  EXPECT_EQ(x->numElements, 0);
  EXPECT_FALSE(x->hasNulls);
  EXPECT_EQ(a->numElements, 0);
  EXPECT_FALSE(a->hasNulls);
  EXPECT_EQ(b->numElements, 0);
  EXPECT_FALSE(b->hasNulls);

  writer->add(*batch);
  writer->close();
}
TEST(TestAdapterWriteNested, writeDenseUnionChunkedMixed1) {
  std::vector<std::shared_ptr<Field>> xFields;
  xFields.push_back(std::make_shared<Field>("a", utf8()));
  xFields.push_back(std::make_shared<Field>("b", int32()));
  auto sharedPtrArrowType = dense_union(xFields);
  DataType* arrowType = sharedPtrArrowType.get();

  StringBuilder builder10, builder11, builder12, builder13, builder14;
  (void)(builder11.Append("A"));
  (void)(builder11.Append("AB"));
  (void)(builder13.AppendNull());
  (void)(builder13.AppendNull());

  Int32Builder builder20, builder21, builder22, builder23, builder24;
  (void)(builder21.Append(3));
  (void)(builder21.AppendNull());
  (void)(builder23.Append(25));
  (void)(builder23.AppendNull());

  std::shared_ptr<Array> array10, array11, array12, array13, array14;
  std::shared_ptr<Array> array20, array21, array22, array23, array24;

  (void)(builder10.Finish(&array10));
  (void)(builder11.Finish(&array11));
  (void)(builder12.Finish(&array12));
  (void)(builder13.Finish(&array13));
  (void)(builder14.Finish(&array14));
  (void)(builder20.Finish(&array20));
  (void)(builder21.Finish(&array21));
  (void)(builder22.Finish(&array22));
  (void)(builder23.Finish(&array23));
  (void)(builder24.Finish(&array24));

  std::vector<std::shared_ptr<Array>> children0, children1, children2, children3,
      children4;
  children0.push_back(array10);
  children0.push_back(array20);
  children1.push_back(array11);
  children1.push_back(array21);
  children2.push_back(array12);
  children2.push_back(array22);
  children3.push_back(array13);
  children3.push_back(array23);
  children4.push_back(array14);
  children4.push_back(array24);

  int8_t type_ids1[4] = {0, 0, 1, 1};
  int32_t valueOffsets1[4] = {0, 1, 0, 1};

  BufferBuilder builder1;
  (void)(builder1.Resize(4));
  (void)(builder1.Append(type_ids1, 4));
  std::shared_ptr<arrow::Buffer> type_idsBuffer1;
  if (!builder1.Finish(&type_idsBuffer1).ok()) {
    FAIL() << "The type_ids buffer can not be constructed!";
  }

  BufferBuilder valueOffsetsBuilder1;
  (void)(valueOffsetsBuilder1.Resize(16));
  (void)(valueOffsetsBuilder1.Append(valueOffsets1, 16));
  std::shared_ptr<arrow::Buffer> valueOffsetsBuffer1;
  if (!valueOffsetsBuilder1.Finish(&valueOffsetsBuffer1).ok()) {
    FAIL() << "The value_offsets buffer can not be constructed!";
  }

  int8_t type_ids3[4] = {1, 0, 1, 0};
  int32_t valueOffsets3[4] = {0, 0, 1, 1};

  BufferBuilder builder3;
  (void)(builder3.Resize(4));
  (void)(builder3.Append(type_ids3, 4));
  std::shared_ptr<arrow::Buffer> type_idsBuffer3;
  if (!builder3.Finish(&type_idsBuffer3).ok()) {
    FAIL() << "The type_ids buffer can not be constructed!";
  }

  BufferBuilder valueOffsetsBuilder3;
  (void)(valueOffsetsBuilder3.Resize(16));
  (void)(valueOffsetsBuilder3.Append(valueOffsets3, 16));
  std::shared_ptr<arrow::Buffer> valueOffsetsBuffer3;
  if (!valueOffsetsBuilder3.Finish(&valueOffsetsBuffer3).ok()) {
    FAIL() << "The value_offsets buffer can not be constructed!";
  }

  auto array0 =
      std::make_shared<DenseUnionArray>(sharedPtrArrowType, 0, children0, NULLPTR);
  auto array2 =
      std::make_shared<DenseUnionArray>(sharedPtrArrowType, 0, children2, NULLPTR);
  auto array4 =
      std::make_shared<DenseUnionArray>(sharedPtrArrowType, 0, children4, NULLPTR);
  auto array1 = std::make_shared<DenseUnionArray>(sharedPtrArrowType, 4, children1,
                                                  type_idsBuffer1, valueOffsetsBuffer1);
  auto array3 = std::make_shared<DenseUnionArray>(sharedPtrArrowType, 4, children3,
                                                  type_idsBuffer3, valueOffsetsBuffer3);
  ArrayVector av;
  av.push_back(array0);
  av.push_back(array1);
  av.push_back(array2);
  av.push_back(array3);
  av.push_back(array4);
  std::shared_ptr<ChunkedArray> carray = std::make_shared<ChunkedArray>(av);

  // RecordProperty("l", carray->length());
  // RecordProperty("l0", carray->chunk(0)->length());
  // RecordProperty("l1", carray->chunk(1)->length());
  // RecordProperty("l2", carray->chunk(2)->length());
  // RecordProperty("l3", carray->chunk(3)->length());
  // RecordProperty("l4", carray->chunk(4)->length());

  // auto a1 = std::static_pointer_cast<DenseUnionArray>(carray->chunk(1));
  // for (int i = 0; i < 4; i++) {
  //   RecordProperty("ci" + std::to_string(i), a1->child_id(i));
  // }

  // auto a3 = std::static_pointer_cast<DenseUnionArray>(carray->chunk(3));
  // for (int i = 0; i < 4; i++) {
  //   RecordProperty("ci" + std::to_string(i + 4), a3->child_id(i));
  // }

  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:uniontype<a:string,b:int>>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  int64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::UnionVectorBatch* x =
      internal::checked_cast<liborc::UnionVectorBatch*>(root->fields[0]);
  liborc::StringVectorBatch* a =
      internal::checked_cast<liborc::StringVectorBatch*>(x->children[0]);
  liborc::LongVectorBatch* b =
      internal::checked_cast<liborc::LongVectorBatch*>(x->children[1]);
  int64_t arrowIndexOffset = 0;
  int arrowChunkOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowIndexOffset, arrowChunkOffset,
                                       batchSize, carray.get());

  // RecordProperty("arrowIndexOffset", arrowIndexOffset);
  // RecordProperty("arrowChunkOffset", arrowChunkOffset);
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
  EXPECT_EQ(a->notNull[1], 1);
  EXPECT_EQ(a->notNull[2], 0);
  EXPECT_EQ(a->notNull[3], 0);
  EXPECT_EQ(b->notNull[0], 1);
  EXPECT_EQ(b->notNull[1], 0);
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

  EXPECT_EQ(x->tags[0], 0);
  EXPECT_EQ(x->tags[1], 0);
  EXPECT_EQ(x->tags[2], 1);
  EXPECT_EQ(x->tags[3], 1);
  EXPECT_EQ(x->tags[4], 1);
  EXPECT_EQ(x->tags[5], 0);
  EXPECT_EQ(x->tags[6], 1);
  EXPECT_EQ(x->tags[7], 0);

  EXPECT_STREQ(a->data[0], "A");
  EXPECT_STREQ(a->data[1], "AB");
  EXPECT_EQ(a->length[0], 1);
  EXPECT_EQ(a->length[1], 2);

  EXPECT_EQ(b->data[0], 3);
  EXPECT_EQ(b->data[2], 25);

  writer->add(*batch);
  writer->close();
}
TEST(TestAdapterWriteNested, writeDenseUnionChunkedMixed2) {
  std::vector<std::shared_ptr<Field>> xFields;
  xFields.push_back(std::make_shared<Field>("a", utf8()));
  xFields.push_back(std::make_shared<Field>("b", int32()));
  auto sharedPtrArrowType = dense_union(xFields);
  DataType* arrowType = sharedPtrArrowType.get();

  StringBuilder builder10, builder11, builder12, builder13, builder14;
  (void)(builder11.Append("A"));
  (void)(builder11.Append("AB"));
  (void)(builder11.Append("ABC"));
  (void)(builder13.AppendNull());
  (void)(builder13.Append("MM"));

  Int32Builder builder20, builder21, builder22, builder23, builder24;
  (void)(builder21.Append(3));
  (void)(builder23.AppendNull());
  (void)(builder23.Append(25));
  (void)(builder23.AppendNull());

  std::shared_ptr<Array> array10, array11, array12, array13, array14;
  std::shared_ptr<Array> array20, array21, array22, array23, array24;

  (void)(builder10.Finish(&array10));
  (void)(builder11.Finish(&array11));
  (void)(builder12.Finish(&array12));
  (void)(builder13.Finish(&array13));
  (void)(builder14.Finish(&array14));
  (void)(builder20.Finish(&array20));
  (void)(builder21.Finish(&array21));
  (void)(builder22.Finish(&array22));
  (void)(builder23.Finish(&array23));
  (void)(builder24.Finish(&array24));

  std::vector<std::shared_ptr<Array>> children0, children1, children2, children3,
      children4;
  children0.push_back(array10);
  children0.push_back(array20);
  children1.push_back(array11);
  children1.push_back(array21);
  children2.push_back(array12);
  children2.push_back(array22);
  children3.push_back(array13);
  children3.push_back(array23);
  children4.push_back(array14);
  children4.push_back(array24);

  int8_t type_ids1[4] = {0, 0, 1, 0};
  int32_t valueOffsets1[4] = {0, 1, 0, 2};

  BufferBuilder builder1;
  (void)(builder1.Resize(4));
  (void)(builder1.Append(type_ids1, 4));
  std::shared_ptr<arrow::Buffer> type_idsBuffer1;
  if (!builder1.Finish(&type_idsBuffer1).ok()) {
    FAIL() << "The type_ids buffer can not be constructed!";
  }

  BufferBuilder valueOffsetsBuilder1;
  (void)(valueOffsetsBuilder1.Resize(16));
  (void)(valueOffsetsBuilder1.Append(valueOffsets1, 16));
  std::shared_ptr<arrow::Buffer> valueOffsetsBuffer1;
  if (!valueOffsetsBuilder1.Finish(&valueOffsetsBuffer1).ok()) {
    FAIL() << "The value_offsets buffer can not be constructed!";
  }

  int8_t type_ids3[5] = {1, 0, 1, 0, 1};
  int32_t valueOffsets3[5] = {0, 0, 1, 1, 2};

  BufferBuilder builder3;
  (void)(builder3.Resize(5));
  (void)(builder3.Append(type_ids3, 5));
  std::shared_ptr<arrow::Buffer> type_idsBuffer3;
  if (!builder3.Finish(&type_idsBuffer3).ok()) {
    FAIL() << "The type_ids buffer can not be constructed!";
  }

  BufferBuilder valueOffsetsBuilder3;
  (void)(valueOffsetsBuilder3.Resize(20));
  (void)(valueOffsetsBuilder3.Append(valueOffsets3, 20));
  std::shared_ptr<arrow::Buffer> valueOffsetsBuffer3;
  if (!valueOffsetsBuilder3.Finish(&valueOffsetsBuffer3).ok()) {
    FAIL() << "The value_offsets buffer can not be constructed!";
  }

  auto array0 =
      std::make_shared<DenseUnionArray>(sharedPtrArrowType, 0, children0, NULLPTR);
  auto array2 =
      std::make_shared<DenseUnionArray>(sharedPtrArrowType, 0, children2, NULLPTR);
  auto array4 =
      std::make_shared<DenseUnionArray>(sharedPtrArrowType, 0, children4, NULLPTR);
  auto array1 = std::make_shared<DenseUnionArray>(sharedPtrArrowType, 4, children1,
                                                  type_idsBuffer1, valueOffsetsBuffer1);
  auto array3 = std::make_shared<DenseUnionArray>(sharedPtrArrowType, 5, children3,
                                                  type_idsBuffer3, valueOffsetsBuffer3);
  ArrayVector av;
  av.push_back(array0);
  av.push_back(array1);
  av.push_back(array2);
  av.push_back(array3);
  av.push_back(array4);
  std::shared_ptr<ChunkedArray> carray = std::make_shared<ChunkedArray>(av);

  // RecordProperty("l", carray->length());
  // RecordProperty("l0", carray->chunk(0)->length());
  // RecordProperty("l1", carray->chunk(1)->length());
  // RecordProperty("l2", carray->chunk(2)->length());
  // RecordProperty("l3", carray->chunk(3)->length());
  // RecordProperty("l4", carray->chunk(4)->length());

  // auto a1 = std::static_pointer_cast<DenseUnionArray>(carray->chunk(1));
  // for (int i = 0; i < 4; i++) {
  //   RecordProperty("ci" + std::to_string(i), a1->child_id(i));
  // }

  // auto a3 = std::static_pointer_cast<DenseUnionArray>(carray->chunk(3));
  // for (int i = 0; i < 4; i++) {
  //   RecordProperty("ci" + std::to_string(i + 4), a3->child_id(i));
  // }

  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:uniontype<a:string,b:int>>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  int64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::UnionVectorBatch* x =
      internal::checked_cast<liborc::UnionVectorBatch*>(root->fields[0]);
  liborc::StringVectorBatch* a =
      internal::checked_cast<liborc::StringVectorBatch*>(x->children[0]);
  liborc::LongVectorBatch* b =
      internal::checked_cast<liborc::LongVectorBatch*>(x->children[1]);
  int64_t arrowIndexOffset = 0;
  int arrowChunkOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowIndexOffset, arrowChunkOffset,
                                       batchSize, carray.get());

  // RecordProperty("arrowIndexOffset", arrowIndexOffset);
  // RecordProperty("arrowChunkOffset", arrowChunkOffset);
  if (!st.ok()) {
    FAIL() << "ORC ColumnBatch not successfully filled";
  }
  EXPECT_EQ(x->numElements, 9);
  EXPECT_FALSE(x->hasNulls);
  EXPECT_EQ(a->numElements, 5);
  EXPECT_TRUE(a->hasNulls);
  EXPECT_EQ(b->numElements, 4);
  EXPECT_TRUE(b->hasNulls);

  EXPECT_EQ(a->notNull[0], 1);
  EXPECT_EQ(a->notNull[1], 1);
  EXPECT_EQ(a->notNull[2], 1);
  EXPECT_EQ(a->notNull[3], 0);
  EXPECT_EQ(a->notNull[4], 1);
  EXPECT_EQ(b->notNull[0], 1);
  EXPECT_EQ(b->notNull[1], 0);
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
  EXPECT_EQ(x->offsets[8], 8);

  EXPECT_EQ(x->tags[0], 0);
  EXPECT_EQ(x->tags[1], 0);
  EXPECT_EQ(x->tags[2], 1);
  EXPECT_EQ(x->tags[3], 0);
  EXPECT_EQ(x->tags[4], 1);
  EXPECT_EQ(x->tags[5], 0);
  EXPECT_EQ(x->tags[6], 1);
  EXPECT_EQ(x->tags[7], 0);
  EXPECT_EQ(x->tags[8], 1);

  EXPECT_STREQ(a->data[0], "A");
  EXPECT_STREQ(a->data[1], "AB");
  EXPECT_STREQ(a->data[2], "ABC");
  EXPECT_STREQ(a->data[4], "MM");
  EXPECT_EQ(a->length[0], 1);
  EXPECT_EQ(a->length[1], 2);
  EXPECT_EQ(a->length[2], 3);
  EXPECT_EQ(a->length[4], 2);

  EXPECT_EQ(b->data[0], 3);
  EXPECT_EQ(b->data[2], 25);

  writer->add(*batch);
  writer->close();
}
TEST(TestAdapterWriteNested, writeDenseUnionChunkedMixed3) {
  std::vector<std::shared_ptr<Field>> xFields;
  xFields.push_back(std::make_shared<Field>("a", utf8()));
  xFields.push_back(std::make_shared<Field>("b", int32()));
  auto sharedPtrArrowType = dense_union(xFields);
  DataType* arrowType = sharedPtrArrowType.get();

  StringBuilder builder10, builder11, builder12, builder13, builder14;
  (void)(builder11.Append("A"));
  (void)(builder11.Append("AB"));
  (void)(builder11.Append("ABC"));
  (void)(builder11.Append("AB"));
  (void)(builder11.Append("ABC"));
  (void)(builder11.AppendNull());
  (void)(builder13.AppendNull());
  (void)(builder13.Append("MM"));

  Int32Builder builder20, builder21, builder22, builder23, builder24;

  std::shared_ptr<Array> array10, array11, array12, array13, array14;
  std::shared_ptr<Array> array20, array21, array22, array23, array24;

  (void)(builder10.Finish(&array10));
  (void)(builder11.Finish(&array11));
  (void)(builder12.Finish(&array12));
  (void)(builder13.Finish(&array13));
  (void)(builder14.Finish(&array14));
  (void)(builder20.Finish(&array20));
  (void)(builder21.Finish(&array21));
  (void)(builder22.Finish(&array22));
  (void)(builder23.Finish(&array23));
  (void)(builder24.Finish(&array24));

  std::vector<std::shared_ptr<Array>> children0, children1, children2, children3,
      children4;
  children0.push_back(array10);
  children0.push_back(array20);
  children1.push_back(array11);
  children1.push_back(array21);
  children2.push_back(array12);
  children2.push_back(array22);
  children3.push_back(array13);
  children3.push_back(array23);
  children4.push_back(array14);
  children4.push_back(array24);

  int8_t type_ids1[6] = {0, 0, 0, 0, 0, 0};
  int32_t valueOffsets1[6] = {0, 1, 2, 3, 4, 5};

  BufferBuilder builder1;
  (void)(builder1.Resize(6));
  (void)(builder1.Append(type_ids1, 6));
  std::shared_ptr<arrow::Buffer> type_idsBuffer1;
  if (!builder1.Finish(&type_idsBuffer1).ok()) {
    FAIL() << "The type_ids buffer can not be constructed!";
  }

  BufferBuilder valueOffsetsBuilder1;
  (void)(valueOffsetsBuilder1.Resize(24));
  (void)(valueOffsetsBuilder1.Append(valueOffsets1, 24));
  std::shared_ptr<arrow::Buffer> valueOffsetsBuffer1;
  if (!valueOffsetsBuilder1.Finish(&valueOffsetsBuffer1).ok()) {
    FAIL() << "The value_offsets buffer can not be constructed!";
  }

  int8_t type_ids3[2] = {0, 0};
  int32_t valueOffsets3[2] = {0, 1};

  BufferBuilder builder3;
  (void)(builder3.Resize(2));
  (void)(builder3.Append(type_ids3, 2));
  std::shared_ptr<arrow::Buffer> type_idsBuffer3;
  if (!builder3.Finish(&type_idsBuffer3).ok()) {
    FAIL() << "The type_ids buffer can not be constructed!";
  }

  BufferBuilder valueOffsetsBuilder3;
  (void)(valueOffsetsBuilder3.Resize(8));
  (void)(valueOffsetsBuilder3.Append(valueOffsets3, 8));
  std::shared_ptr<arrow::Buffer> valueOffsetsBuffer3;
  if (!valueOffsetsBuilder3.Finish(&valueOffsetsBuffer3).ok()) {
    FAIL() << "The value_offsets buffer can not be constructed!";
  }

  auto array0 =
      std::make_shared<DenseUnionArray>(sharedPtrArrowType, 0, children0, NULLPTR);
  auto array2 =
      std::make_shared<DenseUnionArray>(sharedPtrArrowType, 0, children2, NULLPTR);
  auto array4 =
      std::make_shared<DenseUnionArray>(sharedPtrArrowType, 0, children4, NULLPTR);
  auto array1 = std::make_shared<DenseUnionArray>(sharedPtrArrowType, 6, children1,
                                                  type_idsBuffer1, valueOffsetsBuffer1);
  auto array3 = std::make_shared<DenseUnionArray>(sharedPtrArrowType, 2, children3,
                                                  type_idsBuffer3, valueOffsetsBuffer3);
  ArrayVector av;
  av.push_back(array0);
  av.push_back(array1);
  av.push_back(array2);
  av.push_back(array3);
  av.push_back(array4);
  std::shared_ptr<ChunkedArray> carray = std::make_shared<ChunkedArray>(av);

  // RecordProperty("l", carray->length());
  // RecordProperty("l0", carray->chunk(0)->length());
  // RecordProperty("l1", carray->chunk(1)->length());
  // RecordProperty("l2", carray->chunk(2)->length());
  // RecordProperty("l3", carray->chunk(3)->length());
  // RecordProperty("l4", carray->chunk(4)->length());

  // auto a1 = std::static_pointer_cast<DenseUnionArray>(carray->chunk(1));
  // for (int i = 0; i < 4; i++) {
  //   RecordProperty("ci" + std::to_string(i), a1->child_id(i));
  // }

  // auto a3 = std::static_pointer_cast<DenseUnionArray>(carray->chunk(3));
  // for (int i = 0; i < 4; i++) {
  //   RecordProperty("ci" + std::to_string(i + 4), a3->child_id(i));
  // }

  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:uniontype<a:string,b:int>>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  int64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::UnionVectorBatch* x =
      internal::checked_cast<liborc::UnionVectorBatch*>(root->fields[0]);
  liborc::StringVectorBatch* a =
      internal::checked_cast<liborc::StringVectorBatch*>(x->children[0]);
  liborc::LongVectorBatch* b =
      internal::checked_cast<liborc::LongVectorBatch*>(x->children[1]);
  int64_t arrowIndexOffset = 0;
  int arrowChunkOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowIndexOffset, arrowChunkOffset,
                                       batchSize, carray.get());

  // RecordProperty("arrowIndexOffset", arrowIndexOffset);
  // RecordProperty("arrowChunkOffset", arrowChunkOffset);
  if (!st.ok()) {
    FAIL() << "ORC ColumnBatch not successfully filled";
  }
  EXPECT_EQ(x->numElements, 8);
  EXPECT_FALSE(x->hasNulls);
  EXPECT_EQ(a->numElements, 8);
  EXPECT_TRUE(a->hasNulls);
  EXPECT_EQ(b->numElements, 0);
  EXPECT_FALSE(b->hasNulls);

  EXPECT_EQ(a->notNull[0], 1);
  EXPECT_EQ(a->notNull[1], 1);
  EXPECT_EQ(a->notNull[2], 1);
  EXPECT_EQ(a->notNull[3], 1);
  EXPECT_EQ(a->notNull[4], 1);
  EXPECT_EQ(a->notNull[5], 0);
  EXPECT_EQ(a->notNull[6], 0);
  EXPECT_EQ(a->notNull[7], 1);

  EXPECT_EQ(x->offsets[0], 0);
  EXPECT_EQ(x->offsets[1], 1);
  EXPECT_EQ(x->offsets[2], 2);
  EXPECT_EQ(x->offsets[3], 3);
  EXPECT_EQ(x->offsets[4], 4);
  EXPECT_EQ(x->offsets[5], 5);
  EXPECT_EQ(x->offsets[6], 6);
  EXPECT_EQ(x->offsets[7], 7);

  EXPECT_EQ(x->tags[0], 0);
  EXPECT_EQ(x->tags[1], 0);
  EXPECT_EQ(x->tags[2], 0);
  EXPECT_EQ(x->tags[3], 0);
  EXPECT_EQ(x->tags[4], 0);
  EXPECT_EQ(x->tags[5], 0);
  EXPECT_EQ(x->tags[6], 0);
  EXPECT_EQ(x->tags[7], 0);

  EXPECT_STREQ(a->data[0], "A");
  EXPECT_STREQ(a->data[1], "AB");
  EXPECT_STREQ(a->data[2], "ABC");
  EXPECT_STREQ(a->data[3], "AB");
  EXPECT_STREQ(a->data[4], "ABC");
  EXPECT_STREQ(a->data[7], "MM");
  EXPECT_EQ(a->length[0], 1);
  EXPECT_EQ(a->length[1], 2);
  EXPECT_EQ(a->length[2], 3);
  EXPECT_EQ(a->length[3], 2);
  EXPECT_EQ(a->length[4], 3);
  EXPECT_EQ(a->length[7], 2);

  writer->add(*batch);
  writer->close();
}
TEST(TestAdapterWriteNested, writeDenseUnionChunkedMultibatch) {
  std::vector<std::shared_ptr<Field>> xFields;
  xFields.push_back(std::make_shared<Field>("a", utf8()));
  xFields.push_back(std::make_shared<Field>("b", int32()));
  auto sharedPtrArrowType = dense_union(xFields);
  DataType* arrowType = sharedPtrArrowType.get();

  int64_t totalLength = 150;

  StringBuilder builder00, builder01, builder02, builder03, builder04;
  Int32Builder builder10, builder11, builder12, builder13, builder14;

  int8_t type_ids1[99], type_ids3[51];
  int32_t valueOffsets1[99], valueOffsets3[51];
  int32_t stringOffset = 0, intOffset = 0;

  for (int i = 0; i < 99; i++) {
    switch (i % 5) {
      case 0:
      case 1: {
        (void)(builder01.Append("Test " + std::to_string(i)));
        type_ids1[i] = 0;
        valueOffsets1[i] = stringOffset;
        stringOffset++;
        break;
      }
      case 2: {
        (void)(builder01.AppendNull());
        type_ids1[i] = 0;
        valueOffsets1[i] = stringOffset;
        stringOffset++;
        break;
      }
      case 3: {
        (void)(builder11.AppendNull());
        type_ids1[i] = 1;
        valueOffsets1[i] = intOffset;
        intOffset++;
        break;
      }
      default: {
        (void)(builder11.Append(i));
        type_ids1[i] = 1;
        valueOffsets1[i] = intOffset;
        intOffset++;
        break;
      }
    }
  }
  stringOffset = 0;
  intOffset = 0;
  for (int i = 99; i < 150; i++) {
    switch (i % 5) {
      case 0:
      case 1: {
        (void)(builder03.Append("Test " + std::to_string(i)));
        type_ids3[i - 99] = 0;
        valueOffsets3[i - 99] = stringOffset;
        stringOffset++;
        break;
      }
      case 2: {
        (void)(builder03.AppendNull());
        type_ids3[i - 99] = 0;
        valueOffsets3[i - 99] = stringOffset;
        stringOffset++;
        break;
      }
      case 3: {
        (void)(builder13.AppendNull());
        type_ids3[i - 99] = 1;
        valueOffsets3[i - 99] = intOffset;
        intOffset++;
        break;
      }
      default: {
        (void)(builder13.Append(i));
        type_ids3[i - 99] = 1;
        valueOffsets3[i - 99] = intOffset;
        intOffset++;
        break;
      }
    }
  }

  std::shared_ptr<Array> array00, array01, array02, array03, array04;
  std::shared_ptr<Array> array10, array11, array12, array13, array14;

  (void)(builder10.Finish(&array10));
  (void)(builder11.Finish(&array11));
  (void)(builder12.Finish(&array12));
  (void)(builder13.Finish(&array13));
  (void)(builder14.Finish(&array14));
  (void)(builder00.Finish(&array00));
  (void)(builder01.Finish(&array01));
  (void)(builder02.Finish(&array02));
  (void)(builder03.Finish(&array03));
  (void)(builder04.Finish(&array04));

  std::vector<std::shared_ptr<Array>> children0, children1, children2, children3,
      children4;
  children0.push_back(array00);
  children0.push_back(array10);
  children1.push_back(array01);
  children1.push_back(array11);
  children2.push_back(array02);
  children2.push_back(array12);
  children3.push_back(array03);
  children3.push_back(array13);
  children4.push_back(array04);
  children4.push_back(array14);

  BufferBuilder builder1;
  (void)(builder1.Resize(99));
  (void)(builder1.Append(type_ids1, 99));
  std::shared_ptr<arrow::Buffer> type_idsBuffer1;
  if (!builder1.Finish(&type_idsBuffer1).ok()) {
    FAIL() << "The type_ids buffer can not be constructed!";
  }

  BufferBuilder valueOffsetsBuilder1;
  (void)(valueOffsetsBuilder1.Resize(496));
  (void)(valueOffsetsBuilder1.Append(valueOffsets1, 496));
  std::shared_ptr<arrow::Buffer> valueOffsetsBuffer1;
  if (!valueOffsetsBuilder1.Finish(&valueOffsetsBuffer1).ok()) {
    FAIL() << "The value_offsets buffer can not be constructed!";
  }

  BufferBuilder builder3;
  (void)(builder3.Resize(51));
  (void)(builder3.Append(type_ids3, 51));
  std::shared_ptr<arrow::Buffer> type_idsBuffer3;
  if (!builder3.Finish(&type_idsBuffer3).ok()) {
    FAIL() << "The type_ids buffer can not be constructed!";
  }

  BufferBuilder valueOffsetsBuilder3;
  (void)(valueOffsetsBuilder3.Resize(204));
  (void)(valueOffsetsBuilder3.Append(valueOffsets3, 204));
  std::shared_ptr<arrow::Buffer> valueOffsetsBuffer3;
  if (!valueOffsetsBuilder3.Finish(&valueOffsetsBuffer3).ok()) {
    FAIL() << "The value_offsets buffer can not be constructed!";
  }

  auto array0 =
      std::make_shared<DenseUnionArray>(sharedPtrArrowType, 0, children0, NULLPTR);
  auto array2 =
      std::make_shared<DenseUnionArray>(sharedPtrArrowType, 0, children2, NULLPTR);
  auto array4 =
      std::make_shared<DenseUnionArray>(sharedPtrArrowType, 0, children4, NULLPTR);
  auto array1 = std::make_shared<DenseUnionArray>(sharedPtrArrowType, 99, children1,
                                                  type_idsBuffer1, valueOffsetsBuffer1);
  auto array3 = std::make_shared<DenseUnionArray>(sharedPtrArrowType, 51, children3,
                                                  type_idsBuffer3, valueOffsetsBuffer3);
  ArrayVector av;
  av.push_back(array0);
  av.push_back(array1);
  av.push_back(array2);
  av.push_back(array3);
  av.push_back(array4);
  std::shared_ptr<ChunkedArray> carray = std::make_shared<ChunkedArray>(av);

  // RecordProperty("l", carray->length());
  // RecordProperty("l0", carray->chunk(0)->length());
  // RecordProperty("l1", carray->chunk(1)->length());
  // RecordProperty("l2", carray->chunk(2)->length());
  // RecordProperty("l3", carray->chunk(3)->length());
  // RecordProperty("l4", carray->chunk(4)->length());

  // auto a1 = std::static_pointer_cast<DenseUnionArray>(carray->chunk(1));
  // auto a10 = std::static_pointer_cast<StringArray>(a1->field(0));
  // auto a11 = std::static_pointer_cast<Int32Array>(a1->field(1));
  // RecordProperty("l10", a10->length());
  // RecordProperty("l11", a11->length());
  // auto a3 = std::static_pointer_cast<DenseUnionArray>(carray->chunk(3));
  // auto a30 = std::static_pointer_cast<StringArray>(a3->field(0));
  // auto a31 = std::static_pointer_cast<Int32Array>(a3->field(1));
  // RecordProperty("l30", a30->length());
  // RecordProperty("l31", a31->length());

  // for (int i = 0; i < 60; i++) {
  //   RecordProperty("s" + std::to_string(i), a10->GetString(i));
  //   RecordProperty("an" + std::to_string(i), a10->IsNull(i));
  // }
  // for (int i = 0; i < 30; i++) {
  //   RecordProperty("s" + std::to_string(i + 60), a30->GetString(i));
  //   RecordProperty("an" + std::to_string(i + 60), a30->IsNull(i));
  // }
  // for (int i = 0; i < 39; i++) {
  //   RecordProperty("i" + std::to_string(i), a11->Value(i));
  //   RecordProperty("bn" + std::to_string(i), a11->IsNull(i));
  // }
  // for (int i = 0; i < 21; i++) {
  //   RecordProperty("i" + std::to_string(i + 39), a31->Value(i));
  //   RecordProperty("bn" + std::to_string(i + 39), a31->IsNull(i));
  // }

  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:uniontype<a:string,b:int>>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  int64_t batchSize = 3;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::UnionVectorBatch* x =
      internal::checked_cast<liborc::UnionVectorBatch*>(root->fields[0]);
  liborc::StringVectorBatch* a =
      internal::checked_cast<liborc::StringVectorBatch*>(x->children[0]);
  liborc::LongVectorBatch* b =
      internal::checked_cast<liborc::LongVectorBatch*>(x->children[1]);
  int64_t arrowIndexOffset = 0;
  int arrowChunkOffset = 0;
  int64_t resultOffset = 0;
  int32_t res;
  int32_t childOffsets[2], childLengths[2];
  while (resultOffset < totalLength - batchSize) {
    Status st = adapters::orc::FillBatch(arrowType, x, arrowIndexOffset, arrowChunkOffset,
                                         batchSize, carray.get());
    if (!st.ok()) {
      FAIL() << "ORC ColumnBatch not successfully filled";
    }
    writer->add(*batch);
    testDenseUnionStringOffsetGenerator(0, resultOffset, childOffsets);
    testDenseUnionStringOffsetGenerator(resultOffset, resultOffset + batchSize,
                                        childLengths);
    EXPECT_EQ(x->numElements, batchSize);
    EXPECT_EQ(a->numElements, childLengths[0]);
    EXPECT_EQ(b->numElements, childLengths[1]);
    EXPECT_FALSE(x->hasNulls);
    EXPECT_TRUE(a->hasNulls);
    if (resultOffset + batchSize <
        4)  // The first int null is the 3rd element (if we count from 0)
      EXPECT_FALSE(b->hasNulls);
    else
      EXPECT_TRUE(b->hasNulls);
    for (int64_t i = 0; i < batchSize; i++) {
      res = (i + resultOffset) % 5;
      EXPECT_EQ(x->offsets[i], i);
      if (res >= 3) {
        EXPECT_EQ(x->tags[i], 1);
      } else {
        EXPECT_EQ(x->tags[i], 0);
      }
    }
    for (int64_t i = 0; i < childLengths[0]; i++) {
      switch ((i + childOffsets[0]) % 3) {
        case 0: {
          EXPECT_TRUE(a->notNull[i]);
          EXPECT_EQ(a->data[i], "Test " + std::to_string((i + childOffsets[0]) * 5 / 3));
          if ((i + childOffsets[0]) >= 60) {
            EXPECT_EQ(a->length[i], 8);
          } else if ((i + childOffsets[0]) >= 6) {
            EXPECT_EQ(a->length[i], 7);
          } else {
            EXPECT_EQ(a->length[i], 6);
          }
          break;
        }
        case 1: {
          EXPECT_TRUE(a->notNull[i]);
          EXPECT_EQ(a->data[i],
                    "Test " + std::to_string((5 * (i + childOffsets[0]) - 2) / 3));
          if ((i + childOffsets[0]) >= 60) {
            EXPECT_EQ(a->length[i], 8);
          } else if ((i + childOffsets[0]) >= 6) {
            EXPECT_EQ(a->length[i], 7);
          } else {
            EXPECT_EQ(a->length[i], 6);
          }
          break;
        }
        default: {
          EXPECT_FALSE(a->notNull[i]);
        }
      }
    }
    for (int64_t i = 0; i < childLengths[1]; i++) {
      if ((i + childOffsets[1]) % 2) {
        EXPECT_TRUE(b->notNull[i]);
        EXPECT_EQ(b->data[i], (5 * (i + childOffsets[1]) + 3) / 2);
      } else {
        EXPECT_FALSE(b->notNull[i]);
      }
    }
    resultOffset = resultOffset + batchSize;
    batch->clear();
  }
  Status st = adapters::orc::FillBatch(arrowType, x, arrowIndexOffset, arrowChunkOffset,
                                       batchSize, carray.get());
  int64_t lastBatchSize = totalLength - resultOffset;
  testDenseUnionStringOffsetGenerator(0, resultOffset, childOffsets);
  testDenseUnionStringOffsetGenerator(resultOffset, totalLength, childLengths);
  // RecordProperty("ro", resultOffset);
  // RecordProperty("cl0", childLengths[0]);
  // RecordProperty("cl1", childLengths[1]);
  // RecordProperty("co0", childOffsets[0]);
  // RecordProperty("co1", childOffsets[1]);
  EXPECT_EQ(x->numElements, lastBatchSize);
  EXPECT_EQ(a->numElements, childLengths[0]);
  EXPECT_EQ(b->numElements, childLengths[1]);
  EXPECT_FALSE(x->hasNulls);
  EXPECT_TRUE(a->hasNulls);
  EXPECT_TRUE(b->hasNulls);
  for (int64_t i = 0; i < lastBatchSize; i++) {
    res = (i + resultOffset) % 5;
    EXPECT_EQ(x->offsets[i], i);
    if (res >= 3) {
      EXPECT_EQ(x->tags[i], 1);
    } else {
      EXPECT_EQ(x->tags[i], 0);
    }
  }
  for (int64_t i = 0; i < childLengths[0]; i++) {
    // RecordProperty("an-res" + std::to_string(i + childOffsets[0]), a->notNull[i]);
    // RecordProperty("av-res" + std::to_string(i + childOffsets[0]), a->data[i]);
    switch ((i + childOffsets[0]) % 3) {
      case 0: {
        EXPECT_TRUE(a->notNull[i]);
        EXPECT_EQ(a->data[i], "Test " + std::to_string((i + childOffsets[0]) * 5 / 3));
        if ((i + childOffsets[0]) >= 60) {
          EXPECT_EQ(a->length[i], 8);
        } else if ((i + childOffsets[0]) >= 6) {
          EXPECT_EQ(a->length[i], 7);
        } else {
          EXPECT_EQ(a->length[i], 6);
        }
        break;
      }
      case 1: {
        EXPECT_TRUE(a->notNull[i]);
        EXPECT_EQ(a->data[i],
                  "Test " + std::to_string((5 * (i + childOffsets[0]) - 2) / 3));
        if ((i + childOffsets[0]) >= 60) {
          EXPECT_EQ(a->length[i], 8);
        } else if ((i + childOffsets[0]) >= 6) {
          EXPECT_EQ(a->length[i], 7);
        } else {
          EXPECT_EQ(a->length[i], 6);
        }
        break;
      }
      default: {
        EXPECT_FALSE(a->notNull[i]);
      }
    }
  }
  for (int64_t i = 0; i < childLengths[1]; i++) {
    if ((i + childOffsets[1]) % 2) {
      EXPECT_TRUE(b->notNull[i]);
      EXPECT_EQ(b->data[i], (5 * (i + childOffsets[1]) + 3) / 2);
    } else {
      EXPECT_FALSE(b->notNull[i]);
    }
  }
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

  auto array =
      std::make_shared<SparseUnionArray>(sharedPtrArrowType, 0, children, NULLPTR);

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
TEST(TestAdapterWriteNested, writeSparseUnionNoNulls1) {
  std::vector<std::shared_ptr<Field>> xFields;
  xFields.push_back(std::make_shared<Field>("a", utf8()));
  xFields.push_back(std::make_shared<Field>("b", int32()));
  auto sharedPtrArrowType = sparse_union(xFields);
  DataType* arrowType = sharedPtrArrowType.get();

  StringBuilder builder1;
  (void)(builder1.Append("CD"));
  (void)(builder1.Append("AB"));
  (void)(builder1.AppendNull());
  (void)(builder1.AppendNull());

  Int32Builder builder2;
  (void)(builder2.AppendNull());
  (void)(builder2.AppendNull());
  (void)(builder2.Append(25));
  (void)(builder2.Append(76));

  std::shared_ptr<Array> array1;
  (void)(builder1.Finish(&array1));
  std::shared_ptr<Array> array2;
  (void)(builder2.Finish(&array2));

  std::vector<std::shared_ptr<Array>> children;
  children.push_back(array1);
  children.push_back(array2);

  int8_t type_ids[4] = {0, 0, 1, 1};

  BufferBuilder builder;
  (void)(builder.Resize(4));
  (void)(builder.Append(type_ids, 4));
  std::shared_ptr<arrow::Buffer> type_idsBuffer;
  if (!builder.Finish(&type_idsBuffer).ok()) {
    FAIL() << "The type_ids buffer can not be constructed!";
  }

  auto array =
      std::make_shared<SparseUnionArray>(sharedPtrArrowType, 4, children, type_idsBuffer);
  // for (int i = 0; i < 4; i++) {
  //   RecordProperty("ci" + std::to_string(i), array->child_id(i));
  // }
  // auto a10 = std::static_pointer_cast<StringArray>(array->field(0));
  // auto a11 = std::static_pointer_cast<Int32Array>(array->field(1));
  // RecordProperty("l10", a10->length());
  // RecordProperty("l11", a11->length());
  // for (int i = 0; i < 4; i++) {
  //   RecordProperty("s" + std::to_string(i), a10->GetString(i));
  //   RecordProperty("an" + std::to_string(i), a10->IsNull(i));
  // }
  // for (int i = 0; i < 4; i++) {
  //   RecordProperty("i" + std::to_string(i), a11->Value(i));
  //   RecordProperty("bn" + std::to_string(i), a11->IsNull(i));
  // }

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
  EXPECT_EQ(x->numElements, 4);
  EXPECT_FALSE(x->hasNulls);
  EXPECT_EQ(a->numElements, 2);
  EXPECT_FALSE(a->hasNulls);
  EXPECT_EQ(b->numElements, 2);
  EXPECT_FALSE(b->hasNulls);

  EXPECT_EQ(x->offsets[0], 0);
  EXPECT_EQ(x->offsets[1], 1);
  EXPECT_EQ(x->offsets[2], 2);
  EXPECT_EQ(x->offsets[3], 3);

  EXPECT_EQ(x->tags[0], 0);
  EXPECT_EQ(x->tags[1], 0);
  EXPECT_EQ(x->tags[2], 1);
  EXPECT_EQ(x->tags[3], 1);

  EXPECT_STREQ(a->data[0], "CD");
  EXPECT_STREQ(a->data[1], "AB");

  EXPECT_EQ(a->length[0], 2);
  EXPECT_EQ(a->length[1], 2);

  EXPECT_EQ(b->data[0], 25);
  EXPECT_EQ(b->data[1], 76);

  EXPECT_EQ(arrowOffset, 4);
  EXPECT_EQ(orcOffset, 4);
  writer->add(*batch);
  writer->close();
}
TEST(TestAdapterWriteNested, writeSparseUnionNoNulls2) {
  std::vector<std::shared_ptr<Field>> xFields;
  xFields.push_back(std::make_shared<Field>("a", utf8()));
  xFields.push_back(std::make_shared<Field>("b", int32()));
  auto sharedPtrArrowType = sparse_union(xFields);
  DataType* arrowType = sharedPtrArrowType.get();

  StringBuilder builder1;
  (void)(builder1.Append("CD"));
  (void)(builder1.Append("AB"));
  (void)(builder1.Append("EF"));
  (void)(builder1.Append("GH"));

  Int32Builder builder2;
  (void)(builder2.Append(253));
  (void)(builder2.Append(726));
  (void)(builder2.Append(25));
  (void)(builder2.Append(76));

  std::shared_ptr<Array> array1;
  (void)(builder1.Finish(&array1));
  std::shared_ptr<Array> array2;
  (void)(builder2.Finish(&array2));

  std::vector<std::shared_ptr<Array>> children;
  children.push_back(array1);
  children.push_back(array2);

  int8_t type_ids[4] = {0, 0, 1, 0};

  BufferBuilder builder;
  (void)(builder.Resize(4));
  (void)(builder.Append(type_ids, 4));
  std::shared_ptr<arrow::Buffer> type_idsBuffer;
  if (!builder.Finish(&type_idsBuffer).ok()) {
    FAIL() << "The type_ids buffer can not be constructed!";
  }

  auto array =
      std::make_shared<SparseUnionArray>(sharedPtrArrowType, 4, children, type_idsBuffer);
  // for (int i = 0; i < 4; i++) {
  //   RecordProperty("ci" + std::to_string(i), array->child_id(i));
  // }
  // auto a10 = std::static_pointer_cast<StringArray>(array->field(0));
  // auto a11 = std::static_pointer_cast<Int32Array>(array->field(1));
  // RecordProperty("l10", a10->length());
  // RecordProperty("l11", a11->length());
  // for (int i = 0; i < 4; i++) {
  //   RecordProperty("s" + std::to_string(i), a10->GetString(i));
  //   RecordProperty("an" + std::to_string(i), a10->IsNull(i));
  // }
  // for (int i = 0; i < 4; i++) {
  //   RecordProperty("i" + std::to_string(i), a11->Value(i));
  //   RecordProperty("bn" + std::to_string(i), a11->IsNull(i));
  // }

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
  EXPECT_EQ(x->numElements, 4);
  EXPECT_FALSE(x->hasNulls);
  EXPECT_EQ(a->numElements, 3);
  EXPECT_FALSE(a->hasNulls);
  EXPECT_EQ(b->numElements, 1);
  EXPECT_FALSE(b->hasNulls);

  EXPECT_EQ(x->offsets[0], 0);
  EXPECT_EQ(x->offsets[1], 1);
  EXPECT_EQ(x->offsets[2], 2);
  EXPECT_EQ(x->offsets[3], 3);

  EXPECT_EQ(x->tags[0], 0);
  EXPECT_EQ(x->tags[1], 0);
  EXPECT_EQ(x->tags[2], 1);
  EXPECT_EQ(x->tags[3], 0);

  EXPECT_STREQ(a->data[0], "CD");
  EXPECT_STREQ(a->data[1], "AB");
  EXPECT_STREQ(a->data[2], "GH");

  EXPECT_EQ(a->length[0], 2);
  EXPECT_EQ(a->length[1], 2);
  EXPECT_EQ(a->length[2], 2);

  EXPECT_EQ(b->data[0], 25);

  EXPECT_EQ(arrowOffset, 4);
  EXPECT_EQ(orcOffset, 4);
  writer->add(*batch);
  writer->close();
}
TEST(TestAdapterWriteNested, writeSparseUnionNoNulls3) {
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
  (void)(builder2.Append(253));
  (void)(builder2.Append(726));
  (void)(builder2.Append(25));
  (void)(builder2.Append(76));

  std::shared_ptr<Array> array1;
  (void)(builder1.Finish(&array1));
  std::shared_ptr<Array> array2;
  (void)(builder2.Finish(&array2));

  std::vector<std::shared_ptr<Array>> children;
  children.push_back(array1);
  children.push_back(array2);

  int8_t type_ids[4] = {1, 1, 1, 1};

  BufferBuilder builder;
  (void)(builder.Resize(4));
  (void)(builder.Append(type_ids, 4));
  std::shared_ptr<arrow::Buffer> type_idsBuffer;
  if (!builder.Finish(&type_idsBuffer).ok()) {
    FAIL() << "The type_ids buffer can not be constructed!";
  }

  auto array =
      std::make_shared<SparseUnionArray>(sharedPtrArrowType, 4, children, type_idsBuffer);
  // for (int i = 0; i < 4; i++) {
  //   RecordProperty("ci" + std::to_string(i), array->child_id(i));
  // }
  // auto a10 = std::static_pointer_cast<StringArray>(array->field(0));
  // auto a11 = std::static_pointer_cast<Int32Array>(array->field(1));
  // RecordProperty("l10", a10->length());
  // RecordProperty("l11", a11->length());
  // for (int i = 0; i < 4; i++) {
  //   RecordProperty("s" + std::to_string(i), a10->GetString(i));
  //   RecordProperty("an" + std::to_string(i), a10->IsNull(i));
  // }
  // for (int i = 0; i < 4; i++) {
  //   RecordProperty("i" + std::to_string(i), a11->Value(i));
  //   RecordProperty("bn" + std::to_string(i), a11->IsNull(i));
  // }

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
  EXPECT_EQ(x->numElements, 4);
  EXPECT_FALSE(x->hasNulls);
  EXPECT_EQ(a->numElements, 0);
  EXPECT_FALSE(a->hasNulls);
  EXPECT_EQ(b->numElements, 4);
  EXPECT_FALSE(b->hasNulls);

  EXPECT_EQ(x->offsets[0], 0);
  EXPECT_EQ(x->offsets[1], 1);
  EXPECT_EQ(x->offsets[2], 2);
  EXPECT_EQ(x->offsets[3], 3);

  EXPECT_EQ(x->tags[0], 1);
  EXPECT_EQ(x->tags[1], 1);
  EXPECT_EQ(x->tags[2], 1);
  EXPECT_EQ(x->tags[3], 1);

  EXPECT_EQ(b->data[0], 253);
  EXPECT_EQ(b->data[1], 726);
  EXPECT_EQ(b->data[2], 25);
  EXPECT_EQ(b->data[3], 76);

  EXPECT_EQ(arrowOffset, 4);
  EXPECT_EQ(orcOffset, 4);
  writer->add(*batch);
  writer->close();
}
TEST(TestAdapterWriteNested, writeSparseUnionAllNulls1) {
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

  int8_t type_ids[4] = {1, 1, 0, 0};

  BufferBuilder builder;
  (void)(builder.Resize(4));
  (void)(builder.Append(type_ids, 4));
  std::shared_ptr<arrow::Buffer> type_idsBuffer;
  if (!builder.Finish(&type_idsBuffer).ok()) {
    FAIL() << "The type_ids buffer can not be constructed!";
  }

  auto array =
      std::make_shared<SparseUnionArray>(sharedPtrArrowType, 4, children, type_idsBuffer);

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
  EXPECT_EQ(x->numElements, 4);
  EXPECT_FALSE(x->hasNulls);
  EXPECT_EQ(a->numElements, 2);
  EXPECT_TRUE(a->hasNulls);
  EXPECT_EQ(b->numElements, 2);
  EXPECT_TRUE(b->hasNulls);

  EXPECT_EQ(a->notNull[0], 0);
  EXPECT_EQ(a->notNull[1], 0);
  EXPECT_EQ(b->notNull[0], 0);
  EXPECT_EQ(b->notNull[1], 0);

  EXPECT_EQ(x->offsets[0], 0);
  EXPECT_EQ(x->offsets[1], 1);
  EXPECT_EQ(x->offsets[2], 2);
  EXPECT_EQ(x->offsets[3], 3);

  EXPECT_EQ(x->tags[0], 1);
  EXPECT_EQ(x->tags[1], 1);
  EXPECT_EQ(x->tags[2], 0);
  EXPECT_EQ(x->tags[3], 0);

  EXPECT_EQ(arrowOffset, 4);
  EXPECT_EQ(orcOffset, 4);
  writer->add(*batch);
  writer->close();
}
TEST(TestAdapterWriteNested, writeSparseUnionAllNulls2) {
  std::vector<std::shared_ptr<Field>> xFields;
  xFields.push_back(std::make_shared<Field>("a", utf8()));
  xFields.push_back(std::make_shared<Field>("b", int32()));
  auto sharedPtrArrowType = sparse_union(xFields);
  DataType* arrowType = sharedPtrArrowType.get();

  StringBuilder builder1;
  (void)(builder1.Append("AB"));
  (void)(builder1.Append("CD"));
  (void)(builder1.Append("EF"));
  (void)(builder1.Append("GH"));

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

  int8_t type_ids[4] = {1, 1, 1, 1};

  BufferBuilder builder;
  (void)(builder.Resize(4));
  (void)(builder.Append(type_ids, 4));
  std::shared_ptr<arrow::Buffer> type_idsBuffer;
  if (!builder.Finish(&type_idsBuffer).ok()) {
    FAIL() << "The type_ids buffer can not be constructed!";
  }

  auto array =
      std::make_shared<SparseUnionArray>(sharedPtrArrowType, 4, children, type_idsBuffer);

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
  EXPECT_EQ(x->numElements, 4);
  EXPECT_FALSE(x->hasNulls);
  EXPECT_EQ(a->numElements, 0);
  EXPECT_FALSE(a->hasNulls);
  EXPECT_EQ(b->numElements, 4);
  EXPECT_TRUE(b->hasNulls);

  EXPECT_EQ(b->notNull[0], 0);
  EXPECT_EQ(b->notNull[1], 0);
  EXPECT_EQ(b->notNull[2], 0);
  EXPECT_EQ(b->notNull[3], 0);

  EXPECT_EQ(x->offsets[0], 0);
  EXPECT_EQ(x->offsets[1], 1);
  EXPECT_EQ(x->offsets[2], 2);
  EXPECT_EQ(x->offsets[3], 3);

  EXPECT_EQ(x->tags[0], 1);
  EXPECT_EQ(x->tags[1], 1);
  EXPECT_EQ(x->tags[2], 1);
  EXPECT_EQ(x->tags[3], 1);

  EXPECT_EQ(arrowOffset, 4);
  EXPECT_EQ(orcOffset, 4);
  writer->add(*batch);
  writer->close();
}
TEST(TestAdapterWriteNested, writeSparseUnionAllNulls3) {
  std::vector<std::shared_ptr<Field>> xFields;
  xFields.push_back(std::make_shared<Field>("a", utf8()));
  xFields.push_back(std::make_shared<Field>("b", int32()));
  auto sharedPtrArrowType = sparse_union(xFields);
  DataType* arrowType = sharedPtrArrowType.get();

  StringBuilder builder1;
  (void)(builder1.Append("AB"));
  (void)(builder1.AppendNull());
  (void)(builder1.Append("EF"));
  (void)(builder1.Append("GH"));

  Int32Builder builder2;
  (void)(builder2.AppendNull());
  (void)(builder2.Append(5));
  (void)(builder2.AppendNull());
  (void)(builder2.AppendNull());

  std::shared_ptr<Array> array1;
  (void)(builder1.Finish(&array1));
  std::shared_ptr<Array> array2;
  (void)(builder2.Finish(&array2));

  std::vector<std::shared_ptr<Array>> children;
  children.push_back(array1);
  children.push_back(array2);

  int8_t type_ids[4] = {1, 0, 1, 1};

  BufferBuilder builder;
  (void)(builder.Resize(4));
  (void)(builder.Append(type_ids, 4));
  std::shared_ptr<arrow::Buffer> type_idsBuffer;
  if (!builder.Finish(&type_idsBuffer).ok()) {
    FAIL() << "The type_ids buffer can not be constructed!";
  }

  auto array =
      std::make_shared<SparseUnionArray>(sharedPtrArrowType, 4, children, type_idsBuffer);

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
  EXPECT_EQ(x->numElements, 4);
  EXPECT_FALSE(x->hasNulls);
  EXPECT_EQ(a->numElements, 1);
  EXPECT_TRUE(a->hasNulls);
  EXPECT_EQ(b->numElements, 3);
  EXPECT_TRUE(b->hasNulls);

  EXPECT_EQ(a->notNull[0], 0);
  EXPECT_EQ(b->notNull[0], 0);
  EXPECT_EQ(b->notNull[1], 0);
  EXPECT_EQ(b->notNull[2], 0);

  EXPECT_EQ(x->offsets[0], 0);
  EXPECT_EQ(x->offsets[1], 1);
  EXPECT_EQ(x->offsets[2], 2);
  EXPECT_EQ(x->offsets[3], 3);

  EXPECT_EQ(x->tags[0], 1);
  EXPECT_EQ(x->tags[1], 0);
  EXPECT_EQ(x->tags[2], 1);
  EXPECT_EQ(x->tags[3], 1);

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

  int8_t type_ids[4] = {0, 1, 0, 1};

  BufferBuilder builder;
  (void)(builder.Resize(4));
  (void)(builder.Append(type_ids, 4));
  std::shared_ptr<arrow::Buffer> type_idsBuffer;
  if (!builder.Finish(&type_idsBuffer).ok()) {
    FAIL() << "The type_ids buffer can not be constructed!";
  }

  auto array =
      std::make_shared<SparseUnionArray>(sharedPtrArrowType, 4, children, type_idsBuffer);

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
  EXPECT_EQ(x->numElements, 4);
  EXPECT_FALSE(x->hasNulls);
  EXPECT_EQ(a->numElements, 2);
  EXPECT_FALSE(a->hasNulls);
  EXPECT_EQ(b->numElements, 2);
  EXPECT_TRUE(b->hasNulls);

  EXPECT_EQ(a->notNull[0], 1);
  EXPECT_EQ(a->notNull[1], 1);
  EXPECT_EQ(b->notNull[0], 1);
  EXPECT_EQ(b->notNull[1], 0);

  EXPECT_EQ(x->offsets[0], 0);
  EXPECT_EQ(x->offsets[1], 1);
  EXPECT_EQ(x->offsets[2], 2);
  EXPECT_EQ(x->offsets[3], 3);

  EXPECT_EQ(x->tags[0], 0);
  EXPECT_EQ(x->tags[1], 1);
  EXPECT_EQ(x->tags[2], 0);
  EXPECT_EQ(x->tags[3], 1);

  EXPECT_STREQ(a->data[0], "A");
  EXPECT_STREQ(a->data[1], "");
  EXPECT_EQ(a->length[0], 1);
  EXPECT_EQ(a->length[1], 0);

  EXPECT_EQ(b->data[0], -12);

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
  (void)(builder1.Append("AB"));

  Int32Builder builder2;
  (void)(builder2.Append(3));
  (void)(builder2.Append(-12));
  (void)(builder2.Append(25));
  (void)(builder2.AppendNull());
  (void)(builder2.Append(42));

  DoubleBuilder builder3;
  (void)(builder3.Append(3.7));
  (void)(builder3.Append(-12.5));
  (void)(builder3.Append(25.4));
  (void)(builder3.AppendNull());
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

  int8_t type_ids[5] = {0, 0, 2, 0, 1};

  BufferBuilder builder;
  (void)(builder.Resize(5));
  (void)(builder.Append(type_ids, 5));
  std::shared_ptr<arrow::Buffer> type_idsBuffer;
  if (!builder.Finish(&type_idsBuffer).ok()) {
    FAIL() << "The type_ids buffer can not be constructed!";
  }

  auto array =
      std::make_shared<SparseUnionArray>(sharedPtrArrowType, 5, children, type_idsBuffer);

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
  EXPECT_EQ(x->numElements, 5);
  EXPECT_FALSE(x->hasNulls);
  EXPECT_EQ(a->numElements, 3);
  EXPECT_TRUE(a->hasNulls);
  EXPECT_EQ(b->numElements, 1);
  EXPECT_FALSE(b->hasNulls);
  EXPECT_EQ(c->numElements, 1);
  EXPECT_FALSE(c->hasNulls);

  EXPECT_EQ(a->notNull[0], 1);
  EXPECT_EQ(a->notNull[1], 0);
  EXPECT_EQ(a->notNull[2], 1);
  EXPECT_EQ(b->notNull[0], 1);
  EXPECT_EQ(c->notNull[0], 1);

  EXPECT_EQ(x->offsets[0], 0);
  EXPECT_EQ(x->offsets[1], 1);
  EXPECT_EQ(x->offsets[2], 2);
  EXPECT_EQ(x->offsets[3], 3);
  EXPECT_EQ(x->offsets[4], 4);

  EXPECT_EQ(x->tags[0], 0);
  EXPECT_EQ(x->tags[1], 0);
  EXPECT_EQ(x->tags[2], 2);
  EXPECT_EQ(x->tags[3], 0);
  EXPECT_EQ(x->tags[4], 1);

  EXPECT_STREQ(a->data[0], "A");
  EXPECT_STREQ(a->data[2], "ABCD");
  EXPECT_EQ(a->length[0], 1);
  EXPECT_EQ(a->length[2], 4);

  EXPECT_EQ(b->data[0], 42);

  EXPECT_DOUBLE_EQ(c->data[0], 25.4);

  EXPECT_EQ(arrowOffset, 5);
  EXPECT_EQ(orcOffset, 5);
  writer->add(*batch);
  writer->close();
}
TEST(TestAdapterWriteNested, writeSparseUnionChunkedEmpty) {
  std::vector<std::shared_ptr<Field>> xFields;
  xFields.push_back(std::make_shared<Field>("a", utf8()));
  xFields.push_back(std::make_shared<Field>("b", int32()));
  auto sharedPtrArrowType = sparse_union(xFields);
  ArrayVector av;
  std::shared_ptr<ChunkedArray> carray =
      std::make_shared<ChunkedArray>(av, sharedPtrArrowType);
  DataType* arrowType = sharedPtrArrowType.get();

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
  int64_t arrowIndexOffset = 0;
  int arrowChunkOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowIndexOffset, arrowChunkOffset,
                                       batchSize, carray.get());
  if (!st.ok()) {
    FAIL() << "ORC ColumnBatch not successfully filled";
  }
  EXPECT_EQ(x->numElements, 0);
  EXPECT_FALSE(x->hasNulls);
  EXPECT_EQ(a->numElements, 0);
  EXPECT_FALSE(a->hasNulls);
  EXPECT_EQ(b->numElements, 0);
  EXPECT_FALSE(b->hasNulls);

  writer->add(*batch);
  writer->close();
}
TEST(TestAdapterWriteNested, writeSparseUnionChunkedMixed1) {
  std::vector<std::shared_ptr<Field>> xFields;
  xFields.push_back(std::make_shared<Field>("a", utf8()));
  xFields.push_back(std::make_shared<Field>("b", int32()));
  auto sharedPtrArrowType = sparse_union(xFields);
  DataType* arrowType = sharedPtrArrowType.get();

  StringBuilder builder10, builder11, builder12, builder13, builder14;
  (void)(builder11.Append("A"));
  (void)(builder11.Append("AB"));
  (void)(builder13.AppendNull());
  (void)(builder13.AppendNull());

  Int32Builder builder20, builder21, builder22, builder23, builder24;
  (void)(builder21.Append(3));
  (void)(builder21.AppendNull());
  (void)(builder23.Append(25));
  (void)(builder23.AppendNull());

  std::shared_ptr<Array> array10, array11, array12, array13, array14;
  std::shared_ptr<Array> array20, array21, array22, array23, array24;

  (void)(builder10.Finish(&array10));
  (void)(builder11.Finish(&array11));
  (void)(builder12.Finish(&array12));
  (void)(builder13.Finish(&array13));
  (void)(builder14.Finish(&array14));
  (void)(builder20.Finish(&array20));
  (void)(builder21.Finish(&array21));
  (void)(builder22.Finish(&array22));
  (void)(builder23.Finish(&array23));
  (void)(builder24.Finish(&array24));

  std::vector<std::shared_ptr<Array>> children0, children1, children2, children3,
      children4;
  children0.push_back(array10);
  children0.push_back(array20);
  children1.push_back(array11);
  children1.push_back(array21);
  children2.push_back(array12);
  children2.push_back(array22);
  children3.push_back(array13);
  children3.push_back(array23);
  children4.push_back(array14);
  children4.push_back(array24);

  int8_t type_ids1[2] = {0, 1};

  BufferBuilder builder1;
  (void)(builder1.Resize(2));
  (void)(builder1.Append(type_ids1, 2));
  std::shared_ptr<arrow::Buffer> type_idsBuffer1;
  if (!builder1.Finish(&type_idsBuffer1).ok()) {
    FAIL() << "The type_ids buffer can not be constructed!";
  }

  int8_t type_ids3[2] = {0, 0};

  BufferBuilder builder3;
  (void)(builder3.Resize(2));
  (void)(builder3.Append(type_ids3, 2));
  std::shared_ptr<arrow::Buffer> type_idsBuffer3;
  if (!builder3.Finish(&type_idsBuffer3).ok()) {
    FAIL() << "The type_ids buffer can not be constructed!";
  }

  auto array0 =
      std::make_shared<SparseUnionArray>(sharedPtrArrowType, 0, children0, NULLPTR);
  auto array2 =
      std::make_shared<SparseUnionArray>(sharedPtrArrowType, 0, children2, NULLPTR);
  auto array4 =
      std::make_shared<SparseUnionArray>(sharedPtrArrowType, 0, children4, NULLPTR);
  auto array1 = std::make_shared<SparseUnionArray>(sharedPtrArrowType, 2, children1,
                                                   type_idsBuffer1);
  auto array3 = std::make_shared<SparseUnionArray>(sharedPtrArrowType, 2, children3,
                                                   type_idsBuffer3);
  ArrayVector av;
  av.push_back(array0);
  av.push_back(array1);
  av.push_back(array2);
  av.push_back(array3);
  av.push_back(array4);
  std::shared_ptr<ChunkedArray> carray = std::make_shared<ChunkedArray>(av);

  // RecordProperty("l", carray->length());
  // RecordProperty("l0", carray->chunk(0)->length());
  // RecordProperty("l1", carray->chunk(1)->length());
  // RecordProperty("l2", carray->chunk(2)->length());
  // RecordProperty("l3", carray->chunk(3)->length());
  // RecordProperty("l4", carray->chunk(4)->length());

  // auto a1 = std::static_pointer_cast<DenseUnionArray>(carray->chunk(1));
  // for (int i = 0; i < 4; i++) {
  //   RecordProperty("ci" + std::to_string(i), a1->child_id(i));
  // }

  // auto a3 = std::static_pointer_cast<DenseUnionArray>(carray->chunk(3));
  // for (int i = 0; i < 4; i++) {
  //   RecordProperty("ci" + std::to_string(i + 4), a3->child_id(i));
  // }

  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:uniontype<a:string,b:int>>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  int64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::UnionVectorBatch* x =
      internal::checked_cast<liborc::UnionVectorBatch*>(root->fields[0]);
  liborc::StringVectorBatch* a =
      internal::checked_cast<liborc::StringVectorBatch*>(x->children[0]);
  liborc::LongVectorBatch* b =
      internal::checked_cast<liborc::LongVectorBatch*>(x->children[1]);
  int64_t arrowIndexOffset = 0;
  int arrowChunkOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowIndexOffset, arrowChunkOffset,
                                       batchSize, carray.get());

  // RecordProperty("arrowIndexOffset", arrowIndexOffset);
  // RecordProperty("arrowChunkOffset", arrowChunkOffset);
  if (!st.ok()) {
    FAIL() << "ORC ColumnBatch not successfully filled";
  }
  EXPECT_EQ(x->numElements, 4);
  EXPECT_FALSE(x->hasNulls);
  EXPECT_EQ(a->numElements, 3);
  EXPECT_TRUE(a->hasNulls);
  EXPECT_EQ(b->numElements, 1);
  EXPECT_TRUE(b->hasNulls);

  EXPECT_EQ(a->notNull[0], 1);
  EXPECT_EQ(a->notNull[1], 0);
  EXPECT_EQ(a->notNull[2], 0);
  EXPECT_EQ(b->notNull[0], 0);

  EXPECT_EQ(x->offsets[0], 0);
  EXPECT_EQ(x->offsets[1], 1);
  EXPECT_EQ(x->offsets[2], 2);
  EXPECT_EQ(x->offsets[3], 3);

  EXPECT_EQ(x->tags[0], 0);
  EXPECT_EQ(x->tags[1], 1);
  EXPECT_EQ(x->tags[2], 0);
  EXPECT_EQ(x->tags[3], 0);

  EXPECT_STREQ(a->data[0], "A");
  EXPECT_EQ(a->length[0], 1);

  writer->add(*batch);
  writer->close();
}
TEST(TestAdapterWriteNested, writeSparseUnionChunkedMixed2) {
  std::vector<std::shared_ptr<Field>> xFields;
  xFields.push_back(std::make_shared<Field>("a", utf8()));
  xFields.push_back(std::make_shared<Field>("b", int32()));
  auto sharedPtrArrowType = sparse_union(xFields);
  DataType* arrowType = sharedPtrArrowType.get();

  StringBuilder builder10, builder11, builder12, builder13, builder14;
  (void)(builder11.Append("A"));
  (void)(builder11.Append("AB"));
  (void)(builder11.Append("ABC"));
  (void)(builder13.AppendNull());
  (void)(builder13.Append("MM"));

  Int32Builder builder20, builder21, builder22, builder23, builder24;
  (void)(builder21.Append(3));
  (void)(builder21.AppendNull());
  (void)(builder21.Append(36));
  (void)(builder23.Append(25));
  (void)(builder23.AppendNull());

  std::shared_ptr<Array> array10, array11, array12, array13, array14;
  std::shared_ptr<Array> array20, array21, array22, array23, array24;

  (void)(builder10.Finish(&array10));
  (void)(builder11.Finish(&array11));
  (void)(builder12.Finish(&array12));
  (void)(builder13.Finish(&array13));
  (void)(builder14.Finish(&array14));
  (void)(builder20.Finish(&array20));
  (void)(builder21.Finish(&array21));
  (void)(builder22.Finish(&array22));
  (void)(builder23.Finish(&array23));
  (void)(builder24.Finish(&array24));

  std::vector<std::shared_ptr<Array>> children0, children1, children2, children3,
      children4;
  children0.push_back(array10);
  children0.push_back(array20);
  children1.push_back(array11);
  children1.push_back(array21);
  children2.push_back(array12);
  children2.push_back(array22);
  children3.push_back(array13);
  children3.push_back(array23);
  children4.push_back(array14);
  children4.push_back(array24);

  int8_t type_ids1[3] = {0, 0, 0};

  BufferBuilder builder1;
  (void)(builder1.Resize(3));
  (void)(builder1.Append(type_ids1, 3));
  std::shared_ptr<arrow::Buffer> type_idsBuffer1;
  if (!builder1.Finish(&type_idsBuffer1).ok()) {
    FAIL() << "The type_ids buffer can not be constructed!";
  }

  int8_t type_ids3[2] = {0, 1};

  BufferBuilder builder3;
  (void)(builder3.Resize(2));
  (void)(builder3.Append(type_ids3, 2));
  std::shared_ptr<arrow::Buffer> type_idsBuffer3;
  if (!builder3.Finish(&type_idsBuffer3).ok()) {
    FAIL() << "The type_ids buffer can not be constructed!";
  }

  auto array0 =
      std::make_shared<SparseUnionArray>(sharedPtrArrowType, 0, children0, NULLPTR);
  auto array2 =
      std::make_shared<SparseUnionArray>(sharedPtrArrowType, 0, children2, NULLPTR);
  auto array4 =
      std::make_shared<SparseUnionArray>(sharedPtrArrowType, 0, children4, NULLPTR);
  auto array1 = std::make_shared<SparseUnionArray>(sharedPtrArrowType, 3, children1,
                                                   type_idsBuffer1);
  auto array3 = std::make_shared<SparseUnionArray>(sharedPtrArrowType, 2, children3,
                                                   type_idsBuffer3);
  ArrayVector av;
  av.push_back(array0);
  av.push_back(array1);
  av.push_back(array2);
  av.push_back(array3);
  av.push_back(array4);
  std::shared_ptr<ChunkedArray> carray = std::make_shared<ChunkedArray>(av);

  // RecordProperty("l", carray->length());
  // RecordProperty("l0", carray->chunk(0)->length());
  // RecordProperty("l1", carray->chunk(1)->length());
  // RecordProperty("l2", carray->chunk(2)->length());
  // RecordProperty("l3", carray->chunk(3)->length());
  // RecordProperty("l4", carray->chunk(4)->length());

  // auto a1 = std::static_pointer_cast<DenseUnionArray>(carray->chunk(1));
  // for (int i = 0; i < 4; i++) {
  //   RecordProperty("ci" + std::to_string(i), a1->child_id(i));
  // }

  // auto a3 = std::static_pointer_cast<DenseUnionArray>(carray->chunk(3));
  // for (int i = 0; i < 4; i++) {
  //   RecordProperty("ci" + std::to_string(i + 4), a3->child_id(i));
  // }

  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:uniontype<a:string,b:int>>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  int64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::UnionVectorBatch* x =
      internal::checked_cast<liborc::UnionVectorBatch*>(root->fields[0]);
  liborc::StringVectorBatch* a =
      internal::checked_cast<liborc::StringVectorBatch*>(x->children[0]);
  liborc::LongVectorBatch* b =
      internal::checked_cast<liborc::LongVectorBatch*>(x->children[1]);
  int64_t arrowIndexOffset = 0;
  int arrowChunkOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowIndexOffset, arrowChunkOffset,
                                       batchSize, carray.get());

  // RecordProperty("arrowIndexOffset", arrowIndexOffset);
  // RecordProperty("arrowChunkOffset", arrowChunkOffset);
  if (!st.ok()) {
    FAIL() << "ORC ColumnBatch not successfully filled";
  }
  EXPECT_EQ(x->numElements, 5);
  EXPECT_FALSE(x->hasNulls);
  EXPECT_EQ(a->numElements, 4);
  EXPECT_TRUE(a->hasNulls);
  EXPECT_EQ(b->numElements, 1);
  EXPECT_TRUE(b->hasNulls);

  EXPECT_EQ(a->notNull[0], 1);
  EXPECT_EQ(a->notNull[1], 1);
  EXPECT_EQ(a->notNull[2], 1);
  EXPECT_EQ(a->notNull[3], 0);
  EXPECT_EQ(b->notNull[0], 0);

  EXPECT_EQ(x->offsets[0], 0);
  EXPECT_EQ(x->offsets[1], 1);
  EXPECT_EQ(x->offsets[2], 2);
  EXPECT_EQ(x->offsets[3], 3);
  EXPECT_EQ(x->offsets[4], 4);

  EXPECT_EQ(x->tags[0], 0);
  EXPECT_EQ(x->tags[1], 0);
  EXPECT_EQ(x->tags[2], 0);
  EXPECT_EQ(x->tags[3], 0);
  EXPECT_EQ(x->tags[4], 1);

  EXPECT_STREQ(a->data[0], "A");
  EXPECT_STREQ(a->data[1], "AB");
  EXPECT_STREQ(a->data[2], "ABC");
  EXPECT_EQ(a->length[0], 1);
  EXPECT_EQ(a->length[1], 2);
  EXPECT_EQ(a->length[2], 3);

  writer->add(*batch);
  writer->close();
}
TEST(TestAdapterWriteNested, writeSparseUnionChunkedMixed3) {
  std::vector<std::shared_ptr<Field>> xFields;
  xFields.push_back(std::make_shared<Field>("a", utf8()));
  xFields.push_back(std::make_shared<Field>("b", int32()));
  auto sharedPtrArrowType = sparse_union(xFields);
  DataType* arrowType = sharedPtrArrowType.get();

  StringBuilder builder10, builder11, builder12, builder13, builder14;
  (void)(builder11.Append("A"));
  (void)(builder11.Append("AB"));
  (void)(builder11.Append("ABC"));
  (void)(builder11.Append("AB"));
  (void)(builder11.Append("ABC"));
  (void)(builder11.AppendNull());
  (void)(builder13.AppendNull());
  (void)(builder13.Append("MM"));

  Int32Builder builder20, builder21, builder22, builder23, builder24;
  (void)(builder21.AppendNull());
  (void)(builder21.Append(13));
  (void)(builder21.Append(212));
  (void)(builder21.Append(23));
  (void)(builder21.Append(24));
  (void)(builder21.Append(323));
  (void)(builder23.AppendNull());
  (void)(builder23.Append(34));

  std::shared_ptr<Array> array10, array11, array12, array13, array14;
  std::shared_ptr<Array> array20, array21, array22, array23, array24;

  (void)(builder10.Finish(&array10));
  (void)(builder11.Finish(&array11));
  (void)(builder12.Finish(&array12));
  (void)(builder13.Finish(&array13));
  (void)(builder14.Finish(&array14));
  (void)(builder20.Finish(&array20));
  (void)(builder21.Finish(&array21));
  (void)(builder22.Finish(&array22));
  (void)(builder23.Finish(&array23));
  (void)(builder24.Finish(&array24));

  std::vector<std::shared_ptr<Array>> children0, children1, children2, children3,
      children4;
  children0.push_back(array10);
  children0.push_back(array20);
  children1.push_back(array11);
  children1.push_back(array21);
  children2.push_back(array12);
  children2.push_back(array22);
  children3.push_back(array13);
  children3.push_back(array23);
  children4.push_back(array14);
  children4.push_back(array24);

  int8_t type_ids1[6] = {0, 0, 0, 0, 0, 0};

  BufferBuilder builder1;
  (void)(builder1.Resize(6));
  (void)(builder1.Append(type_ids1, 6));
  std::shared_ptr<arrow::Buffer> type_idsBuffer1;
  if (!builder1.Finish(&type_idsBuffer1).ok()) {
    FAIL() << "The type_ids buffer can not be constructed!";
  }

  int8_t type_ids3[2] = {0, 0};

  BufferBuilder builder3;
  (void)(builder3.Resize(2));
  (void)(builder3.Append(type_ids3, 2));
  std::shared_ptr<arrow::Buffer> type_idsBuffer3;
  if (!builder3.Finish(&type_idsBuffer3).ok()) {
    FAIL() << "The type_ids buffer can not be constructed!";
  }

  auto array0 =
      std::make_shared<SparseUnionArray>(sharedPtrArrowType, 0, children0, NULLPTR);
  auto array2 =
      std::make_shared<SparseUnionArray>(sharedPtrArrowType, 0, children2, NULLPTR);
  auto array4 =
      std::make_shared<SparseUnionArray>(sharedPtrArrowType, 0, children4, NULLPTR);
  auto array1 = std::make_shared<SparseUnionArray>(sharedPtrArrowType, 6, children1,
                                                   type_idsBuffer1);
  auto array3 = std::make_shared<SparseUnionArray>(sharedPtrArrowType, 2, children3,
                                                   type_idsBuffer3);
  ArrayVector av;
  av.push_back(array0);
  av.push_back(array1);
  av.push_back(array2);
  av.push_back(array3);
  av.push_back(array4);
  std::shared_ptr<ChunkedArray> carray = std::make_shared<ChunkedArray>(av);

  // RecordProperty("l", carray->length());
  // RecordProperty("l0", carray->chunk(0)->length());
  // RecordProperty("l1", carray->chunk(1)->length());
  // RecordProperty("l2", carray->chunk(2)->length());
  // RecordProperty("l3", carray->chunk(3)->length());
  // RecordProperty("l4", carray->chunk(4)->length());

  // auto a1 = std::static_pointer_cast<DenseUnionArray>(carray->chunk(1));
  // for (int i = 0; i < 4; i++) {
  //   RecordProperty("ci" + std::to_string(i), a1->child_id(i));
  // }

  // auto a3 = std::static_pointer_cast<DenseUnionArray>(carray->chunk(3));
  // for (int i = 0; i < 4; i++) {
  //   RecordProperty("ci" + std::to_string(i + 4), a3->child_id(i));
  // }

  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:uniontype<a:string,b:int>>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  int64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::UnionVectorBatch* x =
      internal::checked_cast<liborc::UnionVectorBatch*>(root->fields[0]);
  liborc::StringVectorBatch* a =
      internal::checked_cast<liborc::StringVectorBatch*>(x->children[0]);
  liborc::LongVectorBatch* b =
      internal::checked_cast<liborc::LongVectorBatch*>(x->children[1]);
  int64_t arrowIndexOffset = 0;
  int arrowChunkOffset = 0;
  Status st = adapters::orc::FillBatch(arrowType, x, arrowIndexOffset, arrowChunkOffset,
                                       batchSize, carray.get());

  // RecordProperty("arrowIndexOffset", arrowIndexOffset);
  // RecordProperty("arrowChunkOffset", arrowChunkOffset);
  if (!st.ok()) {
    FAIL() << "ORC ColumnBatch not successfully filled";
  }
  EXPECT_EQ(x->numElements, 8);
  EXPECT_FALSE(x->hasNulls);
  EXPECT_EQ(a->numElements, 8);
  EXPECT_TRUE(a->hasNulls);
  EXPECT_EQ(b->numElements, 0);
  EXPECT_FALSE(b->hasNulls);

  EXPECT_EQ(a->notNull[0], 1);
  EXPECT_EQ(a->notNull[1], 1);
  EXPECT_EQ(a->notNull[2], 1);
  EXPECT_EQ(a->notNull[3], 1);
  EXPECT_EQ(a->notNull[4], 1);
  EXPECT_EQ(a->notNull[5], 0);
  EXPECT_EQ(a->notNull[6], 0);
  EXPECT_EQ(a->notNull[7], 1);

  EXPECT_EQ(x->offsets[0], 0);
  EXPECT_EQ(x->offsets[1], 1);
  EXPECT_EQ(x->offsets[2], 2);
  EXPECT_EQ(x->offsets[3], 3);
  EXPECT_EQ(x->offsets[4], 4);
  EXPECT_EQ(x->offsets[5], 5);
  EXPECT_EQ(x->offsets[6], 6);
  EXPECT_EQ(x->offsets[7], 7);

  EXPECT_EQ(x->tags[0], 0);
  EXPECT_EQ(x->tags[1], 0);
  EXPECT_EQ(x->tags[2], 0);
  EXPECT_EQ(x->tags[3], 0);
  EXPECT_EQ(x->tags[4], 0);
  EXPECT_EQ(x->tags[5], 0);
  EXPECT_EQ(x->tags[6], 0);
  EXPECT_EQ(x->tags[7], 0);

  EXPECT_STREQ(a->data[0], "A");
  EXPECT_STREQ(a->data[1], "AB");
  EXPECT_STREQ(a->data[2], "ABC");
  EXPECT_STREQ(a->data[3], "AB");
  EXPECT_STREQ(a->data[4], "ABC");
  EXPECT_STREQ(a->data[7], "MM");
  EXPECT_EQ(a->length[0], 1);
  EXPECT_EQ(a->length[1], 2);
  EXPECT_EQ(a->length[2], 3);
  EXPECT_EQ(a->length[3], 2);
  EXPECT_EQ(a->length[4], 3);
  EXPECT_EQ(a->length[7], 2);

  writer->add(*batch);
  writer->close();
}
TEST(TestAdapterWriteNested, writeSparseUnionChunkedMultibatch) {
  std::vector<std::shared_ptr<Field>> xFields;
  xFields.push_back(std::make_shared<Field>("a", utf8()));
  xFields.push_back(std::make_shared<Field>("b", int32()));
  auto sharedPtrArrowType = sparse_union(xFields);
  DataType* arrowType = sharedPtrArrowType.get();

  int64_t totalLength = 150;

  StringBuilder builder00, builder01, builder02, builder03, builder04;
  Int32Builder builder10, builder11, builder12, builder13, builder14;

  int8_t type_ids1[99], type_ids3[51];

  for (int i = 0; i < 99; i++) {
    switch (i % 5) {
      case 0:
      case 1: {
        (void)(builder01.Append("Test " + std::to_string(i)));
        (void)(builder11.Append(i));
        type_ids1[i] = 0;
        break;
      }
      case 2: {
        (void)(builder01.AppendNull());
        (void)(builder11.AppendNull());
        type_ids1[i] = 0;
        break;
      }
      case 3: {
        (void)(builder01.Append("Test " + std::to_string(i)));
        (void)(builder11.AppendNull());
        type_ids1[i] = 1;
        break;
      }
      default: {
        (void)(builder01.AppendNull());
        (void)(builder11.Append(i));
        type_ids1[i] = 1;
        break;
      }
    }
  }
  for (int i = 99; i < 150; i++) {
    switch (i % 5) {
      case 0:
      case 1: {
        (void)(builder03.Append("Test " + std::to_string(i)));
        (void)(builder13.Append(i));
        type_ids3[i - 99] = 0;
        break;
      }
      case 2: {
        (void)(builder03.AppendNull());
        (void)(builder13.AppendNull());
        type_ids3[i - 99] = 0;
        break;
      }
      case 3: {
        (void)(builder03.Append("Test " + std::to_string(i)));
        (void)(builder13.AppendNull());
        type_ids3[i - 99] = 1;
        break;
      }
      default: {
        (void)(builder03.AppendNull());
        (void)(builder13.Append(i));
        type_ids3[i - 99] = 1;
        break;
      }
    }
  }

  std::shared_ptr<Array> array00, array01, array02, array03, array04;
  std::shared_ptr<Array> array10, array11, array12, array13, array14;

  (void)(builder10.Finish(&array10));
  (void)(builder11.Finish(&array11));
  (void)(builder12.Finish(&array12));
  (void)(builder13.Finish(&array13));
  (void)(builder14.Finish(&array14));
  (void)(builder00.Finish(&array00));
  (void)(builder01.Finish(&array01));
  (void)(builder02.Finish(&array02));
  (void)(builder03.Finish(&array03));
  (void)(builder04.Finish(&array04));

  std::vector<std::shared_ptr<Array>> children0, children1, children2, children3,
      children4;
  children0.push_back(array00);
  children0.push_back(array10);
  children1.push_back(array01);
  children1.push_back(array11);
  children2.push_back(array02);
  children2.push_back(array12);
  children3.push_back(array03);
  children3.push_back(array13);
  children4.push_back(array04);
  children4.push_back(array14);

  BufferBuilder builder1;
  (void)(builder1.Resize(99));
  (void)(builder1.Append(type_ids1, 99));
  std::shared_ptr<arrow::Buffer> type_idsBuffer1;
  if (!builder1.Finish(&type_idsBuffer1).ok()) {
    FAIL() << "The type_ids buffer can not be constructed!";
  }

  BufferBuilder builder3;
  (void)(builder3.Resize(51));
  (void)(builder3.Append(type_ids3, 51));
  std::shared_ptr<arrow::Buffer> type_idsBuffer3;
  if (!builder3.Finish(&type_idsBuffer3).ok()) {
    FAIL() << "The type_ids buffer can not be constructed!";
  }

  auto array0 =
      std::make_shared<SparseUnionArray>(sharedPtrArrowType, 0, children0, NULLPTR);
  auto array2 =
      std::make_shared<SparseUnionArray>(sharedPtrArrowType, 0, children2, NULLPTR);
  auto array4 =
      std::make_shared<SparseUnionArray>(sharedPtrArrowType, 0, children4, NULLPTR);
  auto array1 = std::make_shared<SparseUnionArray>(sharedPtrArrowType, 99, children1,
                                                   type_idsBuffer1);
  auto array3 = std::make_shared<SparseUnionArray>(sharedPtrArrowType, 51, children3,
                                                   type_idsBuffer3);
  ArrayVector av;
  av.push_back(array0);
  av.push_back(array1);
  av.push_back(array2);
  av.push_back(array3);
  av.push_back(array4);
  std::shared_ptr<ChunkedArray> carray = std::make_shared<ChunkedArray>(av);

  // RecordProperty("l", carray->length());
  // RecordProperty("l0", carray->chunk(0)->length());
  // RecordProperty("l1", carray->chunk(1)->length());
  // RecordProperty("l2", carray->chunk(2)->length());
  // RecordProperty("l3", carray->chunk(3)->length());
  // RecordProperty("l4", carray->chunk(4)->length());

  // auto a1 = std::static_pointer_cast<DenseUnionArray>(carray->chunk(1));
  // auto a10 = std::static_pointer_cast<StringArray>(a1->field(0));
  // auto a11 = std::static_pointer_cast<Int32Array>(a1->field(1));
  // RecordProperty("l10", a10->length());
  // RecordProperty("l11", a11->length());
  // auto a3 = std::static_pointer_cast<DenseUnionArray>(carray->chunk(3));
  // auto a30 = std::static_pointer_cast<StringArray>(a3->field(0));
  // auto a31 = std::static_pointer_cast<Int32Array>(a3->field(1));
  // RecordProperty("l30", a30->length());
  // RecordProperty("l31", a31->length());

  // for (int i = 0; i < 60; i++) {
  //   RecordProperty("s" + std::to_string(i), a10->GetString(i));
  //   RecordProperty("an" + std::to_string(i), a10->IsNull(i));
  // }
  // for (int i = 0; i < 30; i++) {
  //   RecordProperty("s" + std::to_string(i + 60), a30->GetString(i));
  //   RecordProperty("an" + std::to_string(i + 60), a30->IsNull(i));
  // }
  // for (int i = 0; i < 39; i++) {
  //   RecordProperty("i" + std::to_string(i), a11->Value(i));
  //   RecordProperty("bn" + std::to_string(i), a11->IsNull(i));
  // }
  // for (int i = 0; i < 21; i++) {
  //   RecordProperty("i" + std::to_string(i + 39), a31->Value(i));
  //   RecordProperty("bn" + std::to_string(i + 39), a31->IsNull(i));
  // }

  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:uniontype<a:string,b:int>>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  int64_t batchSize = 4;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::UnionVectorBatch* x =
      internal::checked_cast<liborc::UnionVectorBatch*>(root->fields[0]);
  liborc::StringVectorBatch* a =
      internal::checked_cast<liborc::StringVectorBatch*>(x->children[0]);
  liborc::LongVectorBatch* b =
      internal::checked_cast<liborc::LongVectorBatch*>(x->children[1]);
  int64_t arrowIndexOffset = 0;
  int arrowChunkOffset = 0;
  int64_t resultOffset = 0;
  int32_t res;
  int32_t childOffsets[2], childLengths[2];
  while (resultOffset < totalLength - batchSize) {
    Status st = adapters::orc::FillBatch(arrowType, x, arrowIndexOffset, arrowChunkOffset,
                                         batchSize, carray.get());
    if (!st.ok()) {
      FAIL() << "ORC ColumnBatch not successfully filled";
    }
    writer->add(*batch);
    testDenseUnionStringOffsetGenerator(0, resultOffset, childOffsets);
    testDenseUnionStringOffsetGenerator(resultOffset, resultOffset + batchSize,
                                        childLengths);
    EXPECT_EQ(x->numElements, batchSize);
    EXPECT_EQ(a->numElements, childLengths[0]);
    EXPECT_EQ(b->numElements, childLengths[1]);
    EXPECT_FALSE(x->hasNulls);
    if (resultOffset + batchSize <
        3)  // The first string null is the 2nd element (if we count from 0)
      EXPECT_FALSE(a->hasNulls);
    else
      EXPECT_TRUE(a->hasNulls);
    if (resultOffset + batchSize <
        4)  // The first int null is the 3rd element (if we count from 0)
      EXPECT_FALSE(b->hasNulls);
    else
      EXPECT_TRUE(b->hasNulls);
    for (int64_t i = 0; i < batchSize; i++) {
      res = (i + resultOffset) % 5;
      EXPECT_EQ(x->offsets[i], i);
      if (res >= 3) {
        EXPECT_EQ(x->tags[i], 1);
      } else {
        EXPECT_EQ(x->tags[i], 0);
      }
    }
    for (int64_t i = 0; i < childLengths[0]; i++) {
      switch ((i + childOffsets[0]) % 3) {
        case 0: {
          EXPECT_TRUE(a->notNull[i]);
          EXPECT_EQ(a->data[i], "Test " + std::to_string((i + childOffsets[0]) * 5 / 3));
          if ((i + childOffsets[0]) >= 60) {
            EXPECT_EQ(a->length[i], 8);
          } else if ((i + childOffsets[0]) >= 6) {
            EXPECT_EQ(a->length[i], 7);
          } else {
            EXPECT_EQ(a->length[i], 6);
          }
          break;
        }
        case 1: {
          EXPECT_TRUE(a->notNull[i]);
          EXPECT_EQ(a->data[i],
                    "Test " + std::to_string((5 * (i + childOffsets[0]) - 2) / 3));
          if ((i + childOffsets[0]) >= 60) {
            EXPECT_EQ(a->length[i], 8);
          } else if ((i + childOffsets[0]) >= 6) {
            EXPECT_EQ(a->length[i], 7);
          } else {
            EXPECT_EQ(a->length[i], 6);
          }
          break;
        }
        default: {
          EXPECT_FALSE(a->notNull[i]);
        }
      }
    }
    for (int64_t i = 0; i < childLengths[1]; i++) {
      if ((i + childOffsets[1]) % 2) {
        EXPECT_TRUE(b->notNull[i]);
        EXPECT_EQ(b->data[i], (5 * (i + childOffsets[1]) + 3) / 2);
      } else {
        EXPECT_FALSE(b->notNull[i]);
      }
    }
    resultOffset = resultOffset + batchSize;
    batch->clear();
  }
  Status st = adapters::orc::FillBatch(arrowType, x, arrowIndexOffset, arrowChunkOffset,
                                       batchSize, carray.get());
  int64_t lastBatchSize = totalLength - resultOffset;
  testDenseUnionStringOffsetGenerator(0, resultOffset, childOffsets);
  testDenseUnionStringOffsetGenerator(resultOffset, totalLength, childLengths);
  // RecordProperty("ro", resultOffset);
  // RecordProperty("cl0", childLengths[0]);
  // RecordProperty("cl1", childLengths[1]);
  // RecordProperty("co0", childOffsets[0]);
  // RecordProperty("co1", childOffsets[1]);
  EXPECT_EQ(x->numElements, lastBatchSize);
  EXPECT_EQ(a->numElements, childLengths[0]);
  EXPECT_EQ(b->numElements, childLengths[1]);
  EXPECT_FALSE(x->hasNulls);
  EXPECT_TRUE(a->hasNulls);
  EXPECT_TRUE(b->hasNulls);
  for (int64_t i = 0; i < lastBatchSize; i++) {
    res = (i + resultOffset) % 5;
    EXPECT_EQ(x->offsets[i], i);
    if (res >= 3) {
      EXPECT_EQ(x->tags[i], 1);
    } else {
      EXPECT_EQ(x->tags[i], 0);
    }
  }
  for (int64_t i = 0; i < childLengths[0]; i++) {
    // RecordProperty("an-res" + std::to_string(i + childOffsets[0]), a->notNull[i]);
    // RecordProperty("av-res" + std::to_string(i + childOffsets[0]), a->data[i]);
    switch ((i + childOffsets[0]) % 3) {
      case 0: {
        EXPECT_TRUE(a->notNull[i]);
        EXPECT_EQ(a->data[i], "Test " + std::to_string((i + childOffsets[0]) * 5 / 3));
        if ((i + childOffsets[0]) >= 60) {
          EXPECT_EQ(a->length[i], 8);
        } else if ((i + childOffsets[0]) >= 6) {
          EXPECT_EQ(a->length[i], 7);
        } else {
          EXPECT_EQ(a->length[i], 6);
        }
        break;
      }
      case 1: {
        EXPECT_TRUE(a->notNull[i]);
        EXPECT_EQ(a->data[i],
                  "Test " + std::to_string((5 * (i + childOffsets[0]) - 2) / 3));
        if ((i + childOffsets[0]) >= 60) {
          EXPECT_EQ(a->length[i], 8);
        } else if ((i + childOffsets[0]) >= 6) {
          EXPECT_EQ(a->length[i], 7);
        } else {
          EXPECT_EQ(a->length[i], 6);
        }
        break;
      }
      default: {
        EXPECT_FALSE(a->notNull[i]);
      }
    }
  }
  for (int64_t i = 0; i < childLengths[1]; i++) {
    if ((i + childOffsets[1]) % 2) {
      EXPECT_TRUE(b->notNull[i]);
      EXPECT_EQ(b->data[i], (5 * (i + childOffsets[1]) + 3) / 2);
    } else {
      EXPECT_FALSE(b->notNull[i]);
    }
  }
  writer->add(*batch);
  writer->close();
}

TEST(TestAdapterWriteWrapper, wrapInt8NoNulls) {
  ORC_UNIQUE_PTR<liborc::OutputStream> outStream =
      liborc::writeLocalFile("/Users/karlkatzen/Documents/code/orc-files/test2.orc");
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:tinyint>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, outStream.get(), options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::LongVectorBatch* x =
      internal::checked_cast<liborc::LongVectorBatch*>(root->fields[0]);
  x->data[0] = 1;
  x->data[1] = 2;
  x->numElements = 2;
  root->numElements = 2;
  writer->add(*batch);
  writer->close();
}
TEST(TestAdapterWriteWrapper, wrapInt8NoNullsApp) {
  std::shared_ptr<io::FileOutputStream> file =
      std::move(io::FileOutputStream::Open(
                    "/Users/karlkatzen/Documents/code/orc-files/test.orc", false))
          .ValueOrDie();
  std::unique_ptr<adapters::orc::ArrowOutputFile> io_wrapper(
      new adapters::orc::ArrowOutputFile(file));
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:tinyint>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer =
      createWriter(*schema, io_wrapper.get(), options);
  uint64_t batchSize = 1024;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root =
      internal::checked_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::LongVectorBatch* x =
      internal::checked_cast<liborc::LongVectorBatch*>(root->fields[0]);
  x->data[0] = 1;
  x->data[0] = 1;
  x->data[1] = 2;
  x->numElements = 2;
  root->numElements = 2;
  writer->add(*batch);
  writer->close();
}

TEST(TestAdapterWriteWrapper, wrapInt) {
  MemoryOutputStream mem_stream(DEFAULT_SMALL_MEM_STREAM_SIZE);
  ORC_UNIQUE_PTR<liborc::Type> schema(
      liborc::Type::buildTypeFromString("struct<x:int,y:int>"));
  liborc::WriterOptions options;
  ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, &mem_stream, options);
  uint64_t batchSize = 1024, rowCount = 10000;
  ORC_UNIQUE_PTR<liborc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  liborc::StructVectorBatch* root = dynamic_cast<liborc::StructVectorBatch*>(batch.get());
  liborc::LongVectorBatch* x = dynamic_cast<liborc::LongVectorBatch*>(root->fields[0]);
  liborc::LongVectorBatch* y = dynamic_cast<liborc::LongVectorBatch*>(root->fields[1]);

  uint64_t rows = 0;
  for (uint64_t i = 0; i < rowCount; ++i) {
    x->data[rows] = i;
    y->data[rows] = i * 3;
    rows++;

    if (rows == batchSize) {
      root->numElements = rows;
      x->numElements = rows;
      y->numElements = rows;

      writer->add(*batch);
      rows = 0;
    }
  }

  if (rows != 0) {
    root->numElements = rows;
    x->numElements = rows;
    y->numElements = rows;

    writer->add(*batch);
    rows = 0;
  }

  writer->close();
  RecordProperty("length", mem_stream.getLength());
}
// TEST(TestAdapterWriteWrapper, writeLListChunkedMultibatch) {
//   auto sharedPtrArrowType = list(std::make_shared<Field>("a", int32()));
//   DataType* arrowType = sharedPtrArrowType.get();
//   int64_t totalLength = 100;
//   int64_t totalElementLength = 150;
//   int64_t batchSize = 75;
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

//   ORC_UNIQUE_PTR<liborc::OutputStream> outStream =
//       liborc::writeLocalFile("/Users/karlkatzen/Documents/code/orc-files/test3.orc");
//   ORC_UNIQUE_PTR<liborc::Type> schema(
//       liborc::Type::buildTypeFromString("struct<x:array<a:int>>"));
//   liborc::WriterOptions options;
//   ORC_UNIQUE_PTR<liborc::Writer> writer = createWriter(*schema, outStream.get(),
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
//     root->numElements = x->numElements;
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
//     EXPECT_EQ(x->offsets[batchSize], testListOffsetGenerator(batchSize + resultOffset)
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
//       // RecordProperty("an-res" + std::to_string(j + oldValueOffset), a->notNull[j]);
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
//   root->numElements = x->numElements;
//   writer->add(*batch);
//   writer->close();
// }
TEST(TestAdapterWriteWrapper, cat) {
  int8_t buf[16] = {12, 103, 23, 13, 43, 21, 2, 42, 42, -110, 32, -21, 34, -3, 21, -104};
  std::shared_ptr<io::FileOutputStream> file =
      std::move(io::FileOutputStream::Open(
                    "/Users/karlkatzen/Documents/code/orc-files/test", false))
          .ValueOrDie();
  std::unique_ptr<adapters::orc::ArrowOutputFile> io_wrapper(
      new adapters::orc::ArrowOutputFile(file));
  io_wrapper->write(buf, 16);
  io_wrapper->close();
  ORC_UNIQUE_PTR<liborc::OutputStream> outStream =
      liborc::writeLocalFile("/Users/karlkatzen/Documents/code/orc-files/test-old");
  outStream->write(buf, 16);
  outStream->close();
}
TEST(TestAdapterWriteWrapper, wrapperTest) {
  std::vector<std::shared_ptr<Field>> xFields{
      arrow::field("a", int8()), arrow::field("b", float64()), arrow::field("c", utf8())};
  std::shared_ptr<Schema> sharedPtrSchema = std::make_shared<Schema>(xFields);
  Int8Builder builder10, builder11;
  DoubleBuilder builder2;
  StringBuilder builder3;
  std::shared_ptr<Array> array10, array11, array2, array3;
  (void)(builder10.Append(1));
  (void)(builder10.AppendNull());
  (void)(builder10.Append(-8));
  (void)(builder10.Finish(&array10));
  (void)(builder11.AppendNull());
  (void)(builder11.Finish(&array11));
  (void)(builder2.AppendNull());
  (void)(builder2.AppendNull());
  (void)(builder2.Append(6.5));
  (void)(builder2.Append(-82.52));
  (void)(builder2.Finish(&array2));
  (void)(builder3.Append("Shalom"));
  (void)(builder3.AppendNull());
  (void)(builder3.AppendNull());
  (void)(builder3.Append("Baruch"));
  (void)(builder3.Finish(&array3));
  ArrayVector av1, av2, av3;
  av1.push_back(array10);
  av1.push_back(array11);
  std::shared_ptr<ChunkedArray> carray1 = std::make_shared<ChunkedArray>(av1);
  av2.push_back(array2);
  std::shared_ptr<ChunkedArray> carray2 = std::make_shared<ChunkedArray>(av2);
  av3.push_back(array3);
  std::shared_ptr<ChunkedArray> carray3 = std::make_shared<ChunkedArray>(av3);
  ChunkedArrayVector cv;
  cv.push_back(carray1);
  cv.push_back(carray2);
  cv.push_back(carray3);
  std::shared_ptr<Table> table = Table::Make(sharedPtrSchema, cv);

  std::shared_ptr<liborc::WriterOptions> options =
      std::make_shared<liborc::WriterOptions>();
  std::shared_ptr<adapters::orc::ArrowWriterOptions> arrow_options =
      std::make_shared<adapters::orc::ArrowWriterOptions>(1);
  auto result =
      std::unique_ptr<adapters::orc::ORCFileWriter>(new adapters::orc::ORCFileWriter());
  ORC_THROW_NOT_OK(result->Open(sharedPtrSchema,
                                "/Users/karlkatzen/Documents/code/orc-files/test3.orc",
                                options, arrow_options));
  ORC_THROW_NOT_OK(result->Write(table));
}  // namespace arrow
TEST(TestAdapterWriteWrapper, wrapperTest2) {
  std::vector<std::shared_ptr<Field>> xFields{
      arrow::field("a", int8()), arrow::field("b", float64()), arrow::field("c", utf8())};
  std::shared_ptr<Schema> sharedPtrSchema = std::make_shared<Schema>(xFields);
  Int8Builder builder10, builder11;
  DoubleBuilder builder2;
  StringBuilder builder3;
  std::shared_ptr<Array> array10, array11, array2, array3;
  (void)(builder10.Append(1));
  (void)(builder10.AppendNull());
  (void)(builder10.Append(-8));
  (void)(builder10.Finish(&array10));
  (void)(builder11.AppendNull());
  (void)(builder11.Finish(&array11));
  (void)(builder2.AppendNull());
  (void)(builder2.AppendNull());
  (void)(builder2.Append(6.5));
  (void)(builder2.Append(-82.52));
  (void)(builder2.Finish(&array2));
  (void)(builder3.Append("Shalom"));
  (void)(builder3.AppendNull());
  (void)(builder3.AppendNull());
  (void)(builder3.Append("Baruch"));
  (void)(builder3.Finish(&array3));
  ArrayVector av1, av2, av3;
  av1.push_back(array10);
  av1.push_back(array11);
  std::shared_ptr<ChunkedArray> carray1 = std::make_shared<ChunkedArray>(av1);
  av2.push_back(array2);
  std::shared_ptr<ChunkedArray> carray2 = std::make_shared<ChunkedArray>(av2);
  av3.push_back(array3);
  std::shared_ptr<ChunkedArray> carray3 = std::make_shared<ChunkedArray>(av3);
  ChunkedArrayVector cv;
  cv.push_back(carray1);
  cv.push_back(carray2);
  cv.push_back(carray3);
  std::shared_ptr<Table> table = Table::Make(sharedPtrSchema, cv);

  std::shared_ptr<liborc::WriterOptions> options =
      std::make_shared<liborc::WriterOptions>();
  std::shared_ptr<adapters::orc::ArrowWriterOptions> arrow_options =
      std::make_shared<adapters::orc::ArrowWriterOptions>(1);
  auto result = std::unique_ptr<adapters::orc::ORCFileWriter::Impl>(
      new adapters::orc::ORCFileWriter::Impl());

  std::unique_ptr<liborc::Writer> liborc_writer;
  std::unique_ptr<liborc::Type> orcSchema;
  std::shared_ptr<io::FileOutputStream> file =
      std::move(io::FileOutputStream::Open(
                    "/Users/karlkatzen/Documents/code/orc-files/test4.orc", false))
          .ValueOrDie();
  ORC_THROW_NOT_OK(result->Open(sharedPtrSchema, file, options, arrow_options));
  ORC_THROW_NOT_OK(result->Write(table));
}
}  // namespace arrow
