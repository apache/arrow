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

#include <gmock/gmock-matchers.h>
#include <gtest/gtest.h>

#include <orc/OrcFile.hh>
#include <string>

#include "arrow/adapters/orc/adapter.h"
#include "arrow/adapters/orc/util.h"
#include "arrow/array.h"
#include "arrow/buffer.h"
#include "arrow/buffer_builder.h"
#include "arrow/chunked_array.h"
#include "arrow/compute/cast.h"
#include "arrow/io/interfaces.h"
#include "arrow/io/memory.h"
#include "arrow/status.h"
#include "arrow/table.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/random.h"
#include "arrow/type.h"
#include "arrow/util/key_value_metadata.h"

namespace liborc = orc;

namespace arrow {

using internal::checked_pointer_cast;

constexpr size_t kDefaultSmallMemStreamSize = 16384 * 5;  // 80KB
constexpr size_t kDefaultMemStreamSize = 10 * 1024 * 1024;
constexpr int64_t kNanoMax = std::numeric_limits<int64_t>::max();
constexpr int64_t kNanoMin = std::numeric_limits<int64_t>::lowest();
const int64_t kMicroMax = static_cast<int64_t>(std::floor(kNanoMax / 1000));
const int64_t kMicroMin = static_cast<int64_t>(std::ceil(kNanoMin / 1000));
const int64_t kMilliMax = static_cast<int64_t>(std::floor(kMicroMax / 1000));
const int64_t kMilliMin = static_cast<int64_t>(std::ceil(kMicroMin / 1000));
const int64_t kSecondMax = static_cast<int64_t>(std::floor(kMilliMax / 1000));
const int64_t kSecondMin = static_cast<int64_t>(std::ceil(kMilliMin / 1000));

static constexpr random::SeedType kRandomSeed = 0x0ff1ce;

class MemoryOutputStream : public liborc::OutputStream {
 public:
  explicit MemoryOutputStream(size_t capacity)
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

std::shared_ptr<Buffer> GenerateFixedDifferenceBuffer(int32_t fixed_length,
                                                      int64_t length) {
  BufferBuilder builder;
  std::vector<int32_t> offsets;
  offsets.resize(length);
  ARROW_EXPECT_OK(builder.Resize(4 * length));
  for (int64_t i = 0; i < length; i++) {
    offsets[i] = static_cast<int32_t>(fixed_length * i);
  }
  ARROW_EXPECT_OK(builder.Append(offsets.data(), 4 * length));
  std::shared_ptr<Buffer> buffer;
  ARROW_EXPECT_OK(builder.Finish(&buffer));
  return buffer;
}

std::shared_ptr<Array> CastFixedSizeBinaryArrayToBinaryArray(
    std::shared_ptr<Array> array) {
  auto fixed_size_binary_array = checked_pointer_cast<FixedSizeBinaryArray>(array);
  std::shared_ptr<Buffer> value_offsets = GenerateFixedDifferenceBuffer(
      fixed_size_binary_array->byte_width(), array->length() + 1);
  return std::make_shared<BinaryArray>(array->length(), value_offsets,
                                       array->data()->buffers[1],
                                       array->data()->buffers[0]);
}

template <typename TargetArrayType>
std::shared_ptr<Array> CastInt64ArrayToTemporalArray(
    const std::shared_ptr<DataType>& type, std::shared_ptr<Array> array) {
  std::shared_ptr<ArrayData> new_array_data =
      ArrayData::Make(type, array->length(), array->data()->buffers);
  return std::make_shared<TargetArrayType>(new_array_data);
}

Result<std::shared_ptr<Array>> GenerateRandomDate64Array(int64_t size,
                                                         double null_probability) {
  random::RandomArrayGenerator rand(kRandomSeed);
  return CastInt64ArrayToTemporalArray<Date64Array>(
      date64(), rand.Int64(size, kMilliMin, kMilliMax, null_probability));
}

Result<std::shared_ptr<Array>> GenerateRandomTimestampArray(
    int64_t size, std::shared_ptr<TimestampType> type, double null_probability) {
  random::RandomArrayGenerator rand(kRandomSeed);
  switch (type->unit()) {
    case TimeUnit::type::SECOND: {
      return CastInt64ArrayToTemporalArray<TimestampArray>(
          type, rand.Int64(size, kSecondMin, kSecondMax, null_probability));
    }
    case TimeUnit::type::MILLI: {
      return CastInt64ArrayToTemporalArray<TimestampArray>(
          type, rand.Int64(size, kMilliMin, kMilliMax, null_probability));
    }
    case TimeUnit::type::MICRO: {
      return CastInt64ArrayToTemporalArray<TimestampArray>(
          type, rand.Int64(size, kMicroMin, kMicroMax, null_probability));
    }
    case TimeUnit::type::NANO: {
      return CastInt64ArrayToTemporalArray<TimestampArray>(
          type, rand.Int64(size, kNanoMin, kNanoMax, null_probability));
    }
    default: {
      return Status::TypeError("Unknown or unsupported Arrow TimeUnit: ", type);
    }
  }
}

/// \brief Construct a random weak composition of a nonnegative integer
/// i.e. a way of writing it as the sum of a sequence of n non-negative
/// integers.
///
/// \param[in] n the number of integers in the weak composition
/// \param[in] sum the integer of which a random weak composition is generated
/// \param[out] out The generated weak composition
template <typename T, typename U>
void RandWeakComposition(int64_t n, T sum, std::vector<U>* out) {
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
  std::shuffle(out->begin(), out->end(), gen);
}

std::shared_ptr<ChunkedArray> GenerateRandomChunkedArray(
    const std::shared_ptr<DataType>& data_type, int64_t size, int64_t min_num_chunks,
    int64_t max_num_chunks, double null_probability) {
  random::RandomArrayGenerator rand(kRandomSeed);
  std::vector<int64_t> num_chunks(1, 0);
  std::vector<int64_t> current_size_chunks;
  randint<int64_t, int64_t>(1, min_num_chunks, max_num_chunks, &num_chunks);
  int64_t current_num_chunks = num_chunks[0];
  ArrayVector arrays(current_num_chunks, nullptr);
  RandWeakComposition(current_num_chunks, size, &current_size_chunks);
  for (int j = 0; j < current_num_chunks; j++) {
    switch (data_type->id()) {
      case Type::DATE64: {
        EXPECT_OK_AND_ASSIGN(arrays[j], GenerateRandomDate64Array(current_size_chunks[j],
                                                                  null_probability));
        break;
      }
      case Type::TIMESTAMP: {
        EXPECT_OK_AND_ASSIGN(arrays[j],
                             GenerateRandomTimestampArray(
                                 current_size_chunks[j],
                                 internal::checked_pointer_cast<TimestampType>(data_type),
                                 null_probability));
        break;
      }
      default:
        arrays[j] = rand.ArrayOf(data_type, current_size_chunks[j], null_probability);
    }
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

void AssertTableWriteReadEqual(const std::vector<std::shared_ptr<Table>>& input_tables,
                               const std::shared_ptr<Table>& expected_output_table,
                               const int64_t max_size = kDefaultSmallMemStreamSize,
                               std::vector<int>* opt_selected_read_indices = nullptr) {
  EXPECT_OK_AND_ASSIGN(auto buffer_output_stream,
                       io::BufferOutputStream::Create(max_size));
  auto write_options = adapters::orc::WriteOptions();
#ifdef ARROW_WITH_SNAPPY
  write_options.compression = Compression::SNAPPY;
#else
  write_options.compression = Compression::UNCOMPRESSED;
#endif
  write_options.file_version = adapters::orc::FileVersion(0, 11);
  write_options.compression_block_size = 32768;
  write_options.row_index_stride = 5000;
  EXPECT_OK_AND_ASSIGN(auto writer, adapters::orc::ORCFileWriter::Open(
                                        buffer_output_stream.get(), write_options));
  for (const auto& input_table : input_tables) {
    ARROW_EXPECT_OK(writer->Write(*input_table));
  }
  ARROW_EXPECT_OK(writer->Close());
  EXPECT_OK_AND_ASSIGN(auto buffer, buffer_output_stream->Finish());
  std::shared_ptr<io::RandomAccessFile> in_stream(new io::BufferReader(buffer));
  EXPECT_OK_AND_ASSIGN(
      auto reader, adapters::orc::ORCFileReader::Open(in_stream, default_memory_pool()));
  ASSERT_EQ(reader->GetFileVersion(), write_options.file_version);
  ASSERT_EQ(reader->GetCompression(), write_options.compression);
  ASSERT_EQ(reader->GetCompressionSize(), write_options.compression_block_size);
  ASSERT_EQ(reader->GetRowIndexStride(), write_options.row_index_stride);
  EXPECT_OK_AND_ASSIGN(auto actual_output_table,
                       opt_selected_read_indices == nullptr
                           ? reader->Read()
                           : reader->Read(*opt_selected_read_indices));
  ASSERT_OK(actual_output_table->ValidateFull());
  AssertTablesEqual(*expected_output_table, *actual_output_table, false, false);
}

void AssertBatchWriteReadEqual(
    const std::vector<std::shared_ptr<RecordBatch>>& input_batches,
    const std::shared_ptr<Table>& expected_output_table,
    const int64_t max_size = kDefaultSmallMemStreamSize) {
  EXPECT_OK_AND_ASSIGN(auto buffer_output_stream,
                       io::BufferOutputStream::Create(max_size));
  auto write_options = adapters::orc::WriteOptions();
#ifdef ARROW_WITH_SNAPPY
  write_options.compression = Compression::SNAPPY;
#else
  write_options.compression = Compression::UNCOMPRESSED;
#endif
  write_options.file_version = adapters::orc::FileVersion(0, 11);
  write_options.compression_block_size = 32768;
  write_options.row_index_stride = 5000;
  EXPECT_OK_AND_ASSIGN(auto writer, adapters::orc::ORCFileWriter::Open(
                                        buffer_output_stream.get(), write_options));
  for (auto& input_batch : input_batches) {
    ARROW_EXPECT_OK(writer->Write(*input_batch));
  }
  ARROW_EXPECT_OK(writer->Close());
  EXPECT_OK_AND_ASSIGN(auto buffer, buffer_output_stream->Finish());
  std::shared_ptr<io::RandomAccessFile> in_stream(new io::BufferReader(buffer));
  EXPECT_OK_AND_ASSIGN(
      auto reader, adapters::orc::ORCFileReader::Open(in_stream, default_memory_pool()));
  ASSERT_EQ(reader->GetFileVersion(), write_options.file_version);
  ASSERT_EQ(reader->GetCompression(), write_options.compression);
  ASSERT_EQ(reader->GetCompressionSize(), write_options.compression_block_size);
  ASSERT_EQ(reader->GetRowIndexStride(), write_options.row_index_stride);
  EXPECT_OK_AND_ASSIGN(auto actual_output_table, reader->Read());
  AssertTablesEqual(*expected_output_table, *actual_output_table, false, false);
}

void AssertTableWriteReadEqual(const std::shared_ptr<Table>& input_table,
                               const std::shared_ptr<Table>& expected_output_table,
                               const int64_t max_size = kDefaultSmallMemStreamSize,
                               std::vector<int>* opt_selected_read_indices = nullptr) {
  std::vector<std::shared_ptr<Table>> input_tables;
  input_tables.push_back(input_table);
  AssertTableWriteReadEqual(input_tables, expected_output_table, max_size,
                            opt_selected_read_indices);
}
void AssertArrayWriteReadEqual(const std::shared_ptr<Array>& input_array,
                               const std::shared_ptr<Array>& expected_output_array,
                               const int64_t max_size = kDefaultSmallMemStreamSize) {
  std::shared_ptr<Schema> input_schema = schema({field("col0", input_array->type())}),
                          output_schema =
                              schema({field("col0", expected_output_array->type())});
  auto input_chunked_array = std::make_shared<ChunkedArray>(input_array),
       expected_output_chunked_array =
           std::make_shared<ChunkedArray>(expected_output_array);
  std::shared_ptr<Table> input_table = Table::Make(input_schema, {input_chunked_array}),
                         expected_output_table =
                             Table::Make(output_schema, {expected_output_chunked_array});
  AssertTableWriteReadEqual(input_table, expected_output_table, max_size);
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

TEST(TestAdapterRead, ReadIntAndStringFileMultipleStripes) {
  MemoryOutputStream mem_stream(kDefaultMemStreamSize);
  std::unique_ptr<liborc::Type> type(
      liborc::Type::buildTypeFromString("struct<col1:int,col2:string>"));

  constexpr uint64_t stripe_size = 1024;  // 1K
  constexpr uint64_t stripe_count = 10;
  constexpr uint64_t stripe_row_count = 16384;
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
    std::string data_buffer(stripe_row_count * 5, '\0');
    uint64_t offset = 0;
    for (uint64_t i = 0; i < stripe_row_count; ++i) {
      std::string str_data = std::to_string(accumulated % stripe_row_count);
      long_batch->data[i] = static_cast<int64_t>(accumulated % stripe_row_count);
      str_batch->data[i] = &data_buffer[offset];
      str_batch->length[i] = static_cast<int64_t>(str_data.size());
      memcpy(&data_buffer[offset], str_data.c_str(), str_data.size());
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

  ASSERT_OK_AND_ASSIGN(
      auto reader, adapters::orc::ORCFileReader::Open(in_stream, default_memory_pool()));

  EXPECT_OK_AND_ASSIGN(auto metadata, reader->ReadMetadata());
  auto expected_metadata = std::const_pointer_cast<const KeyValueMetadata>(
      key_value_metadata(std::vector<std::string>(), std::vector<std::string>()));
  ASSERT_TRUE(metadata->Equals(*expected_metadata));
  ASSERT_EQ(stripe_row_count * stripe_count, reader->NumberOfRows());
  ASSERT_EQ(stripe_count, reader->NumberOfStripes());
  ASSERT_EQ(static_cast<int64_t>(stripe_row_count),
            reader->GetStripeInformation(0).num_rows);
  ASSERT_EQ(static_cast<int64_t>(reader->NumberOfRows() - stripe_row_count),
            reader->GetStripeInformation(stripe_count - 1).first_row_id);
  accumulated = 0;
  EXPECT_OK_AND_ASSIGN(auto stripe_reader, reader->NextStripeReader(reader_batch_size));
  while (stripe_reader) {
    std::shared_ptr<RecordBatch> record_batch;
    EXPECT_TRUE(stripe_reader->ReadNext(&record_batch).ok());
    while (record_batch) {
      auto int32_array = checked_pointer_cast<Int32Array>(record_batch->column(0));
      auto str_array = checked_pointer_cast<StringArray>(record_batch->column(1));
      for (int j = 0; j < record_batch->num_rows(); ++j) {
        EXPECT_EQ(accumulated % stripe_row_count, int32_array->Value(j));
        EXPECT_EQ(std::to_string(accumulated % stripe_row_count),
                  str_array->GetString(j));
        accumulated++;
      }
      EXPECT_TRUE(stripe_reader->ReadNext(&record_batch).ok());
    }
    EXPECT_OK_AND_ASSIGN(stripe_reader, reader->NextStripeReader(reader_batch_size));
  }

  // test seek operation
  int64_t start_offset = 830;
  EXPECT_TRUE(reader->Seek(stripe_row_count + start_offset).ok());

  EXPECT_OK_AND_ASSIGN(stripe_reader, reader->NextStripeReader(reader_batch_size));
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

  // test GetRecordBatchReader interface
  EXPECT_TRUE(reader->Seek(0).ok());
  accumulated = 0;
  EXPECT_OK_AND_ASSIGN(auto record_batch_reader,
                       reader->GetRecordBatchReader(reader_batch_size, {"col1", "col2"}));
  int64_t batches = 0;
  int64_t num_rows = 0;
  for (const auto maybe_batch : *record_batch_reader) {
    ASSERT_OK_AND_ASSIGN(record_batch, maybe_batch);
    auto int32_array = checked_pointer_cast<Int32Array>(record_batch->column(0));
    auto str_array = checked_pointer_cast<StringArray>(record_batch->column(1));
    for (int j = 0; j < record_batch->num_rows(); ++j) {
      EXPECT_EQ(accumulated % stripe_row_count, int32_array->Value(j));
      EXPECT_EQ(std::to_string(accumulated % stripe_row_count), str_array->GetString(j));
      accumulated++;
    }
    EXPECT_LE(record_batch->num_rows(), reader_batch_size);
    num_rows += record_batch->num_rows();
    batches++;
  }
  EXPECT_EQ(num_rows, stripe_row_count * stripe_count);
  EXPECT_EQ(num_rows / reader_batch_size, batches);
}

TEST(TestAdapterRead, ReadCharAndVarcharType) {
  MemoryOutputStream mem_stream(kDefaultMemStreamSize);
  auto orc_type = liborc::Type::buildTypeFromString("struct<c1:char(6),c2:varchar(6)>");
  auto writer = CreateWriter(/*stripe_size=*/1024, *orc_type, &mem_stream);

  constexpr int64_t row_count = 2;
  auto batch = writer->createRowBatch(row_count);
  auto struct_batch = internal::checked_cast<liborc::StructVectorBatch*>(batch.get());

  // Verify that longer data will be truncated by ORC char and varchar types.
  // In addition, ORC char type will pad the data with spaces if the data is shorter.
  const std::vector<std::string> data = {"abcd", "ABCDEFGH"};
  const std::vector<std::vector<std::string>> expected_data = {{"abcd  ", "ABCDEF"},
                                                               {"abcd", "ABCDEF"}};

  for (uint64_t col = 0; col < orc_type->getSubtypeCount(); ++col) {
    auto str_batch =
        internal::checked_cast<liborc::StringVectorBatch*>(struct_batch->fields[col]);
    str_batch->hasNulls = false;
    str_batch->numElements = row_count;
    for (int64_t row = 0; row < row_count; ++row) {
      str_batch->data[row] = const_cast<char*>(data[row].c_str());
      str_batch->length[row] = static_cast<int64_t>(data[row].size());
    }
  }
  batch->numElements = row_count;
  writer->add(*batch);
  writer->close();

  std::shared_ptr<io::RandomAccessFile> in_stream(std::make_shared<io::BufferReader>(
      reinterpret_cast<const uint8_t*>(mem_stream.getData()),
      static_cast<int64_t>(mem_stream.getLength())));
  ASSERT_OK_AND_ASSIGN(
      auto reader, adapters::orc::ORCFileReader::Open(in_stream, default_memory_pool()));
  ASSERT_EQ(row_count, reader->NumberOfRows());
  ASSERT_EQ(1, reader->NumberOfStripes());

  EXPECT_OK_AND_ASSIGN(auto stripe_reader, reader->NextStripeReader(row_count));
  std::shared_ptr<RecordBatch> record_batch;
  ASSERT_OK(stripe_reader->ReadNext(&record_batch));
  ASSERT_NE(nullptr, record_batch);
  ASSERT_EQ(row_count, record_batch->num_rows());

  for (int col = 0; col < record_batch->num_columns(); ++col) {
    auto str_array = checked_pointer_cast<StringArray>(record_batch->column(col));
    for (int row = 0; row < row_count; ++row) {
      EXPECT_EQ(expected_data[col][row], str_array->GetString(row));
    }
  }
  ASSERT_OK(stripe_reader->ReadNext(&record_batch));
  ASSERT_EQ(nullptr, record_batch);
}

TEST(TestAdapterRead, ReadFieldAttributes) {
  const std::string id_key = "iceberg.id";
  const std::string required_key = "iceberg.required";

  auto set_attributes = [&](liborc::Type* type, const std::string& id,
                            const std::string& required) {
    type->setAttribute(id_key, id);
    type->setAttribute(required_key, required);
  };

  auto check_attributes = [&](const std::shared_ptr<arrow::Field>& field,
                              const std::string& expect_id,
                              const std::string& expect_required) {
    auto field_metadata = field->metadata();
    ASSERT_NE(field_metadata, nullptr);
    ASSERT_EQ(expect_id, field_metadata->Get(id_key));
    ASSERT_EQ(expect_required, field_metadata->Get(required_key));
  };

  auto c1_type = liborc::createPrimitiveType(liborc::TypeKind::INT);
  set_attributes(c1_type.get(), "1", "true");

  auto c2_elem_type = liborc::createPrimitiveType(liborc::TypeKind::INT);
  set_attributes(c2_elem_type.get(), "3", "false");
  auto c2_type = liborc::createListType(std::move(c2_elem_type));
  set_attributes(c2_type.get(), "2", "false");

  auto c3_key_type = liborc::createPrimitiveType(liborc::TypeKind::INT);
  set_attributes(c3_key_type.get(), "5", "true");
  auto c3_value_type = liborc::createPrimitiveType(liborc::TypeKind::INT);
  set_attributes(c3_value_type.get(), "6", "false");
  auto c3_type = liborc::createMapType(std::move(c3_key_type), std::move(c3_value_type));
  set_attributes(c3_type.get(), "4", "false");

  auto c4_sub_type = liborc::createPrimitiveType(liborc::TypeKind::INT);
  set_attributes(c4_sub_type.get(), "8", "false");
  auto c4_type = liborc::createStructType();
  c4_type->addStructField("c4_1", std::move(c4_sub_type));
  set_attributes(c4_type.get(), "7", "false");

  auto orc_type = liborc::createStructType();
  orc_type->addStructField("c1", std::move(c1_type));
  orc_type->addStructField("c2", std::move(c2_type));
  orc_type->addStructField("c3", std::move(c3_type));
  orc_type->addStructField("c4", std::move(c4_type));

  MemoryOutputStream mem_stream(kDefaultMemStreamSize);
  auto writer = CreateWriter(/*stripe_size=*/1024, *orc_type, &mem_stream);
  writer->close();

  std::shared_ptr<io::RandomAccessFile> in_stream(std::make_shared<io::BufferReader>(
      reinterpret_cast<const uint8_t*>(mem_stream.getData()),
      static_cast<int64_t>(mem_stream.getLength())));
  ASSERT_OK_AND_ASSIGN(
      auto reader, adapters::orc::ORCFileReader::Open(in_stream, default_memory_pool()));
  ASSERT_EQ(0, reader->NumberOfRows());

  ASSERT_OK_AND_ASSIGN(auto schema, reader->ReadSchema());
  ASSERT_EQ(4, schema->num_fields());

  // check top level fields
  check_attributes(schema->field(0), "1", "true");
  check_attributes(schema->field(1), "2", "false");
  check_attributes(schema->field(2), "4", "false");
  check_attributes(schema->field(3), "7", "false");

  // check list element type
  auto list_type = checked_pointer_cast<arrow::ListType>(schema->field(1)->type());
  check_attributes(list_type->value_field(), "3", "false");

  // check map key/value types
  auto map_type = checked_pointer_cast<arrow::MapType>(schema->field(2)->type());
  check_attributes(map_type->key_field(), "5", "true");
  check_attributes(map_type->item_field(), "6", "false");

  // check struct sub-field type
  auto struct_type = checked_pointer_cast<arrow::StructType>(schema->field(3)->type());
  check_attributes(struct_type->field(0), "8", "false");
}

TEST(TestAdapterReadWrite, FieldAttributesRoundTrip) {
  EXPECT_OK_AND_ASSIGN(auto buffer_output_stream, io::BufferOutputStream::Create(1024));
  auto write_options = adapters::orc::WriteOptions();
  write_options.compression = Compression::UNCOMPRESSED;
  EXPECT_OK_AND_ASSIGN(auto writer, adapters::orc::ORCFileWriter::Open(
                                        buffer_output_stream.get(), write_options));

  auto schema = ::arrow::schema(
      {::arrow::field("c0", ::arrow::int64(), /*nullable=*/true,
                      key_value_metadata({"k0"}, {"v0"})),
       ::arrow::field("c1", ::arrow::utf8(), /*nullable=*/true,
                      key_value_metadata({"k1"}, {"v1"})),
       ::arrow::field(
           "c2", ::arrow::list(::arrow::field("item", ::arrow::int64(), /*nullable=*/true,
                                              key_value_metadata({"k2"}, {"v2"})))),
       ::arrow::field("c3",
                      std::make_shared<MapType>(
                          ::arrow::field("key", ::arrow::utf8(), /*nullable=*/false,
                                         key_value_metadata({"k3"}, {"v3"})),
                          ::arrow::field("value", ::arrow::int64(), /*nullable=*/true,
                                         key_value_metadata({"k4"}, {"v4"})))),
       ::arrow::field("c4", ::arrow::struct_({::arrow::field(
                                "sub", ::arrow::int64(),
                                /*nullable=*/true, key_value_metadata({"k5"}, {"v5"}))})),
       ::arrow::field("c5",
                      ::arrow::sparse_union(
                          {::arrow::field("_union_0", ::arrow::int64(), /*nullable=*/true,
                                          key_value_metadata({"k6"}, {"v6"})),
                           ::arrow::field("_union_1", ::arrow::utf8(), /*nullable=*/true,
                                          key_value_metadata({"k7"}, {"v7"}))},
                          {0, 1}))});
  auto expected_output_table = ::arrow::TableFromJSON(
      schema, {R"([[1, "a", [1, 2], [["a", 1]], {"sub": 1}, null]])"});
  ARROW_EXPECT_OK(writer->Write(*expected_output_table));
  ARROW_EXPECT_OK(writer->Close());

  EXPECT_OK_AND_ASSIGN(auto buffer, buffer_output_stream->Finish());
  std::shared_ptr<io::RandomAccessFile> in_stream(new io::BufferReader(buffer));
  EXPECT_OK_AND_ASSIGN(
      auto reader, adapters::orc::ORCFileReader::Open(in_stream, default_memory_pool()));
  EXPECT_OK_AND_ASSIGN(auto actual_output_table, reader->Read());
  ASSERT_OK(actual_output_table->ValidateFull());
  AssertTablesEqual(*expected_output_table, *actual_output_table);

  // Check schema equality with metadata.
  EXPECT_OK_AND_ASSIGN(auto read_schema, reader->ReadSchema());
  AssertSchemaEqual(schema, read_schema, /*check_metadata=*/true);
}

// Trivial

class TestORCWriterTrivialNoWrite : public ::testing::Test {};
TEST_F(TestORCWriterTrivialNoWrite, noWrite) {
  EXPECT_OK_AND_ASSIGN(auto buffer_output_stream,
                       io::BufferOutputStream::Create(kDefaultSmallMemStreamSize / 16));
  auto write_options = adapters::orc::WriteOptions();
#ifdef ARROW_WITH_SNAPPY
  write_options.compression = Compression::SNAPPY;
#else
  write_options.compression = Compression::UNCOMPRESSED;
#endif
  write_options.file_version = adapters::orc::FileVersion(0, 11);
  write_options.compression_block_size = 32768;
  write_options.row_index_stride = 5000;
  EXPECT_OK_AND_ASSIGN(auto writer, adapters::orc::ORCFileWriter::Open(
                                        buffer_output_stream.get(), write_options));
  ARROW_EXPECT_OK(writer->Close());
}
class TestORCWriterTrivialNoConversion : public ::testing::Test {
 public:
  TestORCWriterTrivialNoConversion() {
    table_schema = schema(
        {field("bool", boolean()), field("int8", int8()), field("int16", int16()),
         field("int32", int32()), field("int64", int64()), field("float", float32()),
         field("double", float64()), field("decimal128nz", decimal128(25, 6)),
         field("decimal128z", decimal128(32, 0)), field("date32", date32()),
         field("ts3", timestamp(TimeUnit::NANO)),
         field("ts4", timestamp(TimeUnit::NANO, "UTC")), field("string", utf8()),
         field("binary", binary()),
         field("struct", struct_({field("a", utf8()), field("b", int64())})),
         field("list", list(int32())),
         field("lsl", list(struct_({field("lsl0", list(int32()))}))),
         field("map", map(utf8(), utf8()))});
  }

 protected:
  std::shared_ptr<Schema> table_schema;
};
TEST_F(TestORCWriterTrivialNoConversion, writeTrivialChunk) {
  std::shared_ptr<Table> table = TableFromJSON(table_schema, {R"([])"});
  AssertTableWriteReadEqual(table, table, kDefaultSmallMemStreamSize / 16);
}
TEST_F(TestORCWriterTrivialNoConversion, writeChunkless) {
  std::shared_ptr<Table> table = TableFromJSON(table_schema, {});
  AssertTableWriteReadEqual(table, table, kDefaultSmallMemStreamSize / 16);
}
TEST_F(TestORCWriterTrivialNoConversion, writeTrivialChunkAndSelectField) {
  std::shared_ptr<Table> table = TableFromJSON(table_schema, {R"([])"});
  std::shared_ptr<Schema> schema_selected =
      schema({field("int8", int8()), field("int32", int32())});
  std::shared_ptr<Table> table_selected = TableFromJSON(schema_selected, {R"([])"});
  std::vector<int> selected_indices = {1, 3};
  AssertTableWriteReadEqual(table, table_selected, kDefaultSmallMemStreamSize / 16,
                            &selected_indices);
}
TEST_F(TestORCWriterTrivialNoConversion, writeFilledChunkAndSelectField) {
  std::vector<int> selected_indices = {1, 7};
  random::RandomArrayGenerator rand(kRandomSeed);
  std::shared_ptr<Schema> local_schema = schema({
      field("bool", boolean()),
      field("int32", int32()),
      field("int64", int64()),
      field("float", float32()),
      field("struct", struct_({field("a", utf8()), field("b", int64())})),
      field("double", float64()),
      field("date32", date32()),
      field("ts3", timestamp(TimeUnit::NANO)),
      field("ts4", timestamp(TimeUnit::NANO), "UTC"),
      field("string", utf8()),
      field("binary", binary()),
  });
  auto batch = rand.BatchOf(local_schema->fields(), 100);
  std::shared_ptr<Table> table = Table::Make(local_schema, batch->columns());
  EXPECT_OK_AND_ASSIGN(auto table_selected, table->SelectColumns(selected_indices));
  AssertTableWriteReadEqual(table, table_selected, kDefaultSmallMemStreamSize,
                            &selected_indices);
}

TEST_F(TestORCWriterTrivialNoConversion, writeSlicedBatch) {
  std::shared_ptr<Table> table =
      GenerateRandomTable(table_schema, /*size=*/100, /*min_num_chunks=*/1,
                          /*max_num_chunks*/ 1, /*probability=*/0);
  table = table->Slice(20, 60);

  AssertTableWriteReadEqual(table, table, kDefaultSmallMemStreamSize / 16);
}

class TestORCWriterTrivialWithConversion : public ::testing::Test {
 public:
  TestORCWriterTrivialWithConversion() {
    input_schema = schema(
        {field("date64", date64()), field("ts0", timestamp(TimeUnit::SECOND)),
         field("ts1", timestamp(TimeUnit::MILLI)),
         field("ts2", timestamp(TimeUnit::MICRO)), field("large_string", large_utf8()),
         field("large_binary", large_binary()),
         field("fixed_size_binary0", fixed_size_binary(0)),
         field("fixed_size_binary", fixed_size_binary(5)),
         field("large_list", large_list(int32())),
         field("fixed_size_list", fixed_size_list(int32(), 3))}),
    output_schema = schema(
        {field("date64", timestamp(TimeUnit::NANO)),
         field("ts0", timestamp(TimeUnit::NANO)), field("ts1", timestamp(TimeUnit::NANO)),
         field("ts2", timestamp(TimeUnit::NANO)), field("large_string", utf8()),
         field("large_binary", binary()), field("fixed_size_binary0", binary()),
         field("fixed_size_binary", binary()), field("large_list", list(int32())),
         field("fixed_size_list", list(int32()))});
  }

 protected:
  std::shared_ptr<Schema> input_schema, output_schema;
};
TEST_F(TestORCWriterTrivialWithConversion, writeTrivialChunk) {
  std::shared_ptr<Table> input_table = TableFromJSON(input_schema, {R"([])"}),
                         expected_output_table = TableFromJSON(output_schema, {R"([])"});
  AssertTableWriteReadEqual(input_table, expected_output_table,
                            kDefaultSmallMemStreamSize / 16);
}
TEST_F(TestORCWriterTrivialWithConversion, writeChunkless) {
  std::shared_ptr<Table> input_table = TableFromJSON(input_schema, {}),
                         expected_output_table = TableFromJSON(output_schema, {});
  AssertTableWriteReadEqual(input_table, expected_output_table,
                            kDefaultSmallMemStreamSize / 16);
}

class TestORCWriterInvalidTypes : public ::testing::Test {};

TEST_F(TestORCWriterInvalidTypes, noWriteInvalidTypes) {
  // Unsigned integers are not supported by ORC
  std::shared_ptr<arrow::Schema> table_schema = schema({field("uint64", uint64())});
  const std::shared_ptr<Table> table = GenerateRandomTable(table_schema, 100, 1, 1, 0);
  EXPECT_OK_AND_ASSIGN(auto buffer_output_stream,
                       io::BufferOutputStream::Create(kDefaultSmallMemStreamSize / 16));
  EXPECT_OK_AND_ASSIGN(auto writer,
                       adapters::orc::ORCFileWriter::Open(buffer_output_stream.get()));
  EXPECT_RAISES_WITH_MESSAGE_THAT(NotImplemented,
                                  testing::HasSubstr("Unknown or unsupported Arrow type"),
                                  writer->Write(*table));
}

// General

class TestORCWriterNoConversion : public ::testing::Test {
 public:
  TestORCWriterNoConversion() {
    table_schema = schema(
        {field("bool", boolean()), field("int8", int8()), field("int16", int16()),
         field("int32", int32()), field("int64", int64()), field("float", float32()),
         field("double", float64()), field("date32", date32()),
         field("decimal64", decimal128(18, 4)), field("decimal64z", decimal128(18, 0)),
         field("ts3", timestamp(TimeUnit::NANO)), field("string", utf8()),
         field("binary", binary())});
  }

 protected:
  std::shared_ptr<Schema> table_schema;
};
TEST_F(TestORCWriterNoConversion, writeNoNulls) {
  SchemaORCWriteReadTest(table_schema, 11203, 5, 10, 0, kDefaultSmallMemStreamSize * 5);
}
TEST_F(TestORCWriterNoConversion, writeMixed) {
  SchemaORCWriteReadTest(table_schema, 9405, 1, 20, 0.6, kDefaultSmallMemStreamSize * 5);
}
TEST_F(TestORCWriterNoConversion, writeAllNulls) {
  SchemaORCWriteReadTest(table_schema, 4006, 1, 5, 1);
}

// Converts
// Since Arrow has way more types than ORC type conversions are unavoidable
class TestORCWriterWithConversion : public ::testing::Test {
 public:
  TestORCWriterWithConversion() {
    input_schema = schema(
        {field("date64", date64()), field("ts0", timestamp(TimeUnit::SECOND)),
         field("ts1", timestamp(TimeUnit::MILLI)),
         field("ts2", timestamp(TimeUnit::MICRO)), field("large_string", large_utf8()),
         field("large_binary", large_binary()),
         field("fixed_size_binary0", fixed_size_binary(0)),
         field("fixed_size_binary", fixed_size_binary(5))}),
    output_schema = schema(
        {field("date64", timestamp(TimeUnit::NANO)),
         field("ts0", timestamp(TimeUnit::NANO)), field("ts1", timestamp(TimeUnit::NANO)),
         field("ts2", timestamp(TimeUnit::NANO)), field("large_string", utf8()),
         field("large_binary", binary()), field("fixed_size_binary0", binary()),
         field("fixed_size_binary", binary())});
  }
  void RunTest(int64_t num_rows, double null_possibility,
               int64_t max_size = kDefaultSmallMemStreamSize) {
    int64_t num_cols = (input_schema->fields()).size();
    std::shared_ptr<Table> input_table =
        GenerateRandomTable(input_schema, num_rows, 1, 1, null_possibility);
    ArrayVector av(num_cols);
    for (int i = 0; i < num_cols - 2; i++) {
      EXPECT_OK_AND_ASSIGN(av[i], compute::Cast(*(input_table->column(i)->chunk(0)),
                                                output_schema->field(i)->type()));
    }
    for (int i = static_cast<int>(num_cols - 2); i < static_cast<int>(num_cols); i++) {
      av[i] = CastFixedSizeBinaryArrayToBinaryArray(input_table->column(i)->chunk(0));
    }
    std::shared_ptr<Table> expected_output_table = Table::Make(output_schema, av);
    AssertTableWriteReadEqual(input_table, expected_output_table, max_size);
  }

 protected:
  std::shared_ptr<Schema> input_schema, output_schema;
};
TEST_F(TestORCWriterWithConversion, writeAllNulls) { RunTest(12000, 1); }
TEST_F(TestORCWriterWithConversion, writeNoNulls) { RunTest(10009, 0); }
TEST_F(TestORCWriterWithConversion, writeMixed) { RunTest(8021, 0.5); }

class TestORCWriterSingleArray : public ::testing::Test {
 public:
  TestORCWriterSingleArray() : rand(kRandomSeed) {}

 protected:
  random::RandomArrayGenerator rand;
};

// Nested types
TEST_F(TestORCWriterSingleArray, WriteStruct) {
  std::vector<std::shared_ptr<Field>> subfields{field("int32", boolean())};
  const int64_t num_rows = 1234;
  std::size_t num_subcols = subfields.size();
  ArrayVector av0(num_subcols);
  for (int i = 0; i < static_cast<int>(num_subcols); i++) {
    av0[i] = rand.ArrayOf(subfields[i]->type(), num_rows, 0.4);
  }
  std::shared_ptr<Buffer> bitmap = rand.NullBitmap(num_rows, 0.5);
  std::shared_ptr<Array> array =
      std::make_shared<StructArray>(struct_(subfields), num_rows, av0, bitmap);
  AssertArrayWriteReadEqual(array, array, kDefaultSmallMemStreamSize * 10);
}

TEST_F(TestORCWriterSingleArray, WriteStructOfStruct) {
  std::vector<std::shared_ptr<Field>> subsubfields{
      field("bool", boolean()),
      field("int8", int8()),
      field("int16", int16()),
      field("int32", int32()),
      field("int64", int64()),
      field("date32", date32()),
      field("ts3", timestamp(TimeUnit::NANO)),
      field("string", utf8()),
      field("binary", binary())};
  const int64_t num_rows = 1234;
  std::size_t num_subsubcols = subsubfields.size();
  ArrayVector av00(num_subsubcols), av0(1);
  for (int i = 0; i < static_cast<int>(num_subsubcols); i++) {
    av00[i] = rand.ArrayOf(subsubfields[i]->type(), num_rows, 0);
  }
  std::shared_ptr<Buffer> bitmap0 = rand.NullBitmap(num_rows, 0);
  av0[0] = std::make_shared<StructArray>(struct_(subsubfields), num_rows, av00, bitmap0);
  std::shared_ptr<Buffer> bitmap = rand.NullBitmap(num_rows, 0.2);
  std::shared_ptr<Array> array = std::make_shared<StructArray>(
      struct_({field("struct2", struct_(subsubfields))}), num_rows, av0, bitmap);
  AssertArrayWriteReadEqual(array, array, kDefaultSmallMemStreamSize * 10);
}

TEST_F(TestORCWriterSingleArray, WriteList) {
  const int64_t num_rows = 1234;
  auto value_array = rand.ArrayOf(int32(), 125 * num_rows, 0);
  std::shared_ptr<Array> array = rand.List(*value_array, num_rows, 1);
  AssertArrayWriteReadEqual(array, array, kDefaultSmallMemStreamSize * 100);
}

TEST_F(TestORCWriterSingleArray, WriteLargeList) {
  const int64_t num_rows = 1234;
  auto value_array = rand.ArrayOf(int32(), 5 * num_rows, 0.5);
  auto output_offsets = rand.Offsets(num_rows + 1, 0, 5 * num_rows, 0.6, false);
  EXPECT_OK_AND_ASSIGN(auto input_offsets, compute::Cast(*output_offsets, int64()));
  EXPECT_OK_AND_ASSIGN(auto input_array,
                       LargeListArray::FromArrays(*input_offsets, *value_array));
  EXPECT_OK_AND_ASSIGN(auto output_array,
                       ListArray::FromArrays(*output_offsets, *value_array));
  AssertArrayWriteReadEqual(input_array, output_array, kDefaultSmallMemStreamSize * 10);
}

TEST_F(TestORCWriterSingleArray, WriteFixedSizeList) {
  const int64_t num_rows = 1234;
  std::shared_ptr<Array> value_array = rand.ArrayOf(int32(), 3 * num_rows, 0.8);
  std::shared_ptr<Buffer> bitmap = rand.NullBitmap(num_rows, 1);
  std::shared_ptr<Buffer> buffer = GenerateFixedDifferenceBuffer(3, num_rows + 1);
  std::shared_ptr<Array> input_array = std::make_shared<FixedSizeListArray>(
                             fixed_size_list(int32(), 3), num_rows, value_array, bitmap),
                         output_array = std::make_shared<ListArray>(
                             list(int32()), num_rows, buffer, value_array, bitmap);
  AssertArrayWriteReadEqual(input_array, output_array, kDefaultSmallMemStreamSize * 10);
}

TEST_F(TestORCWriterSingleArray, WriteListOfList) {
  const int64_t num_rows = 1234;
  auto value_value_array = rand.ArrayOf(utf8(), 4 * num_rows, 0.5);
  std::shared_ptr<Array> value_array = rand.List(*value_value_array, 2 * num_rows, 0.7);
  std::shared_ptr<Array> array = rand.List(*value_array, num_rows, 0.4);
  AssertArrayWriteReadEqual(array, array, kDefaultSmallMemStreamSize * 10);
}

TEST_F(TestORCWriterSingleArray, WriteListOfListOfList) {
  const int64_t num_rows = 1234;
  auto value3_array = rand.ArrayOf(int64(), 12 * num_rows, 0.1);
  std::shared_ptr<Array> value2_array = rand.List(*value3_array, 5 * num_rows, 0);
  std::shared_ptr<Array> value_array = rand.List(*value2_array, 2 * num_rows, 0.1);
  std::shared_ptr<Array> array = rand.List(*value_array, num_rows, 0.1);
  AssertArrayWriteReadEqual(array, array, kDefaultSmallMemStreamSize * 35);
}

TEST_F(TestORCWriterSingleArray, WriteListOfStruct) {
  const int64_t num_rows = 1234, num_values = 3 * num_rows;
  ArrayVector av00(1);
  av00[0] = rand.ArrayOf(int32(), num_values, 0);
  std::shared_ptr<Buffer> bitmap = rand.NullBitmap(num_values, 0.2);
  std::shared_ptr<Array> value_array = std::make_shared<StructArray>(
      struct_({field("a", int32())}), num_values, av00, bitmap);
  std::shared_ptr<Array> array = rand.List(*value_array, num_rows, 0);
  AssertArrayWriteReadEqual(array, array, kDefaultSmallMemStreamSize * 30);
}

TEST_F(TestORCWriterSingleArray, WriteStructOfList) {
  const int64_t num_rows = 1234;
  ArrayVector av0(1);
  auto value_array = rand.ArrayOf(int32(), 5 * num_rows, 0.2);
  av0[0] = rand.List(*value_array, num_rows, 0);
  std::shared_ptr<Buffer> bitmap = rand.NullBitmap(num_rows, 0.2);
  std::shared_ptr<Array> array = std::make_shared<StructArray>(
      struct_({field("a", list(int32()))}), num_rows, av0, bitmap);
  AssertArrayWriteReadEqual(array, array, kDefaultSmallMemStreamSize * 20);
}

TEST_F(TestORCWriterSingleArray, WriteMap) {
  const int64_t num_rows = 1234;
  auto key_array = rand.ArrayOf(int32(), 20 * num_rows, 0);
  auto item_array = rand.ArrayOf(int32(), 20 * num_rows, 1);
  std::shared_ptr<Array> array = rand.Map(key_array, item_array, num_rows, 0.1);
  AssertArrayWriteReadEqual(array, array, kDefaultSmallMemStreamSize * 50);
}

TEST_F(TestORCWriterSingleArray, WriteStructOfMap) {
  const int64_t num_rows = 1234, num_values = 5 * num_rows;
  ArrayVector av0(1);
  auto key_array = rand.ArrayOf(binary(), num_values, 0);
  auto item_array = rand.ArrayOf(int32(), num_values, 0.5);
  av0[0] = rand.Map(key_array, item_array, num_rows, 0.2);
  std::shared_ptr<Array> array = std::make_shared<StructArray>(
      struct_({field("a", map(binary(), int32()))}), num_rows, av0);
  AssertArrayWriteReadEqual(array, array, kDefaultSmallMemStreamSize * 20);
}

TEST_F(TestORCWriterSingleArray, WriteMapOfStruct) {
  const int64_t num_rows = 1234, num_values = 10 * num_rows;
  std::shared_ptr<Array> key_array = rand.ArrayOf(utf8(), num_values, 0);
  ArrayVector av00(1);
  av00[0] = rand.ArrayOf(int32(), num_values, 0.1);
  std::shared_ptr<Buffer> bitmap = rand.NullBitmap(num_values, 0.2);
  std::shared_ptr<Array> item_array = std::make_shared<StructArray>(
      struct_({field("a", int32())}), num_values, av00, bitmap);
  std::shared_ptr<Array> array = rand.Map(key_array, item_array, num_rows, 0.1);
  AssertArrayWriteReadEqual(array, array, kDefaultSmallMemStreamSize * 10);
}

TEST_F(TestORCWriterSingleArray, WriteMapOfMap) {
  const int64_t num_rows = 1234;
  auto key_key_array = rand.ArrayOf(utf8(), 4 * num_rows, 0);
  auto key_item_array = rand.ArrayOf(int32(), 4 * num_rows, 0.5);
  std::shared_ptr<Array> key_array =
      rand.Map(key_key_array, key_item_array, 2 * num_rows, 0);
  auto item_key_array = rand.ArrayOf(utf8(), 4 * num_rows, 0);
  auto item_item_array = rand.ArrayOf(int32(), 4 * num_rows, 0.2);
  std::shared_ptr<Array> item_array =
      rand.Map(item_key_array, item_item_array, 2 * num_rows, 0.3);
  std::shared_ptr<Array> array = rand.Map(key_array, item_array, num_rows, 0.4);
  AssertArrayWriteReadEqual(array, array, kDefaultSmallMemStreamSize * 10);
}

TEST_F(TestORCWriterSingleArray, WriteListOfMap) {
  const int64_t num_rows = 1234;
  auto value_key_array = rand.ArrayOf(utf8(), 4 * num_rows, 0);
  auto value_item_array = rand.ArrayOf(int32(), 4 * num_rows, 0.5);
  std::shared_ptr<Array> value_array =
      rand.Map(value_key_array, value_item_array, 2 * num_rows, 0.2);
  std::shared_ptr<Array> array = rand.List(*value_array, num_rows, 0.4);
  AssertArrayWriteReadEqual(array, array, kDefaultSmallMemStreamSize * 10);
}

namespace {

// The random array generator fills random unselected values in the child arrays
// of SparseUnionArray. However, orc file only contains dense union type meaning
// that these unselected values will not be written to the file (so we can never
// read them back and compare equality in the unit test). Because the orc reader
// fills unselected values to nulls when reading from the file. So flattening
// the SparseUnionArray before writing makes it easy for the array equality check.
std::shared_ptr<Array> FlattenSparseUnionArray(std::shared_ptr<Array> array) {
  auto union_array = checked_pointer_cast<SparseUnionArray>(array);
  ArrayVector children;
  for (int i = 0; i < array->num_fields(); ++i) {
    ASSIGN_OR_ABORT(auto flattened_child, union_array->GetFlattenedField(i));
    children.emplace_back(std::move(flattened_child));
  }
  return std::make_shared<SparseUnionArray>(array->type(), array->length(),
                                            std::move(children),
                                            union_array->type_codes(), array->offset());
}

void TestUnionConversion(std::shared_ptr<Array> array) {
  auto length = array->length();
  auto orc_type = liborc::Type::buildTypeFromString("uniontype<string,int>");
  auto orc_batch = orc_type->createRowBatch(array->length(), *liborc::getDefaultPool());

  // Convert from arrow to orc
  int arrow_chunk_offset = 0;
  int64_t arrow_index_offset = 0;
  ASSERT_OK(adapters::orc::WriteBatch(ChunkedArray{array}, length, &arrow_chunk_offset,
                                      &arrow_index_offset, orc_batch.get()));

  // Convert from orc to arrow
  ASSIGN_OR_ABORT(auto array_builder, MakeBuilder(array->type()));
  ASSERT_OK(adapters::orc::AppendBatch(orc_type.get(), orc_batch.get(), 0, length,
                                       array_builder.get()));
  std::shared_ptr<Array> result;
  ASSERT_OK(array_builder->Finish(&result));

  // Compare the result
  AssertArraysEqual(*array, *result);
}

}  // namespace

TEST_F(TestORCWriterSingleArray, WriteSparseUnion) {
  constexpr int64_t num_rows = 1024;
  auto type =
      sparse_union({field("_union_0", utf8()), field("_union_1", int32())}, {0, 1});
  auto array =
      FlattenSparseUnionArray(rand.ArrayOf(type, num_rows, /*null_probability=*/0.4));
  AssertArrayWriteReadEqual(array, array, kDefaultSmallMemStreamSize * 10);
}

TEST(TestWriteReadORCBatch, SparseUnionConversion) {
  random::RandomArrayGenerator rand(kRandomSeed);
  auto type = sparse_union({field("a", utf8()), field("b", int32())}, {0, 1});
  auto array = FlattenSparseUnionArray(
      rand.ArrayOf(type, /*size=*/1024, /*null_probability=*/0.4));
  TestUnionConversion(std::move(array));
}

TEST(TestWriteReadORCBatch, DenseUnionConversion) {
  random::RandomArrayGenerator rand(kRandomSeed);
  auto type = dense_union({field("a", utf8()), field("b", int32())}, {0, 1});
  auto array = rand.ArrayOf(type, /*size=*/1024, /*null_probability=*/0.4);
  TestUnionConversion(std::move(array));
}

class TestORCWriterMultipleWrite : public ::testing::Test {
 public:
  TestORCWriterMultipleWrite() : rand(kRandomSeed) {}

 protected:
  random::RandomArrayGenerator rand;
};

TEST_F(TestORCWriterMultipleWrite, MultipleWritesIntField) {
  const int64_t num_rows = 1234;
  const int num_writes = 5;
  std::shared_ptr<Schema> input_schema = schema({field("col0", int32())});
  ArrayVector vect;
  std::vector<std::shared_ptr<Table>> input_tables;
  for (int i = 0; i < num_writes; i++) {
    auto array_int = rand.ArrayOf(int32(), num_rows, 0);
    vect.push_back(array_int);
    auto input_chunked_array = std::make_shared<ChunkedArray>(array_int);
    input_tables.emplace_back(Table::Make(input_schema, {input_chunked_array}));
  }
  auto expected_output_chunked_array = std::make_shared<ChunkedArray>(vect);
  std::shared_ptr<Table> expected_output_table =
      Table::Make(input_schema, {expected_output_chunked_array});
  AssertTableWriteReadEqual(input_tables, expected_output_table,
                            kDefaultSmallMemStreamSize * 100);
}

TEST_F(TestORCWriterMultipleWrite, MultipleWritesIncoherentSchema) {
  const int64_t num_rows = 1234;
  auto array_int = rand.ArrayOf(int32(), num_rows, 0);
  std::shared_ptr<Schema> input_schema = schema({field("col0", array_int->type())});
  auto array_int2 = rand.ArrayOf(int64(), num_rows, 0);
  std::shared_ptr<Schema> input_schema2 = schema({field("col0", array_int2->type())});

  std::shared_ptr<Table> input_table = Table::Make(input_schema, {array_int});
  std::shared_ptr<Table> input_table2 = Table::Make(input_schema2, {array_int2});
  EXPECT_OK_AND_ASSIGN(auto buffer_output_stream,
                       io::BufferOutputStream::Create(kDefaultSmallMemStreamSize));
  auto write_options = adapters::orc::WriteOptions();
  EXPECT_OK_AND_ASSIGN(auto writer, adapters::orc::ORCFileWriter::Open(
                                        buffer_output_stream.get(), write_options));
  ARROW_EXPECT_OK(writer->Write(*input_table));

  // This should not pass
  ASSERT_RAISES(TypeError, writer->Write(*input_table2));

  ARROW_EXPECT_OK(writer->Close());
}
TEST_F(TestORCWriterMultipleWrite, MultipleWritesIntFieldRecordBatch) {
  const int64_t num_rows = 1234;
  const int num_writes = 5;
  std::shared_ptr<Schema> input_schema = schema({field("col0", int32())});
  ArrayVector vect;
  std::vector<std::shared_ptr<RecordBatch>> input_batches;
  for (int i = 0; i < num_writes; i++) {
    auto array_int = rand.ArrayOf(int32(), num_rows, 0);
    vect.push_back(array_int);
    input_batches.emplace_back(RecordBatch::Make(input_schema, num_rows, {array_int}));
  }
  auto expected_output_chunked_array = std::make_shared<ChunkedArray>(vect);
  std::shared_ptr<Table> expected_output_table =
      Table::Make(input_schema, {expected_output_chunked_array});
  AssertBatchWriteReadEqual(input_batches, expected_output_table,
                            kDefaultSmallMemStreamSize * 100);
}
}  // namespace arrow
