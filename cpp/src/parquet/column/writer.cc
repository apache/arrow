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

#include "parquet/column/writer.h"

#include "parquet/column/properties.h"
#include "parquet/encodings/dictionary-encoding.h"
#include "parquet/encodings/plain-encoding.h"

namespace parquet {

// ----------------------------------------------------------------------
// ColumnWriter

std::shared_ptr<WriterProperties> default_writer_properties() {
  static std::shared_ptr<WriterProperties> default_writer_properties =
      WriterProperties::Builder().build();
  return default_writer_properties;
}

ColumnWriter::ColumnWriter(const ColumnDescriptor* descr,
    std::unique_ptr<PageWriter> pager, int64_t expected_rows, bool has_dictionary,
    Encoding::type encoding, const WriterProperties* properties)
    : descr_(descr),
      pager_(std::move(pager)),
      expected_rows_(expected_rows),
      has_dictionary_(has_dictionary),
      encoding_(encoding),
      properties_(properties),
      allocator_(properties->allocator()),
      pool_(properties->allocator()),
      num_buffered_values_(0),
      num_buffered_encoded_values_(0),
      num_rows_(0),
      total_bytes_written_(0),
      closed_(false) {
  InitSinks();
}

void ColumnWriter::InitSinks() {
  definition_levels_sink_.reset(new InMemoryOutputStream());
  repetition_levels_sink_.reset(new InMemoryOutputStream());
}

void ColumnWriter::WriteDefinitionLevels(int64_t num_levels, const int16_t* levels) {
  DCHECK(!closed_);
  definition_levels_sink_->Write(
      reinterpret_cast<const uint8_t*>(levels), sizeof(int16_t) * num_levels);
}

void ColumnWriter::WriteRepetitionLevels(int64_t num_levels, const int16_t* levels) {
  DCHECK(!closed_);
  repetition_levels_sink_->Write(
      reinterpret_cast<const uint8_t*>(levels), sizeof(int16_t) * num_levels);
}

std::shared_ptr<Buffer> ColumnWriter::RleEncodeLevels(
    const std::shared_ptr<Buffer>& buffer, int16_t max_level) {
  // TODO: This only works with due to some RLE specifics
  int64_t rle_size =
      LevelEncoder::MaxBufferSize(Encoding::RLE, max_level, num_buffered_values_) +
      sizeof(uint32_t);
  auto buffer_rle = std::make_shared<OwnedMutableBuffer>(rle_size, allocator_);
  level_encoder_.Init(Encoding::RLE, max_level, num_buffered_values_,
      buffer_rle->mutable_data() + sizeof(uint32_t),
      buffer_rle->size() - sizeof(uint32_t));
  int encoded = level_encoder_.Encode(
      num_buffered_values_, reinterpret_cast<const int16_t*>(buffer->data()));
  DCHECK_EQ(encoded, num_buffered_values_);
  reinterpret_cast<uint32_t*>(buffer_rle->mutable_data())[0] = level_encoder_.len();
  int64_t encoded_size = level_encoder_.len() + sizeof(uint32_t);
  DCHECK(rle_size >= encoded_size);
  buffer_rle->Resize(encoded_size);
  return std::static_pointer_cast<Buffer>(buffer_rle);
}

void ColumnWriter::AddDataPage() {
  std::shared_ptr<Buffer> definition_levels = definition_levels_sink_->GetBuffer();
  std::shared_ptr<Buffer> repetition_levels = repetition_levels_sink_->GetBuffer();
  std::shared_ptr<Buffer> values = GetValuesBuffer();

  if (descr_->max_definition_level() > 0) {
    definition_levels =
        RleEncodeLevels(definition_levels, descr_->max_definition_level());
  }

  if (descr_->max_repetition_level() > 0) {
    repetition_levels =
        RleEncodeLevels(repetition_levels, descr_->max_repetition_level());
  }

  int64_t uncompressed_size =
      definition_levels->size() + repetition_levels->size() + values->size();

  // Concatenate data into a single buffer
  std::shared_ptr<OwnedMutableBuffer> uncompressed_data =
      std::make_shared<OwnedMutableBuffer>(uncompressed_size, allocator_);
  uint8_t* uncompressed_ptr = uncompressed_data->mutable_data();
  memcpy(uncompressed_ptr, repetition_levels->data(), repetition_levels->size());
  uncompressed_ptr += repetition_levels->size();
  memcpy(uncompressed_ptr, definition_levels->data(), definition_levels->size());
  uncompressed_ptr += definition_levels->size();
  memcpy(uncompressed_ptr, values->data(), values->size());
  DataPage page(
      uncompressed_data, num_buffered_values_, encoding_, Encoding::RLE, Encoding::RLE);

  data_pages_.push_back(std::move(page));

  // Re-initialize the sinks as GetBuffer made them invalid.
  InitSinks();
  num_buffered_values_ = 0;
  num_buffered_encoded_values_ = 0;
}

void ColumnWriter::WriteDataPage(const DataPage& page) {
  int64_t bytes_written = pager_->WriteDataPage(page);
  total_bytes_written_ += bytes_written;
}

int64_t ColumnWriter::Close() {
  if (!closed_) {
    closed_ = true;
    if (has_dictionary_) { WriteDictionaryPage(); }
    // Write all outstanding data to a new page
    if (num_buffered_values_ > 0) { AddDataPage(); }

    for (size_t i = 0; i < data_pages_.size(); i++) {
      WriteDataPage(data_pages_[i]);
    }
  }

  if (num_rows_ != expected_rows_) {
    throw ParquetException(
        "Less then the number of expected rows written in"
        " the current column chunk");
  }

  pager_->Close();

  return total_bytes_written_;
}

// ----------------------------------------------------------------------
// TypedColumnWriter

template <typename Type>
TypedColumnWriter<Type>::TypedColumnWriter(const ColumnDescriptor* schema,
    std::unique_ptr<PageWriter> pager, int64_t expected_rows, Encoding::type encoding,
    const WriterProperties* properties)
    : ColumnWriter(schema, std::move(pager), expected_rows,
          (encoding == Encoding::PLAIN_DICTIONARY ||
                       encoding == Encoding::RLE_DICTIONARY),
          encoding, properties) {
  switch (encoding) {
    case Encoding::PLAIN:
      current_encoder_ = std::unique_ptr<EncoderType>(
          new PlainEncoder<Type>(schema, properties->allocator()));
      break;
    case Encoding::PLAIN_DICTIONARY:
    case Encoding::RLE_DICTIONARY:
      current_encoder_ = std::unique_ptr<EncoderType>(
          new DictEncoder<Type>(schema, &pool_, properties->allocator()));
      break;
    default:
      ParquetException::NYI("Selected encoding is not supported");
  }
}

template <typename Type>
void TypedColumnWriter<Type>::WriteDictionaryPage() {
  auto dict_encoder = static_cast<DictEncoder<Type>*>(current_encoder_.get());
  auto buffer = std::make_shared<OwnedMutableBuffer>(dict_encoder->dict_encoded_size());
  dict_encoder->WriteDict(buffer->mutable_data());
  // TODO Get rid of this deep call
  dict_encoder->mem_pool()->FreeAll();

  DictionaryPage page(
      buffer, dict_encoder->num_entries(), properties_->dictionary_index_encoding());
  total_bytes_written_ += pager_->WriteDictionaryPage(page);
}

// ----------------------------------------------------------------------
// Dynamic column writer constructor

std::shared_ptr<ColumnWriter> ColumnWriter::Make(const ColumnDescriptor* descr,
    std::unique_ptr<PageWriter> pager, int64_t expected_rows,
    const WriterProperties* properties) {
  Encoding::type encoding = properties->encoding(descr->path());
  if (properties->dictionary_enabled(descr->path())) {
    encoding = properties->dictionary_page_encoding();
  }
  switch (descr->physical_type()) {
    case Type::BOOLEAN:
      return std::make_shared<BoolWriter>(
          descr, std::move(pager), expected_rows, encoding, properties);
    case Type::INT32:
      return std::make_shared<Int32Writer>(
          descr, std::move(pager), expected_rows, encoding, properties);
    case Type::INT64:
      return std::make_shared<Int64Writer>(
          descr, std::move(pager), expected_rows, encoding, properties);
    case Type::INT96:
      return std::make_shared<Int96Writer>(
          descr, std::move(pager), expected_rows, encoding, properties);
    case Type::FLOAT:
      return std::make_shared<FloatWriter>(
          descr, std::move(pager), expected_rows, encoding, properties);
    case Type::DOUBLE:
      return std::make_shared<DoubleWriter>(
          descr, std::move(pager), expected_rows, encoding, properties);
    case Type::BYTE_ARRAY:
      return std::make_shared<ByteArrayWriter>(
          descr, std::move(pager), expected_rows, encoding, properties);
    case Type::FIXED_LEN_BYTE_ARRAY:
      return std::make_shared<FixedLenByteArrayWriter>(
          descr, std::move(pager), expected_rows, encoding, properties);
    default:
      ParquetException::NYI("type reader not implemented");
  }
  // Unreachable code, but supress compiler warning
  return std::shared_ptr<ColumnWriter>(nullptr);
}

// ----------------------------------------------------------------------
// Instantiate templated classes

template class TypedColumnWriter<BooleanType>;
template class TypedColumnWriter<Int32Type>;
template class TypedColumnWriter<Int64Type>;
template class TypedColumnWriter<Int96Type>;
template class TypedColumnWriter<FloatType>;
template class TypedColumnWriter<DoubleType>;
template class TypedColumnWriter<ByteArrayType>;
template class TypedColumnWriter<FLBAType>;

}  // namespace parquet
