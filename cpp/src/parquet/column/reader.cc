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

#include "parquet/column/reader.h"

#include <algorithm>
#include <memory>
#include <string>
#include <string.h>

#include "parquet/column/page.h"

#include "parquet/encodings/encodings.h"

namespace parquet_cpp {

ColumnReader::ColumnReader(const ColumnDescriptor* descr,
    std::unique_ptr<PageReader> pager)
  : descr_(descr),
    pager_(std::move(pager)),
    num_buffered_values_(0),
    num_decoded_values_(0) {}

template <int TYPE>
void TypedColumnReader<TYPE>::ConfigureDictionary(const DictionaryPage* page) {
  int encoding = static_cast<int>(parquet::Encoding::RLE_DICTIONARY);

  auto it = decoders_.find(encoding);
  if (it != decoders_.end()) {
    throw ParquetException("Column cannot have more than one dictionary.");
  }

  PlainDecoder<TYPE> dictionary(descr_);
  dictionary.SetData(page->num_values(), page->data(), page->size());

  // The dictionary is fully decoded during DictionaryDecoder::Init, so the
  // DictionaryPage buffer is no longer required after this step
  //
  // TODO(wesm): investigate whether this all-or-nothing decoding of the
  // dictionary makes sense and whether performance can be improved
  std::shared_ptr<DecoderType> decoder(
      new DictionaryDecoder<TYPE>(descr_, &dictionary));

  decoders_[encoding] = decoder;
  current_decoder_ = decoders_[encoding].get();
}

// PLAIN_DICTIONARY is deprecated but used to be used as a dictionary index
// encoding.
static bool IsDictionaryIndexEncoding(const parquet::Encoding::type& e) {
  return e == parquet::Encoding::RLE_DICTIONARY ||
    e == parquet::Encoding::PLAIN_DICTIONARY;
}

template <int TYPE>
bool TypedColumnReader<TYPE>::ReadNewPage() {
  // Loop until we find the next data page.
  const uint8_t* buffer;

  while (true) {
    current_page_ = pager_->NextPage();
    if (!current_page_) {
      // EOS
      return false;
    }

    if (current_page_->type() == parquet::PageType::DICTIONARY_PAGE) {
      ConfigureDictionary(static_cast<const DictionaryPage*>(current_page_.get()));
      continue;
    } else if (current_page_->type() == parquet::PageType::DATA_PAGE) {
      const DataPage* page = static_cast<const DataPage*>(current_page_.get());

      // Read a data page.
      num_buffered_values_ = page->num_values();

      // Have not decoded any values from the data page yet
      num_decoded_values_ = 0;

      buffer = page->data();

      // If the data page includes repetition and definition levels, we
      // initialize the level decoder and subtract the encoded level bytes from
      // the page size to determine the number of bytes in the encoded data.
      size_t data_size = page->size();

      int16_t max_definition_level = descr_->max_definition_level();
      int16_t max_repetition_level = descr_->max_repetition_level();
      //Data page Layout: Repetition Levels - Definition Levels - encoded values.
      //Levels are encoded as rle or bit-packed.
      //Init repetition levels
      if (max_repetition_level > 0) {
        size_t rep_levels_bytes = repetition_level_decoder_.Init(
            page->repetition_level_encoding(),
            max_repetition_level, num_buffered_values_, buffer);
        buffer += rep_levels_bytes;
        data_size -= rep_levels_bytes;
      }
      //TODO figure a way to set max_definition_level_ to 0
      //if the initial value is invalid

      //Init definition levels
      if (max_definition_level > 0) {
        size_t def_levels_bytes = definition_level_decoder_.Init(
            page->definition_level_encoding(),
            max_definition_level, num_buffered_values_, buffer);
        buffer += def_levels_bytes;
        data_size -= def_levels_bytes;
      }

      // Get a decoder object for this page or create a new decoder if this is the
      // first page with this encoding.
      parquet::Encoding::type encoding = page->encoding();

      if (IsDictionaryIndexEncoding(encoding)) {
        encoding = parquet::Encoding::RLE_DICTIONARY;
      }

      auto it = decoders_.find(static_cast<int>(encoding));
      if (it != decoders_.end()) {
        current_decoder_ = it->second.get();
      } else {
        switch (encoding) {
          case parquet::Encoding::PLAIN: {
            std::shared_ptr<DecoderType> decoder(new PlainDecoder<TYPE>(descr_));
            decoders_[static_cast<int>(encoding)] = decoder;
            current_decoder_ = decoder.get();
            break;
          }
          case parquet::Encoding::RLE_DICTIONARY:
            throw ParquetException("Dictionary page must be before data page.");

          case parquet::Encoding::DELTA_BINARY_PACKED:
          case parquet::Encoding::DELTA_LENGTH_BYTE_ARRAY:
          case parquet::Encoding::DELTA_BYTE_ARRAY:
            ParquetException::NYI("Unsupported encoding");

          default:
            throw ParquetException("Unknown encoding type.");
        }
      }
      current_decoder_->SetData(num_buffered_values_, buffer, data_size);
      return true;
    } else {
      // We don't know what this page type is. We're allowed to skip non-data
      // pages.
      continue;
    }
  }
  return true;
}

// ----------------------------------------------------------------------
// Batch read APIs

size_t ColumnReader::ReadDefinitionLevels(size_t batch_size, int16_t* levels) {
  if (descr_->max_definition_level() == 0) {
    return 0;
  }
  return definition_level_decoder_.Decode(batch_size, levels);
}

size_t ColumnReader::ReadRepetitionLevels(size_t batch_size, int16_t* levels) {
  if (descr_->max_repetition_level() == 0) {
    return 0;
  }
  return repetition_level_decoder_.Decode(batch_size, levels);
}

// ----------------------------------------------------------------------
// Dynamic column reader constructor

std::shared_ptr<ColumnReader> ColumnReader::Make(
    const ColumnDescriptor* descr,
    std::unique_ptr<PageReader> pager) {
  switch (descr->physical_type()) {
    case Type::BOOLEAN:
      return std::make_shared<BoolReader>(descr, std::move(pager));
    case Type::INT32:
      return std::make_shared<Int32Reader>(descr, std::move(pager));
    case Type::INT64:
      return std::make_shared<Int64Reader>(descr, std::move(pager));
    case Type::INT96:
      return std::make_shared<Int96Reader>(descr, std::move(pager));
    case Type::FLOAT:
      return std::make_shared<FloatReader>(descr, std::move(pager));
    case Type::DOUBLE:
      return std::make_shared<DoubleReader>(descr, std::move(pager));
    case Type::BYTE_ARRAY:
      return std::make_shared<ByteArrayReader>(descr, std::move(pager));
    case Type::FIXED_LEN_BYTE_ARRAY:
      return std::make_shared<FixedLenByteArrayReader>(descr, std::move(pager));
    default:
      ParquetException::NYI("type reader not implemented");
  }
  // Unreachable code, but supress compiler warning
  return std::shared_ptr<ColumnReader>(nullptr);
}

} // namespace parquet_cpp
