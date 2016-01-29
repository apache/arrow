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

#include "parquet/column_reader.h"

#include <algorithm>
#include <string>
#include <string.h>

#include "parquet/encodings/encodings.h"
#include "parquet/compression/codec.h"
#include "parquet/thrift/util.h"
#include "parquet/util/input_stream.h"

const int DATA_PAGE_SIZE = 64 * 1024;

namespace parquet_cpp {

using parquet::CompressionCodec;
using parquet::Encoding;
using parquet::FieldRepetitionType;
using parquet::PageType;
using parquet::Type;


ColumnReader::~ColumnReader() {
  delete stream_;
}

ColumnReader::ColumnReader(const parquet::ColumnMetaData* metadata,
    const parquet::SchemaElement* schema, InputStream* stream)
  : metadata_(metadata),
    schema_(schema),
    stream_(stream),
    num_buffered_values_(0),
    num_decoded_values_(0),
    buffered_values_offset_(0) {

  switch (metadata->codec) {
    case CompressionCodec::UNCOMPRESSED:
      break;
    case CompressionCodec::SNAPPY:
      decompressor_.reset(new SnappyCodec());
      break;
    default:
      ParquetException::NYI("Reading compressed data");
  }

  config_ = Config::DefaultConfig();
}


// PLAIN_DICTIONARY is deprecated but used to be used as a dictionary index
// encoding.
static bool IsDictionaryIndexEncoding(const Encoding::type& e) {
  return e == Encoding::RLE_DICTIONARY || e == Encoding::PLAIN_DICTIONARY;
}

template <int TYPE>
bool TypedColumnReader<TYPE>::ReadNewPage() {
  // Loop until we find the next data page.


  while (true) {
    int bytes_read = 0;
    const uint8_t* buffer = stream_->Peek(DATA_PAGE_SIZE, &bytes_read);
    if (bytes_read == 0) return false;
    uint32_t header_size = bytes_read;
    DeserializeThriftMsg(buffer, &header_size, &current_page_header_);
    stream_->Read(header_size, &bytes_read);

    int compressed_len = current_page_header_.compressed_page_size;
    int uncompressed_len = current_page_header_.uncompressed_page_size;

    // Read the compressed data page.
    buffer = stream_->Read(compressed_len, &bytes_read);
    if (bytes_read != compressed_len) ParquetException::EofException();

    // Uncompress it if we need to
    if (decompressor_ != NULL) {
      // Grow the uncompressed buffer if we need to.
      if (uncompressed_len > decompression_buffer_.size()) {
        decompression_buffer_.resize(uncompressed_len);
      }
      decompressor_->Decompress(compressed_len, buffer, uncompressed_len,
          &decompression_buffer_[0]);
      buffer = &decompression_buffer_[0];
    }

    if (current_page_header_.type == PageType::DICTIONARY_PAGE) {
      auto it = decoders_.find(Encoding::RLE_DICTIONARY);
      if (it != decoders_.end()) {
        throw ParquetException("Column cannot have more than one dictionary.");
      }

      PlainDecoder<TYPE> dictionary(schema_);
      dictionary.SetData(current_page_header_.dictionary_page_header.num_values,
          buffer, uncompressed_len);
      std::shared_ptr<DecoderType> decoder(new DictionaryDecoder<TYPE>(schema_, &dictionary));

      decoders_[Encoding::RLE_DICTIONARY] = decoder;
      current_decoder_ = decoders_[Encoding::RLE_DICTIONARY].get();
      continue;
    } else if (current_page_header_.type == PageType::DATA_PAGE) {
      // Read a data page.
      num_buffered_values_ = current_page_header_.data_page_header.num_values;

      // Read definition levels.
      if (schema_->repetition_type != FieldRepetitionType::REQUIRED) {
        int num_definition_bytes = *reinterpret_cast<const uint32_t*>(buffer);
        buffer += sizeof(uint32_t);
        definition_level_decoder_.reset(
            new RleDecoder(buffer, num_definition_bytes, 1));
        buffer += num_definition_bytes;
        uncompressed_len -= sizeof(uint32_t);
        uncompressed_len -= num_definition_bytes;
      }

      // TODO: repetition levels

      // Get a decoder object for this page or create a new decoder if this is the
      // first page with this encoding.
      Encoding::type encoding = current_page_header_.data_page_header.encoding;
      if (IsDictionaryIndexEncoding(encoding)) encoding = Encoding::RLE_DICTIONARY;

      auto it = decoders_.find(encoding);
      if (it != decoders_.end()) {
        current_decoder_ = it->second.get();
      } else {
        switch (encoding) {
          case Encoding::PLAIN: {
            std::shared_ptr<DecoderType> decoder(new PlainDecoder<TYPE>(schema_));
            decoders_[encoding] = decoder;
            current_decoder_ = decoder.get();
            break;
          }
          case Encoding::RLE_DICTIONARY:
            throw ParquetException("Dictionary page must be before data page.");

          case Encoding::DELTA_BINARY_PACKED:
          case Encoding::DELTA_LENGTH_BYTE_ARRAY:
          case Encoding::DELTA_BYTE_ARRAY:
            ParquetException::NYI("Unsupported encoding");

          default:
            throw ParquetException("Unknown encoding type.");
        }
      }
      current_decoder_->SetData(num_buffered_values_, buffer, uncompressed_len);
      return true;
    } else {
      // We don't know what this page type is. We're allowed to skip non-data pages.
      continue;
    }
  }
  return true;
}

std::shared_ptr<ColumnReader> ColumnReader::Make(const parquet::ColumnMetaData* metadata,
    const parquet::SchemaElement* element, InputStream* stream) {
  switch (metadata->type) {
    case Type::BOOLEAN:
      return std::make_shared<BoolReader>(metadata, element, stream);
    case Type::INT32:
      return std::make_shared<Int32Reader>(metadata, element, stream);
    case Type::INT64:
      return std::make_shared<Int64Reader>(metadata, element, stream);
    case Type::INT96:
      return std::make_shared<Int96Reader>(metadata, element, stream);
    case Type::FLOAT:
      return std::make_shared<FloatReader>(metadata, element, stream);
    case Type::DOUBLE:
      return std::make_shared<DoubleReader>(metadata, element, stream);
    case Type::BYTE_ARRAY:
      return std::make_shared<ByteArrayReader>(metadata, element, stream);
    case Type::FIXED_LEN_BYTE_ARRAY:
      return std::make_shared<FixedLenByteArrayReader>(metadata, element, stream);
    default:
      ParquetException::NYI("type reader not implemented");
  }
  // Unreachable code, but supress compiler warning
  return std::shared_ptr<ColumnReader>(nullptr);
}

} // namespace parquet_cpp
