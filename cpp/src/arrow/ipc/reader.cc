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

#include "arrow/ipc/reader.h"

#include <cstdint>
#include <cstring>
#include <sstream>
#include <string>
#include <vector>

#include "arrow/buffer.h"
#include "arrow/io/interfaces.h"
#include "arrow/io/memory.h"
#include "arrow/ipc/adapter.h"
#include "arrow/ipc/metadata-internal.h"
#include "arrow/ipc/metadata.h"
#include "arrow/ipc/util.h"
#include "arrow/status.h"
#include "arrow/util/logging.h"

namespace arrow {
namespace ipc {

// ----------------------------------------------------------------------
// StreamReader implementation

static inline FileBlock FileBlockFromFlatbuffer(const flatbuf::Block* block) {
  return FileBlock(block->offset(), block->metaDataLength(), block->bodyLength());
}

static inline std::string message_type_name(Message::Type type) {
  switch (type) {
    case Message::SCHEMA:
      return "schema";
    case Message::RECORD_BATCH:
      return "record batch";
    case Message::DICTIONARY_BATCH:
      return "dictionary";
    default:
      break;
  }
  return "unknown";
}

class StreamReader::StreamReaderImpl {
 public:
  StreamReaderImpl() {}
  ~StreamReaderImpl() {}

  Status Open(const std::shared_ptr<io::InputStream>& stream) {
    stream_ = stream;
    return ReadSchema();
  }

  Status ReadNextMessage(Message::Type expected_type, std::shared_ptr<Message>* message) {
    std::shared_ptr<Buffer> buffer;
    RETURN_NOT_OK(stream_->Read(sizeof(int32_t), &buffer));

    if (buffer->size() != sizeof(int32_t)) {
      *message = nullptr;
      return Status::OK();
    }

    int32_t message_length = *reinterpret_cast<const int32_t*>(buffer->data());

    if (message_length == 0) {
      // Optional 0 EOS control message
      *message = nullptr;
      return Status::OK();
    }

    RETURN_NOT_OK(stream_->Read(message_length, &buffer));
    if (buffer->size() != message_length) {
      return Status::IOError("Unexpected end of stream trying to read message");
    }

    RETURN_NOT_OK(Message::Open(buffer, 0, message));

    if ((*message)->type() != expected_type) {
      std::stringstream ss;
      ss << "Message not expected type: " << message_type_name(expected_type)
         << ", was: " << (*message)->type();
      return Status::IOError(ss.str());
    }
    return Status::OK();
  }

  Status ReadExact(int64_t size, std::shared_ptr<Buffer>* buffer) {
    RETURN_NOT_OK(stream_->Read(size, buffer));

    if ((*buffer)->size() < size) {
      return Status::IOError("Unexpected EOS when reading buffer");
    }
    return Status::OK();
  }

  Status ReadNextDictionary() {
    std::shared_ptr<Message> message;
    RETURN_NOT_OK(ReadNextMessage(Message::DICTIONARY_BATCH, &message));

    DictionaryBatchMetadata metadata(message);

    std::shared_ptr<Buffer> batch_body;
    RETURN_NOT_OK(ReadExact(message->body_length(), &batch_body))
    io::BufferReader reader(batch_body);

    std::shared_ptr<Array> dictionary;
    RETURN_NOT_OK(ReadDictionary(metadata, dictionary_types_, &reader, &dictionary));
    return dictionary_memo_.AddDictionary(metadata.id(), dictionary);
  }

  Status ReadSchema() {
    std::shared_ptr<Message> message;
    RETURN_NOT_OK(ReadNextMessage(Message::SCHEMA, &message));

    SchemaMetadata schema_meta(message);
    RETURN_NOT_OK(schema_meta.GetDictionaryTypes(&dictionary_types_));

    // TODO(wesm): In future, we may want to reconcile the ids in the stream with
    // those found in the schema
    int num_dictionaries = static_cast<int>(dictionary_types_.size());
    for (int i = 0; i < num_dictionaries; ++i) {
      RETURN_NOT_OK(ReadNextDictionary());
    }

    return schema_meta.GetSchema(dictionary_memo_, &schema_);
  }

  Status GetNextRecordBatch(std::shared_ptr<RecordBatch>* batch) {
    std::shared_ptr<Message> message;
    RETURN_NOT_OK(ReadNextMessage(Message::RECORD_BATCH, &message));

    if (message == nullptr) {
      // End of stream
      *batch = nullptr;
      return Status::OK();
    }

    RecordBatchMetadata batch_metadata(message);

    std::shared_ptr<Buffer> batch_body;
    RETURN_NOT_OK(ReadExact(message->body_length(), &batch_body));
    io::BufferReader reader(batch_body);
    return ReadRecordBatch(batch_metadata, schema_, &reader, batch);
  }

  std::shared_ptr<Schema> schema() const { return schema_; }

 private:
  // dictionary_id -> type
  DictionaryTypeMap dictionary_types_;

  DictionaryMemo dictionary_memo_;

  std::shared_ptr<io::InputStream> stream_;
  std::shared_ptr<Schema> schema_;
};

StreamReader::StreamReader() {
  impl_.reset(new StreamReaderImpl());
}

StreamReader::~StreamReader() {}

Status StreamReader::Open(const std::shared_ptr<io::InputStream>& stream,
    std::shared_ptr<StreamReader>* reader) {
  // Private ctor
  *reader = std::shared_ptr<StreamReader>(new StreamReader());
  return (*reader)->impl_->Open(stream);
}

std::shared_ptr<Schema> StreamReader::schema() const {
  return impl_->schema();
}

Status StreamReader::GetNextRecordBatch(std::shared_ptr<RecordBatch>* batch) {
  return impl_->GetNextRecordBatch(batch);
}

// ----------------------------------------------------------------------
// Reader implementation

class FileReader::FileReaderImpl {
 public:
  FileReaderImpl() { dictionary_memo_ = std::make_shared<DictionaryMemo>(); }

  Status ReadFooter() {
    int magic_size = static_cast<int>(strlen(kArrowMagicBytes));

    if (footer_offset_ <= magic_size * 2 + 4) {
      std::stringstream ss;
      ss << "File is too small: " << footer_offset_;
      return Status::Invalid(ss.str());
    }

    std::shared_ptr<Buffer> buffer;
    int file_end_size = static_cast<int>(magic_size + sizeof(int32_t));
    RETURN_NOT_OK(file_->ReadAt(footer_offset_ - file_end_size, file_end_size, &buffer));

    if (memcmp(buffer->data() + sizeof(int32_t), kArrowMagicBytes, magic_size)) {
      return Status::Invalid("Not an Arrow file");
    }

    int32_t footer_length = *reinterpret_cast<const int32_t*>(buffer->data());

    if (footer_length <= 0 || footer_length + magic_size * 2 + 4 > footer_offset_) {
      return Status::Invalid("File is smaller than indicated metadata size");
    }

    // Now read the footer
    RETURN_NOT_OK(file_->ReadAt(
        footer_offset_ - footer_length - file_end_size, footer_length, &footer_buffer_));

    // TODO(wesm): Verify the footer
    footer_ = flatbuf::GetFooter(footer_buffer_->data());
    schema_metadata_.reset(new SchemaMetadata(nullptr, footer_->schema()));

    return Status::OK();
  }

  int num_dictionaries() const { return footer_->dictionaries()->size(); }

  int num_record_batches() const { return footer_->recordBatches()->size(); }

  MetadataVersion::type version() const {
    switch (footer_->version()) {
      case flatbuf::MetadataVersion_V1:
        return MetadataVersion::V1;
      case flatbuf::MetadataVersion_V2:
        return MetadataVersion::V2;
      // Add cases as other versions become available
      default:
        return MetadataVersion::V2;
    }
  }

  FileBlock record_batch(int i) const {
    return FileBlockFromFlatbuffer(footer_->recordBatches()->Get(i));
  }

  FileBlock dictionary(int i) const {
    return FileBlockFromFlatbuffer(footer_->dictionaries()->Get(i));
  }

  const SchemaMetadata& schema_metadata() const { return *schema_metadata_; }

  Status GetRecordBatch(int i, std::shared_ptr<RecordBatch>* batch) {
    DCHECK_GE(i, 0);
    DCHECK_LT(i, num_record_batches());
    FileBlock block = record_batch(i);

    std::shared_ptr<Message> message;
    RETURN_NOT_OK(
        ReadMessage(block.offset, block.metadata_length, file_.get(), &message));
    auto metadata = std::make_shared<RecordBatchMetadata>(message);

    // TODO(wesm): ARROW-388 -- the buffer frame of reference is 0 (see
    // ARROW-384).
    std::shared_ptr<Buffer> buffer_block;
    RETURN_NOT_OK(file_->Read(block.body_length, &buffer_block));
    io::BufferReader reader(buffer_block);

    return ReadRecordBatch(*metadata, schema_, &reader, batch);
  }

  Status ReadSchema() {
    RETURN_NOT_OK(schema_metadata_->GetDictionaryTypes(&dictionary_fields_));

    // Read all the dictionaries
    for (int i = 0; i < num_dictionaries(); ++i) {
      FileBlock block = dictionary(i);
      std::shared_ptr<Message> message;
      RETURN_NOT_OK(
          ReadMessage(block.offset, block.metadata_length, file_.get(), &message));

      // TODO(wesm): ARROW-577: This code is duplicated, can be fixed with a more
      // invasive refactor
      DictionaryBatchMetadata metadata(message);

      // TODO(wesm): ARROW-388 -- the buffer frame of reference is 0 (see
      // ARROW-384).
      std::shared_ptr<Buffer> buffer_block;
      RETURN_NOT_OK(file_->Read(block.body_length, &buffer_block));
      io::BufferReader reader(buffer_block);

      std::shared_ptr<Array> dictionary;
      RETURN_NOT_OK(ReadDictionary(metadata, dictionary_fields_, &reader, &dictionary));
      RETURN_NOT_OK(dictionary_memo_->AddDictionary(metadata.id(), dictionary));
    }

    // Get the schema
    return schema_metadata_->GetSchema(*dictionary_memo_, &schema_);
  }

  Status Open(
      const std::shared_ptr<io::RandomAccessFile>& file, int64_t footer_offset) {
    file_ = file;
    footer_offset_ = footer_offset;
    RETURN_NOT_OK(ReadFooter());
    return ReadSchema();
  }

  std::shared_ptr<Schema> schema() const { return schema_; }

 private:
  std::shared_ptr<io::RandomAccessFile> file_;

  // The location where the Arrow file layout ends. May be the end of the file
  // or some other location if embedded in a larger file.
  int64_t footer_offset_;

  // Footer metadata
  std::shared_ptr<Buffer> footer_buffer_;
  const flatbuf::Footer* footer_;
  std::unique_ptr<SchemaMetadata> schema_metadata_;

  DictionaryTypeMap dictionary_fields_;
  std::shared_ptr<DictionaryMemo> dictionary_memo_;

  // Reconstructed schema, including any read dictionaries
  std::shared_ptr<Schema> schema_;
};

FileReader::FileReader() {
  impl_.reset(new FileReaderImpl());
}

FileReader::~FileReader() {}

Status FileReader::Open(const std::shared_ptr<io::RandomAccessFile>& file,
    std::shared_ptr<FileReader>* reader) {
  int64_t footer_offset;
  RETURN_NOT_OK(file->GetSize(&footer_offset));
  return Open(file, footer_offset, reader);
}

Status FileReader::Open(const std::shared_ptr<io::RandomAccessFile>& file,
    int64_t footer_offset, std::shared_ptr<FileReader>* reader) {
  *reader = std::shared_ptr<FileReader>(new FileReader());
  return (*reader)->impl_->Open(file, footer_offset);
}

std::shared_ptr<Schema> FileReader::schema() const {
  return impl_->schema();
}

int FileReader::num_record_batches() const {
  return impl_->num_record_batches();
}

MetadataVersion::type FileReader::version() const {
  return impl_->version();
}

Status FileReader::GetRecordBatch(int i, std::shared_ptr<RecordBatch>* batch) {
  return impl_->GetRecordBatch(i, batch);
}

}  // namespace ipc
}  // namespace arrow
