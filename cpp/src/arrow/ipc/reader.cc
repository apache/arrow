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
#include "arrow/ipc/File_generated.h"
#include "arrow/ipc/Message_generated.h"
#include "arrow/ipc/metadata.h"
#include "arrow/ipc/util.h"
#include "arrow/status.h"
#include "arrow/table.h"
#include "arrow/type.h"
#include "arrow/tensor.h"
#include "arrow/util/logging.h"

namespace arrow {

namespace flatbuf = org::apache::arrow::flatbuf;

namespace ipc {

// ----------------------------------------------------------------------
// Record batch read path

class IpcComponentSource : public ArrayComponentSource {
 public:
  IpcComponentSource(const flatbuf::RecordBatch* metadata, io::RandomAccessFile* file)
      : metadata_(metadata), file_(file) {}

  Status GetBuffer(int buffer_index, std::shared_ptr<Buffer>* out) override {
    const flatbuf::Buffer* buffer = metadata_->buffers()->Get(buffer_index);

    if (buffer->length() == 0) {
      *out = nullptr;
      return Status::OK();
    } else {
      return file_->ReadAt(buffer->offset(), buffer->length(), out);
    }
  }

  Status GetFieldMetadata(int field_index, FieldMetadata* field) override {
    auto nodes = metadata_->nodes();
    // pop off a field
    if (field_index >= static_cast<int>(nodes->size())) {
      return Status::Invalid("Ran out of field metadata, likely malformed");
    }
    const flatbuf::FieldNode* node = nodes->Get(field_index);

    field->length = node->length();
    field->null_count = node->null_count();
    field->offset = 0;
    return Status::OK();
  }

 private:
  const flatbuf::RecordBatch* metadata_;
  io::RandomAccessFile* file_;
};

Status ReadRecordBatch(const Message& metadata, const std::shared_ptr<Schema>& schema,
    io::RandomAccessFile* file, std::shared_ptr<RecordBatch>* out) {
  return ReadRecordBatch(metadata, schema, kMaxNestingDepth, file, out);
}

static Status LoadRecordBatchFromSource(const std::shared_ptr<Schema>& schema,
    int64_t num_rows, int max_recursion_depth, ArrayComponentSource* source,
    std::shared_ptr<RecordBatch>* out) {
  std::vector<std::shared_ptr<Array>> arrays(schema->num_fields());

  ArrayLoaderContext context;
  context.source = source;
  context.field_index = 0;
  context.buffer_index = 0;
  context.max_recursion_depth = max_recursion_depth;

  for (int i = 0; i < schema->num_fields(); ++i) {
    RETURN_NOT_OK(LoadArray(schema->field(i)->type, &context, &arrays[i]));
    DCHECK_EQ(num_rows, arrays[i]->length())
        << "Array length did not match record batch length";
  }

  *out = std::make_shared<RecordBatch>(schema, num_rows, std::move(arrays));
  return Status::OK();
}

static inline Status ReadRecordBatch(const flatbuf::RecordBatch* metadata,
    const std::shared_ptr<Schema>& schema, int max_recursion_depth,
    io::RandomAccessFile* file, std::shared_ptr<RecordBatch>* out) {
  IpcComponentSource source(metadata, file);
  return LoadRecordBatchFromSource(
      schema, metadata->length(), max_recursion_depth, &source, out);
}

Status ReadRecordBatch(const Message& metadata, const std::shared_ptr<Schema>& schema,
    int max_recursion_depth, io::RandomAccessFile* file,
    std::shared_ptr<RecordBatch>* out) {
  DCHECK_EQ(metadata.type(), Message::RECORD_BATCH);
  auto batch = reinterpret_cast<const flatbuf::RecordBatch*>(metadata.header());
  return ReadRecordBatch(batch, schema, max_recursion_depth, file, out);
}

Status ReadDictionary(const Message& metadata, const DictionaryTypeMap& dictionary_types,
    io::RandomAccessFile* file, int64_t* dictionary_id, std::shared_ptr<Array>* out) {
  auto dictionary_batch =
      reinterpret_cast<const flatbuf::DictionaryBatch*>(metadata.header());

  int64_t id = *dictionary_id = dictionary_batch->id();
  auto it = dictionary_types.find(id);
  if (it == dictionary_types.end()) {
    std::stringstream ss;
    ss << "Do not have type metadata for dictionary with id: " << id;
    return Status::KeyError(ss.str());
  }

  std::vector<std::shared_ptr<Field>> fields = {it->second};

  // We need a schema for the record batch
  auto dummy_schema = std::make_shared<Schema>(fields);

  // The dictionary is embedded in a record batch with a single column
  std::shared_ptr<RecordBatch> batch;
  auto batch_meta =
      reinterpret_cast<const flatbuf::RecordBatch*>(dictionary_batch->data());
  RETURN_NOT_OK(
      ReadRecordBatch(batch_meta, dummy_schema, kMaxNestingDepth, file, &batch));

  if (batch->num_columns() != 1) {
    return Status::Invalid("Dictionary record batch must only contain one field");
  }

  *out = batch->column(0);
  return Status::OK();
}

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
    RETURN_NOT_OK(ReadMessage(stream_.get(), message));

    if ((*message) == nullptr) { return Status::OK(); }

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

    std::shared_ptr<Buffer> batch_body;
    RETURN_NOT_OK(ReadExact(message->body_length(), &batch_body))
    io::BufferReader reader(batch_body);

    std::shared_ptr<Array> dictionary;
    int64_t id;
    RETURN_NOT_OK(ReadDictionary(*message, dictionary_types_, &reader, &id, &dictionary));
    return dictionary_memo_.AddDictionary(id, dictionary);
  }

  Status ReadSchema() {
    std::shared_ptr<Message> message;
    RETURN_NOT_OK(ReadNextMessage(Message::SCHEMA, &message));

    RETURN_NOT_OK(GetDictionaryTypes(message->header(), &dictionary_types_));

    // TODO(wesm): In future, we may want to reconcile the ids in the stream with
    // those found in the schema
    int num_dictionaries = static_cast<int>(dictionary_types_.size());
    for (int i = 0; i < num_dictionaries; ++i) {
      RETURN_NOT_OK(ReadNextDictionary());
    }

    return GetSchema(message->header(), dictionary_memo_, &schema_);
  }

  Status GetNextRecordBatch(std::shared_ptr<RecordBatch>* batch) {
    std::shared_ptr<Message> message;
    RETURN_NOT_OK(ReadNextMessage(Message::RECORD_BATCH, &message));

    if (message == nullptr) {
      // End of stream
      *batch = nullptr;
      return Status::OK();
    }

    std::shared_ptr<Buffer> batch_body;
    RETURN_NOT_OK(ReadExact(message->body_length(), &batch_body));
    io::BufferReader reader(batch_body);
    return ReadRecordBatch(*message, schema_, &reader, batch);
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

  Status GetRecordBatch(int i, std::shared_ptr<RecordBatch>* batch) {
    DCHECK_GE(i, 0);
    DCHECK_LT(i, num_record_batches());
    FileBlock block = record_batch(i);

    std::shared_ptr<Message> message;
    RETURN_NOT_OK(
        ReadMessage(block.offset, block.metadata_length, file_.get(), &message));

    // TODO(wesm): ARROW-388 -- the buffer frame of reference is 0 (see
    // ARROW-384).
    std::shared_ptr<Buffer> buffer_block;
    RETURN_NOT_OK(file_->Read(block.body_length, &buffer_block));
    io::BufferReader reader(buffer_block);

    return ReadRecordBatch(*message, schema_, &reader, batch);
  }

  Status ReadSchema() {
    RETURN_NOT_OK(GetDictionaryTypes(footer_->schema(), &dictionary_fields_));

    // Read all the dictionaries
    for (int i = 0; i < num_dictionaries(); ++i) {
      FileBlock block = dictionary(i);
      std::shared_ptr<Message> message;
      RETURN_NOT_OK(
          ReadMessage(block.offset, block.metadata_length, file_.get(), &message));

      // TODO(wesm): ARROW-577: This code is a bit duplicated, can be fixed
      // with a more invasive refactor

      // TODO(wesm): ARROW-388 -- the buffer frame of reference is 0 (see
      // ARROW-384).
      std::shared_ptr<Buffer> buffer_block;
      RETURN_NOT_OK(file_->Read(block.body_length, &buffer_block));
      io::BufferReader reader(buffer_block);

      std::shared_ptr<Array> dictionary;
      int64_t dictionary_id;
      RETURN_NOT_OK(ReadDictionary(
          *message, dictionary_fields_, &reader, &dictionary_id, &dictionary));
      RETURN_NOT_OK(dictionary_memo_->AddDictionary(dictionary_id, dictionary));
    }

    // Get the schema
    return GetSchema(footer_->schema(), *dictionary_memo_, &schema_);
  }

  Status Open(const std::shared_ptr<io::RandomAccessFile>& file, int64_t footer_offset) {
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

static Status ReadContiguousPayload(int64_t offset, io::RandomAccessFile* file,
    std::shared_ptr<Message>* message, std::shared_ptr<Buffer>* payload) {
  std::shared_ptr<Buffer> buffer;
  RETURN_NOT_OK(file->Seek(offset));
  RETURN_NOT_OK(ReadMessage(file, message));

  if (*message == nullptr) {
    return Status::Invalid("Unable to read metadata at offset");
  }

  // TODO(ARROW-388): The buffer offsets start at 0, so we must construct a
  // RandomAccessFile according to that frame of reference
  RETURN_NOT_OK(file->Read((*message)->body_length(), payload));
  return Status::OK();
}

Status ReadRecordBatch(const std::shared_ptr<Schema>& schema, int64_t offset,
    io::RandomAccessFile* file, std::shared_ptr<RecordBatch>* out) {
  std::shared_ptr<Buffer> payload;
  std::shared_ptr<Message> message;

  RETURN_NOT_OK(ReadContiguousPayload(offset, file, &message, &payload));
  io::BufferReader buffer_reader(payload);
  return ReadRecordBatch(*message, schema, kMaxNestingDepth, &buffer_reader, out);
}

Status ReadTensor(
    int64_t offset, io::RandomAccessFile* file, std::shared_ptr<Tensor>* out) {
  std::shared_ptr<Message> message;
  std::shared_ptr<Buffer> data;
  RETURN_NOT_OK(ReadContiguousPayload(offset, file, &message, &data));

  std::shared_ptr<DataType> type;
  std::vector<int64_t> shape;
  std::vector<int64_t> strides;
  std::vector<std::string> dim_names;
  RETURN_NOT_OK(
      GetTensorMetadata(message->header(), &type, &shape, &strides, &dim_names));
  return MakeTensor(type, data, shape, strides, dim_names, out);
}

}  // namespace ipc
}  // namespace arrow
