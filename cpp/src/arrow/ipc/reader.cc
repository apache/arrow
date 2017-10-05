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
#include <type_traits>
#include <vector>

#include <flatbuffers/flatbuffers.h>  // IWYU pragma: export

#include "arrow/array.h"
#include "arrow/buffer.h"
#include "arrow/io/interfaces.h"
#include "arrow/io/memory.h"
#include "arrow/ipc/File_generated.h"
#include "arrow/ipc/Message_generated.h"
#include "arrow/ipc/Schema_generated.h"
#include "arrow/ipc/dictionary.h"
#include "arrow/ipc/message.h"
#include "arrow/ipc/metadata-internal.h"
#include "arrow/ipc/util.h"
#include "arrow/status.h"
#include "arrow/table.h"
#include "arrow/tensor.h"
#include "arrow/type.h"
#include "arrow/util/bit-util.h"
#include "arrow/util/logging.h"
#include "arrow/visitor_inline.h"

namespace arrow {

namespace flatbuf = org::apache::arrow::flatbuf;

namespace ipc {

using internal::FileBlock;
using internal::kArrowMagicBytes;

// ----------------------------------------------------------------------
// Record batch read path

/// Accessor class for flatbuffers metadata
class IpcComponentSource {
 public:
  IpcComponentSource(const flatbuf::RecordBatch* metadata, io::RandomAccessFile* file)
      : metadata_(metadata), file_(file) {}

  Status GetBuffer(int buffer_index, std::shared_ptr<Buffer>* out) {
    const flatbuf::Buffer* buffer = metadata_->buffers()->Get(buffer_index);

    if (buffer->length() == 0) {
      *out = nullptr;
      return Status::OK();
    } else {
      DCHECK(BitUtil::IsMultipleOf8(buffer->offset()))
          << "Buffer " << buffer_index
          << " did not start on 8-byte aligned offset: " << buffer->offset();
      return file_->ReadAt(buffer->offset(), buffer->length(), out);
    }
  }

  Status GetFieldMetadata(int field_index, ArrayData* out) {
    auto nodes = metadata_->nodes();
    // pop off a field
    if (field_index >= static_cast<int>(nodes->size())) {
      return Status::Invalid("Ran out of field metadata, likely malformed");
    }
    const flatbuf::FieldNode* node = nodes->Get(field_index);

    out->length = node->length();
    out->null_count = node->null_count();
    out->offset = 0;
    return Status::OK();
  }

 private:
  const flatbuf::RecordBatch* metadata_;
  io::RandomAccessFile* file_;
};

/// Bookkeeping struct for loading array objects from their constituent pieces of raw data
///
/// The field_index and buffer_index are incremented in the ArrayLoader
/// based on how much of the batch is "consumed" (through nested data
/// reconstruction, for example)
struct ArrayLoaderContext {
  IpcComponentSource* source;
  int buffer_index;
  int field_index;
  int max_recursion_depth;
};

static Status LoadArray(const std::shared_ptr<DataType>& type,
                        ArrayLoaderContext* context, ArrayData* out);

class ArrayLoader {
 public:
  ArrayLoader(const std::shared_ptr<DataType>& type, ArrayData* out,
              ArrayLoaderContext* context)
      : type_(type), context_(context), out_(out) {}

  Status Load() {
    if (context_->max_recursion_depth <= 0) {
      return Status::Invalid("Max recursion depth reached");
    }

    out_->type = type_;

    RETURN_NOT_OK(VisitTypeInline(*type_, this));
    return Status::OK();
  }

  Status GetBuffer(int buffer_index, std::shared_ptr<Buffer>* out) {
    return context_->source->GetBuffer(buffer_index, out);
  }

  Status LoadCommon() {
    // This only contains the length and null count, which we need to figure
    // out what to do with the buffers. For example, if null_count == 0, then
    // we can skip that buffer without reading from shared memory
    RETURN_NOT_OK(context_->source->GetFieldMetadata(context_->field_index++, out_));

    // extract null_bitmap which is common to all arrays
    if (out_->null_count == 0) {
      out_->buffers[0] = nullptr;
    } else {
      RETURN_NOT_OK(GetBuffer(context_->buffer_index, &out_->buffers[0]));
    }
    context_->buffer_index++;
    return Status::OK();
  }

  template <typename TYPE>
  Status LoadPrimitive() {
    out_->buffers.resize(2);

    RETURN_NOT_OK(LoadCommon());
    if (out_->length > 0) {
      RETURN_NOT_OK(GetBuffer(context_->buffer_index++, &out_->buffers[1]));
    } else {
      context_->buffer_index++;
      out_->buffers[1].reset(new Buffer(nullptr, 0));
    }
    return Status::OK();
  }

  template <typename TYPE>
  Status LoadBinary() {
    out_->buffers.resize(3);

    RETURN_NOT_OK(LoadCommon());
    RETURN_NOT_OK(GetBuffer(context_->buffer_index++, &out_->buffers[1]));
    return GetBuffer(context_->buffer_index++, &out_->buffers[2]);
  }

  Status LoadChild(const Field& field, ArrayData* out) {
    ArrayLoader loader(field.type(), out, context_);
    --context_->max_recursion_depth;
    RETURN_NOT_OK(loader.Load());
    ++context_->max_recursion_depth;
    return Status::OK();
  }

  Status LoadChildren(std::vector<std::shared_ptr<Field>> child_fields) {
    out_->child_data.reserve(static_cast<int>(child_fields.size()));

    for (const auto& child_field : child_fields) {
      auto field_array = std::make_shared<ArrayData>();
      RETURN_NOT_OK(LoadChild(*child_field.get(), field_array.get()));
      out_->child_data.emplace_back(field_array);
    }
    return Status::OK();
  }

  Status Visit(const NullType& type) {
    out_->buffers.resize(1);
    RETURN_NOT_OK(LoadCommon());
    RETURN_NOT_OK(GetBuffer(context_->buffer_index++, &out_->buffers[0]));
    return Status::OK();
  }

  template <typename T>
  typename std::enable_if<std::is_base_of<FixedWidthType, T>::value &&
                              !std::is_base_of<FixedSizeBinaryType, T>::value &&
                              !std::is_base_of<DictionaryType, T>::value,
                          Status>::type
  Visit(const T& type) {
    return LoadPrimitive<T>();
  }

  template <typename T>
  typename std::enable_if<std::is_base_of<BinaryType, T>::value, Status>::type Visit(
      const T& type) {
    return LoadBinary<T>();
  }

  Status Visit(const FixedSizeBinaryType& type) {
    out_->buffers.resize(2);
    RETURN_NOT_OK(LoadCommon());
    return GetBuffer(context_->buffer_index++, &out_->buffers[1]);
  }

  Status Visit(const ListType& type) {
    out_->buffers.resize(2);

    RETURN_NOT_OK(LoadCommon());
    RETURN_NOT_OK(GetBuffer(context_->buffer_index++, &out_->buffers[1]));

    const int num_children = type.num_children();
    if (num_children != 1) {
      std::stringstream ss;
      ss << "Wrong number of children: " << num_children;
      return Status::Invalid(ss.str());
    }

    return LoadChildren(type.children());
  }

  Status Visit(const StructType& type) {
    out_->buffers.resize(1);
    RETURN_NOT_OK(LoadCommon());
    return LoadChildren(type.children());
  }

  Status Visit(const UnionType& type) {
    out_->buffers.resize(3);

    RETURN_NOT_OK(LoadCommon());
    if (out_->length > 0) {
      RETURN_NOT_OK(GetBuffer(context_->buffer_index, &out_->buffers[1]));
      if (type.mode() == UnionMode::DENSE) {
        RETURN_NOT_OK(GetBuffer(context_->buffer_index + 1, &out_->buffers[2]));
      }
    }
    context_->buffer_index += type.mode() == UnionMode::DENSE ? 2 : 1;
    return LoadChildren(type.children());
  }

  Status Visit(const DictionaryType& type) {
    RETURN_NOT_OK(LoadArray(type.index_type(), context_, out_));
    out_->type = type_;
    return Status::OK();
  }

 private:
  const std::shared_ptr<DataType> type_;
  ArrayLoaderContext* context_;

  // Used in visitor pattern
  ArrayData* out_;
};

static Status LoadArray(const std::shared_ptr<DataType>& type,
                        ArrayLoaderContext* context, ArrayData* out) {
  ArrayLoader loader(type, out, context);
  return loader.Load();
}

Status ReadRecordBatch(const Buffer& metadata, const std::shared_ptr<Schema>& schema,
                       io::RandomAccessFile* file, std::shared_ptr<RecordBatch>* out) {
  return ReadRecordBatch(metadata, schema, kMaxNestingDepth, file, out);
}

Status ReadRecordBatch(const Message& message, const std::shared_ptr<Schema>& schema,
                       std::shared_ptr<RecordBatch>* out) {
  io::BufferReader reader(message.body());
  DCHECK_EQ(message.type(), Message::RECORD_BATCH);
  return ReadRecordBatch(*message.metadata(), schema, kMaxNestingDepth, &reader, out);
}

// ----------------------------------------------------------------------
// Array loading

static Status LoadRecordBatchFromSource(const std::shared_ptr<Schema>& schema,
                                        int64_t num_rows, int max_recursion_depth,
                                        IpcComponentSource* source,
                                        std::shared_ptr<RecordBatch>* out) {
  ArrayLoaderContext context;
  context.source = source;
  context.field_index = 0;
  context.buffer_index = 0;
  context.max_recursion_depth = max_recursion_depth;

  std::vector<std::shared_ptr<ArrayData>> arrays(schema->num_fields());
  for (int i = 0; i < schema->num_fields(); ++i) {
    auto arr = std::make_shared<ArrayData>();
    RETURN_NOT_OK(LoadArray(schema->field(i)->type(), &context, arr.get()));
    DCHECK_EQ(num_rows, arr->length) << "Array length did not match record batch length";
    arrays[i] = std::move(arr);
  }

  *out = std::make_shared<RecordBatch>(schema, num_rows, std::move(arrays));
  return Status::OK();
}

static inline Status ReadRecordBatch(const flatbuf::RecordBatch* metadata,
                                     const std::shared_ptr<Schema>& schema,
                                     int max_recursion_depth, io::RandomAccessFile* file,
                                     std::shared_ptr<RecordBatch>* out) {
  IpcComponentSource source(metadata, file);
  return LoadRecordBatchFromSource(schema, metadata->length(), max_recursion_depth,
                                   &source, out);
}

Status ReadRecordBatch(const Buffer& metadata, const std::shared_ptr<Schema>& schema,
                       int max_recursion_depth, io::RandomAccessFile* file,
                       std::shared_ptr<RecordBatch>* out) {
  auto message = flatbuf::GetMessage(metadata.data());
  if (message->header_type() != flatbuf::MessageHeader_RecordBatch) {
    DCHECK_EQ(message->header_type(), flatbuf::MessageHeader_RecordBatch);
  }
  auto batch = reinterpret_cast<const flatbuf::RecordBatch*>(message->header());
  return ReadRecordBatch(batch, schema, max_recursion_depth, file, out);
}

Status ReadDictionary(const Buffer& metadata, const DictionaryTypeMap& dictionary_types,
                      io::RandomAccessFile* file, int64_t* dictionary_id,
                      std::shared_ptr<Array>* out) {
  auto message = flatbuf::GetMessage(metadata.data());
  auto dictionary_batch =
      reinterpret_cast<const flatbuf::DictionaryBatch*>(message->header());

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

static Status ReadMessageAndValidate(MessageReader* reader, Message::Type expected_type,
                                     bool allow_null, std::unique_ptr<Message>* message) {
  RETURN_NOT_OK(reader->ReadNextMessage(message));

  if (!(*message) && !allow_null) {
    std::stringstream ss;
    ss << "Expected " << FormatMessageType(expected_type)
       << " message in stream, was null or length 0";
    return Status::Invalid(ss.str());
  }

  if ((*message) == nullptr) {
    return Status::OK();
  }

  if ((*message)->type() != expected_type) {
    std::stringstream ss;
    ss << "Message not expected type: " << FormatMessageType(expected_type)
       << ", was: " << (*message)->type();
    return Status::IOError(ss.str());
  }
  return Status::OK();
}

// ----------------------------------------------------------------------
// RecordBatchStreamReader implementation

static inline FileBlock FileBlockFromFlatbuffer(const flatbuf::Block* block) {
  return FileBlock{block->offset(), block->metaDataLength(), block->bodyLength()};
}

class RecordBatchStreamReader::RecordBatchStreamReaderImpl {
 public:
  RecordBatchStreamReaderImpl() {}
  ~RecordBatchStreamReaderImpl() {}

  Status Open(std::unique_ptr<MessageReader> message_reader) {
    message_reader_ = std::move(message_reader);
    return ReadSchema();
  }

  Status ReadNextDictionary() {
    std::unique_ptr<Message> message;
    RETURN_NOT_OK(ReadMessageAndValidate(message_reader_.get(), Message::DICTIONARY_BATCH,
                                         false, &message));

    io::BufferReader reader(message->body());

    std::shared_ptr<Array> dictionary;
    int64_t id;
    RETURN_NOT_OK(ReadDictionary(*message->metadata(), dictionary_types_, &reader, &id,
                                 &dictionary));
    return dictionary_memo_.AddDictionary(id, dictionary);
  }

  Status ReadSchema() {
    std::unique_ptr<Message> message;
    RETURN_NOT_OK(
        ReadMessageAndValidate(message_reader_.get(), Message::SCHEMA, false, &message));

    RETURN_NOT_OK(internal::GetDictionaryTypes(message->header(), &dictionary_types_));

    // TODO(wesm): In future, we may want to reconcile the ids in the stream with
    // those found in the schema
    int num_dictionaries = static_cast<int>(dictionary_types_.size());
    for (int i = 0; i < num_dictionaries; ++i) {
      RETURN_NOT_OK(ReadNextDictionary());
    }

    return internal::GetSchema(message->header(), dictionary_memo_, &schema_);
  }

  Status ReadNext(std::shared_ptr<RecordBatch>* batch) {
    std::unique_ptr<Message> message;
    RETURN_NOT_OK(ReadMessageAndValidate(message_reader_.get(), Message::RECORD_BATCH,
                                         true, &message));

    if (message == nullptr) {
      // End of stream
      *batch = nullptr;
      return Status::OK();
    }

    io::BufferReader reader(message->body());
    return ReadRecordBatch(*message->metadata(), schema_, &reader, batch);
  }

  std::shared_ptr<Schema> schema() const { return schema_; }

 private:
  std::unique_ptr<MessageReader> message_reader_;

  // dictionary_id -> type
  DictionaryTypeMap dictionary_types_;
  DictionaryMemo dictionary_memo_;
  std::shared_ptr<Schema> schema_;
};

RecordBatchStreamReader::RecordBatchStreamReader() {
  impl_.reset(new RecordBatchStreamReaderImpl());
}

RecordBatchStreamReader::~RecordBatchStreamReader() {}

Status RecordBatchStreamReader::Open(std::unique_ptr<MessageReader> message_reader,
                                     std::shared_ptr<RecordBatchReader>* reader) {
  // Private ctor
  auto result = std::shared_ptr<RecordBatchStreamReader>(new RecordBatchStreamReader());
  RETURN_NOT_OK(result->impl_->Open(std::move(message_reader)));
  *reader = result;
  return Status::OK();
}

Status RecordBatchStreamReader::Open(io::InputStream* stream,
                                     std::shared_ptr<RecordBatchReader>* out) {
  std::unique_ptr<MessageReader> message_reader(new InputStreamMessageReader(stream));
  return Open(std::move(message_reader), out);
}

Status RecordBatchStreamReader::Open(const std::shared_ptr<io::InputStream>& stream,
                                     std::shared_ptr<RecordBatchReader>* out) {
  std::unique_ptr<MessageReader> message_reader(new InputStreamMessageReader(stream));
  return Open(std::move(message_reader), out);
}

std::shared_ptr<Schema> RecordBatchStreamReader::schema() const {
  return impl_->schema();
}

Status RecordBatchStreamReader::ReadNext(std::shared_ptr<RecordBatch>* batch) {
  return impl_->ReadNext(batch);
}

// ----------------------------------------------------------------------
// Reader implementation

class RecordBatchFileReader::RecordBatchFileReaderImpl {
 public:
  RecordBatchFileReaderImpl() { dictionary_memo_ = std::make_shared<DictionaryMemo>(); }

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

    const int64_t expected_footer_size = magic_size + sizeof(int32_t);
    if (buffer->size() < expected_footer_size) {
      std::stringstream ss;
      ss << "Unable to read " << expected_footer_size << "from end of file";
      return Status::Invalid(ss.str());
    }

    if (memcmp(buffer->data() + sizeof(int32_t), kArrowMagicBytes, magic_size)) {
      return Status::Invalid("Not an Arrow file");
    }

    int32_t footer_length = *reinterpret_cast<const int32_t*>(buffer->data());

    if (footer_length <= 0 || footer_length + magic_size * 2 + 4 > footer_offset_) {
      return Status::Invalid("File is smaller than indicated metadata size");
    }

    // Now read the footer
    RETURN_NOT_OK(file_->ReadAt(footer_offset_ - footer_length - file_end_size,
                                footer_length, &footer_buffer_));

    // TODO(wesm): Verify the footer
    footer_ = flatbuf::GetFooter(footer_buffer_->data());

    return Status::OK();
  }

  int num_dictionaries() const { return footer_->dictionaries()->size(); }

  int num_record_batches() const { return footer_->recordBatches()->size(); }

  MetadataVersion version() const {
    switch (footer_->version()) {
      case flatbuf::MetadataVersion_V1:
        // Arrow 0.1
        return MetadataVersion::V1;
      case flatbuf::MetadataVersion_V2:
        // Arrow 0.2
        return MetadataVersion::V2;
      case flatbuf::MetadataVersion_V3:
        // Arrow 0.3
        return MetadataVersion::V3;
      // Add cases as other versions become available
      default:
        return MetadataVersion::V3;
    }
  }

  FileBlock record_batch(int i) const {
    return FileBlockFromFlatbuffer(footer_->recordBatches()->Get(i));
  }

  FileBlock dictionary(int i) const {
    return FileBlockFromFlatbuffer(footer_->dictionaries()->Get(i));
  }

  Status ReadRecordBatch(int i, std::shared_ptr<RecordBatch>* batch) {
    DCHECK_GE(i, 0);
    DCHECK_LT(i, num_record_batches());
    FileBlock block = record_batch(i);

    DCHECK(BitUtil::IsMultipleOf8(block.offset));
    DCHECK(BitUtil::IsMultipleOf8(block.metadata_length));
    DCHECK(BitUtil::IsMultipleOf8(block.body_length));

    std::unique_ptr<Message> message;
    RETURN_NOT_OK(ReadMessage(block.offset, block.metadata_length, file_, &message));

    io::BufferReader reader(message->body());
    return ::arrow::ipc::ReadRecordBatch(*message->metadata(), schema_, &reader, batch);
  }

  Status ReadSchema() {
    RETURN_NOT_OK(internal::GetDictionaryTypes(footer_->schema(), &dictionary_fields_));

    // Read all the dictionaries
    for (int i = 0; i < num_dictionaries(); ++i) {
      FileBlock block = dictionary(i);

      DCHECK(BitUtil::IsMultipleOf8(block.offset));
      DCHECK(BitUtil::IsMultipleOf8(block.metadata_length));
      DCHECK(BitUtil::IsMultipleOf8(block.body_length));

      std::unique_ptr<Message> message;
      RETURN_NOT_OK(ReadMessage(block.offset, block.metadata_length, file_, &message));

      io::BufferReader reader(message->body());

      std::shared_ptr<Array> dictionary;
      int64_t dictionary_id;
      RETURN_NOT_OK(ReadDictionary(*message->metadata(), dictionary_fields_, &reader,
                                   &dictionary_id, &dictionary));
      RETURN_NOT_OK(dictionary_memo_->AddDictionary(dictionary_id, dictionary));
    }

    // Get the schema
    return internal::GetSchema(footer_->schema(), *dictionary_memo_, &schema_);
  }

  Status Open(const std::shared_ptr<io::RandomAccessFile>& file, int64_t footer_offset) {
    owned_file_ = file;
    return Open(file.get(), footer_offset);
  }

  Status Open(io::RandomAccessFile* file, int64_t footer_offset) {
    file_ = file;
    footer_offset_ = footer_offset;
    RETURN_NOT_OK(ReadFooter());
    return ReadSchema();
  }

  std::shared_ptr<Schema> schema() const { return schema_; }

 private:
  io::RandomAccessFile* file_;

  std::shared_ptr<io::RandomAccessFile> owned_file_;

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

RecordBatchFileReader::RecordBatchFileReader() {
  impl_.reset(new RecordBatchFileReaderImpl());
}

RecordBatchFileReader::~RecordBatchFileReader() {}

Status RecordBatchFileReader::Open(io::RandomAccessFile* file,
                                   std::shared_ptr<RecordBatchFileReader>* reader) {
  int64_t footer_offset;
  RETURN_NOT_OK(file->GetSize(&footer_offset));
  return Open(file, footer_offset, reader);
}

Status RecordBatchFileReader::Open(io::RandomAccessFile* file, int64_t footer_offset,
                                   std::shared_ptr<RecordBatchFileReader>* reader) {
  *reader = std::shared_ptr<RecordBatchFileReader>(new RecordBatchFileReader());
  return (*reader)->impl_->Open(file, footer_offset);
}

Status RecordBatchFileReader::Open(const std::shared_ptr<io::RandomAccessFile>& file,
                                   std::shared_ptr<RecordBatchFileReader>* reader) {
  int64_t footer_offset;
  RETURN_NOT_OK(file->GetSize(&footer_offset));
  return Open(file, footer_offset, reader);
}

Status RecordBatchFileReader::Open(const std::shared_ptr<io::RandomAccessFile>& file,
                                   int64_t footer_offset,
                                   std::shared_ptr<RecordBatchFileReader>* reader) {
  *reader = std::shared_ptr<RecordBatchFileReader>(new RecordBatchFileReader());
  return (*reader)->impl_->Open(file, footer_offset);
}

std::shared_ptr<Schema> RecordBatchFileReader::schema() const { return impl_->schema(); }

int RecordBatchFileReader::num_record_batches() const {
  return impl_->num_record_batches();
}

MetadataVersion RecordBatchFileReader::version() const { return impl_->version(); }

Status RecordBatchFileReader::ReadRecordBatch(int i,
                                              std::shared_ptr<RecordBatch>* batch) {
  return impl_->ReadRecordBatch(i, batch);
}

static Status ReadContiguousPayload(io::InputStream* file,
                                    std::unique_ptr<Message>* message) {
  RETURN_NOT_OK(ReadMessage(file, message));
  if (*message == nullptr) {
    return Status::Invalid("Unable to read metadata at offset");
  }
  return Status::OK();
}

Status ReadSchema(io::InputStream* stream, std::shared_ptr<Schema>* out) {
  std::shared_ptr<RecordBatchReader> reader;
  RETURN_NOT_OK(RecordBatchStreamReader::Open(stream, &reader));
  *out = reader->schema();
  return Status::OK();
}

Status ReadRecordBatch(const std::shared_ptr<Schema>& schema, io::InputStream* file,
                       std::shared_ptr<RecordBatch>* out) {
  std::unique_ptr<Message> message;
  RETURN_NOT_OK(ReadContiguousPayload(file, &message));
  io::BufferReader buffer_reader(message->body());
  return ReadRecordBatch(*message->metadata(), schema, kMaxNestingDepth, &buffer_reader,
                         out);
}

Status ReadTensor(int64_t offset, io::RandomAccessFile* file,
                  std::shared_ptr<Tensor>* out) {
  // Respect alignment of Tensor messages (see WriteTensor)
  offset = PaddedLength(offset);
  RETURN_NOT_OK(file->Seek(offset));

  std::unique_ptr<Message> message;
  RETURN_NOT_OK(ReadContiguousPayload(file, &message));

  std::shared_ptr<DataType> type;
  std::vector<int64_t> shape;
  std::vector<int64_t> strides;
  std::vector<std::string> dim_names;
  RETURN_NOT_OK(internal::GetTensorMetadata(*message->metadata(), &type, &shape, &strides,
                                            &dim_names));
  *out = std::make_shared<Tensor>(type, message->body(), shape, strides, dim_names);
  return Status::OK();
}

}  // namespace ipc
}  // namespace arrow
