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
#include <utility>
#include <vector>

#include <flatbuffers/flatbuffers.h>  // IWYU pragma: export

#include "arrow/array.h"
#include "arrow/buffer.h"
#include "arrow/io/interfaces.h"
#include "arrow/io/memory.h"
#include "arrow/ipc/dictionary.h"
#include "arrow/ipc/message.h"
#include "arrow/ipc/metadata_internal.h"
#include "arrow/record_batch.h"
#include "arrow/sparse_tensor.h"
#include "arrow/status.h"
#include "arrow/tensor.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/logging.h"
#include "arrow/visitor_inline.h"

#include "generated/File_generated.h"  // IWYU pragma: export
#include "generated/Message_generated.h"
#include "generated/Schema_generated.h"

namespace arrow {

namespace flatbuf = org::apache::arrow::flatbuf;

using internal::checked_cast;
using internal::checked_pointer_cast;

namespace ipc {

using internal::FileBlock;
using internal::kArrowMagicBytes;

namespace {

Status InvalidMessageType(Message::Type expected, Message::Type actual) {
  return Status::IOError("Expected IPC message of type ", FormatMessageType(expected),
                         " but got ", FormatMessageType(actual));
}

#define CHECK_MESSAGE_TYPE(expected, actual)           \
  do {                                                 \
    if ((actual) != (expected)) {                      \
      return InvalidMessageType((expected), (actual)); \
    }                                                  \
  } while (0)

#define CHECK_HAS_BODY(message)                                       \
  do {                                                                \
    if ((message).body() == nullptr) {                                \
      return Status::IOError("Expected body in IPC message of type ", \
                             FormatMessageType((message).type()));    \
    }                                                                 \
  } while (0)

#define CHECK_HAS_NO_BODY(message)                                      \
  do {                                                                  \
    if ((message).body_length() != 0) {                                 \
      return Status::IOError("Unexpected body in IPC message of type ", \
                             FormatMessageType((message).type()));      \
    }                                                                   \
  } while (0)

}  // namespace

// ----------------------------------------------------------------------
// Record batch read path

/// Accessor class for flatbuffers metadata
class IpcComponentSource {
 public:
  IpcComponentSource(const flatbuf::RecordBatch* metadata, io::RandomAccessFile* file)
      : metadata_(metadata), file_(file) {}

  Status GetBuffer(int buffer_index, std::shared_ptr<Buffer>* out) {
    auto buffers = metadata_->buffers();
    if (buffers == nullptr) {
      return Status::IOError(
          "Buffers-pointer of flatbuffer-encoded RecordBatch is null.");
    }
    if (buffer_index >= static_cast<int>(buffers->size())) {
      return Status::IOError("buffer_index out of range.");
    }
    const flatbuf::Buffer* buffer = buffers->Get(buffer_index);

    if (buffer->length() == 0) {
      // Should never return a null buffer here.
      // (zero-sized buffer allocations are cheap)
      return AllocateBuffer(0, out);
    } else {
      if (!BitUtil::IsMultipleOf8(buffer->offset())) {
        return Status::Invalid(
            "Buffer ", buffer_index,
            " did not start on 8-byte aligned offset: ", buffer->offset());
      }
      return file_->ReadAt(buffer->offset(), buffer->length()).Value(out);
    }
  }

  Status GetFieldMetadata(int field_index, ArrayData* out) {
    auto nodes = metadata_->nodes();
    if (nodes == nullptr) {
      return Status::IOError("Nodes-pointer of flatbuffer-encoded Table is null.");
    }
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
  const DictionaryMemo* dictionary_memo;
  int buffer_index;
  int field_index;
  int max_recursion_depth;
};

static Status LoadArray(const Field& field, ArrayLoaderContext* context, ArrayData* out);

class ArrayLoader {
 public:
  ArrayLoader(const Field& field, ArrayData* out, ArrayLoaderContext* context)
      : field_(field), context_(context), out_(out) {}

  Status Load() {
    if (context_->max_recursion_depth <= 0) {
      return Status::Invalid("Max recursion depth reached");
    }

    RETURN_NOT_OK(VisitTypeInline(*field_.type(), this));
    out_->type = field_.type();
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

  template <typename TYPE>
  Status LoadList(const TYPE& type) {
    out_->buffers.resize(2);

    RETURN_NOT_OK(LoadCommon());
    RETURN_NOT_OK(GetBuffer(context_->buffer_index++, &out_->buffers[1]));

    const int num_children = type.num_children();
    if (num_children != 1) {
      return Status::Invalid("Wrong number of children: ", num_children);
    }

    return LoadChildren(type.children());
  }

  Status LoadChild(const Field& field, ArrayData* out) {
    ArrayLoader loader(field, out, context_);
    --context_->max_recursion_depth;
    RETURN_NOT_OK(loader.Load());
    ++context_->max_recursion_depth;
    return Status::OK();
  }

  Status LoadChildren(std::vector<std::shared_ptr<Field>> child_fields) {
    out_->child_data.reserve(static_cast<int>(child_fields.size()));

    for (const auto& child_field : child_fields) {
      auto field_array = std::make_shared<ArrayData>();
      RETURN_NOT_OK(LoadChild(*child_field, field_array.get()));
      out_->child_data.emplace_back(field_array);
    }
    return Status::OK();
  }

  Status Visit(const NullType& type) {
    out_->buffers.resize(1);

    // ARROW-6379: NullType has no buffers in the IPC payload
    RETURN_NOT_OK(context_->source->GetFieldMetadata(context_->field_index++, out_));
    return Status::OK();
  }

  template <typename T>
  enable_if_t<std::is_base_of<FixedWidthType, T>::value &&
                  !std::is_base_of<FixedSizeBinaryType, T>::value &&
                  !std::is_base_of<DictionaryType, T>::value,
              Status>
  Visit(const T& type) {
    return LoadPrimitive<T>();
  }

  template <typename T>
  enable_if_base_binary<T, Status> Visit(const T& type) {
    return LoadBinary<T>();
  }

  Status Visit(const FixedSizeBinaryType& type) {
    out_->buffers.resize(2);
    RETURN_NOT_OK(LoadCommon());
    return GetBuffer(context_->buffer_index++, &out_->buffers[1]);
  }

  template <typename T>
  enable_if_base_list<T, Status> Visit(const T& type) {
    return LoadList(type);
  }

  Status Visit(const FixedSizeListType& type) {
    out_->buffers.resize(1);

    RETURN_NOT_OK(LoadCommon());

    const int num_children = type.num_children();
    if (num_children != 1) {
      return Status::Invalid("Wrong number of children: ", num_children);
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
    RETURN_NOT_OK(
        LoadArray(*::arrow::field("indices", type.index_type()), context_, out_));

    // Look up dictionary
    int64_t id = -1;
    RETURN_NOT_OK(context_->dictionary_memo->GetId(field_, &id));
    RETURN_NOT_OK(context_->dictionary_memo->GetDictionary(id, &out_->dictionary));

    return Status::OK();
  }

  Status Visit(const ExtensionType& type) {
    return LoadArray(*::arrow::field("storage", type.storage_type()), context_, out_);
  }

 private:
  const Field& field_;
  ArrayLoaderContext* context_;

  // Used in visitor pattern
  ArrayData* out_;
};

static Status LoadArray(const Field& field, ArrayLoaderContext* context, ArrayData* out) {
  ArrayLoader loader(field, out, context);
  return loader.Load();
}

Status ReadRecordBatch(const Buffer& metadata, const std::shared_ptr<Schema>& schema,
                       const DictionaryMemo* dictionary_memo, io::RandomAccessFile* file,
                       std::shared_ptr<RecordBatch>* out) {
  auto options = IpcOptions::Defaults();
  return ReadRecordBatch(metadata, schema, dictionary_memo, options, file, out);
}

Status ReadRecordBatch(const Message& message, const std::shared_ptr<Schema>& schema,
                       const DictionaryMemo* dictionary_memo,
                       std::shared_ptr<RecordBatch>* out) {
  CHECK_MESSAGE_TYPE(Message::RECORD_BATCH, message.type());
  CHECK_HAS_BODY(message);
  auto options = IpcOptions::Defaults();
  io::BufferReader reader(message.body());
  return ReadRecordBatch(*message.metadata(), schema, dictionary_memo, options, &reader,
                         out);
}

// ----------------------------------------------------------------------
// Array loading

static Status LoadRecordBatchFromSource(const std::shared_ptr<Schema>& schema,
                                        int64_t num_rows, int max_recursion_depth,
                                        IpcComponentSource* source,
                                        const DictionaryMemo* dictionary_memo,
                                        std::shared_ptr<RecordBatch>* out) {
  ArrayLoaderContext context{source, dictionary_memo, /*field_index=*/0,
                             /*buffer_index=*/0, max_recursion_depth};

  std::vector<std::shared_ptr<ArrayData>> arrays(schema->num_fields());
  for (int i = 0; i < schema->num_fields(); ++i) {
    auto arr = std::make_shared<ArrayData>();
    RETURN_NOT_OK(LoadArray(*schema->field(i), &context, arr.get()));
    if (num_rows != arr->length) {
      return Status::IOError("Array length did not match record batch length");
    }
    arrays[i] = std::move(arr);
  }

  *out = RecordBatch::Make(schema, num_rows, std::move(arrays));
  return Status::OK();
}

static inline Status ReadRecordBatch(const flatbuf::RecordBatch* metadata,
                                     const std::shared_ptr<Schema>& schema,
                                     const DictionaryMemo* dictionary_memo,
                                     const IpcOptions& options,
                                     io::RandomAccessFile* file,
                                     std::shared_ptr<RecordBatch>* out) {
  IpcComponentSource source(metadata, file);
  return LoadRecordBatchFromSource(schema, metadata->length(),
                                   options.max_recursion_depth, &source, dictionary_memo,
                                   out);
}

Status ReadRecordBatch(const Buffer& metadata, const std::shared_ptr<Schema>& schema,
                       const DictionaryMemo* dictionary_memo, const IpcOptions& options,
                       io::RandomAccessFile* file, std::shared_ptr<RecordBatch>* out) {
  const flatbuf::Message* message;
  RETURN_NOT_OK(internal::VerifyMessage(metadata.data(), metadata.size(), &message));
  auto batch = message->header_as_RecordBatch();
  if (batch == nullptr) {
    return Status::IOError(
        "Header-type of flatbuffer-encoded Message is not RecordBatch.");
  }
  return ReadRecordBatch(batch, schema, dictionary_memo, options, file, out);
}

Status ReadDictionary(const Buffer& metadata, DictionaryMemo* dictionary_memo,
                      io::RandomAccessFile* file) {
  auto options = IpcOptions::Defaults();

  const flatbuf::Message* message;
  RETURN_NOT_OK(internal::VerifyMessage(metadata.data(), metadata.size(), &message));
  auto dictionary_batch = message->header_as_DictionaryBatch();
  if (dictionary_batch == nullptr) {
    return Status::IOError(
        "Header-type of flatbuffer-encoded Message is not DictionaryBatch.");
  }

  int64_t id = dictionary_batch->id();

  // Look up the field, which must have been added to the
  // DictionaryMemo already prior to invoking this function
  std::shared_ptr<DataType> value_type;
  RETURN_NOT_OK(dictionary_memo->GetDictionaryType(id, &value_type));

  auto value_field = ::arrow::field("dummy", value_type);

  // The dictionary is embedded in a record batch with a single column
  std::shared_ptr<RecordBatch> batch;
  auto batch_meta = dictionary_batch->data();
  RETURN_NOT_OK(ReadRecordBatch(batch_meta, ::arrow::schema({value_field}),
                                dictionary_memo, options, file, &batch));
  if (batch->num_columns() != 1) {
    return Status::Invalid("Dictionary record batch must only contain one field");
  }
  auto dictionary = batch->column(0);
  return dictionary_memo->AddDictionary(id, dictionary);
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

  Status ReadSchema() {
    std::unique_ptr<Message> message;
    RETURN_NOT_OK(message_reader_->ReadNextMessage(&message));
    if (!message) {
      return Status::Invalid("Tried reading schema message, was null or length 0");
    }
    CHECK_MESSAGE_TYPE(Message::SCHEMA, message->type());
    CHECK_HAS_NO_BODY(*message);
    if (message->header() == nullptr) {
      return Status::IOError("Header-pointer of flatbuffer-encoded Message is null.");
    }
    return internal::GetSchema(message->header(), &dictionary_memo_, &schema_);
  }

  Status ParseDictionary(const Message& message) {
    // Only invoke this method if we already know we have a dictionary message
    DCHECK_EQ(message.type(), Message::DICTIONARY_BATCH);
    CHECK_HAS_BODY(message);
    io::BufferReader reader(message.body());
    return ReadDictionary(*message.metadata(), &dictionary_memo_, &reader);
  }

  Status ReadInitialDictionaries() {
    // We must receive all dictionaries before reconstructing the
    // first record batch. Subsequent dictionary deltas modify the memo
    std::unique_ptr<Message> message;

    // TODO(wesm): In future, we may want to reconcile the ids in the stream with
    // those found in the schema
    for (int i = 0; i < dictionary_memo_.num_fields(); ++i) {
      RETURN_NOT_OK(message_reader_->ReadNextMessage(&message));
      if (!message) {
        if (i == 0) {
          /// ARROW-6006: If we fail to find any dictionaries in the stream, then
          /// it may be that the stream has a schema but no actual data. In such
          /// case we communicate that we were unable to find the dictionaries
          /// (but there was no failure otherwise), so the caller can decide what
          /// to do
          empty_stream_ = true;
          break;
        } else {
          // ARROW-6126, the stream terminated before receiving the expected
          // number of dictionaries
          return Status::Invalid("IPC stream ended without reading the expected number (",
                                 dictionary_memo_.num_fields(), ") of dictionaries");
        }
      }

      if (message->type() != Message::DICTIONARY_BATCH) {
        return Status::Invalid("IPC stream did not have the expected number (",
                               dictionary_memo_.num_fields(),
                               ") of dictionaries at the start of the stream");
      }
      RETURN_NOT_OK(ParseDictionary(*message));
    }

    read_initial_dictionaries_ = true;
    return Status::OK();
  }

  Status ReadNext(std::shared_ptr<RecordBatch>* batch) {
    if (!read_initial_dictionaries_) {
      RETURN_NOT_OK(ReadInitialDictionaries());
    }

    if (empty_stream_) {
      // ARROW-6006: Degenerate case where stream contains no data, we do not
      // bother trying to read a RecordBatch message from the stream
      *batch = nullptr;
      return Status::OK();
    }

    std::unique_ptr<Message> message;
    RETURN_NOT_OK(message_reader_->ReadNextMessage(&message));
    if (message == nullptr) {
      // End of stream
      *batch = nullptr;
      return Status::OK();
    }

    if (message->type() == Message::DICTIONARY_BATCH) {
      // TODO(wesm): implement delta dictionaries
      return Status::NotImplemented("Delta dictionaries not yet implemented");
    } else {
      CHECK_HAS_BODY(*message);
      io::BufferReader reader(message->body());
      return ReadRecordBatch(*message->metadata(), schema_, &dictionary_memo_, &reader,
                             batch);
    }
  }

  std::shared_ptr<Schema> schema() const { return schema_; }

 private:
  std::unique_ptr<MessageReader> message_reader_;

  bool read_initial_dictionaries_ = false;

  // Flag to set in case where we fail to observe all dictionaries in a stream,
  // and so the reader should not attempt to parse any messages
  bool empty_stream_ = false;

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

Status RecordBatchStreamReader::Open(std::unique_ptr<MessageReader> message_reader,
                                     std::unique_ptr<RecordBatchReader>* reader) {
  // Private ctor
  auto result = std::unique_ptr<RecordBatchStreamReader>(new RecordBatchStreamReader());
  RETURN_NOT_OK(result->impl_->Open(std::move(message_reader)));
  *reader = std::move(result);
  return Status::OK();
}

Status RecordBatchStreamReader::Open(io::InputStream* stream,
                                     std::shared_ptr<RecordBatchReader>* out) {
  return Open(MessageReader::Open(stream), out);
}

Status RecordBatchStreamReader::Open(const std::shared_ptr<io::InputStream>& stream,
                                     std::shared_ptr<RecordBatchReader>* out) {
  return Open(MessageReader::Open(stream), out);
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
  RecordBatchFileReaderImpl() : file_(NULLPTR), footer_offset_(0), footer_(NULLPTR) {}

  Status ReadFooter() {
    const int32_t magic_size = static_cast<int>(strlen(kArrowMagicBytes));

    if (footer_offset_ <= magic_size * 2 + 4) {
      return Status::Invalid("File is too small: ", footer_offset_);
    }

    int file_end_size = static_cast<int>(magic_size + sizeof(int32_t));
    ARROW_ASSIGN_OR_RAISE(auto buffer,
                          file_->ReadAt(footer_offset_ - file_end_size, file_end_size));

    const int64_t expected_footer_size = magic_size + sizeof(int32_t);
    if (buffer->size() < expected_footer_size) {
      return Status::Invalid("Unable to read ", expected_footer_size, "from end of file");
    }

    if (memcmp(buffer->data() + sizeof(int32_t), kArrowMagicBytes, magic_size)) {
      return Status::Invalid("Not an Arrow file");
    }

    int32_t footer_length = *reinterpret_cast<const int32_t*>(buffer->data());

    if (footer_length <= 0 || footer_length > footer_offset_ - magic_size * 2 - 4) {
      return Status::Invalid("File is smaller than indicated metadata size");
    }

    // Now read the footer
    ARROW_ASSIGN_OR_RAISE(
        footer_buffer_,
        file_->ReadAt(footer_offset_ - footer_length - file_end_size, footer_length));

    auto data = footer_buffer_->data();
    flatbuffers::Verifier verifier(data, footer_buffer_->size(), 128);
    if (!flatbuf::VerifyFooterBuffer(verifier)) {
      return Status::IOError("Verification of flatbuffer-encoded Footer failed.");
    }
    footer_ = flatbuf::GetFooter(data);

    return Status::OK();
  }

  int num_dictionaries() const { return footer_->dictionaries()->size(); }

  int num_record_batches() const { return footer_->recordBatches()->size(); }

  MetadataVersion version() const {
    return internal::GetMetadataVersion(footer_->version());
  }

  FileBlock GetRecordBatchBlock(int i) const {
    return FileBlockFromFlatbuffer(footer_->recordBatches()->Get(i));
  }

  FileBlock GetDictionaryBlock(int i) const {
    return FileBlockFromFlatbuffer(footer_->dictionaries()->Get(i));
  }

  Status ReadMessageFromBlock(const FileBlock& block, std::unique_ptr<Message>* out) {
    if (!BitUtil::IsMultipleOf8(block.offset) ||
        !BitUtil::IsMultipleOf8(block.metadata_length) ||
        !BitUtil::IsMultipleOf8(block.body_length)) {
      return Status::Invalid("Unaligned block in IPC file");
    }

    RETURN_NOT_OK(ReadMessage(block.offset, block.metadata_length, file_, out));

    // TODO(wesm): this breaks integration tests, see ARROW-3256
    // DCHECK_EQ((*out)->body_length(), block.body_length);
    return Status::OK();
  }

  Status ReadDictionaries() {
    // Read all the dictionaries
    for (int i = 0; i < num_dictionaries(); ++i) {
      std::unique_ptr<Message> message;
      RETURN_NOT_OK(ReadMessageFromBlock(GetDictionaryBlock(i), &message));

      CHECK_HAS_BODY(*message);
      io::BufferReader reader(message->body());
      RETURN_NOT_OK(ReadDictionary(*message->metadata(), &dictionary_memo_, &reader));
    }
    return Status::OK();
  }

  Status ReadRecordBatch(int i, std::shared_ptr<RecordBatch>* batch) {
    DCHECK_GE(i, 0);
    DCHECK_LT(i, num_record_batches());

    if (!read_dictionaries_) {
      RETURN_NOT_OK(ReadDictionaries());
      read_dictionaries_ = true;
    }

    std::unique_ptr<Message> message;
    RETURN_NOT_OK(ReadMessageFromBlock(GetRecordBatchBlock(i), &message));

    CHECK_HAS_BODY(*message);
    io::BufferReader reader(message->body());
    return ::arrow::ipc::ReadRecordBatch(*message->metadata(), schema_, &dictionary_memo_,
                                         &reader, batch);
  }

  Status ReadSchema() {
    // Get the schema and record any observed dictionaries
    return internal::GetSchema(footer_->schema(), &dictionary_memo_, &schema_);
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

  bool read_dictionaries_ = false;
  DictionaryMemo dictionary_memo_;

  // Reconstructed schema, including any read dictionaries
  std::shared_ptr<Schema> schema_;
};

RecordBatchFileReader::RecordBatchFileReader() {
  impl_.reset(new RecordBatchFileReaderImpl());
}

RecordBatchFileReader::~RecordBatchFileReader() {}

Status RecordBatchFileReader::Open(io::RandomAccessFile* file,
                                   std::shared_ptr<RecordBatchFileReader>* reader) {
  ARROW_ASSIGN_OR_RAISE(int64_t footer_offset, file->GetSize());
  return Open(file, footer_offset, reader);
}

Status RecordBatchFileReader::Open(io::RandomAccessFile* file, int64_t footer_offset,
                                   std::shared_ptr<RecordBatchFileReader>* reader) {
  *reader = std::shared_ptr<RecordBatchFileReader>(new RecordBatchFileReader());
  return (*reader)->impl_->Open(file, footer_offset);
}

Status RecordBatchFileReader::Open(const std::shared_ptr<io::RandomAccessFile>& file,
                                   std::shared_ptr<RecordBatchFileReader>* reader) {
  ARROW_ASSIGN_OR_RAISE(int64_t footer_offset, file->GetSize());
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

Status ReadSchema(io::InputStream* stream, DictionaryMemo* dictionary_memo,
                  std::shared_ptr<Schema>* out) {
  std::unique_ptr<MessageReader> reader = MessageReader::Open(stream);
  std::unique_ptr<Message> message;
  RETURN_NOT_OK(reader->ReadNextMessage(&message));
  if (!message) {
    return Status::Invalid("Tried reading schema message, was null or length 0");
  }
  CHECK_MESSAGE_TYPE(Message::SCHEMA, message->type());
  return ReadSchema(*message, dictionary_memo, out);
}

Status ReadSchema(const Message& message, DictionaryMemo* dictionary_memo,
                  std::shared_ptr<Schema>* out) {
  std::shared_ptr<RecordBatchReader> reader;
  return internal::GetSchema(message.header(), dictionary_memo, &*out);
}

Status ReadRecordBatch(const std::shared_ptr<Schema>& schema,
                       const DictionaryMemo* dictionary_memo, io::InputStream* file,
                       std::shared_ptr<RecordBatch>* out) {
  auto options = IpcOptions::Defaults();
  std::unique_ptr<Message> message;
  RETURN_NOT_OK(ReadContiguousPayload(file, &message));
  CHECK_HAS_BODY(*message);
  io::BufferReader buffer_reader(message->body());
  return ReadRecordBatch(*message->metadata(), schema, dictionary_memo, options,
                         &buffer_reader, out);
}

Result<std::shared_ptr<Tensor>> ReadTensor(io::InputStream* file) {
  std::unique_ptr<Message> message;
  RETURN_NOT_OK(ReadContiguousPayload(file, &message));
  return ReadTensor(*message);
}

Result<std::shared_ptr<Tensor>> ReadTensor(const Message& message) {
  std::shared_ptr<DataType> type;
  std::vector<int64_t> shape;
  std::vector<int64_t> strides;
  std::vector<std::string> dim_names;
  CHECK_HAS_BODY(message);
  RETURN_NOT_OK(internal::GetTensorMetadata(*message.metadata(), &type, &shape, &strides,
                                            &dim_names));
  return Tensor::Make(type, message.body(), shape, strides, dim_names);
}

namespace {

Result<std::shared_ptr<SparseIndex>> ReadSparseCOOIndex(
    const flatbuf::SparseTensor* sparse_tensor, const std::vector<int64_t>& shape,
    int64_t non_zero_length, io::RandomAccessFile* file) {
  auto* sparse_index = sparse_tensor->sparseIndex_as_SparseTensorIndexCOO();
  const auto ndim = static_cast<int64_t>(shape.size());

  std::shared_ptr<DataType> indices_type;
  RETURN_NOT_OK(internal::GetSparseCOOIndexMetadata(sparse_index, &indices_type));
  const int64_t indices_elsize =
      checked_cast<const IntegerType&>(*indices_type).bit_width() / 8;

  auto* indices_buffer = sparse_index->indicesBuffer();
  ARROW_ASSIGN_OR_RAISE(auto indices_data,
                        file->ReadAt(indices_buffer->offset(), indices_buffer->length()));
  std::vector<int64_t> indices_shape({non_zero_length, ndim});
  auto* indices_strides = sparse_index->indicesStrides();
  std::vector<int64_t> strides(2);
  if (indices_strides && indices_strides->size() > 0) {
    if (indices_strides->size() != 2) {
      return Status::Invalid("Wrong size for indicesStrides in SparseCOOIndex");
    }
    strides[0] = indices_strides->Get(0);
    strides[1] = indices_strides->Get(1);
  } else {
    // Row-major by default
    strides[0] = indices_elsize * ndim;
    strides[1] = indices_elsize;
  }
  return std::make_shared<SparseCOOIndex>(
      std::make_shared<Tensor>(indices_type, indices_data, indices_shape, strides));
}

Result<std::shared_ptr<SparseIndex>> ReadSparseCSXIndex(
    const flatbuf::SparseTensor* sparse_tensor, const std::vector<int64_t>& shape,
    int64_t non_zero_length, io::RandomAccessFile* file) {
  if (shape.size() != 2) {
    return Status::Invalid("Invalid shape length for a sparse matrix");
  }

  auto* sparse_index = sparse_tensor->sparseIndex_as_SparseMatrixIndexCSX();

  std::shared_ptr<DataType> indptr_type, indices_type;
  RETURN_NOT_OK(
      internal::GetSparseCSXIndexMetadata(sparse_index, &indptr_type, &indices_type));

  auto* indptr_buffer = sparse_index->indptrBuffer();
  ARROW_ASSIGN_OR_RAISE(auto indptr_data,
                        file->ReadAt(indptr_buffer->offset(), indptr_buffer->length()));

  auto* indices_buffer = sparse_index->indicesBuffer();
  ARROW_ASSIGN_OR_RAISE(auto indices_data,
                        file->ReadAt(indices_buffer->offset(), indices_buffer->length()));

  std::vector<int64_t> indices_shape({non_zero_length});
  const auto indices_minimum_bytes =
      indices_shape[0] * checked_pointer_cast<FixedWidthType>(indices_type)->bit_width() /
      CHAR_BIT;
  if (indices_minimum_bytes > indices_buffer->length()) {
    return Status::Invalid("shape is inconsistent to the size of indices buffer");
  }

  switch (sparse_index->compressedAxis()) {
    case flatbuf::SparseMatrixCompressedAxis::Row: {
      std::vector<int64_t> indptr_shape({shape[0] + 1});
      const int64_t indptr_minimum_bytes =
          indptr_shape[0] *
          checked_pointer_cast<FixedWidthType>(indptr_type)->bit_width() / CHAR_BIT;
      if (indptr_minimum_bytes > indptr_buffer->length()) {
        return Status::Invalid("shape is inconsistent to the size of indptr buffer");
      }
      return std::make_shared<SparseCSRIndex>(
          std::make_shared<Tensor>(indptr_type, indptr_data, indptr_shape),
          std::make_shared<Tensor>(indices_type, indices_data, indices_shape));
    }
    case flatbuf::SparseMatrixCompressedAxis::Column: {
      std::vector<int64_t> indptr_shape({shape[1] + 1});
      const int64_t indptr_minimum_bytes =
          indptr_shape[0] *
          checked_pointer_cast<FixedWidthType>(indptr_type)->bit_width() / CHAR_BIT;
      if (indptr_minimum_bytes > indptr_buffer->length()) {
        return Status::Invalid("shape is inconsistent to the size of indptr buffer");
      }
      return std::make_shared<SparseCSCIndex>(
          std::make_shared<Tensor>(indptr_type, indptr_data, indptr_shape),
          std::make_shared<Tensor>(indices_type, indices_data, indices_shape));
    }
    default:
      return Status::Invalid("Invalid value of SparseMatrixCompressedAxis");
  }
}

Result<std::shared_ptr<SparseTensor>> MakeSparseTensorWithSparseCOOIndex(
    const std::shared_ptr<DataType>& type, const std::vector<int64_t>& shape,
    const std::vector<std::string>& dim_names,
    const std::shared_ptr<SparseCOOIndex>& sparse_index, int64_t non_zero_length,
    const std::shared_ptr<Buffer>& data) {
  return SparseCOOTensor::Make(sparse_index, type, data, shape, dim_names);
}

Result<std::shared_ptr<SparseTensor>> MakeSparseTensorWithSparseCSRIndex(
    const std::shared_ptr<DataType>& type, const std::vector<int64_t>& shape,
    const std::vector<std::string>& dim_names,
    const std::shared_ptr<SparseCSRIndex>& sparse_index, int64_t non_zero_length,
    const std::shared_ptr<Buffer>& data) {
  return SparseCSRMatrix::Make(sparse_index, type, data, shape, dim_names);
}

Result<std::shared_ptr<SparseTensor>> MakeSparseTensorWithSparseCSCIndex(
    const std::shared_ptr<DataType>& type, const std::vector<int64_t>& shape,
    const std::vector<std::string>& dim_names,
    const std::shared_ptr<SparseCSCIndex>& sparse_index, int64_t non_zero_length,
    const std::shared_ptr<Buffer>& data) {
  return SparseCSCMatrix::Make(sparse_index, type, data, shape, dim_names);
}

Status ReadSparseTensorMetadata(const Buffer& metadata,
                                std::shared_ptr<DataType>* out_type,
                                std::vector<int64_t>* out_shape,
                                std::vector<std::string>* out_dim_names,
                                int64_t* out_non_zero_length,
                                SparseTensorFormat::type* out_format_id,
                                const flatbuf::SparseTensor** out_fb_sparse_tensor,
                                const flatbuf::Buffer** out_buffer) {
  RETURN_NOT_OK(internal::GetSparseTensorMetadata(
      metadata, out_type, out_shape, out_dim_names, out_non_zero_length, out_format_id));

  const flatbuf::Message* message;
  RETURN_NOT_OK(internal::VerifyMessage(metadata.data(), metadata.size(), &message));

  auto sparse_tensor = message->header_as_SparseTensor();
  if (sparse_tensor == nullptr) {
    return Status::IOError(
        "Header-type of flatbuffer-encoded Message is not SparseTensor.");
  }
  *out_fb_sparse_tensor = sparse_tensor;

  auto buffer = sparse_tensor->data();
  if (!BitUtil::IsMultipleOf8(buffer->offset())) {
    return Status::Invalid(
        "Buffer of sparse index data did not start on 8-byte aligned offset: ",
        buffer->offset());
  }
  *out_buffer = buffer;

  return Status::OK();
}

}  // namespace

namespace internal {

namespace {

Result<size_t> GetSparseTensorBodyBufferCount(SparseTensorFormat::type format_id) {
  switch (format_id) {
    case SparseTensorFormat::COO:
      return 2;

    case SparseTensorFormat::CSR:
      return 3;

    default:
      return Status::Invalid("Unrecognized sparse tensor format");
  }
}

Status CheckSparseTensorBodyBufferCount(
    const IpcPayload& payload, SparseTensorFormat::type sparse_tensor_format_id) {
  size_t expected_body_buffer_count = 0;
  ARROW_ASSIGN_OR_RAISE(expected_body_buffer_count,
                        GetSparseTensorBodyBufferCount(sparse_tensor_format_id));
  if (payload.body_buffers.size() != expected_body_buffer_count) {
    return Status::Invalid("Invalid body buffer count for a sparse tensor");
  }

  return Status::OK();
}

}  // namespace

Result<size_t> ReadSparseTensorBodyBufferCount(const Buffer& metadata) {
  SparseTensorFormat::type format_id;

  RETURN_NOT_OK(internal::GetSparseTensorMetadata(metadata, nullptr, nullptr, nullptr,
                                                  nullptr, &format_id));
  return GetSparseTensorBodyBufferCount(format_id);
}

Result<std::shared_ptr<SparseTensor>> ReadSparseTensorPayload(const IpcPayload& payload) {
  std::shared_ptr<DataType> type;
  std::vector<int64_t> shape;
  std::vector<std::string> dim_names;
  int64_t non_zero_length;
  SparseTensorFormat::type sparse_tensor_format_id;
  const flatbuf::SparseTensor* sparse_tensor;
  const flatbuf::Buffer* buffer;

  RETURN_NOT_OK(ReadSparseTensorMetadata(*payload.metadata, &type, &shape, &dim_names,
                                         &non_zero_length, &sparse_tensor_format_id,
                                         &sparse_tensor, &buffer));

  RETURN_NOT_OK(CheckSparseTensorBodyBufferCount(payload, sparse_tensor_format_id));

  switch (sparse_tensor_format_id) {
    case SparseTensorFormat::COO: {
      std::shared_ptr<SparseCOOIndex> sparse_index;
      std::shared_ptr<DataType> indices_type;
      RETURN_NOT_OK(internal::GetSparseCOOIndexMetadata(
          sparse_tensor->sparseIndex_as_SparseTensorIndexCOO(), &indices_type));
      ARROW_ASSIGN_OR_RAISE(sparse_index,
                            SparseCOOIndex::Make(indices_type, shape, non_zero_length,
                                                 payload.body_buffers[0]));
      return MakeSparseTensorWithSparseCOOIndex(type, shape, dim_names, sparse_index,
                                                non_zero_length, payload.body_buffers[1]);
    }
    case SparseTensorFormat::CSR: {
      std::shared_ptr<SparseCSRIndex> sparse_index;
      std::shared_ptr<DataType> indptr_type;
      std::shared_ptr<DataType> indices_type;
      RETURN_NOT_OK(internal::GetSparseCSXIndexMetadata(
          sparse_tensor->sparseIndex_as_SparseMatrixIndexCSX(), &indptr_type,
          &indices_type));
      ARROW_CHECK_EQ(indptr_type, indices_type);
      ARROW_ASSIGN_OR_RAISE(
          sparse_index,
          SparseCSRIndex::Make(indices_type, shape, non_zero_length,
                               payload.body_buffers[0], payload.body_buffers[1]));
      return MakeSparseTensorWithSparseCSRIndex(type, shape, dim_names, sparse_index,
                                                non_zero_length, payload.body_buffers[2]);
    }
    case SparseTensorFormat::CSC: {
      return Status::NotImplemented("TODO: CSC support");
    }
    default:
      return Status::Invalid("Unsupported sparse index format");
  }
}

}  // namespace internal

Result<std::shared_ptr<SparseTensor>> ReadSparseTensor(const Buffer& metadata,
                                                       io::RandomAccessFile* file) {
  std::shared_ptr<DataType> type;
  std::vector<int64_t> shape;
  std::vector<std::string> dim_names;
  int64_t non_zero_length;
  SparseTensorFormat::type sparse_tensor_format_id;
  const flatbuf::SparseTensor* sparse_tensor;
  const flatbuf::Buffer* buffer;

  RETURN_NOT_OK(ReadSparseTensorMetadata(metadata, &type, &shape, &dim_names,
                                         &non_zero_length, &sparse_tensor_format_id,
                                         &sparse_tensor, &buffer));

  ARROW_ASSIGN_OR_RAISE(auto data, file->ReadAt(buffer->offset(), buffer->length()));

  std::shared_ptr<SparseIndex> sparse_index;
  switch (sparse_tensor_format_id) {
    case SparseTensorFormat::COO: {
      ARROW_ASSIGN_OR_RAISE(
          sparse_index, ReadSparseCOOIndex(sparse_tensor, shape, non_zero_length, file));
      return MakeSparseTensorWithSparseCOOIndex(
          type, shape, dim_names, checked_pointer_cast<SparseCOOIndex>(sparse_index),
          non_zero_length, data);
    }
    case SparseTensorFormat::CSR: {
      ARROW_ASSIGN_OR_RAISE(
          sparse_index, ReadSparseCSXIndex(sparse_tensor, shape, non_zero_length, file));
      return MakeSparseTensorWithSparseCSRIndex(
          type, shape, dim_names, checked_pointer_cast<SparseCSRIndex>(sparse_index),
          non_zero_length, data);
    }
    case SparseTensorFormat::CSC: {
      ARROW_ASSIGN_OR_RAISE(
          sparse_index, ReadSparseCSXIndex(sparse_tensor, shape, non_zero_length, file));
      return MakeSparseTensorWithSparseCSCIndex(
          type, shape, dim_names, checked_pointer_cast<SparseCSCIndex>(sparse_index),
          non_zero_length, data);
    }
    default:
      return Status::Invalid("Unsupported sparse index format");
  }
}

Result<std::shared_ptr<SparseTensor>> ReadSparseTensor(const Message& message) {
  CHECK_HAS_BODY(message);
  io::BufferReader buffer_reader(message.body());
  return ReadSparseTensor(*message.metadata(), &buffer_reader);
}

Result<std::shared_ptr<SparseTensor>> ReadSparseTensor(io::InputStream* file) {
  std::unique_ptr<Message> message;
  RETURN_NOT_OK(ReadContiguousPayload(file, &message));
  CHECK_MESSAGE_TYPE(Message::SPARSE_TENSOR, message->type());
  CHECK_HAS_BODY(*message);
  io::BufferReader buffer_reader(message->body());
  return ReadSparseTensor(*message->metadata(), &buffer_reader);
}

///////////////////////////////////////////////////////////////////////////
// Helpers for fuzzing

namespace internal {

Status FuzzIpcStream(const uint8_t* data, int64_t size) {
  auto buffer = std::make_shared<Buffer>(data, size);
  io::BufferReader buffer_reader(buffer);

  std::shared_ptr<RecordBatchReader> batch_reader;
  RETURN_NOT_OK(RecordBatchStreamReader::Open(&buffer_reader, &batch_reader));

  while (true) {
    std::shared_ptr<arrow::RecordBatch> batch;
    RETURN_NOT_OK(batch_reader->ReadNext(&batch));
    if (batch == nullptr) {
      break;
    }
    RETURN_NOT_OK(batch->ValidateFull());
  }

  return Status::OK();
}

Status FuzzIpcFile(const uint8_t* data, int64_t size) {
  auto buffer = std::make_shared<Buffer>(data, size);
  io::BufferReader buffer_reader(buffer);

  std::shared_ptr<RecordBatchFileReader> batch_reader;
  RETURN_NOT_OK(RecordBatchFileReader::Open(&buffer_reader, &batch_reader));

  const int n_batches = batch_reader->num_record_batches();
  for (int i = 0; i < n_batches; ++i) {
    std::shared_ptr<arrow::RecordBatch> batch;
    RETURN_NOT_OK(batch_reader->ReadRecordBatch(i, &batch));
    RETURN_NOT_OK(batch->ValidateFull());
  }

  return Status::OK();
}

}  // namespace internal
}  // namespace ipc
}  // namespace arrow
