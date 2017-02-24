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

#include "arrow/ipc/file.h"

#include <cstdint>
#include <cstring>
#include <sstream>
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

static constexpr const char* kArrowMagicBytes = "ARROW1";

static flatbuffers::Offset<flatbuffers::Vector<const flatbuf::Block*>>
FileBlocksToFlatbuffer(FBB& fbb, const std::vector<FileBlock>& blocks) {
  std::vector<flatbuf::Block> fb_blocks;

  for (const FileBlock& block : blocks) {
    fb_blocks.emplace_back(block.offset, block.metadata_length, block.body_length);
  }

  return fbb.CreateVectorOfStructs(fb_blocks);
}

Status WriteFileFooter(const Schema& schema, const std::vector<FileBlock>& dictionaries,
    const std::vector<FileBlock>& record_batches, DictionaryMemo* dictionary_memo,
    io::OutputStream* out) {
  FBB fbb;

  flatbuffers::Offset<flatbuf::Schema> fb_schema;
  RETURN_NOT_OK(SchemaToFlatbuffer(fbb, schema, dictionary_memo, &fb_schema));

  auto fb_dictionaries = FileBlocksToFlatbuffer(fbb, dictionaries);
  auto fb_record_batches = FileBlocksToFlatbuffer(fbb, record_batches);

  auto footer = flatbuf::CreateFooter(
      fbb, kMetadataVersion, fb_schema, fb_dictionaries, fb_record_batches);

  fbb.Finish(footer);

  int32_t size = fbb.GetSize();

  return out->Write(fbb.GetBufferPointer(), size);
}

static inline FileBlock FileBlockFromFlatbuffer(const flatbuf::Block* block) {
  return FileBlock(block->offset(), block->metaDataLength(), block->bodyLength());
}

// ----------------------------------------------------------------------
// File writer implementation

FileWriter::FileWriter(io::OutputStream* sink, const std::shared_ptr<Schema>& schema)
    : StreamWriter(sink, schema) {}

Status FileWriter::Open(io::OutputStream* sink, const std::shared_ptr<Schema>& schema,
    std::shared_ptr<FileWriter>* out) {
  *out = std::shared_ptr<FileWriter>(new FileWriter(sink, schema));  // ctor is private
  RETURN_NOT_OK((*out)->UpdatePosition());
  return Status::OK();
}

Status FileWriter::Start() {
  RETURN_NOT_OK(WriteAligned(
      reinterpret_cast<const uint8_t*>(kArrowMagicBytes), strlen(kArrowMagicBytes)));

  // We write the schema at the start of the file (and the end). This also
  // writes all the dictionaries at the beginning of the file
  return StreamWriter::Start();
}

Status FileWriter::Close() {
  // Write metadata
  int64_t initial_position = position_;
  RETURN_NOT_OK(WriteFileFooter(
      *schema_, dictionaries_, record_batches_, dictionary_memo_.get(), sink_));
  RETURN_NOT_OK(UpdatePosition());

  // Write footer length
  int32_t footer_length = position_ - initial_position;

  if (footer_length <= 0) { return Status::Invalid("Invalid file footer"); }

  RETURN_NOT_OK(Write(reinterpret_cast<const uint8_t*>(&footer_length), sizeof(int32_t)));

  // Write magic bytes to end file
  return Write(
      reinterpret_cast<const uint8_t*>(kArrowMagicBytes), strlen(kArrowMagicBytes));
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
    int file_end_size = magic_size + sizeof(int32_t);
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
      const std::shared_ptr<io::ReadableFileInterface>& file, int64_t footer_offset) {
    file_ = file;
    footer_offset_ = footer_offset;
    RETURN_NOT_OK(ReadFooter());
    return ReadSchema();
  }

  std::shared_ptr<Schema> schema() const { return schema_; }

 private:
  std::shared_ptr<io::ReadableFileInterface> file_;

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

Status FileReader::Open(const std::shared_ptr<io::ReadableFileInterface>& file,
    std::shared_ptr<FileReader>* reader) {
  int64_t footer_offset;
  RETURN_NOT_OK(file->GetSize(&footer_offset));
  return Open(file, footer_offset, reader);
}

Status FileReader::Open(const std::shared_ptr<io::ReadableFileInterface>& file,
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
