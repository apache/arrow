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

#include "arrow/ipc/writer.h"

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
#include "arrow/memory_pool.h"
#include "arrow/schema.h"
#include "arrow/status.h"
#include "arrow/table.h"
#include "arrow/util/logging.h"

namespace arrow {
namespace ipc {

// ----------------------------------------------------------------------
// Stream writer implementation

class StreamWriter::StreamWriterImpl {
 public:
  StreamWriterImpl()
      : dictionary_memo_(std::make_shared<DictionaryMemo>()),
        pool_(default_memory_pool()),
        position_(-1),
        started_(false) {}

  virtual ~StreamWriterImpl() = default;

  Status Open(io::OutputStream* sink, const std::shared_ptr<Schema>& schema) {
    sink_ = sink;
    schema_ = schema;
    return UpdatePosition();
  }

  virtual Status Start() {
    std::shared_ptr<Buffer> schema_fb;
    RETURN_NOT_OK(WriteSchemaMessage(*schema_, dictionary_memo_.get(), &schema_fb));

    int32_t flatbuffer_size = schema_fb->size();
    RETURN_NOT_OK(
        Write(reinterpret_cast<const uint8_t*>(&flatbuffer_size), sizeof(int32_t)));

    // Write the flatbuffer
    RETURN_NOT_OK(Write(schema_fb->data(), flatbuffer_size));

    // If there are any dictionaries, write them as the next messages
    RETURN_NOT_OK(WriteDictionaries());

    started_ = true;
    return Status::OK();
  }

  virtual Status Close() {
    // Write the schema if not already written
    // User is responsible for closing the OutputStream
    return CheckStarted();
  }

  Status CheckStarted() {
    if (!started_) { return Start(); }
    return Status::OK();
  }

  Status UpdatePosition() { return sink_->Tell(&position_); }

  Status WriteDictionaries() {
    const DictionaryMap& id_to_dictionary = dictionary_memo_->id_to_dictionary();

    dictionaries_.resize(id_to_dictionary.size());

    // TODO(wesm): does sorting by id yield any benefit?
    int dict_index = 0;
    for (const auto& entry : id_to_dictionary) {
      FileBlock* block = &dictionaries_[dict_index++];

      block->offset = position_;

      // Frame of reference in file format is 0, see ARROW-384
      const int64_t buffer_start_offset = 0;
      RETURN_NOT_OK(WriteDictionary(entry.first, entry.second, buffer_start_offset, sink_,
          &block->metadata_length, &block->body_length, pool_));
      RETURN_NOT_OK(UpdatePosition());
      DCHECK(position_ % 8 == 0) << "WriteDictionary did not perform aligned writes";
    }

    return Status::OK();
  }

  Status WriteRecordBatch(const RecordBatch& batch, FileBlock* block) {
    RETURN_NOT_OK(CheckStarted());

    block->offset = position_;

    // Frame of reference in file format is 0, see ARROW-384
    const int64_t buffer_start_offset = 0;
    RETURN_NOT_OK(arrow::ipc::WriteRecordBatch(batch, buffer_start_offset, sink_,
        &block->metadata_length, &block->body_length, pool_));
    RETURN_NOT_OK(UpdatePosition());

    DCHECK(position_ % 8 == 0) << "WriteRecordBatch did not perform aligned writes";

    return Status::OK();
  }

  Status WriteRecordBatch(const RecordBatch& batch) {
    // Push an empty FileBlock. Can be written in the footer later
    record_batches_.emplace_back(0, 0, 0);
    return WriteRecordBatch(batch, &record_batches_[record_batches_.size() - 1]);
  }

  // Adds padding bytes if necessary to ensure all memory blocks are written on
  // 8-byte boundaries.
  Status Align() {
    int64_t remainder = PaddedLength(position_) - position_;
    if (remainder > 0) { return Write(kPaddingBytes, remainder); }
    return Status::OK();
  }

  // Write data and update position
  Status Write(const uint8_t* data, int64_t nbytes) {
    RETURN_NOT_OK(sink_->Write(data, nbytes));
    position_ += nbytes;
    return Status::OK();
  }

  // Write and align
  Status WriteAligned(const uint8_t* data, int64_t nbytes) {
    RETURN_NOT_OK(Write(data, nbytes));
    return Align();
  }

  void set_memory_pool(MemoryPool* pool) { pool_ = pool; }

 protected:
  io::OutputStream* sink_;
  std::shared_ptr<Schema> schema_;

  // When writing out the schema, we keep track of all the dictionaries we
  // encounter, as they must be written out first in the stream
  std::shared_ptr<DictionaryMemo> dictionary_memo_;

  MemoryPool* pool_;

  int64_t position_;
  bool started_;

  std::vector<FileBlock> dictionaries_;
  std::vector<FileBlock> record_batches_;
};

StreamWriter::StreamWriter() {
  impl_.reset(new StreamWriterImpl());
}

Status StreamWriter::WriteRecordBatch(const RecordBatch& batch) {
  return impl_->WriteRecordBatch(batch);
}

void StreamWriter::set_memory_pool(MemoryPool* pool) {
  impl_->set_memory_pool(pool);
}

Status StreamWriter::Open(io::OutputStream* sink, const std::shared_ptr<Schema>& schema,
    std::shared_ptr<StreamWriter>* out) {
  // ctor is private
  *out = std::shared_ptr<StreamWriter>(new StreamWriter());
  return (*out)->impl_->Open(sink, schema);
}

Status StreamWriter::Close() {
  return impl_->Close();
}

// ----------------------------------------------------------------------
// File writer implementation

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

class FileWriter::FileWriterImpl : public StreamWriter::StreamWriterImpl {
 public:
  using BASE = StreamWriter::StreamWriterImpl;

  Status Start() override {
    RETURN_NOT_OK(WriteAligned(
        reinterpret_cast<const uint8_t*>(kArrowMagicBytes), strlen(kArrowMagicBytes)));

    // We write the schema at the start of the file (and the end). This also
    // writes all the dictionaries at the beginning of the file
    return BASE::Start();
  }

  Status Close() override {
    // Write metadata
    int64_t initial_position = position_;
    RETURN_NOT_OK(WriteFileFooter(
        *schema_, dictionaries_, record_batches_, dictionary_memo_.get(), sink_));
    RETURN_NOT_OK(UpdatePosition());

    // Write footer length
    int32_t footer_length = position_ - initial_position;

    if (footer_length <= 0) { return Status::Invalid("Invalid file footer"); }

    RETURN_NOT_OK(
        Write(reinterpret_cast<const uint8_t*>(&footer_length), sizeof(int32_t)));

    // Write magic bytes to end file
    return Write(
        reinterpret_cast<const uint8_t*>(kArrowMagicBytes), strlen(kArrowMagicBytes));
  }
};

FileWriter::FileWriter() {
  impl_.reset(new FileWriterImpl());
}

Status FileWriter::Open(io::OutputStream* sink, const std::shared_ptr<Schema>& schema,
    std::shared_ptr<FileWriter>* out) {
  *out = std::shared_ptr<FileWriter>(new FileWriter());  // ctor is private
  return (*out)->impl_->Open(sink, schema);
}

Status FileWriter::WriteRecordBatch(const RecordBatch& batch) {
  return impl_->WriteRecordBatch(batch);
}

Status FileWriter::Close() {
  return impl_->Close();
}

}  // namespace ipc
}  // namespace arrow
