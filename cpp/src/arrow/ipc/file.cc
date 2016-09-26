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

#include "arrow/io/interfaces.h"
#include "arrow/ipc/adapter.h"
#include "arrow/ipc/metadata.h"
#include "arrow/ipc/util.h"
#include "arrow/util/buffer.h"
#include "arrow/util/logging.h"
#include "arrow/util/status.h"

namespace arrow {
namespace ipc {

static constexpr const char* kArrowMagicBytes = "ARROW1";

// ----------------------------------------------------------------------
// Writer implementation

FileWriter::FileWriter(io::OutputStream* sink, const std::shared_ptr<Schema>& schema)
    : sink_(sink), schema_(schema), position_(-1), started_(false) {}

Status FileWriter::UpdatePosition() {
  return sink_->Tell(&position_);
}

Status FileWriter::Open(io::OutputStream* sink, const std::shared_ptr<Schema>& schema,
    std::shared_ptr<FileWriter>* out) {
  *out = std::shared_ptr<FileWriter>(new FileWriter(sink, schema));  // ctor is private
  RETURN_NOT_OK((*out)->UpdatePosition());
  return Status::OK();
}

Status FileWriter::Write(const uint8_t* data, int64_t nbytes) {
  RETURN_NOT_OK(sink_->Write(data, nbytes));
  position_ += nbytes;
  return Status::OK();
}

Status FileWriter::Align() {
  int64_t remainder = PaddedLength(position_) - position_;
  if (remainder > 0) { return Write(kPaddingBytes, remainder); }
  return Status::OK();
}

Status FileWriter::WriteAligned(const uint8_t* data, int64_t nbytes) {
  RETURN_NOT_OK(Write(data, nbytes));
  return Align();
}

Status FileWriter::Start() {
  RETURN_NOT_OK(WriteAligned(
      reinterpret_cast<const uint8_t*>(kArrowMagicBytes), strlen(kArrowMagicBytes)));
  started_ = true;
  return Status::OK();
}

Status FileWriter::CheckStarted() {
  if (!started_) { return Start(); }
  return Status::OK();
}

Status FileWriter::WriteRecordBatch(
    const std::vector<std::shared_ptr<Array>>& columns, int32_t num_rows) {
  RETURN_NOT_OK(CheckStarted());

  int64_t offset = position_;

  int64_t body_end_offset;
  int64_t header_end_offset;
  RETURN_NOT_OK(arrow::ipc::WriteRecordBatch(
      columns, num_rows, sink_, &body_end_offset, &header_end_offset));
  RETURN_NOT_OK(UpdatePosition());

  DCHECK(position_ % 8 == 0) << "ipc::WriteRecordBatch did not perform aligned writes";

  // There may be padding ever the end of the metadata, so we cannot rely on
  // position_
  int32_t metadata_length = header_end_offset - body_end_offset;
  int32_t body_length = body_end_offset - offset;

  // Append metadata, to be written in the footer later
  record_batches_.emplace_back(offset, metadata_length, body_length);

  return Status::OK();
}

Status FileWriter::Close() {
  // Write metadata
  int64_t initial_position = position_;
  RETURN_NOT_OK(WriteFileFooter(schema_.get(), dictionaries_, record_batches_, sink_));
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

FileReader::FileReader(
    const std::shared_ptr<io::ReadableFileInterface>& file, int64_t footer_offset)
    : file_(file), footer_offset_(footer_offset) {}

FileReader::~FileReader() {}

Status FileReader::Open(const std::shared_ptr<io::ReadableFileInterface>& file,
    std::shared_ptr<FileReader>* reader) {
  int64_t footer_offset;
  RETURN_NOT_OK(file->GetSize(&footer_offset));
  return Open(file, footer_offset, reader);
}

Status FileReader::Open(const std::shared_ptr<io::ReadableFileInterface>& file,
    int64_t footer_offset, std::shared_ptr<FileReader>* reader) {
  *reader = std::shared_ptr<FileReader>(new FileReader(file, footer_offset));
  return (*reader)->ReadFooter();
}

Status FileReader::ReadFooter() {
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
      footer_offset_ - footer_length - file_end_size, footer_length, &buffer));
  RETURN_NOT_OK(FileFooter::Open(buffer, &footer_));

  // Get the schema
  return footer_->GetSchema(&schema_);
}

const std::shared_ptr<Schema>& FileReader::schema() const {
  return schema_;
}

int FileReader::num_dictionaries() const {
  return footer_->num_dictionaries();
}

int FileReader::num_record_batches() const {
  return footer_->num_record_batches();
}

MetadataVersion::type FileReader::version() const {
  return footer_->version();
}

Status FileReader::GetRecordBatch(int i, std::shared_ptr<RecordBatch>* batch) {
  DCHECK_GE(i, 0);
  DCHECK_LT(i, num_record_batches());
  FileBlock block = footer_->record_batch(i);
  int64_t metadata_end_offset = block.offset + block.body_length + block.metadata_length;

  std::shared_ptr<RecordBatchReader> reader;
  RETURN_NOT_OK(RecordBatchReader::Open(file_.get(), metadata_end_offset, &reader));

  return reader->GetRecordBatch(schema_, batch);
}

}  // namespace ipc
}  // namespace arrow
