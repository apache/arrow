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

#include "arrow/ipc/stream.h"

#include <cstdint>
#include <cstring>
#include <sstream>
#include <vector>

#include "arrow/buffer.h"
#include "arrow/io/interfaces.h"
#include "arrow/io/memory.h"
#include "arrow/ipc/adapter.h"
#include "arrow/ipc/metadata.h"
#include "arrow/ipc/util.h"
#include "arrow/status.h"
#include "arrow/util/logging.h"

namespace arrow {
namespace ipc {

// ----------------------------------------------------------------------
// Implement base stream writer functions

BaseStreamWriter::~BaseStreamWriter() {}

BaseStreamWriter::BaseStreamWriter(
    io::OutputStream* sink, const std::shared_ptr<Schema>& schema)
    : sink_(sink), schema_(schema), position_(-1), started_(false) {}

Status BaseStreamWriter::UpdatePosition() {
  return sink_->Tell(&position_);
}

Status BaseStreamWriter::Write(const uint8_t* data, int64_t nbytes) {
  RETURN_NOT_OK(sink_->Write(data, nbytes));
  position_ += nbytes;
  return Status::OK();
}

Status BaseStreamWriter::Align() {
  int64_t remainder = PaddedLength(position_) - position_;
  if (remainder > 0) { return Write(kPaddingBytes, remainder); }
  return Status::OK();
}

Status BaseStreamWriter::WriteAligned(const uint8_t* data, int64_t nbytes) {
  RETURN_NOT_OK(Write(data, nbytes));
  return Align();
}

Status BaseStreamWriter::CheckStarted() {
  if (!started_) { return Start(); }
  return Status::OK();
}

Status BaseStreamWriter::WriteRecordBatch(const RecordBatch& batch, FileBlock* block) {
  RETURN_NOT_OK(CheckStarted());

  block->offset = position_;

  // Frame of reference in file format is 0, see ARROW-384
  const int64_t buffer_start_offset = 0;
  RETURN_NOT_OK(arrow::ipc::WriteRecordBatch(
      batch, buffer_start_offset, sink_, &block->metadata_length, &block->body_length));
  RETURN_NOT_OK(UpdatePosition());

  DCHECK(position_ % 8 == 0) << "WriteRecordBatch did not perform aligned writes";

  return Status::OK();
}

// ----------------------------------------------------------------------
// StreamWriter implementation

Status StreamWriter::Start() {
  std::shared_ptr<Buffer> schema_fb;
  RETURN_NOT_OK(WriteSchema(*schema_, &schema_fb));

  int32_t flatbuffer_size = schema_fb->size();
  RETURN_NOT_OK(
      Write(reinterpret_cast<const uint8_t*>(&flatbuffer_size), sizeof(int32_t)));

  // Write the flatbuffer
  RETURN_NOT_OK(Write(schema_fb->data(), flatbuffer_size));
  started_ = true;
  return Status::OK();
}

Status StreamWriter::WriteRecordBatch(const RecordBatch& batch) {
  // Pass FileBlock, but results not used
  FileBlock dummy_block;
  return BaseStreamWriter::WriteRecordBatch(batch, &dummy_block);
}

Status StreamWriter::Close() {
  // Close the stream
  RETURN_NOT_OK(CheckStarted());
  return sink_->Close();
}

// ----------------------------------------------------------------------
// Base stream reader functions

BaseStreamReader::~BaseStreamReader() {}

}  // namespace ipc
}  // namespace arrow
