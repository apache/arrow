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

#include "arrow/ipc/metadata.h"
#include "arrow/io/interfaces.h"
#include "arrow/util/buffer.h"
#include "arrow/util/logging.h"
#include "arrow/util/status.h"

namespace arrow {
namespace ipc {

static constexpr const char* kArrowMagicBytes = "ARROW1";

// Align on 8-byte boundaries
static constexpr int kArrowAlignment = 8;
static constexpr uint8_t kPaddingBytes[kArrowAlignment] = {0};

static inline int64_t PaddedLength(int64_t nbytes) {
  return ((nbytes + kArrowAlignment - 1) / kArrowAlignment) * kArrowAlignment;
}

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
  RETURN_NOT_OK(sink_->Write(data, nbytes));
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

Status FileWriter::WriteRecordBatch(
    const std::vector<std::shared_ptr<Array>>& columns, int32_t num_rows) {
  return Status::OK();
}

// ----------------------------------------------------------------------
// Reader implementation

}  // namespace ipc
}  // namespace arrow
