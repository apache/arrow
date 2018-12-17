// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// A Status encapsulates the result of an operation.  It may indicate success,
// or it may indicate an error with an associated error message.
//
// Multiple threads can invoke const methods on a Status without
// external synchronization, but if any of the threads may call a
// non-const method, all threads accessing the same Status must use
// external synchronization.

#include "arrow/status.h"

#include <algorithm>
#include <cstring>
#include <sstream>

#include "arrow/util/logging.h"

namespace arrow {

uint64_t StatusRange::RangeIdFromName(const char* name) {
  uint64_t n = std::min<uint64_t>(8U, strlen(name));
  uint64_t range_id = 0;
  for (uint64_t i = 0; i < n; ++i) {
    range_id = (range_id << 8) | static_cast<uint8_t>(name[i]);
  }
  return range_id;
}

StatusRange::StatusRange(const char* name, uint64_t range_id, int32_t num_codes,
                         const char** messages)
  : name_(name), range_id_(range_id), num_codes_(num_codes), messages_(messages)
{
  Status::RegisterStatusRange(this);
  // XXX should unregister in destructor?  Could be safer if non-trivial code
  // is executed at program end...
}

StatusRange::StatusRange(const char* name, int32_t num_codes, const char** messages)
  : StatusRange(name, RangeIdFromName(name), num_codes, messages)
{
}

std::string StatusRange::CodeAsString(int32_t code) const {
  DCHECK_GT(code, 0) << "error code should be > 0";
  DCHECK_LE(code, num_codes_) << "error code should be <= num_codes";
  return messages_[code - 1];
}

std::unordered_map<uint64_t, const StatusRange*> Status::ranges_;

void Status::RegisterStatusRange(const StatusRange* status_range) {
  uint64_t range_id = status_range->range_id();
  DCHECK_EQ(range_id >> 56, 0) << "Upper byte of status range should be == 0";
  auto p = ranges_.insert(std::make_pair(range_id, status_range));
  DCHECK_EQ(p.second, true)
      << "Duplicate range id " << range_id << " for range '"
      << status_range->name() << "'";
}

Status::Status(StatusCode code, const std::string& msg) {
  DCHECK_NE(code, StatusCode::OK);
  state_ = new State{0U, static_cast<int32_t>(code), msg};
}

Status::Status(uint64_t range_id, int32_t code, const std::string& msg) {
  DCHECK_GT(code, 0);
  state_ = new State{range_id, code, msg};
}

void Status::CopyFrom(const Status& s) {
  delete state_;
  if (s.state_ == nullptr) {
    state_ = nullptr;
  } else {
    state_ = new State(*s.state_);
  }
}

const StatusRange* Status::status_range() const {
  if (state_ == nullptr) {
    return nullptr;
  }
  auto range_id = state_->range_id;
  if (range_id != 0) {
    auto it = ranges_.find(range_id);
    if (it != ranges_.end()) {
      return it->second;
    }
  }
  return nullptr;
}

std::string Status::CodeAsString() const {
  if (state_ == nullptr) {
    return "OK";
  }

  if (state_->range_id != 0) {
    // Delegate to supplementary status range
    auto range = status_range();
    if (range != nullptr) {
      std::stringstream ss;
      ss << "[" << range->name() << "] ";
      ss << range->CodeAsString(state_->code);
      return ss.str();
    } else {
      std::stringstream ss;
      ss << "Error " << state_->code << " in unregistered status range " << state_->range_id;
      return ss.str();
    }
  }

  const char* type;
  switch (code()) {
    case StatusCode::OK:
      type = "OK";
      break;
    case StatusCode::OutOfMemory:
      type = "Out of memory";
      break;
    case StatusCode::KeyError:
      type = "Key error";
      break;
    case StatusCode::TypeError:
      type = "Type error";
      break;
    case StatusCode::Invalid:
      type = "Invalid";
      break;
    case StatusCode::IOError:
      type = "IOError";
      break;
    case StatusCode::CapacityError:
      type = "Capacity error";
      break;
    case StatusCode::UnknownError:
      type = "Unknown error";
      break;
    case StatusCode::NotImplemented:
      type = "NotImplemented";
      break;
    case StatusCode::SerializationError:
      type = "Serialization error";
      break;
    case StatusCode::PythonError:
      type = "Python error";
      break;
    case StatusCode::PlasmaObjectExists:
      type = "Plasma object exists";
      break;
    case StatusCode::PlasmaObjectNonexistent:
      type = "Plasma object is nonexistent";
      break;
    case StatusCode::PlasmaStoreFull:
      type = "Plasma store is full";
      break;
    case StatusCode::PlasmaObjectAlreadySealed:
      type = "Plasma object is already sealed";
      break;
    case StatusCode::CodeGenError:
      type = "CodeGenError in Gandiva";
      break;
    case StatusCode::ExpressionValidationError:
      type = "ExpressionValidationError";
      break;
    case StatusCode::ExecutionError:
      type = "ExecutionError in Gandiva";
      break;
    default:
      type = "Unknown";
      break;
  }
  return std::string(type);
}

std::string Status::ToString() const {
  std::string result(CodeAsString());
  if (state_ == NULL) {
    return result;
  }
  result += ": ";
  result += state_->msg;
  return result;
}

}  // namespace arrow
