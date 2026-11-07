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
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <algorithm>
#include <cstring>
#include <iostream>
#include <limits>
#include <memory>
#include <streambuf>

#include "arrow/status.h"

namespace arrow {
namespace fs {
namespace internal {

// The SDK writes error bodies through the GetObject response stream too. Keep the
// caller's buffer as the put area for normal reads; capture a bounded prefix if
// an error body exceeds the requested range.
class GetObjectRangeStreamBuf : public std::streambuf {
 public:
  static constexpr size_t kSpillSize = 64 * 1024;

  GetObjectRangeStreamBuf(void* data, size_t size) : data_(static_cast<char*>(data)) {
    setp(data_, data_ + size);
    setg(data_, data_, data_);
  }

  bool overflowed() const { return overflowed_; }

 protected:
  std::streamsize xsputn(const char* source, std::streamsize count) override {
    if (count <= 0) {
      return 0;
    }
    auto remaining = static_cast<size_t>(count);
    while (remaining != 0) {
      if (pptr() == epptr() && !spill_) {
        Spill();
      }
      const auto n = std::min(remaining, static_cast<size_t>(epptr() - pptr()));
      if (n == 0) {
        break;
      }
      std::memcpy(pptr(), source, n);
      Advance(n);
      source += n;
      remaining -= n;
    }
    // A short write makes the SDK lose the HTTP error and its retry decision.
    return count;
  }

  int_type overflow(int_type ch) override {
    if (traits_type::eq_int_type(ch, traits_type::eof())) {
      return traits_type::not_eof(ch);
    }
    const char c = traits_type::to_char_type(ch);
    xsputn(&c, 1);
    return ch;
  }

  int_type underflow() override {
    if (gptr() == egptr()) {
      const auto pos = static_cast<size_t>(gptr() - eback());
      setg(Storage(), Storage() + pos, Storage() + Written());
    }
    return gptr() == egptr() ? traits_type::eof() : traits_type::to_int_type(*gptr());
  }

  pos_type seekoff(off_type offset, std::ios_base::seekdir dir,
                   std::ios_base::openmode which) override {
    if (which == std::ios_base::out) {
      return dir == std::ios_base::cur && offset == 0
                 ? pos_type(static_cast<off_type>(Written()))
                 : pos_type(off_type(-1));
    }
    if (which != std::ios_base::in) {
      return pos_type(off_type(-1));
    }
    const auto size = static_cast<off_type>(Written());
    const auto base = dir == std::ios_base::beg ? off_type(0)
                      : dir == std::ios_base::cur
                          ? static_cast<off_type>(gptr() - eback())
                          : size;
    if (dir != std::ios_base::beg && dir != std::ios_base::cur &&
        dir != std::ios_base::end) {
      return pos_type(off_type(-1));
    }
    if (offset < -base || offset > size - base) {
      return pos_type(off_type(-1));
    }
    const auto pos = base + offset;
    if (pos < 0 || pos > size) {
      return pos_type(off_type(-1));
    }
    setg(Storage(), Storage() + pos, Storage() + size);
    return pos_type(pos);
  }

  pos_type seekpos(pos_type pos, std::ios_base::openmode which) override {
    return seekoff(static_cast<off_type>(pos), std::ios_base::beg, which);
  }

  int sync() override { return 0; }

 private:
  char* Storage() const { return spill_ ? spill_.get() : data_; }
  size_t Written() const { return static_cast<size_t>(pptr() - Storage()); }

  void Advance(size_t n) {
    while (n != 0) {
      const auto step = static_cast<int>(
          std::min(n, static_cast<size_t>(std::numeric_limits<int>::max())));
      pbump(step);
      n -= step;
    }
  }

  void Spill() {
    overflowed_ = true;
    const auto kept = std::min(Written(), kSpillSize);
    const auto read_pos = std::min(static_cast<size_t>(gptr() - eback()), kept);
    auto spill = std::make_unique<char[]>(kSpillSize);
    if (kept != 0) {
      std::memcpy(spill.get(), data_, kept);
    }
    spill_ = std::move(spill);
    setp(spill_.get(), spill_.get() + kSpillSize);
    Advance(kept);
    setg(spill_.get(), spill_.get() + read_pos, spill_.get() + kept);
  }

  char* data_;
  std::unique_ptr<char[]> spill_;
  bool overflowed_ = false;
};

class GetObjectRangeStream : private GetObjectRangeStreamBuf, public std::iostream {
 public:
  GetObjectRangeStream(void* data, size_t size)
      : GetObjectRangeStreamBuf(data, size), std::iostream(this) {}

  Status CheckForOverflow() const {
    if (overflowed()) {
      return Status::IOError("GetObject response exceeded the requested range");
    }
    return Status::OK();
  }
};

}  // namespace internal
}  // namespace fs
}  // namespace arrow
