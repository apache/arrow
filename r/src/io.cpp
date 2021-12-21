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

#include "./arrow_types.h"

#if defined(ARROW_R_WITH_ARROW)
#include <arrow/io/transform.h>
#include <arrow/io/file.h>
#include <arrow/io/memory.h>
#include <R_ext/Riconv.h>

// ------ arrow::io::Readable

// [[arrow::export]]
std::shared_ptr<arrow::Buffer> io___Readable__Read(
    const std::shared_ptr<arrow::io::Readable>& x, int64_t nbytes) {
  return ValueOrStop(x->Read(nbytes));
}

// ------ arrow::io::InputStream

// [[arrow::export]]
void io___InputStream__Close(const std::shared_ptr<arrow::io::InputStream>& x) {
  StopIfNotOk(x->Close());
}

// ------ arrow::io::OutputStream

// [[arrow::export]]
void io___OutputStream__Close(const std::shared_ptr<arrow::io::OutputStream>& x) {
  StopIfNotOk(x->Close());
}

// ------ arrow::io::RandomAccessFile

// [[arrow::export]]
int64_t io___RandomAccessFile__GetSize(
    const std::shared_ptr<arrow::io::RandomAccessFile>& x) {
  return ValueOrStop(x->GetSize());
}

// [[arrow::export]]
bool io___RandomAccessFile__supports_zero_copy(
    const std::shared_ptr<arrow::io::RandomAccessFile>& x) {
  return x->supports_zero_copy();
}

// [[arrow::export]]
void io___RandomAccessFile__Seek(const std::shared_ptr<arrow::io::RandomAccessFile>& x,
                                 int64_t position) {
  StopIfNotOk(x->Seek(position));
}

// [[arrow::export]]
int64_t io___RandomAccessFile__Tell(
    const std::shared_ptr<arrow::io::RandomAccessFile>& x) {
  return ValueOrStop(x->Tell());
}

// [[arrow::export]]
std::shared_ptr<arrow::Buffer> io___RandomAccessFile__Read0(
    const std::shared_ptr<arrow::io::RandomAccessFile>& x) {
  int64_t current = ValueOrStop(x->Tell());

  int64_t n = ValueOrStop(x->GetSize());

  return ValueOrStop(x->Read(n - current));
}

// [[arrow::export]]
std::shared_ptr<arrow::Buffer> io___RandomAccessFile__ReadAt(
    const std::shared_ptr<arrow::io::RandomAccessFile>& x, int64_t position,
    int64_t nbytes) {
  return ValueOrStop(x->ReadAt(position, nbytes));
}

// ------ arrow::io::MemoryMappedFile

// [[arrow::export]]
std::shared_ptr<arrow::io::MemoryMappedFile> io___MemoryMappedFile__Create(
    const std::string& path, int64_t size) {
  return ValueOrStop(arrow::io::MemoryMappedFile::Create(path, size));
}

// [[arrow::export]]
std::shared_ptr<arrow::io::MemoryMappedFile> io___MemoryMappedFile__Open(
    const std::string& path, arrow::io::FileMode::type mode) {
  return ValueOrStop(arrow::io::MemoryMappedFile::Open(path, mode));
}

// [[arrow::export]]
void io___MemoryMappedFile__Resize(const std::shared_ptr<arrow::io::MemoryMappedFile>& x,
                                   int64_t size) {
  StopIfNotOk(x->Resize(size));
}

// ------ arrow::io::ReadableFile

// [[arrow::export]]
std::shared_ptr<arrow::io::ReadableFile> io___ReadableFile__Open(
    const std::string& path) {
  return ValueOrStop(arrow::io::ReadableFile::Open(path, gc_memory_pool()));
}

// ------ arrow::io::BufferReader

// [[arrow::export]]
std::shared_ptr<arrow::io::BufferReader> io___BufferReader__initialize(
    const std::shared_ptr<arrow::Buffer>& buffer) {
  return std::make_shared<arrow::io::BufferReader>(buffer);
}

// ------- arrow::io::Writable

// [[arrow::export]]
void io___Writable__write(const std::shared_ptr<arrow::io::Writable>& stream,
                          const std::shared_ptr<arrow::Buffer>& buf) {
  StopIfNotOk(stream->Write(buf->data(), buf->size()));
}

// ------- arrow::io::OutputStream

// [[arrow::export]]
int64_t io___OutputStream__Tell(const std::shared_ptr<arrow::io::OutputStream>& stream) {
  return ValueOrStop(stream->Tell());
}

// ------ arrow::io::FileOutputStream

// [[arrow::export]]
std::shared_ptr<arrow::io::FileOutputStream> io___FileOutputStream__Open(
    const std::string& path) {
  return ValueOrStop(arrow::io::FileOutputStream::Open(path));
}

// ------ arrow::BufferOutputStream

// [[arrow::export]]
std::shared_ptr<arrow::io::BufferOutputStream> io___BufferOutputStream__Create(
    int64_t initial_capacity) {
  return ValueOrStop(
      arrow::io::BufferOutputStream::Create(initial_capacity, gc_memory_pool()));
}

// [[arrow::export]]
int64_t io___BufferOutputStream__capacity(
    const std::shared_ptr<arrow::io::BufferOutputStream>& stream) {
  return stream->capacity();
}

// [[arrow::export]]
std::shared_ptr<arrow::Buffer> io___BufferOutputStream__Finish(
    const std::shared_ptr<arrow::io::BufferOutputStream>& stream) {
  return ValueOrStop(stream->Finish());
}

// [[arrow::export]]
int64_t io___BufferOutputStream__Tell(
    const std::shared_ptr<arrow::io::BufferOutputStream>& stream) {
  return ValueOrStop(stream->Tell());
}

// [[arrow::export]]
void io___BufferOutputStream__Write(
    const std::shared_ptr<arrow::io::BufferOutputStream>& stream, cpp11::raws bytes) {
  StopIfNotOk(stream->Write(RAW(bytes), bytes.size()));
}

// TransformInputStream::TransformFunc wrapper

class RIconvWrapper {
public:
  RIconvWrapper(std::string to, std::string from)
    : handle_(Riconv_open(to.c_str(), from.c_str())) {
      if (handle_ == ((void*) -1)) {
        cpp11::stop("Can't convert encoding from '%s' to '%s'", from.c_str(), to.c_str());
      }
    }

  size_t iconv(const char **inbuf, size_t *inbytesleft, char **outbuf, size_t *outbytesleft) {
    return Riconv(handle_, inbuf, inbytesleft, outbuf, outbytesleft);
  }

  ~RIconvWrapper() {
    if (handle_ != ((void*) -1)) {
      Riconv_close(handle_);
    }
  }

protected:
  void* handle_;
};

struct ReencodeUTF8TransformFunctionWrapper {
  ReencodeUTF8TransformFunctionWrapper(std::string from)
    : from_(from), iconv_("UTF-8", from), n_pending_(0) {}

  // This may get copied and we need a freshly created RIconvWrapper
  // for each copy.
  ReencodeUTF8TransformFunctionWrapper(const ReencodeUTF8TransformFunctionWrapper& ref)
    : ReencodeUTF8TransformFunctionWrapper(ref.from_) {}

  arrow::Result<std::shared_ptr<arrow::Buffer>> operator()(const std::shared_ptr<arrow::Buffer>& src) {
    ARROW_ASSIGN_OR_RAISE(auto dest, arrow::AllocateResizableBuffer(32));
    const char* in_buf = (const char*) src->data();
    size_t in_bytes_left = src->size();
    size_t out_bytes_left = dest->size();
    char* out_buf = (char*) dest->data();
    size_t out_bytes_used = 0;

    // There may be a few leftover characters from the last call to iconv. Process these first
    // using the internal buffer as the source. This may also result in a partial character
    // leftover but will always get us into the src buffer.
    if (n_pending_ > 0) {
      cpp11::stop("not implemented");
    }

    // UTF-8 has a maximum of 4 bytes per character, so it's OK if we have a few bytes
    // left after processing all of src. If we have more than this, it means the
    // output buffer wasn't big enough.
    while (in_bytes_left >= 4) {
      int64_t new_size = std::max<int64_t>(src->size(), dest->size() * 2);
      auto reserve_result = dest->Reserve(new_size);
      if (!reserve_result.ok()) {
        cpp11::stop("Failed to allocate buffer of size %ld", new_size);
      }

      out_buf = (char*) dest->data() + out_bytes_used;
      out_bytes_left = dest->size() - out_bytes_used;
      size_t result = iconv_.iconv(&in_buf, &in_bytes_left, &out_buf, &out_bytes_left);
      if (result == ((size_t) -1)) {
        return arrow::Status::IOError("Riconv() failed with code -1");
      }

      int64_t chars_read_in = src->size() - in_bytes_left;
      int64_t chars_read_out = out_buf - ((char*) dest->data()) + out_bytes_used;
      out_bytes_used += chars_read_out;
      in_buf += chars_read_in;
    }

    // Keep the leftover characters until the next call to the function
    n_pending_ = in_bytes_left;
    if (in_bytes_left > 0) {
      memcpy(pending_, in_buf, in_bytes_left);
    }

    // Shrink the output buffer to only the size used
    auto resize_result = dest->Resize(out_bytes_used);
    if (!resize_result.ok()) {
      cpp11::stop("Failed to resize iconv result buffer");
    }

    return std::move(dest);
  }

protected:
  std::string from_;
  RIconvWrapper iconv_;
  char pending_[8];
  size_t n_pending_;
};

// [[arrow::export]]
std::shared_ptr<arrow::io::InputStream> MakeRencodeInputStream(const std::shared_ptr<arrow::io::InputStream>& wrapped, std::string from) {
  arrow::io::TransformInputStream::TransformFunc transform(ReencodeUTF8TransformFunctionWrapper{from});
  return std::make_shared<arrow::io::TransformInputStream>(std::move(wrapped), std::move(transform));
}

#endif
