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
#include "./safe-call-into-r.h"

#include <R_ext/Riconv.h>

#include <arrow/buffer_builder.h>
#include <arrow/io/file.h>
#include <arrow/io/memory.h>
#include <arrow/io/transform.h>
#include <arrow/util/key_value_metadata.h>

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
r_vec_size io___RandomAccessFile__GetSize(
    const std::shared_ptr<arrow::io::RandomAccessFile>& x) {
  return r_vec_size(ValueOrStop(x->GetSize()));
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
r_vec_size io___RandomAccessFile__Tell(
    const std::shared_ptr<arrow::io::RandomAccessFile>& x) {
  return r_vec_size(ValueOrStop(x->Tell()));
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

// [[arrow::export]]
cpp11::strings io___RandomAccessFile__ReadMetadata(
    const std::shared_ptr<arrow::io::RandomAccessFile>& x) {
  std::shared_ptr<const arrow::KeyValueMetadata> metadata =
      ValueOrStop(x->ReadMetadata());
  if (metadata.get() == nullptr) {
    return cpp11::writable::strings();
  }

  cpp11::writable::strings metadata_r;
  cpp11::writable::strings metadata_r_names;
  metadata_r.reserve(metadata->size());
  metadata_r_names.reserve(metadata->size());

  for (int64_t i = 0; i < metadata->size(); i++) {
    metadata_r.push_back(metadata->value(i));
    metadata_r_names.push_back(metadata->key(i));
  }

  metadata_r.names() = metadata_r_names;
  return metadata_r;
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
r_vec_size io___OutputStream__Tell(
    const std::shared_ptr<arrow::io::OutputStream>& stream) {
  return r_vec_size(ValueOrStop(stream->Tell()));
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
r_vec_size io___BufferOutputStream__capacity(
    const std::shared_ptr<arrow::io::BufferOutputStream>& stream) {
  return r_vec_size(stream->capacity());
}

// [[arrow::export]]
std::shared_ptr<arrow::Buffer> io___BufferOutputStream__Finish(
    const std::shared_ptr<arrow::io::BufferOutputStream>& stream) {
  return ValueOrStop(stream->Finish());
}

// [[arrow::export]]
r_vec_size io___BufferOutputStream__Tell(
    const std::shared_ptr<arrow::io::BufferOutputStream>& stream) {
  return r_vec_size(ValueOrStop(stream->Tell()));
}

// [[arrow::export]]
void io___BufferOutputStream__Write(
    const std::shared_ptr<arrow::io::BufferOutputStream>& stream, cpp11::raws bytes) {
  StopIfNotOk(stream->Write(RAW(bytes), bytes.size()));
}

// ------ RConnectionInputStream / RConnectionOutputStream

class RConnectionFileInterface : public virtual arrow::io::FileInterface {
 public:
  explicit RConnectionFileInterface(cpp11::sexp connection_sexp)
      : connection_sexp_(connection_sexp), closed_(false) {
    check_closed();
  }

  arrow::Status Close() {
    if (closed_) {
      return arrow::Status::OK();
    }

    closed_ = true;

    return SafeCallIntoRVoid(
        [&]() { cpp11::package("base")["close"](connection_sexp_); });
  }

  arrow::Result<int64_t> Tell() const {
    if (closed()) {
      return arrow::Status::IOError("R connection is closed");
    }

    return SafeCallIntoR<int64_t>([&]() {
      cpp11::sexp result = cpp11::package("base")["seek"](connection_sexp_);
      return cpp11::as_cpp<int64_t>(result);
    });
  }

  bool closed() const { return closed_; }

 protected:
  cpp11::sexp connection_sexp_;

  // Define the logic here because multiple inheritance makes it difficult
  // for this base class, the InputStream and the RandomAccessFile
  // interfaces to co-exist.
  arrow::Result<int64_t> ReadBase(int64_t nbytes, void* out) {
    if (closed()) {
      return arrow::Status::IOError("R connection is closed");
    }

    return SafeCallIntoR<int64_t>([&] {
      cpp11::function read_bin = cpp11::package("base")["readBin"];
      cpp11::writable::raws ptype((R_xlen_t)0);
      cpp11::integers n = cpp11::as_sexp<int>(nbytes);

      cpp11::sexp result = read_bin(connection_sexp_, ptype, n);

      int64_t result_size = cpp11::safe[Rf_xlength](result);
      memcpy(out, cpp11::safe[RAW](result), result_size);
      return result_size;
    });
  }

  arrow::Result<std::shared_ptr<arrow::Buffer>> ReadBase(int64_t nbytes) {
    arrow::BufferBuilder builder;
    RETURN_NOT_OK(builder.Reserve(nbytes));

    ARROW_ASSIGN_OR_RAISE(int64_t bytes_read, ReadBase(nbytes, builder.mutable_data()));
    builder.UnsafeAdvance(bytes_read);
    return builder.Finish();
  }

  arrow::Status WriteBase(const void* data, int64_t nbytes) {
    if (closed()) {
      return arrow::Status::IOError("R connection is closed");
    }

    return SafeCallIntoRVoid([&]() {
      cpp11::writable::raws data_raw(nbytes);
      memcpy(cpp11::safe[RAW](data_raw), data, nbytes);

      cpp11::function write_bin = cpp11::package("base")["writeBin"];
      write_bin(data_raw, connection_sexp_);
    });
  }

  arrow::Status SeekBase(int64_t pos) {
    if (closed()) {
      return arrow::Status::IOError("R connection is closed");
    }

    return SafeCallIntoRVoid([&]() {
      cpp11::package("base")["seek"](connection_sexp_, cpp11::as_sexp<double>(pos));
    });
  }

 private:
  bool closed_;

  bool check_closed() {
    if (closed_) {
      return true;
    }

    auto is_open_result = SafeCallIntoR<bool>([&]() {
      cpp11::sexp result = cpp11::package("base")["isOpen"](connection_sexp_);
      return cpp11::as_cpp<bool>(result);
    });

    if (!is_open_result.ok()) {
      closed_ = true;
    } else {
      closed_ = !is_open_result.ValueUnsafe();
    }

    return closed_;
  }
};

class RConnectionInputStream : public virtual arrow::io::InputStream,
                               public RConnectionFileInterface {
 public:
  explicit RConnectionInputStream(cpp11::sexp connection_sexp)
      : RConnectionFileInterface(connection_sexp) {}

  arrow::Result<int64_t> Read(int64_t nbytes, void* out) { return ReadBase(nbytes, out); }

  arrow::Result<std::shared_ptr<arrow::Buffer>> Read(int64_t nbytes) {
    return ReadBase(nbytes);
  }
};

class RConnectionRandomAccessFile : public arrow::io::RandomAccessFile,
                                    public RConnectionFileInterface {
 public:
  explicit RConnectionRandomAccessFile(cpp11::sexp connection_sexp)
      : RConnectionFileInterface(connection_sexp) {
    // save the current position to seek back to it
    auto current_pos = Tell();
    if (!current_pos.ok()) {
      cpp11::stop("Tell() returned an error");
    }
    int64_t initial_pos = current_pos.ValueUnsafe();

    cpp11::package("base")["seek"](connection_sexp_, 0, "end");
    current_pos = Tell();
    if (!current_pos.ok()) {
      cpp11::stop("Tell() returned an error");
    }
    size_ = current_pos.ValueUnsafe();

    auto status = Seek(initial_pos);
    if (!status.ok()) {
      cpp11::stop("Seek() returned an error");
    }
  }

  arrow::Result<int64_t> GetSize() { return size_; }

  arrow::Status Seek(int64_t pos) { return SeekBase(pos); }

  arrow::Result<int64_t> Read(int64_t nbytes, void* out) { return ReadBase(nbytes, out); }

  arrow::Result<std::shared_ptr<arrow::Buffer>> Read(int64_t nbytes) {
    return ReadBase(nbytes);
  }

 private:
  int64_t size_;
};

class RConnectionOutputStream : public arrow::io::OutputStream,
                                public RConnectionFileInterface {
 public:
  explicit RConnectionOutputStream(cpp11::sexp connection_sexp)
      : RConnectionFileInterface(connection_sexp) {}

  arrow::Status Write(const void* data, int64_t nbytes) {
    return WriteBase(data, nbytes);
  }
};

// [[arrow::export]]
std::shared_ptr<arrow::io::InputStream> MakeRConnectionInputStream(cpp11::sexp con) {
  return std::make_shared<RConnectionInputStream>(con);
}

// [[arrow::export]]
std::shared_ptr<arrow::io::OutputStream> MakeRConnectionOutputStream(cpp11::sexp con) {
  return std::make_shared<RConnectionOutputStream>(con);
}

// [[arrow::export]]
std::shared_ptr<arrow::io::RandomAccessFile> MakeRConnectionRandomAccessFile(
    cpp11::sexp con) {
  return std::make_shared<RConnectionRandomAccessFile>(con);
}

// ------ MakeReencodeInputStream()

class RIconvWrapper {
 public:
  RIconvWrapper(std::string to, std::string from)
      : handle_(Riconv_open(to.c_str(), from.c_str())) {
    if (handle_ == ((void*)-1)) {
      cpp11::stop("Can't convert encoding from '%s' to '%s'", from.c_str(), to.c_str());
    }
  }

  size_t iconv(const uint8_t** inbuf, int64_t* inbytesleft, uint8_t** outbuf,
               int64_t* outbytesleft) {
    // This iconv signature uses the types that Arrow C++ uses to minimize
    // deviations from the style guide; however, iconv() uses pointers
    // to char* and size_t instead of uint8_t and int64_t.
    size_t inbytesleft_size_t = *inbytesleft;
    size_t outbytesleft_size_t = *outbytesleft;
    const char** inbuf_const_char = reinterpret_cast<const char**>(inbuf);
    char** outbuf_char = reinterpret_cast<char**>(outbuf);

    size_t return_value = Riconv(handle_, inbuf_const_char, &inbytesleft_size_t,
                                 outbuf_char, &outbytesleft_size_t);

    *inbytesleft = inbytesleft_size_t;
    *outbytesleft = outbytesleft_size_t;
    return return_value;
  }

  ~RIconvWrapper() {
    if (handle_ != ((void*)-1)) {
      Riconv_close(handle_);
    }
  }

 protected:
  void* handle_;
};

struct ReencodeUTF8TransformFunctionWrapper {
  explicit ReencodeUTF8TransformFunctionWrapper(std::string from)
      : from_(from),
        iconv_(std::make_shared<RIconvWrapper>("UTF-8", from)),
        n_pending_(0) {}

  arrow::Result<std::shared_ptr<arrow::Buffer>> operator()(
      const std::shared_ptr<arrow::Buffer>& src) {
    // A pre-allocation factor to account for possible data growth when
    // converting to UTF-8.
    constexpr double kOversizeFactor = 1.2;

    arrow::BufferBuilder builder;
    const int64_t initial_size = static_cast<int64_t>(src->size() * kOversizeFactor);
    RETURN_NOT_OK(builder.Reserve(initial_size));

    int64_t out_bytes_left = builder.capacity();
    uint8_t* out_buf = builder.mutable_data();

    int64_t in_bytes_left;
    const uint8_t* in_buf;
    int64_t n_src_bytes_in_pending = 0;

    // There may be a few left over bytes from the last call to iconv.
    // Process these first using the internal buffer (with as many bytes
    // as possible added from src) as the source. This may also result in
    // a partial character left over but will always get us into the src buffer.
    if (n_pending_ > 0) {
      n_src_bytes_in_pending =
          std::min<int64_t>(sizeof(pending_) - n_pending_, src->size());
      memcpy(pending_ + n_pending_, src->data(), n_src_bytes_in_pending);
      in_buf = pending_;
      in_bytes_left = n_pending_ + n_src_bytes_in_pending;

      iconv_->iconv(&in_buf, &in_bytes_left, &out_buf, &out_bytes_left);

      // Rather than check the error return code (which is often returned
      // in the case of a partial character at the end of the pending_
      // buffer), check that we have read enough characters to get into
      // `src` (after which the loop below will error for invalid characters).
      int64_t bytes_read_in = in_buf - pending_;
      if (bytes_read_in < n_pending_) {
        return StatusInvalidInput();
      }

      int64_t bytes_read_out = out_buf - builder.mutable_data();
      builder.UnsafeAdvance(bytes_read_out);

      int64_t chars_read_in = n_pending_ + n_src_bytes_in_pending - in_bytes_left;
      in_buf = src->data() + chars_read_in - n_pending_;
      in_bytes_left = src->size() + n_pending_ - chars_read_in;
    } else {
      in_buf = src->data();
      in_bytes_left = src->size();
    }

    // Try to call iconv() as many times as we need, potentially enlarging
    // the output buffer as needed. When zero bytes are appended, the loop
    // will either error (if there are more than 4 bytes left) or copy the
    // bytes to pending_ and wait for more input. We use 4 bytes because
    // this is the maximum number of bytes per complete character in UTF-8,
    // UTF-16, and UTF-32.
    while (in_bytes_left > 0) {
      // Make enough place in the output to hopefully consume all of the input.
      RETURN_NOT_OK(
          builder.Reserve(std::max<int64_t>(in_bytes_left * kOversizeFactor, 4)));
      out_buf = builder.mutable_data() + builder.length();
      out_bytes_left = builder.capacity() - builder.length();

      // iconv() can return an error code ((size_t) -1) but it's not
      // useful as it can occur because of invalid input, because
      // of a full output buffer, or because there are partial characters
      // at the end of the input buffer that were not completely decoded.
      // We handle each of these cases separately based on the number of bytes
      // read or written.
      uint8_t* out_buf_before = out_buf;

      iconv_->iconv(&in_buf, &in_bytes_left, &out_buf, &out_bytes_left);

      int64_t bytes_read_out = out_buf - out_buf_before;
      builder.UnsafeAdvance(bytes_read_out);

      // If no bytes were written out, we either have a partial valid
      // character or invalid input. If there are only a few bytes
      // left in the buffer it's likely that we have a partial character
      // that can be handled in the next call when there is more input
      // (which will error if the input is invalid).
      if (bytes_read_out == 0) {
        if (in_bytes_left <= 4) {
          break;
        } else {
          return StatusInvalidInput();
        }
      }
    }

    // Keep the leftover characters until the next call to the function
    n_pending_ = in_bytes_left;
    if (in_bytes_left > 0) {
      memcpy(pending_, in_buf, in_bytes_left);
    }

    // Shrink the output buffer to only the size used
    return builder.Finish();
  }

 protected:
  std::string from_;
  std::shared_ptr<RIconvWrapper> iconv_;
  uint8_t pending_[8];
  int64_t n_pending_;

  arrow::Status StatusInvalidInput() {
    return arrow::Status::Invalid("Encountered invalid input bytes ",
                                  "(input encoding was '", from_, "'");
  }
};

// [[arrow::export]]
std::shared_ptr<arrow::io::InputStream> MakeReencodeInputStream(
    const std::shared_ptr<arrow::io::InputStream>& wrapped, std::string from) {
  arrow::io::TransformInputStream::TransformFunc transform(
      ReencodeUTF8TransformFunctionWrapper{from});
  return std::make_shared<arrow::io::TransformInputStream>(std::move(wrapped),
                                                           std::move(transform));
}
