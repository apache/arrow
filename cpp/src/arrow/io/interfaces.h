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

#ifndef ARROW_IO_INTERFACES_H
#define ARROW_IO_INTERFACES_H

#include <cstdint>
#include <memory>
#include <string>

#include "arrow/util/macros.h"
#include "arrow/util/visibility.h"

namespace arrow {

class Buffer;
class MemoryPool;
class Status;

namespace io {

struct FileMode {
  enum type { READ, WRITE, READWRITE };
};

struct ObjectType {
  enum type { FILE, DIRECTORY };
};

class FileSystemClient {
 public:
  virtual ~FileSystemClient() {}
};

class ARROW_EXPORT FileInterface {
 public:
  virtual ~FileInterface() = 0;
  virtual Status Close() = 0;
  virtual Status Tell(int64_t* position) = 0;

  FileMode::type mode() const { return mode_; }

 protected:
  FileInterface() {}
  FileMode::type mode_;
  void set_mode(FileMode::type mode) { mode_ = mode; }

 private:
  DISALLOW_COPY_AND_ASSIGN(FileInterface);
};

class Seekable {
 public:
  virtual Status Seek(int64_t position) = 0;
};

class ARROW_EXPORT Writeable {
 public:
  virtual Status Write(const uint8_t* data, int64_t nbytes) = 0;

  Status Write(const std::string& data);
};

class Readable {
 public:
  virtual Status Read(int64_t nbytes, int64_t* bytes_read, uint8_t* out) = 0;

  // Does not copy if not necessary
  virtual Status Read(int64_t nbytes, std::shared_ptr<Buffer>* out) = 0;
};

class OutputStream : public FileInterface, public Writeable {
 protected:
  OutputStream() {}
};

class InputStream : public FileInterface, public Readable {
 protected:
  InputStream() {}
};

class ARROW_EXPORT ReadableFileInterface : public InputStream, public Seekable {
 public:
  virtual Status GetSize(int64_t* size) = 0;

  virtual bool supports_zero_copy() const = 0;

  // Read at position, provide default implementations using Read(...), but can
  // be overridden
  virtual Status ReadAt(
      int64_t position, int64_t nbytes, int64_t* bytes_read, uint8_t* out);

  virtual Status ReadAt(int64_t position, int64_t nbytes, std::shared_ptr<Buffer>* out);

 protected:
  ReadableFileInterface();
};

class WriteableFileInterface : public OutputStream, public Seekable {
 public:
  virtual Status WriteAt(int64_t position, const uint8_t* data, int64_t nbytes) = 0;

 protected:
  WriteableFileInterface() { set_mode(FileMode::READ); }
};

class ReadWriteFileInterface : public ReadableFileInterface,
                               public WriteableFileInterface {
 protected:
  ReadWriteFileInterface() { ReadableFileInterface::set_mode(FileMode::READWRITE); }
};

}  // namespace io
}  // namespace arrow

#endif  // ARROW_IO_INTERFACES_H
