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

namespace arrow {

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

class FileBase {
 public:
  virtual Status Close() = 0;
  virtual Status Tell(int64_t* position) = 0;
};

class ReadableFile : public FileBase {
 public:
  virtual Status ReadAt(
      int64_t position, int64_t nbytes, int64_t* bytes_read, uint8_t* buffer) = 0;

  virtual Status Read(int64_t nbytes, int64_t* bytes_read, uint8_t* buffer) = 0;

  virtual Status GetSize(int64_t* size) = 0;
};

class RandomAccessFile : public ReadableFile {
 public:
  virtual Status Seek(int64_t position) = 0;
};

class WriteableFile : public FileBase {
 public:
  virtual Status Write(const uint8_t* buffer, int64_t nbytes) = 0;
};

}  // namespace io
}  // namespace arrow

#endif  // ARROW_IO_INTERFACES_H
