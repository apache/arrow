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

#ifndef ARROW_UTIL_IO_UTIL_H
#define ARROW_UTIL_IO_UTIL_H

#include <memory>
#include <string>

#include "arrow/io/interfaces.h"
#include "arrow/status.h"
#include "arrow/util/macros.h"

// The Windows API defines DeleteFile as a macro resolving to either
// DeleteFileA or DeleteFileW.  Need to undo it.
#if defined(_WIN32) && defined(DeleteFile)
#undef DeleteFile
#endif

namespace arrow {

class Buffer;

namespace io {

// Output stream that just writes to stdout.
class ARROW_EXPORT StdoutStream : public OutputStream {
 public:
  StdoutStream();
  ~StdoutStream() override {}

  Status Close() override;
  bool closed() const override;

  Status Tell(int64_t* position) const override;

  Status Write(const void* data, int64_t nbytes) override;

 private:
  int64_t pos_;
};

// Output stream that just writes to stderr.
class ARROW_EXPORT StderrStream : public OutputStream {
 public:
  StderrStream();
  ~StderrStream() override {}

  Status Close() override;
  bool closed() const override;

  Status Tell(int64_t* position) const override;

  Status Write(const void* data, int64_t nbytes) override;

 private:
  int64_t pos_;
};

// Input stream that just reads from stdin.
class ARROW_EXPORT StdinStream : public InputStream {
 public:
  StdinStream();
  ~StdinStream() override {}

  Status Close() override;
  bool closed() const override;

  Status Tell(int64_t* position) const override;

  Status Read(int64_t nbytes, int64_t* bytes_read, void* out) override;

  Status Read(int64_t nbytes, std::shared_ptr<Buffer>* out) override;

 private:
  int64_t pos_;
};

}  // namespace io

namespace internal {

class ARROW_EXPORT PlatformFilename {
 public:
#if defined(_WIN32)
  using NativePathString = std::wstring;
#else
  using NativePathString = std::string;
#endif

  ~PlatformFilename();
  PlatformFilename();
  PlatformFilename(const PlatformFilename&);
  PlatformFilename(PlatformFilename&&);
  PlatformFilename& operator=(const PlatformFilename&);
  PlatformFilename& operator=(PlatformFilename&&);
  explicit PlatformFilename(const NativePathString& path);

  const NativePathString& ToNative() const;
  std::string ToString() const;

  // These functions can fail for character encoding reasons.
  static Status FromString(const std::string& file_name, PlatformFilename* out);
  Status Join(const std::string& child_name, PlatformFilename* out) const;

 private:
  struct Impl;
  std::unique_ptr<Impl> impl_;

  explicit PlatformFilename(const Impl& impl);
  explicit PlatformFilename(Impl&& impl);

  // Those functions need access to the embedded path object
  friend ARROW_EXPORT Status CreateDir(const PlatformFilename&, bool*);
  friend ARROW_EXPORT Status DeleteDirTree(const PlatformFilename&, bool*);
  friend ARROW_EXPORT Status DeleteFile(const PlatformFilename&, bool*);
  friend ARROW_EXPORT Status FileExists(const PlatformFilename&, bool*);
};

ARROW_EXPORT
Status CreateDir(const PlatformFilename& dir_path, bool* created = NULLPTR);
ARROW_EXPORT
Status DeleteDirTree(const PlatformFilename& dir_path, bool* deleted = NULLPTR);
ARROW_EXPORT
Status DeleteFile(const PlatformFilename& file_path, bool* deleted = NULLPTR);
ARROW_EXPORT
Status FileExists(const PlatformFilename& path, bool* out);

ARROW_EXPORT
Status FileNameFromString(const std::string& file_name, PlatformFilename* out);

ARROW_EXPORT
Status FileOpenReadable(const PlatformFilename& file_name, int* fd);
ARROW_EXPORT
Status FileOpenWritable(const PlatformFilename& file_name, bool write_only, bool truncate,
                        bool append, int* fd);

ARROW_EXPORT
Status FileRead(int fd, uint8_t* buffer, const int64_t nbytes, int64_t* bytes_read);
ARROW_EXPORT
Status FileReadAt(int fd, uint8_t* buffer, int64_t position, int64_t nbytes,
                  int64_t* bytes_read);
ARROW_EXPORT
Status FileWrite(int fd, const uint8_t* buffer, const int64_t nbytes);
ARROW_EXPORT
Status FileTruncate(int fd, const int64_t size);

ARROW_EXPORT
Status FileTell(int fd, int64_t* pos);
ARROW_EXPORT
Status FileSeek(int fd, int64_t pos);
ARROW_EXPORT
Status FileSeek(int fd, int64_t pos, int whence);
ARROW_EXPORT
Status FileGetSize(int fd, int64_t* size);

ARROW_EXPORT
Status FileClose(int fd);

ARROW_EXPORT
Status CreatePipe(int fd[2]);

ARROW_EXPORT
Status MemoryMapRemap(void* addr, size_t old_size, size_t new_size, int fildes,
                      void** new_addr);

ARROW_EXPORT
Status GetEnvVar(const char* name, std::string* out);
ARROW_EXPORT
Status GetEnvVar(const std::string& name, std::string* out);
ARROW_EXPORT
Status SetEnvVar(const char* name, const char* value);
ARROW_EXPORT
Status SetEnvVar(const std::string& name, const std::string& value);
ARROW_EXPORT
Status DelEnvVar(const char* name);
ARROW_EXPORT
Status DelEnvVar(const std::string& name);

class ARROW_EXPORT TemporaryDir {
 public:
  ~TemporaryDir();

  const PlatformFilename& path() { return path_; }

  static Status Make(const std::string& prefix, std::unique_ptr<TemporaryDir>* out);

 private:
  PlatformFilename path_;

  explicit TemporaryDir(PlatformFilename&&);
};

}  // namespace internal
}  // namespace arrow

#endif  // ARROW_UTIL_IO_UTIL_H
