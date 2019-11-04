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

// FileStype

// [[arrow::export]]
arrow::fs::FileType fs___FileStats__type(const std::shared_ptr<arrow::fs::FileStats>& x) {
  return x->type();
}

// [[arrow::export]]
void fs___FileStats__set_type(const std::shared_ptr<arrow::fs::FileStats>& x,
                              arrow::fs::FileType type) {
  x->set_type(type);
}

// [[arrow::export]]
std::string fs___FileStats__path(const std::shared_ptr<arrow::fs::FileStats>& x) {
  return x->path();
}

// [[arrow::export]]
void fs___FileStats__set_path(const std::shared_ptr<arrow::fs::FileStats>& x,
                              const std::string& path) {
  x->set_path(path);
}

// [[arrow::export]]
int64_t fs___FileStats__size(const std::shared_ptr<arrow::fs::FileStats>& x) {
  return x->size();
}

// [[arrow::export]]
void fs___FileStats__set_size(const std::shared_ptr<arrow::fs::FileStats>& x,
                              int64_t size) {
  x->set_size(size);
}

// [[arrow::export]]
std::string fs___FileStats__base_name(const std::shared_ptr<arrow::fs::FileStats>& x) {
  return x->base_name();
}

// [[arrow::export]]
std::string fs___FileStats__extension(const std::shared_ptr<arrow::fs::FileStats>& x) {
  return x->extension();
}

// [[arrow::export]]
SEXP fs___FileStats__mtime(const std::shared_ptr<arrow::fs::FileStats>& x) {
  SEXP res = PROTECT(Rf_allocVector(REALSXP, 1));
  // .mtime() gets us nanoseconds since epoch, POSIXct is seconds since epoch as a double
  REAL(res)[0] = static_cast<double>(x->mtime().time_since_epoch().count()) / 1000000000;
  Rf_classgets(res, arrow::r::data::classes_POSIXct);
  UNPROTECT(1);
  return res;
}

// [[arrow::export]]
void fs___FileStats__set_mtime(const std::shared_ptr<arrow::fs::FileStats>& x,
                               SEXP time) {
  auto nanosecs =
      std::chrono::nanoseconds(static_cast<int64_t>(REAL(time)[0] * 1000000000));
  x->set_mtime(arrow::fs::TimePoint(nanosecs));
}

// Selector

// [[arrow::export]]
std::string fs___Selector__base_dir(
    const std::shared_ptr<arrow::fs::Selector>& selector) {
  return selector->base_dir;
}

// [[arrow::export]]
bool fs___Selector__allow_non_existent(
    const std::shared_ptr<arrow::fs::Selector>& selector) {
  return selector->allow_non_existent;
}

// [[arrow::export]]
bool fs___Selector__recursive(const std::shared_ptr<arrow::fs::Selector>& selector) {
  return selector->recursive;
}

// [[arrow::export]]
std::shared_ptr<arrow::fs::Selector> fs___Selector__create(const std::string& base_dir,
                                                           bool allow_non_existent,
                                                           bool recursive) {
  auto selector = std::make_shared<arrow::fs::Selector>();
  selector->base_dir = base_dir;
  selector->allow_non_existent = allow_non_existent;
  selector->recursive = recursive;
  return selector;
}

// FileSystem

template <typename T>
std::vector<std::shared_ptr<T>> shared_ptr_vector(const std::vector<T>& vec) {
  std::vector<std::shared_ptr<arrow::fs::FileStats>> res(vec.size());
  std::transform(vec.begin(), vec.end(), res.begin(), [](const arrow::fs::FileStats& x) {
    return std::make_shared<arrow::fs::FileStats>(x);
  });
  return res;
}

// [[arrow::export]]
std::vector<std::shared_ptr<arrow::fs::FileStats>> fs___FileSystem__GetTargetStats_Paths(
    const std::shared_ptr<arrow::fs::FileSystem>& file_system,
    const std::vector<std::string>& paths) {
  std::vector<arrow::fs::FileStats> out;
  STOP_IF_NOT_OK(file_system->GetTargetStats(paths, &out));
  return shared_ptr_vector(out);
}

// [[arrow::export]]
std::vector<std::shared_ptr<arrow::fs::FileStats>>
fs___FileSystem__GetTargetStats_Selector(
    const std::shared_ptr<arrow::fs::FileSystem>& file_system,
    const std::shared_ptr<arrow::fs::Selector>& selector) {
  std::vector<arrow::fs::FileStats> out;
  STOP_IF_NOT_OK(file_system->GetTargetStats(*selector, &out));
  return shared_ptr_vector(out);
}

// [[arrow::export]]
void fs___FileSystem__CreateDir(const std::shared_ptr<arrow::fs::FileSystem>& file_system,
                                const std::string& path, bool recursive) {
  STOP_IF_NOT_OK(file_system->CreateDir(path, recursive));
}

// [[arrow::export]]
void fs___FileSystem__DeleteDir(const std::shared_ptr<arrow::fs::FileSystem>& file_system,
                                const std::string& path) {
  STOP_IF_NOT_OK(file_system->DeleteDir(path));
}

// [[arrow::export]]
void fs___FileSystem__DeleteDirContents(
    const std::shared_ptr<arrow::fs::FileSystem>& file_system, const std::string& path) {
  STOP_IF_NOT_OK(file_system->DeleteDirContents(path));
}

// [[arrow::export]]
void fs___FileSystem__DeleteFile(
    const std::shared_ptr<arrow::fs::FileSystem>& file_system, const std::string& path) {
  STOP_IF_NOT_OK(file_system->DeleteFile(path));
}

// [[arrow::export]]
void fs___FileSystem__DeleteFiles(
    const std::shared_ptr<arrow::fs::FileSystem>& file_system,
    const std::vector<std::string>& paths) {
  STOP_IF_NOT_OK(file_system->DeleteFiles(paths));
}

// [[arrow::export]]
void fs___FileSystem__Move(const std::shared_ptr<arrow::fs::FileSystem>& file_system,
                           const std::string& src, const std::string& dest) {
  STOP_IF_NOT_OK(file_system->Move(src, dest));
}

// [[arrow::export]]
void fs___FileSystem__CopyFile(const std::shared_ptr<arrow::fs::FileSystem>& file_system,
                               const std::string& src, const std::string& dest) {
  STOP_IF_NOT_OK(file_system->CopyFile(src, dest));
}

// [[arrow::export]]
std::shared_ptr<arrow::io::InputStream> fs___FileSystem__OpenInputStream(
    const std::shared_ptr<arrow::fs::FileSystem>& file_system, const std::string& path) {
  std::shared_ptr<arrow::io::InputStream> stream;
  STOP_IF_NOT_OK(file_system->OpenInputStream(path, &stream));
  return stream;
}

// [[arrow::export]]
std::shared_ptr<arrow::io::RandomAccessFile> fs___FileSystem__OpenInputFile(
    const std::shared_ptr<arrow::fs::FileSystem>& file_system, const std::string& path) {
  std::shared_ptr<arrow::io::RandomAccessFile> file;
  STOP_IF_NOT_OK(file_system->OpenInputFile(path, &file));
  return file;
}

// [[arrow::export]]
std::shared_ptr<arrow::io::OutputStream> fs___FileSystem__OpenOutputStream(
    const std::shared_ptr<arrow::fs::FileSystem>& file_system, const std::string& path) {
  std::shared_ptr<arrow::io::OutputStream> stream;
  STOP_IF_NOT_OK(file_system->OpenOutputStream(path, &stream));
  return stream;
}

// [[arrow::export]]
std::shared_ptr<arrow::io::OutputStream> fs___FileSystem__OpenAppendStream(
    const std::shared_ptr<arrow::fs::FileSystem>& file_system, const std::string& path) {
  std::shared_ptr<arrow::io::OutputStream> stream;
  STOP_IF_NOT_OK(file_system->OpenAppendStream(path, &stream));
  return stream;
}

// [[arrow::export]]
std::shared_ptr<arrow::fs::LocalFileSystem> fs___LocalFileSystem__create() {
  return std::make_shared<arrow::fs::LocalFileSystem>();
}

// [[arrow::export]]
std::shared_ptr<arrow::fs::SubTreeFileSystem> fs___SubTreeFileSystem__create(
    const std::string& base_path, const std::shared_ptr<arrow::fs::FileSystem>& base_fs) {
  return std::make_shared<arrow::fs::SubTreeFileSystem>(base_path, base_fs);
}

#endif
