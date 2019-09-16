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
void fs___FileStats__set_type(const std::shared_ptr<arrow::fs::FileStats>& x, arrow::fs::FileType type) {
  x->set_type(type);
}

// [[arrow::export]]
std::string fs___FileStats__path(const std::shared_ptr<arrow::fs::FileStats>& x) {
  return x->path();
}

// [[arrow::export]]
void fs___FileStats__set_path(const std::shared_ptr<arrow::fs::FileStats>& x, const std::string& path) {
  x->set_path(path);
}

// [[arrow::export]]
int64_t fs___FileStats__size(const std::shared_ptr<arrow::fs::FileStats>& x) {
  return x->size();
}

// [[arrow::export]]
void fs___FileStats__set_size(const std::shared_ptr<arrow::fs::FileStats>& x, int64_t size) {
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
  REAL(res)[0] = static_cast<double>(x->mtime().time_since_epoch().count()) / 1000000;
  Rf_classgets(res, arrow::r::data::classes_POSIXct);
  UNPROTECT(1);
  return res;
}

// [[arrow::export]]
void fs___FileStats__set_mtime(const std::shared_ptr<arrow::fs::FileStats>& x, SEXP time) {
  auto secs = std::chrono::seconds(static_cast<int64_t>(REAL(time)[0] * 1000000));
  x->set_mtime(arrow::fs::TimePoint(secs));
}


// Selector

// [[arrow::export]]
std::string fs___Selector__base_dir(const std::shared_ptr<arrow::fs::Selector>& selector) {
  return selector->base_dir;
}

// [[arrow::export]]
bool fs___Selector__allow_non_existent(const std::shared_ptr<arrow::fs::Selector>& selector) {
  return selector->allow_non_existent;
}

// [[arrow::export]]
bool fs___Selector__recursive(const std::shared_ptr<arrow::fs::Selector>& selector) {
  return selector->recursive;
}


#endif
