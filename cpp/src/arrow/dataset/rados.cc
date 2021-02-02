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

#include "arrow/dataset/rados.h"

#include <iostream>
#include <vector>

namespace arrow {
namespace dataset {

int IoCtxWrapper::write_full(const std::string& oid, librados::bufferlist& bl) {
  return this->ioCtx->write_full(oid, bl);
}

int IoCtxWrapper::read(const std::string& oid, librados::bufferlist& bl, size_t len,
                       uint64_t offset) {
  return this->ioCtx->read(oid, bl, len, offset);
}

int IoCtxWrapper::exec(const std::string& oid, const char* cls, const char* method,
                       librados::bufferlist& in, librados::bufferlist& out) {
  return this->ioCtx->exec(oid, cls, method, in, out);
}

int IoCtxWrapper::stat(const std::string& oid, uint64_t* psize) {
  return this->ioCtx->stat(oid, psize, NULL);
}

std::vector<std::string> IoCtxWrapper::list() {
  std::vector<std::string> oids;
  librados::NObjectIterator begin = this->ioCtx->nobjects_begin();
  librados::NObjectIterator end = this->ioCtx->nobjects_end();
  for (; begin != end; begin++) {
    oids.push_back(begin->get_oid());
  }
  return oids;
}

int RadosWrapper::init2(const char* const name, const char* const clustername,
                        uint64_t flags) {
  return this->cluster->init2(name, clustername, flags);
}

int RadosWrapper::ioctx_create(const char* name, IoCtxInterface* pioctx) {
  librados::IoCtx ioCtx;
  int ret = this->cluster->ioctx_create(name, ioCtx);
  pioctx->setIoCtx(&ioCtx);
  return ret;
}

int RadosWrapper::conf_read_file(const char* const path) {
  return this->cluster->conf_read_file(path);
}

int RadosWrapper::connect() { return this->cluster->connect(); }

void RadosWrapper::shutdown() { return this->cluster->shutdown(); }

}  // namespace dataset
}  // namespace arrow
