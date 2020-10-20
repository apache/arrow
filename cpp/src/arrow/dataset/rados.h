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

#pragma once

#include <iostream>
#include <rados/librados.hpp>

#include "arrow/dataset/type_fwd.h"
#include "arrow/dataset/visibility.h"
#include "arrow/util/macros.h"

namespace arrow {
namespace dataset {

// should be an abstract class
class ARROW_DS_EXPORT IoCtxInterface {
  public:
    IoCtxInterface() {};
    virtual int write_full(const std::string& oid, librados::bufferlist& bl) = 0;
    virtual int read(const std::string& oid, librados::bufferlist& bl, size_t len, uint64_t off) = 0;
    virtual int exec(const std::string& oid, const char *cls, const char *method, librados::bufferlist& inbl, librados::bufferlist& outbl) = 0;
  private:
    friend class RadosWrapper;
    virtual void setIoCtx(librados::IoCtx *ioCtx_) = 0;
};

class ARROW_DS_EXPORT IoCtxWrapper: public IoCtxInterface {
  public:
     IoCtxWrapper() {
       ioCtx = new librados::IoCtx();
     }
    ~IoCtxWrapper() {
      delete ioCtx;
    }
    int write_full(const std::string& oid, librados::bufferlist& bl) override;
    int read(const std::string& oid, librados::bufferlist& bl, size_t len, uint64_t off) override;
    int exec(const std::string& oid, const char *cls, const char *method, librados::bufferlist& inbl, librados::bufferlist& outbl) override;
  private:
    void setIoCtx(librados::IoCtx *ioCtx_) override {
      *ioCtx = *ioCtx_;
    }
    librados::IoCtx *ioCtx;
};

// should be an abstract class
class ARROW_DS_EXPORT RadosInterface {
  public:
    RadosInterface() {};
    virtual int init2(const char * const name, const char * const clustername, uint64_t flags) = 0;
    virtual int ioctx_create(const char *name, IoCtxInterface *pioctx) = 0;
    virtual int conf_read_file(const char * const path) = 0;
    virtual int connect() = 0;
    virtual void shutdown() = 0;
};

class ARROW_DS_EXPORT RadosWrapper: public RadosInterface {
  public:
    RadosWrapper(){
      cluster = new librados::Rados();
    }

    ~RadosWrapper() {
      delete cluster;
    }

    int init2(const char * const name, const char * const clustername, uint64_t flags) override;
    int ioctx_create(const char *name, IoCtxInterface *pioctx) override;
    int conf_read_file(const char * const path);
    int connect();
    void shutdown();
  private:
    librados::Rados *cluster;
};

}
}
