#pragma once

#include <iostream>
#include <rados/librados.hpp>

#include "arrow/dataset/type_fwd.h"
#include "arrow/dataset/visibility.h"
#include "arrow/util/macros.h"

namespace arrow {
namespace dataset {

class ARROW_DS_EXPORT IoCtxInterface {
  public:
    IoCtxInterface();
    int write_full(const std::string& oid, librados::bufferlist& bl);
    int read(const std::string& oid, librados::bufferlist& bl, size_t len, uint64_t off);
    int exec(const std::string& oid, const char *cls, const char *method, librados::bufferlist& inbl, librados::bufferlist& outbl);
    librados::IoCtx& getIoCtx();

  private:
    librados::IoCtx &ioCtx;
};

class ARROW_DS_EXPORT RadosInterface {
  public:
    RadosInterface();
    int init2(const char * const name, const char * const clustername, uint64_t flags);
    int conf_read_file(const char * const path);
    int connect();
    librados::Rados& getCluster();
  
  private:
    librados::Rados &cluster;
};

}
}
