#pragma once

#include <iostream>
#include <rados/librados.hpp>

namespace arrow {
namespace dataset {

class ARROW_DS_EXPORT IoCtxInterface {
  public:
    virtual int write_full(const std::string& oid, librados::bufferlist& bl) = 0;
    virtual int read(const std::string& oid, librados::bufferlist& bl, size_t len, uint64_t off) = 0;
    virtual int exec(const std::string& oid, const char *cls, const char *method, librados::bufferlist& inbl, librados::bufferlist& outbl) = 0;
    librados::IoCtx& getIoCtx();

  protected:
    librados::IoCtx& ioCtx
};

class ARROW_DS_EXPORT RadosInterface {
  public:
    virtual int init2(const char * const name, const char * const clustername, uint64_t flags) = 0;
    virtual int conf_read_file(const char * const path) = 0;
    virtual int connect() = 0;
    librados::Rados& getCluster();
  
  protected:
    librados::Rados& cluster;
};

}
}
