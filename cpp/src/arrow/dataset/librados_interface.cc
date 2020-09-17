#include <iostream>
#include <rados/librados.hpp>

#include "arrow/dataset/librados_interface.h"

namespace arrow {
namespace dataset {

int IoCtxInterface::write_full(const std::string& oid, librados::bufferlist& bl) {
    return this->ioCtx.write_full(oid,bl);
}

int IoCtxInterface::read(const std::string& oid, librados::bufferlist& bl, size_t len, uint64_t off) {
    return this->ioCtx.read(oid, bl, len, off);
}

int IoCtxInterface::exec(const std::string& oid, const char *cls, const char *method, librados::bufferlist& inbl, librados::bufferlist& outbl) {
    return this->ioCtx.exec(oid, cls, method, inbl, outbl);
}

librados::IoCtx& IoCtxInterface::getIoCtx(){
    return this->ioCtx;
}

int RadosInterface::init2(const char * const name, const char * const clustername, uint64_t flags) {
    return this->cluster.init2(name, clustername, flags);
}

int RadosInterface::conf_read_file(const char * const path) {
    return this->cluster.conf_read_file(path);
}

int RadosInterface::connect() {
    return this->cluster.connect();
}

librados::Rados& RadosInterface::getCluster(){
    return this->cluster;
}

}
}
