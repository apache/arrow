#include <iostream>
#include <rados/librados.hpp>

#include "arrow/dataset/librados_interface.h"

int IoCtxInterface::write_full(const std::string& oid, librados::bufferlist& bl) override{
    return this->ioCtx.write_full(oid,bl);
}

int IoCtxInterface::read(const std::string& oid, librados::bufferlist& bl, size_t len, uint64_t off) override{
    return this->ioCtx.read(oid, bl, len, off);
}

int IoCtxInterface::exec(const std::string& oid, const char *cls, const char *method, librados::bufferlist& inbl, librados::bufferlist& outbl) override{
    return this->ioCtx.exec(oid, cls, method, inbl, outbl);
}

librados::IoCtx& IoCtxInterface::getIoCtx(){
    return this->ioCtx;
}

Radosinterface::init2(const char * const name, const char * const clustername, uint64_t flags) override {
    return cluster.init2(name, clustername, flags);
}

int RadosInterface::conf_read_file(const char * const path) override{
    return cluster.conf_read_file(path);
}

int RadosInterface::connect() {
    return cluster.connect();
}

librados::Rados& RadosInterface::getCluster(){
    return cluster;
}
