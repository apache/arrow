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

#include <rados/librados.hpp>

#include "arrow/status.h"

#include "skyhook/client/file_skyhook.h"

#include <memory>

namespace skyhook {
namespace rados {

class IoCtxInterface {
 public:
  IoCtxInterface() { ioCtx = std::make_unique<librados::IoCtx>(); }
  /// \brief Read from a RADOS object.
  ///
  /// \param[in] oid the ID of the object to read.
  /// \param[in] bl a bufferlist to hold the contents of the read object.
  /// \param[in] len the length of data to read from the object.
  /// \param[in] offset the offset to read from in the object.
  arrow::Status read(const std::string& oid, ceph::bufferlist& bl, size_t len,
                     uint64_t offset);
  /// \brief Executes a Ceph Object Class method.
  ///
  /// \param[in] oid the object ID on which to invoke the CLS function.
  /// \param[in] cls the name of the object class.
  /// \param[in] method the name of the object class method.
  /// \param[in] in a bufferlist to send data to the object class method.
  /// \param[in] out a bufferlist to recieve data from the object class method.
  arrow::Status exec(const std::string& oid, const char* cls, const char* method,
                     ceph::bufferlist& in, ceph::bufferlist& out);
  /// \brief Execute POSIX stat on a RADOS object.
  ///
  /// \param[in] oid the object ID on which to call stat.
  /// \param[out] psize hold the size of the object.
  arrow::Status stat(const std::string& oid, uint64_t* psize);
  /// \brief Set the `librados::IoCtx` instance inside a IoCtxInterface instance.
  void setIoCtx(librados::IoCtx* ioCtx_) { *ioCtx = *ioCtx_; }

 private:
  std::unique_ptr<librados::IoCtx> ioCtx;
};

class RadosInterface {
 public:
  RadosInterface() { cluster = std::make_unique<librados::Rados>(); }
  /// Initializes a cluster handle.
  arrow::Status init2(const char* const name, const char* const clustername,
                      uint64_t flags);
  /// Create an I/O context
  arrow::Status ioctx_create(const char* name, IoCtxInterface* pioctx);
  /// Read the Ceph config file.
  arrow::Status conf_read_file(const char* const path);
  /// Connect to the Ceph cluster.
  arrow::Status connect();
  /// Close connection to the Ceph cluster.
  void shutdown();

 private:
  std::unique_ptr<librados::Rados> cluster;
};

/// Connect to a Ceph cluster and hold the connection
/// information for use in later stages.
class RadosConn {
 public:
  explicit RadosConn(std::shared_ptr<skyhook::RadosConnCtx> ctx)
      : ctx(std::move(ctx)),
        rados(std::make_unique<RadosInterface>()),
        io_ctx(std::make_unique<IoCtxInterface>()),
        connected(false) {}
  ~RadosConn();
  /// Connect to the Ceph cluster.
  arrow::Status Connect();
  /// Shutdown the connection to the Ceph
  /// cluster if already connected.
  void Shutdown();

  std::shared_ptr<skyhook::RadosConnCtx> ctx;
  std::unique_ptr<RadosInterface> rados;
  std::unique_ptr<IoCtxInterface> io_ctx;
  bool connected;
};

}  // namespace rados
}  // namespace skyhook
