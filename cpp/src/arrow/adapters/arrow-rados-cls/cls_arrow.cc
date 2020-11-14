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

#include <rados/objclass.h>
#include <rados/librados.hpp>

#include "arrow/api.h"
#include "arrow/dataset/rados_utils.h"
#include "arrow/io/api.h"
#include "arrow/ipc/api.h"

CLS_VER(1, 0)
CLS_NAME(arrow)

cls_handle_t h_class;
cls_method_handle_t h_read_and_scan;
cls_method_handle_t h_write;

/// \brief Write data to an object.
///
/// \param[in] hctx the function execution context
/// \param[in] in the input bufferlist
/// \param[in] out the output bufferlist
static int write(cls_method_context_t hctx, ceph::buffer::list* in,
                 ceph::buffer::list* out) {
  int ret;

  CLS_LOG(0, "create an object");
  ret = cls_cxx_create(hctx, false);
  if (ret < 0) {
    CLS_ERR("ERROR: failed to create an object");
    return ret;
  }

  CLS_LOG(0, "write data into the object");
  ret = cls_cxx_write(hctx, 0, in->length(), in);
  if (ret < 0) {
    CLS_ERR("ERROR: failed to write to object");
    return ret;
  }

  return 0;
}

/// \brief Read record batches from an object and
/// apply the pushed down scan operations on them.
///
/// \param[in] hctx the function execution context
/// \param[in] in the input bufferlist
/// \param[in] out the output bufferlist
static int read_and_scan(cls_method_context_t hctx, ceph::buffer::list* in,
                         ceph::buffer::list* out) {
  int ret;
  arrow::Status arrow_ret;

  CLS_LOG(0, "deserializing scan request from the [in] bufferlist");
  std::shared_ptr<arrow::dataset::Expression> filter;
  std::shared_ptr<arrow::Schema> schema;
  arrow_ret =
      arrow::dataset::deserialize_scan_request_from_bufferlist(&filter, &schema, *in);
  if (!arrow_ret.ok()) {
    CLS_ERR("ERROR: failed to extract expression and schema");
    return -1;
  }

  CLS_LOG(0, "reading the the entire object into a bufferlist");
  ceph::buffer::list bl;
  ret = cls_cxx_read(hctx, 0, 0, &bl);
  if (ret < 0) {
    CLS_ERR("ERROR: failed to read an object");
    return ret;
  }

  CLS_LOG(0, "reading the vector of record batches from the bufferlist");
  arrow::RecordBatchVector batches;
  arrow_ret = arrow::dataset::extract_batches_from_bufferlist(&batches, bl);
  if (!arrow_ret.ok()) {
    CLS_ERR("ERROR: failed to extract record batch vector from bufferlist");
    return -1;
  }

  CLS_LOG(0, "applying scan operations over the vector of record batches");
  std::shared_ptr<arrow::Table> result_table;
  arrow_ret = arrow::dataset::scan_batches(filter, schema, batches, &result_table);
  if (!arrow_ret.ok()) {
    CLS_ERR("ERROR: failed to scan vector of record batches");
    return -1;
  }

  CLS_LOG(0, "writing the resultant table into the [out] bufferlist");
  ceph::buffer::list result_bl;
  arrow_ret = arrow::dataset::serialize_table_to_bufferlist(result_table, result_bl);
  if (!arrow_ret.ok()) {
    CLS_ERR("ERROR: failed to write table to bufferlist");
    return -1;
  }
  *out = result_bl;

  return 0;
}

void __cls_init() {
  CLS_LOG(0, "loading cls_arrow");

  cls_register("arrow", &h_class);

  cls_register_cxx_method(h_class, "read_and_scan", CLS_METHOD_RD | CLS_METHOD_WR,
                          read_and_scan, &h_read_and_scan);

  cls_register_cxx_method(h_class, "write", CLS_METHOD_RD | CLS_METHOD_WR, write,
                          &h_write);
}
