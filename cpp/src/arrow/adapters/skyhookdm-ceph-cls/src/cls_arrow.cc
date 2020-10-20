#include "rados/objclass.h"
#include "cls_arrow_utils.h"

#include <arrow/api.h>
#include <arrow/dataset/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>

CLS_VER(1, 0)
CLS_NAME(arrow)

cls_handle_t h_class;
cls_method_handle_t h_read;
cls_method_handle_t h_write;

static int write(cls_method_context_t hctx, ceph::buffer::list *in, ceph::buffer::list *out) {
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

static int read(cls_method_context_t hctx, ceph::buffer::list *in, ceph::buffer::list *out) {
  int ret;
  arrow::Status arrow_ret;
  
  CLS_LOG(0, "deserializing scan request from the [in] bufferlist");
  std::shared_ptr<arrow::dataset::Expression> filter;
  std::shared_ptr<arrow::Schema> schema;
  arrow_ret = deserialize_scan_request_from_bufferlist(&filter, &schema, *in);
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
  arrow_ret = extract_batches_from_bufferlist(&batches, bl);
  if (!arrow_ret.ok()) {
    CLS_ERR("ERROR: failed to extract record batch vector from bufferlist");
    return -1;
  }

  CLS_LOG(0, "applying scan operations over the vector of record batches");
  std::shared_ptr<arrow::Table> result_table;
  arrow_ret = scan_batches(filter, schema, batches, &result_table);
  if (!arrow_ret.ok()) {
    CLS_ERR("ERROR: failed to scan vector of record batches");
    return -1;
  }

  CLS_LOG(0, "writing the resultant table into the [out] bufferlist");
  ceph::buffer::list result_bl;
  arrow_ret = write_table_to_bufferlist(result_table, result_bl);
  if (!arrow_ret.ok()) {
    CLS_ERR("ERROR: failed to write table to bufferlist");
    return -1;
  }
  *out = result_bl;  
  
  return 0;
}

CLS_INIT(arrow)
{
  CLS_LOG(0, "loading cls_arrow");

  cls_register("arrow", &h_class);

  cls_register_cxx_method(h_class, "read",
                          CLS_METHOD_RD | CLS_METHOD_WR, read,
                          &h_read);

  cls_register_cxx_method(h_class, "write",
                          CLS_METHOD_RD | CLS_METHOD_WR, write,
                          &h_write);
}
