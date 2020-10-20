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

#include "rados_utils.h"

#include <iostream>
#include <vector>

#include "arrow/buffer.h"
#include "arrow/table.h"
#include "arrow/status.h"
#include "arrow/array.h"
#include "arrow/record_batch.h"
#include "arrow/ipc/writer.h"
#include "arrow/builder.h"

namespace arrow {
namespace dataset {

Result<std::shared_ptr<RecordBatch>> wrap_rados_scan_request(
        std::shared_ptr<Expression> filter,
        const Schema& projection_schema,
        int64_t batch_size,
        int64_t seq_num
) {
    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<Buffer> filter_buffer, filter->Serialize())
    int64_t filter_size = filter_buffer->size();

    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<Buffer> projection_buffer, ipc::SerializeSchema(projection_schema))
    int64_t projection_size = projection_buffer->size();

    std::shared_ptr<Field> filter_field = field("filter", fixed_size_binary(filter_size));
    std::shared_ptr<Field> schema_field = field("projection", fixed_size_binary(projection_size));
    std::shared_ptr<Field> batch_size_field = field("batch_size", int64());
    std::shared_ptr<Field> seq_num_field = field("seq_num", int64());

    std::vector<std::shared_ptr<Field>> schema_vector = {
            filter_field,
            schema_field,
            batch_size_field,
            seq_num_field,
    };
    std::shared_ptr<Schema> request_schema = std::make_shared<arrow::Schema>(schema_vector);

    FixedSizeBinaryBuilder filter_array_builder(fixed_size_binary(filter_size));
    RETURN_NOT_OK(filter_array_builder.Append(filter_buffer->data()));
    std::shared_ptr<Array> filter_array;
    RETURN_NOT_OK(filter_array_builder.Finish(&filter_array));

    FixedSizeBinaryBuilder projection_array_builder(fixed_size_binary(projection_size));
    RETURN_NOT_OK(projection_array_builder.Append(projection_buffer->data()));
    std::shared_ptr<Array> projection_array;
    RETURN_NOT_OK(projection_array_builder.Finish(&projection_array));

    auto batch_size_builder = std::make_shared<Int64Builder>();
    batch_size_builder->Reset();
    RETURN_NOT_OK(batch_size_builder->Append(batch_size));
    std::shared_ptr<Array> batch_size_array;
    RETURN_NOT_OK(batch_size_builder->Finish(&batch_size_array));

    auto seq_num_builder = std::make_shared<Int64Builder>();
    seq_num_builder->Reset();
    RETURN_NOT_OK(seq_num_builder->Append(seq_num));
    std::shared_ptr<Array> seq_num_array;
    RETURN_NOT_OK(seq_num_builder->Finish(&seq_num_array));

    std::vector<std::shared_ptr<Array>> columns = {
            filter_array,
            projection_array,
            batch_size_array,
            seq_num_array
    };

    return RecordBatch::Make(request_schema, 1, columns);
}

Result<ScanRequest> unwrap_rados_scan_request(
        std::shared_ptr<RecordBatch> request
){
    ScanRequest scanRequest;
    //reading the filter
    std::shared_ptr<Expression> filter_ptr;
    std::shared_ptr<FixedSizeBinaryArray> filter_array(new FixedSizeBinaryArray(request->GetColumnByName("filter")->data()));
    std::shared_ptr<Buffer> filter_buffer(new Buffer(filter_array->Value(0), filter_array->byte_width()));
    ARROW_ASSIGN_OR_RAISE(filter_ptr, Expression::Deserialize(*(filter_buffer.get())));
    scanRequest.filter = filter_ptr;

    std::shared_ptr<FixedSizeBinaryArray> proj_schema_array(new FixedSizeBinaryArray(request->GetColumnByName("projection")->data()));
    std::shared_ptr<Buffer> proj_schema_buffer(new Buffer(proj_schema_array->Value(0), proj_schema_array->byte_width()));
    io::BufferReader reader(proj_schema_buffer);
    ipc::DictionaryMemo in_memo;
    ARROW_ASSIGN_OR_RAISE(auto proj_schema, ipc::ReadSchema(&reader, &in_memo));
    std::shared_ptr<RecordBatchProjector> proj_ptr(new RecordBatchProjector(proj_schema));
    scanRequest.projector = proj_ptr;

    std::shared_ptr<Int64Array> batch_size_array(new Int64Array(request->GetColumnByName("batch_size")->data()));
    int64_t batch_size_value = batch_size_array->Value(0);
    scanRequest.batch_size = batch_size_value;

    std::shared_ptr<Int64Array> seq_num_array(new Int64Array(request->GetColumnByName("seq_num")->data()));
    int64_t seq_num_value = seq_num_array->Value(0);
    scanRequest.seq_num = seq_num_value;

    return scanRequest;
}

Status int64_to_char(uint8_t *num_buffer, int64_t num) {
  BasicDecimal128 decimal(num);
  decimal.ToBytes(num_buffer);
  return Status::OK();
}

Status char_to_int64(uint8_t *num_buffer, int64_t &num) {
  BasicDecimal128 basic_decimal(num_buffer);
  Decimal128 decimal(basic_decimal);
  num = (int64_t)decimal;
  return Status::OK();
}

Status serialize_scan_request_to_bufferlist(
        std::shared_ptr<Expression> filter, 
        std::shared_ptr<Schema> schema,
        librados::bufferlist &bl) {

  ARROW_ASSIGN_OR_RAISE(auto filter_buffer, filter->Serialize());
  ARROW_ASSIGN_OR_RAISE(auto schema_buffer, ipc::SerializeSchema(*schema));

  char *filter_size_buffer = new char[8];
  ARROW_RETURN_NOT_OK(int64_to_char((uint8_t*)filter_size_buffer, filter_buffer->size()));

  char *schema_size_buffer = new char[8];
  ARROW_RETURN_NOT_OK(int64_to_char((uint8_t*)schema_size_buffer, schema_buffer->size()));

  bl.append(filter_size_buffer, 8);
  bl.append((char*)filter_buffer->data(), filter_buffer->size());
  
  bl.append(schema_size_buffer, 8);
  bl.append((char*)schema_buffer->data(), schema_buffer->size());

  delete filter_size_buffer;
  delete schema_size_buffer;

  return Status::OK();
}

Status deserialize_scan_request_from_bufferlist(
        std::shared_ptr<Expression> *filter, 
        std::shared_ptr<Schema> *schema,
        librados::bufferlist bl) {

  int64_t filter_size = 0;
  char *filter_size_buffer = new char[8];
  bl.begin(0).copy(8, filter_size_buffer);
  ARROW_RETURN_NOT_OK(char_to_int64((uint8_t*)filter_size_buffer, filter_size));

  char *filter_buffer = new char[filter_size];
  bl.begin(8).copy(filter_size, filter_buffer);

  int64_t schema_size = 0;
  char *schema_size_buffer = new char[8];
  bl.begin(8 + filter_size).copy(8, schema_size_buffer);
  ARROW_RETURN_NOT_OK(char_to_int64((uint8_t*)schema_size_buffer, schema_size));

  char *schema_buffer = new char[schema_size];
  bl.begin(16 + filter_size).copy(schema_size, schema_buffer);

  ARROW_ASSIGN_OR_RAISE(auto filter_, Expression::Deserialize(Buffer((uint8_t*)filter_buffer, filter_size)));
  *filter = filter_;

  ipc::DictionaryMemo empty_memo;
  io::BufferReader schema_reader((uint8_t*)schema_buffer, schema_size);

  ARROW_ASSIGN_OR_RAISE(auto schema_, ipc::ReadSchema(&schema_reader, &empty_memo));
  *schema = schema_;

  return Status::OK();
}

Status read_table_from_bufferlist(
        std::shared_ptr<Table> *table,
        librados::bufferlist bl) {
  
  io::BufferReader reader((uint8_t*)bl.c_str(), bl.length());
  ARROW_ASSIGN_OR_RAISE(auto record_batch_reader, ipc::RecordBatchStreamReader::Open(&reader));
  ARROW_ASSIGN_OR_RAISE(auto table_, Table::FromRecordBatchReader(record_batch_reader.get()));
  *table = table_;
  return Status::OK();
}

Status write_table_to_bufferlist(std::shared_ptr<arrow::Table> &table,
                                 librados::bufferlist &bl) {

  ARROW_ASSIGN_OR_RAISE(auto buffer_output_stream, io::BufferOutputStream::Create());
  const auto options = arrow::ipc::IpcWriteOptions::Defaults();
  ARROW_ASSIGN_OR_RAISE(auto writer, ipc::NewStreamWriter(buffer_output_stream.get(), table->schema(), options));
  
  writer->WriteTable(*table);
  writer->Close();
  
  ARROW_ASSIGN_OR_RAISE(auto buffer, buffer_output_stream->Finish());
  bl.append((char*)buffer->data(), buffer->size());
  return Status::OK();
}

}  // namespace dataset
}  // namespace arrow