#include "cls_arrow_utils.h"

#include <iostream>

int create_test_arrow_table(std::shared_ptr<arrow::Table> *out_table) {
    
  // create a memory pool
  arrow::MemoryPool *pool = arrow::default_memory_pool();
    
  // arrow array builder for each table column
  arrow::Int32Builder id_builder(pool);
  arrow::DoubleBuilder cost_builder(pool);
  arrow::ListBuilder components_builder(
      pool, std::make_shared<arrow::DoubleBuilder>(pool));
    
  // The following builder is owned by components_builder.
  arrow::DoubleBuilder &cost_components_builder = *(
      static_cast<arrow::DoubleBuilder *>(components_builder.value_builder()));

  // append some fake data
  for (int i = 0; i < 10; ++i) {
    id_builder.Append(i);
    cost_builder.Append(i + 1.0);
    // Indicate the start of a new list row. This will memorise the current
    // offset in the values builder.
    components_builder.Append();
    std::vector<double> nums;
    nums.push_back(i + 1.0);
    nums.push_back(i + 2.0);
    nums.push_back(i + 3.0);
    cost_components_builder.AppendValues(nums.data(), nums.size());
  }
  
  // At the end, we finalise the arrays, declare the (type) schema and combine
  // them into a single `arrow::Table`:
  std::shared_ptr<arrow::Int32Array> id_array;
  id_builder.Finish(&id_array);
  std::shared_ptr<arrow::DoubleArray> cost_array;
  cost_builder.Finish(&cost_array);
  
  // No need to invoke cost_components_builder.Finish because it is implied by
  // the parent builder's Finish invocation.
  std::shared_ptr<arrow::ListArray> cost_components_array;
  components_builder.Finish(&cost_components_array);

  // create table schema and make table from col arrays and schema
  std::vector<std::shared_ptr<arrow::Field>> schema_vector = {
      arrow::field("id", arrow::int32()),
      arrow::field("cost", arrow::float64()),
      arrow::field("cost_components", arrow::list(arrow::float64()))};
  auto schema = std::make_shared<arrow::Schema>(schema_vector);
  *out_table =
      arrow::Table::Make(schema, {id_array, cost_array, cost_components_array});

  if (*out_table == nullptr) {
    return -1;
  }
  return 0;
}

arrow::Status int64_to_char(uint8_t *num_buffer, int64_t num) {
  arrow::BasicDecimal128 decimal(num);
  decimal.ToBytes(num_buffer);
  return arrow::Status::OK();
}

arrow::Status char_to_int64(uint8_t num_buffer[8], int64_t &num) {
  arrow::BasicDecimal128 basic_decimal(num_buffer);
  arrow::Decimal128 decimal(basic_decimal);
  num = (int64_t)decimal;
  return arrow::Status::OK();
}

arrow::Status deserialize_scan_request_from_bufferlist(std::shared_ptr<arrow::dataset::Expression> *filter, std::shared_ptr<arrow::Schema> *schema, librados::bufferlist bl) {
  int64_t filter_size = 0;
  char filter_size_buffer[8];
  bl.begin(0).copy(8, filter_size_buffer);
  ARROW_RETURN_NOT_OK(char_to_int64((uint8_t*)filter_size_buffer, filter_size));

  char *filter_buffer = new char[filter_size];
  bl.begin(8).copy(filter_size, filter_buffer);

  int64_t schema_size = 0;
  char schema_size_buffer[8];
  bl.begin(8 + filter_size).copy(8, schema_size_buffer);
  ARROW_RETURN_NOT_OK(char_to_int64((uint8_t*)schema_size_buffer, schema_size));

  char *schema_buffer = new char[schema_size];
  bl.begin(16 + filter_size).copy(schema_size, schema_buffer);

  ARROW_ASSIGN_OR_RAISE(auto filter_, arrow::dataset::Expression::Deserialize(arrow::Buffer((uint8_t*)filter_buffer, filter_size)));
  *filter = filter_;

  arrow::ipc::DictionaryMemo empty_memo;
  arrow::io::BufferReader schema_reader((uint8_t*)schema_buffer, schema_size);

  ARROW_ASSIGN_OR_RAISE(auto schema_, arrow::ipc::ReadSchema(&schema_reader, &empty_memo));
  *schema = schema_;

  return arrow::Status::OK();
}

arrow::Status serialize_scan_request_to_bufferlist(std::shared_ptr<arrow::dataset::Expression> filter, std::shared_ptr<arrow::Schema> schema, librados::bufferlist &bl) {
  ARROW_ASSIGN_OR_RAISE(auto filter_buffer, filter->Serialize());

  ARROW_ASSIGN_OR_RAISE(auto schema_buffer, arrow::ipc::SerializeSchema(*schema));

  char *filter_size_buffer = new char[8];
  ARROW_RETURN_NOT_OK(int64_to_char((uint8_t*)filter_size_buffer, filter_buffer->size()));

  char *schema_size_buffer = new char[8];
  ARROW_RETURN_NOT_OK(int64_to_char((uint8_t*)schema_size_buffer, schema_buffer->size()));

  bl.append(filter_size_buffer, 8);
  bl.append((char*)filter_buffer->data(), filter_buffer->size());

  bl.append(schema_size_buffer, 8);
  bl.append((char*)schema_buffer->data(), schema_buffer->size());

  return arrow::Status::OK();
}


arrow::Status extract_batches_from_bufferlist(arrow::RecordBatchVector *batches, ceph::buffer::list &bl) {
  std::shared_ptr<arrow::Buffer> buffer = std::make_shared<arrow::Buffer>((uint8_t*)bl.c_str(), bl.length());
  std::shared_ptr<arrow::io::BufferReader> buffer_reader = std::make_shared<arrow::io::BufferReader>(buffer);
  ARROW_ASSIGN_OR_RAISE(auto record_batch_reader, arrow::ipc::RecordBatchStreamReader::Open(buffer_reader));
  ARROW_RETURN_NOT_OK(record_batch_reader->ReadAll(batches));
  return arrow::Status::OK();
}

arrow::Status write_table_to_bufferlist(std::shared_ptr<arrow::Table> &table, ceph::buffer::list &bl) {
  ARROW_ASSIGN_OR_RAISE(auto buffer_output_stream, arrow::io::BufferOutputStream::Create());
  const auto options = arrow::ipc::IpcWriteOptions::Defaults();
  ARROW_ASSIGN_OR_RAISE(auto writer, arrow::ipc::MakeStreamWriter(buffer_output_stream, table->schema(), options));

  writer->WriteTable(*table);
  writer->Close();

  ARROW_ASSIGN_OR_RAISE(auto buffer, buffer_output_stream->Finish());
  bl.append((char*)buffer->data(), buffer->size());
  return arrow::Status::OK();
}

arrow::Status scan_batches(std::shared_ptr<arrow::dataset::Expression> &filter, std::shared_ptr<arrow::Schema> &schema, arrow::RecordBatchVector &batches, std::shared_ptr<arrow::Table> *table) {
  std::shared_ptr<arrow::dataset::ScanContext> scan_context = std::make_shared<arrow::dataset::ScanContext>();
  std::shared_ptr<arrow::dataset::InMemoryFragment> fragment = std::make_shared<arrow::dataset::InMemoryFragment>(batches);
  auto batch_schema = batches[0]->schema();
  std::shared_ptr<arrow::dataset::ScannerBuilder> builder = std::make_shared<arrow::dataset::ScannerBuilder>(batch_schema, fragment, scan_context);
  ARROW_RETURN_NOT_OK(builder->Filter(filter));
  ARROW_RETURN_NOT_OK(builder->Project(schema->field_names()));
  ARROW_ASSIGN_OR_RAISE(auto scanner, builder->Finish());
  ARROW_ASSIGN_OR_RAISE(auto table_, scanner->ToTable());

  *table = table_;
  return arrow::Status::OK();
}

arrow::Status read_table_from_bufferlist(std::shared_ptr<arrow::Table> *table, librados::bufferlist &bl) {
  arrow::io::BufferReader reader((uint8_t*)bl.c_str(), bl.length());
  ARROW_ASSIGN_OR_RAISE(auto record_batch_reader, arrow::ipc::RecordBatchStreamReader::Open(&reader));
  ARROW_ASSIGN_OR_RAISE(auto table_, arrow::Table::FromRecordBatchReader(record_batch_reader.get()));
  *table = table_;
  return arrow::Status::OK();
}
