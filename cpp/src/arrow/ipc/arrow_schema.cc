// TEMPORARY. DON'T MERGE WITH MASTER

#include <iostream>
#include <vector>

#include "arrow/type.h"
#include "arrow/ipc/metadata_internal.h"

int main(int argc, char* argv[]) {
  std::vector<std::shared_ptr<arrow::Field>> schema_vector = {
    arrow::field("id", arrow::int64()),
    arrow::field("cost", arrow::float64()),
    arrow::field("cost_components", arrow::list(arrow::float64()))};
  auto schema = arrow::Schema(schema_vector);

  auto schema_json = schema.ToJson();
  std::cout << "Schema:\n" << schema_json << std::endl;

  auto loaded_schema = arrow::Schema::FromJson(schema_json);
  return 0;
}
