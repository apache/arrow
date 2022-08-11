#include <arrow/api.h>
#include <arrow/compute/api.h>

#include <iostream>

arrow::Status RunMain(int argc, char** argv) {
  // Saving and Loading Tables
  arrow::Int32Builder int32builder;
  int32_t some_nums_raw[5] = {34, 624, 2223, 5654, 4356};
  ARROW_RETURN_NOT_OK(int32builder.AppendValues(some_nums_raw, 5));
  std::shared_ptr<arrow::Array> some_nums;
  ARROW_ASSIGN_OR_RAISE(some_nums, int32builder.Finish());

  int32_t more_nums_raw[5] = {75342, 23, 64, 17, 736};
  ARROW_RETURN_NOT_OK(int32builder.AppendValues(more_nums_raw, 5));
  std::shared_ptr<arrow::Array> more_nums;
  ARROW_ASSIGN_OR_RAISE(more_nums, int32builder.Finish());

  std::shared_ptr<arrow::Field> field_a, field_b;
  std::shared_ptr<arrow::Schema> schema;

  field_a = arrow::field("A", arrow::int32());
  field_b = arrow::field("B", arrow::int32());

  schema = arrow::schema({field_a, field_b});

  std::shared_ptr<arrow::Table> table; 
  table = arrow::Table::Make(schema, {some_nums, more_nums}, 5);

  // Performing Computations
  arrow::Datum sum;
  ARROW_ASSIGN_OR_RAISE(sum,
                      arrow::compute::Sum({table->GetColumnByName("A")}));                    
  std::cout << sum.type()->ToString() << std::endl;
  std::cout << sum.scalar_as<arrow::Int64Scalar>().value << std::endl;

  // Performing Computations
  arrow::Datum element_wise_sum;
  ARROW_ASSIGN_OR_RAISE(element_wise_sum,
                      arrow::compute::CallFunction("add", {table->GetColumnByName("A"),
                                                           table->GetColumnByName("B")}));
  std::cout << element_wise_sum.type()->ToString() << std::endl;
  std::cout << element_wise_sum.chunked_array()->ToString() << std::endl;

  return arrow::Status::OK();
}

int main(int argc, char** argv) {
  arrow::Status st = RunMain(argc, argv);
  if (!st.ok()) {
    std::cerr << st << std::endl;
    return 1;
  }
  return 0;
}
