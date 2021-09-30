// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

#include <arrow/api.h>
#include <arrow/result.h>

#include <cstdint>
#include <iomanip>
#include <iostream>
#include <vector>

using arrow::DoubleBuilder;
using arrow::Int64Builder;
using arrow::ListBuilder;

// While we want to use columnar data structures to build efficient operations, we
// often receive data in a row-wise fashion from other systems. In the following,
// we want give a brief introduction into the classes provided by Apache Arrow by
// showing how to transform row-wise data into a columnar table.
//
// The table contains an id for a product, the number of components in the product
// and the cost of each component.
//
// The data in this example is stored in the following struct:
struct data_row {
  int64_t id;
  int64_t components;
  std::vector<double> component_cost;
};

// Transforming a vector of structs into a columnar Table.
//
// The final representation should be an `arrow::Table` which in turn
// is made up of an `arrow::Schema` and a list of
// `arrow::ChunkedArray` instances. As the first step, we will iterate
// over the data and build up the arrays incrementally.  For this
// task, we provide `arrow::ArrayBuilder` classes that help in the
// construction of the final `arrow::Array` instances.
//
// For each type, Arrow has a specially typed builder class. For the primitive
// values `id` and `components` we can use the `arrow::Int64Builder`. For the
// `component_cost` vector, we need to have two builders, a top-level
// `arrow::ListBuilder` that builds the array of offsets and a nested
// `arrow::DoubleBuilder` that constructs the underlying values array that
// is referenced by the offsets in the former array.
arrow::Result<std::shared_ptr<arrow::Table>> VectorToColumnarTable(
    const std::vector<struct data_row>& rows) {
  // The builders are more efficient using
  // arrow::jemalloc::MemoryPool::default_pool() as this can increase the size of
  // the underlying memory regions in-place. At the moment, arrow::jemalloc is only
  // supported on Unix systems, not Windows.
  arrow::MemoryPool* pool = arrow::default_memory_pool();

  Int64Builder id_builder(pool);
  Int64Builder components_builder(pool);
  ListBuilder component_cost_builder(pool, std::make_shared<DoubleBuilder>(pool));
  // The following builder is owned by component_cost_builder.
  DoubleBuilder* component_item_cost_builder =
      (static_cast<DoubleBuilder*>(component_cost_builder.value_builder()));

  // Now we can loop over our existing data and insert it into the builders. The
  // `Append` calls here may fail (e.g. we cannot allocate enough additional memory).
  // Thus we need to check their return values. For more information on these values,
  // check the documentation about `arrow::Status`.
  for (const data_row& row : rows) {
    ARROW_RETURN_NOT_OK(id_builder.Append(row.id));
    ARROW_RETURN_NOT_OK(components_builder.Append(row.components));

    // Indicate the start of a new list row. This will memorise the current
    // offset in the values builder.
    ARROW_RETURN_NOT_OK(component_cost_builder.Append());
    // Store the actual values. The same memory layout is
    // used for the component cost data, in this case a vector of
    // type double, as for the memory that Arrow uses to hold this
    // data and will be created.
    ARROW_RETURN_NOT_OK(component_item_cost_builder->AppendValues(
        row.component_cost.data(), row.component_cost.size()));
  }

  // At the end, we finalise the arrays, declare the (type) schema and combine them
  // into a single `arrow::Table`:
  std::shared_ptr<arrow::Array> id_array;
  ARROW_RETURN_NOT_OK(id_builder.Finish(&id_array));
  std::shared_ptr<arrow::Array> components_array;
  ARROW_RETURN_NOT_OK(components_builder.Finish(&components_array));
  // No need to invoke component_cost_builder.Finish because it is implied by
  // the parent builder's Finish invocation.
  std::shared_ptr<arrow::Array> component_cost_array;
  ARROW_RETURN_NOT_OK(component_cost_builder.Finish(&component_cost_array));

  std::vector<std::shared_ptr<arrow::Field>> schema_vector = {
      arrow::field("id", arrow::int64()), arrow::field("components", arrow::int64()),
      arrow::field("component_cost", arrow::list(arrow::float64()))};

  auto schema = std::make_shared<arrow::Schema>(schema_vector);

  // The final `table` variable is the one we can then pass on to other functions
  // that can consume Apache Arrow memory structures. This object has ownership of
  // all referenced data, thus we don't have to care about undefined references once
  // we leave the scope of the function building the table and its underlying arrays.
  std::shared_ptr<arrow::Table> table =
      arrow::Table::Make(schema, {id_array, components_array, component_cost_array});

  return table;
}

arrow::Result<std::vector<data_row>> ColumnarTableToVector(
    const std::shared_ptr<arrow::Table>& table) {
  // To convert an Arrow table back into the same row-wise representation as in the
  // above section, we first will check that the table conforms to our expected
  // schema and then will build up the vector of rows incrementally.
  //
  // For the check if the table is as expected, we can utilise solely its schema.
  std::vector<std::shared_ptr<arrow::Field>> schema_vector = {
      arrow::field("id", arrow::int64()), arrow::field("components", arrow::int64()),
      arrow::field("component_cost", arrow::list(arrow::float64()))};
  auto expected_schema = std::make_shared<arrow::Schema>(schema_vector);

  if (!expected_schema->Equals(*table->schema())) {
    // The table doesn't have the expected schema thus we cannot directly
    // convert it to our target representation.
    return arrow::Status::Invalid("Schemas are not matching!");
  }

  // As we have ensured that the table has the expected structure, we can unpack the
  // underlying arrays. For the primitive columns `id` and `components` we can use the
  // high level functions to get the values whereas for the nested column
  // `component_costs` we need to access the C-pointer to the data to copy its
  // contents into the resulting `std::vector<double>`. Here we need to be careful to
  // also add the offset to the pointer. This offset is needed to enable zero-copy
  // slicing operations. While this could be adjusted automatically for double
  // arrays, this cannot be done for the accompanying bitmap as often the slicing
  // border would be inside a byte.

  auto ids = std::static_pointer_cast<arrow::Int64Array>(table->column(0)->chunk(0));
  auto components =
      std::static_pointer_cast<arrow::Int64Array>(table->column(1)->chunk(0));
  auto component_cost =
      std::static_pointer_cast<arrow::ListArray>(table->column(2)->chunk(0));
  auto component_cost_values =
      std::static_pointer_cast<arrow::DoubleArray>(component_cost->values());
  // To enable zero-copy slices, the native values pointer might need to account
  // for this slicing offset. This is not needed for the higher level functions
  // like Value(…) that already account for this offset internally.
  const double* ccv_ptr = component_cost_values->raw_values();
  std::vector<data_row> rows;
  for (int64_t i = 0; i < table->num_rows(); i++) {
    // Another simplification in this example is that we assume that there are
    // no null entries, e.g. each row is fill with valid values.
    int64_t id = ids->Value(i);
    int64_t component = components->Value(i);
    const double* first = ccv_ptr + component_cost->value_offset(i);
    const double* last = ccv_ptr + component_cost->value_offset(i + 1);
    std::vector<double> components_vec(first, last);
    rows.push_back({id, component, components_vec});
  }

  return rows;
}

int main(int argc, char** argv) {
  std::vector<data_row> rows = {
      {1, 1, {10.0}}, {2, 3, {11.0, 12.0, 13.0}}, {3, 2, {15.0, 25.0}}};
  std::shared_ptr<arrow::Table> table;
  std::vector<data_row> expected_rows;

  arrow::Result<std::shared_ptr<arrow::Table>> table_result = VectorToColumnarTable(rows);
  table = std::move(table_result).ValueOrDie();

  arrow::Result<std::vector<data_row>> expected_rows_result =
      ColumnarTableToVector(table);
  expected_rows = std::move(expected_rows_result).ValueOrDie();

  assert(rows.size() == expected_rows.size());

  // Print out contents of table, should get
  // ID Components Component prices
  // 1  1          10
  // 2  3          11  12  13
  // 3  2          15  25
  std::cout << std::left << std::setw(3) << "ID " << std::left << std::setw(11)
            << "Components " << std::left << std::setw(15) << "Component prices "
            << std::endl;
  for (const auto& row : rows) {
    std::cout << std::left << std::setw(3) << row.id << std::left << std::setw(11)
              << row.components;
    for (const auto& cost : row.component_cost) {
      std::cout << std::left << std::setw(4) << cost;
    }
    std::cout << std::endl;
  }
  return EXIT_SUCCESS;
}
