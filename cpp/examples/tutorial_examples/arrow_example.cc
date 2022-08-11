#include <arrow/api.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/table.h>

#include <iostream>

using arrow::Status;

namespace { 

Status RunMain(int argc, char** argv) {

  // Creating Arrays and Tables
  arrow::Int8Builder int8builder; 
  int8_t days_raw[5] = {1, 12, 17, 23, 28};
  ARROW_RETURN_NOT_OK(int8builder.AppendValues(days_raw, 5));
  std::shared_ptr<arrow::Array> days;
  ARROW_ASSIGN_OR_RAISE(days, int8builder.Finish());

  int8_t months_raw[5] = {1, 3, 5, 7, 1};
  ARROW_RETURN_NOT_OK(int8builder.AppendValues(months_raw, 5));
  std::shared_ptr<arrow::Array> months;
  ARROW_ASSIGN_OR_RAISE(months, int8builder.Finish());

  arrow::Int16Builder int16builder;
  int16_t years_raw[5] = {1990, 2000, 1995, 2000, 1995};
  ARROW_RETURN_NOT_OK(int16builder.AppendValues(years_raw, 5));
  std::shared_ptr<arrow::Array> years;
  ARROW_ASSIGN_OR_RAISE(years, int16builder.Finish());

  std::shared_ptr<arrow::Field> field_day, field_month, field_year;
  std::shared_ptr<arrow::Schema> schema;

  field_day = arrow::field("Day", arrow::int8());
  field_month = arrow::field("Month", arrow::int8());
  field_year = arrow::field("Year", arrow::int16());

  schema = arrow::schema({field_day, field_month, field_year});

  std::shared_ptr<arrow::RecordBatch> rbatch;
  rbatch = arrow::RecordBatch::Make(schema, days->length(), {days, months, years});

  std::cout << rbatch->ToString();

  int8_t days_raw2[5] = {6, 12, 3, 30, 22};
  ARROW_RETURN_NOT_OK(int8builder.AppendValues(days_raw2, 5));
  std::shared_ptr<arrow::Array> days2;
  ARROW_ASSIGN_OR_RAISE(days2, int8builder.Finish());

  int8_t months_raw2[5] = {5, 4, 11, 3, 2};
  ARROW_RETURN_NOT_OK(int8builder.AppendValues(months_raw2, 5));
  std::shared_ptr<arrow::Array> months2;
  ARROW_ASSIGN_OR_RAISE(months2, int8builder.Finish());

  int16_t years_raw2[5] = {1980, 2001, 1915, 2020, 1996};
  ARROW_RETURN_NOT_OK(int16builder.AppendValues(years_raw2, 5));
  std::shared_ptr<arrow::Array> years2;
  ARROW_ASSIGN_OR_RAISE(years2, int16builder.Finish());

  arrow::ArrayVector day_vecs(2);
  day_vecs[0] = days;
  day_vecs[1] = days2;
  std::shared_ptr<arrow::ChunkedArray> day_chunks =
          std::make_shared<arrow::ChunkedArray>(day_vecs);

  arrow::ArrayVector month_vecs(2);
  month_vecs[0] = months;
  month_vecs[1] = months2;
  std::shared_ptr<arrow::ChunkedArray> month_chunks =
          std::make_shared<arrow::ChunkedArray>(month_vecs);

  arrow::ArrayVector year_vecs(2);
  year_vecs[0] = years;
  year_vecs[1] = years2;
  std::shared_ptr<arrow::ChunkedArray> year_chunks =
          std::make_shared<arrow::ChunkedArray>(year_vecs);

  std::shared_ptr<arrow::Table> table; 
  table = arrow::Table::Make(schema, {day_chunks, month_chunks, year_chunks}, 10);

  std::cout << table->ToString();

  return Status::OK();
}
}
int main(int argc, char** argv) {
  Status st = RunMain(argc, argv);
  if (!st.ok()) {
    std::cerr << st << std::endl;
    return 1;
  }
  return 0;
}
