#include <arrow/api.h>
#include <arrow/csv/api.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>

#include <iostream>

arrow::Status gen_initial_file(){
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

  std::vector<std::shared_ptr<arrow::Array>> columns = {days, months, years};

  std::shared_ptr<arrow::Field> field_day, field_month, field_year;
  std::shared_ptr<arrow::Schema> schema;

  field_day = arrow::field("Day", arrow::int8());
  field_month = arrow::field("Month", arrow::int8());
  field_year = arrow::field("Year", arrow::int16());

  schema = arrow::schema({field_day, field_month, field_year});

  std::shared_ptr<arrow::Table> table; 
  table = arrow::Table::Make(schema, columns);

  std::shared_ptr<arrow::io::FileOutputStream> outfile;
  ARROW_ASSIGN_OR_RAISE(outfile, arrow::io::FileOutputStream::Open("test_in.ipc"));
  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::ipc::RecordBatchWriter> ipc_writer,
                        arrow::ipc::MakeFileWriter(outfile, schema));
  ARROW_RETURN_NOT_OK(ipc_writer->WriteTable(*table));
  ARROW_RETURN_NOT_OK(ipc_writer->Close());

  ARROW_ASSIGN_OR_RAISE(outfile, arrow::io::FileOutputStream::Open("test_in.csv"));
  ARROW_ASSIGN_OR_RAISE(auto csv_writer, arrow::csv::MakeCSVWriter(
          outfile, table->schema()));
  ARROW_RETURN_NOT_OK(csv_writer->WriteTable(*table));
  ARROW_RETURN_NOT_OK(csv_writer->Close());

  ARROW_ASSIGN_OR_RAISE(outfile, arrow::io::FileOutputStream::Open("test_in.parquet"));
  PARQUET_THROW_NOT_OK(parquet::arrow::WriteTable(*table,
                                                  arrow::default_memory_pool(),
                                                  outfile, 5));

  return arrow::Status::OK();
}

arrow::Status RunMain(int argc, char** argv) {
 
  ARROW_RETURN_NOT_OK(gen_initial_file());
 
  // Saving and Loading Tables

  std::shared_ptr<arrow::io::ReadableFile> infile;
  ARROW_ASSIGN_OR_RAISE(infile,
                          arrow::io::ReadableFile::Open("test_in.ipc",
                                                        arrow::default_memory_pool()));

  ARROW_ASSIGN_OR_RAISE(auto ipc_reader,
                        arrow::ipc::RecordBatchFileReader::Open(infile));
  std::shared_ptr<arrow::RecordBatch> rbatch;
  ARROW_ASSIGN_OR_RAISE(rbatch, ipc_reader->ReadRecordBatch(0));

  std::shared_ptr<arrow::io::FileOutputStream> outfile;
  ARROW_ASSIGN_OR_RAISE(outfile, arrow::io::FileOutputStream::Open("test_out.ipc"));
  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::ipc::RecordBatchWriter> ipc_writer,
                        arrow::ipc::MakeFileWriter(outfile,
                                                   rbatch->schema()));
  ARROW_RETURN_NOT_OK(ipc_writer->WriteRecordBatch(*rbatch));
  ARROW_RETURN_NOT_OK(ipc_writer->Close());
  
  ARROW_ASSIGN_OR_RAISE(infile, arrow::io::ReadableFile::Open("test_in.csv"));
  std::shared_ptr<arrow::Table> csv_table;
  ARROW_ASSIGN_OR_RAISE(auto csv_reader,
                        arrow::csv::TableReader::Make(
                                arrow::io::default_io_context(),
                                infile, arrow::csv::ReadOptions::Defaults(),
                                arrow::csv::ParseOptions::Defaults(),
                                arrow::csv::ConvertOptions::Defaults()));
  ARROW_ASSIGN_OR_RAISE(csv_table, csv_reader->Read())

  ARROW_ASSIGN_OR_RAISE(outfile, arrow::io::FileOutputStream::Open("test_out.csv"));
  ARROW_ASSIGN_OR_RAISE(auto csv_writer, arrow::csv::MakeCSVWriter(
          outfile, csv_table->schema()));
  ARROW_RETURN_NOT_OK(csv_writer->WriteTable(*csv_table));
  ARROW_RETURN_NOT_OK(csv_writer->Close());

  std::unique_ptr<parquet::arrow::FileReader> reader;
  ARROW_ASSIGN_OR_RAISE(infile, arrow::io::ReadableFile::Open("test_in.parquet"));
  PARQUET_THROW_NOT_OK(
      parquet::arrow::OpenFile(infile, arrow::default_memory_pool(),
                               &reader));
  std::shared_ptr<arrow::Table> parquet_table;
  PARQUET_THROW_NOT_OK(reader->ReadTable(&parquet_table));

  ARROW_ASSIGN_OR_RAISE(outfile, arrow::io::FileOutputStream::Open(
          "test_out.parquet"));
  PARQUET_THROW_NOT_OK(parquet::arrow::WriteTable(*parquet_table,
                                                  arrow::default_memory_pool(),
                                                  outfile, 5));
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
