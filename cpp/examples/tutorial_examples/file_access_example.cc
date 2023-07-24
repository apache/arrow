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

// (Doc section: File I/O)

// (Doc section: Includes)
#include <arrow/api.h>
#include <arrow/csv/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>

#include <iostream>
// (Doc section: Includes)

// (Doc section: GenInitialFile)
arrow::Status GenInitialFile() {
  // Make a couple 8-bit integer arrays and a 16-bit integer array -- just like
  // basic Arrow example.
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

  // Get a vector of our Arrays
  std::vector<std::shared_ptr<arrow::Array>> columns = {days, months, years};

  // Make a schema to initialize the Table with
  std::shared_ptr<arrow::Field> field_day, field_month, field_year;
  std::shared_ptr<arrow::Schema> schema;

  field_day = arrow::field("Day", arrow::int8());
  field_month = arrow::field("Month", arrow::int8());
  field_year = arrow::field("Year", arrow::int16());

  schema = arrow::schema({field_day, field_month, field_year});
  // With the schema and data, create a Table
  std::shared_ptr<arrow::Table> table;
  table = arrow::Table::Make(schema, columns);

  // Write out test files in IPC, CSV, and Parquet for the example to use.
  std::shared_ptr<arrow::io::FileOutputStream> outfile;
  ARROW_ASSIGN_OR_RAISE(outfile, arrow::io::FileOutputStream::Open("test_in.arrow"));
  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::ipc::RecordBatchWriter> ipc_writer,
                        arrow::ipc::MakeFileWriter(outfile, schema));
  ARROW_RETURN_NOT_OK(ipc_writer->WriteTable(*table));
  ARROW_RETURN_NOT_OK(ipc_writer->Close());

  ARROW_ASSIGN_OR_RAISE(outfile, arrow::io::FileOutputStream::Open("test_in.csv"));
  ARROW_ASSIGN_OR_RAISE(auto csv_writer,
                        arrow::csv::MakeCSVWriter(outfile, table->schema()));
  ARROW_RETURN_NOT_OK(csv_writer->WriteTable(*table));
  ARROW_RETURN_NOT_OK(csv_writer->Close());

  ARROW_ASSIGN_OR_RAISE(outfile, arrow::io::FileOutputStream::Open("test_in.parquet"));
  PARQUET_THROW_NOT_OK(
      parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), outfile, 5));

  return arrow::Status::OK();
}
// (Doc section: GenInitialFile)

// (Doc section: RunMain)
arrow::Status RunMain() {
  // (Doc section: RunMain)
  // (Doc section: Gen Files)
  // Generate initial files for each format with a helper function -- don't worry,
  // we'll also write a table in this example.
  ARROW_RETURN_NOT_OK(GenInitialFile());
  // (Doc section: Gen Files)

  // (Doc section: ReadableFile Definition)
  // First, we have to set up a ReadableFile object, which just lets us point our
  // readers to the right data on disk. We'll be reusing this object, and rebinding
  // it to multiple files throughout the example.
  std::shared_ptr<arrow::io::ReadableFile> infile;
  // (Doc section: ReadableFile Definition)
  // (Doc section: Arrow ReadableFile Open)
  // Get "test_in.arrow" into our file pointer
  ARROW_ASSIGN_OR_RAISE(infile, arrow::io::ReadableFile::Open(
                                    "test_in.arrow", arrow::default_memory_pool()));
  // (Doc section: Arrow ReadableFile Open)
  // (Doc section: Arrow Read Open)
  // Open up the file with the IPC features of the library, gives us a reader object.
  ARROW_ASSIGN_OR_RAISE(auto ipc_reader, arrow::ipc::RecordBatchFileReader::Open(infile));
  // (Doc section: Arrow Read Open)
  // (Doc section: Arrow Read)
  // Using the reader, we can read Record Batches. Note that this is specific to IPC;
  // for other formats, we focus on Tables, but here, RecordBatches are used.
  std::shared_ptr<arrow::RecordBatch> rbatch;
  ARROW_ASSIGN_OR_RAISE(rbatch, ipc_reader->ReadRecordBatch(0));
  // (Doc section: Arrow Read)

  // (Doc section: Arrow Write Open)
  // Just like with input, we get an object for the output file.
  std::shared_ptr<arrow::io::FileOutputStream> outfile;
  // Bind it to "test_out.arrow"
  ARROW_ASSIGN_OR_RAISE(outfile, arrow::io::FileOutputStream::Open("test_out.arrow"));
  // (Doc section: Arrow Write Open)
  // (Doc section: Arrow Writer)
  // Set up a writer with the output file -- and the schema! We're defining everything
  // here, loading to fire.
  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::ipc::RecordBatchWriter> ipc_writer,
                        arrow::ipc::MakeFileWriter(outfile, rbatch->schema()));
  // (Doc section: Arrow Writer)
  // (Doc section: Arrow Write)
  // Write the record batch.
  ARROW_RETURN_NOT_OK(ipc_writer->WriteRecordBatch(*rbatch));
  // (Doc section: Arrow Write)
  // (Doc section: Arrow Close)
  // Specifically for IPC, the writer needs to be explicitly closed.
  ARROW_RETURN_NOT_OK(ipc_writer->Close());
  // (Doc section: Arrow Close)

  // (Doc section: CSV Read Open)
  // Bind our input file to "test_in.csv"
  ARROW_ASSIGN_OR_RAISE(infile, arrow::io::ReadableFile::Open("test_in.csv"));
  // (Doc section: CSV Read Open)
  // (Doc section: CSV Table Declare)
  std::shared_ptr<arrow::Table> csv_table;
  // (Doc section: CSV Table Declare)
  // (Doc section: CSV Reader Make)
  // The CSV reader has several objects for various options. For now, we'll use defaults.
  ARROW_ASSIGN_OR_RAISE(
      auto csv_reader,
      arrow::csv::TableReader::Make(
          arrow::io::default_io_context(), infile, arrow::csv::ReadOptions::Defaults(),
          arrow::csv::ParseOptions::Defaults(), arrow::csv::ConvertOptions::Defaults()));
  // (Doc section: CSV Reader Make)
  // (Doc section: CSV Read)
  // Read the table.
  ARROW_ASSIGN_OR_RAISE(csv_table, csv_reader->Read())
  // (Doc section: CSV Read)

  // (Doc section: CSV Write)
  // Bind our output file to "test_out.csv"
  ARROW_ASSIGN_OR_RAISE(outfile, arrow::io::FileOutputStream::Open("test_out.csv"));
  // The CSV writer has simpler defaults, review API documentation for more complex usage.
  ARROW_ASSIGN_OR_RAISE(auto csv_writer,
                        arrow::csv::MakeCSVWriter(outfile, csv_table->schema()));
  ARROW_RETURN_NOT_OK(csv_writer->WriteTable(*csv_table));
  // Not necessary, but a safe practice.
  ARROW_RETURN_NOT_OK(csv_writer->Close());
  // (Doc section: CSV Write)

  // (Doc section: Parquet Read Open)
  // Bind our input file to "test_in.parquet"
  ARROW_ASSIGN_OR_RAISE(infile, arrow::io::ReadableFile::Open("test_in.parquet"));
  // (Doc section: Parquet Read Open)
  // (Doc section: Parquet FileReader)
  std::unique_ptr<parquet::arrow::FileReader> reader;
  // (Doc section: Parquet FileReader)
  // (Doc section: Parquet OpenFile)
  // Note that Parquet's OpenFile() takes the reader by reference, rather than returning
  // a reader.
  PARQUET_THROW_NOT_OK(
      parquet::arrow::OpenFile(infile, arrow::default_memory_pool(), &reader));
  // (Doc section: Parquet OpenFile)

  // (Doc section: Parquet Read)
  std::shared_ptr<arrow::Table> parquet_table;
  // Read the table.
  PARQUET_THROW_NOT_OK(reader->ReadTable(&parquet_table));
  // (Doc section: Parquet Read)

  // (Doc section: Parquet Write)
  // Parquet writing does not need a declared writer object. Just get the output
  // file bound, then pass in the table, memory pool, output, and chunk size for
  // breaking up the Table on-disk.
  ARROW_ASSIGN_OR_RAISE(outfile, arrow::io::FileOutputStream::Open("test_out.parquet"));
  PARQUET_THROW_NOT_OK(parquet::arrow::WriteTable(
      *parquet_table, arrow::default_memory_pool(), outfile, 5));
  // (Doc section: Parquet Write)
  // (Doc section: Return)
  return arrow::Status::OK();
}
// (Doc section: Return)

// (Doc section: Main)
int main() {
  arrow::Status st = RunMain();
  if (!st.ok()) {
    std::cerr << st << std::endl;
    return 1;
  }
  return 0;
}
// (Doc section: Main)
// (Doc section: File I/O)
