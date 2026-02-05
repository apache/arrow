// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

#include <arrow/io/file.h>
#include <arrow/util/logging.h>
#include <parquet/api/writer.h>
#include <parquet/types.h>

#include <cctype>
#include <filesystem>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

namespace {

constexpr int64_t kNumRows = 1024;
constexpr char kColumnName[] = "value_f64";

struct EncodingSelection {
  parquet::Encoding::type encoding = parquet::Encoding::UNKNOWN;
  bool requires_dictionary = false;
};

std::string NormalizeEncodingName(const std::string& raw) {
  std::string normalized;
  normalized.reserve(raw.size());
  for (char ch : raw) {
    if (ch == '-' || ch == ' ') {
      normalized.push_back('_');
    } else {
      normalized.push_back(static_cast<char>(std::toupper(static_cast<unsigned char>(ch))));
    }
  }
  return normalized;
}

EncodingSelection ParseEncoding(const std::string& raw_name) {
  const std::string name = NormalizeEncodingName(raw_name);
  if (name == "PLAIN") {
    return {parquet::Encoding::PLAIN, false};
  }
  if (name == "BYTE_STREAM_SPLIT" || name == "BYTESTREAMSPLIT") {
    return {parquet::Encoding::BYTE_STREAM_SPLIT, false};
  }
  if (name == "ALP") {
    return {parquet::Encoding::ALP, false};
  }
  if (name == "RLE_DICTIONARY") {
    return {parquet::Encoding::RLE_DICTIONARY, true};
  }
  if (name == "PLAIN_DICTIONARY") {
    return {parquet::Encoding::PLAIN_DICTIONARY, true};
  }
  return {};
}

void PrintUsage() {
  std::cerr << "Usage: write_parquet --encoding <encoding_name> <output_directory>\n"
            << "Supported encodings for DOUBLE: PLAIN, BYTE_STREAM_SPLIT, ALP,\n"
            << "  RLE_DICTIONARY, PLAIN_DICTIONARY\n";
}

std::shared_ptr<parquet::schema::GroupNode> MakeSchema() {
  parquet::schema::NodeVector fields;
  fields.push_back(parquet::schema::PrimitiveNode::Make(
      kColumnName, parquet::Repetition::REQUIRED, parquet::Type::DOUBLE,
      parquet::ConvertedType::NONE));
  return std::static_pointer_cast<parquet::schema::GroupNode>(
      parquet::schema::GroupNode::Make("schema", parquet::Repetition::REQUIRED, fields));
}

std::vector<double> MakeValues(bool dictionary_friendly) {
  std::vector<double> values;
  values.reserve(kNumRows);
  if (dictionary_friendly) {
    for (int64_t i = 0; i < kNumRows; ++i) {
      values.push_back(static_cast<double>(i % 8));
    }
  } else {
    for (int64_t i = 0; i < kNumRows; ++i) {
      values.push_back(0.125 + static_cast<double>(i) * 0.25);
    }
  }
  return values;
}

}  // namespace

int main(int argc, char** argv) {
  if (argc != 4 || std::string(argv[1]) != "--encoding") {
    PrintUsage();
    return 2;
  }

  const std::string encoding_name = argv[2];
  const std::filesystem::path output_dir(argv[3]);

  if (!std::filesystem::exists(output_dir) || !std::filesystem::is_directory(output_dir)) {
    std::cerr << "Output directory does not exist or is not a directory: "
              << output_dir.string() << "\n";
    return 2;
  }

  EncodingSelection selection = ParseEncoding(encoding_name);
  if (selection.encoding == parquet::Encoding::UNKNOWN) {
    std::cerr << "Unsupported encoding: " << encoding_name << "\n";
    PrintUsage();
    return 2;
  }

  try {
    std::string normalized = NormalizeEncodingName(encoding_name);
    std::filesystem::path out_path =
        output_dir / ("single_f64_" + normalized + ".parquet");

    std::shared_ptr<arrow::io::FileOutputStream> out_file;
    PARQUET_ASSIGN_OR_THROW(out_file, arrow::io::FileOutputStream::Open(out_path.string()));

    auto schema = MakeSchema();

    parquet::WriterProperties::Builder builder;
    builder.compression(parquet::Compression::UNCOMPRESSED);

    if (selection.requires_dictionary) {
      builder.enable_dictionary();
      builder.encoding(parquet::Encoding::PLAIN);
    } else {
      builder.disable_dictionary();
      builder.encoding(selection.encoding);
    }

    std::shared_ptr<parquet::WriterProperties> props = builder.build();

    std::shared_ptr<parquet::ParquetFileWriter> file_writer =
        parquet::ParquetFileWriter::Open(out_file, schema, props);

    parquet::RowGroupWriter* row_group_writer = file_writer->AppendRowGroup();
    auto* double_writer =
        static_cast<parquet::DoubleWriter*>(row_group_writer->NextColumn());

    std::vector<double> values = MakeValues(selection.requires_dictionary);
    double_writer->WriteBatch(static_cast<int64_t>(values.size()), nullptr, nullptr,
                              values.data());

    file_writer->Close();
    ARROW_DCHECK(out_file->Close().ok());

    std::cout << "Wrote " << out_path.string() << "\n";
    std::cout << "Requested encoding: " << normalized << "\n";
  } catch (const std::exception& e) {
    std::cerr << "Parquet write error: " << e.what() << "\n";
    return 1;
  }

  return 0;
}
