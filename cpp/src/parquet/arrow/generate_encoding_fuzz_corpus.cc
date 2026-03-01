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

// A command line executable that generates a bunch of valid Parquet files
// containing example record batches.  Those are used as fuzzing seeds
// to make fuzzing more efficient.

#include <cstdlib>
#include <iostream>
#include <limits>
#include <memory>
#include <set>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "arrow/array.h"
#include "arrow/io/file.h"
#include "arrow/result.h"
#include "arrow/testing/random.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/io_util.h"
#include "arrow/util/logging_internal.h"
#include "parquet/arrow/fuzz_encoding_internal.h"
#include "parquet/encoding.h"
#include "parquet/schema.h"

namespace arrow {

using internal::checked_cast;

using Encoding = ::parquet::Encoding;
using ParquetType = ::parquet::Type;
using ::parquet::Int96;
using ::parquet::fuzzing::internal::FuzzEncodingHeader;

using EncodingVector = std::vector<Encoding::type>;

struct FullParquetType {
  ParquetType::type type;
  int type_length = -1;

  static FullParquetType FromArrow(const DataType& type) {
    switch (type.id()) {
      case Type::BOOL:
        return {ParquetType::BOOLEAN};
      case Type::INT32:
        return {ParquetType::INT32};
      case Type::INT64:
        return {ParquetType::INT64};
      case Type::FLOAT:
        return {ParquetType::FLOAT};
      case Type::DOUBLE:
        return {ParquetType::DOUBLE};
      case Type::BINARY:
      case Type::STRING:
        return {ParquetType::BYTE_ARRAY};
      case Type::FIXED_SIZE_BINARY:
        return {ParquetType::FIXED_LEN_BYTE_ARRAY,
                checked_cast<const FixedSizeBinaryType&>(type).byte_width()};
      default:
        Status::TypeError("Unsupported Arrow type").Abort();
    }
  }

  std::string ToString() const { return ::parquet::TypeToString(type, type_length); }
};

struct DataWithEncodings {
  FullParquetType pq_type;
  std::shared_ptr<Array> values;
  EncodingVector encodings;
};

Result<std::vector<DataWithEncodings>> SampleData() {
  auto rag = random::RandomArrayGenerator(/*seed=*/42);
  std::vector<DataWithEncodings> all_data;

  auto push_array = [&](std::shared_ptr<Array> arr, EncodingVector encodings = {}) {
    auto pq_type = FullParquetType::FromArrow(*arr->type());
    if (encodings.empty()) {
      encodings = ::parquet::SupportedEncodings(pq_type.type);
    }
    all_data.emplace_back(pq_type, arr, std::move(encodings));
  };

  for (const auto size : {100, 10000}) {
    push_array(rag.Int32(size, /*min=*/0, /*max=*/10));
    push_array(rag.Int32(size, /*min=*/std::numeric_limits<int32_t>::min(),
                         /*max=*/std::numeric_limits<int32_t>::max()));
    push_array(rag.Int64(size, /*min=*/0, /*max=*/0));
    push_array(rag.Int64(size, /*min=*/0, /*max=*/10'000));
    push_array(rag.Int64(size, /*min=*/std::numeric_limits<int64_t>::min(),
                         /*max=*/std::numeric_limits<int64_t>::max()));
    for (const double true_probability : {0.01, 0.5, 0.9}) {
      push_array(rag.Boolean(size, true_probability));
    }
    push_array(rag.Float32(size, /*min=*/-1.0f, /*max=*/1.0f));
    push_array(rag.Float32(size, /*min=*/std::numeric_limits<float>::lowest(),
                           /*max=*/std::numeric_limits<float>::max(),
                           /*null_probability=*/0.0, /*nan_probability=*/1e-2));
    push_array(rag.Float64(size, /*min=*/-1.0, /*max=*/1.0));
    push_array(rag.Float64(size, /*min=*/std::numeric_limits<double>::lowest(),
                           /*max=*/std::numeric_limits<double>::max(),
                           /*null_probability=*/0.0, /*nan_probability=*/1e-2));
    for (const int32_t byte_width : {1, 3, 16}) {
      const auto fsb_size = size / byte_width;
      push_array(rag.FixedSizeBinary(fsb_size, byte_width));
    }
    for (const int32_t max_binary_length : {1, 30}) {
      const auto binary_size = size / max_binary_length;
      push_array(rag.BinaryWithRepeats(binary_size, /*unique=*/binary_size / 2,
                                       /*min_length=*/0,
                                       /*max_length=*/max_binary_length));
    }
  }

  return all_data;
}

using Int96Vector = std::vector<Int96>;

Result<std::vector<Int96Vector>> SampleInt96() {
  auto rag = random::RandomArrayGenerator(/*seed=*/42);
  std::vector<Int96Vector> all_data;

  for (const auto size : {50, 5000}) {
    // The max value is chosen so that the nanoseconds component remains
    // smaller than kNanosecondsPerDay.
    auto ints = rag.Int32(size * 3, /*min=*/0, /*max=*/20000);
    const Int96* int96_data = reinterpret_cast<const Int96*>(
        checked_cast<const Int32Array&>(*ints).raw_values());
    all_data.push_back(Int96Vector(int96_data, int96_data + size));
  }
  return all_data;
}

Status DoMain(const std::string& out_dir) {
  ARROW_ASSIGN_OR_RAISE(auto dir_fn, internal::PlatformFilename::FromString(out_dir));
  RETURN_NOT_OK(internal::CreateDir(dir_fn));

  int sample_num = 1;
  auto sample_file_name = [&](const std::string& name = "") -> std::string {
    std::stringstream ss;
    if (!name.empty()) {
      ss << name << "-";
    }
    ss << sample_num++ << ".pq";
    return std::move(ss).str();
  };

  auto write_sample = [&](Encoding::type source_encoding,
                          Encoding::type roundtrip_encoding, FullParquetType type,
                          const BufferVector& buffers) -> Status {
    std::stringstream ss;
    ss << type.ToString() << "-" << ::parquet::EncodingToString(source_encoding) << "-"
       << ::parquet::EncodingToString(roundtrip_encoding);
    ARROW_ASSIGN_OR_RAISE(auto sample_fn, dir_fn.Join(sample_file_name(ss.str())));
    std::cerr << sample_fn.ToString() << std::endl;
    ARROW_ASSIGN_OR_RAISE(auto file, io::FileOutputStream::Open(sample_fn.ToString()));
    for (const auto& buf : buffers) {
      RETURN_NOT_OK(file->Write(buf));
    }
    return file->Close();
  };

  ARROW_ASSIGN_OR_RAISE(auto all_data, SampleData());
  for (const auto& data : all_data) {
    const auto descr = parquet::fuzzing::internal::MakeColumnDescriptor(
        data.pq_type.type, data.pq_type.type_length);
    for (const auto roundtrip_encoding : data.encodings) {
      // We always add PLAIN as a source encoding because the fuzzer might be able
      // to explore more search space using this encoding.
      for (const auto source_encoding : std::set{Encoding::PLAIN, roundtrip_encoding}) {
        BufferVector buffers;
        FuzzEncodingHeader header(
            source_encoding, roundtrip_encoding, &descr, /*num_values=*/
            static_cast<int>(data.values->length() - data.values->null_count()));

        buffers.push_back(Buffer::FromString(header.Serialize()));
        BEGIN_PARQUET_CATCH_EXCEPTIONS
        auto encoder = ::parquet::MakeEncoder(data.pq_type.type, source_encoding,
                                              /*use_dictionary=*/false, &descr);
        encoder->Put(*data.values);
        buffers.push_back(encoder->FlushValues());
        END_PARQUET_CATCH_EXCEPTIONS
        RETURN_NOT_OK(
            write_sample(source_encoding, roundtrip_encoding, data.pq_type, buffers));
      }
    }
  }

  // Special-case INT96 as there is no direct Arrow equivalent to encode.
  ARROW_ASSIGN_OR_RAISE(auto int96_data, SampleInt96());
  for (const auto& data : int96_data) {
    const auto pq_type = FullParquetType{ParquetType::INT96};
    const auto descr = parquet::fuzzing::internal::MakeColumnDescriptor(
        pq_type.type, pq_type.type_length);
    const auto source_encoding = Encoding::PLAIN;
    const auto roundtrip_encoding = Encoding::PLAIN;

    BufferVector buffers;
    FuzzEncodingHeader header(source_encoding, roundtrip_encoding, &descr, /*num_values=*/
                              static_cast<int>(data.size()));
    buffers.push_back(Buffer::FromString(header.Serialize()));
    BEGIN_PARQUET_CATCH_EXCEPTIONS
    auto encoder = ::parquet::MakeEncoder(pq_type.type, source_encoding,
                                          /*use_dictionary=*/false, &descr);
    dynamic_cast<::parquet::Int96Encoder*>(encoder.get())->Put(data);
    buffers.push_back(encoder->FlushValues());
    END_PARQUET_CATCH_EXCEPTIONS
    RETURN_NOT_OK(write_sample(source_encoding, roundtrip_encoding, pq_type, buffers));
  }

  return Status::OK();
}

ARROW_NORETURN void Usage() {
  std::cerr << "Usage: parquet-generate-encoding-fuzz-corpus "
            << "<output directory>" << std::endl;
  std::exit(2);
}

int Main(int argc, char** argv) {
  if (argc != 2) {
    Usage();
  }
  auto out_dir = std::string(argv[1]);

  Status st = DoMain(out_dir);
  if (!st.ok()) {
    std::cerr << st.ToString() << std::endl;
    return 1;
  }
  return 0;
}

}  // namespace arrow

int main(int argc, char** argv) { return ::arrow::Main(argc, argv); }
