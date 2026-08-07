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

/*
cd cpp
cmake -S . -B build \
  -DCMAKE_BUILD_TYPE=Release \
  -DARROW_PARQUET=ON \
  -DPARQUET_BUILD_EXAMPLES=ON \
  -DARROW_WITH_ZSTD=ON
cmake --build build --target parquet-write-parquet
 */

// Generates the ALP example/test parquet file described in
// https://github.com/apache/parquet-testing/issues/105#issuecomment-5172111570
//
// Build and run
// -------------
// Configure an Arrow C++ build with Parquet, the Parquet examples, and ZSTD
// (ZSTD is used to compress the PLAIN reference columns):
//
//   cd cpp
//   cmake -S . -B build \
//     -DCMAKE_BUILD_TYPE=Release \
//     -DARROW_PARQUET=ON \
//     -DPARQUET_BUILD_EXAMPLES=ON \
//     -DARROW_WITH_ZSTD=ON
//   cmake --build build --target parquet-write-parquet
//
// Then run it with the directory the file should be written to:
//
//   mkdir -p /tmp/alp_out
//   ./build/release/parquet-write-parquet /tmp/alp_out
//
// This writes /tmp/alp_out/alp_extended.zstd.parquet, prints the ALP page
// structure of every ALP column (header, vectors, exceptions), verifies the
// file decodes bit-exactly, and exits non-zero if the layout does not match
// the expectations from the issue comment.
//
// The file contains the same 9032 logical rows in several columns:
//   float_plain,     double_plain     PLAIN + ZSTD (in-file reference values)
//   float_alp_1024,  double_alp_1024  ALP, 1024-value vectors (default)
//   float_alp_4096,  double_alp_4096  ALP, 4096-value vectors
//   float_alp_32,    double_alp_32    ALP, 32-value vectors
//
// The rows are split into five row groups so that each region with a
// distinct value distribution is sampled on its own; in particular the
// 4-decimal-digit range gets a different ALP exponent/factor than the
// 2-decimal base data. (Mixing regions in one row group lets one region
// dominate the sampled encoding preset, which produces spurious exceptions
// in the other regions.)
//   row group 0: rows 0-6143     (base data + special values + random ranges;
//                                 the NaNs include non-canonical payloads)
//   row group 1: rows 6144-7167  (4 decimal digits => different exponent/factor)
//   row group 2: rows 7168-8191  (constant vector, bit_width = 0)
//   row group 3: rows 8192-8999  (partial vector with nulls)
//   row group 4: rows 9000-9031 (large-magnitude values => 64-bit FOR bit
//                                 width for doubles, all-exception vector for
//                                 floats)
//
// After writing, the tool re-opens the file and prints, for every ALP
// column and row group, the ALP page header, the number of vectors, and
// per-vector metadata (exponent/factor/bit_width/frame_of_reference and
// the number of exceptions plus the first few exception values). Finally
// it checks the actual layout against the expectations from the issue
// comment and verifies that every column decodes to the generated values.

#include <arrow/io/file.h>
#include <arrow/util/compression.h>
#include <parquet/api/reader.h>
#include <parquet/api/writer.h>
#include <parquet/column_page.h>

#include <cmath>
#include <cstring>
#include <filesystem>
#include <iomanip>
#include <iostream>
#include <limits>
#include <memory>
#include <random>
#include <sstream>
#include <string>
#include <vector>

namespace {

constexpr int64_t kNumRows = 9032;
constexpr uint32_t kSeed = 42;
constexpr char kFileName[] = "alp_extended.zstd.parquet";

// Row group boundaries; see the comment at the top of the file.
const std::vector<int64_t> kRowGroupBoundaries = {0, 6144, 7168, 8192, 9000, kNumRows};

// Bounds of the large-magnitude range (rows 9000-9031). Every double with
// magnitude >= 2^53 is integer-valued, so these values are losslessly
// encodable with exponent=0/factor=0, and the pinned endpoints force a FOR
// range of 1.6e19 >= 2^63, i.e. a 64-bit FOR bit width. The magnitude stays
// below the double encoder's limit (~9.22e18, int64 range).
constexpr double kBigMagnitude = 8.0e18;

// ----------------------------------------------------------------------
// Mirror of the ALP per-value encode/decode arithmetic
// (AlpInlines<T>::EncodeValue / DecodeValue in arrow/util/alp/alp.cc).
//
// Used to pre-filter generated "base" values so that they round trip
// losslessly (produce no ALP exceptions). Notably, many exact 2-decimal
// floats do NOT round trip: e.g. 2.38f is stored as 2.3800001144...f and
// decoding computes 238 * 10^4 * 1e-6f = 2.3799998760f (1 ulp off),
// because the power-of-ten multipliers are themselves inexact in float.

template <typename T>
struct AlpMirrorConstants;

template <>
struct AlpMirrorConstants<float> {
  using SignedExact = int32_t;
  static constexpr float kMagicNumber = 12582912.0f;  // 2^22 + 2^23
  static constexpr float kEncodingUpperLimit = 2147483520.0f;
  static constexpr float kEncodingLowerLimit = -2147483520.0f;
};

template <>
struct AlpMirrorConstants<double> {
  using SignedExact = int64_t;
  static constexpr double kMagicNumber = 6755399441055744.0;  // 2^51 + 2^52
  static constexpr double kEncodingUpperLimit = 9223372036854774784.0;
  static constexpr double kEncodingLowerLimit = -9223372036854774784.0;
};

template <typename T>
T PowerOfTen(int power) {
  T result = 1;
  for (int i = 0; i < std::abs(power); ++i) {
    result *= 10;
  }
  return power >= 0 ? result : T{1} / result;
}

// Returns true if `value` survives ALP encode+decode bit-exactly with the
// given exponent/factor combination.
template <typename T>
bool AlpRoundTrips(T value, int exponent, int factor) {
  using Constants = AlpMirrorConstants<T>;
  using SignedExact = typename Constants::SignedExact;

  const T encoded = value * PowerOfTen<T>(exponent) * PowerOfTen<T>(-factor);
  if (std::isnan(encoded) || encoded > Constants::kEncodingUpperLimit ||
      encoded < Constants::kEncodingLowerLimit ||
      (encoded == 0.0 && std::signbit(encoded))) {
    return false;
  }
  T rounded = encoded;
  if (rounded >= 0) {
    rounded = rounded + Constants::kMagicNumber - Constants::kMagicNumber;
  } else {
    rounded = rounded - Constants::kMagicNumber + Constants::kMagicNumber;
  }
  const SignedExact integer = static_cast<SignedExact>(rounded);
  const int64_t factor_multiplier = static_cast<int64_t>(PowerOfTen<double>(factor));
  const T decoded = static_cast<T>(integer) * static_cast<T>(factor_multiplier) *
                    PowerOfTen<T>(-exponent);
  return decoded == value;
}

// The exponent/factor combinations the encoder empirically selects for the
// base data (they have the fewest float round-trip failures, so once the
// data is filtered to round trip under them they become exception-free and
// the encoder's cost model picks them). If the encoder ever selects a
// different combination, the expectation checker below will flag it.
constexpr int kFloat2DecExponent = 6, kFloat2DecFactor = 4;
constexpr int kFloat4DecExponent = 6, kFloat4DecFactor = 2;
constexpr int kDouble2DecExponent = 14, kDouble2DecFactor = 12;
constexpr int kDouble4DecExponent = 16, kDouble4DecFactor = 12;

// ----------------------------------------------------------------------
// Data generation (see the row-range table in the issue comment)

struct GeneratedData {
  // Logical values for all kNumRows rows; entries where valid[i] == false
  // are nulls (their value entries are unused).
  std::vector<double> doubles;
  std::vector<float> floats;
  std::vector<bool> valid;
};

GeneratedData MakeData() {
  GeneratedData data;
  data.doubles.resize(kNumRows);
  data.floats.resize(kNumRows);
  data.valid.assign(kNumRows, true);

  std::mt19937 gen(kSeed);
  // base distribution: [-10.00, 10.00] with exactly 2 decimal digits
  std::uniform_int_distribution<int> cents(-1000, 1000);
  // 4 decimal digit variant for rows 6144-7167
  std::uniform_int_distribution<int> ten_thousandths(-100000, 100000);
  // full-mantissa random values
  std::uniform_real_distribution<double> full(-10.0, 10.0);
  // large-magnitude values for the 64-bit-FOR row group (rows 9000-9031)
  std::uniform_real_distribution<double> big(-kBigMagnitude, kBigMagnitude);

  // Base values must produce no ALP exceptions, so redraw until both the
  // float and the double representation round trip (~4% of 2-decimal and
  // ~3% of 4-decimal floats do not; doubles virtually always do).
  auto set_base2 = [&](int64_t i) {
    while (true) {
      const int c = cents(gen);
      const double d = static_cast<double>(c) / 100.0;
      const float f = static_cast<float>(c) / 100.0f;
      if (AlpRoundTrips(f, kFloat2DecExponent, kFloat2DecFactor) &&
          AlpRoundTrips(d, kDouble2DecExponent, kDouble2DecFactor)) {
        data.doubles[i] = d;
        data.floats[i] = f;
        return;
      }
    }
  };
  auto set_base4 = [&](int64_t i) {
    while (true) {
      const int c = ten_thousandths(gen);
      const double d = static_cast<double>(c) / 10000.0;
      const float f = static_cast<float>(c) / 10000.0f;
      if (AlpRoundTrips(f, kFloat4DecExponent, kFloat4DecFactor) &&
          AlpRoundTrips(d, kDouble4DecExponent, kDouble4DecFactor)) {
        data.doubles[i] = d;
        data.floats[i] = f;
        return;
      }
    }
  };
  auto set_full = [&](int64_t i) {
    const double v = full(gen);
    data.doubles[i] = v;
    data.floats[i] = static_cast<float>(v);
  };

  for (int64_t i = 0; i < kNumRows; ++i) {
    if (i < 4096) {
      set_base2(i);  // special values patched below
    } else if (i < 5120) {
      // every 2nd value full-mantissa, rest base
      if (i % 2 == 0) {
        set_full(i);
      } else {
        set_base2(i);
      }
    } else if (i < 6144) {
      set_full(i);  // all exceptions
    } else if (i < 7168) {
      set_base4(i);  // different exponent/factor (own row group)
    } else if (i < 8192) {
      data.doubles[i] = 7.77;  // constant vector, bit_width = 0
      data.floats[i] = 7.77f;
    } else if (i < 9000) {
      set_base2(i);
      if (i % 100 == 0) {
        data.valid[i] = false;
      }
    } else {
      // rows 9000-9031: large-magnitude integer-valued doubles requiring a
      // 64-bit FOR bit width (see kBigMagnitude). The same values overflow
      // the float encoder's int32 range, so the float columns store this
      // vector entirely as exceptions.
      while (true) {
        const double v = big(gen);
        // Redraw the (rare) small draws that are not integer-valued and
        // would therefore not round trip under exponent=0/factor=0.
        if (AlpRoundTrips(v, 0, 0)) {
          data.doubles[i] = v;
          data.floats[i] = static_cast<float>(v);
          break;
        }
      }
    }
  }

  auto set_both = [&](int64_t i, double d, float f) {
    data.doubles[i] = d;
    data.floats[i] = f;
  };

  // rows 1024-2047: NaN / Inf / -0.0 / subnormal edge values
  //
  // The three NaNs use distinct bit patterns so that readers are checked
  // for preserving non-canonical NaN payloads (they are stored bit-exactly
  // as ALP exceptions): a canonical quiet NaN, a quiet NaN with a payload,
  // and a negative quiet NaN with a payload. Signaling NaNs are deliberately
  // not used: platforms/languages (e.g. the JVM) are allowed to quieten
  // them, which would make cross-implementation bit-exact comparison flaky.
  auto double_from_bits = [](uint64_t bits) {
    double d;
    std::memcpy(&d, &bits, sizeof(d));
    return d;
  };
  auto float_from_bits = [](uint32_t bits) {
    float f;
    std::memcpy(&f, &bits, sizeof(f));
    return f;
  };
  // canonical quiet NaN; first element of the (1024-sized) vector
  set_both(1024, double_from_bits(0x7FF8000000000000ULL),
           float_from_bits(0x7FC00000U));
  // quiet NaN with a non-canonical payload
  set_both(1500, double_from_bits(0x7FF800DEADBEEF00ULL),
           float_from_bits(0x7FC0DEADU));
  // negative quiet NaN with payload; last element of the (1024-sized) vector
  set_both(2047, double_from_bits(0xFFF8000000000001ULL),
           float_from_bits(0xFFC00001U));
  set_both(2000, std::numeric_limits<double>::infinity(),
           std::numeric_limits<float>::infinity());
  set_both(2001, -std::numeric_limits<double>::infinity(),
           -std::numeric_limits<float>::infinity());
  set_both(2002, -0.0, -0.0f);
  set_both(2003, std::numeric_limits<double>::denorm_min(),  // 5e-324
           std::numeric_limits<float>::denorm_min());        // ~1e-45

  // rows 2048-3071: exactly one exception (full-mantissa pi)
  set_both(2500, 3.141592653589793, static_cast<float>(3.141592653589793));

  // rows 3072-4095: large-magnitude values
  set_both(3100, 44974934523.343, static_cast<float>(44974934523.343));
  set_both(3711, -1243432432.3432, static_cast<float>(-1243432432.3432));

  // rows 9000-9031: pin the endpoints of the large-magnitude range so the
  // FOR range is exactly 2 * kBigMagnitude = 1.6e19 >= 2^63, guaranteeing a
  // 64-bit FOR bit width in the first vector of every double ALP column.
  set_both(9000, -kBigMagnitude, static_cast<float>(-kBigMagnitude));
  set_both(9001, kBigMagnitude, static_cast<float>(kBigMagnitude));

  return data;
}

// ----------------------------------------------------------------------
// Writing

struct ColumnSpec {
  std::string name;
  parquet::Type::type physical_type;
  parquet::Encoding::type encoding;
  int32_t alp_vector_size;  // only used for ALP columns
};

const std::vector<ColumnSpec>& GetColumnSpecs() {
  static const std::vector<ColumnSpec> specs = {
      {"float_plain", parquet::Type::FLOAT, parquet::Encoding::PLAIN, 0},
      {"double_plain", parquet::Type::DOUBLE, parquet::Encoding::PLAIN, 0},
      {"float_alp_1024", parquet::Type::FLOAT, parquet::Encoding::ALP, 1024},
      {"double_alp_1024", parquet::Type::DOUBLE, parquet::Encoding::ALP, 1024},
      {"float_alp_4096", parquet::Type::FLOAT, parquet::Encoding::ALP, 4096},
      {"double_alp_4096", parquet::Type::DOUBLE, parquet::Encoding::ALP, 4096},
      {"float_alp_32", parquet::Type::FLOAT, parquet::Encoding::ALP, 32},
      {"double_alp_32", parquet::Type::DOUBLE, parquet::Encoding::ALP, 32},
  };
  return specs;
}

std::shared_ptr<parquet::schema::GroupNode> MakeSchema() {
  parquet::schema::NodeVector fields;
  for (const auto& spec : GetColumnSpecs()) {
    fields.push_back(parquet::schema::PrimitiveNode::Make(
        spec.name, parquet::Repetition::OPTIONAL, spec.physical_type,
        parquet::ConvertedType::NONE));
  }
  return std::static_pointer_cast<parquet::schema::GroupNode>(
      parquet::schema::GroupNode::Make("schema", parquet::Repetition::REQUIRED, fields));
}

std::shared_ptr<parquet::WriterProperties> MakeWriterProperties() {
  parquet::WriterProperties::Builder builder;
  builder.disable_dictionary()->compression(parquet::Compression::UNCOMPRESSED);
  for (const auto& spec : GetColumnSpecs()) {
    builder.encoding(spec.name, spec.encoding);
    if (spec.encoding == parquet::Encoding::ALP) {
      builder.alp_vector_size(spec.name, spec.alp_vector_size);
    }
  }
  // Only the PLAIN reference columns are compressed (with ZSTD); the ALP
  // columns are left uncompressed.
  builder.compression("float_plain", parquet::Compression::ZSTD);
  builder.compression("double_plain", parquet::Compression::ZSTD);
  return builder.build();
}

void WriteFile(const std::string& path, const GeneratedData& data) {
  std::shared_ptr<arrow::io::FileOutputStream> out_file;
  PARQUET_ASSIGN_OR_THROW(out_file, arrow::io::FileOutputStream::Open(path));

  auto file_writer =
      parquet::ParquetFileWriter::Open(out_file, MakeSchema(), MakeWriterProperties());

  for (size_t rg = 0; rg + 1 < kRowGroupBoundaries.size(); ++rg) {
    const int64_t begin = kRowGroupBoundaries[rg];
    const int64_t end = kRowGroupBoundaries[rg + 1];
    const int64_t num_rows = end - begin;

    // Definition levels (shared by all columns): 1 = present, 0 = null
    std::vector<int16_t> def_levels(num_rows);
    // Dense (nulls removed) value arrays
    std::vector<double> dense_doubles;
    std::vector<float> dense_floats;
    for (int64_t i = begin; i < end; ++i) {
      def_levels[i - begin] = data.valid[i] ? 1 : 0;
      if (data.valid[i]) {
        dense_doubles.push_back(data.doubles[i]);
        dense_floats.push_back(data.floats[i]);
      }
    }

    parquet::RowGroupWriter* row_group_writer = file_writer->AppendRowGroup();
    for (const auto& spec : GetColumnSpecs()) {
      if (spec.physical_type == parquet::Type::FLOAT) {
        auto* writer = static_cast<parquet::FloatWriter*>(row_group_writer->NextColumn());
        writer->WriteBatch(num_rows, def_levels.data(), nullptr, dense_floats.data());
      } else {
        auto* writer =
            static_cast<parquet::DoubleWriter*>(row_group_writer->NextColumn());
        writer->WriteBatch(num_rows, def_levels.data(), nullptr, dense_doubles.data());
      }
    }
  }

  file_writer->Close();
  PARQUET_THROW_NOT_OK(out_file->Close());
}

// ----------------------------------------------------------------------
// ALP page parsing (parses the serialized bytes directly, independent of
// the encoder implementation, so it double-checks the on-disk format)
//
// Page layout:
//   [AlpHeader (7B)][Offset_0 .. Offset_{n-1} (uint32 LE each)]
//   [Vector_0][Vector_1]...
// where each Vector = [exponent u8][factor u8][num_exceptions i16]
//                     [frame_of_reference u32/u64][bit_width u8]
//                     [packed values][exception positions i16[]][exception values T[]]
// Offsets are relative to the start of the body (i.e. after the 7-byte header).

template <typename V>
V LoadLE(const uint8_t* p) {
  V v;
  std::memcpy(&v, p, sizeof(V));
  return v;  // assumes little-endian host, like the ALP on-disk format
}

struct AlpVectorSummary {
  int32_t num_elements = 0;
  uint8_t exponent = 0;
  uint8_t factor = 0;
  uint8_t bit_width = 0;
  uint64_t frame_of_reference = 0;
  int16_t num_exceptions = 0;
  // first up to 5 exceptions
  std::vector<int16_t> exception_positions;
  std::vector<double> exception_values;
};

struct AlpPageSummary {
  uint8_t compression_mode = 0;
  uint8_t integer_encoding = 0;
  uint8_t log_vector_size = 0;
  int32_t num_elements = 0;
  int32_t vector_size = 0;
  int32_t num_vectors = 0;
  int64_t total_exceptions = 0;
  std::vector<AlpVectorSummary> vectors;
};

template <typename T>
AlpPageSummary ParseAlpPage(const uint8_t* data, int64_t size) {
  using ExactType = std::conditional_t<std::is_same_v<T, float>, uint32_t, uint64_t>;
  constexpr int64_t kHeaderSize = 7;

  if (size < kHeaderSize) {
    throw std::runtime_error("ALP page too small for header: " + std::to_string(size));
  }
  AlpPageSummary summary;
  summary.compression_mode = data[0];
  summary.integer_encoding = data[1];
  summary.log_vector_size = data[2];
  summary.num_elements = LoadLE<int32_t>(data + 3);
  summary.vector_size = 1 << summary.log_vector_size;
  summary.num_vectors =
      static_cast<int32_t>((summary.num_elements + summary.vector_size - 1) /
                           summary.vector_size);

  const uint8_t* body = data + kHeaderSize;
  const int64_t body_size = size - kHeaderSize;
  const int64_t offsets_size = int64_t{summary.num_vectors} * sizeof(uint32_t);
  if (body_size < offsets_size) {
    throw std::runtime_error("ALP page too small for offsets section");
  }

  for (int32_t v = 0; v < summary.num_vectors; ++v) {
    const uint32_t offset = LoadLE<uint32_t>(body + int64_t{v} * sizeof(uint32_t));

    AlpVectorSummary vec;
    const int32_t num_full_vectors = summary.num_elements / summary.vector_size;
    const int32_t remainder = summary.num_elements % summary.vector_size;
    vec.num_elements = (v < num_full_vectors) ? summary.vector_size : remainder;

    constexpr int64_t kVectorMetadataSize = 4 + sizeof(ExactType) + 1;
    if (offset + kVectorMetadataSize > body_size) {
      throw std::runtime_error("ALP vector offset out of bounds: vector " +
                               std::to_string(v));
    }
    const uint8_t* p = body + offset;
    vec.exponent = p[0];
    vec.factor = p[1];
    vec.num_exceptions = LoadLE<int16_t>(p + 2);
    vec.frame_of_reference = LoadLE<ExactType>(p + 4);
    vec.bit_width = p[4 + sizeof(ExactType)];

    const uint8_t* vec_data = p + kVectorMetadataSize;
    const int64_t packed_size =
        (int64_t{vec.num_elements} * vec.bit_width + 7) / 8;
    const int64_t exceptions_size =
        int64_t{vec.num_exceptions} * (sizeof(int16_t) + sizeof(T));
    if (offset + kVectorMetadataSize + packed_size + exceptions_size > body_size) {
      throw std::runtime_error("ALP vector data out of bounds: vector " +
                               std::to_string(v));
    }
    const uint8_t* positions = vec_data + packed_size;
    const uint8_t* values = positions + int64_t{vec.num_exceptions} * sizeof(int16_t);
    const int num_to_show = std::min<int>(vec.num_exceptions, 5);
    for (int e = 0; e < num_to_show; ++e) {
      vec.exception_positions.push_back(LoadLE<int16_t>(positions + e * sizeof(int16_t)));
      vec.exception_values.push_back(
          static_cast<double>(LoadLE<T>(values + e * sizeof(T))));
    }
    summary.total_exceptions += vec.num_exceptions;
    summary.vectors.push_back(std::move(vec));
  }
  return summary;
}

std::string FormatValue(double v) {
  std::ostringstream out;
  out << std::setprecision(std::numeric_limits<double>::max_digits10) << v;
  return out.str();
}

void PrintAlpPageSummary(const AlpPageSummary& summary) {
  std::cout << "    ALP header: compression_mode=" << int{summary.compression_mode}
            << " integer_encoding=" << int{summary.integer_encoding}
            << " log_vector_size=" << int{summary.log_vector_size}
            << " (vector_size=" << summary.vector_size << ")"
            << " num_elements=" << summary.num_elements << "\n";
  std::cout << "    num_vectors=" << summary.num_vectors
            << " total_exceptions=" << summary.total_exceptions << "\n";
  std::cout << "    vector elems     exp fac bw  frame_of_ref         n_exc  "
               "first exceptions (pos:value)\n";
  for (size_t v = 0; v < summary.vectors.size(); ++v) {
    const auto& vec = summary.vectors[v];
    std::cout << "    " << std::left << std::setw(6) << v << " " << std::setw(9)
              << vec.num_elements << " " << std::setw(3) << int{vec.exponent} << " "
              << std::setw(3) << int{vec.factor} << " " << std::setw(3)
              << int{vec.bit_width} << " " << std::setw(20) << vec.frame_of_reference
              << " " << std::setw(6) << vec.num_exceptions << std::right;
    for (size_t e = 0; e < vec.exception_values.size(); ++e) {
      std::cout << (e == 0 ? " " : ", ") << vec.exception_positions[e] << ":"
                << FormatValue(vec.exception_values[e]);
    }
    if (vec.num_exceptions > static_cast<int16_t>(vec.exception_values.size())) {
      std::cout << ", ...";
    }
    std::cout << "\n";
  }
}

// ----------------------------------------------------------------------
// Inspection: locate the ALP page bytes of each ALP column

// Returns the ALP-encoded portion of a data page (after skipping the
// rep/def level sections).
std::pair<const uint8_t*, int64_t> GetAlpPageData(const parquet::DataPage& page,
                                                  int16_t max_definition_level) {
  const uint8_t* data = page.data();
  int64_t size = page.size();
  if (page.type() == parquet::PageType::DATA_PAGE) {
    // Data page v1: [def levels: uint32 length + RLE bytes][values]
    // (no rep levels for these flat columns)
    if (max_definition_level > 0) {
      if (size < 4) throw std::runtime_error("data page too small for level section");
      const uint32_t levels_size = LoadLE<uint32_t>(data);
      data += 4 + levels_size;
      size -= 4 + levels_size;
    }
  } else {
    const auto& v2 = static_cast<const parquet::DataPageV2&>(page);
    const int64_t levels_size =
        v2.repetition_levels_byte_length() + v2.definition_levels_byte_length();
    data += levels_size;
    size -= levels_size;
  }
  if (size < 0) throw std::runtime_error("negative ALP data size");
  return {data, size};
}

struct ColumnAlpInfo {
  std::string name;
  parquet::Type::type physical_type;
  // One entry per row group (each ALP column chunk holds a single data page)
  std::vector<AlpPageSummary> row_group_pages;
};

std::vector<ColumnAlpInfo> InspectFile(const std::string& path) {
  std::unique_ptr<parquet::ParquetFileReader> reader =
      parquet::ParquetFileReader::OpenFile(path, /*memory_map=*/false);
  const auto* file_metadata = reader->metadata().get();

  std::cout << "File: " << path << "\n";
  std::cout << "  rows: " << file_metadata->num_rows()
            << ", row groups: " << file_metadata->num_row_groups()
            << ", columns: " << file_metadata->num_columns() << "\n\n";

  std::vector<ColumnAlpInfo> result;
  for (int c = 0; c < file_metadata->num_columns(); ++c) {
    const auto* descr = file_metadata->schema()->Column(c);

    ColumnAlpInfo info;
    info.name = descr->name();
    info.physical_type = descr->physical_type();
    bool is_alp_column = false;

    std::cout << "Column '" << descr->name() << "' ("
              << parquet::TypeToString(descr->physical_type()) << ")\n";

    for (int rg = 0; rg < file_metadata->num_row_groups(); ++rg) {
      auto row_group_reader = reader->RowGroup(rg);
      auto chunk_metadata = row_group_reader->metadata()->ColumnChunk(c);

      std::ostringstream encodings;
      for (const auto& e : chunk_metadata->encodings()) {
        encodings << parquet::EncodingToString(e) << " ";
      }
      std::cout << "  Row group " << rg << ": rows=" << chunk_metadata->num_values()
                << " encodings=[ " << encodings.str() << "] compression="
                << ::arrow::util::Codec::GetCodecAsString(chunk_metadata->compression())
                << " null_count="
                << (chunk_metadata->statistics()
                        ? std::to_string(chunk_metadata->statistics()->null_count())
                        : std::string("n/a"))
                << " compressed_bytes=" << chunk_metadata->total_compressed_size()
                << "\n";

      const auto& chunk_encodings = chunk_metadata->encodings();
      const bool is_alp = std::find(chunk_encodings.begin(), chunk_encodings.end(),
                                    parquet::Encoding::ALP) != chunk_encodings.end();
      if (!is_alp) continue;
      is_alp_column = true;

      auto pager = row_group_reader->GetColumnPageReader(c);
      while (std::shared_ptr<parquet::Page> page = pager->NextPage()) {
        if (page->type() != parquet::PageType::DATA_PAGE &&
            page->type() != parquet::PageType::DATA_PAGE_V2) {
          continue;
        }
        const auto& data_page = static_cast<const parquet::DataPage&>(*page);
        auto [alp_data, alp_size] =
            GetAlpPageData(data_page, descr->max_definition_level());
        AlpPageSummary summary = (descr->physical_type() == parquet::Type::FLOAT)
                                     ? ParseAlpPage<float>(alp_data, alp_size)
                                     : ParseAlpPage<double>(alp_data, alp_size);
        PrintAlpPageSummary(summary);
        info.row_group_pages.push_back(std::move(summary));
      }
    }
    std::cout << "\n";
    if (is_alp_column) {
      result.push_back(std::move(info));
    }
  }
  return result;
}

// ----------------------------------------------------------------------
// Expectation checks against the issue comment

void CheckExpectations(const std::vector<ColumnAlpInfo>& columns,
                       std::vector<std::string>* failures) {
  auto expect = [&](bool ok, const std::string& msg) {
    if (!ok) failures->push_back(msg);
  };

  // Values per row group: {6144, 1024, 1024, 808 rows - 8 nulls = 800, 32}
  const std::vector<int32_t> expected_elements = {6144, 1024, 1024, 800, 32};
  const int kNumRowGroups = static_cast<int>(expected_elements.size());

  for (const auto& col : columns) {
    const bool is_float = col.physical_type == parquet::Type::FLOAT;
    // All base data is exception-free, so the only exceptions are in row
    // group 0 (7 specials + 1 pi + 2 large + 512 half-random + 1024 random)
    // and, for the float columns only, row group 4: its large-magnitude
    // values overflow the float encoder's int32 range, so the whole vector
    // is stored as exceptions (the doubles encode them losslessly).
    const std::vector<int64_t> expected_exceptions_per_rg =
        is_float ? std::vector<int64_t>{7 + 1 + 2 + 512 + 1024, 0, 0, 0, 32}
                 : std::vector<int64_t>{7 + 1 + 2 + 512 + 1024, 0, 0, 0, 0};
    expect(col.row_group_pages.size() == static_cast<size_t>(kNumRowGroups),
           col.name + ": expected " + std::to_string(kNumRowGroups) +
               " ALP pages (one per row group), got " +
               std::to_string(col.row_group_pages.size()));
    if (col.row_group_pages.size() != static_cast<size_t>(kNumRowGroups)) continue;

    for (int rg = 0; rg < kNumRowGroups; ++rg) {
      const AlpPageSummary& page = col.row_group_pages[rg];
      expect(page.num_elements == expected_elements[rg],
             col.name + " rg " + std::to_string(rg) + ": expected num_elements=" +
                 std::to_string(expected_elements[rg]) + ", got " +
                 std::to_string(page.num_elements));
      expect(page.total_exceptions == expected_exceptions_per_rg[rg],
             col.name + " rg " + std::to_string(rg) + ": expected " +
                 std::to_string(expected_exceptions_per_rg[rg]) +
                 " total exceptions, got " + std::to_string(page.total_exceptions));
    }

    // The 4-decimal row group must use a different exponent/factor than the
    // 2-decimal base data.
    const auto& rg0 = col.row_group_pages[0];
    const auto& rg1 = col.row_group_pages[1];
    if (!rg0.vectors.empty() && !rg1.vectors.empty()) {
      const bool differs = rg1.vectors[0].exponent != rg0.vectors[0].exponent ||
                           rg1.vectors[0].factor != rg0.vectors[0].factor;
      expect(differs, col.name +
                          ": expected the 4-decimal row group 1 to use a different "
                          "exponent/factor than row group 0");
    }

    // Detailed per-vector expectations for the 1024-value-vector columns,
    // where each row range in the issue is exactly one vector.
    if (rg0.vector_size == 1024) {
      const std::vector<std::vector<int>> expected_exc = {
          {0, 7, 1, 2, 512, 1024},   // rg 0: rows 0-6143
          {0},                       // rg 1: 4-decimal digits
          {0},                       // rg 2: constant
          {0},                       // rg 3: trailing partial + nulls
          {is_float ? 32 : 0},       // rg 4: 64-bit FOR (float: all exceptions)
      };
      for (int rg = 0; rg < kNumRowGroups; ++rg) {
        const AlpPageSummary& page = col.row_group_pages[rg];
        expect(page.num_vectors == static_cast<int32_t>(expected_exc[rg].size()),
               col.name + " rg " + std::to_string(rg) + ": expected " +
                   std::to_string(expected_exc[rg].size()) + " vectors, got " +
                   std::to_string(page.num_vectors));
        if (page.vectors.size() != expected_exc[rg].size()) continue;
        for (size_t v = 0; v < expected_exc[rg].size(); ++v) {
          expect(page.vectors[v].num_exceptions == expected_exc[rg][v],
                 col.name + " rg " + std::to_string(rg) + " vector " +
                     std::to_string(v) + ": expected " +
                     std::to_string(expected_exc[rg][v]) + " exceptions, got " +
                     std::to_string(page.vectors[v].num_exceptions));
        }
      }
      if (!col.row_group_pages[2].vectors.empty()) {
        expect(col.row_group_pages[2].vectors[0].bit_width == 0,
               col.name + " rg 2 vector 0 (constant 7.77): expected bit_width=0, got " +
                   std::to_string(col.row_group_pages[2].vectors[0].bit_width));
      }
      if (!col.row_group_pages[3].vectors.empty()) {
        expect(col.row_group_pages[3].vectors[0].num_elements == 800,
               col.name + " rg 3 vector 0: expected 800 elements (trailing partial "
                          "vector), got " +
                   std::to_string(col.row_group_pages[3].vectors[0].num_elements));
      }
    }

    // The large-magnitude row group must produce a 64-bit FOR bit width in
    // the double columns (the pinned +/-8e18 endpoints are in vector 0 for
    // every vector size).
    if (!is_float && !col.row_group_pages[4].vectors.empty()) {
      expect(col.row_group_pages[4].vectors[0].bit_width == 64,
             col.name + " rg 4 vector 0: expected bit_width=64, got " +
                 std::to_string(col.row_group_pages[4].vectors[0].bit_width));
    }
  }
}

// ----------------------------------------------------------------------
// Round-trip verification: read every column back and compare bit-exactly

template <typename T>
bool BitwiseEqual(T a, T b) {
  return std::memcmp(&a, &b, sizeof(T)) == 0;
}

template <typename ParquetType, typename T>
int64_t CountColumnMismatches(parquet::ColumnReader* untyped_reader, int64_t row_begin,
                              int64_t row_end, const GeneratedData& data,
                              const std::vector<T>& all_values) {
  auto* reader = static_cast<parquet::TypedColumnReader<ParquetType>*>(untyped_reader);
  const int64_t num_rows = row_end - row_begin;
  std::vector<int16_t> def_levels(num_rows);
  std::vector<T> values(num_rows);
  int64_t values_read = 0;
  int64_t total_levels = 0;
  int64_t total_values = 0;
  while (reader->HasNext() && total_levels < num_rows) {
    const int64_t levels_read =
        reader->ReadBatch(num_rows - total_levels, def_levels.data() + total_levels,
                          nullptr, values.data() + total_values, &values_read);
    total_levels += levels_read;
    total_values += values_read;
  }

  int64_t mismatches = 0;
  if (total_levels != num_rows) mismatches++;
  int64_t value_index = 0;
  for (int64_t i = 0; i < total_levels; ++i) {
    const int64_t row = row_begin + i;
    const bool is_valid = def_levels[i] == 1;
    if (is_valid != data.valid[row]) {
      mismatches++;
      continue;
    }
    if (is_valid) {
      if (!BitwiseEqual(values[value_index], all_values[row])) {
        mismatches++;
      }
      value_index++;
    }
  }
  return mismatches;
}

void VerifyRoundTrip(const std::string& path, const GeneratedData& data,
                     std::vector<std::string>* failures) {
  auto reader = parquet::ParquetFileReader::OpenFile(path, /*memory_map=*/false);
  const auto& specs = GetColumnSpecs();
  std::vector<int64_t> mismatches(specs.size(), 0);
  for (size_t rg = 0; rg + 1 < kRowGroupBoundaries.size(); ++rg) {
    auto row_group = reader->RowGroup(static_cast<int>(rg));
    for (int c = 0; c < static_cast<int>(specs.size()); ++c) {
      auto column_reader = row_group->Column(c);
      if (specs[c].physical_type == parquet::Type::FLOAT) {
        mismatches[c] += CountColumnMismatches<parquet::FloatType>(
            column_reader.get(), kRowGroupBoundaries[rg], kRowGroupBoundaries[rg + 1],
            data, data.floats);
      } else {
        mismatches[c] += CountColumnMismatches<parquet::DoubleType>(
            column_reader.get(), kRowGroupBoundaries[rg], kRowGroupBoundaries[rg + 1],
            data, data.doubles);
      }
    }
  }

  std::cout << "Round-trip verification (bit-exact, including NaN/-0.0):\n";
  for (size_t c = 0; c < specs.size(); ++c) {
    std::cout << "  " << std::left << std::setw(16) << specs[c].name << std::right
              << (mismatches[c] == 0
                      ? " OK"
                      : " MISMATCHES: " + std::to_string(mismatches[c]))
              << "\n";
    if (mismatches[c] != 0) {
      failures->push_back(specs[c].name + ": " + std::to_string(mismatches[c]) +
                          " round-trip mismatches vs generated data");
    }
  }
  std::cout << "\n";
}

void PrintUsage() {
  std::cerr << "Usage: write_parquet <output_directory>\n"
            << "Writes " << kFileName << " (the ALP example file from\n"
            << "https://github.com/apache/parquet-testing/issues/105) into the given\n"
            << "directory and prints the resulting ALP page structure.\n";
}

}  // namespace

int main(int argc, char** argv) {
  if (argc != 2) {
    PrintUsage();
    return 2;
  }
  const std::filesystem::path output_dir(argv[1]);
  if (!std::filesystem::exists(output_dir) ||
      !std::filesystem::is_directory(output_dir)) {
    std::cerr << "Output directory does not exist or is not a directory: "
              << output_dir.string() << "\n";
    return 2;
  }
  const std::string out_path = (output_dir / kFileName).string();

  try {
    const GeneratedData data = MakeData();
    WriteFile(out_path, data);
    std::cout << "Wrote " << out_path << " ("
              << std::filesystem::file_size(out_path) << " bytes)\n\n";

    std::vector<ColumnAlpInfo> columns = InspectFile(out_path);

    std::vector<std::string> failures;
    CheckExpectations(columns, &failures);
    VerifyRoundTrip(out_path, data, &failures);

    if (failures.empty()) {
      std::cout << "All expectations from the issue comment matched.\n";
      return 0;
    }
    std::cout << "MISMATCHES vs expectations in "
                 "https://github.com/apache/parquet-testing/issues/105"
                 "#issuecomment-5172111570:\n";
    for (const auto& failure : failures) {
      std::cout << "  - " << failure << "\n";
    }
    return 1;
  } catch (const std::exception& e) {
    std::cerr << "Error: " << e.what() << "\n";
    return 1;
  }
}
