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

#include <memory>
#include <numeric>
#include <sstream>
#include <utility>

#include "benchmark/benchmark.h"

#include <aws/core/auth/AWSCredentials.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/CreateBucketRequest.h>
#include <aws/s3/model/HeadBucketRequest.h>
#include <aws/s3/model/PutObjectRequest.h>

#include "arrow/filesystem/s3_internal.h"
#include "arrow/filesystem/s3_test_util.h"
#include "arrow/filesystem/s3fs.h"
#include "arrow/io/caching.h"
#include "arrow/io/interfaces.h"
#include "arrow/status.h"
#include "arrow/table.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/random.h"
#include "arrow/util/key_value_metadata.h"
#include "arrow/util/range.h"

#include "parquet/arrow/reader.h"
#include "parquet/arrow/writer.h"
#include "parquet/properties.h"

namespace arrow {
namespace fs {

using ::arrow::fs::internal::ConnectRetryStrategy;
using ::arrow::fs::internal::OutcomeToStatus;
using ::arrow::fs::internal::ToAwsString;

// Environment variables to configure the S3 test environment
static const char* kEnvBucketName = "ARROW_TEST_S3_BUCKET";
static const char* kEnvSkipSetup = "ARROW_TEST_S3_SKIP_SETUP";
static const char* kEnvAwsRegion = "ARROW_TEST_S3_REGION";

// Set up Minio and create the test bucket and files.
class MinioFixture : public benchmark::Fixture {
 public:
  void SetUp(const ::benchmark::State& state) override {
    minio_.reset(new MinioTestServer());
    ASSERT_OK(minio_->Start());

    const char* region_str = std::getenv(kEnvAwsRegion);
    if (region_str) {
      region_ = region_str;
      std::cerr << "Using region from environment: " << region_ << std::endl;
    } else {
      std::cerr << "Using default region" << std::endl;
    }

    const char* bucket_str = std::getenv(kEnvBucketName);
    if (bucket_str) {
      bucket_ = bucket_str;
      std::cerr << "Using bucket from environment: " << bucket_ << std::endl;
    } else {
      bucket_ = "bucket";
      std::cerr << "Using default bucket: " << bucket_ << std::endl;
    }

    client_config_.endpointOverride = ToAwsString(minio_->connect_string());
    client_config_.scheme = Aws::Http::Scheme::HTTP;
    if (!region_.empty()) {
      client_config_.region = ToAwsString(region_);
    }
    client_config_.retryStrategy = std::make_shared<ConnectRetryStrategy>();
    credentials_ = {ToAwsString(minio_->access_key()), ToAwsString(minio_->secret_key())};
    bool use_virtual_addressing = false;
    client_.reset(
        new Aws::S3::S3Client(credentials_, client_config_,
                              Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
                              use_virtual_addressing));

    MakeFileSystem();

    const char* skip_str = std::getenv(kEnvSkipSetup);
    const std::string skip = skip_str ? std::string(skip_str) : "";
    if (!skip.empty()) {
      std::cerr << "Skipping creation of bucket/objects as requested" << std::endl;
    } else {
      ASSERT_OK(MakeBucket());
      ASSERT_OK(MakeObject("bytes_1mib", 1024 * 1024));
      ASSERT_OK(MakeObject("bytes_100mib", 100 * 1024 * 1024));
      ASSERT_OK(MakeObject("bytes_500mib", 500 * 1024 * 1024));
      ASSERT_OK(MakeParquetObject(bucket_ + "/pq_c402_r250k", 400, 250000));
    }
  }

  void MakeFileSystem() {
    options_.ConfigureAccessKey(minio_->access_key(), minio_->secret_key());
    options_.scheme = "http";
    if (!region_.empty()) {
      options_.region = region_;
    }
    options_.endpoint_override = minio_->connect_string();
    ASSERT_OK_AND_ASSIGN(fs_, S3FileSystem::Make(options_));
  }

  /// Set up bucket if it doesn't exist.
  ///
  /// When using Minio we'll have a fresh setup each time, but
  /// otherwise we may have a leftover bucket.
  Status MakeBucket() {
    Aws::S3::Model::HeadBucketRequest head;
    head.SetBucket(ToAwsString(bucket_));
    const Status st = OutcomeToStatus("HeadBucket", client_->HeadBucket(head));
    if (st.ok()) {
      // Bucket exists already
      return st;
    }
    Aws::S3::Model::CreateBucketRequest req;
    req.SetBucket(ToAwsString(bucket_));
    return OutcomeToStatus("CreateBucket", client_->CreateBucket(req));
  }

  /// Make an object with dummy data.
  Status MakeObject(const std::string& name, int size) {
    Aws::S3::Model::PutObjectRequest req;
    req.SetBucket(ToAwsString(bucket_));
    req.SetKey(ToAwsString(name));
    req.SetBody(std::make_shared<std::stringstream>(std::string(size, 'a')));
    return OutcomeToStatus("PutObject", client_->PutObject(req));
  }

  /// Make an object with Parquet data.
  /// Appends integer columns to the beginning (to act as indices).
  Status MakeParquetObject(const std::string& path, int num_columns, int num_rows) {
    std::vector<std::shared_ptr<ChunkedArray>> columns;
    FieldVector fields{
        field("timestamp", int64(), /*nullable=*/true,
              key_value_metadata(
                  {{"min", "0"}, {"max", "10000000000"}, {"null_probability", "0"}})),
        field("val", int32(), /*nullable=*/true,
              key_value_metadata(
                  {{"min", "0"}, {"max", "1000000000"}, {"null_probability", "0"}}))};
    for (int i = 0; i < num_columns; i++) {
      std::stringstream ss;
      ss << "col" << i;
      fields.push_back(
          field(ss.str(), float64(), /*nullable=*/true,
                key_value_metadata(
                    {{"min", "-1.e10"}, {"max", "1e10"}, {"null_probability", "0"}})));
    }
    auto batch = random::GenerateBatch(fields, num_rows, 0);
    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<Table> table,
                          Table::FromRecordBatches({batch}));

    std::shared_ptr<io::OutputStream> sink;
    ARROW_ASSIGN_OR_RAISE(sink, fs_->OpenOutputStream(path));
    RETURN_NOT_OK(
        parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), sink, num_rows));

    return Status::OK();
  }

  void TearDown(const ::benchmark::State& state) override {
    ASSERT_OK(minio_->Stop());
    // Delete temporary directory, freeing up disk space
    minio_.reset();
  }

 protected:
  std::unique_ptr<MinioTestServer> minio_;
  std::string region_;
  std::string bucket_;
  Aws::Client::ClientConfiguration client_config_;
  Aws::Auth::AWSCredentials credentials_;
  std::unique_ptr<Aws::S3::S3Client> client_;
  S3Options options_;
  std::shared_ptr<S3FileSystem> fs_;
};

/// Set up/tear down the AWS SDK globally.
/// (GBenchmark doesn't run GTest environments.)
class S3BenchmarkEnvironment {
 public:
  S3BenchmarkEnvironment() { s3_env_.SetUp(); }
  ~S3BenchmarkEnvironment() { s3_env_.TearDown(); }

 private:
  S3Environment s3_env_;
};

S3BenchmarkEnvironment env{};

/// Read the entire file into memory in one go to measure bandwidth.
static void NaiveRead(benchmark::State& st, S3FileSystem* fs, const std::string& path) {
  int64_t total_bytes = 0;
  int total_items = 0;
  for (auto _ : st) {
    std::shared_ptr<io::RandomAccessFile> file;
    std::shared_ptr<Buffer> buf;
    int64_t size;
    ASSERT_OK_AND_ASSIGN(file, fs->OpenInputFile(path));
    ASSERT_OK_AND_ASSIGN(size, file->GetSize());
    ASSERT_OK_AND_ASSIGN(buf, file->ReadAt(0, size));
    total_bytes += buf->size();
    total_items += 1;
  }
  st.SetBytesProcessed(total_bytes);
  st.SetItemsProcessed(total_items);
  std::cerr << "Read the file " << total_items << " times" << std::endl;
}

constexpr int64_t kChunkSize = 5 * 1024 * 1024;

/// Mimic the Parquet reader, reading the file in small chunks.
static void ChunkedRead(benchmark::State& st, S3FileSystem* fs, const std::string& path) {
  int64_t total_bytes = 0;
  int total_items = 0;
  for (auto _ : st) {
    std::shared_ptr<io::RandomAccessFile> file;
    std::shared_ptr<Buffer> buf;
    int64_t size = 0;
    ASSERT_OK_AND_ASSIGN(file, fs->OpenInputFile(path));
    ASSERT_OK_AND_ASSIGN(size, file->GetSize());
    total_items += 1;

    int64_t offset = 0;
    while (offset < size) {
      const int64_t read = std::min(size, kChunkSize);
      ASSERT_OK_AND_ASSIGN(buf, file->ReadAt(offset, read));
      total_bytes += buf->size();
      offset += buf->size();
    }
  }
  st.SetBytesProcessed(total_bytes);
  st.SetItemsProcessed(total_items);
  std::cerr << "Read the file " << total_items << " times" << std::endl;
}

/// Read the file in small chunks, but using read coalescing.
static void CoalescedRead(benchmark::State& st, S3FileSystem* fs,
                          const std::string& path) {
  int64_t total_bytes = 0;
  int total_items = 0;
  for (auto _ : st) {
    std::shared_ptr<io::RandomAccessFile> file;
    std::shared_ptr<Buffer> buf;
    int64_t size = 0;
    ASSERT_OK_AND_ASSIGN(file, fs->OpenInputFile(path));
    ASSERT_OK_AND_ASSIGN(size, file->GetSize());
    total_items += 1;

    io::internal::ReadRangeCache cache(
        file, {},
        io::CacheOptions{/*hole_size_limit=*/8192, /*range_size_limit=*/64 * 1024 * 1024,
                         /*lazy=*/false});
    std::vector<io::ReadRange> ranges;

    int64_t offset = 0;
    while (offset < size) {
      const int64_t read = std::min(size, kChunkSize);
      ranges.push_back(io::ReadRange{offset, read});
      offset += read;
    }
    ASSERT_OK(cache.Cache(ranges));

    offset = 0;
    while (offset < size) {
      const int64_t read = std::min(size, kChunkSize);
      ASSERT_OK_AND_ASSIGN(buf, cache.Read({offset, read}));
      total_bytes += buf->size();
      offset += read;
    }
  }
  st.SetBytesProcessed(total_bytes);
  st.SetItemsProcessed(total_items);
  std::cerr << "Read the file " << total_items << " times" << std::endl;
}

/// Read a Parquet file from S3.
static void ParquetRead(benchmark::State& st, S3FileSystem* fs, const std::string& path,
                        std::vector<int> column_indices, bool pre_buffer,
                        std::string read_strategy) {
  int64_t total_bytes = 0;
  int total_items = 0;

  parquet::ArrowReaderProperties properties;
  properties.set_use_threads(true);
  properties.set_pre_buffer(pre_buffer);
  parquet::ReaderProperties parquet_properties = parquet::default_reader_properties();

  for (auto _ : st) {
    std::shared_ptr<io::RandomAccessFile> file;
    int64_t size = 0;
    ASSERT_OK_AND_ASSIGN(file, fs->OpenInputFile(path));
    ASSERT_OK_AND_ASSIGN(size, file->GetSize());

    std::unique_ptr<parquet::arrow::FileReader> reader;
    parquet::arrow::FileReaderBuilder builder;
    ASSERT_OK(builder.Open(file, parquet_properties));
    ASSERT_OK(builder.properties(properties)->Build(&reader));

    if (read_strategy == "ReadTable") {
      std::shared_ptr<Table> table;
      ASSERT_OK(reader->ReadTable(column_indices, &table));
    } else {
      std::shared_ptr<RecordBatchReader> rb_reader;
      ASSERT_OK(reader->GetRecordBatchReader({0}, column_indices, &rb_reader));
      ASSERT_OK(rb_reader->ToTable());
    }

    // TODO: actually measure table memory usage
    total_bytes += size;
    total_items += 1;
  }
  st.SetBytesProcessed(total_bytes);
  st.SetItemsProcessed(total_items);
}

/// Helper function used in the macros below to template benchmarks.
static void ParquetReadAll(benchmark::State& st, S3FileSystem* fs,
                           const std::string& bucket, int64_t file_rows,
                           int64_t file_cols, bool pre_buffer,
                           std::string read_strategy) {
  std::vector<int> column_indices(file_cols);
  std::iota(column_indices.begin(), column_indices.end(), 0);
  std::stringstream ss;
  ss << bucket << "/pq_c" << file_cols << "_r" << file_rows << "k";
  ParquetRead(st, fs, ss.str(), column_indices, false, read_strategy);
}

/// Helper function used in the macros below to template benchmarks.
static void ParquetReadSome(benchmark::State& st, S3FileSystem* fs,
                            const std::string& bucket, int64_t file_rows,
                            int64_t file_cols, std::vector<int> cols_to_read,
                            bool pre_buffer, std::string read_strategy) {
  std::stringstream ss;
  ss << bucket << "/pq_c" << file_cols << "_r" << file_rows << "k";
  ParquetRead(st, fs, ss.str(), cols_to_read, false, read_strategy);
}

BENCHMARK_DEFINE_F(MinioFixture, ReadAll1Mib)(benchmark::State& st) {
  NaiveRead(st, fs_.get(), bucket_ + "/bytes_1mib");
}
BENCHMARK_REGISTER_F(MinioFixture, ReadAll1Mib)->UseRealTime();
BENCHMARK_DEFINE_F(MinioFixture, ReadAll100Mib)(benchmark::State& st) {
  NaiveRead(st, fs_.get(), bucket_ + "/bytes_100mib");
}
BENCHMARK_REGISTER_F(MinioFixture, ReadAll100Mib)->UseRealTime();
BENCHMARK_DEFINE_F(MinioFixture, ReadAll500Mib)(benchmark::State& st) {
  NaiveRead(st, fs_.get(), bucket_ + "/bytes_500mib");
}
BENCHMARK_REGISTER_F(MinioFixture, ReadAll500Mib)->UseRealTime();

BENCHMARK_DEFINE_F(MinioFixture, ReadChunked100Mib)(benchmark::State& st) {
  ChunkedRead(st, fs_.get(), bucket_ + "/bytes_100mib");
}
BENCHMARK_REGISTER_F(MinioFixture, ReadChunked100Mib)->UseRealTime();
BENCHMARK_DEFINE_F(MinioFixture, ReadChunked500Mib)(benchmark::State& st) {
  ChunkedRead(st, fs_.get(), bucket_ + "/bytes_500mib");
}
BENCHMARK_REGISTER_F(MinioFixture, ReadChunked500Mib)->UseRealTime();

BENCHMARK_DEFINE_F(MinioFixture, ReadCoalesced100Mib)(benchmark::State& st) {
  CoalescedRead(st, fs_.get(), bucket_ + "/bytes_100mib");
}
BENCHMARK_REGISTER_F(MinioFixture, ReadCoalesced100Mib)->UseRealTime();
BENCHMARK_DEFINE_F(MinioFixture, ReadCoalesced500Mib)(benchmark::State& st) {
  CoalescedRead(st, fs_.get(), bucket_ + "/bytes_500mib");
}
BENCHMARK_REGISTER_F(MinioFixture, ReadCoalesced500Mib)->UseRealTime();

// Helpers to generate various multiple benchmarks for a given Parquet file.

// NAME: the base name of the benchmark.
// ROWS: the number of rows in the Parquet file.
// COLS: the number of columns in the Parquet file.
// STRATEGY: how to read the file (ReadTable or GetRecordBatchReader)
#define PQ_BENCHMARK_IMPL(NAME, ROWS, COLS, STRATEGY)                                 \
  BENCHMARK_DEFINE_F(MinioFixture, NAME##STRATEGY##AllNaive)(benchmark::State & st) { \
    ParquetReadAll(st, fs_.get(), bucket_, ROWS, COLS, false, #STRATEGY);             \
  }                                                                                   \
  BENCHMARK_REGISTER_F(MinioFixture, NAME##STRATEGY##AllNaive)->UseRealTime();        \
  BENCHMARK_DEFINE_F(MinioFixture, NAME##STRATEGY##AllCoalesced)                      \
  (benchmark::State & st) {                                                           \
    ParquetReadAll(st, fs_.get(), bucket_, ROWS, COLS, true, #STRATEGY);              \
  }                                                                                   \
  BENCHMARK_REGISTER_F(MinioFixture, NAME##STRATEGY##AllCoalesced)->UseRealTime();

// COL_INDICES: a vector specifying a subset of column indices to read.
#define PQ_BENCHMARK_PICK_IMPL(NAME, ROWS, COLS, COL_INDICES, STRATEGY)                 \
  BENCHMARK_DEFINE_F(MinioFixture, NAME##STRATEGY##PickNaive)(benchmark::State & st) {  \
    ParquetReadSome(st, fs_.get(), bucket_, ROWS, COLS, COL_INDICES, false, #STRATEGY); \
  }                                                                                     \
  BENCHMARK_REGISTER_F(MinioFixture, NAME##STRATEGY##PickNaive)->UseRealTime();         \
  BENCHMARK_DEFINE_F(MinioFixture, NAME##STRATEGY##PickCoalesced)                       \
  (benchmark::State & st) {                                                             \
    ParquetReadSome(st, fs_.get(), bucket_, ROWS, COLS, COL_INDICES, true, #STRATEGY);  \
  }                                                                                     \
  BENCHMARK_REGISTER_F(MinioFixture, NAME##STRATEGY##PickCoalesced)->UseRealTime();

#define PQ_BENCHMARK(ROWS, COLS)                                   \
  PQ_BENCHMARK_IMPL(ReadParquet_c##COLS##_r##ROWS##K_, ROWS, COLS, \
                    GetRecordBatchReader);                         \
  PQ_BENCHMARK_IMPL(ReadParquet_c##COLS##_r##ROWS##K_, ROWS, COLS, ReadTable);

#define PQ_BENCHMARK_PICK(NAME, ROWS, COLS, COL_INDICES)                         \
  PQ_BENCHMARK_PICK_IMPL(ReadParquet_c##COLS##_r##ROWS##K_##NAME##_, ROWS, COLS, \
                         COL_INDICES, GetRecordBatchReader);                     \
  PQ_BENCHMARK_PICK_IMPL(ReadParquet_c##COLS##_r##ROWS##K_##NAME##_, ROWS, COLS, \
                         COL_INDICES, ReadTable);

// Test a Parquet file with 250k rows, 402 columns.
PQ_BENCHMARK(250, 402);
// Scenario A: test selecting a small set of contiguous columns, and a "far" column.
PQ_BENCHMARK_PICK(A, 250, 402, (std::vector<int>{0, 1, 2, 3, 4, 90}));
// Scenario B: test selecting a large set of contiguous columns.
PQ_BENCHMARK_PICK(B, 250, 402, (::arrow::internal::Iota(41)));

}  // namespace fs
}  // namespace arrow
