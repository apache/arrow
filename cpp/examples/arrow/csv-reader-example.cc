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
#include <arrow/csv/api.h>
#include <arrow/filesystem/api.h>

#include "arrow/util/task_group.h"
#include "arrow/util/thread_pool.h"

#include <cstdlib>
#include <iostream>
#include <thread>

// const int NUM_FILES = 20;
const int NUM_FILES = 5;

arrow::csv::ReadOptions CreateThreadedSyncReadOptions() {
  auto result = arrow::csv::ReadOptions::Defaults();
  result.use_threads = true;
  return result;
}

arrow::csv::ReadOptions CreateSerialSyncReadOptions() {
  auto result = arrow::csv::ReadOptions::Defaults();
  result.use_threads = false;
  return result;
}

arrow::csv::ReadOptions CreateThreadedAsyncReadOptions() {
  auto result = arrow::csv::ReadOptions::Defaults();
  result.use_threads = true;
  result.read_async = true;
  return result;
}

arrow::csv::ParseOptions CreateParseOptions() {
  auto result = arrow::csv::ParseOptions::Defaults();
  return result;
}

arrow::csv::ConvertOptions CreateConvertOptions() {
  auto result = arrow::csv::ConvertOptions::Defaults();
  return result;
}

arrow::Status DoReadFile(std::shared_ptr<arrow::csv::TableReader> table_reader,
                         std::shared_ptr<arrow::io::InputStream> input_stream,
                         int file_index) {
  std::cout << "File At Index (" << file_index << ") START" << std::endl;
  auto start = std::chrono::high_resolution_clock::now();
  auto table = *table_reader->Read();
  auto end = std::chrono::high_resolution_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
  std::cout << "File At Index (" << file_index << ") " << duration.count() << std::endl;
  return arrow::Status::OK();
}

arrow::Future<arrow::Status> DoReadFileAsync(
    std::shared_ptr<arrow::csv::TableReader> table_reader,
    std::shared_ptr<arrow::io::InputStream> input_stream, int file_index) {
  std::cout << "File At Index (" << file_index << ") START" << std::endl;
  auto start = std::chrono::high_resolution_clock::now();
  auto table_future = table_reader->ReadAsync();
  return table_future.Then(
      [start, file_index](const arrow::Result<std::shared_ptr<arrow::Table>>& table) {
        auto end = std::chrono::high_resolution_clock::now();
        auto duration =
            std::chrono::duration_cast<std::chrono::microseconds>(end - start);
        std::cout << "File At Index (" << file_index << ") " << duration.count()
                  << std::endl;
        return arrow::Status::OK();
      });
}

double DoReadFiles(arrow::MemoryPool* memory_pool,
                   std::shared_ptr<arrow::fs::FileSystem> fs,
                   std::shared_ptr<arrow::internal::TaskGroup> task_group,
                   arrow::csv::ReadOptions read_options, std::string bucket_name) {
  // TODO: Get rid of these "keepalives"
  std::vector<std::shared_ptr<arrow::io::InputStream>> input_streams;
  std::vector<std::shared_ptr<arrow::csv::TableReader>> table_readers;

  auto total_start = std::chrono::high_resolution_clock::now();
  auto total_end = std::chrono::high_resolution_clock::now();
  auto total_duration =
      std::chrono::duration_cast<std::chrono::milliseconds>(total_end - total_start);

  total_start = std::chrono::high_resolution_clock::now();
  for (int file_index = 0; file_index < NUM_FILES; file_index++) {
    std::shared_ptr<arrow::io::InputStream> input_stream;
    if (fs->type_name() == "s3") {
      input_stream =
          *fs->OpenInputStream(bucket_name + "/" + std::to_string(file_index) + ".csv");
    } else {
      input_stream = *fs->OpenInputStream("/home/ubuntu/datasets/arrow/csv/" +
                                          std::to_string(file_index) + ".csv");
    }
    input_streams.push_back(input_stream);
    auto table_reader =
        *arrow::csv::TableReader::Make(memory_pool, input_stream, read_options,
                                       CreateParseOptions(), CreateConvertOptions());
    table_readers.push_back(table_reader);
    if (read_options.read_async) {
      task_group->Append(DoReadFileAsync(table_reader, input_stream, file_index));
    } else {
      task_group->Append([table_reader, input_stream, file_index] {
        return DoReadFile(table_reader, input_stream, file_index);
      });
    }
  }
  auto final_status = task_group->Finish();
  if (!final_status.ok()) {
    std::cout << "Method failed.  (err=" << final_status.message() << ")" << std::endl;
  }
  total_end = std::chrono::high_resolution_clock::now();
  total_duration =
      std::chrono::duration_cast<std::chrono::milliseconds>(total_end - total_start);
  return total_duration.count() / static_cast<double>(NUM_FILES);
}

int main(int argc, char** argv) {
  auto* thread_pool = arrow::internal::GetCpuThreadPool();
  std::cout << "Num threads: " << std::thread::hardware_concurrency() << std::endl;
  auto memory_pool = arrow::default_memory_pool();
  auto access_key = std::getenv("S3_ACCESS_KEY_ID");
  auto access_secret = std::getenv("S3_ACCESS_KEY_SECRET");
  auto aws_region = std::getenv("S3_REGION");
  auto aws_bucket_name = std::getenv("S3_BUCKET_NAME");
  arrow::fs::S3GlobalOptions options;
  options.log_level = arrow::fs::S3LogLevel::Fatal;
  if (!InitializeS3(options).ok()) {
    std::cout << "Error initializing S3 subsystem" << std::endl;
    return -1;
  }
  // auto fs =
  // std::make_shared<arrow::fs::LocalFileSystem>(arrow::fs::LocalFileSystemOptions());
  auto s3_options = arrow::fs::S3Options::FromAccessKey(access_key, access_secret);
  s3_options.region = aws_region;
  auto fs = *arrow::fs::S3FileSystem::Make(s3_options);
  double avg_duration = 0;

  // std::cout << "Serial outer loop threaded inner loop file I/O on thread pool" <<
  // std::endl; avg_duration = DoReadFiles(memory_pool, fs,
  // arrow::internal::TaskGroup::MakeSerial(), CreateThreadedSyncReadOptions(),
  // aws_bucket_name); std::cout << "  Finished reading in all files (avg=" <<
  // avg_duration << ")" << std::endl;

  // std::cout << "Threaded outer loop threaded inner loop file I/O on thread pool
  // (FAILS)" << std::endl; DoReadFiles(thread_pool, memory_pool, fs,
  // arrow::internal::TaskGroup::MakeThreaded(), CreateThreadedSyncReadOptions(),
  // aws_bucket_name); std::cout << "  Finished reading in all files (avg=" <<
  // (total_duration.count() / static_cast<double>(NUM_FILES)) << ")" << std::endl;

  std::cout << "Threaded outer loop serial inner loop file I/O on thread pool"
            << std::endl;
  avg_duration =
      DoReadFiles(memory_pool, fs, arrow::internal::TaskGroup::MakeThreaded(thread_pool),
                  CreateSerialSyncReadOptions(), aws_bucket_name);
  std::cout << "  Finished reading in all files (avg=" << avg_duration << ")"
            << std::endl;

  std::cout << "Composable futures method (threaded outer, threaded inner)" << std::endl;
  avg_duration =
      DoReadFiles(memory_pool, fs, arrow::internal::TaskGroup::MakeThreaded(thread_pool),
                  CreateThreadedAsyncReadOptions(), aws_bucket_name);
  std::cout << "  Finished reading in all files (avg=" << avg_duration << ")"
            << std::endl;

  std::cout << "Composable futures method (serial outer, threaded inner)" << std::endl;
  avg_duration = DoReadFiles(memory_pool, fs, arrow::internal::TaskGroup::MakeSerial(),
                             CreateThreadedAsyncReadOptions(), aws_bucket_name);
  std::cout << "  Finished reading in all files (avg=" << avg_duration << ")"
            << std::endl;

  // input_streams.clear();
  // table_readers.clear();
  // total_start = std::chrono::high_resolution_clock::now();
  // auto futures_task_group = arrow::internal::TaskGroup::MakeThreaded(thread_pool);
  // for (int file_index = 0; file_index < NUM_FILES; file_index++) {
  //   auto input_stream = *fs->OpenInputStream("/home/ubuntu/datasets/arrow/csv/" +
  //   std::to_string(file_index) + ".csv"); auto table_reader =
  //   *arrow::csv::TableReader::Make(memory_pool, input_stream,
  //   CreateAsyncIOReadOptions(), CreateParseOptions(), CreateConvertOptions()); auto
  //   start = std::chrono::high_resolution_clock::now();
  //   input_streams.push_back(input_stream);
  //   table_readers.push_back(table_reader);
  //   auto read_table_future = table_reader->ReadAsync();
  //   read_table_future.Then([start, file_index] (const
  //   arrow::Result<std::shared_ptr<arrow::Table>>& result) {
  //     auto end = std::chrono::high_resolution_clock::now();
  //     auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end -
  //     start); if (result.ok()) {
  //       auto table = result.ValueUnsafe();
  //       std::cout << "Finished reading file with " << table->num_rows() << " rows (" <<
  //       file_index << ") " << duration.count() << std::endl;
  //     }
  //   });
  //   futures_task_group->Append(read_table_future);
  // }
  // futures_task_group->Finish();

  // total_end = std::chrono::high_resolution_clock::now();
  // total_duration = std::chrono::duration_cast<std::chrono::milliseconds>(total_end -
  // total_start); std::cout << "  Finished reading in all files (avg=" <<
  // (total_duration.count() / static_cast<double>(NUM_FILES)) << ")" << std::endl;

  return EXIT_SUCCESS;
}
