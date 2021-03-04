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

// A command line executable that generates a bunch of valid IPC files
// containing example tensors.  Those are used as fuzzing seeds to make
// fuzzing more efficient.

#include <cstdlib>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "arrow/io/file.h"
#include "arrow/io/memory.h"
#include "arrow/ipc/test_common.h"
#include "arrow/ipc/writer.h"
#include "arrow/result.h"
#include "arrow/tensor.h"
#include "arrow/util/io_util.h"

namespace arrow {
namespace ipc {

using ::arrow::internal::PlatformFilename;

Result<PlatformFilename> PrepareDirectory(const std::string& dir) {
  ARROW_ASSIGN_OR_RAISE(auto dir_fn, PlatformFilename::FromString(dir));
  RETURN_NOT_OK(::arrow::internal::CreateDir(dir_fn));
  return std::move(dir_fn);
}

Result<std::shared_ptr<Buffer>> MakeSerializedBuffer(
    std::function<Status(const std::shared_ptr<io::BufferOutputStream>&)> fn) {
  ARROW_ASSIGN_OR_RAISE(auto sink, io::BufferOutputStream::Create(1024));
  RETURN_NOT_OK(fn(sink));
  return sink->Finish();
}

Result<std::shared_ptr<Buffer>> SerializeTensor(const std::shared_ptr<Tensor>& tensor) {
  return MakeSerializedBuffer(
      [&](const std::shared_ptr<io::BufferOutputStream>& sink) -> Status {
        int32_t metadata_length;
        int64_t body_length;
        return ipc::WriteTensor(*tensor, sink.get(), &metadata_length, &body_length);
      });
}

Result<std::vector<std::shared_ptr<Tensor>>> Tensors() {
  std::vector<std::shared_ptr<Tensor>> tensors;
  std::shared_ptr<Tensor> tensor;
  std::vector<int64_t> shape = {5, 3, 7};
  std::shared_ptr<DataType> types[] = {int8(),  int16(),  int32(),  int64(),
                                       uint8(), uint16(), uint32(), uint64()};
  uint32_t seed = 0;
  for (auto type : types) {
    RETURN_NOT_OK(
        test::MakeRandomTensor(type, shape, /*row_major_p=*/true, &tensor, seed++));
    tensors.push_back(tensor);
    RETURN_NOT_OK(
        test::MakeRandomTensor(type, shape, /*row_major_p=*/false, &tensor, seed++));
    tensors.push_back(tensor);
  }
  return tensors;
}

Status GenerateTensors(const PlatformFilename& dir_fn) {
  int sample_num = 1;
  auto sample_name = [&]() -> std::string {
    return "tensor-" + std::to_string(sample_num++);
  };

  ARROW_ASSIGN_OR_RAISE(auto tensors, Tensors());

  for (const auto& tensor : tensors) {
    ARROW_ASSIGN_OR_RAISE(auto buf, SerializeTensor(tensor));
    ARROW_ASSIGN_OR_RAISE(auto sample_fn, dir_fn.Join(sample_name()));
    std::cerr << sample_fn.ToString() << std::endl;
    ARROW_ASSIGN_OR_RAISE(auto file, io::FileOutputStream::Open(sample_fn.ToString()));
    RETURN_NOT_OK(file->Write(buf));
    RETURN_NOT_OK(file->Close());
  }
  return Status::OK();
}

Status DoMain(const std::string& out_dir) {
  ARROW_ASSIGN_OR_RAISE(auto dir_fn, PrepareDirectory(out_dir));
  return GenerateTensors(dir_fn);
}

ARROW_NORETURN void Usage() {
  std::cerr << "Usage: arrow-ipc-generate-tensor-fuzz-corpus "
            << "-stream <output directory>" << std::endl;
  std::exit(2);
}

int Main(int argc, char** argv) {
  if (argc != 3) {
    Usage();
  }

  auto opt = std::string(argv[1]);
  if (opt != "-stream") {
    Usage();
  }

  auto out_dir = std::string(argv[2]);

  Status st = DoMain(out_dir);
  if (!st.ok()) {
    std::cerr << st.ToString() << std::endl;
    return 1;
  }
  return 0;
}

}  // namespace ipc
}  // namespace arrow

int main(int argc, char** argv) { return arrow::ipc::Main(argc, argv); }
