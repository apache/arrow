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

#include "arrow/io/memory.h"
#include "arrow/ipc/writer.h"
#include "arrow/tensor.h"

#include "plasma/client.h"

void PlasmaIntegrationWriteTensor(const char* plasma_socket) {
  plasma::PlasmaClient client;
  ARROW_CHECK_OK(client.Connect(plasma_socket, ""));

  // Create a tensor
  int64_t input_length = 1000;
  std::vector<float> input(input_length);
  for (int64_t i = 0; i < input_length; ++i) {
    input[i] = 2.0;
  }
  const uint8_t* bytes_array = reinterpret_cast<const uint8_t*>(input.data());
  auto value_buffer = std::make_shared<arrow::Buffer>(bytes_array, sizeof(float) * input_length);
  arrow::Tensor t(arrow::float32(), value_buffer, {input_length});

  // Get the size of tensor for Plasma
  int64_t datasize;
  ARROW_CHECK_OK(arrow::ipc::GetTensorSize(t, &datasize));

  plasma::ObjectID object_id = plasma::ObjectID::from_binary("11111111111111111111");
  std::shared_ptr<arrow::Buffer> buffer;
  ARROW_CHECK_OK(
    client.Create(object_id, datasize, NULL, 0, &buffer));

  int32_t meta_len = 0;
  arrow::io::FixedSizeBufferWriter stream(buffer);
  ARROW_CHECK_OK(arrow::ipc::WriteTensor(t, &stream, &meta_len, &datasize));

  ARROW_CHECK_OK(client.Seal(object_id));
}

int main(int argc, char** argv) {
  ARROW_CHECK(argc == 2);
  PlasmaIntegrationWriteTensor(argv[1]);
}
