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

#include <sstream>

#include <sys/types.h>
#include <unistd.h>

#include <gtest/gtest.h>
#include <memory>

#include "arrow/testing/gtest_util.h"
#include "arrow/util/io-util.h"

#include "plasma/common.h"
#include "plasma/io/connection.h"
#include "plasma/protocol.h"
#include "plasma/test-util.h"

namespace plasma {

using flatbuf::MessageType;
using io::ClientConnection;
using io::ServerConnection;

class TestPlasmaSerialization : public ::testing::Test {
 public:
  void SetUp() override {
    using asio::local::stream_protocol;
    stream_protocol::socket parentSocket(io_context_);
    stream_protocol::socket childSocket(io_context_);
    // create socket pair
    asio::local::connect_pair(childSocket, parentSocket);
    client_ = ServerConnection::Create(std::move(childSocket));
    io::MessageHandler monk_handler = [](std::shared_ptr<ClientConnection> client,
                                         int64_t type, int64_t length,
                                         const uint8_t* msg) {};
    server_ = ClientConnection::Create(std::move(parentSocket), monk_handler);
  }

  void TearDown() override {
    client_->Close();
    server_->Close();
  }

 protected:
  asio::io_context io_context_;
  std::shared_ptr<ServerConnection> client_;
  std::shared_ptr<ClientConnection> server_;
};

Status PlasmaReceive(const std::shared_ptr<ServerConnection>& client,
                     MessageType message_type, std::vector<uint8_t>* buffer) {
  return client->ReadMessage(static_cast<int64_t>(message_type), buffer);
}

Status PlasmaReceive(const std::shared_ptr<ClientConnection>& client,
                     MessageType message_type, std::vector<uint8_t>* buffer) {
  return client->ReadMessage(static_cast<int64_t>(message_type), buffer);
}

PlasmaObject random_plasma_object(void) {
  unsigned int seed = static_cast<unsigned int>(time(NULL));
  int random = rand_r(&seed);
  PlasmaObject object = {};
  object.store_fd = random + 7;
  object.data_offset = random + 1;
  object.metadata_offset = random + 2;
  object.data_size = random + 3;
  object.metadata_size = random + 4;
  object.device_num = 0;
  return object;
}

TEST_F(TestPlasmaSerialization, CreateRequest) {
  ObjectID object_id1 = random_object_id();
  int64_t data_size1 = 42;
  int64_t metadata_size1 = 11;
  int device_num1 = 0;
  ASSERT_OK(
      SendCreateRequest(client_, object_id1, data_size1, metadata_size1, device_num1));
  std::vector<uint8_t> data;
  ASSERT_OK(PlasmaReceive(server_, MessageType::PlasmaCreateRequest, &data));
  ObjectID object_id2;
  int64_t data_size2;
  int64_t metadata_size2;
  int device_num2;
  ASSERT_OK(ReadCreateRequest(data.data(), data.size(), &object_id2, &data_size2,
                              &metadata_size2, &device_num2));
  ASSERT_EQ(data_size1, data_size2);
  ASSERT_EQ(metadata_size1, metadata_size2);
  ASSERT_EQ(object_id1, object_id2);
  ASSERT_EQ(device_num1, device_num2);
}

TEST_F(TestPlasmaSerialization, CreateReply) {
  ObjectID object_id1 = random_object_id();
  PlasmaObject object1 = random_plasma_object();
  int64_t mmap_size1 = 1000000;
  ASSERT_OK(SendCreateReply(server_, object_id1, &object1, PlasmaError::OK, mmap_size1));
  std::vector<uint8_t> data;
  ASSERT_OK(PlasmaReceive(client_, MessageType::PlasmaCreateReply, &data));
  ObjectID object_id2;
  PlasmaObject object2 = {};
  int store_fd;
  int64_t mmap_size2;
  ASSERT_OK(ReadCreateReply(data.data(), data.size(), &object_id2, &object2, &store_fd,
                            &mmap_size2));
  ASSERT_EQ(object_id1, object_id2);
  ASSERT_EQ(object1.store_fd, store_fd);
  ASSERT_EQ(mmap_size1, mmap_size2);
  ASSERT_EQ(memcmp(&object1, &object2, sizeof(object1)), 0);
}

TEST_F(TestPlasmaSerialization, SealRequest) {
  ObjectID object_id1 = random_object_id();
  unsigned char digest1[kDigestSize];
  memset(&digest1[0], 7, kDigestSize);
  ASSERT_OK(SendSealRequest(client_, object_id1, &digest1[0]));
  std::vector<uint8_t> data;
  ASSERT_OK(PlasmaReceive(server_, MessageType::PlasmaSealRequest, &data));
  ObjectID object_id2;
  unsigned char digest2[kDigestSize];
  ASSERT_OK(ReadSealRequest(data.data(), data.size(), &object_id2, &digest2[0]));
  ASSERT_EQ(object_id1, object_id2);
  ASSERT_EQ(memcmp(&digest1[0], &digest2[0], kDigestSize), 0);
}

TEST_F(TestPlasmaSerialization, SealReply) {
  ObjectID object_id1 = random_object_id();
  ASSERT_OK(SendSealReply(server_, object_id1, PlasmaError::ObjectExists));
  std::vector<uint8_t> data;
  ASSERT_OK(PlasmaReceive(client_, MessageType::PlasmaSealReply, &data));
  ObjectID object_id2;
  Status s = ReadSealReply(data.data(), data.size(), &object_id2);
  ASSERT_EQ(object_id1, object_id2);
  ASSERT_TRUE(IsPlasmaObjectExists(s));
}

TEST_F(TestPlasmaSerialization, GetRequest) {
  ObjectID object_ids[2];
  object_ids[0] = random_object_id();
  object_ids[1] = random_object_id();
  int64_t timeout_ms = 1234;
  ASSERT_OK(SendGetRequest(client_, object_ids, 2, timeout_ms));
  std::vector<uint8_t> data;
  ASSERT_OK(PlasmaReceive(server_, MessageType::PlasmaGetRequest, &data));
  std::vector<ObjectID> object_ids_return;
  int64_t timeout_ms_return;
  ASSERT_OK(
      ReadGetRequest(data.data(), data.size(), object_ids_return, &timeout_ms_return));
  ASSERT_EQ(object_ids[0], object_ids_return[0]);
  ASSERT_EQ(object_ids[1], object_ids_return[1]);
  ASSERT_EQ(timeout_ms, timeout_ms_return);
}

TEST_F(TestPlasmaSerialization, GetReply) {
  ObjectID object_ids[2];
  object_ids[0] = random_object_id();
  object_ids[1] = random_object_id();
  std::unordered_map<ObjectID, PlasmaObject> plasma_objects;
  plasma_objects[object_ids[0]] = random_plasma_object();
  plasma_objects[object_ids[1]] = random_plasma_object();
  std::vector<int> store_fds = {1, 2, 3};
  std::vector<int64_t> mmap_sizes = {100, 200, 300};
  ASSERT_OK(SendGetReply(server_, object_ids, plasma_objects, 2, store_fds, mmap_sizes));

  std::vector<uint8_t> data;
  ASSERT_OK(PlasmaReceive(client_, MessageType::PlasmaGetReply, &data));
  ObjectID object_ids_return[2];
  PlasmaObject plasma_objects_return[2];
  std::vector<int> store_fds_return;
  std::vector<int64_t> mmap_sizes_return;
  memset(&plasma_objects_return, 0, sizeof(plasma_objects_return));
  ASSERT_OK(ReadGetReply(data.data(), data.size(), object_ids_return,
                         &plasma_objects_return[0], 2, store_fds_return,
                         mmap_sizes_return));

  ASSERT_EQ(object_ids[0], object_ids_return[0]);
  ASSERT_EQ(object_ids[1], object_ids_return[1]);

  PlasmaObject po, po2;
  for (int i = 0; i < 2; ++i) {
    po = plasma_objects[object_ids[i]];
    po2 = plasma_objects_return[i];
    ASSERT_EQ(po, po2);
  }
  ASSERT_TRUE(store_fds == store_fds_return);
  ASSERT_TRUE(mmap_sizes == mmap_sizes_return);
}

TEST_F(TestPlasmaSerialization, ReleaseRequest) {
  ObjectID object_id1 = random_object_id();
  ASSERT_OK(SendReleaseRequest(client_, object_id1));
  std::vector<uint8_t> data;
  ASSERT_OK(PlasmaReceive(server_, MessageType::PlasmaReleaseRequest, &data));
  ObjectID object_id2;
  ASSERT_OK(ReadReleaseRequest(data.data(), data.size(), &object_id2));
  ASSERT_EQ(object_id1, object_id2);
}

TEST_F(TestPlasmaSerialization, ReleaseReply) {
  ObjectID object_id1 = random_object_id();
  ASSERT_OK(SendReleaseReply(server_, object_id1, PlasmaError::ObjectExists));
  std::vector<uint8_t> data;
  ASSERT_OK(PlasmaReceive(client_, MessageType::PlasmaReleaseReply, &data));
  ObjectID object_id2;
  Status s = ReadReleaseReply(data.data(), data.size(), &object_id2);
  ASSERT_EQ(object_id1, object_id2);
  ASSERT_TRUE(IsPlasmaObjectExists(s));
}

TEST_F(TestPlasmaSerialization, DeleteRequest) {
  ObjectID object_id1 = random_object_id();
  ASSERT_OK(SendDeleteRequest(client_, std::vector<ObjectID>{object_id1}));
  std::vector<uint8_t> data;
  ASSERT_OK(PlasmaReceive(server_, MessageType::PlasmaDeleteRequest, &data));
  std::vector<ObjectID> object_vec;
  ASSERT_OK(ReadDeleteRequest(data.data(), data.size(), &object_vec));
  ASSERT_EQ(object_vec.size(), 1);
  ASSERT_EQ(object_id1, object_vec[0]);
}

TEST_F(TestPlasmaSerialization, DeleteReply) {
  ObjectID object_id1 = random_object_id();
  PlasmaError error1 = PlasmaError::ObjectExists;
  ASSERT_OK(SendDeleteReply(server_, std::vector<ObjectID>{object_id1},
                            std::vector<PlasmaError>{error1}));
  std::vector<uint8_t> data;
  ASSERT_OK(PlasmaReceive(client_, MessageType::PlasmaDeleteReply, &data));
  std::vector<ObjectID> object_vec;
  std::vector<PlasmaError> error_vec;
  Status s = ReadDeleteReply(data.data(), data.size(), &object_vec, &error_vec);
  ASSERT_EQ(object_vec.size(), 1);
  ASSERT_EQ(object_id1, object_vec[0]);
  ASSERT_EQ(error_vec.size(), 1);
  ASSERT_TRUE(error_vec[0] == PlasmaError::ObjectExists);
  ASSERT_TRUE(s.ok());
}

TEST_F(TestPlasmaSerialization, EvictRequest) {
  int64_t num_bytes = 111;
  ASSERT_OK(SendEvictRequest(client_, num_bytes));
  std::vector<uint8_t> data;
  ASSERT_OK(PlasmaReceive(server_, MessageType::PlasmaEvictRequest, &data));
  int64_t num_bytes_received;
  ASSERT_OK(ReadEvictRequest(data.data(), data.size(), &num_bytes_received));
  ASSERT_EQ(num_bytes, num_bytes_received);
}

TEST_F(TestPlasmaSerialization, EvictReply) {
  int64_t num_bytes = 111;
  ASSERT_OK(SendEvictReply(server_, num_bytes));
  std::vector<uint8_t> data;
  ASSERT_OK(PlasmaReceive(client_, MessageType::PlasmaEvictReply, &data));
  int64_t num_bytes_received;
  ASSERT_OK(ReadEvictReply(data.data(), data.size(), num_bytes_received));
  ASSERT_EQ(num_bytes, num_bytes_received);
}

TEST_F(TestPlasmaSerialization, DataRequest) {
  ObjectID object_id1 = random_object_id();
  const char* address1 = "address1";
  int port1 = 12345;
  ASSERT_OK(SendDataRequest(client_, object_id1, address1, port1));
  /* Reading message back. */
  std::vector<uint8_t> data;
  ASSERT_OK(PlasmaReceive(server_, MessageType::PlasmaDataRequest, &data));
  ObjectID object_id2;
  char* address2;
  int port2;
  ASSERT_OK(ReadDataRequest(data.data(), data.size(), &object_id2, &address2, &port2));
  ASSERT_EQ(object_id1, object_id2);
  ASSERT_EQ(strcmp(address1, address2), 0);
  ASSERT_EQ(port1, port2);
  free(address2);
}

TEST_F(TestPlasmaSerialization, DataReply) {
  ObjectID object_id1 = random_object_id();
  int64_t object_size1 = 146;
  int64_t metadata_size1 = 198;
  ASSERT_OK(SendDataReply(server_, object_id1, object_size1, metadata_size1));
  /* Reading message back. */
  std::vector<uint8_t> data;
  ASSERT_OK(PlasmaReceive(client_, MessageType::PlasmaDataReply, &data));
  ObjectID object_id2;
  int64_t object_size2;
  int64_t metadata_size2;
  ASSERT_OK(ReadDataReply(data.data(), data.size(), &object_id2, &object_size2,
                          &metadata_size2));
  ASSERT_EQ(object_id1, object_id2);
  ASSERT_EQ(object_size1, object_size2);
  ASSERT_EQ(metadata_size1, metadata_size2);
}

}  // namespace plasma
