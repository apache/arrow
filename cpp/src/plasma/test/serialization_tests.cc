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

#include "gtest/gtest.h"

#include <sys/types.h>
#include <unistd.h>

#include "plasma/common.h"
#include "plasma/io.h"
#include "plasma/plasma.h"
#include "plasma/protocol.h"

namespace plasma {

/**
 * Create a temporary file. Needs to be closed by the caller.
 *
 * @return File descriptor of the file.
 */
int create_temp_file(void) {
  static char temp[] = "/tmp/tempfileXXXXXX";
  char file_name[32];
  strncpy(file_name, temp, 32);
  return mkstemp(file_name);
}

/**
 * Seek to the beginning of a file and read a message from it.
 *
 * @param fd File descriptor of the file.
 * @param message_type Message type that we expect in the file.
 *
 * @return Pointer to the content of the message. Needs to be freed by the
 * caller.
 */
std::vector<uint8_t> read_message_from_file(int fd, int message_type) {
  /* Go to the beginning of the file. */
  lseek(fd, 0, SEEK_SET);
  int64_t type;
  std::vector<uint8_t> data;
  ARROW_CHECK_OK(ReadMessage(fd, &type, &data));
  ARROW_CHECK(type == message_type);
  return data;
}

PlasmaObject random_plasma_object(void) {
  unsigned int seed = static_cast<unsigned int>(time(NULL));
  int random = rand_r(&seed);
  PlasmaObject object;
  memset(&object, 0, sizeof(object));
  object.handle.store_fd = random + 7;
  object.handle.mmap_size = random + 42;
  object.data_offset = random + 1;
  object.metadata_offset = random + 2;
  object.data_size = random + 3;
  object.metadata_size = random + 4;
  return object;
}

TEST(PlasmaSerialization, CreateRequest) {
  int fd = create_temp_file();
  ObjectID object_id1 = ObjectID::from_random();
  int64_t data_size1 = 42;
  int64_t metadata_size1 = 11;
  ARROW_CHECK_OK(SendCreateRequest(fd, object_id1, data_size1, metadata_size1));
  std::vector<uint8_t> data = read_message_from_file(fd, MessageType_PlasmaCreateRequest);
  ObjectID object_id2;
  int64_t data_size2;
  int64_t metadata_size2;
  ARROW_CHECK_OK(ReadCreateRequest(data.data(), data.size(), &object_id2, &data_size2,
                                   &metadata_size2));
  ASSERT_EQ(data_size1, data_size2);
  ASSERT_EQ(metadata_size1, metadata_size2);
  ASSERT_EQ(object_id1, object_id2);
  close(fd);
}

TEST(PlasmaSerialization, CreateReply) {
  int fd = create_temp_file();
  ObjectID object_id1 = ObjectID::from_random();
  PlasmaObject object1 = random_plasma_object();
  ARROW_CHECK_OK(SendCreateReply(fd, object_id1, &object1, 0));
  std::vector<uint8_t> data = read_message_from_file(fd, MessageType_PlasmaCreateReply);
  ObjectID object_id2;
  PlasmaObject object2;
  memset(&object2, 0, sizeof(object2));
  ARROW_CHECK_OK(ReadCreateReply(data.data(), data.size(), &object_id2, &object2));
  ASSERT_EQ(object_id1, object_id2);
  ASSERT_EQ(memcmp(&object1, &object2, sizeof(object1)), 0);
  close(fd);
}

TEST(PlasmaSerialization, SealRequest) {
  int fd = create_temp_file();
  ObjectID object_id1 = ObjectID::from_random();
  unsigned char digest1[kDigestSize];
  memset(&digest1[0], 7, kDigestSize);
  ARROW_CHECK_OK(SendSealRequest(fd, object_id1, &digest1[0]));
  std::vector<uint8_t> data = read_message_from_file(fd, MessageType_PlasmaSealRequest);
  ObjectID object_id2;
  unsigned char digest2[kDigestSize];
  ARROW_CHECK_OK(ReadSealRequest(data.data(), data.size(), &object_id2, &digest2[0]));
  ASSERT_EQ(object_id1, object_id2);
  ASSERT_EQ(memcmp(&digest1[0], &digest2[0], kDigestSize), 0);
  close(fd);
}

TEST(PlasmaSerialization, SealReply) {
  int fd = create_temp_file();
  ObjectID object_id1 = ObjectID::from_random();
  ARROW_CHECK_OK(SendSealReply(fd, object_id1, PlasmaError_ObjectExists));
  std::vector<uint8_t> data = read_message_from_file(fd, MessageType_PlasmaSealReply);
  ObjectID object_id2;
  Status s = ReadSealReply(data.data(), data.size(), &object_id2);
  ASSERT_EQ(object_id1, object_id2);
  ASSERT_TRUE(s.IsPlasmaObjectExists());
  close(fd);
}

TEST(PlasmaSerialization, GetRequest) {
  int fd = create_temp_file();
  ObjectID object_ids[2];
  object_ids[0] = ObjectID::from_random();
  object_ids[1] = ObjectID::from_random();
  int64_t timeout_ms = 1234;
  ARROW_CHECK_OK(SendGetRequest(fd, object_ids, 2, timeout_ms));
  std::vector<uint8_t> data = read_message_from_file(fd, MessageType_PlasmaGetRequest);
  std::vector<ObjectID> object_ids_return;
  int64_t timeout_ms_return;
  ARROW_CHECK_OK(
      ReadGetRequest(data.data(), data.size(), object_ids_return, &timeout_ms_return));
  ASSERT_EQ(object_ids[0], object_ids_return[0]);
  ASSERT_EQ(object_ids[1], object_ids_return[1]);
  ASSERT_EQ(timeout_ms, timeout_ms_return);
  close(fd);
}

TEST(PlasmaSerialization, GetReply) {
  int fd = create_temp_file();
  ObjectID object_ids[2];
  object_ids[0] = ObjectID::from_random();
  object_ids[1] = ObjectID::from_random();
  std::unordered_map<ObjectID, PlasmaObject, UniqueIDHasher> plasma_objects;
  plasma_objects[object_ids[0]] = random_plasma_object();
  plasma_objects[object_ids[1]] = random_plasma_object();
  ARROW_CHECK_OK(SendGetReply(fd, object_ids, plasma_objects, 2));
  std::vector<uint8_t> data = read_message_from_file(fd, MessageType_PlasmaGetReply);
  ObjectID object_ids_return[2];
  PlasmaObject plasma_objects_return[2];
  memset(&plasma_objects_return, 0, sizeof(plasma_objects_return));
  ARROW_CHECK_OK(ReadGetReply(data.data(), data.size(), object_ids_return,
                              &plasma_objects_return[0], 2));
  ASSERT_EQ(object_ids[0], object_ids_return[0]);
  ASSERT_EQ(object_ids[1], object_ids_return[1]);
  ASSERT_EQ(memcmp(&plasma_objects[object_ids[0]], &plasma_objects_return[0],
                   sizeof(PlasmaObject)),
            0);
  ASSERT_EQ(memcmp(&plasma_objects[object_ids[1]], &plasma_objects_return[1],
                   sizeof(PlasmaObject)),
            0);
  close(fd);
}

TEST(PlasmaSerialization, ReleaseRequest) {
  int fd = create_temp_file();
  ObjectID object_id1 = ObjectID::from_random();
  ARROW_CHECK_OK(SendReleaseRequest(fd, object_id1));
  std::vector<uint8_t> data =
      read_message_from_file(fd, MessageType_PlasmaReleaseRequest);
  ObjectID object_id2;
  ARROW_CHECK_OK(ReadReleaseRequest(data.data(), data.size(), &object_id2));
  ASSERT_EQ(object_id1, object_id2);
  close(fd);
}

TEST(PlasmaSerialization, ReleaseReply) {
  int fd = create_temp_file();
  ObjectID object_id1 = ObjectID::from_random();
  ARROW_CHECK_OK(SendReleaseReply(fd, object_id1, PlasmaError_ObjectExists));
  std::vector<uint8_t> data = read_message_from_file(fd, MessageType_PlasmaReleaseReply);
  ObjectID object_id2;
  Status s = ReadReleaseReply(data.data(), data.size(), &object_id2);
  ASSERT_EQ(object_id1, object_id2);
  ASSERT_TRUE(s.IsPlasmaObjectExists());
  close(fd);
}

TEST(PlasmaSerialization, DeleteRequest) {
  int fd = create_temp_file();
  ObjectID object_id1 = ObjectID::from_random();
  ARROW_CHECK_OK(SendDeleteRequest(fd, object_id1));
  std::vector<uint8_t> data = read_message_from_file(fd, MessageType_PlasmaDeleteRequest);
  ObjectID object_id2;
  ARROW_CHECK_OK(ReadDeleteRequest(data.data(), data.size(), &object_id2));
  ASSERT_EQ(object_id1, object_id2);
  close(fd);
}

TEST(PlasmaSerialization, DeleteReply) {
  int fd = create_temp_file();
  ObjectID object_id1 = ObjectID::from_random();
  int error1 = PlasmaError_ObjectExists;
  ARROW_CHECK_OK(SendDeleteReply(fd, object_id1, error1));
  std::vector<uint8_t> data = read_message_from_file(fd, MessageType_PlasmaDeleteReply);
  ObjectID object_id2;
  Status s = ReadDeleteReply(data.data(), data.size(), &object_id2);
  ASSERT_EQ(object_id1, object_id2);
  ASSERT_TRUE(s.IsPlasmaObjectExists());
  close(fd);
}

TEST(PlasmaSerialization, StatusRequest) {
  int fd = create_temp_file();
  constexpr int64_t num_objects = 2;
  ObjectID object_ids[num_objects];
  object_ids[0] = ObjectID::from_random();
  object_ids[1] = ObjectID::from_random();
  ARROW_CHECK_OK(SendStatusRequest(fd, object_ids, num_objects));
  std::vector<uint8_t> data = read_message_from_file(fd, MessageType_PlasmaStatusRequest);
  ObjectID object_ids_read[num_objects];
  ARROW_CHECK_OK(
      ReadStatusRequest(data.data(), data.size(), object_ids_read, num_objects));
  ASSERT_EQ(object_ids[0], object_ids_read[0]);
  ASSERT_EQ(object_ids[1], object_ids_read[1]);
  close(fd);
}

TEST(PlasmaSerialization, StatusReply) {
  int fd = create_temp_file();
  ObjectID object_ids[2];
  object_ids[0] = ObjectID::from_random();
  object_ids[1] = ObjectID::from_random();
  int object_statuses[2] = {42, 43};
  ARROW_CHECK_OK(SendStatusReply(fd, object_ids, object_statuses, 2));
  std::vector<uint8_t> data = read_message_from_file(fd, MessageType_PlasmaStatusReply);
  int64_t num_objects = ReadStatusReply_num_objects(data.data(), data.size());

  std::vector<ObjectID> object_ids_read(num_objects);
  std::vector<int> object_statuses_read(num_objects);
  ARROW_CHECK_OK(ReadStatusReply(data.data(), data.size(), object_ids_read.data(),
                                 object_statuses_read.data(), num_objects));
  ASSERT_EQ(object_ids[0], object_ids_read[0]);
  ASSERT_EQ(object_ids[1], object_ids_read[1]);
  ASSERT_EQ(object_statuses[0], object_statuses_read[0]);
  ASSERT_EQ(object_statuses[1], object_statuses_read[1]);
  close(fd);
}

TEST(PlasmaSerialization, EvictRequest) {
  int fd = create_temp_file();
  int64_t num_bytes = 111;
  ARROW_CHECK_OK(SendEvictRequest(fd, num_bytes));
  std::vector<uint8_t> data = read_message_from_file(fd, MessageType_PlasmaEvictRequest);
  int64_t num_bytes_received;
  ARROW_CHECK_OK(ReadEvictRequest(data.data(), data.size(), &num_bytes_received));
  ASSERT_EQ(num_bytes, num_bytes_received);
  close(fd);
}

TEST(PlasmaSerialization, EvictReply) {
  int fd = create_temp_file();
  int64_t num_bytes = 111;
  ARROW_CHECK_OK(SendEvictReply(fd, num_bytes));
  std::vector<uint8_t> data = read_message_from_file(fd, MessageType_PlasmaEvictReply);
  int64_t num_bytes_received;
  ARROW_CHECK_OK(ReadEvictReply(data.data(), data.size(), num_bytes_received));
  ASSERT_EQ(num_bytes, num_bytes_received);
  close(fd);
}

TEST(PlasmaSerialization, FetchRequest) {
  int fd = create_temp_file();
  ObjectID object_ids[2];
  object_ids[0] = ObjectID::from_random();
  object_ids[1] = ObjectID::from_random();
  ARROW_CHECK_OK(SendFetchRequest(fd, object_ids, 2));
  std::vector<uint8_t> data = read_message_from_file(fd, MessageType_PlasmaFetchRequest);
  std::vector<ObjectID> object_ids_read;
  ARROW_CHECK_OK(ReadFetchRequest(data.data(), data.size(), object_ids_read));
  ASSERT_EQ(object_ids[0], object_ids_read[0]);
  ASSERT_EQ(object_ids[1], object_ids_read[1]);
  close(fd);
}

TEST(PlasmaSerialization, WaitRequest) {
  int fd = create_temp_file();
  const int num_objects_in = 2;
  ObjectRequest object_requests_in[num_objects_in] = {
      ObjectRequest({ObjectID::from_random(), PLASMA_QUERY_ANYWHERE, 0}),
      ObjectRequest({ObjectID::from_random(), PLASMA_QUERY_LOCAL, 0})};
  const int num_ready_objects_in = 1;
  int64_t timeout_ms = 1000;

  ARROW_CHECK_OK(SendWaitRequest(fd, &object_requests_in[0], num_objects_in,
                                 num_ready_objects_in, timeout_ms));
  /* Read message back. */
  std::vector<uint8_t> data = read_message_from_file(fd, MessageType_PlasmaWaitRequest);
  int num_ready_objects_out;
  int64_t timeout_ms_read;
  ObjectRequestMap object_requests_out;
  ARROW_CHECK_OK(ReadWaitRequest(data.data(), data.size(), object_requests_out,
                                 &timeout_ms_read, &num_ready_objects_out));
  ASSERT_EQ(num_objects_in, object_requests_out.size());
  ASSERT_EQ(num_ready_objects_out, num_ready_objects_in);
  for (int i = 0; i < num_objects_in; i++) {
    const ObjectID& object_id = object_requests_in[i].object_id;
    ASSERT_EQ(1, object_requests_out.count(object_id));
    const auto& entry = object_requests_out.find(object_id);
    ASSERT_TRUE(entry != object_requests_out.end());
    ASSERT_EQ(entry->second.object_id, object_requests_in[i].object_id);
    ASSERT_EQ(entry->second.type, object_requests_in[i].type);
  }
  close(fd);
}

TEST(PlasmaSerialization, WaitReply) {
  int fd = create_temp_file();
  const int num_objects_in = 2;
  /* Create a map with two ObjectRequests in it. */
  ObjectRequestMap objects_in(num_objects_in);
  ObjectID id1 = ObjectID::from_random();
  objects_in[id1] = ObjectRequest({id1, 0, ObjectStatus_Local});
  ObjectID id2 = ObjectID::from_random();
  objects_in[id2] = ObjectRequest({id2, 0, ObjectStatus_Nonexistent});

  ARROW_CHECK_OK(SendWaitReply(fd, objects_in, num_objects_in));
  /* Read message back. */
  std::vector<uint8_t> data = read_message_from_file(fd, MessageType_PlasmaWaitReply);
  ObjectRequest objects_out[2];
  int num_objects_out;
  ARROW_CHECK_OK(
      ReadWaitReply(data.data(), data.size(), &objects_out[0], &num_objects_out));
  ASSERT_EQ(num_objects_in, num_objects_out);
  for (int i = 0; i < num_objects_out; i++) {
    /* Each object request must appear exactly once. */
    ASSERT_EQ(objects_in.count(objects_out[i].object_id), 1);
    const auto& entry = objects_in.find(objects_out[i].object_id);
    ASSERT_TRUE(entry != objects_in.end());
    ASSERT_EQ(entry->second.object_id, objects_out[i].object_id);
    ASSERT_EQ(entry->second.status, objects_out[i].status);
  }
  close(fd);
}

TEST(PlasmaSerialization, DataRequest) {
  int fd = create_temp_file();
  ObjectID object_id1 = ObjectID::from_random();
  const char* address1 = "address1";
  int port1 = 12345;
  ARROW_CHECK_OK(SendDataRequest(fd, object_id1, address1, port1));
  /* Reading message back. */
  std::vector<uint8_t> data = read_message_from_file(fd, MessageType_PlasmaDataRequest);
  ObjectID object_id2;
  char* address2;
  int port2;
  ARROW_CHECK_OK(
      ReadDataRequest(data.data(), data.size(), &object_id2, &address2, &port2));
  ASSERT_EQ(object_id1, object_id2);
  ASSERT_EQ(strcmp(address1, address2), 0);
  ASSERT_EQ(port1, port2);
  free(address2);
  close(fd);
}

TEST(PlasmaSerialization, DataReply) {
  int fd = create_temp_file();
  ObjectID object_id1 = ObjectID::from_random();
  int64_t object_size1 = 146;
  int64_t metadata_size1 = 198;
  ARROW_CHECK_OK(SendDataReply(fd, object_id1, object_size1, metadata_size1));
  /* Reading message back. */
  std::vector<uint8_t> data = read_message_from_file(fd, MessageType_PlasmaDataReply);
  ObjectID object_id2;
  int64_t object_size2;
  int64_t metadata_size2;
  ARROW_CHECK_OK(ReadDataReply(data.data(), data.size(), &object_id2, &object_size2,
                               &metadata_size2));
  ASSERT_EQ(object_id1, object_id2);
  ASSERT_EQ(object_size1, object_size2);
  ASSERT_EQ(metadata_size1, metadata_size2);
}

}  // namespace plasma
