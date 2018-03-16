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

#include <assert.h>
#include <signal.h>
#include <stdlib.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>

#include <random>

#include "plasma/client.h"
#include "plasma/common.h"
#include "plasma/plasma.h"
#include "plasma/protocol.h"

#include "gtest/gtest.h"

namespace plasma {

std::string test_executable;  // NOLINT

class TestPlasmaStore : public ::testing::Test {
 public:
  // TODO(pcm): At the moment, stdout of the test gets mixed up with
  // stdout of the object store. Consider changing that.
  void SetUp() {
    std::mt19937 rng;
    rng.seed(std::random_device()());
    std::string store_index = std::to_string(rng());

    std::string plasma_directory =
        test_executable.substr(0, test_executable.find_last_of("/"));
    std::string plasma_command = plasma_directory +
                                 "/plasma_store -m 1000000000 -s /tmp/store" +
                                 store_index + " 1> /dev/null 2> /dev/null &";
    system(plasma_command.c_str());
    ARROW_CHECK_OK(
        client_.Connect("/tmp/store" + store_index, "", PLASMA_DEFAULT_RELEASE_DELAY));
    ARROW_CHECK_OK(
        client2_.Connect("/tmp/store" + store_index, "", PLASMA_DEFAULT_RELEASE_DELAY));
  }
  virtual void Finish() {
    ARROW_CHECK_OK(client_.Disconnect());
    ARROW_CHECK_OK(client2_.Disconnect());
    system("killall plasma_store &");
  }

 protected:
  PlasmaClient client_;
  PlasmaClient client2_;
};

TEST_F(TestPlasmaStore, DeleteTest) {
  ObjectID object_id = ObjectID::from_random();

  // Test for deleting non-existance object.
  Status result = client_.Delete(object_id);
  ASSERT_EQ(result.IsPlasmaObjectNonexistent(), true);

  // Test for the object being in local Plasma store.
  // First create object.
  int64_t data_size = 100;
  uint8_t metadata[] = {5};
  int64_t metadata_size = sizeof(metadata);
  std::shared_ptr<Buffer> data;
  ARROW_CHECK_OK(client_.Create(object_id, data_size, metadata, metadata_size, &data));
  ARROW_CHECK_OK(client_.Seal(object_id));

  // Object is in use, can't be delete.
  result = client_.Delete(object_id);
  ASSERT_EQ(result.IsUnknownError(), true);

  // Avoid race condition of Plasma Manager waiting for notification.
  ARROW_CHECK_OK(client_.Release(object_id));
  ARROW_CHECK_OK(client_.Delete(object_id));
}

TEST_F(TestPlasmaStore, ContainsTest) {
  ObjectID object_id = ObjectID::from_random();

  // Test for object non-existence.
  bool has_object;
  ARROW_CHECK_OK(client_.Contains(object_id, &has_object));
  ASSERT_EQ(has_object, false);

  // Test for the object being in local Plasma store.
  // First create object.
  int64_t data_size = 100;
  uint8_t metadata[] = {5};
  int64_t metadata_size = sizeof(metadata);
  std::shared_ptr<Buffer> data;
  ARROW_CHECK_OK(client_.Create(object_id, data_size, metadata, metadata_size, &data));
  ARROW_CHECK_OK(client_.Seal(object_id));
  // Avoid race condition of Plasma Manager waiting for notification.
  ObjectBuffer object_buffer;
  ARROW_CHECK_OK(client_.Get(&object_id, 1, -1, &object_buffer));
  ARROW_CHECK_OK(client_.Contains(object_id, &has_object));
  ASSERT_EQ(has_object, true);
}

TEST_F(TestPlasmaStore, GetTest) {
  ObjectID object_id = ObjectID::from_random();
  ObjectBuffer object_buffer;

  // Test for object non-existence.
  ARROW_CHECK_OK(client_.Get(&object_id, 1, 0, &object_buffer));
  ASSERT_EQ(object_buffer.data_size, -1);

  // Test for the object being in local Plasma store.
  // First create object.
  int64_t data_size = 4;
  uint8_t metadata[] = {5};
  int64_t metadata_size = sizeof(metadata);
  std::shared_ptr<Buffer> data_buffer;
  uint8_t* data;
  ARROW_CHECK_OK(
      client_.Create(object_id, data_size, metadata, metadata_size, &data_buffer));
  data = data_buffer->mutable_data();
  for (int64_t i = 0; i < data_size; i++) {
    data[i] = static_cast<uint8_t>(i % 4);
  }
  ARROW_CHECK_OK(client_.Seal(object_id));

  ARROW_CHECK_OK(client_.Get(&object_id, 1, -1, &object_buffer));
  const uint8_t* object_data = object_buffer.data->data();
  for (int64_t i = 0; i < data_size; i++) {
    ASSERT_EQ(data[i], object_data[i]);
  }
}

TEST_F(TestPlasmaStore, MultipleGetTest) {
  ObjectID object_id1 = ObjectID::from_random();
  ObjectID object_id2 = ObjectID::from_random();
  ObjectID object_ids[2] = {object_id1, object_id2};
  ObjectBuffer object_buffer[2];

  int64_t data_size = 4;
  uint8_t metadata[] = {5};
  int64_t metadata_size = sizeof(metadata);
  std::shared_ptr<Buffer> data;
  ARROW_CHECK_OK(client_.Create(object_id1, data_size, metadata, metadata_size, &data));
  data->mutable_data()[0] = 1;
  ARROW_CHECK_OK(client_.Seal(object_id1));

  ARROW_CHECK_OK(client_.Create(object_id2, data_size, metadata, metadata_size, &data));
  data->mutable_data()[0] = 2;
  ARROW_CHECK_OK(client_.Seal(object_id2));

  ARROW_CHECK_OK(client_.Get(object_ids, 2, -1, object_buffer));
  ASSERT_EQ(object_buffer[0].data->data()[0], 1);
  ASSERT_EQ(object_buffer[1].data->data()[0], 2);
}

TEST_F(TestPlasmaStore, AbortTest) {
  ObjectID object_id = ObjectID::from_random();
  ObjectBuffer object_buffer;

  // Test for object non-existence.
  ARROW_CHECK_OK(client_.Get(&object_id, 1, 0, &object_buffer));
  ASSERT_EQ(object_buffer.data_size, -1);

  // Test object abort.
  // First create object.
  int64_t data_size = 4;
  uint8_t metadata[] = {5};
  int64_t metadata_size = sizeof(metadata);
  std::shared_ptr<Buffer> data;
  uint8_t* data_ptr;
  ARROW_CHECK_OK(client_.Create(object_id, data_size, metadata, metadata_size, &data));
  data_ptr = data->mutable_data();
  // Write some data.
  for (int64_t i = 0; i < data_size / 2; i++) {
    data_ptr[i] = static_cast<uint8_t>(i % 4);
  }
  // Attempt to abort. Test that this fails before the first release.
  Status status = client_.Abort(object_id);
  ASSERT_TRUE(status.IsInvalid());
  // Release, then abort.
  ARROW_CHECK_OK(client_.Release(object_id));
  ARROW_CHECK_OK(client_.Abort(object_id));

  // Test for object non-existence after the abort.
  ARROW_CHECK_OK(client_.Get(&object_id, 1, 0, &object_buffer));
  ASSERT_EQ(object_buffer.data_size, -1);

  // Create the object successfully this time.
  ARROW_CHECK_OK(client_.Create(object_id, data_size, metadata, metadata_size, &data));
  data_ptr = data->mutable_data();
  for (int64_t i = 0; i < data_size; i++) {
    data_ptr[i] = static_cast<uint8_t>(i % 4);
  }
  ARROW_CHECK_OK(client_.Seal(object_id));

  // Test that we can get the object.
  ARROW_CHECK_OK(client_.Get(&object_id, 1, -1, &object_buffer));
  const uint8_t* buffer_ptr = object_buffer.data->data();
  for (int64_t i = 0; i < data_size; i++) {
    ASSERT_EQ(data_ptr[i], buffer_ptr[i]);
  }
}

TEST_F(TestPlasmaStore, MultipleClientTest) {
  ObjectID object_id = ObjectID::from_random();

  // Test for object non-existence on the first client.
  bool has_object;
  ARROW_CHECK_OK(client_.Contains(object_id, &has_object));
  ASSERT_EQ(has_object, false);

  // Test for the object being in local Plasma store.
  // First create and seal object on the second client.
  int64_t data_size = 100;
  uint8_t metadata[] = {5};
  int64_t metadata_size = sizeof(metadata);
  std::shared_ptr<Buffer> data;
  ARROW_CHECK_OK(client2_.Create(object_id, data_size, metadata, metadata_size, &data));
  ARROW_CHECK_OK(client2_.Seal(object_id));
  // Test that the first client can get the object.
  ObjectBuffer object_buffer;
  ARROW_CHECK_OK(client_.Get(&object_id, 1, -1, &object_buffer));
  ARROW_CHECK_OK(client_.Contains(object_id, &has_object));
  ASSERT_EQ(has_object, true);

  // Test that one client disconnecting does not interfere with the other.
  // First create object on the second client.
  object_id = ObjectID::from_random();
  ARROW_CHECK_OK(client2_.Create(object_id, data_size, metadata, metadata_size, &data));
  // Disconnect the first client.
  ARROW_CHECK_OK(client_.Disconnect());
  // Test that the second client can seal and get the created object.
  ARROW_CHECK_OK(client2_.Seal(object_id));
  ARROW_CHECK_OK(client2_.Get(&object_id, 1, -1, &object_buffer));
  ARROW_CHECK_OK(client2_.Contains(object_id, &has_object));
  ASSERT_EQ(has_object, true);
}

TEST_F(TestPlasmaStore, ManyObjectTest) {
  // Create many objects on the first client. Seal one third, abort one third,
  // and leave the last third unsealed.
  std::vector<ObjectID> object_ids;
  for (int i = 0; i < 100; i++) {
    ObjectID object_id = ObjectID::from_random();
    object_ids.push_back(object_id);

    // Test for object non-existence on the first client.
    bool has_object;
    ARROW_CHECK_OK(client_.Contains(object_id, &has_object));
    ASSERT_EQ(has_object, false);

    // Test for the object being in local Plasma store.
    // First create and seal object on the first client.
    int64_t data_size = 100;
    uint8_t metadata[] = {5};
    int64_t metadata_size = sizeof(metadata);
    std::shared_ptr<Buffer> data;
    ARROW_CHECK_OK(client_.Create(object_id, data_size, metadata, metadata_size, &data));

    if (i % 3 == 0) {
      // Seal one third of the objects.
      ARROW_CHECK_OK(client_.Seal(object_id));
      // Test that the first client can get the object.
      ARROW_CHECK_OK(client_.Contains(object_id, &has_object));
      ASSERT_EQ(has_object, true);
    } else if (i % 3 == 1) {
      // Abort one third of the objects.
      ARROW_CHECK_OK(client_.Release(object_id));
      ARROW_CHECK_OK(client_.Abort(object_id));
    }
  }
  // Disconnect the first client. All unsealed objects should be aborted.
  ARROW_CHECK_OK(client_.Disconnect());

  // Check that the second client can query the object store for the first
  // client's objects.
  int i = 0;
  for (auto const& object_id : object_ids) {
    bool has_object;
    ARROW_CHECK_OK(client2_.Contains(object_id, &has_object));
    if (i % 3 == 0) {
      // The first third should be sealed.
      ASSERT_EQ(has_object, true);
    } else {
      // The rest were aborted, so the object is not in the store.
      ASSERT_EQ(has_object, false);
    }
    i++;
  }
}

#ifdef PLASMA_GPU
using arrow::gpu::CudaBuffer;
using arrow::gpu::CudaBufferReader;
using arrow::gpu::CudaBufferWriter;

TEST_F(TestPlasmaStore, GetGPUTest) {
  ObjectID object_id = ObjectID::from_random();
  ObjectBuffer object_buffer;

  // Test for object non-existence.
  ARROW_CHECK_OK(client_.Get(&object_id, 1, 0, &object_buffer));
  ASSERT_EQ(object_buffer.data_size, -1);

  // Test for the object being in local Plasma store.
  // First create object.
  uint8_t data[] = {4, 5, 3, 1};
  int64_t data_size = sizeof(data);
  uint8_t metadata[] = {5};
  int64_t metadata_size = sizeof(metadata);
  std::shared_ptr<Buffer> data_buffer;
  std::shared_ptr<CudaBuffer> gpu_buffer;
  ARROW_CHECK_OK(
      client_.Create(object_id, data_size, metadata, metadata_size, &data_buffer, 1));
  gpu_buffer = std::dynamic_pointer_cast<CudaBuffer>(data_buffer);
  CudaBufferWriter writer(gpu_buffer);
  writer.Write(data, data_size);
  ARROW_CHECK_OK(client_.Seal(object_id));

  ARROW_CHECK_OK(client_.Get(&object_id, 1, -1, &object_buffer));
  gpu_buffer = std::dynamic_pointer_cast<CudaBuffer>(object_buffer.data);
  CudaBufferReader reader(gpu_buffer);
  uint8_t read_data[data_size];
  int64_t read_data_size;
  reader.Read(data_size, &read_data_size, read_data);
  for (int64_t i = 0; i < data_size; i++) {
    ASSERT_EQ(data[i], read_data[i]);
  }
}

TEST_F(TestPlasmaStore, MultipleClientGPUTest) {
  ObjectID object_id = ObjectID::from_random();

  // Test for object non-existence on the first client.
  bool has_object;
  ARROW_CHECK_OK(client_.Contains(object_id, &has_object));
  ASSERT_EQ(has_object, false);

  // Test for the object being in local Plasma store.
  // First create and seal object on the second client.
  int64_t data_size = 100;
  uint8_t metadata[] = {5};
  int64_t metadata_size = sizeof(metadata);
  std::shared_ptr<Buffer> data;
  ARROW_CHECK_OK(
      client2_.Create(object_id, data_size, metadata, metadata_size, &data, 1));
  ARROW_CHECK_OK(client2_.Seal(object_id));
  // Test that the first client can get the object.
  ObjectBuffer object_buffer;
  ARROW_CHECK_OK(client_.Get(&object_id, 1, -1, &object_buffer));
  ARROW_CHECK_OK(client_.Contains(object_id, &has_object));
  ASSERT_EQ(has_object, true);

  // Test that one client disconnecting does not interfere with the other.
  // First create object on the second client.
  object_id = ObjectID::from_random();
  ARROW_CHECK_OK(
      client2_.Create(object_id, data_size, metadata, metadata_size, &data, 1));
  // Disconnect the first client.
  ARROW_CHECK_OK(client_.Disconnect());
  // Test that the second client can seal and get the created object.
  ARROW_CHECK_OK(client2_.Seal(object_id));
  ARROW_CHECK_OK(client2_.Get(&object_id, 1, -1, &object_buffer));
  ARROW_CHECK_OK(client2_.Contains(object_id, &has_object));
  ASSERT_EQ(has_object, true);
}

#endif

}  // namespace plasma

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  plasma::test_executable = std::string(argv[0]);
  return RUN_ALL_TESTS();
}
