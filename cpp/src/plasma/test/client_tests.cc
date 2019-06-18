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

#include <memory>
#include <thread>

#include <gtest/gtest.h>

#include "arrow/testing/gtest_util.h"
#include "arrow/util/io-util.h"

#include "plasma/client.h"
#include "plasma/common.h"
#include "plasma/plasma.h"
#include "plasma/protocol.h"
#include "plasma/test-util.h"

namespace plasma {

using arrow::internal::TemporaryDir;

std::string test_executable;  // NOLINT

void AssertObjectBufferEqual(const ObjectBuffer& object_buffer,
                             const std::vector<uint8_t>& metadata,
                             const std::vector<uint8_t>& data) {
  arrow::AssertBufferEqual(*object_buffer.metadata, metadata);
  arrow::AssertBufferEqual(*object_buffer.data, data);
}

class TestPlasmaStore : public ::testing::Test {
 public:
  // TODO(pcm): At the moment, stdout of the test gets mixed up with
  // stdout of the object store. Consider changing that.

  void SetUp() {
    ARROW_CHECK_OK(TemporaryDir::Make("cli-test-", &temp_dir_));
    store_socket_name_ = temp_dir_->path().ToString() + "store";

    std::string plasma_directory =
        test_executable.substr(0, test_executable.find_last_of("/"));
    std::string plasma_command =
        plasma_directory + "/plasma_store_server -m 10000000 -s " + store_socket_name_ +
        " 1> /dev/null 2> /dev/null & " + "echo $! > " + store_socket_name_ + ".pid";
    PLASMA_CHECK_SYSTEM(system(plasma_command.c_str()));
    ARROW_CHECK_OK(client_.Connect(store_socket_name_, ""));
    ARROW_CHECK_OK(client2_.Connect(store_socket_name_, ""));
  }

  virtual void TearDown() {
    ARROW_CHECK_OK(client_.Disconnect());
    ARROW_CHECK_OK(client2_.Disconnect());
    // Kill plasma_store process that we started
#ifdef COVERAGE_BUILD
    // Ask plasma_store to exit gracefully and give it time to write out
    // coverage files
    std::string plasma_term_command =
        "kill -TERM `cat " + store_socket_name_ + ".pid` || exit 0";
    PLASMA_CHECK_SYSTEM(system(plasma_term_command.c_str()));
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
#endif
    std::string plasma_kill_command =
        "kill -KILL `cat " + store_socket_name_ + ".pid` || exit 0";
    PLASMA_CHECK_SYSTEM(system(plasma_kill_command.c_str()));
  }

  void CreateObject(PlasmaClient& client, const ObjectID& object_id,
                    const std::vector<uint8_t>& metadata,
                    const std::vector<uint8_t>& data, bool release = true) {
    std::shared_ptr<Buffer> data_buffer;
    ARROW_CHECK_OK(client.Create(object_id, data.size(), &metadata[0], metadata.size(),
                                 &data_buffer));
    for (size_t i = 0; i < data.size(); i++) {
      data_buffer->mutable_data()[i] = data[i];
    }
    ARROW_CHECK_OK(client.Seal(object_id));
    if (release) {
      ARROW_CHECK_OK(client.Release(object_id));
    }
  }

 protected:
  PlasmaClient client_;
  PlasmaClient client2_;
  std::unique_ptr<TemporaryDir> temp_dir_;
  std::string store_socket_name_;
};

TEST_F(TestPlasmaStore, NewSubscriberTest) {
  PlasmaClient local_client, local_client2;

  ARROW_CHECK_OK(local_client.Connect(store_socket_name_, ""));
  ARROW_CHECK_OK(local_client2.Connect(store_socket_name_, ""));

  ObjectID object_id = random_object_id();

  // Test for the object being in local Plasma store.
  // First create object.
  int64_t data_size = 100;
  uint8_t metadata[] = {5};
  int64_t metadata_size = sizeof(metadata);
  std::shared_ptr<Buffer> data;
  ARROW_CHECK_OK(
      local_client.Create(object_id, data_size, metadata, metadata_size, &data));
  ARROW_CHECK_OK(local_client.Seal(object_id));

  // Test that new subscriber client2 can receive notifications about existing objects.
  int fd = -1;
  ARROW_CHECK_OK(local_client2.Subscribe(&fd));
  ASSERT_GT(fd, 0);

  ObjectID object_id2 = random_object_id();
  int64_t data_size2 = 0;
  int64_t metadata_size2 = 0;
  ARROW_CHECK_OK(
      local_client2.GetNotification(fd, &object_id2, &data_size2, &metadata_size2));
  ASSERT_EQ(object_id, object_id2);
  ASSERT_EQ(data_size, data_size2);
  ASSERT_EQ(metadata_size, metadata_size2);

  // Delete the object.
  ARROW_CHECK_OK(local_client.Release(object_id));
  ARROW_CHECK_OK(local_client.Delete(object_id));

  ARROW_CHECK_OK(
      local_client2.GetNotification(fd, &object_id2, &data_size2, &metadata_size2));
  ASSERT_EQ(object_id, object_id2);
  ASSERT_EQ(-1, data_size2);
  ASSERT_EQ(-1, metadata_size2);

  ARROW_CHECK_OK(local_client2.Disconnect());
  ARROW_CHECK_OK(local_client.Disconnect());
}

TEST_F(TestPlasmaStore, SealErrorsTest) {
  ObjectID object_id = random_object_id();

  Status result = client_.Seal(object_id);
  ASSERT_TRUE(result.IsPlasmaObjectNonexistent());

  // Create object.
  std::vector<uint8_t> data(100, 0);
  CreateObject(client_, object_id, {42}, data, false);

  // Trying to seal it again.
  result = client_.Seal(object_id);
  ASSERT_TRUE(result.IsPlasmaObjectAlreadySealed());
  ARROW_CHECK_OK(client_.Release(object_id));
}

TEST_F(TestPlasmaStore, DeleteTest) {
  ObjectID object_id = random_object_id();

  // Test for deleting non-existance object.
  Status result = client_.Delete(object_id);
  ARROW_CHECK_OK(result);

  // Test for the object being in local Plasma store.
  // First create object.
  int64_t data_size = 100;
  uint8_t metadata[] = {5};
  int64_t metadata_size = sizeof(metadata);
  std::shared_ptr<Buffer> data;
  ARROW_CHECK_OK(client_.Create(object_id, data_size, metadata, metadata_size, &data));
  ARROW_CHECK_OK(client_.Seal(object_id));

  result = client_.Delete(object_id);
  ARROW_CHECK_OK(result);
  bool has_object = false;
  ARROW_CHECK_OK(client_.Contains(object_id, &has_object));
  ASSERT_TRUE(has_object);

  ARROW_CHECK_OK(client_.Release(object_id));
  // object_id is marked as to-be-deleted, when it is not in use, it will be deleted.
  ARROW_CHECK_OK(client_.Contains(object_id, &has_object));
  ASSERT_FALSE(has_object);
  ARROW_CHECK_OK(client_.Delete(object_id));
}

TEST_F(TestPlasmaStore, DeleteObjectsTest) {
  ObjectID object_id1 = random_object_id();
  ObjectID object_id2 = random_object_id();

  // Test for deleting non-existance object.
  Status result = client_.Delete(std::vector<ObjectID>{object_id1, object_id2});
  ARROW_CHECK_OK(result);
  // Test for the object being in local Plasma store.
  // First create object.
  int64_t data_size = 100;
  uint8_t metadata[] = {5};
  int64_t metadata_size = sizeof(metadata);
  std::shared_ptr<Buffer> data;
  ARROW_CHECK_OK(client_.Create(object_id1, data_size, metadata, metadata_size, &data));
  ARROW_CHECK_OK(client_.Seal(object_id1));
  ARROW_CHECK_OK(client_.Create(object_id2, data_size, metadata, metadata_size, &data));
  ARROW_CHECK_OK(client_.Seal(object_id2));
  // Release the ref count of Create function.
  ARROW_CHECK_OK(client_.Release(object_id1));
  ARROW_CHECK_OK(client_.Release(object_id2));
  // Increase the ref count by calling Get using client2_.
  std::vector<ObjectBuffer> object_buffers;
  ARROW_CHECK_OK(client2_.Get({object_id1, object_id2}, 0, &object_buffers));
  // Objects are still used by client2_.
  result = client_.Delete(std::vector<ObjectID>{object_id1, object_id2});
  ARROW_CHECK_OK(result);
  // The object is used and it should not be deleted right now.
  bool has_object = false;
  ARROW_CHECK_OK(client_.Contains(object_id1, &has_object));
  ASSERT_TRUE(has_object);
  ARROW_CHECK_OK(client_.Contains(object_id2, &has_object));
  ASSERT_TRUE(has_object);
  // Decrease the ref count by deleting the PlasmaBuffer (in ObjectBuffer).
  // client2_ won't send the release request immediately because the trigger
  // condition is not reached. The release is only added to release cache.
  object_buffers.clear();
  // Delete the objects.
  result = client2_.Delete(std::vector<ObjectID>{object_id1, object_id2});
  ARROW_CHECK_OK(client_.Contains(object_id1, &has_object));
  ASSERT_FALSE(has_object);
  ARROW_CHECK_OK(client_.Contains(object_id2, &has_object));
  ASSERT_FALSE(has_object);
}

TEST_F(TestPlasmaStore, ContainsTest) {
  ObjectID object_id = random_object_id();

  // Test for object non-existence.
  bool has_object;
  ARROW_CHECK_OK(client_.Contains(object_id, &has_object));
  ASSERT_FALSE(has_object);

  // Test for the object being in local Plasma store.
  // First create object.
  std::vector<uint8_t> data(100, 0);
  CreateObject(client_, object_id, {42}, data);
  std::vector<ObjectBuffer> object_buffers;
  ARROW_CHECK_OK(client_.Get({object_id}, -1, &object_buffers));
  ARROW_CHECK_OK(client_.Contains(object_id, &has_object));
  ASSERT_TRUE(has_object);
}

TEST_F(TestPlasmaStore, GetTest) {
  std::vector<ObjectBuffer> object_buffers;

  ObjectID object_id = random_object_id();

  // Test for object non-existence.
  ARROW_CHECK_OK(client_.Get({object_id}, 0, &object_buffers));
  ASSERT_EQ(object_buffers.size(), 1);
  ASSERT_FALSE(object_buffers[0].metadata);
  ASSERT_FALSE(object_buffers[0].data);
  EXPECT_FALSE(client_.IsInUse(object_id));

  // Test for the object being in local Plasma store.
  // First create object.
  std::vector<uint8_t> data = {3, 5, 6, 7, 9};
  CreateObject(client_, object_id, {42}, data);
  EXPECT_FALSE(client_.IsInUse(object_id));

  object_buffers.clear();
  ARROW_CHECK_OK(client_.Get({object_id}, -1, &object_buffers));
  ASSERT_EQ(object_buffers.size(), 1);
  ASSERT_EQ(object_buffers[0].device_num, 0);
  AssertObjectBufferEqual(object_buffers[0], {42}, {3, 5, 6, 7, 9});

  // Metadata keeps object in use
  {
    auto metadata = object_buffers[0].metadata;
    object_buffers.clear();
    ::arrow::AssertBufferEqual(*metadata, std::string{42});
    EXPECT_TRUE(client_.IsInUse(object_id));
  }
  // Object is automatically released
  EXPECT_FALSE(client_.IsInUse(object_id));
}

TEST_F(TestPlasmaStore, LegacyGetTest) {
  // Test for old non-releasing Get() variant
  ObjectID object_id = random_object_id();
  {
    ObjectBuffer object_buffer;

    // Test for object non-existence.
    ARROW_CHECK_OK(client_.Get(&object_id, 1, 0, &object_buffer));
    ASSERT_FALSE(object_buffer.metadata);
    ASSERT_FALSE(object_buffer.data);
    EXPECT_FALSE(client_.IsInUse(object_id));

    // First create object.
    std::vector<uint8_t> data = {3, 5, 6, 7, 9};
    CreateObject(client_, object_id, {42}, data);
    EXPECT_FALSE(client_.IsInUse(object_id));

    ARROW_CHECK_OK(client_.Get(&object_id, 1, -1, &object_buffer));
    AssertObjectBufferEqual(object_buffer, {42}, {3, 5, 6, 7, 9});
  }
  // Object needs releasing manually
  EXPECT_TRUE(client_.IsInUse(object_id));
  ARROW_CHECK_OK(client_.Release(object_id));
  EXPECT_FALSE(client_.IsInUse(object_id));
}

TEST_F(TestPlasmaStore, MultipleGetTest) {
  ObjectID object_id1 = random_object_id();
  ObjectID object_id2 = random_object_id();
  std::vector<ObjectID> object_ids = {object_id1, object_id2};
  std::vector<ObjectBuffer> object_buffers;

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

  ARROW_CHECK_OK(client_.Get(object_ids, -1, &object_buffers));
  ASSERT_EQ(object_buffers[0].data->data()[0], 1);
  ASSERT_EQ(object_buffers[1].data->data()[0], 2);
}

TEST_F(TestPlasmaStore, AbortTest) {
  ObjectID object_id = random_object_id();
  std::vector<ObjectBuffer> object_buffers;

  // Test for object non-existence.
  ARROW_CHECK_OK(client_.Get({object_id}, 0, &object_buffers));
  ASSERT_FALSE(object_buffers[0].data);

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
  EXPECT_TRUE(client_.IsInUse(object_id));

  ARROW_CHECK_OK(client_.Abort(object_id));
  EXPECT_FALSE(client_.IsInUse(object_id));

  // Test for object non-existence after the abort.
  ARROW_CHECK_OK(client_.Get({object_id}, 0, &object_buffers));
  ASSERT_FALSE(object_buffers[0].data);

  // Create the object successfully this time.
  CreateObject(client_, object_id, {42, 43}, {1, 2, 3, 4, 5});

  // Test that we can get the object.
  ARROW_CHECK_OK(client_.Get({object_id}, -1, &object_buffers));
  AssertObjectBufferEqual(object_buffers[0], {42, 43}, {1, 2, 3, 4, 5});
}

TEST_F(TestPlasmaStore, OneIdCreateRepeatedlyTest) {
  const int64_t loop_times = 5;

  ObjectID object_id = random_object_id();
  std::vector<ObjectBuffer> object_buffers;

  // Test for object non-existence.
  ARROW_CHECK_OK(client_.Get({object_id}, 0, &object_buffers));
  ASSERT_FALSE(object_buffers[0].data);

  int64_t data_size = 20;
  uint8_t metadata[] = {5};
  int64_t metadata_size = sizeof(metadata);

  // Test the sequence: create -> release -> abort -> ...
  for (int64_t i = 0; i < loop_times; i++) {
    std::shared_ptr<Buffer> data;
    ARROW_CHECK_OK(client_.Create(object_id, data_size, metadata, metadata_size, &data));
    ARROW_CHECK_OK(client_.Release(object_id));
    ARROW_CHECK_OK(client_.Abort(object_id));
  }

  // Test the sequence: create -> seal -> release -> delete -> ...
  for (int64_t i = 0; i < loop_times; i++) {
    std::shared_ptr<Buffer> data;
    ARROW_CHECK_OK(client_.Create(object_id, data_size, metadata, metadata_size, &data));
    ARROW_CHECK_OK(client_.Seal(object_id));
    ARROW_CHECK_OK(client_.Release(object_id));
    ARROW_CHECK_OK(client_.Delete(object_id));
  }
}

TEST_F(TestPlasmaStore, MultipleClientTest) {
  ObjectID object_id = random_object_id();
  std::vector<ObjectBuffer> object_buffers;

  // Test for object non-existence on the first client.
  bool has_object;
  ARROW_CHECK_OK(client_.Contains(object_id, &has_object));
  ASSERT_FALSE(has_object);

  // Test for the object being in local Plasma store.
  // First create and seal object on the second client.
  int64_t data_size = 100;
  uint8_t metadata[] = {5};
  int64_t metadata_size = sizeof(metadata);
  std::shared_ptr<Buffer> data;
  ARROW_CHECK_OK(client2_.Create(object_id, data_size, metadata, metadata_size, &data));
  ARROW_CHECK_OK(client2_.Seal(object_id));
  // Test that the first client can get the object.
  ARROW_CHECK_OK(client_.Get({object_id}, -1, &object_buffers));
  ASSERT_TRUE(object_buffers[0].data);
  ARROW_CHECK_OK(client_.Contains(object_id, &has_object));
  ASSERT_TRUE(has_object);

  // Test that one client disconnecting does not interfere with the other.
  // First create object on the second client.
  object_id = random_object_id();
  ARROW_CHECK_OK(client2_.Create(object_id, data_size, metadata, metadata_size, &data));
  // Disconnect the first client.
  ARROW_CHECK_OK(client_.Disconnect());
  // Test that the second client can seal and get the created object.
  ARROW_CHECK_OK(client2_.Seal(object_id));
  ARROW_CHECK_OK(client2_.Get({object_id}, -1, &object_buffers));
  ASSERT_TRUE(object_buffers[0].data);
  ARROW_CHECK_OK(client2_.Contains(object_id, &has_object));
  ASSERT_TRUE(has_object);
}

TEST_F(TestPlasmaStore, ManyObjectTest) {
  // Create many objects on the first client. Seal one third, abort one third,
  // and leave the last third unsealed.
  std::vector<ObjectID> object_ids;
  for (int i = 0; i < 100; i++) {
    ObjectID object_id = random_object_id();
    object_ids.push_back(object_id);

    // Test for object non-existence on the first client.
    bool has_object;
    ARROW_CHECK_OK(client_.Contains(object_id, &has_object));
    ASSERT_FALSE(has_object);

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
      ASSERT_TRUE(has_object);
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
      ASSERT_TRUE(has_object);
    } else {
      // The rest were aborted, so the object is not in the store.
      ASSERT_FALSE(has_object);
    }
    i++;
  }
}

#ifdef PLASMA_CUDA
using arrow::cuda::CudaBuffer;
using arrow::cuda::CudaBufferReader;
using arrow::cuda::CudaBufferWriter;

namespace {

void AssertCudaRead(const std::shared_ptr<Buffer>& buffer,
                    const std::vector<uint8_t>& expected_data) {
  std::shared_ptr<CudaBuffer> gpu_buffer;
  const size_t data_size = expected_data.size();

  ARROW_CHECK_OK(CudaBuffer::FromBuffer(buffer, &gpu_buffer));
  ASSERT_EQ(gpu_buffer->size(), data_size);

  CudaBufferReader reader(gpu_buffer);
  std::vector<uint8_t> read_data(data_size);
  int64_t read_data_size;
  ARROW_CHECK_OK(reader.Read(data_size, &read_data_size, read_data.data()));
  ASSERT_EQ(read_data_size, data_size);

  for (size_t i = 0; i < data_size; i++) {
    ASSERT_EQ(read_data[i], expected_data[i]);
  }
}

}  // namespace

TEST_F(TestPlasmaStore, GetGPUTest) {
  ObjectID object_id = random_object_id();
  std::vector<ObjectBuffer> object_buffers;

  // Test for object non-existence.
  ARROW_CHECK_OK(client_.Get({object_id}, 0, &object_buffers));
  ASSERT_EQ(object_buffers.size(), 1);
  ASSERT_FALSE(object_buffers[0].data);

  // Test for the object being in local Plasma store.
  // First create object.
  uint8_t data[] = {4, 5, 3, 1};
  int64_t data_size = sizeof(data);
  uint8_t metadata[] = {42};
  int64_t metadata_size = sizeof(metadata);
  std::shared_ptr<Buffer> data_buffer;
  std::shared_ptr<CudaBuffer> gpu_buffer;
  ARROW_CHECK_OK(
      client_.Create(object_id, data_size, metadata, metadata_size, &data_buffer, 1));
  ARROW_CHECK_OK(CudaBuffer::FromBuffer(data_buffer, &gpu_buffer));
  CudaBufferWriter writer(gpu_buffer);
  ARROW_CHECK_OK(writer.Write(data, data_size));
  ARROW_CHECK_OK(client_.Seal(object_id));

  object_buffers.clear();
  ARROW_CHECK_OK(client_.Get({object_id}, -1, &object_buffers));
  ASSERT_EQ(object_buffers.size(), 1);
  ASSERT_EQ(object_buffers[0].device_num, 1);
  // Check data
  AssertCudaRead(object_buffers[0].data, {4, 5, 3, 1});
  // Check metadata
  AssertCudaRead(object_buffers[0].metadata, {42});
}

TEST_F(TestPlasmaStore, DeleteObjectsGPUTest) {
  ObjectID object_id1 = random_object_id();
  ObjectID object_id2 = random_object_id();

  // Test for deleting non-existance object.
  Status result = client_.Delete(std::vector<ObjectID>{object_id1, object_id2});
  ARROW_CHECK_OK(result);
  // Test for the object being in local Plasma store.
  // First create object.
  int64_t data_size = 100;
  uint8_t metadata[] = {5};
  int64_t metadata_size = sizeof(metadata);
  std::shared_ptr<Buffer> data;
  ARROW_CHECK_OK(
      client_.Create(object_id1, data_size, metadata, metadata_size, &data, 1));
  ARROW_CHECK_OK(client_.Seal(object_id1));
  ARROW_CHECK_OK(
      client_.Create(object_id2, data_size, metadata, metadata_size, &data, 1));
  ARROW_CHECK_OK(client_.Seal(object_id2));
  // Release the ref count of Create function.
  data = nullptr;
  ARROW_CHECK_OK(client_.Release(object_id1));
  ARROW_CHECK_OK(client_.Release(object_id2));
  // Increase the ref count by calling Get using client2_.
  std::vector<ObjectBuffer> object_buffers;
  ARROW_CHECK_OK(client2_.Get({object_id1, object_id2}, 0, &object_buffers));
  // Objects are still used by client2_.
  result = client_.Delete(std::vector<ObjectID>{object_id1, object_id2});
  ARROW_CHECK_OK(result);
  // The object is used and it should not be deleted right now.
  bool has_object = false;
  ARROW_CHECK_OK(client_.Contains(object_id1, &has_object));
  ASSERT_TRUE(has_object);
  ARROW_CHECK_OK(client_.Contains(object_id2, &has_object));
  ASSERT_TRUE(has_object);
  // Decrease the ref count by deleting the PlasmaBuffer (in ObjectBuffer).
  // client2_ won't send the release request immediately because the trigger
  // condition is not reached. The release is only added to release cache.
  object_buffers.clear();
  // Delete the objects.
  result = client2_.Delete(std::vector<ObjectID>{object_id1, object_id2});
  ARROW_CHECK_OK(client_.Contains(object_id1, &has_object));
  ASSERT_FALSE(has_object);
  ARROW_CHECK_OK(client_.Contains(object_id2, &has_object));
  ASSERT_FALSE(has_object);
}

TEST_F(TestPlasmaStore, RepeatlyCreateGPUTest) {
  const int64_t loop_times = 100;
  const int64_t object_num = 5;
  const int64_t data_size = 40;

  std::vector<ObjectID> object_ids;

  // create new gpu objects
  for (int64_t i = 0; i < object_num; i++) {
    object_ids.push_back(random_object_id());
    ObjectID& object_id = object_ids[i];

    std::shared_ptr<Buffer> data;
    ARROW_CHECK_OK(client_.Create(object_id, data_size, 0, 0, &data, 1));
    ARROW_CHECK_OK(client_.Seal(object_id));
    ARROW_CHECK_OK(client_.Release(object_id));
  }

  // delete and create again
  for (int64_t i = 0; i < loop_times; i++) {
    ObjectID& object_id = object_ids[i % object_num];

    ARROW_CHECK_OK(client_.Delete(object_id));

    std::shared_ptr<Buffer> data;
    ARROW_CHECK_OK(client_.Create(object_id, data_size, 0, 0, &data, 1));
    ARROW_CHECK_OK(client_.Seal(object_id));

    data = nullptr;
    ARROW_CHECK_OK(client_.Release(object_id));
  }

  // delete all
  ARROW_CHECK_OK(client_.Delete(object_ids));
}

TEST_F(TestPlasmaStore, MultipleClientGPUTest) {
  ObjectID object_id = random_object_id();
  std::vector<ObjectBuffer> object_buffers;

  // Test for object non-existence on the first client.
  bool has_object;
  ARROW_CHECK_OK(client_.Contains(object_id, &has_object));
  ASSERT_FALSE(has_object);

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
  ARROW_CHECK_OK(client_.Get({object_id}, -1, &object_buffers));
  ARROW_CHECK_OK(client_.Contains(object_id, &has_object));
  ASSERT_TRUE(has_object);

  // Test that one client disconnecting does not interfere with the other.
  // First create object on the second client.
  object_id = random_object_id();
  ARROW_CHECK_OK(
      client2_.Create(object_id, data_size, metadata, metadata_size, &data, 1));
  // Disconnect the first client.
  ARROW_CHECK_OK(client_.Disconnect());
  // Test that the second client can seal and get the created object.
  ARROW_CHECK_OK(client2_.Seal(object_id));
  object_buffers.clear();
  ARROW_CHECK_OK(client2_.Contains(object_id, &has_object));
  ASSERT_TRUE(has_object);
  ARROW_CHECK_OK(client2_.Get({object_id}, -1, &object_buffers));
  ASSERT_EQ(object_buffers.size(), 1);
  ASSERT_EQ(object_buffers[0].device_num, 1);
  AssertCudaRead(object_buffers[0].metadata, {5});
}

#endif  // PLASMA_CUDA

}  // namespace plasma

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  plasma::test_executable = std::string(argv[0]);
  return RUN_ALL_TESTS();
}
