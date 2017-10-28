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
    std::string plasma_directory =
        test_executable.substr(0, test_executable.find_last_of("/"));
    std::string plasma_command =
        plasma_directory +
        "/plasma_store -m 1000000000 -s /tmp/store 1> /dev/null 2> /dev/null &";
    system(plasma_command.c_str());
    ARROW_CHECK_OK(client_.Connect("/tmp/store", "", PLASMA_DEFAULT_RELEASE_DELAY));
  }
  virtual void Finish() {
    ARROW_CHECK_OK(client_.Disconnect());
    system("killall plasma_store &");
  }

 protected:
  PlasmaClient client_;
};

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
  uint8_t* data;
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
  uint8_t* data;
  ARROW_CHECK_OK(client_.Create(object_id, data_size, metadata, metadata_size, &data));
  for (int64_t i = 0; i < data_size; i++) {
    data[i] = static_cast<uint8_t>(i % 4);
  }
  ARROW_CHECK_OK(client_.Seal(object_id));

  ARROW_CHECK_OK(client_.Get(&object_id, 1, -1, &object_buffer));
  for (int64_t i = 0; i < data_size; i++) {
    ASSERT_EQ(data[i], object_buffer.data[i]);
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
  uint8_t* data;
  ARROW_CHECK_OK(client_.Create(object_id1, data_size, metadata, metadata_size, &data));
  data[0] = 1;
  ARROW_CHECK_OK(client_.Seal(object_id1));

  ARROW_CHECK_OK(client_.Create(object_id2, data_size, metadata, metadata_size, &data));
  data[0] = 2;
  ARROW_CHECK_OK(client_.Seal(object_id2));

  ARROW_CHECK_OK(client_.Get(object_ids, 2, -1, object_buffer));
  ASSERT_EQ(object_buffer[0].data[0], 1);
  ASSERT_EQ(object_buffer[1].data[0], 2);
}

}  // namespace plasma

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  plasma::test_executable = std::string(argv[0]);
  return RUN_ALL_TESTS();
}
