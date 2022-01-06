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

#include <gtest/gtest.h>
#include <unordered_set>

#include "gandiva/execution_context.h"
#include "gandiva/hash_utils.h"

TEST(TestShaHashUtils, TestSha1Numeric) {
  gandiva::ExecutionContext ctx;

  auto ctx_ptr = reinterpret_cast<int64_t>(&ctx);

  std::vector<uint64_t> values_to_be_hashed;

  // Generate a list of values to obtains the SHA1 hash
  values_to_be_hashed.push_back(gandiva::gdv_double_to_long(0.0));
  values_to_be_hashed.push_back(gandiva::gdv_double_to_long(0.1));
  values_to_be_hashed.push_back(gandiva::gdv_double_to_long(0.2));
  values_to_be_hashed.push_back(gandiva::gdv_double_to_long(-0.10000001));
  values_to_be_hashed.push_back(gandiva::gdv_double_to_long(-0.0000001));
  values_to_be_hashed.push_back(gandiva::gdv_double_to_long(1.000000));
  values_to_be_hashed.push_back(gandiva::gdv_double_to_long(-0.0000002));
  values_to_be_hashed.push_back(gandiva::gdv_double_to_long(0.999999));

  // Checks if the hash value is different for each one of the values
  std::unordered_set<std::string> sha_values;

  int sha1_size = 40;

  for (auto value : values_to_be_hashed) {
    int out_length;
    const char* sha_1 =
        gandiva::gdv_sha1_hash(ctx_ptr, &value, sizeof(value), &out_length);
    std::string sha1_as_str(sha_1, out_length);
    EXPECT_EQ(sha1_as_str.size(), sha1_size);

    // The value can not exists inside the set with the hash results
    EXPECT_EQ(sha_values.find(sha1_as_str), sha_values.end());
    sha_values.insert(sha1_as_str);
  }
}

TEST(TestShaHashUtils, TestSha256Numeric) {
  gandiva::ExecutionContext ctx;

  auto ctx_ptr = reinterpret_cast<int64_t>(&ctx);

  std::vector<uint64_t> values_to_be_hashed;

  // Generate a list of values to obtains the SHA1 hash
  values_to_be_hashed.push_back(gandiva::gdv_double_to_long(0.0));
  values_to_be_hashed.push_back(gandiva::gdv_double_to_long(0.1));
  values_to_be_hashed.push_back(gandiva::gdv_double_to_long(0.2));
  values_to_be_hashed.push_back(gandiva::gdv_double_to_long(-0.10000001));
  values_to_be_hashed.push_back(gandiva::gdv_double_to_long(-0.0000001));
  values_to_be_hashed.push_back(gandiva::gdv_double_to_long(1.000000));
  values_to_be_hashed.push_back(gandiva::gdv_double_to_long(-0.0000002));
  values_to_be_hashed.push_back(gandiva::gdv_double_to_long(0.999999));

  // Checks if the hash value is different for each one of the values
  std::unordered_set<std::string> sha_values;

  int sha256_size = 64;

  for (auto value : values_to_be_hashed) {
    int out_length;
    const char* sha_256 =
        gandiva::gdv_sha256_hash(ctx_ptr, &value, sizeof(value), &out_length);
    std::string sha256_as_str(sha_256, out_length);
    EXPECT_EQ(sha256_as_str.size(), sha256_size);

    // The value can not exists inside the set with the hash results
    EXPECT_EQ(sha_values.find(sha256_as_str), sha_values.end());
    sha_values.insert(sha256_as_str);
  }
}

TEST(TestShaHashUtils, TestMD5Numeric) {
  gandiva::ExecutionContext ctx;

  auto ctx_ptr = reinterpret_cast<int64_t>(&ctx);

  std::vector<uint64_t> values_to_be_hashed;

  // Generate a list of values to obtains the MD5 hash
  values_to_be_hashed.push_back(gandiva::gdv_double_to_long(0.0));
  values_to_be_hashed.push_back(gandiva::gdv_double_to_long(0.1));
  values_to_be_hashed.push_back(gandiva::gdv_double_to_long(0.2));
  values_to_be_hashed.push_back(gandiva::gdv_double_to_long(-0.10000001));
  values_to_be_hashed.push_back(gandiva::gdv_double_to_long(-0.0000001));
  values_to_be_hashed.push_back(gandiva::gdv_double_to_long(1.000000));
  values_to_be_hashed.push_back(gandiva::gdv_double_to_long(-0.0000002));
  values_to_be_hashed.push_back(gandiva::gdv_double_to_long(0.999999));

  // Checks if the hash value is different for each one of the values
  std::unordered_set<std::string> md5_values;

  int md5_size = 32;

  for (auto value : values_to_be_hashed) {
    int out_length;
    const char* md5 = gandiva::gdv_md5_hash(ctx_ptr, &value, sizeof(value), &out_length);
    std::string md5_as_str(md5, out_length);
    EXPECT_EQ(md5_as_str.size(), md5_size);

    // The value can not exists inside the set with the hash results
    EXPECT_EQ(md5_values.find(md5_as_str), md5_values.end());
    md5_values.insert(md5_as_str);
  }
}

TEST(TestShaHashUtils, TestSha1Varlen) {
  gandiva::ExecutionContext ctx;

  auto ctx_ptr = reinterpret_cast<int64_t>(&ctx);

  std::string first_string =
      "ði ıntəˈnæʃənəl fəˈnɛtık əsoʊsiˈeıʃn\nY [ˈʏpsilɔn], "
      "Yen [jɛn], Yoga [ˈjoːgɑ]";

  std::string second_string =
      "ði ıntəˈnæʃənəl fəˈnɛtık əsoʊsiˈeın\nY [ˈʏpsilɔn], "
      "Yen [jɛn], Yoga [ˈjoːgɑ] コンニチハ";

  // The strings expected hashes are obtained from shell executing the following command:
  // echo -n <output-string> | openssl dgst sha1
  std::string expected_first_result = "160fcdbc2fa694d884868f5fae7a4bae82706185";
  std::string expected_second_result = "a456b3e0f88669d2482170a42fade226a815bee1";

  // Generate the hashes and compare with expected outputs
  const int sha1_size = 40;
  int out_length;

  const char* sha_1 = gandiva::gdv_sha1_hash(ctx_ptr, first_string.c_str(),
                                             first_string.size(), &out_length);
  std::string sha1_as_str(sha_1, out_length);
  EXPECT_EQ(sha1_as_str.size(), sha1_size);
  EXPECT_EQ(sha1_as_str, expected_first_result);

  const char* sha_2 = gandiva::gdv_sha1_hash(ctx_ptr, second_string.c_str(),
                                             second_string.size(), &out_length);
  std::string sha2_as_str(sha_2, out_length);
  EXPECT_EQ(sha2_as_str.size(), sha1_size);
  EXPECT_EQ(sha2_as_str, expected_second_result);
}

TEST(TestShaHashUtils, TestSha256Varlen) {
  gandiva::ExecutionContext ctx;

  auto ctx_ptr = reinterpret_cast<int64_t>(&ctx);

  std::string first_string =
      "ði ıntəˈnæʃənəl fəˈnɛtık əsoʊsiˈeıʃn\nY [ˈʏpsilɔn], "
      "Yen [jɛn], Yoga [ˈjoːgɑ]";

  std::string second_string =
      "ði ıntəˈnæʃənəl fəˈnɛtık əsoʊsiˈeın\nY [ˈʏpsilɔn], "
      "Yen [jɛn], Yoga [ˈjoːgɑ] コンニチハ";

  // The strings expected hashes are obtained from shell executing the following command:
  // echo -n <output-string> | openssl dgst sha1
  std::string expected_first_result =
      "55aeb2e789871dbd289edae94d4c1c82a1c25ca0bcd5a873924da2fefdd57acb";
  std::string expected_second_result =
      "86b29c13d0d0e26ea8f85bfa649dc9b8622ae59a4da2409d7d9b463e86e796f2";

  // Generate the hashes and compare with expected outputs
  const int sha256_size = 64;
  int out_length;

  const char* sha_1 = gandiva::gdv_sha256_hash(ctx_ptr, first_string.c_str(),
                                               first_string.size(), &out_length);
  std::string sha1_as_str(sha_1, out_length);
  EXPECT_EQ(sha1_as_str.size(), sha256_size);
  EXPECT_EQ(sha1_as_str, expected_first_result);

  const char* sha_2 = gandiva::gdv_sha256_hash(ctx_ptr, second_string.c_str(),
                                               second_string.size(), &out_length);
  std::string sha2_as_str(sha_2, out_length);
  EXPECT_EQ(sha2_as_str.size(), sha256_size);
  EXPECT_EQ(sha2_as_str, expected_second_result);
}

TEST(TestShaHashUtils, TestMD5Varlen) {
  gandiva::ExecutionContext ctx;

  auto ctx_ptr = reinterpret_cast<int64_t>(&ctx);

  std::string first_string =
      "ði ıntəˈnæʃənəl fəˈnɛtık əsoʊsiˈeıʃnY [ˈʏpsilɔn], Yen [jɛn], Yoga [ˈjoːgɑ]";

  std::string second_string =
      "ði ıntəˈnæʃənəl fəˈnɛtık əsoʊsiˈeınY [ˈʏpsilɔn], "
      "Yen [jɛn], Yoga [ˈjoːgɑ] コンニチハ";

  // The strings expected hashes are obtained from shell executing the following command:
  // echo -n <output-string> | openssl dgst md5
  std::string expected_first_result = "a633460644425b44e0e023d6980849cc";
  std::string expected_second_result = "407983529dba21e95d95951ccffd30c3";

  // Generate the hashes and compare with expected outputs
  const int md5_size = 32;
  int out_length;

  const char* md5_1 = gandiva::gdv_md5_hash(ctx_ptr, first_string.c_str(),
                                            first_string.size(), &out_length);
  std::string md5_as_str(md5_1, out_length);
  EXPECT_EQ(md5_as_str.size(), md5_size);
  EXPECT_EQ(md5_as_str, expected_first_result);

  const char* md5_2 = gandiva::gdv_md5_hash(ctx_ptr, second_string.c_str(),
                                            second_string.size(), &out_length);
  std::string md5_2_as_str(md5_2, out_length);
  EXPECT_EQ(md5_2_as_str.size(), md5_size);
  EXPECT_EQ(md5_2_as_str, expected_second_result);
}
