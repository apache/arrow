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

#include <string>

#include <gtest/gtest.h>

#include "parquet/encryption/key_material.h"
#include "parquet/encryption/key_metadata.h"

namespace parquet::encryption::test {

TEST(KeyMetadataTest, InternalMaterialStorage) {
  bool is_footer_key = true;
  std::string kms_instance_id = "DEFAULT";
  std::string kms_instance_url = "DEFAULT";
  std::string master_key_id = "kf";
  bool double_wrapped = true;
  std::string kek_id = "FANqyCuxfU1c526Uzb+MTA==";
  std::string encoded_wrapped_kek =
      "{\"masterKeyVersion\":\"NO_VERSION\",\"encryptedKey\":\"LAAAAGaoSfMV1YH/"
      "oxwG2ES8Phva8wimEZcT7zi5bpuK5Jdvw9/zJuqDeIPGGFXd\"}";
  std::string encoded_wrapped_dek =
      "LAAAAA3RcNYT1Rxb/gqhA1KvBgHcjvEppST9+cV3bU5nLmtaZHJhsZakR20qRErX";
  bool internal_storage = true;
  std::string json = KeyMaterial::SerializeToJson(
      is_footer_key, kms_instance_id, kms_instance_url, master_key_id, double_wrapped,
      kek_id, encoded_wrapped_kek, encoded_wrapped_dek, internal_storage);

  KeyMetadata key_metadata = KeyMetadata::Parse(json);

  ASSERT_EQ(key_metadata.key_material_stored_internally(), true);

  const KeyMaterial& key_material = key_metadata.key_material();
  ASSERT_EQ(key_material.is_footer_key(), is_footer_key);
  ASSERT_EQ(key_material.kms_instance_id(), kms_instance_id);
  ASSERT_EQ(key_material.kms_instance_url(), kms_instance_url);
  ASSERT_EQ(key_material.master_key_id(), master_key_id);
  ASSERT_EQ(key_material.is_double_wrapped(), double_wrapped);
  ASSERT_EQ(key_material.kek_id(), kek_id);
  ASSERT_EQ(key_material.wrapped_kek(), encoded_wrapped_kek);
  ASSERT_EQ(key_material.wrapped_dek(), encoded_wrapped_dek);
}

TEST(KeyMetadataTest, ExternalMaterialStorage) {
  const std::string key_reference = "X44KIHSxDSFAS5q2223";

  // generate key_metadata string in parquet file
  std::string key_metadata_str =
      KeyMetadata::CreateSerializedForExternalMaterial(key_reference);

  // parse key_metadata string back
  KeyMetadata key_metadata = KeyMetadata::Parse(key_metadata_str);

  ASSERT_EQ(key_metadata.key_material_stored_internally(), false);
  ASSERT_EQ(key_metadata.key_reference(), key_reference);
}

}  // namespace parquet::encryption::test
