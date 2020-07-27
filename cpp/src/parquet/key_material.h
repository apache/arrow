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

#pragma once

#include <map>
#include <string>

#include <rapidjson/document.h>

namespace parquet {

namespace encryption {

class KeyMaterial {
 public:
  static constexpr char KEY_MATERIAL_TYPE_FIELD[] = "keyMaterialType";
  static constexpr char KEY_MATERIAL_TYPE1[] = "PKMT1";

  static constexpr char FOOTER_KEY_ID_IN_FILE[] = "footerKey";
  static constexpr char COLUMN_KEY_ID_IN_FILE_PREFIX[] = "columnKey";

  static constexpr char IS_FOOTER_KEY_FIELD[] = "isFooterKey";
  static constexpr char DOUBLE_WRAPPING_FIELD[] = "doubleWrapping";
  static constexpr char KMS_INSTANCE_ID_FIELD[] = "kmsInstanceID";
  static constexpr char KMS_INSTANCE_URL_FIELD[] = "kmsInstanceURL";
  static constexpr char MASTER_KEY_ID_FIELD[] = "masterKeyID";
  static constexpr char WRAPPED_DEK_FIELD[] = "wrappedDEK";
  static constexpr char KEK_ID_FIELD[] = "keyEncryptionKeyID";
  static constexpr char WRAPPED_KEK_FIELD[] = "wrappedKEK";

 public:
  static KeyMaterial Parse(const std::string& key_material_string);

  static std::string CreateSerialized(bool is_footer_key,
                                      const std::string& kms_instance_id,
                                      const std::string& kms_instance_url,
                                      const std::string& master_key_id,
                                      bool is_double_wrapped, const std::string& kek_id,
                                      const std::string& encoded_wrapped_kek,
                                      const std::string& encoded_wrapped_dek,
                                      bool is_internal_storage);

  bool is_footer_key() const { return is_footer_key_; }
  bool is_double_wrapped() const { return is_double_wrapped_; }
  const std::string& master_key_id() const { return master_key_id_; }
  const std::string& wrapped_dek() const { return encoded_wrapped_dek_; }
  const std::string& kek_id() const { return kek_id_; }
  const std::string& wrapped_kek() const { return encoded_wrapped_kek_; }
  const std::string& kms_instance_id() const { return kms_instance_id_; }
  const std::string& kms_instance_url() const { return kms_instance_url_; }

 private:
  static KeyMaterial Parse(const rapidjson::Document& key_material_json);

  KeyMaterial(bool is_footer_key, const std::string& kms_instance_id,
              const std::string& kms_instance_url, const std::string& master_key_id,
              bool is_double_wrapped, const std::string& kek_id,
              const std::string& encoded_wrapped_kek,
              const std::string& encoded_wrapped_dek);

  bool is_footer_key_;
  std::string kms_instance_id_;
  std::string kms_instance_url_;
  std::string master_key_id_;
  bool is_double_wrapped_;
  std::string kek_id_;
  std::string encoded_wrapped_kek_;
  std::string encoded_wrapped_dek_;
};

}  // namespace encryption

}  // namespace parquet
