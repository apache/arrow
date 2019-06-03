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

#include "parquet/metadata.h"

#include <gtest/gtest.h>

#include "parquet/properties.h"
#include "parquet/schema.h"

namespace parquet {

namespace metadata {

const char kFooterEncryptionKey[] = "0123456789012345";  // 128bit/16
const char kColumnEncryptionKey1[] = "1234567890123450";
const char kColumnEncryptionKey2[] = "1234567890123451";

TEST(Metadata, EncryptFooter) {
  parquet::schema::NodeVector fields;
  parquet::schema::NodePtr root;
  parquet::SchemaDescriptor schema;

  fields.push_back(parquet::schema::Int32("int_col", Repetition::REQUIRED));
  fields.push_back(parquet::schema::Float("float_col", Repetition::REQUIRED));
  root = parquet::schema::GroupNode::Make("schema", Repetition::REPEATED, fields);
  schema.Init(root);

  FileEncryptionProperties::Builder encryption_prop_builder(kFooterEncryptionKey);
  encryption_prop_builder.footer_key_metadata("kf");

  WriterProperties::Builder writer_prop_builder;
  writer_prop_builder.encryption(encryption_prop_builder.build());
  auto props = writer_prop_builder.build();

  auto f_builder = FileMetaDataBuilder::Make(&schema, props);
  auto file_metadata = f_builder->Finish();
  ASSERT_EQ(false, file_metadata->is_encryption_algorithm_set());

  auto file_crypto_metadata = f_builder->GetCryptoMetaData();
  ASSERT_EQ(true, file_crypto_metadata != NULLPTR);
}

TEST(Metadata, PlaintextFooter) {
  parquet::schema::NodeVector fields;
  parquet::schema::NodePtr root;
  parquet::SchemaDescriptor schema;

  fields.push_back(parquet::schema::Int32("int_col", Repetition::REQUIRED));
  fields.push_back(parquet::schema::Float("float_col", Repetition::REQUIRED));
  root = parquet::schema::GroupNode::Make("schema", Repetition::REPEATED, fields);
  schema.Init(root);

  FileEncryptionProperties::Builder encryption_prop_builder(kFooterEncryptionKey);
  encryption_prop_builder.footer_key_metadata("kf");
  encryption_prop_builder.set_plaintext_footer();

  WriterProperties::Builder writer_prop_builder;
  writer_prop_builder.encryption(encryption_prop_builder.build());
  auto props = writer_prop_builder.build();

  auto f_builder = FileMetaDataBuilder::Make(&schema, props);
  auto file_metadata = f_builder->Finish();
  ASSERT_EQ(true, file_metadata->is_encryption_algorithm_set());

  auto file_crypto_metadata = f_builder->GetCryptoMetaData();
  ASSERT_EQ(NULLPTR, file_crypto_metadata);
}

}  // namespace metadata
}  // namespace parquet
