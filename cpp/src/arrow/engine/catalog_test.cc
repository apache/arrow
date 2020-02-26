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

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "arrow/testing/gtest_util.h"

#include "arrow/engine/catalog.h"
#include "arrow/table.h"
#include "arrow/type.h"

namespace arrow {
namespace engine {

using Entry = Catalog::Entry;

class TestCatalog : public testing::Test {
 public:
  std::shared_ptr<Schema> schema_ = schema({field("f", int32())});
  std::shared_ptr<Table> table(std::shared_ptr<Schema> schema) const {
    return MockTable(schema);
  }
  std::shared_ptr<Table> table() const { return table(schema_); }
};

void AssertCatalogKeyIs(const std::shared_ptr<Catalog>& catalog, const Catalog::Key& key,
                        const std::shared_ptr<Table>& expected) {
  ASSERT_OK_AND_ASSIGN(auto t, catalog->Get(key));
  ASSERT_EQ(t.kind(), Catalog::Entry::Kind::TABLE);
  AssertTablesEqual(*t.table(), *expected);

  ASSERT_OK_AND_ASSIGN(auto schema, catalog->GetSchema(key));
  AssertSchemaEqual(*schema, *expected->schema());
}

TEST_F(TestCatalog, EmptyCatalog) {
  ASSERT_OK_AND_ASSIGN(auto empty_catalog, Catalog::Make({}));
  ASSERT_RAISES(KeyError, empty_catalog->Get(""));
  ASSERT_RAISES(KeyError, empty_catalog->Get("a_key"));
}

TEST_F(TestCatalog, Make) {
  auto key_1 = "a";
  auto table_1 = table(schema({field(key_1, int32())}));
  auto key_2 = "b";
  auto table_2 = table(schema({field(key_2, int32())}));
  auto key_3 = "c";
  auto table_3 = table(schema({field(key_3, int32())}));

  std::vector<Catalog::KeyValue> tables{
      {key_1, Entry(table_1)}, {key_2, Entry(table_2)}, {key_3, Entry(table_3)}};

  ASSERT_OK_AND_ASSIGN(auto catalog, Catalog::Make(std::move(tables)));
  AssertCatalogKeyIs(catalog, key_1, table_1);
  AssertCatalogKeyIs(catalog, key_2, table_2);
  AssertCatalogKeyIs(catalog, key_3, table_3);
}

class TestCatalogBuilder : public TestCatalog {};

TEST_F(TestCatalogBuilder, EmptyCatalog) {
  CatalogBuilder builder;
  ASSERT_OK_AND_ASSIGN(auto empty_catalog, builder.Finish());
  ASSERT_RAISES(KeyError, empty_catalog->Get("a_key"));
}

TEST_F(TestCatalogBuilder, Basic) {
  auto key_1 = "a";
  auto table_1 = table(schema({field(key_1, int32())}));
  auto key_2 = "b";
  auto table_2 = table(schema({field(key_2, int32())}));
  auto key_3 = "c";
  auto table_3 = table(schema({field(key_3, int32())}));

  CatalogBuilder builder;
  ASSERT_OK(builder.Add(key_1, table_1));
  ASSERT_OK(builder.Add(key_2, table_2));
  ASSERT_OK(builder.Add(key_3, table_3));
  ASSERT_OK_AND_ASSIGN(auto catalog, builder.Finish());

  AssertCatalogKeyIs(catalog, key_1, table_1);
  AssertCatalogKeyIs(catalog, key_2, table_2);
  AssertCatalogKeyIs(catalog, key_3, table_3);

  ASSERT_RAISES(KeyError, catalog->Get("invalid_key"));
}

TEST_F(TestCatalogBuilder, NullOrEmptyKeys) {
  CatalogBuilder builder;

  auto invalid_key = "";
  // Invalid empty key
  ASSERT_RAISES(Invalid, builder.Add(invalid_key, table()));

  auto valid_key = "valid_key";
  // Invalid nullptr Table
  ASSERT_RAISES(Invalid, builder.Add(valid_key, std::shared_ptr<Table>{}));
  // Invalid nullptr Dataset
  ASSERT_RAISES(Invalid, builder.Add(valid_key, std::shared_ptr<dataset::Dataset>{}));
}

TEST_F(TestCatalogBuilder, DuplicateKeys) {
  CatalogBuilder builder;

  auto key = "a_key";

  ASSERT_OK(builder.Add(key, table()));
  // Key already in catalog
  ASSERT_RAISES(KeyError, builder.Add(key, table()));

  // Should still yield a valid catalog if requested.
  ASSERT_OK_AND_ASSIGN(auto catalog, builder.Finish());

  ASSERT_OK_AND_ASSIGN(auto t, catalog->Get(key));
  ASSERT_EQ(t.kind(), Catalog::Entry::Kind::TABLE);
  AssertTablesEqual(*t.table(), *table());
}

}  // namespace engine
}  // namespace arrow
