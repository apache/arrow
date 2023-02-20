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

#include <gmock/gmock-matchers.h>

#include "arrow/compute/exec/exec_plan.h"
#include "arrow/compute/exec/options.h"
#include "arrow/testing/generator.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/matchers.h"

#include "arrow/table.h"

namespace arrow {
namespace compute {

constexpr int kNumBatches = 64;
constexpr int kRowsPerBatch = 64;

TEST(PivotLongerNode, Basic) {
  std::shared_ptr<Table> input =
      gen::Gen({gen::Step(), gen::Step(), gen::Step(), gen::Step()})
          ->FailOnError()
          ->Table(kRowsPerBatch, kNumBatches);

  PivotLongerNodeOptions options;
  options.feature_field_names = {"feature1", "feature2"};
  options.measurement_field_names = {"meas1", "meas2"};
  options.row_templates = {{{"a", "x"}, {{1}, {3}}}, {{"b", "y"}, {{2}, std::nullopt}}};

  Declaration plan = Declaration::Sequence({
      {"table_source", TableSourceNodeOptions(std::move(input))},
      {"pivot_longer", options},
  });

  ASSERT_OK_AND_ASSIGN(std::shared_ptr<Table> output,
                       DeclarationToTable(std::move(plan)));

  std::shared_ptr<Schema> expected_out_schema = schema({
      field("f0", uint32()),
      field("f1", uint32()),
      field("f2", uint32()),
      field("f3", uint32()),
      field("feature1", utf8()),
      field("feature2", utf8()),
      field("meas1", uint32()),
      field("meas2", uint32()),
  });

  ASSERT_EQ(output->num_rows(), kNumBatches * kRowsPerBatch * 2);
  AssertSchemaEqual(expected_out_schema, output->schema());
}

void CheckError(const PivotLongerNodeOptions& options, const std::string& message) {
  std::shared_ptr<Table> input = gen::Gen({gen::Step(), gen::Random(boolean())})
                                     ->FailOnError()
                                     ->Table(/*rows_per_chunk=*/1, /*num_chunks=*/1);

  Declaration plan = Declaration::Sequence({
      {"table_source", TableSourceNodeOptions(std::move(input))},
      {"pivot_longer", options},
  });

  ASSERT_THAT(DeclarationToStatus(std::move(plan)),
              Raises(StatusCode::Invalid, testing::HasSubstr(message)));
}

TEST(PivotLongerNode, Error) {
  PivotLongerNodeOptions options;
  CheckError(options, "There must be at least one row template");

  options.row_templates = {{{}, {{0}}}};
  CheckError(options, "at least one feature column and one measurement column");

  options.feature_field_names = {"feat1"};
  options.measurement_field_names = {"meas1"};
  options.row_templates = {{{}, {{0}}}};
  CheckError(options,
             "There were names given for 1 feature columns but one of the row templates "
             "only had 0 feature values");

  options.row_templates = {{{"x"}, {}}};
  CheckError(
      options,
      "There were names given for 1 measurement columns but one of the row templates "
      "only had 0 field references");

  options.row_templates = {{{"x"}, {{0}}}, {{"y"}, {{1}}}};
  CheckError(
      options,
      "Some row templates had the type uint32 but later row templates had the type bool");

  options.row_templates = {{{"x"}, {std::nullopt}}, {{"y"}, {std::nullopt}}};
  CheckError(options, "All row templates had nullopt");
}

// The following examples are smaller versions of examples taken from
// https://tidyr.tidyverse.org/reference/pivot_longer.html
TEST(PivotLongerNode, ExamplesFromTidyr1) {
  std::shared_ptr<Schema> test_schema =
      schema({field("religion", utf8()), field("<$10k", float64()),
              field("$10k-20k", float64()), field("$20k-30k", float64()),
              field("$30k-40k", float64()), field("$40k-50k", float64())});
  std::shared_ptr<Table> input = TableFromJSON(test_schema, {{
                                                                R"([
        ["Agnostic", 27, 34, 60, 81, 76],
        ["Atheist", 12, 27, 37, 52, 35],
        ["Buddhist", 27, 21, 30, 34, 33]
    ])"}});

  PivotLongerNodeOptions options;
  options.feature_field_names = {"income"};
  options.measurement_field_names = {"count"};
  options.row_templates = {{{"<$10k"}, {{1}}},
                           {{"$10k-20k"}, {{2}}},
                           {{"$20k-30k"}, {{3}}},
                           {{"$30k-40k"}, {{4}}},
                           {{"$40k-50k"}, {{5}}}};

  Declaration plan = Declaration::Sequence(
      {{"table_source", TableSourceNodeOptions(std::move(input))},
       {"pivot_longer", options},
       {"project", ProjectNodeOptions({field_ref(0), field_ref(6), field_ref(7)},
                                      {"religion", "income", "count"})}});

  ASSERT_OK_AND_ASSIGN(std::shared_ptr<Table> output,
                       DeclarationToTable(std::move(plan)));

  std::shared_ptr<Schema> expected_schema = schema(
      {field("religion", utf8()), field("income", utf8()), field("count", float64())});
  std::shared_ptr<Table> expected = TableFromJSON(expected_schema, {{
                                                                       R"([
        ["Agnostic", "<$10k", 27],
        ["Atheist", "<$10k", 12],
        ["Buddhist", "<$10k", 27],
        ["Agnostic", "$10k-20k", 34],
        ["Atheist", "$10k-20k", 27],
        ["Buddhist", "$10k-20k", 21],
        ["Agnostic", "$20k-30k", 60],
        ["Atheist", "$20k-30k", 37],
        ["Buddhist", "$20k-30k", 30],
        ["Agnostic", "$30k-40k", 81],
        ["Atheist", "$30k-40k", 52],
        ["Buddhist", "$30k-40k", 34],
        ["Agnostic", "$40k-50k", 76],
        ["Atheist", "$40k-50k", 35],
        ["Buddhist", "$40k-50k", 33]
    ])"}});

  AssertTablesEqual(*expected, *output, /*same_chunk_layout=*/false);
}

TEST(PivotLongerNode, ExamplesFromTidyr2) {
  // Demonstrates that feature values don't have to exactly match column names
  std::shared_ptr<Schema> test_schema =
      schema({field("artist", utf8()), field("track", utf8()), field("wk1", float64()),
              field("wk2", float64())});
  std::shared_ptr<Table> input = TableFromJSON(test_schema, {{
                                                                R"([
        ["2 Pac", "Baby Don't Cry", 87, 82],
        ["2Ge+her", "The Hardest Part Of", 91, 87]
    ])"}});

  PivotLongerNodeOptions options;
  options.feature_field_names = {"week"};
  options.measurement_field_names = {"rank"};
  options.row_templates = {{{"1"}, {{2}}}, {{"2"}, {{3}}}};

  Declaration plan = Declaration::Sequence(
      {{"table_source", TableSourceNodeOptions(std::move(input))},
       {"pivot_longer", options},
       {"project",
        ProjectNodeOptions({field_ref(0), field_ref(1), field_ref(4), field_ref(5)},
                           {"artist", "track", "week", "rank"})}});

  ASSERT_OK_AND_ASSIGN(std::shared_ptr<Table> output,
                       DeclarationToTable(std::move(plan)));

  std::shared_ptr<Schema> expected_schema =
      schema({field("artist", utf8()), field("track", utf8()), field("week", utf8()),
              field("rank", float64())});
  std::shared_ptr<Table> expected = TableFromJSON(expected_schema, {{
                                                                       R"([
        ["2 Pac", "Baby Don't Cry", "1", 87],
        ["2Ge+her", "The Hardest Part Of", "1", 91],
        ["2 Pac", "Baby Don't Cry", "2", 82],
        ["2Ge+her", "The Hardest Part Of", "2", 87]
    ])"}});

  AssertTablesEqual(*expected, *output, /*same_chunk_layout=*/false);
}

TEST(PivotLongerNode, ExamplesFromTidyr3) {
  // Demonstrates that one column can correspond to multiple feature values
  std::shared_ptr<Schema> test_schema =
      schema({field("country", utf8()), field("new_sp_m014", float64()),
              field("new_sp_m1524", float64())});
  std::shared_ptr<Table> input = TableFromJSON(test_schema, {{
                                                                R"([
        ["Afghanistan", 1, 2]
    ])"}});

  PivotLongerNodeOptions options;
  options.feature_field_names = {"diagnosis", "gender", "age"};
  options.measurement_field_names = {"count"};
  options.row_templates = {{{"sp", "m", "014"}, {{1}}}, {{"sp", "m", "1524"}, {{2}}}};

  Declaration plan = Declaration::Sequence(
      {{"table_source", TableSourceNodeOptions(std::move(input))},
       {"pivot_longer", options},
       {"project",
        ProjectNodeOptions(
            {field_ref(0), field_ref(3), field_ref(4), field_ref(5), field_ref(6)},
            {"country", "diagnosis", "gender", "age", "count"})}});

  ASSERT_OK_AND_ASSIGN(std::shared_ptr<Table> output,
                       DeclarationToTable(std::move(plan)));

  std::shared_ptr<Schema> expected_schema =
      schema({field("country", utf8()), field("diagnosis", utf8()),
              field("gender", utf8()), field("age", utf8()), field("count", float64())});
  std::shared_ptr<Table> expected = TableFromJSON(expected_schema, {{
                                                                       R"([
        ["Afghanistan", "sp", "m", "014", 1],
        ["Afghanistan", "sp", "m", "1524", 2]
    ])"}});

  AssertTablesEqual(*expected, *output, /*same_chunk_layout=*/false);
}

TEST(PivotLongerNode, ExamplesFromTidyr4) {
  // A more interesting case that demonstrates multiple values per row
  std::shared_ptr<Schema> test_schema =
      schema({field("x1", float64()), field("x2", float64()), field("y1", float64()),
              field("y2", float64())});
  std::shared_ptr<Table> input = TableFromJSON(test_schema, {{
                                                                R"([
        [10, 10, 8.04, 9.14],
        [8, 8, 6.95, 8.14]
    ])"}});

  PivotLongerNodeOptions options;
  options.feature_field_names = {"set"};
  options.measurement_field_names = {"x", "y"};
  options.row_templates = {{{"1"}, {{0}, {2}}}, {{"2"}, {{1}, {3}}}};

  Declaration plan = Declaration::Sequence(
      {{"table_source", TableSourceNodeOptions(std::move(input))},
       {"pivot_longer", options},
       {"project", ProjectNodeOptions({field_ref(4), field_ref(5), field_ref(6)},
                                      {"set", "x", "y"})}});

  ASSERT_OK_AND_ASSIGN(std::shared_ptr<Table> output,
                       DeclarationToTable(std::move(plan)));

  std::shared_ptr<Schema> expected_schema =
      schema({field("set", utf8()), field("x", float64()), field("y", float64())});
  std::shared_ptr<Table> expected = TableFromJSON(expected_schema, {{
                                                                       R"([
        ["1", 10, 8.04],
        ["1", 8, 6.95],
        ["2", 10, 9.14],
        ["2", 8, 8.14]
    ])"}});

  AssertTablesEqual(*expected, *output, /*same_chunk_layout=*/false);
}

}  // namespace compute
}  // namespace arrow
