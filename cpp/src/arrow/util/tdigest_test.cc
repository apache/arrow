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

// XXX: There's no rigid error bound available. The accuracy is to some degree
// *random*, which depends on input data and quantiles to be calculated. I also
// find small gaps among linux/windows/macos.
// In below tests, most quantiles are within 1% deviation from exact values,
// while the worst test case is about 10% drift.
// To make test result stable, I relaxed error bound to be *good enough*.
// #define _TDIGEST_STRICT_TEST   // enable more strict tests

#include <algorithm>
#include <cmath>
#include <numeric>
#include <vector>

#include <gtest/gtest.h>

#include "arrow/testing/gtest_util.h"
#include "arrow/testing/random.h"
#include "arrow/testing/util.h"
#include "arrow/util/tdigest_internal.h"

namespace arrow {
namespace internal {

TEST(TDigestTest, SingleValue) {
  const double value = 0.12345678;

  TDigest td;
  td.Add(value);
  ASSERT_OK(td.Validate());
  // all quantiles equal to same single value
  for (double q = 0; q <= 1; q += 0.1) {
    EXPECT_EQ(td.Quantile(q), value);
  }
}

TEST(TDigestTest, FewValues) {
  // exact quantile at 0.1 interval, test sorted and unsorted input
  std::vector<std::vector<double>> values_vector = {
      {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
      {4, 1, 9, 0, 3, 2, 5, 6, 8, 7, 10},
  };

  for (const auto& values : values_vector) {
    TDigest td;
    for (double v : values) {
      td.Add(v);
    }
    ASSERT_OK(td.Validate());

    double q = 0;
    for (size_t i = 0; i < values.size(); ++i) {
      double expected = static_cast<double>(i);
      EXPECT_EQ(td.Quantile(q), expected);
      q += 0.1;
    }
  }
}

// Calculate exact quantile as truth
std::vector<double> ExactQuantile(std::vector<double> values,
                                  const std::vector<double>& quantiles) {
  std::sort(values.begin(), values.end());

  std::vector<double> output;
  for (double q : quantiles) {
    const double index = (values.size() - 1) * q;
    const int64_t lower_index = static_cast<int64_t>(index);
    const double fraction = index - lower_index;
    if (fraction == 0) {
      output.push_back(values[lower_index]);
    } else {
      const double lerp =
          fraction * values[lower_index + 1] + (1 - fraction) * values[lower_index];
      output.push_back(lerp);
    }
  }
  return output;
}

void TestRandom(size_t size) {
  const std::vector<double> fixed_quantiles = {0, 0.01, 0.1, 0.2, 0.5, 0.8, 0.9, 0.99, 1};

  // append random quantiles to test
  std::vector<double> quantiles;
  random_real(50, 0x11223344, 0.0, 1.0, &quantiles);
  quantiles.insert(quantiles.end(), fixed_quantiles.cbegin(), fixed_quantiles.cend());

  // generate random test values
  const double min = 1e3, max = 1e10;
  std::vector<double> values;
  random_real(size, 0x11223344, min, max, &values);

  TDigest td(200);
  for (double value : values) {
    td.Add(value);
  }
  ASSERT_OK(td.Validate());

  const std::vector<double> expected = ExactQuantile(values, quantiles);
  std::vector<double> approximated;
  for (auto q : quantiles) {
    approximated.push_back(td.Quantile(q));
  }

  // r-square of expected and approximated quantiles should be greater than 0.999
  const double expected_mean =
      std::accumulate(expected.begin(), expected.end(), 0.0) / expected.size();
  double rss = 0, tss = 0;
  for (size_t i = 0; i < quantiles.size(); ++i) {
    rss += (expected[i] - approximated[i]) * (expected[i] - approximated[i]);
    tss += (expected[i] - expected_mean) * (expected[i] - expected_mean);
  }
  const double r2 = 1 - rss / tss;
  EXPECT_GT(r2, 0.999);

  // make sure no quantile drifts too much from the truth
#ifdef _TDIGEST_STRICT_TEST
  const double error_ratio = 0.02;
#else
  const double error_ratio = 0.05;
#endif
  for (size_t i = 0; i < quantiles.size(); ++i) {
    const double tolerance = std::fabs(expected[i]) * error_ratio;
    EXPECT_NEAR(approximated[i], expected[i], tolerance) << quantiles[i];
  }
}

TEST(TDigestTest, RandomValues) { TestRandom(100000); }

// too heavy to run in ci
TEST(TDigestTest, DISABLED_HugeVolume) { TestRandom(1U << 30); }

void TestMerge(const std::vector<std::vector<double>>& values_vector, uint32_t delta,
               double error_ratio) {
  const std::vector<double> quantiles = {0,   0.01, 0.1, 0.2, 0.3,  0.4, 0.5,
                                         0.6, 0.7,  0.8, 0.9, 0.99, 1};

  std::vector<TDigest> tds;
  for (const auto& values : values_vector) {
    TDigest td(delta);
    for (double value : values) {
      td.Add(value);
    }
    ASSERT_OK(td.Validate());
    tds.push_back(std::move(td));
  }

  std::vector<double> values_combined;
  for (const auto& values : values_vector) {
    values_combined.insert(values_combined.end(), values.begin(), values.end());
  }
  const std::vector<double> expected = ExactQuantile(values_combined, quantiles);

  // merge into an empty tdigest
  {
    TDigest td(delta);
    td.Merge(tds);
    ASSERT_OK(td.Validate());
    for (size_t i = 0; i < quantiles.size(); ++i) {
      const double tolerance = std::max(std::fabs(expected[i]) * error_ratio, 0.1);
      EXPECT_NEAR(td.Quantile(quantiles[i]), expected[i], tolerance) << quantiles[i];
    }
  }

  // merge into a non empty tdigest
  {
    TDigest td = std::move(tds[0]);
    tds.erase(tds.begin(), tds.begin() + 1);
    td.Merge(tds);
    ASSERT_OK(td.Validate());
    for (size_t i = 0; i < quantiles.size(); ++i) {
      const double tolerance = std::max(std::fabs(expected[i]) * error_ratio, 0.1);
      EXPECT_NEAR(td.Quantile(quantiles[i]), expected[i], tolerance) << quantiles[i];
    }
  }
}

// merge tdigests with same distribution
TEST(TDigestTest, MergeUniform) {
  const std::vector<size_t> sizes = {20000, 3000, 1500, 18000, 9999, 6666};
  std::vector<std::vector<double>> values_vector;
  for (auto size : sizes) {
    std::vector<double> values;
    random_real(size, 0x11223344, -123456789.0, 987654321.0, &values);
    values_vector.push_back(std::move(values));
  }

#ifdef _TDIGEST_STRICT_TEST
  TestMerge(values_vector, /*delta=*/100, /*error_ratio=*/0.01);
#else
  TestMerge(values_vector, /*delta=*/200, /*error_ratio=*/0.05);
#endif
}

// merge tdigests with different distributions
TEST(TDigestTest, MergeNonUniform) {
  struct {
    size_t size;
    double min;
    double max;
  } configs[] = {
      {2000, 1e8, 1e9}, {0, 0, 0}, {3000, -1, 1}, {500, -1e6, -1e5}, {800, 100, 100},
  };
  std::vector<std::vector<double>> values_vector;
  for (const auto& cfg : configs) {
    std::vector<double> values;
    random_real(cfg.size, 0x11223344, cfg.min, cfg.max, &values);
    values_vector.push_back(std::move(values));
  }

#ifdef _TDIGEST_STRICT_TEST
  TestMerge(values_vector, /*delta=*/200, /*error_ratio=*/0.01);
#else
  TestMerge(values_vector, /*delta=*/200, /*error_ratio=*/0.05);
#endif
}

TEST(TDigestTest, Misc) {
  const size_t size = 100000;
  const double min = -1000, max = 1000;
  const std::vector<double> quantiles = {0, 0.01, 0.1, 0.4, 0.7, 0.9, 0.99, 1};

  std::vector<double> values;
  random_real(size, 0x11223344, min, max, &values);
  const std::vector<double> expected = ExactQuantile(values, quantiles);

  // test small delta and buffer
  {
#ifdef _TDIGEST_STRICT_TEST
    const double error_ratio = 0.06;  // low accuracy for small delta
#else
    const double error_ratio = 0.15;
#endif

    TDigest td(10, 50);
    for (double value : values) {
      td.Add(value);
    }
    ASSERT_OK(td.Validate());

    for (size_t i = 0; i < quantiles.size(); ++i) {
      const double tolerance = std::max(std::fabs(expected[i]) * error_ratio, 0.1);
      EXPECT_NEAR(td.Quantile(quantiles[i]), expected[i], tolerance) << quantiles[i];
    }
  }

  // test many duplicated values
  {
#ifdef _TDIGEST_STRICT_TEST
    const double error_ratio = 0.02;
#else
    const double error_ratio = 0.05;
#endif

    auto values_integer = values;
    for (double& value : values_integer) {
      value = std::ceil(value);
    }

    TDigest td(100);
    for (double value : values_integer) {
      td.Add(value);
    }
    ASSERT_OK(td.Validate());

    for (size_t i = 0; i < quantiles.size(); ++i) {
      const double tolerance = std::max(std::fabs(expected[i]) * error_ratio, 0.1);
      EXPECT_NEAR(td.Quantile(quantiles[i]), expected[i], tolerance) << quantiles[i];
    }
  }
}

}  // namespace internal
}  // namespace arrow
