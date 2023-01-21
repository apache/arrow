/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include <catch2/catch.hpp>

#include <vector>
#include <utility>

#include <iostream>

#include "quantiles_sorted_view.hpp"

namespace datasketches {

TEST_CASE("empty", "sorted view") {
  auto view = quantiles_sorted_view<float, std::less<float>, std::allocator<float>>(1, std::less<float>(), std::allocator<float>());
  REQUIRE_THROWS_AS(view.get_rank(0), std::runtime_error);
  REQUIRE_THROWS_AS(view.get_quantile(0), std::runtime_error);
  const float split_points[1] {0};
  REQUIRE_THROWS_AS(view.get_CDF(split_points, 1), std::runtime_error);
  REQUIRE_THROWS_AS(view.get_PMF(split_points, 1), std::runtime_error);
}

TEST_CASE("set 0", "sorted view") {
  auto view = quantiles_sorted_view<float, std::less<float>, std::allocator<float>>(1, std::less<float>(), std::allocator<float>());
    std::vector<float> l0 {10};
    view.add(l0.begin(), l0.end(), 1);
    view.convert_to_cummulative();
    REQUIRE(view.size() == 1);

    auto it = view.begin();
    // using operator->
    REQUIRE(it->first == 10);
    REQUIRE(it->second == 1);
    // using operator*
    REQUIRE((*it).first == 10);
    REQUIRE((*it).second == 1);
    REQUIRE(it.get_weight() == 1);
    REQUIRE(it.get_cumulative_weight() == 1);
    REQUIRE(it.get_cumulative_weight(false) == 0);
    ++it;
    REQUIRE(it == view.end());

    REQUIRE(view.get_rank(5, true) == 0);
    REQUIRE(view.get_rank(10, true) == 1);
    REQUIRE(view.get_rank(15, true) == 1);

    REQUIRE(view.get_rank(5, false) == 0);
    REQUIRE(view.get_rank(10, false) == 0);
    REQUIRE(view.get_rank(15, false) == 1);

    REQUIRE(view.get_quantile(0, true) == 10);
    REQUIRE(view.get_quantile(0.5, true) == 10);
    REQUIRE(view.get_quantile(1, true) == 10);

    REQUIRE(view.get_quantile(0, false) == 10);
    REQUIRE(view.get_quantile(0.5, false) == 10);
    REQUIRE(view.get_quantile(1, false) == 10);
}

TEST_CASE("set 1", "sorted view") {
  auto view = quantiles_sorted_view<float, std::less<float>, std::allocator<float>>(1, std::less<float>(), std::allocator<float>());
    std::vector<float> l0 {10, 10};
    view.add(l0.begin(), l0.end(), 1);
    view.convert_to_cummulative();
    REQUIRE(view.size() == 2);

    auto it = view.begin();
    REQUIRE(it->first == 10);
    REQUIRE(it->second == 1);
    REQUIRE(it.get_weight() == 1);
    REQUIRE(it.get_cumulative_weight() == 1);
    REQUIRE(it.get_cumulative_weight(false) == 0);
    ++it;
    REQUIRE(it->first == 10);
    REQUIRE(it->second == 2);
    REQUIRE(it.get_weight() == 1);
    REQUIRE(it.get_cumulative_weight() == 2);
    REQUIRE(it.get_cumulative_weight(false) == 1);
    ++it;
    REQUIRE(it == view.end());

    REQUIRE(view.get_rank(5, true) == 0);
    REQUIRE(view.get_rank(10, true) == 1);
    REQUIRE(view.get_rank(15, true) == 1);

    REQUIRE(view.get_rank(5, false) == 0);
    REQUIRE(view.get_rank(10, false) == 0);
    REQUIRE(view.get_rank(15, false) == 1);

    REQUIRE(view.get_quantile(0, true) == 10);
    REQUIRE(view.get_quantile(0.25, true) == 10);
    REQUIRE(view.get_quantile(0.5, true) == 10);
    REQUIRE(view.get_quantile(0.75, true) == 10);
    REQUIRE(view.get_quantile(1, true) == 10);

    REQUIRE(view.get_quantile(0, false) == 10);
    REQUIRE(view.get_quantile(0.25, false) == 10);
    REQUIRE(view.get_quantile(0.5, false) == 10);
    REQUIRE(view.get_quantile(0.75, false) == 10);
    REQUIRE(view.get_quantile(1, false) == 10);
}

TEST_CASE("set 2", "sorted view") {
  auto view = quantiles_sorted_view<float, std::less<float>, std::allocator<float>>(1, std::less<float>(), std::allocator<float>());
    std::vector<float> l1 {10, 20, 30, 40};
    view.add(l1.begin(), l1.end(), 2);
    view.convert_to_cummulative();
    REQUIRE(view.size() == 4);

    auto it = view.begin();
    REQUIRE(it->first == 10);
    REQUIRE(it->second == 2);
    REQUIRE(it.get_weight() == 2);
    REQUIRE(it.get_cumulative_weight() == 2);
    REQUIRE(it.get_cumulative_weight(false) == 0);
    ++it;
    REQUIRE(it->first == 20);
    REQUIRE(it->second == 4);
    REQUIRE(it.get_weight() == 2);
    REQUIRE(it.get_cumulative_weight() == 4);
    REQUIRE(it.get_cumulative_weight(false) == 2);
    ++it;
    REQUIRE(it->first == 30);
    REQUIRE(it->second == 6);
    REQUIRE(it.get_weight() == 2);
    REQUIRE(it.get_cumulative_weight() == 6);
    REQUIRE(it.get_cumulative_weight(false) == 4);
    ++it;
    REQUIRE(it->first == 40);
    REQUIRE(it->second == 8);
    REQUIRE(it.get_weight() == 2);
    REQUIRE(it.get_cumulative_weight() == 8);
    REQUIRE(it.get_cumulative_weight(false) == 6);
    ++it;
    REQUIRE(it == view.end());

    REQUIRE(view.get_rank(5, true) == 0);
    REQUIRE(view.get_rank(10, true) == 0.25);
    REQUIRE(view.get_rank(15, true) == 0.25);
    REQUIRE(view.get_rank(20, true) == 0.5);
    REQUIRE(view.get_rank(25, true) == 0.5);
    REQUIRE(view.get_rank(30, true) == 0.75);
    REQUIRE(view.get_rank(35, true) == 0.75);
    REQUIRE(view.get_rank(40, true) == 1);
    REQUIRE(view.get_rank(45, true) == 1);

    REQUIRE(view.get_rank(5, false) == 0);
    REQUIRE(view.get_rank(10, false) == 0);
    REQUIRE(view.get_rank(15, false) == 0.25);
    REQUIRE(view.get_rank(20, false) == 0.25);
    REQUIRE(view.get_rank(25, false) == 0.5);
    REQUIRE(view.get_rank(30, false) == 0.5);
    REQUIRE(view.get_rank(35, false) == 0.75);
    REQUIRE(view.get_rank(40, false) == 0.75);
    REQUIRE(view.get_rank(45, false) == 1);

    REQUIRE(view.get_quantile(0, true) == 10);
    REQUIRE(view.get_quantile(0.0625, true) == 10);
    REQUIRE(view.get_quantile(0.125, true) == 10);
    REQUIRE(view.get_quantile(0.1875, true) == 10);
    REQUIRE(view.get_quantile(0.25, true) == 10);
    REQUIRE(view.get_quantile(0.3125, true) == 20);
    REQUIRE(view.get_quantile(0.375, true) == 20);
    REQUIRE(view.get_quantile(0.4375, true) == 20);
    REQUIRE(view.get_quantile(0.5, true) == 20);
    REQUIRE(view.get_quantile(0.5625, true) == 30);
    REQUIRE(view.get_quantile(0.625, true) == 30);
    REQUIRE(view.get_quantile(0.6875, true) == 30);
    REQUIRE(view.get_quantile(0.75, true) == 30);
    REQUIRE(view.get_quantile(0.8125, true) == 40);
    REQUIRE(view.get_quantile(0.875, true) == 40);
    REQUIRE(view.get_quantile(0.9375, true) == 40);
    REQUIRE(view.get_quantile(1, true) == 40);

    REQUIRE(view.get_quantile(0, false) == 10);
    REQUIRE(view.get_quantile(0.0625, false) == 10);
    REQUIRE(view.get_quantile(0.125, false) == 10);
    REQUIRE(view.get_quantile(0.1875, false) == 10);
    REQUIRE(view.get_quantile(0.25, false) == 20);
    REQUIRE(view.get_quantile(0.3125, false) == 20);
    REQUIRE(view.get_quantile(0.375, false) == 20);
    REQUIRE(view.get_quantile(0.4375, false) == 20);
    REQUIRE(view.get_quantile(0.5, false) == 30);
    REQUIRE(view.get_quantile(0.5625, false) == 30);
    REQUIRE(view.get_quantile(0.625, false) == 30);
    REQUIRE(view.get_quantile(0.6875, false) == 30);
    REQUIRE(view.get_quantile(0.75, false) == 40);
    REQUIRE(view.get_quantile(0.8125, false) == 40);
    REQUIRE(view.get_quantile(0.875, false) == 40);
    REQUIRE(view.get_quantile(0.9375, false) == 40);
    REQUIRE(view.get_quantile(1, false) == 40);
}

TEST_CASE("set 3", "sorted view") {
  auto view = quantiles_sorted_view<float, std::less<float>, std::allocator<float>>(8, std::less<float>(), std::allocator<float>());
    std::vector<float> l1 {10, 20, 20, 30, 30, 30, 40, 50};
    view.add(l1.begin(), l1.end(), 2);
    view.convert_to_cummulative();
    REQUIRE(view.size() == 8);

    auto it = view.begin();
    REQUIRE(it->first == 10);
    REQUIRE(it->second == 2);
    REQUIRE(it.get_weight() == 2);
    ++it;
    REQUIRE(it->first == 20);
    REQUIRE(it->second == 4);
    REQUIRE(it.get_weight() == 2);
    ++it;
    REQUIRE(it->first == 20);
    REQUIRE(it->second == 6);
    REQUIRE(it.get_weight() == 2);
    ++it;
    REQUIRE(it->first == 30);
    REQUIRE(it->second == 8);
    REQUIRE(it.get_weight() == 2);
    ++it;
    REQUIRE(it->first == 30);
    REQUIRE(it->second == 10);
    REQUIRE(it.get_weight() == 2);
    ++it;
    REQUIRE(it->first == 30);
    REQUIRE(it->second == 12);
    REQUIRE(it.get_weight() == 2);
    ++it;
    REQUIRE(it->first == 40);
    REQUIRE(it->second == 14);
    REQUIRE(it.get_weight() == 2);
    ++it;
    REQUIRE(it->first == 50);
    REQUIRE(it->second == 16);
    REQUIRE(it.get_weight() == 2);

    REQUIRE(view.get_rank(5, true) == 0);
    REQUIRE(view.get_rank(10, true) == 0.125);
    REQUIRE(view.get_rank(15, true) == 0.125);
    REQUIRE(view.get_rank(20, true) == 0.375);
    REQUIRE(view.get_rank(25, true) == 0.375);
    REQUIRE(view.get_rank(30, true) == 0.75);
    REQUIRE(view.get_rank(35, true) == 0.75);
    REQUIRE(view.get_rank(40, true) == 0.875);
    REQUIRE(view.get_rank(45, true) == 0.875);
    REQUIRE(view.get_rank(50, true) == 1);
    REQUIRE(view.get_rank(55, true) == 1);

    REQUIRE(view.get_rank(5, false) == 0);
    REQUIRE(view.get_rank(10, false) == 0);
    REQUIRE(view.get_rank(15, false) == 0.125);
    REQUIRE(view.get_rank(20, false) == 0.125);
    REQUIRE(view.get_rank(25, false) == 0.375);
    REQUIRE(view.get_rank(30, false) == 0.375);
    REQUIRE(view.get_rank(35, false) == 0.75);
    REQUIRE(view.get_rank(40, false) == 0.75);
    REQUIRE(view.get_rank(45, false) == 0.875);
    REQUIRE(view.get_rank(50, false) == 0.875);
    REQUIRE(view.get_rank(55, false) == 1);

    REQUIRE(view.get_quantile(0, true) == 10);
    REQUIRE(view.get_quantile(0.03125, true) == 10);
    REQUIRE(view.get_quantile(0.0625, true) == 10);
    REQUIRE(view.get_quantile(0.09375, true) == 10);
    REQUIRE(view.get_quantile(0.125, true) == 10);
    REQUIRE(view.get_quantile(0.15625, true) == 20);
    REQUIRE(view.get_quantile(0.1875, true) == 20);
    REQUIRE(view.get_quantile(0.21875, true) == 20);
    REQUIRE(view.get_quantile(0.25, true) == 20);
    REQUIRE(view.get_quantile(0.28125, true) == 20);
    REQUIRE(view.get_quantile(0.3125, true) == 20);
    REQUIRE(view.get_quantile(0.34375, true) == 20);
    REQUIRE(view.get_quantile(0.375, true) == 20);
    REQUIRE(view.get_quantile(0.40625, true) == 30);
    REQUIRE(view.get_quantile(0.4375, true) == 30);
    REQUIRE(view.get_quantile(0.46875, true) == 30);
    REQUIRE(view.get_quantile(0.5, true) == 30);
    REQUIRE(view.get_quantile(0.53125, true) == 30);
    REQUIRE(view.get_quantile(0.5625, true) == 30);
    REQUIRE(view.get_quantile(0.59375, true) == 30);
    REQUIRE(view.get_quantile(0.625, true) == 30);
    REQUIRE(view.get_quantile(0.65625, true) == 30);
    REQUIRE(view.get_quantile(0.6875, true) == 30);
    REQUIRE(view.get_quantile(0.71875, true) == 30);
    REQUIRE(view.get_quantile(0.75, true) == 30);
    REQUIRE(view.get_quantile(0.78125, true) == 40);
    REQUIRE(view.get_quantile(0.8125, true) == 40);
    REQUIRE(view.get_quantile(0.84375, true) == 40);
    REQUIRE(view.get_quantile(0.875, true) == 40);
    REQUIRE(view.get_quantile(0.90625, true) == 50);
    REQUIRE(view.get_quantile(0.9375, true) == 50);
    REQUIRE(view.get_quantile(0.96875, true) == 50);
    REQUIRE(view.get_quantile(1, true) == 50);

    REQUIRE(view.get_quantile(0, false) == 10);
    REQUIRE(view.get_quantile(0.03125, false) == 10);
    REQUIRE(view.get_quantile(0.0625, false) == 10);
    REQUIRE(view.get_quantile(0.09375, false) == 10);
    REQUIRE(view.get_quantile(0.125, false) == 20);
    REQUIRE(view.get_quantile(0.15625, false) == 20);
    REQUIRE(view.get_quantile(0.1875, false) == 20);
    REQUIRE(view.get_quantile(0.21875, false) == 20);
    REQUIRE(view.get_quantile(0.25, false) == 20);
    REQUIRE(view.get_quantile(0.28125, false) == 20);
    REQUIRE(view.get_quantile(0.3125, false) == 20);
    REQUIRE(view.get_quantile(0.34375, false) == 20);
    REQUIRE(view.get_quantile(0.375, false) == 30);
    REQUIRE(view.get_quantile(0.40625, false) == 30);
    REQUIRE(view.get_quantile(0.4375, false) == 30);
    REQUIRE(view.get_quantile(0.46875, false) == 30);
    REQUIRE(view.get_quantile(0.5, false) == 30);
    REQUIRE(view.get_quantile(0.53125, false) == 30);
    REQUIRE(view.get_quantile(0.5625, false) == 30);
    REQUIRE(view.get_quantile(0.59375, false) == 30);
    REQUIRE(view.get_quantile(0.625, false) == 30);
    REQUIRE(view.get_quantile(0.65625, false) == 30);
    REQUIRE(view.get_quantile(0.6875, false) == 30);
    REQUIRE(view.get_quantile(0.71875, false) == 30);
    REQUIRE(view.get_quantile(0.75, false) == 40);
    REQUIRE(view.get_quantile(0.78125, false) == 40);
    REQUIRE(view.get_quantile(0.8125, false) == 40);
    REQUIRE(view.get_quantile(0.84375, false) == 40);
    REQUIRE(view.get_quantile(0.875, false) == 50);
    REQUIRE(view.get_quantile(0.90625, false) == 50);
    REQUIRE(view.get_quantile(0.9375, false) == 50);
    REQUIRE(view.get_quantile(0.96875, false) == 50);
    REQUIRE(view.get_quantile(1, false) == 50);
}

TEST_CASE("set 4", "sorted view") {
  auto view = quantiles_sorted_view<float, std::less<float>, std::allocator<float>>(8, std::less<float>(), std::allocator<float>());
    std::vector<float> l1 {10, 20, 30, 40};
    view.add(l1.begin(), l1.end(), 2);
    std::vector<float> l0 {10, 20, 30, 40};
    view.add(l0.begin(), l0.end(), 1);
    view.convert_to_cummulative();
    REQUIRE(view.size() == 8);

    auto it = view.begin();
    REQUIRE(it->first == 10);
    REQUIRE(it->second == 2);
    REQUIRE(it.get_weight() == 2);
    ++it;
    REQUIRE(it->first == 10);
    REQUIRE(it->second == 3);
    REQUIRE(it.get_weight() == 1);
    ++it;
    REQUIRE(it->first == 20);
    REQUIRE(it->second == 5);
    REQUIRE(it.get_weight() == 2);
    ++it;
    REQUIRE(it->first == 20);
    REQUIRE(it->second == 6);
    REQUIRE(it.get_weight() == 1);
    ++it;
    REQUIRE(it->first == 30);
    REQUIRE(it->second == 8);
    REQUIRE(it.get_weight() == 2);
    ++it;
    REQUIRE(it->first == 30);
    REQUIRE(it->second == 9);
    REQUIRE(it.get_weight() == 1);
    ++it;
    REQUIRE(it->first == 40);
    REQUIRE(it->second == 11);
    REQUIRE(it.get_weight() == 2);
    ++it;
    REQUIRE(it->first == 40);
    REQUIRE(it->second == 12);
    REQUIRE(it.get_weight() == 1);

    REQUIRE(view.get_rank(5, true) == 0);
    REQUIRE(view.get_rank(10, true) == 0.25);
    REQUIRE(view.get_rank(15, true) == 0.25);
    REQUIRE(view.get_rank(20, true) == 0.5);
    REQUIRE(view.get_rank(25, true) == 0.5);
    REQUIRE(view.get_rank(30, true) == 0.75);
    REQUIRE(view.get_rank(35, true) == 0.75);
    REQUIRE(view.get_rank(40, true) == 1);
    REQUIRE(view.get_rank(45, true) == 1);

    REQUIRE(view.get_rank(5, false) == 0);
    REQUIRE(view.get_rank(10, false) == 0);
    REQUIRE(view.get_rank(15, false) == 0.25);
    REQUIRE(view.get_rank(20, false) == 0.25);
    REQUIRE(view.get_rank(25, false) == 0.5);
    REQUIRE(view.get_rank(30, false) == 0.5);
    REQUIRE(view.get_rank(35, false) == 0.75);
    REQUIRE(view.get_rank(40, false) == 0.75);
    REQUIRE(view.get_rank(45, false) == 1);

    REQUIRE(view.get_quantile(0, true) == 10);
    REQUIRE(view.get_quantile(0.0417, true) == 10);
    REQUIRE(view.get_quantile(0.0833, true) == 10);
    REQUIRE(view.get_quantile(0.125, true) == 10);
    REQUIRE(view.get_quantile(0.1667, true) == 10);
    REQUIRE(view.get_quantile(0.2083, true) == 10);
    REQUIRE(view.get_quantile(0.25, true) == 10);
    REQUIRE(view.get_quantile(0.2917, true) == 20);
    REQUIRE(view.get_quantile(0.3333, true) == 20);
    REQUIRE(view.get_quantile(0.375, true) == 20);
    REQUIRE(view.get_quantile(0.4167, true) == 20);
    REQUIRE(view.get_quantile(0.4583, true) == 20);
    REQUIRE(view.get_quantile(0.5, true) == 20);
    REQUIRE(view.get_quantile(0.5417, true) == 30);
    REQUIRE(view.get_quantile(0.5833, true) == 30);
    REQUIRE(view.get_quantile(0.625, true) == 30);
    REQUIRE(view.get_quantile(0.6667, true) == 30);
    REQUIRE(view.get_quantile(0.7083, true) == 30);
    REQUIRE(view.get_quantile(0.75, true) == 30);
    REQUIRE(view.get_quantile(0.7917, true) == 40);
    REQUIRE(view.get_quantile(0.8333, true) == 40);
    REQUIRE(view.get_quantile(0.875, true) == 40);
    REQUIRE(view.get_quantile(0.9167, true) == 40);
    REQUIRE(view.get_quantile(0.9583, true) == 40);
    REQUIRE(view.get_quantile(1, true) == 40);

    REQUIRE(view.get_quantile(0, false) == 10);
    REQUIRE(view.get_quantile(0.0417, false) == 10);
    REQUIRE(view.get_quantile(0.0833, false) == 10);
    REQUIRE(view.get_quantile(0.125, false) == 10);
    REQUIRE(view.get_quantile(0.1667, false) == 10);
    REQUIRE(view.get_quantile(0.2083, false) == 10);
    REQUIRE(view.get_quantile(0.25, false) == 20);
    REQUIRE(view.get_quantile(0.2917, false) == 20);
    REQUIRE(view.get_quantile(0.3333, false) == 20);
    REQUIRE(view.get_quantile(0.375, false) == 20);
    REQUIRE(view.get_quantile(0.4167, false) == 20);
    REQUIRE(view.get_quantile(0.4583, false) == 20);
    REQUIRE(view.get_quantile(0.5, false) == 30);
    REQUIRE(view.get_quantile(0.5417, false) == 30);
    REQUIRE(view.get_quantile(0.5833, false) == 30);
    REQUIRE(view.get_quantile(0.625, false) == 30);
    REQUIRE(view.get_quantile(0.6667, false) == 30);
    REQUIRE(view.get_quantile(0.7083, false) == 30);
    REQUIRE(view.get_quantile(0.75, false) == 40);
    REQUIRE(view.get_quantile(0.7917, false) == 40);
    REQUIRE(view.get_quantile(0.8333, false) == 40);
    REQUIRE(view.get_quantile(0.875, false) == 40);
    REQUIRE(view.get_quantile(0.9167, false) == 40);
    REQUIRE(view.get_quantile(0.9583, false) == 40);
    REQUIRE(view.get_quantile(1, false) == 40);
}

} /* namespace datasketches */
