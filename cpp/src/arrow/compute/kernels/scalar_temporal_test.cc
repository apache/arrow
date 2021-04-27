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
#include "arrow/compute/kernels/test_util.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/formatting.h"

namespace arrow {

using internal::StringFormatter;

class TestArray : public ::testing::Test {
 public:
  void SetUp() { pool_ = default_memory_pool(); }

 protected:
  MemoryPool* pool_;
};

namespace compute {

TEST(TestArray, TestTemporalcomponentExtraction) {
  const char* json =
      R"(["1970-01-01T10:10:59","2000-02-29T23:23:23","3989-07-14T18:04:01","1900-02-28T07:59:20"])";
  const char* year = "[1970, 2000, 3989, 1900]";
  const char* month = "[1, 2, 7, 2]";
  const char* day = "[1, 29, 14, 28]";
  const char* day_of_year = "[0, 59, 194, 58]";
  const char* week = "[1, 9, 28, 9]";
  const char* quarter = "[1, 1, 3, 1]";
  const char* day_of_week = "[4, 2, 5, 3]";
  const char* hour = "[10, 23, 18, 7]";
  const char* minute = "[10, 23, 4, 59]";
  const char* second = "[59, 23, 1, 20]";

  CheckScalarUnary("year", ArrayFromJSON(timestamp(TimeUnit::SECOND), json),
                   ArrayFromJSON(int64(), year));
  CheckScalarUnary("month", ArrayFromJSON(timestamp(TimeUnit::SECOND), json),
                   ArrayFromJSON(int64(), month));
  CheckScalarUnary("day", ArrayFromJSON(timestamp(TimeUnit::SECOND), json),
                   ArrayFromJSON(int64(), day));
  CheckScalarUnary("day_of_year", ArrayFromJSON(timestamp(TimeUnit::SECOND), json),
                   ArrayFromJSON(int64(), day_of_year));
  CheckScalarUnary("week", ArrayFromJSON(timestamp(TimeUnit::SECOND), json),
                   ArrayFromJSON(int64(), week));
  CheckScalarUnary("quarter", ArrayFromJSON(timestamp(TimeUnit::SECOND), json),
                   ArrayFromJSON(int64(), quarter));
  CheckScalarUnary("day_of_week", ArrayFromJSON(timestamp(TimeUnit::SECOND), json),
                   ArrayFromJSON(int64(), day_of_week));
  CheckScalarUnary("hour", ArrayFromJSON(timestamp(TimeUnit::SECOND), json),
                   ArrayFromJSON(int64(), hour));
  CheckScalarUnary("minute", ArrayFromJSON(timestamp(TimeUnit::SECOND), json),
                   ArrayFromJSON(int64(), minute));
  CheckScalarUnary("second", ArrayFromJSON(timestamp(TimeUnit::SECOND), json),
                   ArrayFromJSON(int64(), second));
}

}  // namespace compute
}  // namespace arrow
