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

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "arrow/dataset/api.h"
#include "arrow/dataset/partition.h"
#include "arrow/dataset/test_util.h"
#include "arrow/filesystem/localfs.h"
#include "arrow/filesystem/path_util.h"
#include "arrow/status.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/util/io_util.h"
#include "arrow/util/string_view.h"

namespace arrow {
namespace dataset {

class TestPartitionScheme : public ::testing::Test {
 public:
  Status Parse(util::string_view path, std::shared_ptr<Expression>* parsed) {
    if (scheme_->CanParse(path)) {
      return scheme_->Parse(path, parsed);
    }
    return Status::Invalid("can't parse ", path);
  }

  Status Parse(util::string_view path) {
    std::shared_ptr<Expression> ignored;
    return Parse(path, &ignored);
  }

  void AssertParse(util::string_view path, std::shared_ptr<Expression> expected) {
    std::shared_ptr<Expression> parsed;
    ASSERT_OK(Parse(path, &parsed));

    if (parsed == nullptr) {
      ASSERT_EQ(expected, nullptr);
    } else {
      ASSERT_TRUE(parsed->Equals(*expected)) << parsed->ToString() << "\n"
                                             << expected->ToString();
    }
  }

  void AssertParse(util::string_view path, const Expression& expected) {
    AssertParse(path, expected.Copy());
  }

 protected:
  std::shared_ptr<PartitionScheme> scheme_;
};

TEST_F(TestPartitionScheme, Simple) {
  auto expr = equal(field_ref("alpha"), scalar<int16_t>(3));
  scheme_ = std::make_shared<SimplePartitionScheme>(expr);
  AssertParse("/hello/world", expr);
}

TEST_F(TestPartitionScheme, HiveBasics) {
  scheme_ = std::make_shared<HivePartitionScheme>(
      schema({field("alpha", int32()), field("beta", float32())}));

  AssertParse("/base/alpha=0/beta=3.25", "alpha"_ == int32_t(0) and "beta"_ == 3.25f);
  AssertParse("/base/alpha=0/beta=3.25/ignored=2341",
              "alpha"_ == int32_t(0) and "beta"_ == 3.25f);
}

TEST_F(TestPartitionScheme, HiveParseError) {
  scheme_ = std::make_shared<HivePartitionScheme>(
      schema({field("alpha", int32()), field("beta", float32())}));

  ASSERT_RAISES(Invalid, Parse("/base/alpha=0.0/beta=3.25"));
}

}  // namespace dataset
}  // namespace arrow
