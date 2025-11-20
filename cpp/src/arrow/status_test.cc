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

#include <sstream>

#include <gmock/gmock-matchers.h>
#include <gtest/gtest.h>

#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/status_internal.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/matchers.h"

namespace arrow {

namespace {

class TestStatusDetail : public StatusDetail {
 public:
  const char* type_id() const override { return "type_id"; }
  std::string ToString() const override { return "a specific detail message"; }
};

}  // namespace

namespace my_namespace {

struct StatusLike {
  int value;  // ok if 42
};

}  // namespace my_namespace

template <>
struct IntoStatus<my_namespace::StatusLike> {
  static inline Status ToStatus(my_namespace::StatusLike v) {
    if (v.value == 42) {
      return Status::OK();
    } else {
      return Status::UnknownError("StatusLike: ", v.value);
    }
  }
};

TEST(StatusTest, TestCodeAndMessage) {
  Status ok = Status::OK();
  ASSERT_EQ(StatusCode::OK, ok.code());
  Status file_error = Status::IOError("file error");
  ASSERT_EQ(StatusCode::IOError, file_error.code());
  ASSERT_EQ("file error", file_error.message());
}

TEST(StatusTest, TestToString) {
  Status file_error = Status::IOError("file error");
  ASSERT_EQ("IOError: file error", file_error.ToString());

  std::stringstream ss;
  ss << file_error;
  ASSERT_EQ(file_error.ToString(), ss.str());
}

TEST(StatusTest, TestToStringWithDetail) {
  Status status(StatusCode::IOError, "summary", std::make_shared<TestStatusDetail>());
  ASSERT_EQ("IOError: summary. Detail: a specific detail message", status.ToString());

  std::stringstream ss;
  ss << status;
  ASSERT_EQ(status.ToString(), ss.str());
}

TEST(StatusTest, TestWithDetail) {
  Status status(StatusCode::IOError, "summary");
  auto detail = std::make_shared<TestStatusDetail>();
  Status new_status = status.WithDetail(detail);

  ASSERT_EQ(new_status.code(), status.code());
  ASSERT_EQ(new_status.message(), status.message());
  ASSERT_EQ(new_status.detail(), detail);
}

TEST(StatusTest, TestCoverageWarnNotOK) {
  ARROW_WARN_NOT_OK(Status::Invalid("invalid"), "Expected warning");
}

TEST(StatusTest, StatusConstant) {
  internal::StatusConstant constant{StatusCode::Invalid, "default error"};
  Status st = constant;

  ASSERT_EQ(st.code(), StatusCode::Invalid);
  ASSERT_EQ(st.message(), "default error");
  ASSERT_EQ(st.detail(), nullptr);

  Status copy = st;
  ASSERT_EQ(&st.message(), &copy.message());
  Status moved = std::move(st);
  ASSERT_EQ(&copy.message(), &moved.message());
  ASSERT_OK(st);

  Status other = constant;
  ASSERT_EQ(other.code(), StatusCode::Invalid);
  ASSERT_EQ(other.message(), "default error");
  ASSERT_EQ(other.detail(), nullptr);
  ASSERT_EQ(&other.message(), &moved.message());
}

TEST(StatusTest, AndStatus) {
  Status a = Status::OK();
  Status b = Status::OK();
  Status c = Status::Invalid("invalid value");
  Status d = Status::IOError("file error");

  Status res;
  res = a & b;
  ASSERT_TRUE(res.ok());
  res = a & c;
  ASSERT_TRUE(res.IsInvalid());
  res = d & c;
  ASSERT_TRUE(res.IsIOError());

  res = Status::OK();
  res &= c;
  ASSERT_TRUE(res.IsInvalid());
  res &= d;
  ASSERT_TRUE(res.IsInvalid());

  // With rvalues
  res = Status::OK() & Status::Invalid("foo");
  ASSERT_TRUE(res.IsInvalid());
  res = Status::Invalid("foo") & Status::OK();
  ASSERT_TRUE(res.IsInvalid());
  res = Status::Invalid("foo") & Status::IOError("bar");
  ASSERT_TRUE(res.IsInvalid());

  res = Status::OK();
  res &= Status::OK();
  ASSERT_TRUE(res.ok());
  res &= Status::Invalid("foo");
  ASSERT_TRUE(res.IsInvalid());
  res &= Status::IOError("bar");
  ASSERT_TRUE(res.IsInvalid());
}

TEST(StatusTest, TestEquality) {
  ASSERT_EQ(Status(), Status::OK());
  ASSERT_EQ(Status::Invalid("error"), Status::Invalid("error"));

  ASSERT_NE(Status::Invalid("error"), Status::OK());
  ASSERT_NE(Status::Invalid("error"), Status::Invalid("other error"));
}

TEST(StatusTest, MatcherExamples) {
  EXPECT_THAT(Status::Invalid("arbitrary error"), Raises(StatusCode::Invalid));

  EXPECT_THAT(Status::Invalid("arbitrary error"),
              Raises(StatusCode::Invalid, testing::HasSubstr("arbitrary")));

  // message doesn't match, so no match
  EXPECT_THAT(
      Status::Invalid("arbitrary error"),
      testing::Not(Raises(StatusCode::Invalid, testing::HasSubstr("reasonable"))));

  // different error code, so no match
  EXPECT_THAT(Status::TypeError("arbitrary error"),
              testing::Not(Raises(StatusCode::Invalid)));

  // not an error, so no match
  EXPECT_THAT(Status::OK(), testing::Not(Raises(StatusCode::Invalid)));
}

TEST(StatusTest, MatcherDescriptions) {
  testing::Matcher<Status> matcher = Raises(StatusCode::Invalid);

  {
    std::stringstream ss;
    matcher.DescribeTo(&ss);
    EXPECT_THAT(ss.str(), testing::StrEq("raises StatusCode::Invalid"));
  }

  {
    std::stringstream ss;
    matcher.DescribeNegationTo(&ss);
    EXPECT_THAT(ss.str(), testing::StrEq("does not raise StatusCode::Invalid"));
  }
}

TEST(StatusTest, MessageMatcherDescriptions) {
  testing::Matcher<Status> matcher =
      Raises(StatusCode::Invalid, testing::HasSubstr("arbitrary"));

  {
    std::stringstream ss;
    matcher.DescribeTo(&ss);
    EXPECT_THAT(
        ss.str(),
        testing::StrEq(
            "raises StatusCode::Invalid and message has substring \"arbitrary\""));
  }

  {
    std::stringstream ss;
    matcher.DescribeNegationTo(&ss);
    EXPECT_THAT(ss.str(), testing::StrEq("does not raise StatusCode::Invalid or message "
                                         "has no substring \"arbitrary\""));
  }
}

TEST(StatusTest, MatcherExplanations) {
  testing::Matcher<Status> matcher = Raises(StatusCode::Invalid);

  {
    testing::StringMatchResultListener listener;
    EXPECT_TRUE(matcher.MatchAndExplain(Status::Invalid("XXX"), &listener));
    EXPECT_THAT(listener.str(), testing::StrEq("whose error matches"));
  }

  {
    testing::StringMatchResultListener listener;
    EXPECT_FALSE(matcher.MatchAndExplain(Status::OK(), &listener));
    EXPECT_THAT(listener.str(), testing::StrEq("whose non-error doesn't match"));
  }

  {
    testing::StringMatchResultListener listener;
    EXPECT_FALSE(matcher.MatchAndExplain(Status::TypeError("XXX"), &listener));
    EXPECT_THAT(listener.str(), testing::StrEq("whose error doesn't match"));
  }
}

TEST(StatusTest, TestDetailEquality) {
  const auto status_with_detail =
      arrow::Status(StatusCode::IOError, "", std::make_shared<TestStatusDetail>());
  const auto status_with_detail2 =
      arrow::Status(StatusCode::IOError, "", std::make_shared<TestStatusDetail>());
  const auto status_without_detail = arrow::Status::IOError("");

  ASSERT_EQ(*status_with_detail.detail(), *status_with_detail2.detail());
  ASSERT_EQ(status_with_detail, status_with_detail2);
  ASSERT_NE(status_with_detail, status_without_detail);
  ASSERT_NE(status_without_detail, status_with_detail);
}

TEST(StatusTest, OrElse) {
  int called = 0;

  auto or_else_returning_status = [&](Status st) {
    ++called;
    return st.WithMessage("Prefixed: ", st.message());
  };
  auto or_else_returning_result = [&](Status st) {
    ++called;
    return Result<int>(st.WithMessage("Prefixed: ", st.message()));
  };
  auto or_else_returning_user_class = [&](Status st) {
    ++called;
    return my_namespace::StatusLike{43};
  };
  auto or_else_returning_void = [&](auto) { ++called; };

  auto ok_status = Status::OK();
  auto error_status = Status::IOError("some message");
  Status st;

  st = ok_status.OrElse(or_else_returning_status);
  ASSERT_TRUE(st.ok());
  st = ok_status.OrElse(or_else_returning_result);
  ASSERT_TRUE(st.ok());
  st = ok_status.OrElse(or_else_returning_void);
  ASSERT_TRUE(st.ok());
  st = ok_status.OrElse(or_else_returning_user_class);
  ASSERT_TRUE(st.ok());
  ASSERT_EQ(called, 0);

  st = error_status.OrElse(or_else_returning_status);
  ASSERT_EQ(st.code(), StatusCode::IOError);
  ASSERT_EQ(st.message(), "Prefixed: some message");
  ASSERT_EQ(called, 1);
  st = error_status.OrElse(or_else_returning_result);
  ASSERT_EQ(st.code(), StatusCode::IOError);
  ASSERT_EQ(st.message(), "Prefixed: some message");
  ASSERT_EQ(called, 2);
  st = error_status.OrElse(or_else_returning_void);
  ASSERT_EQ(st.code(), StatusCode::IOError);
  ASSERT_EQ(st.message(), "some message");
  ASSERT_EQ(called, 3);
  st = error_status.OrElse(or_else_returning_user_class);
  ASSERT_EQ(st.code(), StatusCode::UnknownError);
  ASSERT_EQ(st.message(), "StatusLike: 43");
  ASSERT_EQ(called, 4);
}

std::string StripContext(const std::string& message) {
#ifdef ARROW_EXTRA_ERROR_CONTEXT
  auto pos = message.find_first_of('\n');
  if (pos != message.npos) {
    return message.substr(0, pos);
  }
#endif
  return message;
}

TEST(StatusTest, ReturnIfNotOk) {
  auto f = [](auto v) {
    RETURN_NOT_OK(v);
    return Status::OK();
  };

  auto ok_status = Status::OK();
  auto error_status = Status::IOError("some message");
  Status st;

  st = f(ok_status);
  ASSERT_TRUE(st.ok());
  st = f(error_status);
  ASSERT_EQ(st.code(), StatusCode::IOError);
  ASSERT_EQ(StripContext(st.message()), error_status.message());

  st = f(Result<int>(42));
  ASSERT_TRUE(st.ok());
  st = f(Result<int>(error_status));
  ASSERT_EQ(st.code(), StatusCode::IOError);
  ASSERT_EQ(StripContext(st.message()), error_status.message());

  st = f(my_namespace::StatusLike{42});
  ASSERT_TRUE(st.ok());
  st = f(my_namespace::StatusLike{43});
  ASSERT_EQ(st.code(), StatusCode::UnknownError);
  ASSERT_EQ(StripContext(st.message()), "StatusLike: 43");
}

}  // namespace arrow
