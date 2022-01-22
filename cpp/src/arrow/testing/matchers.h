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

#include <utility>

#include <gmock/gmock-matchers.h>

#include "arrow/datum.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/testing/future_util.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/util/future.h"

namespace arrow {

class PointeesEqualMatcher {
 public:
  template <typename PtrPair>
  operator testing::Matcher<PtrPair>() const {  // NOLINT runtime/explicit
    struct Impl : testing::MatcherInterface<const PtrPair&> {
      void DescribeTo(::std::ostream* os) const override { *os << "pointees are equal"; }

      void DescribeNegationTo(::std::ostream* os) const override {
        *os << "pointees are not equal";
      }

      bool MatchAndExplain(const PtrPair& pair,
                           testing::MatchResultListener* listener) const override {
        const auto& first = *std::get<0>(pair);
        const auto& second = *std::get<1>(pair);
        const bool match = first.Equals(second);
        *listener << "whose pointees " << testing::PrintToString(first) << " and "
                  << testing::PrintToString(second)
                  << (match ? " are equal" : " are not equal");
        return match;
      }
    };

    return testing::Matcher<PtrPair>(new Impl());
  }
};

// A matcher that checks that the values pointed to are Equals().
// Useful in conjunction with other googletest matchers.
inline PointeesEqualMatcher PointeesEqual() { return {}; }

template <typename ResultMatcher>
class FutureMatcher {
 public:
  explicit FutureMatcher(ResultMatcher result_matcher, double wait_seconds)
      : result_matcher_(std::move(result_matcher)), wait_seconds_(wait_seconds) {}

  template <typename Fut,
            typename ValueType = typename std::decay<Fut>::type::ValueType>
  operator testing::Matcher<Fut>() const {  // NOLINT runtime/explicit
    struct Impl : testing::MatcherInterface<const Fut&> {
      explicit Impl(const ResultMatcher& result_matcher, double wait_seconds)
          : result_matcher_(testing::MatcherCast<Result<ValueType>>(result_matcher)),
            wait_seconds_(wait_seconds) {}

      void DescribeTo(::std::ostream* os) const override {
        *os << "value ";
        result_matcher_.DescribeTo(os);
      }

      void DescribeNegationTo(::std::ostream* os) const override {
        *os << "value ";
        result_matcher_.DescribeNegationTo(os);
      }

      bool MatchAndExplain(const Fut& fut,
                           testing::MatchResultListener* listener) const override {
        if (!fut.Wait(wait_seconds_)) {
          *listener << "which didn't finish within " << wait_seconds_ << " seconds";
          return false;
        }
        return result_matcher_.MatchAndExplain(fut.result(), listener);
      }

      const testing::Matcher<Result<ValueType>> result_matcher_;
      const double wait_seconds_;
    };

    return testing::Matcher<Fut>(new Impl(result_matcher_, wait_seconds_));
  }

 private:
  const ResultMatcher result_matcher_;
  const double wait_seconds_;
};

template <typename ValueMatcher>
class ResultMatcher {
 public:
  explicit ResultMatcher(ValueMatcher value_matcher)
      : value_matcher_(std::move(value_matcher)) {}

  template <typename Res,
            typename ValueType = typename std::decay<Res>::type::ValueType>
  operator testing::Matcher<Res>() const {  // NOLINT runtime/explicit
    struct Impl : testing::MatcherInterface<const Res&> {
      explicit Impl(const ValueMatcher& value_matcher)
          : value_matcher_(testing::MatcherCast<ValueType>(value_matcher)) {}

      void DescribeTo(::std::ostream* os) const override {
        *os << "value ";
        value_matcher_.DescribeTo(os);
      }

      void DescribeNegationTo(::std::ostream* os) const override {
        *os << "value ";
        value_matcher_.DescribeNegationTo(os);
      }

      bool MatchAndExplain(const Res& maybe_value,
                           testing::MatchResultListener* listener) const override {
        if (!maybe_value.status().ok()) {
          *listener << "whose error "
                    << testing::PrintToString(maybe_value.status().ToString())
                    << " doesn't match";
          return false;
        }
        const ValueType& value = maybe_value.ValueOrDie();
        testing::StringMatchResultListener value_listener;
        const bool match = value_matcher_.MatchAndExplain(value, &value_listener);
        *listener << "whose value " << testing::PrintToString(value)
                  << (match ? " matches" : " doesn't match");
        testing::internal::PrintIfNotEmpty(value_listener.str(), listener->stream());
        return match;
      }

      const testing::Matcher<ValueType> value_matcher_;
    };

    return testing::Matcher<Res>(new Impl(value_matcher_));
  }

 private:
  const ValueMatcher value_matcher_;
};

class ErrorMatcher {
 public:
  explicit ErrorMatcher(StatusCode code,
                        util::optional<testing::Matcher<std::string>> message_matcher)
      : code_(code), message_matcher_(std::move(message_matcher)) {}

  template <typename Res>
  operator testing::Matcher<Res>() const {  // NOLINT runtime/explicit
    struct Impl : testing::MatcherInterface<const Res&> {
      explicit Impl(StatusCode code,
                    util::optional<testing::Matcher<std::string>> message_matcher)
          : code_(code), message_matcher_(std::move(message_matcher)) {}

      void DescribeTo(::std::ostream* os) const override {
        *os << "raises StatusCode::" << Status::CodeAsString(code_);
        if (message_matcher_) {
          *os << " and message ";
          message_matcher_->DescribeTo(os);
        }
      }

      void DescribeNegationTo(::std::ostream* os) const override {
        *os << "does not raise StatusCode::" << Status::CodeAsString(code_);
        if (message_matcher_) {
          *os << " or message ";
          message_matcher_->DescribeNegationTo(os);
        }
      }

      bool MatchAndExplain(const Res& maybe_value,
                           testing::MatchResultListener* listener) const override {
        const Status& status = internal::GenericToStatus(maybe_value);
        testing::StringMatchResultListener value_listener;

        bool match = status.code() == code_;
        if (message_matcher_) {
          match = match &&
                  message_matcher_->MatchAndExplain(status.message(), &value_listener);
        }

        *listener << "whose value " << testing::PrintToString(status.ToString())
                  << (match ? " matches" : " doesn't match");
        testing::internal::PrintIfNotEmpty(value_listener.str(), listener->stream());
        return match;
      }

      const StatusCode code_;
      const util::optional<testing::Matcher<std::string>> message_matcher_;
    };

    return testing::Matcher<Res>(new Impl(code_, message_matcher_));
  }

 private:
  const StatusCode code_;
  const util::optional<testing::Matcher<std::string>> message_matcher_;
};

class OkMatcher {
 public:
  template <typename Res>
  operator testing::Matcher<Res>() const {  // NOLINT runtime/explicit
    struct Impl : testing::MatcherInterface<const Res&> {
      void DescribeTo(::std::ostream* os) const override { *os << "is ok"; }

      void DescribeNegationTo(::std::ostream* os) const override { *os << "is not ok"; }

      bool MatchAndExplain(const Res& maybe_value,
                           testing::MatchResultListener* listener) const override {
        const Status& status = internal::GenericToStatus(maybe_value);

        const bool match = status.ok();
        *listener << "whose value " << testing::PrintToString(status.ToString())
                  << (match ? " matches" : " doesn't match");
        return match;
      }
    };

    return testing::Matcher<Res>(new Impl());
  }
};

// Returns a matcher that waits on a Future (by default for 16 seconds)
// then applies a matcher to the result.
template <typename ResultMatcher>
FutureMatcher<ResultMatcher> Finishes(
    const ResultMatcher& result_matcher,
    double wait_seconds = kDefaultAssertFinishesWaitSeconds) {
  return FutureMatcher<ResultMatcher>(result_matcher, wait_seconds);
}

// Returns a matcher that matches the value of a successful Result<T>.
template <typename ValueMatcher>
ResultMatcher<ValueMatcher> ResultWith(const ValueMatcher& value_matcher) {
  return ResultMatcher<ValueMatcher>(value_matcher);
}

// Returns a matcher that matches an ok Status or Result<T>.
inline OkMatcher Ok() { return {}; }

// Returns a matcher that matches the StatusCode of a Status or Result<T>.
// Do not use Raises(StatusCode::OK) to match a non error code.
inline ErrorMatcher Raises(StatusCode code) { return ErrorMatcher(code, util::nullopt); }

// Returns a matcher that matches the StatusCode and message of a Status or Result<T>.
template <typename MessageMatcher>
ErrorMatcher Raises(StatusCode code, const MessageMatcher& message_matcher) {
  return ErrorMatcher(code, testing::MatcherCast<std::string>(message_matcher));
}

class DataEqMatcher {
 public:
  explicit DataEqMatcher(Datum expected) : expected_(std::move(expected)) {}

  template <typename Data>
  operator testing::Matcher<Data>() const {  // NOLINT runtime/explicit
    struct Impl : testing::MatcherInterface<const Data&> {
      explicit Impl(Datum expected) : expected_(std::move(expected)) {}

      void DescribeTo(::std::ostream* os) const override {
        *os << "has data ";
        PrintTo(expected_, os);
      }

      void DescribeNegationTo(::std::ostream* os) const override {
        *os << "doesn't have data ";
        PrintTo(expected_, os);
      }

      bool MatchAndExplain(const Data& data,
                           testing::MatchResultListener* listener) const override {
        Datum boxed(data);

        if (boxed.kind() != expected_.kind()) {
          *listener << "whose Datum::kind " << boxed.ToString() << " doesn't match "
                    << expected_.ToString();
          return false;
        }

        if (*boxed.type() != *expected_.type()) {
          *listener << "whose DataType " << boxed.type()->ToString() << " doesn't match "
                    << expected_.type()->ToString();
          return false;
        }

        const bool match = boxed == expected_;
        *listener << "whose value ";
        PrintTo(boxed, listener->stream());
        *listener << (match ? " matches" : " doesn't match");
        return match;
      }

      Datum expected_;
    };

    return testing::Matcher<Data>(new Impl(expected_));
  }

 private:
  Datum expected_;
};

template <typename Data>
DataEqMatcher DataEq(Data&& dat) {
  return DataEqMatcher(Datum(std::forward<Data>(dat)));
}

}  // namespace arrow
