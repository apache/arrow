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

#include <gmock/gmock-matchers.h>

#include "arrow/result.h"
#include "arrow/status.h"

namespace arrow {

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
        const ValueType& value = GetValue(maybe_value);
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
  template <typename T>
  static const T& GetValue(const Result<T>& maybe_value) {
    return maybe_value.ValueOrDie();
  }

  template <typename T>
  static const T& GetValue(const Future<T>& value_fut) {
    return GetValue(value_fut.result());
  }

  const ValueMatcher value_matcher_;
};

class StatusMatcher {
 public:
  explicit StatusMatcher(StatusCode code,
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
        const Status& status = GetStatus(maybe_value);
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
  static const Status& GetStatus(const Status& status) { return status; }

  template <typename T>
  static const Status& GetStatus(const Result<T>& maybe_value) {
    return maybe_value.status();
  }

  template <typename T>
  static const Status& GetStatus(const Future<T>& value_fut) {
    return value_fut.status();
  }

  const StatusCode code_;
  const util::optional<testing::Matcher<std::string>> message_matcher_;
};

// Returns a matcher that matches the value of a successful Result<T> or Future<T>.
// (Future<T> will be waited upon to acquire its result for matching.)
template <typename ValueMatcher>
ResultMatcher<ValueMatcher> ResultWith(const ValueMatcher& value_matcher) {
  return ResultMatcher<ValueMatcher>(value_matcher);
}

// Returns a matcher that matches the StatusCode of a Status, Result<T>, or Future<T>.
// (Future<T> will be waited upon to acquire its result for matching.)
inline StatusMatcher Raises(StatusCode code) {
  return StatusMatcher(code, util::nullopt);
}

// Returns a matcher that matches the StatusCode and message of a Status, Result<T>, or
// Future<T>. (Future<T> will be waited upon to acquire its result for matching.)
template <typename MessageMatcher>
StatusMatcher Raises(StatusCode code, const MessageMatcher& message_matcher) {
  return StatusMatcher(code, testing::MatcherCast<std::string>(message_matcher));
}

}  // namespace arrow
