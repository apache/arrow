/*
 * Copyright 2018 Kenta Murata <mrkn@mrkn.jp>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef RUBY_HPP
#define RUBY_HPP 1

#include <ruby.h>
#include <string>
#include <functional>

namespace rb {

class error {
 public:
  explicit error(VALUE exc) : exc_(exc) {}

  error(VALUE exc_klass, const char* message) {
    exc_ = rb_exc_new_cstr(exc_klass, message);
  }

  error(VALUE exc_klass, const std::string& message)
      : error(exc_klass, message.c_str()) {}

  VALUE exception_object() const { return exc_; }

 private:
  VALUE exc_;
};

namespace internal {

VALUE protect_function_call(VALUE arg);

}  // namespace internal

inline VALUE protect(std::function<VALUE()> func) {
  VALUE arg = reinterpret_cast<VALUE>(&func);
  VALUE result = ::rb_protect(internal::protect_function_call, arg, 0);
  VALUE exc = rb_errinfo();
  if (!NIL_P(exc)) {
    throw error(exc);
  }
  return result;
}

}  // namespace rb

#endif /* RUBY_HPP */
