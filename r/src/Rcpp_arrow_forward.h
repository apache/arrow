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

#include "./symbols.h"

#define STOP_IF_NOT(TEST, MSG)    \
do {                              \
  if (!(TEST)) Rcpp::stop(MSG);   \
} while (0)

#define STOP_IF_NOT_OK(s) STOP_IF_NOT(s.ok(), s.ToString())

template <typename T>
inline void STOP_IF_NULL(T* ptr) {
  STOP_IF_NOT(ptr, "invalid data");
}

template <typename T>
struct NoDelete {
  inline void operator()(T* ptr) {}
};

namespace Rcpp {
namespace internal {

template <typename Pointer>
Pointer r6_to_smart_pointer(SEXP self) {
  return reinterpret_cast<Pointer>(
    EXTPTR_PTR(Rf_findVarInFrame(self, arrow::r::symbols::xp)));
}

}  // namespace internal

template <typename T>
class ConstReferenceSmartPtrInputParameter {
public:
  using const_reference = const T&;

  explicit ConstReferenceSmartPtrInputParameter(SEXP self)
    : ptr(internal::r6_to_smart_pointer<const T*>(self)) {}

  inline operator const_reference() { return *ptr; }

private:
  const T* ptr;
};

namespace traits {

template <typename T>
struct input_parameter<const std::shared_ptr<T>&> {
  typedef typename Rcpp::ConstReferenceSmartPtrInputParameter<std::shared_ptr<T>> type;
};

template <typename T>
struct input_parameter<const std::unique_ptr<T>&> {
  typedef typename Rcpp::ConstReferenceSmartPtrInputParameter<std::unique_ptr<T>> type;
};

struct wrap_type_shared_ptr_tag {};
struct wrap_type_unique_ptr_tag {};

template <typename T>
struct wrap_type_traits<std::shared_ptr<T>> {
  using wrap_category = wrap_type_shared_ptr_tag;
};

template <typename T>
struct wrap_type_traits<std::unique_ptr<T>> {
  using wrap_category = wrap_type_unique_ptr_tag;
};

}  // namespace traits

namespace internal {

template <typename T>
inline SEXP wrap_dispatch(const T& x, Rcpp::traits::wrap_type_shared_ptr_tag);

template <typename T>
inline SEXP wrap_dispatch(const T& x, Rcpp::traits::wrap_type_unique_ptr_tag);

}  // namespace internal
}  // namespace Rcpp
