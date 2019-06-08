//
// Copyright 2017 Asylo authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

// Adapted from Asylo

#pragma once

#include <utility>

#include "arrow/status.h"
#include "arrow/util/macros.h"

namespace arrow {

struct ErrorOrConstants {
  static const char kValueMoveConstructorMsg[];
  static const char kValueMoveAssignmentMsg[];
  static const char kValueOrDieMovedMsg[];
  static const char kStatusMoveConstructorMsg[];
  static const char kStatusMoveAssignmentMsg[];
};

// A class for representing either a usable value, or an error.
///
/// A ErrorOr object either contains a value of type `T` or a Status object
/// explaining why such a value is not present. The type `T` must be
/// copy-constructible and/or move-constructible.
///
/// The state of a ErrorOr object may be determined by calling ok() or
/// status(). The ok() method returns true if the object contains a valid value.
/// The status() method returns the internal Status object. A ErrorOr object
/// that contains a valid value will return an OK Status for a call to status().
///
/// A value of type `T` may be extracted from a ErrorOr object through a call
/// to ValueOrDie(). This function should only be called if a call to ok()
/// returns true. Sample usage:
///
/// ```
///   arrow::ErrorOr<Foo> result = CalculateFoo();
///   if (result.ok()) {
///     Foo foo = result.ValueOrDie();
///     foo->DoSomethingCool();
///   } else {
///     ARROW_LOG(ERROR) << result.status();
///  }
/// ```
///
/// If `T` is a move-only type, like `std::unique_ptr<>`, then the value should
/// only be extracted after invoking `std::move()` on the ErrorOr object.
/// Sample usage:
///
/// ```
///   arrow::ErrorOr<std::unique_ptr<Foo>> result = CalculateFoo();
///   if (result.ok()) {
///     std::unique_ptr<Foo> foo = std::move(result).ValueOrDie();
///     foo->DoSomethingCool();
///   } else {
///     ARROW_LOG(ERROR) << result.status();
///   }
/// ```
///
/// ErrorOr is provided for the convenience of implementing functions that
/// return some value but may fail during execution. For instance, consider a
/// function with the following signature:
///
/// ```
///   arrow::Status CalculateFoo(int *output);
/// ```
///
/// This function may instead be written as:
///
/// ```
///   arrow::ErrorOr<int> CalculateFoo();
/// ```
template <class T>
class ErrorOr {
  template <typename U>
  friend class ErrorOr;

 public:
  /// Constructs a ErrorOr object that contains a non-OK status.
  /// The non-OK status has an error code of -1. This is a non-standard POSIX
  /// error code and is used in this context to indicate an unknown error.
  ///
  /// This constructor is marked `explicit` to prevent attempts to `return {}`
  /// from a function with a return type of, for example,
  /// `ErrorOr<std::vector<int>>`. While `return {}` seems like it would return
  /// an empty vector, it will actually invoke the default constructor of
  /// ErrorOr.
  explicit ErrorOr()  // NOLINT(runtime/explicit)
      : variant_(Status::UnknownError("Unknown error")), has_value_(false) {}

  ~ErrorOr() {
    if (has_value_) {
      variant_.value_.~T();
    } else {
      variant_.status_.~Status();
    }
  }

  /// Constructs a ErrorOr object with the given non-OK Status object. All
  /// calls to ValueOrDie() on this object will abort. The given `status` must
  /// not be an OK status, otherwise this constructor will abort.
  ///
  /// This constructor is not declared explicit so that a function with a return
  /// type of `ErrorOr<T>` can return a Status object, and the status will be
  /// implicitly converted to the appropriate return type as a matter of
  /// convenience.
  ///
  /// \param status The non-OK Status object to initalize to.
  ErrorOr(const Status& status)  // NOLINT(runtime/explicit)
      : variant_(status), has_value_(false) {
    if (status.ok()) {
      std::abort();
    }
  }

  /// Constructs a ErrorOr object that contains `value`. The resulting object
  /// is considered to have an OK status. The wrapped element can be accessed
  /// with ValueOrDie().
  ///
  /// This constructor is made implicit so that a function with a return type of
  /// `ErrorOr<T>` can return an object of type `U &&`, implicitly converting
  /// it to a `ErrorOr<T>` object.
  ///
  /// Note that `T` must be implicitly constructible from `U`, and `U` must not
  /// be a (cv-qualified) Status or Status-reference type. Due to C++
  /// reference-collapsing rules and perfect-forwarding semantics, this
  /// constructor matches invocations that pass `value` either as a const
  /// reference or as an rvalue reference. Since ErrorOr needs to work for both
  /// reference and rvalue-reference types, the constructor uses perfect
  /// forwarding to avoid invalidating arguments that were passed by reference.
  /// See http://thbecker.net/articles/rvalue_references/section_08.html for
  /// additional details.
  ///
  /// \param value The value to initialize to.
  template <typename U,
            typename E = typename std::enable_if<
                std::is_constructible<T, U>::value && std::is_convertible<U, T>::value &&
                !std::is_same<typename std::remove_reference<
                                  typename std::remove_cv<U>::type>::type,
                              Status>::value>::type>
  ErrorOr(U&& value)  // NOLINT(runtime/explicit)
      : variant_(std::forward<U>(value)), has_value_(true) {}

  /// Copy constructor.
  ///
  /// This constructor needs to be explicitly defined because the presence of
  /// the move-assignment operator deletes the default copy constructor. In such
  /// a scenario, since the deleted copy constructor has stricter binding rules
  /// than the templated copy constructor, the templated constructor cannot act
  /// as a copy constructor, and any attempt to copy-construct a `ErrorOr`
  /// object results in a compilation error.
  ///
  /// \param other The value to copy from.
  ErrorOr(const ErrorOr& other)
      : has_value_(other.has_value_) {  // NOLINT(runtime/explicit)
    if (has_value_) {
      new (&variant_) variant(other.variant_.value_);
    } else {
      new (&variant_) variant(other.variant_.status_);
    }
  }

  /// Templatized constructor that constructs a `ErrorOr<T>` from a const
  /// reference to a `ErrorOr<U>`.
  ///
  /// `T` must be implicitly constructible from `const U &`.
  ///
  /// \param other The value to copy from.
  template <typename U,
            typename E = typename std::enable_if<std::is_constructible<T, U>::value &&
                                                 std::is_convertible<U, T>::value>::type>
  ErrorOr(const ErrorOr<U>& other) : has_value_(other.has_value_) {
    if (has_value_) {
      new (&variant_) variant(other.variant_.value_);
    } else {
      new (&variant_) variant(other.variant_.status_);
    }
  }

  /// Copy-assignment operator.
  ///
  /// \param other The ErrorOr object to copy.
  ErrorOr& operator=(const ErrorOr& other) {
    // Check for self-assignment.
    if (this == &other) {
      return *this;
    }

    // Construct the variant object using the variant object of the source.
    if (other.has_value_) {
      AssignValue(other.variant_.value_);
    } else {
      AssignStatus(other.variant_.status_);
    }
    return *this;
  }

  /// Templatized constructor which constructs a `ErrorOr<T>` by moving the
  /// contents of a `ErrorOr<U>`. `T` must be implicitly constructible from `U
  /// &&`.
  ///
  /// Sets `other` to contain a non-OK status with a`StatusError::Invalid`
  /// error code.
  ///
  /// \param other The ErrorOr object to move from and set to a non-OK status.
  template <typename U,
            typename E = typename std::enable_if<std::is_constructible<T, U>::value &&
                                                 std::is_convertible<U, T>::value>::type>
  ErrorOr(ErrorOr<U>&& other) : has_value_(other.has_value_) {
    if (has_value_) {
      new (&variant_) variant(std::move(other.variant_.value_));
      other.OverwriteValueWithStatus(
          Status::Invalid(ErrorOrConstants::kValueMoveConstructorMsg));
    } else {
      new (&variant_) variant(std::move(other.variant_.status_));
#ifndef NDEBUG
      // The other.variant_.status_ gets moved and invalidated with a Status-
      // specific error message above. To aid debugging, set the status to a
      // ErrorOr-specific error message.
      other.variant_.status_ =
          Status::Invalid(ErrorOrConstants::kStatusMoveConstructorMsg);
#endif
    }
  }

  /// Move-assignment operator.
  ///
  /// Sets `other` to contain a non-OK status.
  ///
  /// \param other The ErrorOr object to assign from and set to a non-OK
  /// status.
  ErrorOr& operator=(ErrorOr&& other) {
    // Check for self-assignment.
    if (this == &other) {
      return *this;
    }

    // Construct the variant object using the variant object of the donor.
    if (other.has_value_) {
      AssignValue(std::move(other.variant_.value_));
      other.OverwriteValueWithStatus(
          Status::Invalid(ErrorOrConstants::kValueMoveAssignmentMsg));
    } else {
      AssignStatus(std::move(other.variant_.status_));
#ifndef NDEBUG
      // The other.variant_.status_ gets moved and invalidated with a Status-
      // specific error message above. To aid debugging, set the status to a
      // ErrorOr-specific error message.
      other.variant_.status_ =
          Status::Invalid(ErrorOrConstants::kStatusMoveAssignmentMsg);
#endif
    }

    return *this;
  }

  /// Indicates whether the object contains a `T` value.
  ///
  /// \return True if this ErrorOr object's status is OK (i.e. a call to ok()
  /// returns true). If this function returns true, then it is safe to access
  /// the wrapped element through a call to ValueOrDie().
  bool ok() const { return has_value_; }

  /// Gets the stored status object, or an OK status if a `T` value is stored.
  ///
  /// \return The stored non-OK status object, or an OK status if this object
  ///         has a value.
  Status status() const { return ok() ? Status::OK() : variant_.status_; }

  /// Gets the stored `T` value.
  ///
  /// This method should only be called if this ErrorOr object's status is OK
  /// (i.e. a call to ok() returns true), otherwise this call will abort.
  ///
  /// \return The stored `T` value.
  const T& ValueOrDie() const& {
    if (!ok()) {
      std::abort();
    }
    return variant_.value_;
  }

  /// Gets a mutable reference to the stored `T` value.
  ///
  /// This method should only be called if this ErrorOr object's status is OK
  /// (i.e. a call to ok() returns true), otherwise this call will abort.
  ///
  /// \return The stored `T` value.
  T& ValueOrDie() & {
    if (!ok()) {
      std::abort();
    }
    return variant_.value_;
  }

  /// Moves and returns the internally-stored `T` value.
  ///
  /// This method should only be called if this ErrorOr object's status is OK
  /// (i.e. a call to ok() returns true), otherwise this call will abort. The
  /// ErrorOr object is invalidated after this call and will be updated to
  /// contain a non-OK status.
  ///
  /// \return The stored `T` value.
  T ValueOrDie() && {
    if (!ok()) {
      std::abort();
    }
    T tmp(std::move(variant_.value_));

    // Invalidate this ErrorOr object.
    OverwriteValueWithStatus(Status::Invalid(ErrorOrConstants::kValueOrDieMovedMsg));
    return std::move(tmp);
  }

 private:
  // Resets the |variant_| member to contain |status|.
  template <class U>
  void AssignStatus(U&& status) {
    if (ok()) {
      OverwriteValueWithStatus(std::forward<U>(status));
    } else {
      // Reuse the existing Status object. has_value_ is already false.
      variant_.status_ = std::forward<U>(status);
    }
  }

  // Under the assumption that |this| is currently holding a value, resets the
  // |variant_| member to contain |status| and sets |has_value_| to indicate
  // that |this| does not have a value. Destroys the existing |variant_| member.
  template <class U>
  void OverwriteValueWithStatus(U&& status) {
#ifndef NDEBUG
    if (!ok()) {
      std::abort();
    }
#endif
    variant_.value_.~T();
    new (&variant_) variant(std::forward<U>(status));
    has_value_ = false;
  }

  // Resets the |variant_| member to contain the |value| and sets |has_value_|
  // to indicate that the ErrorOr object has a value. Destroys the existing
  // |variant_| member.
  template <class U>
  void AssignValue(U&& value) {
    if (ok()) {
      // We cannot assume that T is move-assignable.
      variant_.value_.~T();
    } else {
      variant_.status_.~Status();
    }
    new (&variant_) variant(std::forward<U>(value));
    has_value_ = true;
  }

  // Use custom variant instead of c++ standard, to avoid header polution.
  union variant {
    // A non-OK status.
    Status status_;

    // An element of type T.
    T value_;

    variant() {}

    variant(const Status& status) : status_(status) {}

    variant(Status&& status) : status_(std::move(status)) {}

    template <typename U, typename E = typename std::enable_if<
                              std::is_constructible<T, U>::value &&
                              std::is_convertible<U, T>::value>::type>
    variant(U&& value) : value_(std::forward<U>(value)) {}

    // This destructor must be explicitly defined because it is deleted due to
    // the variant type having non-static data members with non-trivial
    // destructors.
    ~variant() {}
  };

  // One of: a non-OK status or an element of type T.
  variant variant_;

  // Indicates the active member of the variant_ member.
  //
  // A value of true indicates that value_ is the active member of variant_.
  //
  // A value of false indicates that status_ is the active member of variant_.
  bool has_value_;
};

#define ASSIGN_OR_RAISE_IMPL(status_name, lhs, rexpr) \
  auto status_name = (rexpr);                         \
  RETURN_NOT_OK(status_name.status());                \
  lhs = std::move(status_name.ValueOrDie());

#define ASSIGN_OR_RAISE_NAME(x, y) ARROW_CONCAT(x, y)

// Executes an expression that returns a ErrorOr, extracting its value
// into the variable defined by lhs (or returning on error).
//
// Example: Assigning to an existing value
//   ValueType value;
//   ASSIGN_OR_RAISE(value, MaybeGetValue(arg));
//
// WARNING: ASSIGN_OR_RAISE expands into multiple statements; it cannot be used
//  in a single statement (e.g. as the body of an if statement without {})!
#define ASSIGN_OR_RAISE(lhs, rexpr) \
  ASSIGN_OR_RAISE_IMPL(ASSIGN_OR_RAISE_NAME(_status_or_value, __COUNTER__), lhs, rexpr);

}  // namespace arrow
