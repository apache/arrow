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

// Interfaces for defining middleware for Flight clients and
// servers. Currently experimental.

#pragma once

#include <map>
#include <memory>
#include <string>
#include <utility>

#include "arrow/flight/visibility.h"  // IWYU pragma: keep
#include "arrow/status.h"
#include "arrow/util/string_view.h"

namespace arrow {

namespace flight {

struct ARROW_FLIGHT_EXPORT HeaderIterator {
 public:
  typedef std::pair<util::string_view, util::string_view> value_type;

  class Impl {
   public:
    virtual void Next() = 0;
    virtual const value_type& Dereference() const = 0;
    virtual bool Equals(const void* other) const = 0;
    virtual std::unique_ptr<Impl> Clone() const = 0;
    virtual ~Impl() = default;
  };

  /// \brief Nullary constructor to allow usage in Cython.
  /// Do NOT use this constructor yourself.
  HeaderIterator();
  explicit HeaderIterator(std::unique_ptr<Impl> impl);
  HeaderIterator(const HeaderIterator& copy);

  ~HeaderIterator() = default;

  const value_type& operator*() const;
  HeaderIterator& operator++();
  HeaderIterator operator++(int);
  bool operator==(const HeaderIterator& r) const;
  bool operator!=(const HeaderIterator& r) const;
  HeaderIterator& operator=(const HeaderIterator& other);

 private:
  std::unique_ptr<Impl> impl_;
};

/// \brief A read-only wrapper around headers for an RPC call.
class ARROW_FLIGHT_EXPORT CallHeaders {
 public:
  /// \brief The iterator type. Dereferences to a std::pair of string
  /// views.
  typedef HeaderIterator const_iterator;

  virtual ~CallHeaders() = default;

  /// \brief Get all the values for the given header.
  virtual std::pair<const_iterator, const_iterator> GetHeaders(
      const std::string& key) const = 0;
  /// \brief Count the number of values for the header (returns 0 if
  /// header does not exist).
  virtual std::size_t Count(const std::string& key) const = 0;
  /// \brief Const iterator for header-value pairs.
  virtual const_iterator cbegin() const noexcept = 0;
  /// \brief Const iterator for header-value pairs.
  virtual const_iterator cend() const noexcept = 0;
};

/// \brief A write-only wrapper around headers for an RPC call.
class ARROW_FLIGHT_EXPORT AddCallHeaders {
 public:
  virtual ~AddCallHeaders() = default;

  /// \brief Add a header to be sent to the client.
  virtual void AddHeader(const std::string& key, const std::string& value) = 0;
};

/// \brief An enumeration of the RPC methods Flight implements.
enum class FlightMethod : char {
  Invalid = 0,
  Handshake = 1,
  ListFlights = 2,
  GetFlightInfo = 3,
  GetSchema = 4,
  DoGet = 5,
  DoPut = 6,
  DoAction = 7,
  ListActions = 8,
};

/// \brief Information about an instance of a Flight RPC.
class ARROW_FLIGHT_EXPORT CallInfo {
 public:
  /// \brief The RPC method of this call.
  FlightMethod method;
};

}  // namespace flight

}  // namespace arrow
