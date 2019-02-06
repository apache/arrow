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

// Data structure for Flight RPC. API should be considered experimental for now

#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "arrow/util/visibility.h"

namespace arrow {

class Buffer;
class Schema;
class Status;

namespace flight {

/// \brief A type of action that can be performed with the DoAction RPC
struct ActionType {
  /// Name of action
  std::string type;

  /// Opaque action description
  std::string description;
};

/// \brief Opaque selection critera for ListFlights RPC
struct Criteria {
  /// Opaque criteria expression, dependent on server implementation
  std::string expression;
};

/// \brief An action to perform with the DoAction RPC
struct Action {
  /// The action type
  std::string type;

  /// The action content as a Buffer
  std::shared_ptr<Buffer> body;
};

/// \brief Opaque result returned after executing an action
struct Result {
  std::shared_ptr<Buffer> body;
};

/// \brief A message received after completing a DoPut stream
struct PutResult {};

/// \brief A request to retrieve or generate a dataset
struct FlightDescriptor {
  enum DescriptorType {
    UNKNOWN = 0,  /// Unused
    PATH = 1,     /// Named path identifying a dataset
    CMD = 2       /// Opaque command to generate a dataset
  };

  /// The descriptor type
  DescriptorType type;

  /// Opaque value used to express a command. Should only be defined when type
  /// is CMD
  std::string cmd;

  /// List of strings identifying a particular dataset. Should only be defined
  /// when type is PATH
  std::vector<std::string> path;
};

/// \brief Data structure providing an opaque identifier or credential to use
/// when requesting a data stream with the DoGet RPC
struct Ticket {
  std::string ticket;
};

/// \brief A host location (hostname and port)
struct Location {
  std::string host;
  int32_t port;
};

/// \brief A flight ticket and list of locations where the ticket can be
/// redeemed
struct FlightEndpoint {
  /// Opaque ticket identify; use with DoGet RPC
  Ticket ticket;

  /// List of locations where ticket can be redeemed. If the list is empty, the
  /// ticket can only be redeemed on the current service where the ticket was
  /// generated
  std::vector<Location> locations;
};

/// \brief The access coordinates for retireval of a dataset, returned by
/// GetFlightInfo
class FlightInfo {
 public:
  struct Data {
    std::string schema;
    FlightDescriptor descriptor;
    std::vector<FlightEndpoint> endpoints;
    uint64_t total_records;
    uint64_t total_bytes;
  };

  explicit FlightInfo(const Data& data) : data_(data), reconstructed_schema_(false) {}
  explicit FlightInfo(Data&& data)
      : data_(std::move(data)), reconstructed_schema_(false) {}

  /// Deserialize the Arrow schema of the dataset, to be passed to each call to
  /// DoGet
  Status GetSchema(std::shared_ptr<Schema>* out) const;

  const std::string& serialized_schema() const { return data_.schema; }

  /// The descriptor associated with this flight, may not be set
  const FlightDescriptor& descriptor() const { return data_.descriptor; }

  /// A list of endpoints associated with the flight (dataset). To consume the
  /// whole flight, all endpoints must be consumed
  const std::vector<FlightEndpoint>& endpoints() const { return data_.endpoints; }

  /// The total number of records (rows) in the dataset. If unknown, set to -1
  uint64_t total_records() const { return data_.total_records; }

  /// The total number of bytes in the dataset. If unknown, set to -1
  uint64_t total_bytes() const { return data_.total_bytes; }

 private:
  Data data_;
  mutable std::shared_ptr<Schema> schema_;
  mutable bool reconstructed_schema_;
};

/// \brief An iterator to FlightInfo instances returned by ListFlights
class ARROW_EXPORT FlightListing {
 public:
  virtual ~FlightListing() = default;

  /// \brief Retrieve the next FlightInfo from the iterator. Returns nullptr
  /// when there are none left
  /// \param[out] info a single FlightInfo
  /// \return Status
  virtual Status Next(std::unique_ptr<FlightInfo>* info) = 0;
};

/// \brief An iterator to Result instances returned by DoAction
class ARROW_EXPORT ResultStream {
 public:
  virtual ~ResultStream() = default;

  /// \brief Retrieve the next Result from the iterator. Returns nullptr
  /// when there are none left
  /// \param[out] info a single Result
  /// \return Status
  virtual Status Next(std::unique_ptr<Result>* info) = 0;
};

// \brief Create a FlightListing from a vector of FlightInfo objects. This can
// be iterated once, then it is consumed
class ARROW_EXPORT SimpleFlightListing : public FlightListing {
 public:
  explicit SimpleFlightListing(const std::vector<FlightInfo>& flights);
  explicit SimpleFlightListing(std::vector<FlightInfo>&& flights);

  Status Next(std::unique_ptr<FlightInfo>* info) override;

 private:
  int position_;
  std::vector<FlightInfo> flights_;
};

class ARROW_EXPORT SimpleResultStream : public ResultStream {
 public:
  explicit SimpleResultStream(std::vector<Result>&& results);
  Status Next(std::unique_ptr<Result>* result) override;

 private:
  std::vector<Result> results_;
  size_t position_;
};

}  // namespace flight
}  // namespace arrow
