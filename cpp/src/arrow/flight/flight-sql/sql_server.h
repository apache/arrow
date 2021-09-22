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

// Interfaces to use for defining Flight RPC servers. API should be considered
// experimental for now

#pragma once

#include <arrow/flight/flight-sql/FlightSql.pb.h>
#include <google/protobuf/any.pb.h>

#include "../server.h"

namespace pb = arrow::flight::protocol;

namespace arrow {
namespace flight {
namespace sql {

class FlightSqlServerBase : public FlightServerBase {
 public:
  FlightSqlServerBase(){};

  ~FlightSqlServerBase() = default;

  Status GetFlightInfo(const ServerCallContext& context, const FlightDescriptor& request,
                       std::unique_ptr<FlightInfo>* info) override {
    google::protobuf::Any any;
    any.ParseFromArray(request.cmd.data(), static_cast<int>(request.cmd.size()));

    if (any.Is<pb::sql::CommandStatementQuery>()) {
      pb::sql::CommandStatementQuery command;
      any.UnpackTo(&command);

      GetFlightInfoStatement(command, context, request, info);
    } else if (any.Is<pb::sql::CommandStatementUpdate>()) {
      std::cout << "Update" << std::endl;
    }

    return Status::OK();
  };

  Status DoGet(const ServerCallContext& context, const Ticket& request,
               std::unique_ptr<FlightDataStream>* stream) override {
    google::protobuf::Any anyCommand;

    anyCommand.ParseFromArray(request.ticket.data(),
                              static_cast<int>(request.ticket.size()));

    if (anyCommand.Is<pb::sql::TicketStatementQuery>()) {
      pb::sql::TicketStatementQuery command;
      anyCommand.UnpackTo(&command);

      DoGetStatement(command, context, request, stream);
    }

    return Status::OK();
  }

  /// \brief Gets information about a particular SQL query based data stream.
  /// \param[in] command      The sql command to generate the data stream.
  /// \param[in] context      Per-call context.
  /// \param[in] descriptor   The descriptor identifying the data stream.
  /// \param[out] info        The FlightInfo describing where to access the
  ///                         dataset.
  /// \return                 Status.
  virtual Status GetFlightInfoStatement(const pb::sql::CommandStatementQuery& command,
                                        const ServerCallContext& context,
                                        const FlightDescriptor& descriptor,
                                        std::unique_ptr<FlightInfo>* info){};

  /// \brief Returns data for a SQL query based data stream.
  /// \param[in] command      The sql command to generate the data stream.
  /// \param[in] context      Per-call context.
  /// \param[in] descriptor   The descriptor identifying the data stream.
  /// \param[out]result           The Result iterator.
  /// \return                 Status.
  virtual Status DoGetStatement(const pb::sql::TicketStatementQuery& command,
                                const ServerCallContext& context, const Ticket& ticket,
                                std::unique_ptr<FlightDataStream>* result){};

  /// \brief Gets information about a particular prepared statement data stream.
  /// \param[in] command      The sql command to generate the data stream.
  /// \param[in] context      Per-call context.
  /// \param[in] descriptor   The descriptor identifying the data stream.
  /// \param[out] info        The FlightInfo describing where to access the
  ///                         dataset.
  /// \return                 Status.
  virtual Status GetFlightInfoPreparedStatement(
      const pb::sql::CommandPreparedStatementQuery& command,
      const ServerCallContext& context, const FlightDescriptor& descriptor,
      std::unique_ptr<FlightInfo>* info){};

  virtual Status DoGetPreparedStatement(
      const pb::sql::CommandPreparedStatementQuery& command, const Ticket& ticket,
      const ServerCallContext& context, std::unique_ptr<ResultStream>* result){};

  /// \brief Returns a FlightInfo object which contains the data to access the
  ///        stream of data.
  /// \param[in] command      The sql command to generate the data stream.
  /// \param[in] context      Per-call context.
  /// \param[in] descriptor   The descriptor identifying the data stream.
  /// \param[out] info        The FlightInfo describing where to access the
  ///                         dataset.
  /// \return                 Status.
  virtual Status GetFlightInfoCatalogs(const pb::sql::CommandGetCatalogs& command,
                                       const ServerCallContext& context,
                                       const FlightDescriptor& descriptor,
                                       std::unique_ptr<FlightInfo>* info){};

  /// \brief Returns data for catalogs based on a data stream.
  /// \param[in] ticket   The application-defined ticket identifying this stream.
  /// \param[in] context  Per-call context.
  /// \param[out] result  An interface for sending data back to the client.
  /// \return             Status.
  virtual Status DoGetCatalogs(const Ticket& ticket, const ServerCallContext& context,
                               std::unique_ptr<ResultStream>* result){};

  /// \brief Returns a FlightInfo object which contains the data to access the
  ///        stream of data.
  /// \param[in] command      The sql command to generate the data stream.
  /// \param[in] context      Per-call context.
  /// \param[in] descriptor   The descriptor identifying the data stream.
  /// \param[out] info        The FlightInfo describing where to access the
  ///                         dataset.
  /// \return                 Status.
  virtual Status GetFlightInfoSqlInfo(const pb::sql::CommandStatementQuery& command,
                                      const ServerCallContext& context,
                                      const FlightDescriptor& descriptor,
                                      std::unique_ptr<FlightInfo>* info){};

  /// \brief Returns data for sqlInfo based on a data stream.
  /// \param[in]command  The sql command to generate the data stream.
  /// \param[in]ticket   The application-defined ticket identifying this stream.
  /// \param[in]context  Per-call context.
  /// \param[out]result   The Result iterator.
  /// \return         Status.
  virtual Status DoGetSqlInfo(const pb::sql::CommandStatementQuery& command,
                              const Ticket& ticket, const ServerCallContext& context,
                              std::unique_ptr<ResultStream>* result){};

  /// \brief Returns a FlightInfo object which contains the data to access the
  ///        stream of data.
  /// \param[in] command      The sql command to generate the data stream.
  /// \param[in] context      Per-call context.
  /// \param[in] descriptor   The descriptor identifying the data stream.
  /// \param[out] info        The FlightInfo describing where to access the
  ///                         dataset.
  /// \return                 Status.
  virtual Status GetFlightInfoSchemas(const pb::sql::CommandGetSchemas& command,
                                      const ServerCallContext& context,
                                      const FlightDescriptor& descriptor,
                                      std::unique_ptr<FlightInfo>* info){};

  /// \brief Returns data for schemas based on a data stream.
  /// \param[in]command  The sql command to generate the data stream.
  /// \param[in]ticket   The application-defined ticket identifying this stream.
  /// \param[in]context  Per-call context.
  /// \param[out]result   The Result iterator.
  /// \return         Status.
  virtual Status DoGetSchemas(const pb::sql::CommandGetSchemas& command,
                              const Ticket& ticket, const ServerCallContext& context,
                              std::unique_ptr<ResultStream>* result){};

  ///\brief Returns a FlightInfo object which contains the data to access the
  ///        stream of data.
  /// \param[in] command      The sql command to generate the data stream.
  /// \param[in] context      Per-call context.
  /// \param[in] descriptor   The descriptor identifying the data stream.
  /// \param[out] info        The FlightInfo describing where to access the
  ///                         dataset.
  /// \return                 Status.
  virtual Status GetFlightInfoTables(const pb::sql::CommandGetTables& command,
                                     const ServerCallContext& context,
                                     const FlightDescriptor& descriptor,
                                     std::unique_ptr<FlightInfo>* info){};

  /// \brief Returns data for tables based on a data stream.
  /// \param[in]ticket   The application-defined ticket identifying this stream.
  /// \param[in]context  Per-call context.
  /// \param[out]result   The Result iterator.
  /// \return         Status.
  virtual Status DoGetTables(const pb::sql::CommandGetTables, const Ticket& ticket,
                             const ServerCallContext& context,
                             std::unique_ptr<ResultStream>* result){};

  /// \brief Returns a FlightInfo object which contains the data to access the
  ///        stream of data.
  /// \param[in] command      The sql command to generate the data stream.
  /// \param[in] context      Per-call context.
  /// \param[in] descriptor   The descriptor identifying the data stream.
  /// \param[out] info        The FlightInfo describing where to access the
  ///                         dataset.
  /// \return                 Status.
  virtual Status GetFlightInfoTableTypes(const pb::sql::CommandGetTableTypes& command,
                                         const ServerCallContext& context,
                                         const FlightDescriptor& descriptor,
                                         std::unique_ptr<FlightInfo>* info){};

  /// \brief Returns data for table types based on a data stream.
  /// \param[in]ticket   The application-defined ticket identifying this stream.
  /// \param[in]context  Per-call context.
  /// \param[out]result   The Result iterator.
  /// \return         Status.
  virtual Status DoGetTablesTypes(const Ticket& ticket, const ServerCallContext& context,
                                  std::unique_ptr<ResultStream>* result){};

  /// \brief Returns a FlightInfo object which contains the data to access the
  ///        stream of data.
  /// \param[in] command      The sql command to generate the data stream.
  /// \param[in] context      Per-call context.
  /// \param[in] descriptor   The descriptor identifying the data stream.
  /// \param[out] info        The FlightInfo describing where to access the
  ///                         dataset.
  /// \return                 Status.
  virtual Status GetFlightInfoPrimaryKeys(const pb::sql::CommandGetPrimaryKeys& command,
                                          const ServerCallContext& context,
                                          const FlightDescriptor& descriptor,
                                          std::unique_ptr<FlightInfo>* info){};

  /// \brief Returns data for primary keys based on a data stream.
  /// \param[in]command  The sql command to generate the data stream.
  /// \param[in]ticket   The application-defined ticket identifying this stream.
  /// \param[in]context  Per-call context.
  /// \param[out]result   The Result iterator.
  /// \return         Status.
  virtual Status DoGetPrimaryKeys(const pb::sql::CommandGetPrimaryKeys& command,
                                  const Ticket& ticket, const ServerCallContext& context,
                                  std::unique_ptr<ResultStream>* result){};

  /// \brief Returns a FlightInfo object which contains the data to access the
  ///        stream of data.
  /// \param[in] command      The sql command to generate the data stream.
  /// \param[in] context      Per-call context.
  /// \param[in] descriptor   The descriptor identifying the data stream.
  /// \param[out] info        The FlightInfo describing where to access the
  ///                         dataset.
  /// \return                 Status.
  virtual Status GetFlightInfoExportedKeys(const pb::sql::CommandGetExportedKeys& command,
                                           const ServerCallContext& context,
                                           const FlightDescriptor& descriptor,
                                           std::unique_ptr<FlightInfo>* info){};

  /// \brief Returns data for exported keys based on a data stream.
  /// \param[in]command  The sql command to generate the data stream.
  /// \param[in]ticket   The application-defined ticket identifying this stream.
  /// \param[in]context  Per-call context.
  /// \param[out]result   The Result iterator.
  /// \return         Status.
  virtual Status DoGetExportedKeys(const pb::sql::CommandGetExportedKeys& command,
                                   const Ticket& ticket, const ServerCallContext& context,
                                   std::unique_ptr<ResultStream>* result){};

  /// \brief Returns a FlightInfo object which contains the data to access the
  ///        stream of data.
  /// \param[in] command      The sql command to generate the data stream.
  /// \param[in] context      Per-call context.
  /// \param[in] descriptor   The descriptor identifying the data stream.
  /// \param[out] info        The FlightInfo describing where to access the
  ///                         dataset.
  /// \return                 Status.
  virtual Status GetFlightInfoImportedKeys(const pb::sql::CommandGetImportedKeys& command,
                                           const ServerCallContext& context,
                                           const FlightDescriptor& descriptor,
                                           std::unique_ptr<FlightInfo>* info){};

  /// \brief Returns data for imported keys based on a data stream.
  /// \param[in]command  The sql command to generate the data stream.
  /// \param[in]ticket   The application-defined ticket identifying this stream.
  /// \param[in]context  Per-call context.
  /// \param[out]result   The Result iterator.
  /// \return         Status.
  virtual Status DoGetImportedKeys(const pb::sql::CommandGetImportedKeys& command,
                                   const Ticket& ticket, const ServerCallContext& context,
                                   std::unique_ptr<ResultStream>* result){};

  /// \brief Creates a prepared statement on the server and returns a handle and metadata
  /// for in a
  ///        ActionCreatePreparedStatementResult object.
  /// \param[in]command  The sql command to generate the data stream.
  /// \param[in]context  Per-call context.
  /// \param[out]result   The Result iterator.
  /// \return         Status.
  virtual Status CreatePreparedStatement(
      const pb::sql::ActionCreatePreparedStatementRequest& command,
      const ServerCallContext& context, std::unique_ptr<ResultStream>* result){};

  /// \brief Closes a prepared statement on the server. No result is expected.
  /// \param[in]command  The sql command to generate the data stream.
  /// \param[in]context  Per-call context.
  /// \param[out]result   The Result iterator.
  /// \return         Status.
  virtual Status ClosePreparedStatement(
      const pb::sql::ActionClosePreparedStatementRequest& command,
      const ServerCallContext& context, std::unique_ptr<ResultStream>* result){};
};
}  // namespace sql
}  // namespace flight
}  // namespace arrow
