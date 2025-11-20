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

#include <memory>

#include "arrow/flight/test_flight_server.h"

#include "arrow/array/array_base.h"
#include "arrow/array/array_primitive.h"
#include "arrow/array/builder_primitive.h"
#include "arrow/flight/server.h"
#include "arrow/flight/test_util.h"
#include "arrow/flight/type_fwd.h"
#include "arrow/status.h"

namespace arrow::flight {
namespace {

class ErrorRecordBatchReader : public RecordBatchReader {
 public:
  ErrorRecordBatchReader() : schema_(arrow::schema({})) {}

  std::shared_ptr<Schema> schema() const override { return schema_; }

  Status ReadNext(std::shared_ptr<RecordBatch>* out) override {
    *out = nullptr;
    return Status::OK();
  }

  Status Close() override {
    // This should be propagated over DoGet to the client
    return Status::IOError("Expected error");
  }

 private:
  std::shared_ptr<Schema> schema_;
};

Status GetBatchForFlight(const Ticket& ticket, std::shared_ptr<RecordBatchReader>* out) {
  if (ticket.ticket == "ticket-ints-1") {
    RecordBatchVector batches;
    RETURN_NOT_OK(ExampleIntBatches(&batches));
    ARROW_ASSIGN_OR_RAISE(*out, RecordBatchReader::Make(batches));
    return Status::OK();
  } else if (ticket.ticket == "ticket-floats-1") {
    RecordBatchVector batches;
    RETURN_NOT_OK(ExampleFloatBatches(&batches));
    ARROW_ASSIGN_OR_RAISE(*out, RecordBatchReader::Make(batches));
    return Status::OK();
  } else if (ticket.ticket == "ticket-dicts-1") {
    RecordBatchVector batches;
    RETURN_NOT_OK(ExampleDictBatches(&batches));
    ARROW_ASSIGN_OR_RAISE(*out, RecordBatchReader::Make(batches));
    return Status::OK();
  } else if (ticket.ticket == "ticket-large-batch-1") {
    RecordBatchVector batches;
    RETURN_NOT_OK(ExampleLargeBatches(&batches));
    ARROW_ASSIGN_OR_RAISE(*out, RecordBatchReader::Make(batches));
    return Status::OK();
  } else {
    return Status::NotImplemented("no stream implemented for ticket: " + ticket.ticket);
  }
}

}  // namespace

std::unique_ptr<FlightServerBase> TestFlightServer::Make() {
  return std::make_unique<TestFlightServer>();
}

Status TestFlightServer::ListFlights(const ServerCallContext& context,
                                     const Criteria* criteria,
                                     std::unique_ptr<FlightListing>* listings) {
  std::vector<FlightInfo> flights = ExampleFlightInfo();
  if (criteria && criteria->expression != "") {
    // For test purposes, if we get criteria, return no results
    flights.clear();
  }
  *listings = std::make_unique<SimpleFlightListing>(flights);
  return Status::OK();
}

Status TestFlightServer::GetFlightInfo(const ServerCallContext& context,
                                       const FlightDescriptor& request,
                                       std::unique_ptr<FlightInfo>* out) {
  // Test that Arrow-C++ status codes make it through the transport
  if (request.type == FlightDescriptor::DescriptorType::CMD &&
      request.cmd == "status-outofmemory") {
    return Status::OutOfMemory("Sentinel");
  }

  std::vector<FlightInfo> flights = ExampleFlightInfo();

  for (const auto& info : flights) {
    if (info.descriptor().Equals(request)) {
      *out = std::make_unique<FlightInfo>(info);
      return Status::OK();
    }
  }
  return Status::Invalid("Flight not found: ", request.ToString());
}

Status TestFlightServer::DoGet(const ServerCallContext& context, const Ticket& request,
                               std::unique_ptr<FlightDataStream>* data_stream) {
  // Test for ARROW-5095
  if (request.ticket == "ARROW-5095-fail") {
    return Status::UnknownError("Server-side error");
  }
  if (request.ticket == "ARROW-5095-success") {
    return Status::OK();
  }
  if (request.ticket == "ARROW-13253-DoGet-Batch") {
    // Make batch > 2GiB in size
    ARROW_ASSIGN_OR_RAISE(auto batch, VeryLargeBatch());
    ARROW_ASSIGN_OR_RAISE(auto reader, RecordBatchReader::Make({batch}));
    *data_stream = std::make_unique<RecordBatchStream>(std::move(reader));
    return Status::OK();
  }
  if (request.ticket == "ticket-stream-error") {
    auto reader = std::make_shared<ErrorRecordBatchReader>();
    *data_stream = std::make_unique<RecordBatchStream>(std::move(reader));
    return Status::OK();
  }

  std::shared_ptr<RecordBatchReader> batch_reader;
  RETURN_NOT_OK(GetBatchForFlight(request, &batch_reader));

  *data_stream = std::make_unique<RecordBatchStream>(batch_reader);
  return Status::OK();
}

Status TestFlightServer::DoPut(const ServerCallContext&,
                               std::unique_ptr<FlightMessageReader> reader,
                               std::unique_ptr<FlightMetadataWriter> writer) {
  return reader->ToRecordBatches().status();
}

Status TestFlightServer::DoExchange(const ServerCallContext& context,
                                    std::unique_ptr<FlightMessageReader> reader,
                                    std::unique_ptr<FlightMessageWriter> writer) {
  // Test various scenarios for a DoExchange
  if (reader->descriptor().type != FlightDescriptor::DescriptorType::CMD) {
    return Status::Invalid("Must provide a command descriptor");
  }

  const std::string& cmd = reader->descriptor().cmd;
  if (cmd == "error") {
    // Immediately return an error to the client.
    return Status::NotImplemented("Expected error");
  } else if (cmd == "get") {
    return RunExchangeGet(std::move(reader), std::move(writer));
  } else if (cmd == "put") {
    return RunExchangePut(std::move(reader), std::move(writer));
  } else if (cmd == "counter") {
    return RunExchangeCounter(std::move(reader), std::move(writer));
  } else if (cmd == "total") {
    return RunExchangeTotal(std::move(reader), std::move(writer));
  } else if (cmd == "echo") {
    return RunExchangeEcho(std::move(reader), std::move(writer));
  } else if (cmd == "large_batch") {
    return RunExchangeLargeBatch(std::move(reader), std::move(writer));
  } else if (cmd == "TestUndrained") {
    ARROW_ASSIGN_OR_RAISE(auto schema, reader->GetSchema());
    return Status::OK();
  } else {
    return Status::NotImplemented("Scenario not implemented: ", cmd);
  }
}

// A simple example - act like DoGet.
Status TestFlightServer::RunExchangeGet(std::unique_ptr<FlightMessageReader> reader,
                                        std::unique_ptr<FlightMessageWriter> writer) {
  RETURN_NOT_OK(writer->Begin(ExampleIntSchema()));
  RecordBatchVector batches;
  RETURN_NOT_OK(ExampleIntBatches(&batches));
  for (const auto& batch : batches) {
    RETURN_NOT_OK(writer->WriteRecordBatch(*batch));
  }
  return Status::OK();
}

// A simple example - act like DoPut
Status TestFlightServer::RunExchangePut(std::unique_ptr<FlightMessageReader> reader,
                                        std::unique_ptr<FlightMessageWriter> writer) {
  ARROW_ASSIGN_OR_RAISE(auto schema, reader->GetSchema());
  if (!schema->Equals(ExampleIntSchema(), false)) {
    return Status::Invalid("Schema is not as expected");
  }
  RecordBatchVector batches;
  RETURN_NOT_OK(ExampleIntBatches(&batches));
  FlightStreamChunk chunk;
  for (const auto& batch : batches) {
    ARROW_ASSIGN_OR_RAISE(chunk, reader->Next());
    if (!chunk.data) {
      return Status::Invalid("Expected another batch");
    }
    if (!batch->Equals(*chunk.data)) {
      return Status::Invalid("Batch does not match");
    }
  }
  ARROW_ASSIGN_OR_RAISE(chunk, reader->Next());
  if (chunk.data || chunk.app_metadata) {
    return Status::Invalid("Too many batches");
  }

  RETURN_NOT_OK(writer->WriteMetadata(Buffer::FromString("done")));
  return Status::OK();
}

// Read some number of record batches from the client, send a
// metadata message back with the count, then echo the batches back.
Status TestFlightServer::RunExchangeCounter(std::unique_ptr<FlightMessageReader> reader,
                                            std::unique_ptr<FlightMessageWriter> writer) {
  std::vector<std::shared_ptr<RecordBatch>> batches;
  FlightStreamChunk chunk;
  int chunks = 0;
  while (true) {
    ARROW_ASSIGN_OR_RAISE(chunk, reader->Next());
    if (!chunk.data && !chunk.app_metadata) {
      break;
    }
    if (chunk.data) {
      batches.push_back(chunk.data);
      chunks++;
    }
  }

  // Echo back the number of record batches read.
  std::shared_ptr<Buffer> buf = Buffer::FromString(std::to_string(chunks));
  RETURN_NOT_OK(writer->WriteMetadata(buf));
  // Echo the record batches themselves.
  if (chunks > 0) {
    ARROW_ASSIGN_OR_RAISE(auto schema, reader->GetSchema());
    RETURN_NOT_OK(writer->Begin(schema));

    for (const auto& batch : batches) {
      RETURN_NOT_OK(writer->WriteRecordBatch(*batch));
    }
  }

  return Status::OK();
}

// Read int64 batches from the client, each time sending back a
// batch with a running sum of columns.
Status TestFlightServer::RunExchangeTotal(std::unique_ptr<FlightMessageReader> reader,
                                          std::unique_ptr<FlightMessageWriter> writer) {
  FlightStreamChunk chunk{};
  ARROW_ASSIGN_OR_RAISE(auto schema, reader->GetSchema());
  // Ensure the schema contains only int64 columns
  for (const auto& field : schema->fields()) {
    if (field->type()->id() != Type::type::INT64) {
      return Status::Invalid("Field is not INT64: ", field->name());
    }
  }
  std::vector<int64_t> sums(schema->num_fields());
  std::vector<std::shared_ptr<Array>> columns(schema->num_fields());
  RETURN_NOT_OK(writer->Begin(schema));
  while (true) {
    ARROW_ASSIGN_OR_RAISE(chunk, reader->Next());
    if (!chunk.data && !chunk.app_metadata) {
      break;
    }
    if (chunk.data) {
      if (!chunk.data->schema()->Equals(schema, false)) {
        // A compliant client implementation would make this impossible
        return Status::Invalid("Schemas are incompatible");
      }

      // Update the running totals
      auto builder = std::make_shared<Int64Builder>();
      int col_index = 0;
      for (const auto& column : chunk.data->columns()) {
        auto arr = std::dynamic_pointer_cast<Int64Array>(column);
        if (!arr) {
          return MakeFlightError(FlightStatusCode::Internal, "Could not cast array");
        }
        for (int row = 0; row < column->length(); row++) {
          if (!arr->IsNull(row)) {
            sums[col_index] += arr->Value(row);
          }
        }

        builder->Reset();
        RETURN_NOT_OK(builder->Append(sums[col_index]));
        RETURN_NOT_OK(builder->Finish(&columns[col_index]));

        col_index++;
      }

      // Echo the totals to the client
      auto response = RecordBatch::Make(schema, /* num_rows */ 1, columns);
      RETURN_NOT_OK(writer->WriteRecordBatch(*response));
    }
  }
  return Status::OK();
}

// Echo the client's messages back.
Status TestFlightServer::RunExchangeEcho(std::unique_ptr<FlightMessageReader> reader,
                                         std::unique_ptr<FlightMessageWriter> writer) {
  FlightStreamChunk chunk;
  bool begun = false;
  while (true) {
    ARROW_ASSIGN_OR_RAISE(chunk, reader->Next());
    if (!chunk.data && !chunk.app_metadata) {
      break;
    }
    if (!begun && chunk.data) {
      begun = true;
      RETURN_NOT_OK(writer->Begin(chunk.data->schema()));
    }
    if (chunk.data && chunk.app_metadata) {
      RETURN_NOT_OK(writer->WriteWithMetadata(*chunk.data, chunk.app_metadata));
    } else if (chunk.data) {
      RETURN_NOT_OK(writer->WriteRecordBatch(*chunk.data));
    } else if (chunk.app_metadata) {
      RETURN_NOT_OK(writer->WriteMetadata(chunk.app_metadata));
    }
  }
  return Status::OK();
}

// Regression test for ARROW-13253
Status TestFlightServer::RunExchangeLargeBatch(
    std::unique_ptr<FlightMessageReader>, std::unique_ptr<FlightMessageWriter> writer) {
  ARROW_ASSIGN_OR_RAISE(auto batch, VeryLargeBatch());
  RETURN_NOT_OK(writer->Begin(batch->schema()));
  return writer->WriteRecordBatch(*batch);
}

Status TestFlightServer::RunAction1(const Action& action,
                                    std::unique_ptr<ResultStream>* out) {
  std::vector<Result> results;
  for (int i = 0; i < 3; ++i) {
    Result result;
    std::string value = action.body->ToString() + "-part" + std::to_string(i);
    result.body = Buffer::FromString(std::move(value));
    results.push_back(result);
  }
  *out = std::make_unique<SimpleResultStream>(std::move(results));
  return Status::OK();
}

Status TestFlightServer::RunAction2(std::unique_ptr<ResultStream>* out) {
  // Empty
  *out = std::make_unique<SimpleResultStream>(std::vector<Result>{});
  return Status::OK();
}

Status TestFlightServer::ListIncomingHeaders(const ServerCallContext& context,
                                             const Action& action,
                                             std::unique_ptr<ResultStream>* out) {
  std::vector<Result> results;
  std::string_view prefix(*action.body);
  for (const auto& header : context.incoming_headers()) {
    if (header.first.substr(0, prefix.size()) != prefix) {
      continue;
    }
    Result result;
    result.body =
        Buffer::FromString(std::string(header.first) + ": " + std::string(header.second));
    results.push_back(result);
  }
  *out = std::make_unique<SimpleResultStream>(std::move(results));
  return Status::OK();
}

Status TestFlightServer::DoAction(const ServerCallContext& context, const Action& action,
                                  std::unique_ptr<ResultStream>* out) {
  if (action.type == "action1") {
    return RunAction1(action, out);
  } else if (action.type == "action2") {
    return RunAction2(out);
  } else if (action.type == "list-incoming-headers") {
    return ListIncomingHeaders(context, action, out);
  } else {
    return Status::NotImplemented(action.type);
  }
}

Status TestFlightServer::ListActions(const ServerCallContext& context,
                                     std::vector<ActionType>* out) {
  std::vector<ActionType> actions = ExampleActionTypes();
  *out = std::move(actions);
  return Status::OK();
}

Status TestFlightServer::GetSchema(const ServerCallContext& context,
                                   const FlightDescriptor& request,
                                   std::unique_ptr<SchemaResult>* schema) {
  std::vector<FlightInfo> flights = ExampleFlightInfo();

  for (const auto& info : flights) {
    if (info.descriptor().Equals(request)) {
      *schema = std::make_unique<SchemaResult>(info.serialized_schema());
      return Status::OK();
    }
  }
  return Status::Invalid("Flight not found: ", request.ToString());
}

}  // namespace arrow::flight
