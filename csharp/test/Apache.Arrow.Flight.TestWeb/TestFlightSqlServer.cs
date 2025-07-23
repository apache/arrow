// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using System;
using System.Linq;
using System.Threading.Tasks;
using Apache.Arrow.Flight.Server;
using Apache.Arrow.Flight.Sql;
using Apache.Arrow.Types;
using Arrow.Flight.Protocol.Sql;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;

namespace Apache.Arrow.Flight.TestWeb;

public class TestFlightSqlServer : FlightServer
{
    private readonly FlightStore _flightStore;

    public TestFlightSqlServer(FlightStore flightStore)
    {
        _flightStore = flightStore;
    }

    public override async Task DoAction(FlightAction request, IAsyncStreamWriter<FlightResult> responseStream,
        ServerCallContext context)
    {
        switch (request.Type)
        {
            case "test":
                await responseStream.WriteAsync(new FlightResult("test data")).ConfigureAwait(false);
                break;
            case SqlAction.GetPrimaryKeysRequest:
                await responseStream.WriteAsync(new FlightResult("test data")).ConfigureAwait(false);
                break;
            case SqlAction.CancelFlightInfoRequest:
                var cancelRequest = new FlightInfoCancelResult();
                cancelRequest.SetStatus(1);
                await responseStream.WriteAsync(new FlightResult(Any.Pack(cancelRequest).Serialize().ToByteArray()))
                    .ConfigureAwait(false);
                break;
            case SqlAction.BeginTransactionRequest:
            case SqlAction.CommitRequest:
            case SqlAction.RollbackRequest:
                await responseStream.WriteAsync(new FlightResult(ByteString.CopyFromUtf8("sample-transaction-id")))
                    .ConfigureAwait(false);
                break;
            case SqlAction.CreateRequest:
            case SqlAction.CloseRequest:
                var schema = new Schema.Builder()
                    .Field(f => f.Name("id").DataType(Int32Type.Default))
                    .Field(f => f.Name("name").DataType(StringType.Default))
                    .Build();
                var datasetSchemaBytes = SchemaExtensions.SerializeSchema(schema);
                var parameterSchemaBytes = SchemaExtensions.SerializeSchema(schema);

                var preparedStatementResponse = new ActionCreatePreparedStatementResult
                {
                    PreparedStatementHandle = ByteString.CopyFromUtf8("sample-testing-prepared-statement"),
                    DatasetSchema = ByteString.CopyFrom(datasetSchemaBytes),
                    ParameterSchema = ByteString.CopyFrom(parameterSchemaBytes)
                };
                byte[] packedResult = Any.Pack(preparedStatementResponse).Serialize().ToByteArray();
                var flightResult = new FlightResult(packedResult);
                await responseStream.WriteAsync(flightResult).ConfigureAwait(false);
                break;
            default:
                throw new NotImplementedException();
        }
    }

    public override async Task DoGet(FlightTicket ticket, FlightServerRecordBatchStreamWriter responseStream,
        ServerCallContext context)
    {
        FlightDescriptor flightDescriptor = FlightDescriptor.CreateCommandDescriptor(ticket.Ticket.ToStringUtf8());

        if (_flightStore.Flights.TryGetValue(flightDescriptor, out var flightHolder))
        {
            var batches = flightHolder.GetRecordBatches();

            foreach (var batch in batches)
            {
                await responseStream.WriteAsync(batch.RecordBatch, batch.Metadata);
            }
        }
    }
    
    public override async Task DoPut(FlightServerRecordBatchStreamReader requestStream, IAsyncStreamWriter<FlightPutResult> responseStream, ServerCallContext context)
    {
        var flightDescriptor = await requestStream.FlightDescriptor;

        if (!_flightStore.Flights.TryGetValue(flightDescriptor, out var flightHolder))
        {
            flightHolder = new FlightHolder(flightDescriptor, await requestStream.Schema, $"http://{context.Host}");
            _flightStore.Flights.Add(flightDescriptor, flightHolder);
        }
        
        int affectedRows = 0;
        while (await requestStream.MoveNext())
        {
            // Process the record batch (if needed) here
            // Increment the affected row count for demonstration purposes
            affectedRows += requestStream.Current.Column(0).Length;  // Example of counting rows in the first column
        }

        // Create a DoPutUpdateResult with the affected row count
        var updateResult = new DoPutUpdateResult
        {
            RecordCount = affectedRows // Set the actual affected row count
        };

        // Serialize the DoPutUpdateResult into a ByteString
        var metadata = updateResult.ToByteString();

        // Send the metadata back as part of the FlightPutResult
        var flightPutResult = new FlightPutResult(metadata);
        await responseStream.WriteAsync(flightPutResult);
    }
    
    public override Task<FlightInfo> GetFlightInfo(FlightDescriptor request, ServerCallContext context)
    {
        if (_flightStore.Flights.TryGetValue(request, out var flightHolder))
        {
            return Task.FromResult(flightHolder.GetFlightInfo());
        }

        if (_flightStore.Flights.Count > 0)
        {
            return Task.FromResult(_flightStore.Flights.First().Value.GetFlightInfo());
        }

        throw new RpcException(new Status(StatusCode.NotFound, "Flight not found"));
    }
    
    public override Task<Schema> GetSchema(FlightDescriptor request, ServerCallContext context)
    {
        if (_flightStore.Flights.TryGetValue(request, out var flightHolder))
        {
            return Task.FromResult(flightHolder.GetFlightInfo().Schema);
        }

        if (_flightStore.Flights.Count > 0)
        {
            return Task.FromResult(_flightStore.Flights.First().Value.GetFlightInfo().Schema);
        }

        throw new RpcException(new Status(StatusCode.NotFound, "Flight not found"));
    }
}
