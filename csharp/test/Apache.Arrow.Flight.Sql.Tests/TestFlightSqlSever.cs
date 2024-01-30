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

using System.Collections.Generic;
using System.Reflection;
using System.Threading.Tasks;
using Apache.Arrow.Flight.Server;
using Apache.Arrow.Types;
using Arrow.Flight.Protocol.Sql;
using Grpc.Core;

namespace Apache.Arrow.Flight.Sql.Tests;

public class TestFlightSqlSever : FlightSqlServer
{
    protected override Task<FlightInfo> GetStatementQueryFlightInfo(CommandStatementQuery commandStatementQuery, FlightDescriptor flightDescriptor, ServerCallContext serverCallContext) => Task.FromResult(new FlightInfo(null, FlightDescriptor.CreatePathDescriptor(MethodBase.GetCurrentMethod().Name), System.Array.Empty<FlightEndpoint>()));

    protected override Task<FlightInfo> GetPreparedStatementQueryFlightInfo(CommandPreparedStatementQuery preparedStatementQuery, FlightDescriptor flightDescriptor, ServerCallContext serverCallContext) => Task.FromResult(new FlightInfo(null, FlightDescriptor.CreatePathDescriptor(MethodBase.GetCurrentMethod().Name), System.Array.Empty<FlightEndpoint>()));

    protected override Task<FlightInfo> GetCatalogFlightInfo(CommandGetCatalogs command, FlightDescriptor flightDescriptor, ServerCallContext serverCallContext) => Task.FromResult(new FlightInfo(null, FlightDescriptor.CreatePathDescriptor(MethodBase.GetCurrentMethod().Name), System.Array.Empty<FlightEndpoint>()));

    protected override Task<FlightInfo> GetDbSchemaFlightInfo(CommandGetDbSchemas command, FlightDescriptor flightDescriptor, ServerCallContext serverCallContext) => Task.FromResult(new FlightInfo(null, FlightDescriptor.CreatePathDescriptor(MethodBase.GetCurrentMethod().Name), System.Array.Empty<FlightEndpoint>()));

    protected override Task<FlightInfo> GetTablesFlightInfo(CommandGetTables command, FlightDescriptor flightDescriptor, ServerCallContext serverCallContext) => Task.FromResult(new FlightInfo(null, FlightDescriptor.CreatePathDescriptor(MethodBase.GetCurrentMethod().Name), System.Array.Empty<FlightEndpoint>()));

    protected override Task<FlightInfo> GetTableTypesFlightInfo(CommandGetTableTypes command, FlightDescriptor flightDescriptor, ServerCallContext serverCallContext) => Task.FromResult(new FlightInfo(null, FlightDescriptor.CreatePathDescriptor(MethodBase.GetCurrentMethod().Name), System.Array.Empty<FlightEndpoint>()));

    protected override Task<FlightInfo> GetSqlFlightInfo(CommandGetSqlInfo commandGetSqlInfo, FlightDescriptor flightDescriptor, ServerCallContext serverCallContext) => Task.FromResult(new FlightInfo(null, FlightDescriptor.CreatePathDescriptor(MethodBase.GetCurrentMethod().Name), System.Array.Empty<FlightEndpoint>()));

    protected override Task<FlightInfo> GetPrimaryKeysFlightInfo(CommandGetPrimaryKeys command, FlightDescriptor flightDescriptor, ServerCallContext serverCallContext) => Task.FromResult(new FlightInfo(null, FlightDescriptor.CreatePathDescriptor(MethodBase.GetCurrentMethod().Name), System.Array.Empty<FlightEndpoint>()));

    protected override Task<FlightInfo> GetExportedKeysFlightInfo(CommandGetExportedKeys command, FlightDescriptor flightDescriptor, ServerCallContext serverCallContext) => Task.FromResult(new FlightInfo(null, FlightDescriptor.CreatePathDescriptor(MethodBase.GetCurrentMethod().Name), System.Array.Empty<FlightEndpoint>()));

    protected override Task<FlightInfo> GetImportedKeysFlightInfo(CommandGetImportedKeys command, FlightDescriptor flightDescriptor, ServerCallContext serverCallContext) => Task.FromResult(new FlightInfo(null, FlightDescriptor.CreatePathDescriptor(MethodBase.GetCurrentMethod().Name), System.Array.Empty<FlightEndpoint>()));

    protected override Task<FlightInfo> GetCrossReferenceFlightInfo(CommandGetCrossReference command, FlightDescriptor flightDescriptor, ServerCallContext serverCallContext) => Task.FromResult(new FlightInfo(null, FlightDescriptor.CreatePathDescriptor(MethodBase.GetCurrentMethod().Name), System.Array.Empty<FlightEndpoint>()));

    protected override Task<FlightInfo> GetXdbcTypeFlightInfo(CommandGetXdbcTypeInfo command, FlightDescriptor flightDescriptor, ServerCallContext serverCallContext) => Task.FromResult(new FlightInfo(null, FlightDescriptor.CreatePathDescriptor(MethodBase.GetCurrentMethod().Name), System.Array.Empty<FlightEndpoint>()));

    protected override async Task DoGetPreparedStatementQuery(CommandPreparedStatementQuery preparedStatementQuery, FlightServerRecordBatchStreamWriter responseStream, ServerCallContext context) => await responseStream.WriteAsync(MockRecordBatch("DoGetPreparedStatementQuery"));

    protected override async Task DoGetSqlInfo(CommandGetSqlInfo getSqlInfo, FlightServerRecordBatchStreamWriter responseStream, ServerCallContext context) => await responseStream.WriteAsync(MockRecordBatch("DoGetSqlInfo"));

    protected override async Task DoGetCatalog(CommandGetCatalogs command, FlightServerRecordBatchStreamWriter responseStream, ServerCallContext context) => await responseStream.WriteAsync(MockRecordBatch("DoGetCatalog"));

    protected override async Task DoGetTableType(CommandGetTableTypes command, FlightServerRecordBatchStreamWriter responseStream, ServerCallContext context) => await responseStream.WriteAsync(MockRecordBatch("DoGetTableType"));

    protected override async Task DoGetTables(CommandGetTables command, FlightServerRecordBatchStreamWriter responseStream, ServerCallContext context) => await responseStream.WriteAsync(MockRecordBatch("DoGetTables"));

    protected override async Task DoGetPrimaryKeys(CommandGetPrimaryKeys command, FlightServerRecordBatchStreamWriter responseStream, ServerCallContext context) => await responseStream.WriteAsync(MockRecordBatch("DoGetPrimaryKeys"));

    protected override async Task DoGetDbSchema(CommandGetDbSchemas command, FlightServerRecordBatchStreamWriter responseStream, ServerCallContext context) => await responseStream.WriteAsync(MockRecordBatch("DoGetDbSchema"));

    protected override async Task DoGetExportedKeys(CommandGetExportedKeys command, FlightServerRecordBatchStreamWriter responseStream, ServerCallContext context) => await responseStream.WriteAsync(MockRecordBatch("DoGetExportedKeys"));

    protected override async Task DoGetImportedKeys(CommandGetImportedKeys command, FlightServerRecordBatchStreamWriter responseStream, ServerCallContext context) => await responseStream.WriteAsync(MockRecordBatch("DoGetImportedKeys"));

    protected override async Task DoGetCrossReference(CommandGetCrossReference command, FlightServerRecordBatchStreamWriter responseStream, ServerCallContext context) => await responseStream.WriteAsync(MockRecordBatch("DoGetCrossReference"));

    protected override async Task DoGetXbdcTypeInfo(CommandGetXdbcTypeInfo command, FlightServerRecordBatchStreamWriter responseStream, ServerCallContext context) => await responseStream.WriteAsync(MockRecordBatch("DoGetXbdcTypeInfo"));

    protected override async Task CreatePreparedStatement(ActionCreatePreparedStatementRequest request, FlightAction action, IAsyncStreamWriter<FlightResult> streamWriter, ServerCallContext serverCallContext) => await streamWriter.WriteAsync(new FlightResult("CreatePreparedStatement"));

    protected override async Task ClosePreparedStatement(ActionClosePreparedStatementRequest request, FlightAction action, IAsyncStreamWriter<FlightResult> streamWriter, ServerCallContext serverCallContext) => await streamWriter.WriteAsync(new FlightResult("ClosePreparedStatement"));

    protected override async Task PutPreparedStatementUpdate(CommandPreparedStatementUpdate command, FlightServerRecordBatchStreamReader requestStream, IAsyncStreamWriter<FlightPutResult> responseStream, ServerCallContext context) => await responseStream.WriteAsync(new FlightPutResult("PutPreparedStatementUpdate"));

    protected override async Task PutStatementUpdate(CommandStatementUpdate command, FlightServerRecordBatchStreamReader requestStream, IAsyncStreamWriter<FlightPutResult> responseStream, ServerCallContext context) => await responseStream.WriteAsync(new FlightPutResult("PutStatementUpdate"));

    protected override async Task PutPreparedStatementQuery(CommandPreparedStatementQuery command, FlightServerRecordBatchStreamReader requestStream, IAsyncStreamWriter<FlightPutResult> responseStream, ServerCallContext context) => await responseStream.WriteAsync(new FlightPutResult("PutPreparedStatementQuery"));

    private RecordBatch MockRecordBatch(string name)
    {
        var schema = new Schema(new List<Field> {new(name, StringType.Default, false)}, System.Array.Empty<KeyValuePair<string, string>>());
        return new RecordBatch(schema, new []{ new StringArray.Builder().Append(name).Build() }, 1);
    }
}
