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

#nullable enable
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Flight.Server;
using Apache.Arrow.Types;
using Arrow.Flight.Protocol.Sql;
using Google.Protobuf;
using Grpc.Core;
using Xunit;

namespace Apache.Arrow.Flight.Sql.Tests;

public class FlightSqlServerTests
{
    [Theory]
    [InlineData(FlightDescriptorType.Path, null)]
    [InlineData(FlightDescriptorType.Command, null)]
    [InlineData(FlightDescriptorType.Command, typeof(CommandGetCatalogs))]
    public void EnsureGetCommandReturnsTheCorrectResponse(FlightDescriptorType type, Type? expectedResult)
    {
        //Given
        FlightDescriptor descriptor;
        if (type == FlightDescriptorType.Command)
        {
            descriptor = expectedResult != null ?
                FlightDescriptor.CreateCommandDescriptor(((IMessage) Activator.CreateInstance(expectedResult!)!).PackAndSerialize().ToByteArray()) :
                FlightDescriptor.CreateCommandDescriptor(ByteString.Empty.ToStringUtf8());
        }
        else
        {
            descriptor = FlightDescriptor.CreatePathDescriptor(System.Array.Empty<string>());
        }

        //When
        var result = FlightSqlServer.GetCommand(descriptor);

        //Then
        Assert.Equal(expectedResult, result?.GetType());
    }

    [Fact]
    public async Task EnsureTheCorrectActionsAreGiven()
    {
        //Given
        var producer = new TestFlightSqlSever();
        var streamWriter = new MockServerStreamWriter<FlightActionType>();

        //When
        await producer.ListActions(streamWriter, new MockServerCallContext()).ConfigureAwait(false);
        var actions = streamWriter.Messages.ToArray();

        Assert.Equal(FlightSqlUtils.FlightSqlActions, actions);
    }

    [Theory]
    [InlineData(false,
        new[] {"catalog_name", "db_schema_name", "table_name", "table_type"},
        new[] {typeof(StringType), typeof(StringType), typeof(StringType), typeof(StringType)},
        new[] {true, true, false, false})
    ]
    [InlineData(true,
        new[] {"catalog_name", "db_schema_name", "table_name", "table_type", "table_schema"},
        new[] {typeof(StringType), typeof(StringType), typeof(StringType), typeof(StringType), typeof(BinaryType)},
        new[] {true, true, false, false, false})
    ]
    public void EnsureTableSchemaIsCorrectWithoutTableSchema(bool includeTableSchemaField, string[] expectedNames, Type[] expectedTypes, bool[] expectedIsNullable)
    {
        // Arrange

        // Act
        var schema = FlightSqlServer.GetTableSchema(includeTableSchemaField);
        var fields = schema.FieldsList;

        //Assert
        Assert.False(schema.HasMetadata);
        Assert.Equal(expectedNames.Length, fields.Count);
        for (int i = 0; i < fields.Count; i++)
        {
            Assert.Equal(expectedNames[i], fields[i].Name);
            Assert.Equal(expectedTypes[i], fields[i].DataType.GetType());
            Assert.Equal(expectedIsNullable[i], fields[i].IsNullable);
        }
    }

    #region FlightInfoTests
    [Theory]
    [InlineData(typeof(CommandStatementQuery), "GetStatementQueryFlightInfo")]
    [InlineData(typeof(CommandPreparedStatementQuery), "GetPreparedStatementQueryFlightInfo")]
    [InlineData(typeof(CommandGetCatalogs), "GetCatalogFlightInfo")]
    [InlineData(typeof(CommandGetDbSchemas), "GetDbSchemaFlightInfo")]
    [InlineData(typeof(CommandGetTables), "GetTablesFlightInfo")]
    [InlineData(typeof(CommandGetTableTypes), "GetTableTypesFlightInfo")]
    [InlineData(typeof(CommandGetSqlInfo), "GetSqlFlightInfo")]
    [InlineData(typeof(CommandGetPrimaryKeys), "GetPrimaryKeysFlightInfo")]
    [InlineData(typeof(CommandGetExportedKeys), "GetExportedKeysFlightInfo")]
    [InlineData(typeof(CommandGetImportedKeys), "GetImportedKeysFlightInfo")]
    [InlineData(typeof(CommandGetCrossReference), "GetCrossReferenceFlightInfo")]
    [InlineData(typeof(CommandGetXdbcTypeInfo), "GetXdbcTypeFlightInfo")]
    public async void EnsureGetFlightInfoIsCorrectlyRoutedForCommand(Type commandType, string expectedResult)
    {
        //Given
        var command = (IMessage) Activator.CreateInstance(commandType)!;
        var producer = new TestFlightSqlSever();
        var descriptor = FlightDescriptor.CreateCommandDescriptor(command.PackAndSerialize().ToArray());

        //When
        var flightInfo = await producer.GetFlightInfo(descriptor, new MockServerCallContext());

        //Then
        Assert.Equal(expectedResult, flightInfo.Descriptor.Paths.First());
    }


    [Fact]
    public async void EnsureAnInvalidOperationExceptionIsThrownWhenACommandIsNotSupportedAndHasNoDescriptor()
    {
        //Given
        var producer = new TestFlightSqlSever();

        //When
        async Task<FlightInfo> Act() => await producer.GetFlightInfo(FlightDescriptor.CreatePathDescriptor(""), new MockServerCallContext());
        var exception = await Record.ExceptionAsync(Act);

        //Then
        Assert.Equal("command type  not supported", exception?.Message);
    }

    [Fact]
    public async void EnsureAnInvalidOperationExceptionIsThrownWhenACommandIsNotSupported()
    {
        //Given
        var producer = new TestFlightSqlSever();
        var command = new CommandPreparedStatementUpdate();
        var descriptor = FlightDescriptor.CreateCommandDescriptor(command.PackAndSerialize().ToArray());

        //When
        async Task<FlightInfo> Act() => await producer.GetFlightInfo(descriptor, new MockServerCallContext());
        var exception = await Record.ExceptionAsync(Act);

        //Then
        Assert.Equal("command type CommandPreparedStatementUpdate not supported", exception?.Message);
    }
    #endregion

    #region DoGetTests

    [Theory]
    [InlineData(typeof(CommandPreparedStatementQuery), "DoGetPreparedStatementQuery")]
    [InlineData(typeof(CommandGetSqlInfo), "DoGetSqlInfo")]
    [InlineData(typeof(CommandGetCatalogs), "DoGetCatalog")]
    [InlineData(typeof(CommandGetTableTypes), "DoGetTableType")]
    [InlineData(typeof(CommandGetTables), "DoGetTables")]
    [InlineData(typeof(CommandGetDbSchemas), "DoGetDbSchema")]
    [InlineData(typeof(CommandGetPrimaryKeys), "DoGetPrimaryKeys")]
    [InlineData(typeof(CommandGetExportedKeys), "DoGetExportedKeys")]
    [InlineData(typeof(CommandGetImportedKeys), "DoGetImportedKeys")]
    [InlineData(typeof(CommandGetCrossReference), "DoGetCrossReference")]
    [InlineData(typeof(CommandGetXdbcTypeInfo), "DoGetXbdcTypeInfo")]
    public async void EnsureDoGetIsCorrectlyRoutedForADoGetCommand(Type commandType, string expectedResult)
    {
        //Given
        var producer = new TestFlightSqlSever();
        var command = (IMessage) Activator.CreateInstance(commandType)!;
        var ticket = new FlightTicket(command.PackAndSerialize());
        var streamWriter = new MockServerStreamWriter<FlightData>();

        //When
        await producer.DoGet(ticket, new FlightServerRecordBatchStreamWriter(streamWriter), new MockServerCallContext());
        var schema = await streamWriter.Messages.GetSchema();

        //Then
        Assert.Equal(expectedResult, schema.FieldsList[0].Name);
    }

    [Fact]
    public async void EnsureAnInvalidOperationExceptionIsThrownWhenADoGetCommandIsNotSupported()
    {
        //Given
        var producer = new TestFlightSqlSever();
        var ticket = new FlightTicket("");
        var streamWriter = new MockServerStreamWriter<FlightData>();

        //When
        async Task Act() => await producer.DoGet(ticket, new FlightServerRecordBatchStreamWriter(streamWriter), new MockServerCallContext());
        var exception = await Record.ExceptionAsync(Act);

        //Then
        Assert.Equal("Status(StatusCode=\"InvalidArgument\", Detail=\"DoGet command  is not supported.\")", exception?.Message);
    }
    #endregion

    #region DoActionTests
    [Theory]
    [InlineData(SqlAction.CloseRequest, typeof(ActionClosePreparedStatementRequest), "ClosePreparedStatement")]
    [InlineData(SqlAction.CreateRequest, typeof(ActionCreatePreparedStatementRequest), "CreatePreparedStatement")]
    [InlineData("BadCommand", typeof(ActionCreatePreparedStatementRequest), "Action type BadCommand not supported", true)]
    public async void EnsureDoActionIsCorrectlyRoutedForAnActionRequest(string actionType, Type actionBodyType, string expectedResponse, bool isException = false)
    {
        //Given
        var producer = new TestFlightSqlSever();
        var actionBody = (IMessage) Activator.CreateInstance(actionBodyType)!;
        var action = new FlightAction(actionType, actionBody.PackAndSerialize());
        var mockStreamWriter = new MockStreamWriter<FlightResult>();

        //When
        async Task Act() => await producer.DoAction(action, mockStreamWriter, new MockServerCallContext());
        var exception = await Record.ExceptionAsync(Act);
        string? actualMessage = isException ? exception?.Message : mockStreamWriter.Messages[0].Body.ToStringUtf8();

        //Then
        Assert.Equal(expectedResponse, actualMessage);
    }
    #endregion

    #region DoPutTests
    [Theory]
    [InlineData(typeof(CommandStatementUpdate), "PutStatementUpdate")]
    [InlineData(typeof(CommandPreparedStatementQuery), "PutPreparedStatementQuery")]
    [InlineData(typeof(CommandPreparedStatementUpdate), "PutPreparedStatementUpdate")]
    [InlineData(typeof(CommandGetXdbcTypeInfo), "Command CommandGetXdbcTypeInfo not supported", true)]
    public async void EnsureDoPutIsCorrectlyRoutedForTheCommand(Type commandType, string expectedResponse, bool isException = false)
    {
        //Given
        var command = (IMessage) Activator.CreateInstance(commandType)!;
        var producer = new TestFlightSqlSever();
        var descriptor = FlightDescriptor.CreateCommandDescriptor(command.PackAndSerialize().ToArray());
        var recordBatch = new RecordBatch(new Schema(new List<Field>(), null), System.Array.Empty<Int8Array>(), 0);
        var reader = new MockStreamReader<FlightData>(await recordBatch.ToFlightData(descriptor).ConfigureAwait(false));
        var batchReader = new FlightServerRecordBatchStreamReader(reader);
        var mockStreamWriter = new MockServerStreamWriter<FlightPutResult>();

        //When
        async Task Act() => await producer.DoPut(batchReader, mockStreamWriter, new MockServerCallContext()).ConfigureAwait(false);
        var exception = await Record.ExceptionAsync(Act);
        string? actualMessage = isException ? exception?.Message : mockStreamWriter.Messages[0].ApplicationMetadata.ToStringUtf8();

        //Then
        Assert.Equal(expectedResponse, actualMessage);
    }
    #endregion

    private class MockServerCallContext : ServerCallContext
    {
        protected override Task WriteResponseHeadersAsyncCore(Metadata responseHeaders) => throw new NotImplementedException();

        protected override ContextPropagationToken CreatePropagationTokenCore(ContextPropagationOptions? options) => throw new NotImplementedException();
        protected override string MethodCore => "";
        protected override string HostCore => "";
        protected override string PeerCore => "";
        protected override DateTime DeadlineCore => new();
        protected override Metadata RequestHeadersCore => new();
        protected override CancellationToken CancellationTokenCore => default;
        protected override Metadata ResponseTrailersCore => new();
        protected override Status StatusCore { get; set; }
        protected override WriteOptions WriteOptionsCore { get; set; } = WriteOptions.Default;
        protected override AuthContext AuthContextCore => new("", new Dictionary<string, List<AuthProperty>>());
    }
}

internal class MockStreamWriter<T> : IServerStreamWriter<T>
{
    public Task WriteAsync(T message)
    {
        _messages.Add(message);
        return Task.FromResult(message);
    }

    public IReadOnlyList<T> Messages => new ReadOnlyCollection<T>(_messages);
    public WriteOptions? WriteOptions { get; set; }
    private readonly List<T> _messages = new();
}

internal class MockServerStreamWriter<T> : IServerStreamWriter<T>
{
    public Task WriteAsync(T message)
    {
        _messages.Add(message);
        return Task.FromResult(message);
    }

    public IReadOnlyList<T> Messages => new ReadOnlyCollection<T>(_messages);
    public WriteOptions? WriteOptions { get; set; }
    private readonly List<T> _messages = new();
}

internal static class MockStreamReaderWriterExtensions
{
    public static async Task<List<RecordBatch>> GetRecordBatches(this IReadOnlyList<FlightData> flightDataList)
    {
        var list = new List<RecordBatch>();
        var recordBatchReader = new FlightServerRecordBatchStreamReader(new MockStreamReader<FlightData>(flightDataList));
        while (await recordBatchReader.MoveNext().ConfigureAwait(false))
        {
            list.Add(recordBatchReader.Current);
        }

        return list;
    }

    public static async Task<Schema> GetSchema(this IEnumerable<FlightData> flightDataList)
    {
        var recordBatchReader = new FlightServerRecordBatchStreamReader(new MockStreamReader<FlightData>(flightDataList));
        return await recordBatchReader.Schema;
    }

    public static async Task<IEnumerable<FlightData>> ToFlightData(this RecordBatch recordBatch, FlightDescriptor? descriptor = null)
    {
        var responseStream = new MockFlightServerRecordBatchStreamWriter();
        await responseStream.WriteRecordBatchAsync(recordBatch).ConfigureAwait(false);
        if (descriptor == null)
        {
            return responseStream.FlightData;
        }

        return responseStream.FlightData.Select(
            flightData => new FlightData(descriptor, flightData.DataBody, flightData.DataHeader, flightData.AppMetadata)
            );
    }
}

internal class MockStreamReader<T>: IAsyncStreamReader<T>
{
    private readonly IEnumerator<T> _flightActions;

    public MockStreamReader(IEnumerable<T> flightActions)
    {
        _flightActions = flightActions.GetEnumerator();
    }

    public Task<bool> MoveNext(CancellationToken cancellationToken)
    {
        return Task.FromResult(_flightActions.MoveNext());
    }

    public T Current => _flightActions.Current;
}

internal class MockFlightServerRecordBatchStreamWriter : FlightServerRecordBatchStreamWriter
{
    private readonly MockStreamWriter<FlightData> _streamWriter;
    public MockFlightServerRecordBatchStreamWriter() : this(new MockStreamWriter<FlightData>()) { }

    private MockFlightServerRecordBatchStreamWriter(MockStreamWriter<FlightData> clientStreamWriter) : base(clientStreamWriter)
    {
        _streamWriter = clientStreamWriter;
    }

    public IEnumerable<FlightData> FlightData => _streamWriter.Messages;

    public async Task WriteRecordBatchAsync(RecordBatch recordBatch)
    {
        await WriteAsync(recordBatch).ConfigureAwait(false);
    }
}


