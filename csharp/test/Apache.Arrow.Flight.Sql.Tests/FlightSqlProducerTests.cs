using System.Collections.ObjectModel;
using Apache.Arrow.Flight.Protocol;
using Apache.Arrow.Flight.Server;
using Apache.Arrow.Types;
using Arrow.Flight.Protocol.Sql;
using Google.Protobuf;
using Google.Protobuf.Reflection;
using Grpc.Core;
using Xunit;

namespace Apache.Arrow.Flight.Sql.Tests;

public class FlightSqlProducerTests
{
    [Theory]
    [InlineData(FlightDescriptorType.Path, null, null)]
    [InlineData(FlightDescriptorType.Command, "", null)]
    [InlineData(FlightDescriptorType.Command, "CkB0eXBlLmdvb2dsZWFwaXMuY29tL2Fycm93LmZsaWdodC5wcm90b2NvbC5zcWwuQ29tbWFuZEdldENhdGFsb2dz", typeof(CommandGetCatalogs))]
    public void EnsureGetCommandReturnsTheCorrectResponse(FlightDescriptorType type, string? command, Type? expectedResult)
    {
        //Given
        FlightDescriptor descriptor;
        if (type == FlightDescriptorType.Command)
        {
            descriptor = command != null ? FlightDescriptor.CreateCommandDescriptor(ByteString.FromBase64(command).ToByteArray()) : FlightDescriptor.CreateCommandDescriptor(ByteString.Empty.ToStringUtf8());
        }
        else
        {
            descriptor = FlightDescriptor.CreatePathDescriptor(System.Array.Empty<string>());
        }

        //When
        var result = FlightSqlProducer.GetCommand(descriptor);

        //Then
        Assert.Equal(expectedResult, result?.GetType());
    }

    [Fact]
    public void EnsureTheCorrectActionsAreGiven()
    {
        //Given
        var producer = new TestFlightSqlProducer();

        //When
        var actions = producer.ListActions();

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
    public void EnsureTableSchemaIsCorrectWithoutTableSchema(bool includeTableSchemaField, string[] expectedNames, System.Type[] expectedTypes, bool[] expectedIsNullable)
    {
        // Arrange

        // Act
        var schema = TestFlightSqlProducer.GetTableSchema(includeTableSchemaField);
        var fields = schema.Fields.Values.ToArray();

        //Assert
        Assert.False(schema.HasMetadata);
        Assert.Equal(expectedNames.Length, fields.Length);
        for (var i = 0; i < fields.Length; i++)
        {
            Assert.Equal(expectedNames[i], fields[i].Name);
            Assert.Equal(expectedTypes[i], fields[i].DataType.GetType());
            Assert.Equal(expectedIsNullable[i], fields[i].IsNullable);
        }
    }

    #region FlightInfoTests
    [Fact]
    public async void EnsureGetFlightInfoIsCorrectlyRoutedForTheCommandStatementQuery()
    {
        //Given
        var producer = new TestFlightSqlProducer();
        var command = new CommandStatementQuery();

        //When
        var flightInfo = await producer.GetFlightInfo(command, FlightDescriptor.CreatePathDescriptor(""), new MockServerCallContext());

        //Then
        Assert.Equal("GetStatementQueryFlightInfo", flightInfo.Descriptor.Paths.First());
    }

    [Fact]
    public async void EnsureGetFlightInfoIsCorrectlyRoutedForTheCommandPreparedStatementQuery()
    {
        //Given
        var producer = new TestFlightSqlProducer();
        var command = new CommandPreparedStatementQuery();

        //When
        var flightInfo = await producer.GetFlightInfo(command, FlightDescriptor.CreatePathDescriptor(""), new MockServerCallContext());

        //Then
        Assert.Equal("GetPreparedStatementQueryFlightInfo", flightInfo.Descriptor.Paths.First());
    }

    [Fact]
    public async void EnsureGetFlightInfoIsCorrectlyRoutedForTheCommandGetCatalogs()
    {
        //Given
        var producer = new TestFlightSqlProducer();
        var command = new CommandGetCatalogs();

        //When
        var flightInfo = await producer.GetFlightInfo(command, FlightDescriptor.CreatePathDescriptor(""), new MockServerCallContext());

        //Then
        Assert.Equal("GetCatalogFlightInfo", flightInfo.Descriptor.Paths.First());
    }

    [Fact]
    public async void EnsureGetFlightInfoIsCorrectlyRoutedForTheCommandGetDbSchemas()
    {
        //Given
        var producer = new TestFlightSqlProducer();
        var command = new CommandGetDbSchemas();

        //When
        var flightInfo = await producer.GetFlightInfo(command, FlightDescriptor.CreatePathDescriptor(""), new MockServerCallContext());

        //Then
        Assert.Equal("GetDbSchemaFlightInfo", flightInfo.Descriptor.Paths.First());
    }

    [Fact]
    public async void EnsureGetFlightInfoIsCorrectlyRoutedForTheCommandGetTables()
    {
        //Given
        var producer = new TestFlightSqlProducer();
        var command = new CommandGetTables();

        //When
        var flightInfo = await producer.GetFlightInfo(command, FlightDescriptor.CreatePathDescriptor(""), new MockServerCallContext());

        //Then
        Assert.Equal("GetTablesFlightInfo", flightInfo.Descriptor.Paths.First());
    }

    [Fact]
    public async void EnsureGetFlightInfoIsCorrectlyRoutedForTheCommandGetTableTypes()
    {
        //Given
        var producer = new TestFlightSqlProducer();
        var command = new CommandGetTableTypes();

        //When
        var flightInfo = await producer.GetFlightInfo(command, FlightDescriptor.CreatePathDescriptor(""), new MockServerCallContext());

        //Then
        Assert.Equal("GetTableTypesFlightInfo", flightInfo.Descriptor.Paths.First());
    }

    [Fact]
    public async void EnsureGetFlightInfoIsCorrectlyRoutedForTheCommandGetSqlInfo()
    {
        //Given
        var producer = new TestFlightSqlProducer();
        var command = new CommandGetSqlInfo();

        //When
        var flightInfo = await producer.GetFlightInfo(command, FlightDescriptor.CreatePathDescriptor(""), new MockServerCallContext());

        //Then
        Assert.Equal("GetSqlFlightInfo", flightInfo.Descriptor.Paths.First());
    }

    [Fact]
    public async void EnsureGetFlightInfoIsCorrectlyRoutedForTheCommandGetPrimaryKeys()
    {
        //Given
        var producer = new TestFlightSqlProducer();
        var command = new CommandGetPrimaryKeys();

        //When
        var flightInfo = await producer.GetFlightInfo(command, FlightDescriptor.CreatePathDescriptor(""), new MockServerCallContext());

        //Then
        Assert.Equal("GetPrimaryKeysFlightInfo", flightInfo.Descriptor.Paths.First());
    }

    [Fact]
    public async void EnsureGetFlightInfoIsCorrectlyRoutedForTheCommandGetExportedKeys()
    {
        //Given
        var producer = new TestFlightSqlProducer();
        var command = new CommandGetExportedKeys();

        //When
        var flightInfo = await producer.GetFlightInfo(command, FlightDescriptor.CreatePathDescriptor(""), new MockServerCallContext());

        //Then
        Assert.Equal("GetExportedKeysFlightInfo", flightInfo.Descriptor.Paths.First());
    }

    [Fact]
    public async void EnsureGetFlightInfoIsCorrectlyRoutedForTheCommandGetImportedKeys()
    {
        //Given
        var producer = new TestFlightSqlProducer();
        var command = new CommandGetImportedKeys();

        //When
        var flightInfo = await producer.GetFlightInfo(command, FlightDescriptor.CreatePathDescriptor(""), new MockServerCallContext());

        //Then
        Assert.Equal("GetImportedKeysFlightInfo", flightInfo.Descriptor.Paths.First());
    }

    [Fact]
    public async void EnsureGetFlightInfoIsCorrectlyRoutedForTheCommandGetCrossReference()
    {
        //Given
        var producer = new TestFlightSqlProducer();
        var command = new CommandGetCrossReference();

        //When
        var flightInfo = await producer.GetFlightInfo(command, FlightDescriptor.CreatePathDescriptor(""), new MockServerCallContext());

        //Then
        Assert.Equal("GetCrossReferenceFlightInfo", flightInfo.Descriptor.Paths.First());
    }

    [Fact]
    public async void EnsureGetFlightInfoIsCorrectlyRoutedForTheCommandGetXdbcTypeInfo()
    {
        //Given
        var producer = new TestFlightSqlProducer();
        var command = new CommandGetXdbcTypeInfo();

        //When
        var flightInfo = await producer.GetFlightInfo(command, FlightDescriptor.CreatePathDescriptor(""), new MockServerCallContext());

        //Then
        Assert.Equal("GetXdbcTypeFlightInfo", flightInfo.Descriptor.Paths.First());
    }

    [Fact]
    public async void EnsureAnInvalidOperationExceptionIsThrownWhenACommandIsNotSupportedAndHasNoDescriptor()
    {
        //Given
        var producer = new TestFlightSqlProducer();
        var command = new BadCommand();

        //When
        var act = async () => await producer.GetFlightInfo(command, FlightDescriptor.CreatePathDescriptor(""), new MockServerCallContext());
        var exception = await Record.ExceptionAsync(act);

        //Then
        Assert.Equal("command type  not supported", exception.Message);
    }

    [Fact]
    public async void EnsureAnInvalidOperationExceptionIsThrownWhenACommandIsNotSupported()
    {
        //Given
        var producer = new TestFlightSqlProducer();
        var command = new CommandPreparedStatementUpdate();

        //When
        var act = async () => await producer.GetFlightInfo(command, FlightDescriptor.CreatePathDescriptor(""), new MockServerCallContext());
        var exception = await Record.ExceptionAsync(act);

        //Then
        Assert.Equal("command type CommandPreparedStatementUpdate not supported", exception.Message);
    }
    #endregion

    #region DoGetTests
    [Fact]
    public async void EnsureDoGetIsCorrectlyRoutedForTheCommandPreparedStatementQuery()
    {
        //Given
        var producer = new TestFlightSqlProducer();
        var command = new CommandPreparedStatementQuery();
        var ticket = new FlightTicket(command.PackAndSerialize());
        var streamWriter = new MockServerStreamWriter<FlightData>();

        //When
        await producer.DoGet(ticket, new FlightServerRecordBatchStreamWriter(streamWriter), new MockServerCallContext());

        //Then
        var schema = await streamWriter.Messages.GetSchema();
        Assert.Equal("DoGetPreparedStatementQuery", schema.Fields.First().Key);
    }

    [Fact]
    public async void EnsureDoGetIsCorrectlyRoutedForTheCommandGetSqlInfo()
    {
        //Given
        var producer = new TestFlightSqlProducer();
        var command = new CommandGetSqlInfo();
        var ticket = new FlightTicket(command.PackAndSerialize());
        var streamWriter = new MockServerStreamWriter<FlightData>();

        //When
        await producer.DoGet(ticket, new FlightServerRecordBatchStreamWriter(streamWriter), new MockServerCallContext());

        //Then
        var schema = await streamWriter.Messages.GetSchema();
        Assert.Equal("DoGetSqlInfo", schema.Fields.First().Key);
    }

    [Fact]
    public async void EnsureDoGetIsCorrectlyRoutedForTheCommandGetCatalogs()
    {
        //Given
        var producer = new TestFlightSqlProducer();
        var command = new CommandGetCatalogs();
        var ticket = new FlightTicket(command.PackAndSerialize());
        var streamWriter = new MockServerStreamWriter<FlightData>();

        //When
        await producer.DoGet(ticket, new FlightServerRecordBatchStreamWriter(streamWriter), new MockServerCallContext());

        //Then
        var schema = await streamWriter.Messages.GetSchema();
        Assert.Equal("DoGetCatalog", schema.Fields.First().Key);
    }

    [Fact]
    public async void EnsureDoGetIsCorrectlyRoutedForTheCommandGetTableTypes()
    {
        //Given
        var producer = new TestFlightSqlProducer();
        var command = new CommandGetTableTypes();
        var ticket = new FlightTicket(command.PackAndSerialize());
        var streamWriter = new MockServerStreamWriter<FlightData>();

        //When
        await producer.DoGet(ticket, new FlightServerRecordBatchStreamWriter(streamWriter), new MockServerCallContext());

        //Then
        var schema = await streamWriter.Messages.GetSchema();
        Assert.Equal("DoGetTableType", schema.Fields.First().Key);
    }

    [Fact]
    public async void EnsureDoGetIsCorrectlyRoutedForTheCommandGetTables()
    {
        //Given
        var producer = new TestFlightSqlProducer();
        var command = new CommandGetTables();
        var ticket = new FlightTicket(command.PackAndSerialize());
        var streamWriter = new MockServerStreamWriter<FlightData>();

        //When
        await producer.DoGet(ticket, new FlightServerRecordBatchStreamWriter(streamWriter), new MockServerCallContext());

        //Then
        var schema = await streamWriter.Messages.GetSchema();
        Assert.Equal("DoGetTables", schema.Fields.First().Key);
    }

    [Fact]
    public async void EnsureDoGetIsCorrectlyRoutedForTheCommandGetDbSchemas()
    {
        //Given
        var producer = new TestFlightSqlProducer();
        var command = new CommandGetDbSchemas();
        var ticket = new FlightTicket(command.PackAndSerialize());
        var streamWriter = new MockServerStreamWriter<FlightData>();

        //When
        await producer.DoGet(ticket, new FlightServerRecordBatchStreamWriter(streamWriter), new MockServerCallContext());

        //Then
        var schema = await streamWriter.Messages.GetSchema();
        Assert.Equal("DoGetDbSchema", schema.Fields.First().Key);
    }

    [Fact]
    public async void EnsureDoGetIsCorrectlyRoutedForTheCommandGetPrimaryKeys()
    {
        //Given
        var producer = new TestFlightSqlProducer();
        var command = new CommandGetPrimaryKeys();
        var ticket = new FlightTicket(command.PackAndSerialize());
        var streamWriter = new MockServerStreamWriter<FlightData>();

        //When
        await producer.DoGet(ticket, new FlightServerRecordBatchStreamWriter(streamWriter), new MockServerCallContext());

        //Then
        var schema = await streamWriter.Messages.GetSchema();
        Assert.Equal("DoGetPrimaryKeys", schema.Fields.First().Key);
    }

    [Fact]
    public async void EnsureDoGetIsCorrectlyRoutedForTheCommandGetExportedKeys()
    {
        //Given
        var producer = new TestFlightSqlProducer();
        var command = new CommandGetExportedKeys();
        var ticket = new FlightTicket(command.PackAndSerialize());
        var streamWriter = new MockServerStreamWriter<FlightData>();

        //When
        await producer.DoGet(ticket, new FlightServerRecordBatchStreamWriter(streamWriter), new MockServerCallContext());

        //Then
        var schema = await streamWriter.Messages.GetSchema();
        Assert.Equal("DoGetExportedKeys", schema.Fields.First().Key);
    }

    [Fact]
    public async void EnsureDoGetIsCorrectlyRoutedForTheCommandGetImportedKeys()
    {
        //Given
        var producer = new TestFlightSqlProducer();
        var command = new CommandGetImportedKeys();
        var ticket = new FlightTicket(command.PackAndSerialize());
        var streamWriter = new MockServerStreamWriter<FlightData>();

        //When
        await producer.DoGet(ticket, new FlightServerRecordBatchStreamWriter(streamWriter), new MockServerCallContext());

        //Then
        var schema = await streamWriter.Messages.GetSchema();
        Assert.Equal("DoGetImportedKeys", schema.Fields.First().Key);
    }

    [Fact]
    public async void EnsureDoGetIsCorrectlyRoutedForTheCommandGetCrossReference()
    {
        //Given
        var producer = new TestFlightSqlProducer();
        var command = new CommandGetCrossReference();
        var ticket = new FlightTicket(command.PackAndSerialize());
        var streamWriter = new MockServerStreamWriter<FlightData>();

        //When
        await producer.DoGet(ticket, new FlightServerRecordBatchStreamWriter(streamWriter), new MockServerCallContext());

        //Then
        var schema = await streamWriter.Messages.GetSchema();
        Assert.Equal("DoGetCrossReference", schema.Fields.First().Key);
    }

    [Fact]
    public async void EnsureDoGetIsCorrectlyRoutedForTheCommandGetXdbcTypeInfo()
    {
        //Given
        var producer = new TestFlightSqlProducer();
        var command = new CommandGetXdbcTypeInfo();
        var ticket = new FlightTicket(command.PackAndSerialize());
        var streamWriter = new MockServerStreamWriter<FlightData>();

        //When
        await producer.DoGet(ticket, new FlightServerRecordBatchStreamWriter(streamWriter), new MockServerCallContext());

        //Then
        var schema = await streamWriter.Messages.GetSchema();
        Assert.Equal("DoGetXbdcTypeInfo", schema.Fields.First().Key);
    }

    [Fact]
    public async void EnsureAnInvalidOperationExceptionIsThrownWhenADoGetCommandIsNotSupported()
    {
        //Given
        var producer = new TestFlightSqlProducer();
        var ticket = new FlightTicket("");
        var streamWriter = new MockServerStreamWriter<FlightData>();

        //When
        var act = async () => await producer.DoGet(ticket, new FlightServerRecordBatchStreamWriter(streamWriter), new MockServerCallContext());;
        var exception = await Record.ExceptionAsync(act);

        //Then
        Assert.Equal("Status(StatusCode=\"InvalidArgument\", Detail=\"DoGet command  is not supported.\")", exception?.Message);
    }
    #endregion

    #region DoActionTests
    [Fact]
    public async void EnsureDoActionIsCorrectlyRoutedForTheActionCreateRequest()
    {
        //Given
        var producer = new TestFlightSqlProducer();
        var action = new FlightAction(SqlAction.CreateRequest, new ActionCreatePreparedStatementRequest().PackAndSerialize());
        var mockStreamWriter = new MockStreamWriter<FlightResult>();

        //When
        await producer.DoAction(action, mockStreamWriter, new MockServerCallContext());

        //Then
        Assert.Equal("CreatePreparedStatement", mockStreamWriter.Messages.First().Body.ToStringUtf8());
    }

    [Fact]
    public async void EnsureDoActionIsCorrectlyRoutedForTheActionCloseRequest()
    {
        //Given
        var producer = new TestFlightSqlProducer();
        var action = new FlightAction(SqlAction.CloseRequest, new ActionClosePreparedStatementRequest().PackAndSerialize());
        var mockStreamWriter = new MockStreamWriter<FlightResult>();

        //When
        await producer.DoAction(action, mockStreamWriter, new MockServerCallContext());

        //Then
        Assert.Equal("ClosePreparedStatement", mockStreamWriter.Messages.First().Body.ToStringUtf8());
    }

    [Fact]
    public async void EnsureAnInvalidOperationExceptionIsThrownWhenADoActionCommandIsNotSupported()
    {
        //Given
        var producer = new TestFlightSqlProducer();
        var action = new FlightAction("BadCommand");
        var mockStreamWriter = new MockStreamWriter<FlightResult>();

        //When
        var act = async () => await producer.DoAction(action, mockStreamWriter, new MockServerCallContext());
        var exception = await Record.ExceptionAsync(act);

        //Then
        Assert.Equal("Action type BadCommand not supported", exception?.Message);
    }
    #endregion

    #region DoPutTests
    [Fact]
    public async void EnsureDoPutIsCorrectlyRoutedForTheCommandStatementUpdate()
    {
        //Given
        var producer = new TestFlightSqlProducer();
        var command = new CommandStatementUpdate();
        var reader = new MockStreamReader<FlightData>(System.Array.Empty<FlightData>());
        var mockStreamWriter = new MockServerStreamWriter<FlightPutResult>();

        //When
        await producer.DoPut(command, new FlightServerRecordBatchStreamReader(reader), mockStreamWriter, new MockServerCallContext()).ConfigureAwait(false);

        //Then
        Assert.Equal("PutStatementUpdate", mockStreamWriter.Messages[0].ApplicationMetadata.ToStringUtf8());
    }

    [Fact]
    public async void EnsureDoPutIsCorrectlyRoutedForTheCommandPreparedStatementQuery()
    {
        //Given
        var producer = new TestFlightSqlProducer();
        var command = new CommandPreparedStatementQuery();
        var reader = new MockStreamReader<FlightData>(System.Array.Empty<FlightData>());
        var mockStreamWriter = new MockServerStreamWriter<FlightPutResult>();

        //When
        await producer.DoPut(command, new FlightServerRecordBatchStreamReader(reader), mockStreamWriter, new MockServerCallContext()).ConfigureAwait(false);

        //Then
        Assert.Equal("PutPreparedStatementQuery", mockStreamWriter.Messages[0].ApplicationMetadata.ToStringUtf8());
    }

    [Fact]
    public async void EnsureDoPutIsCorrectlyRoutedForTheCommandPreparedStatementUpdate()
    {
        //Given
        var producer = new TestFlightSqlProducer();
        var command = new CommandPreparedStatementUpdate();
        var reader = new MockStreamReader<FlightData>(System.Array.Empty<FlightData>());
        var mockStreamWriter = new MockServerStreamWriter<FlightPutResult>();

        //When
        await producer.DoPut(command, new FlightServerRecordBatchStreamReader(reader), mockStreamWriter, new MockServerCallContext()).ConfigureAwait(false);

        //Then
        Assert.Equal("PutPreparedStatementUpdate", mockStreamWriter.Messages[0].ApplicationMetadata.ToStringUtf8());
    }

    [Fact]
    public async void EnsureAnInvalidOperationExceptionIsThrownWhenADoPutCommandIsNotSupported()
    {
        //Given
        var producer = new TestFlightSqlProducer();
        var command = new CommandGetXdbcTypeInfo();
        var reader = new MockStreamReader<FlightData>(System.Array.Empty<FlightData>());
        var mockStreamWriter = new MockServerStreamWriter<FlightPutResult>();

        //When
        var act = async () => await producer.DoPut(command, new FlightServerRecordBatchStreamReader(reader), mockStreamWriter, new MockServerCallContext()).ConfigureAwait(false);
        var exception = await Record.ExceptionAsync(act);

        //Then
        Assert.Equal("Command CommandGetXdbcTypeInfo not supported", exception?.Message);
    }
    #endregion

    private class MockServerCallContext : ServerCallContext
    {
        protected override Task WriteResponseHeadersAsyncCore(Metadata responseHeaders) => throw new NotImplementedException();

        protected override ContextPropagationToken CreatePropagationTokenCore(ContextPropagationOptions? options) => throw new NotImplementedException();

        protected override string MethodCore { get; }
        protected override string HostCore { get; }
        protected override string PeerCore { get; }
        protected override DateTime DeadlineCore { get; }
        protected override Metadata RequestHeadersCore { get; }
        protected override CancellationToken CancellationTokenCore { get; }
        protected override Metadata ResponseTrailersCore { get; }
        protected override Status StatusCore { get; set; }
        protected override WriteOptions WriteOptionsCore { get; set; }
        protected override AuthContext AuthContextCore { get; }
    }



    private class BadCommand : IMessage
    {
        public BadCommand(MessageDescriptor? descriptor = null)
        {
            Descriptor = descriptor;
        }

        public void MergeFrom(CodedInputStream input) {}

        public void WriteTo(CodedOutputStream output) {}

        public int CalculateSize() => 0;

        public MessageDescriptor Descriptor { get; }
    }
}

internal class MockStreamWriter<T> : IAsyncStreamWriter<T>
{
    public Task WriteAsync(T message)
    {
        messages.Add(message);
        return Task.FromResult(message);
    }

    public IReadOnlyList<T> Messages => new ReadOnlyCollection<T>(messages);
    public WriteOptions? WriteOptions { get; set; }
    private readonly List<T> messages = new();
}

internal class MockServerStreamWriter<T> : IServerStreamWriter<T>
{
    public Task WriteAsync(T message)
    {
        messages.Add(message);
        return Task.FromResult(message);
    }

    public IReadOnlyList<T> Messages => new ReadOnlyCollection<T>(messages);
    public WriteOptions? WriteOptions { get; set; }
    private readonly List<T> messages = new();
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

    public static async Task<Schema> GetSchema(this IReadOnlyList<FlightData> flightDataList)
    {
        var recordBatchReader = new FlightServerRecordBatchStreamReader(new MockStreamReader<FlightData>(flightDataList));
        return await recordBatchReader.Schema;
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
