using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Apache.Arrow.Flight.Client;
using Apache.Arrow.Types;
using Arrow.Flight.Protocol.Sql;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;

namespace Apache.Arrow.Flight.Sql.Client;

public class FlightSqlClient
{
    private readonly FlightClient _client;

    public FlightSqlClient(FlightClient client)
    {
        _client = client;
    }

    /// <summary>
    /// Execute a SQL query on the server.
    /// </summary>
    /// <param name="options">RPC-layer hints for this call.</param>
    /// <param name="query">The UTF8-encoded SQL query to be executed.</param>
    /// <param name="transaction">A transaction to associate this query with.</param>
    /// <returns>The FlightInfo describing where to access the dataset.</returns>
    public async Task<FlightInfo> ExecuteAsync(string query, Transaction? transaction = null, FlightCallOptions? options = default)
    {
        transaction ??= Transaction.NoTransaction;

        if (string.IsNullOrEmpty(query))
        {
            throw new ArgumentException($"Query cannot be null or empty: {nameof(query)}");
        }

        try
        {
            var prepareStatementRequest =
                new ActionCreatePreparedStatementRequest { Query = query, TransactionId = transaction.TransactionId };
            var action = new FlightAction(SqlAction.CreateRequest, prepareStatementRequest.PackAndSerialize());
            var call = _client.DoAction(action, options?.Headers);

            await foreach (var result in call.ResponseStream.ReadAllAsync().ConfigureAwait(false))
            {
                var preparedStatementResponse =
                    FlightSqlUtils.ParseAndUnpack<ActionCreatePreparedStatementResult>(result.Body);
                var commandSqlCall = new CommandPreparedStatementQuery
                {
                    PreparedStatementHandle = preparedStatementResponse.PreparedStatementHandle
                };

                byte[] commandSqlCallPackedAndSerialized = commandSqlCall.PackAndSerialize();
                var descriptor = FlightDescriptor.CreateCommandDescriptor(commandSqlCallPackedAndSerialized);
                return await GetFlightInfoAsync(descriptor, options).ConfigureAwait(false);
            }

            throw new InvalidOperationException("No results returned from the query.");
        }
        catch (RpcException ex)
        {
            throw new InvalidOperationException("Failed to execute query", ex);
        }
    }

    /// <summary>
    /// Executes an update query on the server.
    /// </summary>
    /// <param name="options">RPC-layer hints for this call.</param>
    /// <param name="query">The UTF8-encoded SQL query to be executed.</param>
    /// <param name="transaction">A transaction to associate this query with. Defaults to no transaction if not provided.</param>
    /// <returns>The number of rows affected by the operation.</returns>
    public async Task<long> ExecuteUpdateAsync(string query, Transaction? transaction = null, FlightCallOptions? options = default)
    {
        transaction ??= Transaction.NoTransaction;

        if (string.IsNullOrEmpty(query))
        {
            throw new ArgumentException("Query cannot be null or empty", nameof(query));
        }

        try
        {
            var updateRequestCommand =
                new ActionCreatePreparedStatementRequest { Query = query, TransactionId = transaction.TransactionId };
            byte[] serializedUpdateRequestCommand = updateRequestCommand.PackAndSerialize();
            var action = new FlightAction(SqlAction.CreateRequest, serializedUpdateRequestCommand);
            var call = DoActionAsync(action, options);
            long affectedRows = 0;

            await foreach (var result in call.ConfigureAwait(false))
            {
                var preparedStatementResponse = result.Body.ParseAndUnpack<ActionCreatePreparedStatementResult>();
                var command = new CommandPreparedStatementQuery
                {
                    PreparedStatementHandle = preparedStatementResponse.PreparedStatementHandle
                };

                var descriptor = FlightDescriptor.CreateCommandDescriptor(command.PackAndSerialize());
                var flightInfo = await GetFlightInfoAsync(descriptor, options);
                var doGetResult = DoGetAsync(flightInfo.Endpoints[0].Ticket, options);
                await foreach (var recordBatch in doGetResult.ConfigureAwait(false))
                {
                    affectedRows += recordBatch.Column(0).Length;
                }
            }

            return affectedRows;
        }
        catch (RpcException ex)
        {
            throw new InvalidOperationException("Failed to execute update query", ex);
        }
    }

    /// <summary>
    /// Asynchronously retrieves flight information for a given flight descriptor.
    /// </summary>
    /// <param name="options">RPC-layer hints for this call.</param>
    /// <param name="descriptor">The descriptor of the dataset request, whether a named dataset or a command.</param>
    /// <returns>A task that represents the asynchronous operation. The task result contains the FlightInfo describing where to access the dataset.</returns>
    public async Task<FlightInfo> GetFlightInfoAsync(FlightDescriptor descriptor, FlightCallOptions? options = default)
    {
        if (descriptor is null)
        {
            throw new ArgumentNullException(nameof(descriptor));
        }

        try
        {
            var flightInfoCall = _client.GetInfo(descriptor, options?.Headers);
            var flightInfo = await flightInfoCall.ResponseAsync.ConfigureAwait(false);
            return flightInfo;
        }
        catch (RpcException ex)
        {
            throw new InvalidOperationException("Failed to get flight info", ex);
        }
    }

    /// <summary>
    /// Perform the indicated action, returning an iterator to the stream of results, if any.
    /// </summary>
    /// <param name="options">Per-RPC options</param>
    /// <param name="action">The action to be performed</param>
    /// <returns>An async enumerable of results</returns>
    public async IAsyncEnumerable<FlightResult> DoActionAsync(FlightAction action, FlightCallOptions? options = default)
    {
        if (action is null)
            throw new ArgumentNullException(nameof(action));

        var call = _client.DoAction(action, options?.Headers);

        await foreach (var result in call.ResponseStream.ReadAllAsync().ConfigureAwait(false))
        {
            yield return result;
        }
    }

    /// <summary>
    /// Get the result set schema from the server for the given query.
    /// </summary>
    /// <param name="options">Per-RPC options</param>
    /// <param name="query">The UTF8-encoded SQL query</param>
    /// <param name="transaction">A transaction to associate this query with</param>
    /// <returns>The SchemaResult describing the schema of the result set</returns>
    public async Task<Schema> GetExecuteSchemaAsync(string query, Transaction? transaction = null, FlightCallOptions? options = default)
    {
        transaction ??= Transaction.NoTransaction;

        if (string.IsNullOrEmpty(query))
            throw new ArgumentException($"Query cannot be null or empty: {nameof(query)}");

        FlightInfo schemaResult = null!;
        try
        {
            var prepareStatementRequest =
                new ActionCreatePreparedStatementRequest { Query = query, TransactionId = transaction.TransactionId };
            var action = new FlightAction(SqlAction.CreateRequest, prepareStatementRequest.PackAndSerialize());
            var call = _client.DoAction(action, options?.Headers);

            await foreach (var result in call.ResponseStream.ReadAllAsync().ConfigureAwait(false))
            {
                var preparedStatementResponse =
                    FlightSqlUtils.ParseAndUnpack<ActionCreatePreparedStatementResult>(result.Body);
                var commandSqlCall = new CommandPreparedStatementQuery
                {
                    PreparedStatementHandle = preparedStatementResponse.PreparedStatementHandle
                };
                byte[] commandSqlCallPackedAndSerialized = commandSqlCall.PackAndSerialize();
                var descriptor = FlightDescriptor.CreateCommandDescriptor(commandSqlCallPackedAndSerialized);
                schemaResult = await GetFlightInfoAsync(descriptor, options);
            }

            return schemaResult.Schema;
        }
        catch (RpcException ex)
        {
            throw new InvalidOperationException("Failed to get execute schema", ex);
        }
    }

    /// <summary>
    /// Request a list of catalogs.
    /// </summary>
    /// <param name="options">RPC-layer hints for this call.</param>
    /// <returns>The FlightInfo describing where to access the dataset.</returns>
    public async Task<FlightInfo> GetCatalogsAsync(FlightCallOptions? options = default)
    {
        if (options == null)
        {
            throw new ArgumentNullException(nameof(options));
        }

        try
        {
            var command = new CommandGetCatalogs();
            var descriptor = FlightDescriptor.CreateCommandDescriptor(command.PackAndSerialize());
            var catalogsInfo = await GetFlightInfoAsync(descriptor, options).ConfigureAwait(false);
            return catalogsInfo;
        }
        catch (RpcException ex)
        {
            throw new InvalidOperationException("Failed to get catalogs", ex);
        }
    }

    /// <summary>
    /// Get the catalogs schema from the server.
    /// </summary>
    /// <param name="options">RPC-layer hints for this call.</param>
    /// <returns>The SchemaResult describing the schema of the catalogs.</returns>
    public async Task<Schema> GetCatalogsSchemaAsync(FlightCallOptions? options = default)
    {
        try
        {
            var commandGetCatalogsSchema = new CommandGetCatalogs();
            var descriptor = FlightDescriptor.CreateCommandDescriptor(commandGetCatalogsSchema.PackAndSerialize());
            var schemaResult = await GetSchemaAsync(descriptor, options).ConfigureAwait(false);
            return schemaResult;
        }
        catch (RpcException ex)
        {
            throw new InvalidOperationException("Failed to get catalogs schema", ex);
        }
    }

    /// <summary>
    /// Asynchronously retrieves schema information for a given flight descriptor.
    /// </summary>
    /// <param name="options">RPC-layer hints for this call.</param>
    /// <param name="descriptor">The descriptor of the dataset request, whether a named dataset or a command.</param>
    /// <returns>A task that represents the asynchronous operation. The task result contains the SchemaResult describing the dataset schema.</returns>
    public virtual async Task<Schema> GetSchemaAsync(FlightDescriptor descriptor, FlightCallOptions? options = default)
    {
        if (descriptor is null)
        {
            throw new ArgumentNullException(nameof(descriptor));
        }

        try
        {
            var schemaResultCall = _client.GetSchema(descriptor, options?.Headers);
            var schemaResult = await schemaResultCall.ResponseAsync.ConfigureAwait(false);
            return schemaResult;
        }
        catch (RpcException ex)
        {
            throw new InvalidOperationException("Failed to get schema", ex);
        }
    }

    /// <summary>
    /// Request a list of database schemas.
    /// </summary>
    /// <param name="options">RPC-layer hints for this call.</param>
    /// <param name="catalog">The catalog.</param>
    /// <param name="dbSchemaFilterPattern">The schema filter pattern.</param>
    /// <returns>The FlightInfo describing where to access the dataset.</returns>
    public async Task<FlightInfo> GetDbSchemasAsync(string? catalog = null, string? dbSchemaFilterPattern = null, FlightCallOptions? options = default)
    {
        try
        {
            var command = new CommandGetDbSchemas();

            if (catalog != null)
            {
                command.Catalog = catalog;
            }

            if (dbSchemaFilterPattern != null)
            {
                command.DbSchemaFilterPattern = dbSchemaFilterPattern;
            }

            byte[] serializedAndPackedCommand = command.PackAndSerialize();
            var descriptor = FlightDescriptor.CreateCommandDescriptor(serializedAndPackedCommand);
            var flightInfoCall = GetFlightInfoAsync(descriptor, options);
            var flightInfo = await flightInfoCall.ConfigureAwait(false);

            return flightInfo;
        }
        catch (RpcException ex)
        {
            throw new InvalidOperationException("Failed to get database schemas", ex);
        }
    }

    /// <summary>
    /// Get the database schemas schema from the server.
    /// </summary>
    /// <param name="options">RPC-layer hints for this call.</param>
    /// <returns>The SchemaResult describing the schema of the database schemas.</returns>
    public async Task<Schema> GetDbSchemasSchemaAsync(FlightCallOptions? options = default)
    {
        if (options == null)
        {
            throw new ArgumentNullException(nameof(options));
        }

        try
        {
            var command = new CommandGetDbSchemas();
            var descriptor = FlightDescriptor.CreateCommandDescriptor(command.PackAndSerialize());
            var schemaResult = await GetSchemaAsync(descriptor, options).ConfigureAwait(false);
            return schemaResult;
        }
        catch (RpcException ex)
        {
            throw new InvalidOperationException("Failed to get database schemas schema", ex);
        }
    }

    /// <summary>
    /// Given a flight ticket and schema, request to be sent the stream. Returns record batch stream reader.
    /// </summary>
    /// <param name="options">Per-RPC options</param>
    /// <param name="ticket">The flight ticket to use</param>
    /// <returns>The returned RecordBatchReader</returns>
    public async IAsyncEnumerable<RecordBatch> DoGetAsync(FlightTicket ticket, FlightCallOptions? options = default)
    {
        if (ticket == null)
        {
            throw new ArgumentNullException(nameof(ticket));
        }

        var call = _client.GetStream(ticket, options?.Headers);
        await foreach (var recordBatch in call.ResponseStream.ReadAllAsync().ConfigureAwait(false))
        {
            yield return recordBatch;
        }
    }

    /// <summary>
    /// Upload data to a Flight described by the given descriptor. The caller must call Close() on the returned stream
    /// once they are done writing.
    /// </summary>
    /// <param name="options">RPC-layer hints for this call.</param>
    /// <param name="descriptor">The descriptor of the stream.</param>
    /// <param name="schema">The schema for the data to upload.</param>
    /// <returns>A Task representing the asynchronous operation. The task result contains a DoPutResult struct holding a reader and a writer.</returns>
    public async Task<DoPutResult> DoPutAsync(FlightDescriptor descriptor, Schema schema, FlightCallOptions? options = default)
    {
        if (descriptor is null)
            throw new ArgumentNullException(nameof(descriptor));

        if (schema is null)
            throw new ArgumentNullException(nameof(schema));
        try
        {
            var doPutResult = _client.StartPut(descriptor, options?.Headers);
            var writer = doPutResult.RequestStream;
            var reader = doPutResult.ResponseStream;

            var recordBatch = new RecordBatch(schema, BuildArrowArraysFromSchema(schema, schema.FieldsList.Count), 0);
            await writer.WriteAsync(recordBatch).ConfigureAwait(false);
            await writer.CompleteAsync().ConfigureAwait(false);

            return new DoPutResult(writer, reader);
        }
        catch (RpcException ex)
        {
            throw new InvalidOperationException("Failed to perform DoPut operation", ex);
        }
    }

    public List<IArrowArray> BuildArrowArraysFromSchema(Schema schema, int rowCount)
    {
        var arrays = new List<IArrowArray>();

        foreach (var field in schema.FieldsList)
        {
            switch (field.DataType)
            {
                case Int32Type _:
                    // Create an Int32 array
                    var intArrayBuilder = new Int32Array.Builder();
                    for (int i = 0; i < rowCount; i++)
                    {
                        intArrayBuilder.Append(i); 
                    }

                    arrays.Add(intArrayBuilder.Build());
                    break;

                case StringType:
                    var stringArrayBuilder = new StringArray.Builder();
                    for (int i = 0; i < rowCount; i++)
                    {
                        stringArrayBuilder.Append($"Value-{i}");
                    }

                    arrays.Add(stringArrayBuilder.Build());
                    break;

                case Int64Type:
                    var longArrayBuilder = new Int64Array.Builder();
                    for (int i = 0; i < rowCount; i++)
                    {
                        longArrayBuilder.Append((long)i * 100);
                    }

                    arrays.Add(longArrayBuilder.Build());
                    break;

                case FloatType:
                    var floatArrayBuilder = new FloatArray.Builder();
                    for (int i = 0; i < rowCount; i++)
                    {
                        floatArrayBuilder.Append((float)(i * 1.1));
                    }

                    arrays.Add(floatArrayBuilder.Build());
                    break;

                case BooleanType:
                    var boolArrayBuilder = new BooleanArray.Builder();
                    for (int i = 0; i < rowCount; i++)
                    {
                        boolArrayBuilder.Append(i % 2 == 0);
                    }
                    arrays.Add(boolArrayBuilder.Build());
                    break;

                default:
                    throw new NotSupportedException($"Data type {field.DataType} not supported yet.");
            }
        }

        return arrays;
    }


    /// <summary>
    /// Request the primary keys for a table.
    /// </summary>
    /// <param name="options">RPC-layer hints for this call.</param>
    /// <param name="tableRef">The table reference.</param>
    /// <returns>The FlightInfo describing where to access the dataset.</returns>
    public async Task<FlightInfo> GetPrimaryKeysAsync(TableRef tableRef, FlightCallOptions? options = default)
    {
        if (tableRef == null)
            throw new ArgumentNullException(nameof(tableRef));

        try
        {
            var getPrimaryKeysRequest = new CommandGetPrimaryKeys
            {
                Catalog = tableRef.Catalog ?? string.Empty, DbSchema = tableRef.DbSchema, Table = tableRef.Table
            };
            byte[] packedRequest = getPrimaryKeysRequest.PackAndSerialize();
            var descriptor = FlightDescriptor.CreateCommandDescriptor(packedRequest);
            var flightInfo = await GetFlightInfoAsync(descriptor, options).ConfigureAwait(false);

            return flightInfo;
        }
        catch (RpcException ex)
        {
            throw new InvalidOperationException("Failed to get primary keys", ex);
        }
    }

    /// <summary>
    /// Request a list of tables.
    /// </summary>
    /// <param name="options">RPC-layer hints for this call.</param>
    /// <param name="catalog">The catalog.</param>
    /// <param name="dbSchemaFilterPattern">The schema filter pattern.</param>
    /// <param name="tableFilterPattern">The table filter pattern.</param>
    /// <param name="includeSchema">True to include the schema upon return, false to not include the schema.</param>
    /// <param name="tableTypes">The table types to include.</param>
    /// <returns>The FlightInfo describing where to access the dataset.</returns>
    public async Task<IEnumerable<FlightInfo>> 
        GetTablesAsync(string? catalog = null, string? dbSchemaFilterPattern = null, string? tableFilterPattern = null, bool includeSchema = false, IEnumerable<string>? tableTypes = null, FlightCallOptions? options = default)
    {
        var command = new CommandGetTables
        {
            Catalog = catalog ?? string.Empty,
            DbSchemaFilterPattern = dbSchemaFilterPattern ?? string.Empty,
            TableNameFilterPattern = tableFilterPattern ?? string.Empty,
            IncludeSchema = includeSchema
        };

        if (tableTypes != null)
        {
            command.TableTypes.AddRange(tableTypes);
        }

        var descriptor = FlightDescriptor.CreateCommandDescriptor(command.PackAndSerialize());
        var flightInfoCall = GetFlightInfoAsync(descriptor, options);
        var flightInfo = await flightInfoCall.ConfigureAwait(false);
        var flightInfos = new List<FlightInfo> { flightInfo };

        return flightInfos;
    }


    /// <summary>
    /// Retrieves a description about the foreign key columns that reference the primary key columns of the given table.
    /// </summary>
    /// <param name="options">RPC-layer hints for this call.</param>
    /// <param name="tableRef">The table reference.</param>
    /// <returns>The FlightInfo describing where to access the dataset.</returns>
    public async Task<FlightInfo> GetExportedKeysAsync(TableRef tableRef, FlightCallOptions? options = default)
    {
        if (tableRef == null)
            throw new ArgumentNullException(nameof(tableRef));

        try
        {
            var getExportedKeysRequest = new CommandGetExportedKeys
            {
                Catalog = tableRef.Catalog ?? string.Empty, DbSchema = tableRef.DbSchema, Table = tableRef.Table
            };

            var descriptor = FlightDescriptor.CreateCommandDescriptor(getExportedKeysRequest.PackAndSerialize());
            var flightInfo = await GetFlightInfoAsync(descriptor, options).ConfigureAwait(false);
            return flightInfo;
        }
        catch (RpcException ex)
        {
            throw new InvalidOperationException("Failed to get exported keys", ex);
        }
    }

    /// <summary>
    /// Get the exported keys schema from the server.
    /// </summary>
    /// <param name="options">RPC-layer hints for this call.</param>
    /// <returns>The SchemaResult describing the schema of the exported keys.</returns>
    public async Task<Schema> GetExportedKeysSchemaAsync(FlightCallOptions? options = default)
    {
        if (options == null)
        {
            throw new ArgumentNullException(nameof(options));
        }

        try
        {
            var commandGetExportedKeysSchema = new CommandGetExportedKeys();
            var descriptor = FlightDescriptor.CreateCommandDescriptor(commandGetExportedKeysSchema.PackAndSerialize());
            var schemaResult = await GetSchemaAsync(descriptor, options).ConfigureAwait(false);
            return schemaResult;
        }
        catch (RpcException ex)
        {
            throw new InvalidOperationException("Failed to get exported keys schema", ex);
        }
    }

    /// <summary>
    /// Retrieves the foreign key columns for the given table.
    /// </summary>
    /// <param name="options">RPC-layer hints for this call.</param>
    /// <param name="tableRef">The table reference.</param>
    /// <returns>The FlightInfo describing where to access the dataset.</returns>
    public async Task<FlightInfo> GetImportedKeysAsync(TableRef tableRef, FlightCallOptions? options = default)
    {
        if (tableRef == null)
            throw new ArgumentNullException(nameof(tableRef));

        try
        {
            var getImportedKeysRequest = new CommandGetImportedKeys
            {
                Catalog = tableRef.Catalog ?? string.Empty, DbSchema = tableRef.DbSchema, Table = tableRef.Table
            };
            var descriptor = FlightDescriptor.CreateCommandDescriptor(getImportedKeysRequest.PackAndSerialize());
            var flightInfo = await GetFlightInfoAsync(descriptor, options).ConfigureAwait(false);
            return flightInfo;
        }
        catch (RpcException ex)
        {
            throw new InvalidOperationException("Failed to get imported keys", ex);
        }
    }

    /// <summary>
    /// Get the imported keys schema from the server.
    /// </summary>
    /// <param name="options">RPC-layer hints for this call.</param>
    /// <returns>The SchemaResult describing the schema of the imported keys.</returns>
    public async Task<Schema> GetImportedKeysSchemaAsync(FlightCallOptions? options = default)
    {
        if (options == null)
        {
            throw new ArgumentNullException(nameof(options));
        }

        try
        {
            var commandGetImportedKeysSchema = new CommandGetImportedKeys();
            var descriptor = FlightDescriptor.CreateCommandDescriptor(commandGetImportedKeysSchema.PackAndSerialize());
            var schemaResult = await GetSchemaAsync(descriptor, options).ConfigureAwait(false);
            return schemaResult;
        }
        catch (RpcException ex)
        {
            throw new InvalidOperationException("Failed to get imported keys schema", ex);
        }
    }

    /// <summary>
    /// Retrieves a description of the foreign key columns in the given foreign key table that reference the primary key or the columns representing a unique constraint of the parent table.
    /// </summary>
    /// <param name="options">RPC-layer hints for this call.</param>
    /// <param name="pkTableRef">The table reference that exports the key.</param>
    /// <param name="fkTableRef">The table reference that imports the key.</param>
    /// <returns>The FlightInfo describing where to access the dataset.</returns>
    public async Task<FlightInfo> GetCrossReferenceAsync(TableRef pkTableRef, TableRef fkTableRef, FlightCallOptions? options = default)
    {
        if (pkTableRef == null)
            throw new ArgumentNullException(nameof(pkTableRef));

        if (fkTableRef == null)
            throw new ArgumentNullException(nameof(fkTableRef));

        try
        {
            var commandGetCrossReference = new CommandGetCrossReference
            {
                PkCatalog = pkTableRef.Catalog ?? string.Empty,
                PkDbSchema = pkTableRef.DbSchema,
                PkTable = pkTableRef.Table,
                FkCatalog = fkTableRef.Catalog ?? string.Empty,
                FkDbSchema = fkTableRef.DbSchema,
                FkTable = fkTableRef.Table
            };

            var descriptor = FlightDescriptor.CreateCommandDescriptor(commandGetCrossReference.PackAndSerialize());
            var flightInfo = await GetFlightInfoAsync(descriptor, options).ConfigureAwait(false);

            return flightInfo;
        }
        catch (RpcException ex)
        {
            throw new InvalidOperationException("Failed to get cross reference", ex);
        }
    }

    /// <summary>
    /// Get the cross-reference schema from the server.
    /// </summary>
    /// <param name="options">RPC-layer hints for this call.</param>
    /// <returns>The SchemaResult describing the schema of the cross-reference.</returns>
    public async Task<Schema> GetCrossReferenceSchemaAsync(FlightCallOptions? options = default)
    {
        if (options == null)
        {
            throw new ArgumentNullException(nameof(options));
        }

        try
        {
            var commandGetCrossReferenceSchema = new CommandGetCrossReference();
            var descriptor =
                FlightDescriptor.CreateCommandDescriptor(commandGetCrossReferenceSchema.PackAndSerialize());
            var schemaResultCall = GetSchemaAsync(descriptor, options);
            var schemaResult = await schemaResultCall.ConfigureAwait(false);

            return schemaResult;
        }
        catch (RpcException ex)
        {
            throw new InvalidOperationException("Failed to get cross-reference schema", ex);
        }
    }

    /// <summary>
    /// Request a list of table types.
    /// </summary>
    /// <param name="options">RPC-layer hints for this call.</param>
    /// <returns>The FlightInfo describing where to access the dataset.</returns>
    public async Task<FlightInfo> GetTableTypesAsync(FlightCallOptions? options = default)
    {
        if (options == null)
        {
            throw new ArgumentNullException(nameof(options));
        }

        try
        {
            var command = new CommandGetTableTypes();
            var descriptor = FlightDescriptor.CreateCommandDescriptor(command.PackAndSerialize());
            var flightInfo = await GetFlightInfoAsync(descriptor, options).ConfigureAwait(false);
            return flightInfo;
        }
        catch (RpcException ex)
        {
            throw new InvalidOperationException("Failed to get table types", ex);
        }
    }

    /// <summary>
    /// Get the table types schema from the server.
    /// </summary>
    /// <param name="options">RPC-layer hints for this call.</param>
    /// <returns>The SchemaResult describing the schema of the table types.</returns>
    public async Task<Schema> GetTableTypesSchemaAsync(FlightCallOptions? options = default)
    {
        if (options == null)
        {
            throw new ArgumentNullException(nameof(options));
        }

        try
        {
            var command = new CommandGetTableTypes();
            var descriptor = FlightDescriptor.CreateCommandDescriptor(command.PackAndSerialize());
            var schemaResult = await GetSchemaAsync(descriptor, options).ConfigureAwait(false);
            return schemaResult;
        }
        catch (RpcException ex)
        {
            throw new InvalidOperationException("Failed to get table types schema", ex);
        }
    }

    /// <summary>
    /// Request the information about all the data types supported with filtering by data type.
    /// </summary>
    /// <param name="options">RPC-layer hints for this call.</param>
    /// <param name="dataType">The data type to search for as filtering.</param>
    /// <returns>The FlightInfo describing where to access the dataset.</returns>
    public async Task<FlightInfo> GetXdbcTypeInfoAsync(int dataType, FlightCallOptions? options = default)
    {
        if (options == null)
        {
            throw new ArgumentNullException(nameof(options));
        }

        try
        {
            var command = new CommandGetXdbcTypeInfo { DataType = dataType };
            var descriptor = FlightDescriptor.CreateCommandDescriptor(command.PackAndSerialize());
            var flightInfo = await GetFlightInfoAsync(descriptor, options).ConfigureAwait(false);
            return flightInfo;
        }
        catch (RpcException ex)
        {
            throw new InvalidOperationException("Failed to get XDBC type info", ex);
        }
    }

    /// <summary>
    /// Request the information about all the data types supported.
    /// </summary>
    /// <param name="options">RPC-layer hints for this call.</param>
    /// <returns>The FlightInfo describing where to access the dataset.</returns>
    public async Task<FlightInfo> GetXdbcTypeInfoAsync(FlightCallOptions? options = default)
    {
        if (options == null)
        {
            throw new ArgumentNullException(nameof(options));
        }

        try
        {
            var command = new CommandGetXdbcTypeInfo();
            var descriptor = FlightDescriptor.CreateCommandDescriptor(command.PackAndSerialize());
            var flightInfo = await GetFlightInfoAsync(descriptor, options).ConfigureAwait(false);
            return flightInfo;
        }
        catch (RpcException ex)
        {
            throw new InvalidOperationException("Failed to get XDBC type info", ex);
        }
    }

    /// <summary>
    /// Get the type info schema from the server.
    /// </summary>
    /// <param name="options">RPC-layer hints for this call.</param>
    /// <returns>The SchemaResult describing the schema of the type info.</returns>
    public async Task<Schema> GetXdbcTypeInfoSchemaAsync(FlightCallOptions? options = default)
    {
        if (options == null)
        {
            throw new ArgumentNullException(nameof(options));
        }

        try
        {
            var command = new CommandGetXdbcTypeInfo();
            var descriptor = FlightDescriptor.CreateCommandDescriptor(command.PackAndSerialize());
            var schemaResult = await GetSchemaAsync(descriptor, options).ConfigureAwait(false);
            return schemaResult;
        }
        catch (RpcException ex)
        {
            throw new InvalidOperationException("Failed to get XDBC type info schema", ex);
        }
    }

    /// <summary>
    /// Request a list of SQL information.
    /// </summary>
    /// <param name="options">RPC-layer hints for this call.</param>
    /// <param name="sqlInfo">The SQL info required.</param>
    /// <returns>The FlightInfo describing where to access the dataset.</returns>
    public async Task<FlightInfo> GetSqlInfoAsync(List<int> sqlInfo, FlightCallOptions? options = default)
    {
        if (options == null)
        {
            throw new ArgumentNullException(nameof(options));
        }

        if (sqlInfo == null || sqlInfo.Count == 0)
        {
            throw new ArgumentException("SQL info list cannot be null or empty", nameof(sqlInfo));
        }

        try
        {
            var command = new CommandGetSqlInfo();
            command.Info.AddRange(sqlInfo.ConvertAll(item => (uint)item));
            var descriptor = FlightDescriptor.CreateCommandDescriptor(command.PackAndSerialize());
            var flightInfo = await GetFlightInfoAsync(descriptor, options).ConfigureAwait(false);
            return flightInfo;
        }
        catch (RpcException ex)
        {
            throw new InvalidOperationException("Failed to get SQL info", ex);
        }
    }

    /// <summary>
    /// Get the SQL information schema from the server.
    /// </summary>
    /// <param name="options">RPC-layer hints for this call.</param>
    /// <returns>The SchemaResult describing the schema of the SQL information.</returns>
    public async Task<Schema> GetSqlInfoSchemaAsync(FlightCallOptions? options = default)
    {
        if (options == null)
        {
            throw new ArgumentNullException(nameof(options));
        }

        try
        {
            var command = new CommandGetSqlInfo();
            var descriptor = FlightDescriptor.CreateCommandDescriptor(command.PackAndSerialize());
            var schemaResultCall = _client.GetSchema(descriptor, options.Headers);
            var schemaResult = await schemaResultCall.ResponseAsync.ConfigureAwait(false);

            return schemaResult;
        }
        catch (RpcException ex)
        {
            throw new InvalidOperationException("Failed to get SQL info schema", ex);
        }
    }

    /// <summary>
    /// Explicitly cancel a FlightInfo.
    /// </summary>
    /// <param name="options">RPC-layer hints for this call.</param>
    /// <param name="request">The CancelFlightInfoRequest.</param>
    /// <returns>A Task representing the asynchronous operation. The task result contains the CancelFlightInfoResult describing the canceled result.</returns>
    public async Task<FlightInfoCancelResult> CancelFlightInfoAsync(FlightInfoCancelRequest request, FlightCallOptions? options = default)
    {
        if (request == null) throw new ArgumentNullException(nameof(request));

        try
        {
            var action = new FlightAction(SqlAction.CancelFlightInfoRequest, request.PackAndSerialize());
            var call = _client.DoAction(action, options?.Headers);
            await foreach (var result in call.ResponseStream.ReadAllAsync().ConfigureAwait(false))
            {
                if (Any.Parser.ParseFrom(result.Body) is Any anyResult &&
                    anyResult.TryUnpack(out FlightInfoCancelResult cancelResult))
                {
                    return cancelResult;
                }
            }

            throw new InvalidOperationException("No response received for the CancelFlightInfo request.");
        }
        catch (RpcException ex)
        {
            throw new InvalidOperationException("Failed to cancel flight info", ex);
        }
    }

    /// <summary>
    /// Explicitly cancel a query.
    /// </summary>
    /// <param name="options">RPC-layer hints for this call.</param>
    /// <param name="info">The FlightInfo of the query to cancel.</param>
    /// <returns>A Task representing the asynchronous operation.</returns>
    public async Task<FlightInfoCancelResult> CancelQueryAsync(FlightInfo info, FlightCallOptions? options = default)
    {
        if (info == null)
            throw new ArgumentNullException(nameof(info));

        try
        {
            var cancelQueryRequest = new FlightInfoCancelRequest(info);
            var cancelQueryAction =
                new FlightAction(SqlAction.CancelFlightInfoRequest, cancelQueryRequest.PackAndSerialize());
            var cancelQueryCall = _client.DoAction(cancelQueryAction, options?.Headers);

            await foreach (var result in cancelQueryCall.ResponseStream.ReadAllAsync().ConfigureAwait(false))
            {
                if (Any.Parser.ParseFrom(result.Body) is Any anyResult &&
                    anyResult.TryUnpack(out FlightInfoCancelResult cancelResult))
                {
                    return cancelResult;
                }
            }
            throw new InvalidOperationException("Failed to cancel query: No response received.");
        }
        catch (RpcException ex)
        {
            throw new InvalidOperationException("Failed to cancel query", ex);
        }
    }

    /// <summary>
    /// Begin a new transaction.
    /// </summary>
    /// <param name="options">RPC-layer hints for this call.</param>
    /// <returns>A Task representing the asynchronous operation. The task result contains the Transaction object representing the new transaction.</returns>
    public async Task<Transaction> BeginTransactionAsync(FlightCallOptions? options = default)
    {
        try
        {
            var actionBeginTransaction = new ActionBeginTransactionRequest();
            var action = new FlightAction(SqlAction.BeginTransactionRequest, actionBeginTransaction.PackAndSerialize());
            var responseStream = _client.DoAction(action, options?.Headers);
            await foreach (var result in responseStream.ResponseStream.ReadAllAsync().ConfigureAwait(false))
            {
                string? beginTransactionResult = result.Body.ToStringUtf8();
                return new Transaction(beginTransactionResult);
            }

            throw new InvalidOperationException("Failed to begin transaction: No response received.");
        }
        catch (RpcException ex)
        {
            throw new InvalidOperationException("Failed to begin transaction", ex);
        }
    }

    /// <summary>
    /// Commit a transaction.
    /// After this, the transaction and all associated savepoints will be invalidated.
    /// </summary>
    /// <param name="options">RPC-layer hints for this call.</param>
    /// <param name="transaction">The transaction.</param>
    /// <returns>A Task representing the asynchronous operation.</returns>
    public AsyncServerStreamingCall<FlightResult> CommitAsync(Transaction transaction, FlightCallOptions? options = default)
    {
        if (transaction == null)
            throw new ArgumentNullException(nameof(transaction));

        try
        {
            var actionCommit = new FlightAction(SqlAction.CommitRequest, transaction.TransactionId);
            return _client.DoAction(actionCommit, options?.Headers);
        }
        catch (RpcException ex)
        {
            throw new InvalidOperationException("Failed to commit transaction", ex);
        }
    }

    /// <summary>
    /// Rollback a transaction.
    /// After this, the transaction and all associated savepoints will be invalidated.
    /// </summary>
    /// <param name="options">RPC-layer hints for this call.</param>
    /// <param name="transaction">The transaction to rollback.</param>
    /// <returns>A Task representing the asynchronous operation.</returns>
    public AsyncServerStreamingCall<FlightResult> RollbackAsync(Transaction transaction, FlightCallOptions? options = default)
    {
        if (transaction == null)
        {
            throw new ArgumentNullException(nameof(transaction));
        }

        try
        {
            var actionRollback = new FlightAction(SqlAction.RollbackRequest, transaction.TransactionId);
            return _client.DoAction(actionRollback, options?.Headers);
        }
        catch (RpcException ex)
        {
            throw new InvalidOperationException("Failed to rollback transaction", ex);
        }
    }

    /// <summary>
    /// Create a prepared statement object.
    /// </summary>
    /// <param name="options">RPC-layer hints for this call.</param>
    /// <param name="query">The query that will be executed.</param>
    /// <param name="transaction">A transaction to associate this query with.</param>
    /// <returns>The created prepared statement.</returns>
    public async Task<PreparedStatement> PrepareAsync(string query, Transaction? transaction = null, FlightCallOptions? options = default)
    {
        transaction ??= Transaction.NoTransaction;

        if (string.IsNullOrEmpty(query))
        {
            throw new ArgumentException("Query cannot be null or empty", nameof(query));
        }

        try
        {
            var preparedStatementRequest = new ActionCreatePreparedStatementRequest
            {
                Query = query, TransactionId = transaction.TransactionId
            };

            var action = new FlightAction(SqlAction.CreateRequest, preparedStatementRequest.PackAndSerialize());
            var call = _client.DoAction(action, options?.Headers);

            await foreach (var result in call.ResponseStream.ReadAllAsync())
            {
                var preparedStatementResponse =
                    FlightSqlUtils.ParseAndUnpack<ActionCreatePreparedStatementResult>(result.Body);

                var commandSqlCall = new CommandPreparedStatementQuery
                {
                    PreparedStatementHandle = preparedStatementResponse.PreparedStatementHandle
                };
                byte[] commandSqlCallPackedAndSerialized = commandSqlCall.PackAndSerialize();
                var descriptor = FlightDescriptor.CreateCommandDescriptor(commandSqlCallPackedAndSerialized);
                var flightInfo = await GetFlightInfoAsync(descriptor, options).ConfigureAwait(false);
                return new PreparedStatement(this, transaction.TransactionId.ToStringUtf8(), flightInfo.Schema, flightInfo.Schema);
            }

            throw new NullReferenceException($"{nameof(PreparedStatement)} was not able to be created");
        }
        catch (RpcException ex)
        {
            throw new InvalidOperationException("Failed to prepare statement", ex);
        }
    }
}

internal static class FlightDescriptorExtensions
{
    public static byte[] PackAndSerialize(this IMessage command)
    {
        return Any.Pack(command).Serialize().ToByteArray();
    }

    public static T ParseAndUnpack<T>(this ByteString source) where T : IMessage<T>, new()
    {
        return Any.Parser.ParseFrom(source).Unpack<T>();
    }
}
