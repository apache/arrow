using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Apache.Arrow.Flight.Client;
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
        _client = client ?? throw new ArgumentNullException(nameof(client));
    }

    public static Transaction NoTransaction() => new(null);

    /// <summary>
    /// Execute a SQL query on the server.
    /// </summary>
    /// <param name="options">RPC-layer hints for this call.</param>
    /// <param name="query">The UTF8-encoded SQL query to be executed.</param>
    /// <param name="transaction">A transaction to associate this query with.</param>
    /// <returns>The FlightInfo describing where to access the dataset.</returns>
    public async Task<FlightInfo> ExecuteAsync(FlightCallOptions options, string query, Transaction? transaction = null)
    {
        transaction ??= NoTransaction();

        if (options == null)
        {
            throw new ArgumentNullException(nameof(options));
        }

        if (string.IsNullOrEmpty(query))
        {
            throw new ArgumentException($"Query cannot be null or empty: {nameof(query)}");
        }

        try
        {
            var prepareStatementRequest = new ActionCreatePreparedStatementRequest { Query = query };
            var action = new FlightAction(SqlAction.CreateRequest, prepareStatementRequest.PackAndSerialize());
            var call = _client.DoAction(action, options.Headers);

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
                return await GetFlightInfoAsync(options, descriptor);
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
    public async Task<long> ExecuteUpdateAsync(FlightCallOptions options, string query, Transaction? transaction = null)
    {
        transaction ??= NoTransaction();

        if (options == null)
        {
            throw new ArgumentNullException(nameof(options));
        }

        if (string.IsNullOrEmpty(query))
        {
            throw new ArgumentException("Query cannot be null or empty", nameof(query));
        }

        try
        {
            var updateRequestCommand = new ActionCreatePreparedStatementRequest { Query = query };
            byte[] serializedUpdateRequestCommand = updateRequestCommand.PackAndSerialize();
            var action = new FlightAction(SqlAction.CreateRequest, serializedUpdateRequestCommand);
            var call = DoActionAsync(options, action);
            long affectedRows = 0;

            await foreach (var result in call)
            {
                var preparedStatementResponse = result.Body.ParseAndUnpack<ActionCreatePreparedStatementResult>();
                var command = new CommandPreparedStatementQuery
                {
                    PreparedStatementHandle = preparedStatementResponse.PreparedStatementHandle
                };

                var descriptor = FlightDescriptor.CreateCommandDescriptor(command.PackAndSerialize());
                var flightInfo = await GetFlightInfoAsync(options, descriptor);
                var doGetResult = DoGetAsync(options, flightInfo.Endpoints[0].Ticket);
                await foreach (var recordBatch in doGetResult)
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
    public async Task<FlightInfo> GetFlightInfoAsync(FlightCallOptions options, FlightDescriptor descriptor)
    {
        if (descriptor is null)
        {
            throw new ArgumentNullException(nameof(descriptor));
        }

        try
        {
            var flightInfoCall = _client.GetInfo(descriptor, options.Headers);
            var flightInfo = await flightInfoCall.ResponseAsync.ConfigureAwait(false);
            return flightInfo;
        }
        catch (RpcException ex)
        {
            throw new InvalidOperationException("Failed to get flight info", ex);
        }
    }

    /// <summary>
    /// Asynchronously retrieves flight information for a given flight descriptor.
    /// </summary>
    /// <param name="descriptor">The descriptor of the dataset request, whether a named dataset or a command.</param>
    /// <returns>A task that represents the asynchronous operation. The task result contains the FlightInfo describing where to access the dataset.</returns>
    public Task<FlightInfo> GetFlightInfoAsync(FlightDescriptor descriptor)
    {
        var options = new FlightCallOptions();
        return GetFlightInfoAsync(options, descriptor);
    }

    /// <summary>
    /// Perform the indicated action, returning an iterator to the stream of results, if any.
    /// </summary>
    /// <param name="options">Per-RPC options</param>
    /// <param name="action">The action to be performed</param>
    /// <returns>An async enumerable of results</returns>
    public async IAsyncEnumerable<FlightResult> DoActionAsync(FlightCallOptions options, FlightAction action)
    {
        if (options is null)
            throw new ArgumentNullException(nameof(options));

        if (action is null)
            throw new ArgumentNullException(nameof(action));

        var call = _client.DoAction(action, options.Headers);

        await foreach (var result in call.ResponseStream.ReadAllAsync())
        {
            yield return result;
        }
    }

    /// <summary>
    /// Perform the indicated action with default options, returning an iterator to the stream of results, if any.
    /// </summary>
    /// <param name="action">The action to be performed</param>
    /// <returns>An async enumerable of results</returns>
    public async IAsyncEnumerable<FlightResult> DoActionAsync(FlightAction action)
    {
        await foreach (var result in DoActionAsync(new FlightCallOptions(), action))
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
    public async Task<Schema> GetExecuteSchemaAsync(FlightCallOptions options, string query,
        Transaction? transaction = null)
    {
        transaction ??= NoTransaction();

        if (options is null)
            throw new ArgumentNullException(nameof(options));

        if (string.IsNullOrEmpty(query))
            throw new ArgumentException($"Query cannot be null or empty: {nameof(query)}");

        FlightInfo schemaResult = null!;
        try
        {
            var prepareStatementRequest = new ActionCreatePreparedStatementRequest { Query = query };
            var action = new FlightAction(SqlAction.CreateRequest, prepareStatementRequest.PackAndSerialize());
            var call = _client.DoAction(action, options.Headers);

            // Process the response
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
                schemaResult = await GetFlightInfoAsync(options, descriptor);
            }

            return schemaResult.Schema;
        }
        catch (RpcException ex)
        {
            // Handle gRPC exceptions
            Console.WriteLine($@"gRPC Error: {ex.Status.Detail}");
            throw new InvalidOperationException("Failed to get execute schema", ex);
        }
        catch (Exception ex)
        {
            // Handle other exceptions
            Console.WriteLine($@"Unexpected Error: {ex.Message}");
            throw;
        }
    }

    /// <summary>
    /// Request a list of catalogs.
    /// </summary>
    /// <param name="options">RPC-layer hints for this call.</param>
    /// <returns>The FlightInfo describing where to access the dataset.</returns>
    public async Task<FlightInfo> GetCatalogs(FlightCallOptions options)
    {
        if (options == null)
        {
            throw new ArgumentNullException(nameof(options));
        }

        try
        {
            var command = new CommandGetCatalogs();
            var descriptor = FlightDescriptor.CreateCommandDescriptor(command.PackAndSerialize());
            var catalogsInfo = await GetFlightInfoAsync(options, descriptor);
            return catalogsInfo;
        }
        catch (RpcException ex)
        {
            Console.WriteLine($@"gRPC Error: {ex.Status}");
            throw new InvalidOperationException("Failed to get catalogs", ex);
        }
        catch (Exception ex)
        {
            Console.WriteLine($@"Unexpected Error: {ex.Message}");
            throw;
        }
    }

    /// <summary>
    /// Get the catalogs schema from the server.
    /// </summary>
    /// <param name="options">RPC-layer hints for this call.</param>
    /// <returns>The SchemaResult describing the schema of the catalogs.</returns>
    public async Task<Schema> GetCatalogsSchema(FlightCallOptions options)
    {
        if (options == null)
        {
            throw new ArgumentNullException(nameof(options));
        }

        try
        {
            var commandGetCatalogsSchema = new CommandGetCatalogs();
            var descriptor = FlightDescriptor.CreateCommandDescriptor(commandGetCatalogsSchema.PackAndSerialize());
            var schemaResult = await GetSchemaAsync(options, descriptor);
            return schemaResult;
        }
        catch (RpcException ex)
        {
            Console.WriteLine($@"gRPC Error: {ex.Status}");
            throw new InvalidOperationException("Failed to get catalogs schema", ex);
        }
        catch (Exception ex)
        {
            Console.WriteLine($@"Unexpected Error: {ex.Message}");
            throw;
        }
    }

    /// <summary>
    /// Asynchronously retrieves schema information for a given flight descriptor.
    /// </summary>
    /// <param name="options">RPC-layer hints for this call.</param>
    /// <param name="descriptor">The descriptor of the dataset request, whether a named dataset or a command.</param>
    /// <returns>A task that represents the asynchronous operation. The task result contains the SchemaResult describing the dataset schema.</returns>
    public async Task<Schema> GetSchemaAsync(FlightCallOptions options, FlightDescriptor descriptor)
    {
        if (descriptor is null)
        {
            throw new ArgumentNullException(nameof(descriptor));
        }

        try
        {
            var schemaResultCall = _client.GetSchema(descriptor, options.Headers);
            var schemaResult = await schemaResultCall.ResponseAsync.ConfigureAwait(false);
            return schemaResult;
        }
        catch (RpcException ex)
        {
            // Handle gRPC exceptions
            Console.WriteLine($@"gRPC Error: {ex.Status.Detail}");
            throw new InvalidOperationException("Failed to get schema", ex);
        }
        catch (Exception ex)
        {
            // Handle other exceptions
            Console.WriteLine($@"Unexpected Error: {ex.Message}");
            throw;
        }
    }

    /// <summary>
    /// Asynchronously retrieves schema information for a given flight descriptor.
    /// </summary>
    /// <param name="descriptor">The descriptor of the dataset request, whether a named dataset or a command.</param>
    /// <returns>A task that represents the asynchronous operation. The task result contains the SchemaResult describing the dataset schema.</returns>
    public Task<Schema> GetSchemaAsync(FlightDescriptor descriptor)
    {
        var options = new FlightCallOptions();
        return GetSchemaAsync(options, descriptor);
    }

    /// <summary>
    /// Request a list of database schemas.
    /// </summary>
    /// <param name="options">RPC-layer hints for this call.</param>
    /// <param name="catalog">The catalog.</param>
    /// <param name="dbSchemaFilterPattern">The schema filter pattern.</param>
    /// <returns>The FlightInfo describing where to access the dataset.</returns>
    public async Task<FlightInfo> GetDbSchemasAsync(FlightCallOptions options, string? catalog = null,
        string? dbSchemaFilterPattern = null)
    {
        if (options == null)
        {
            throw new ArgumentNullException(nameof(options));
        }

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
            var flightInfoCall = GetFlightInfoAsync(options, descriptor);
            var flightInfo = await flightInfoCall.ConfigureAwait(false);

            return flightInfo;
        }
        catch (RpcException ex)
        {
            Console.WriteLine($@"gRPC Error: {ex.Status.Detail}");
            throw new InvalidOperationException("Failed to get database schemas", ex);
        }
        catch (Exception ex)
        {
            Console.WriteLine($@"Unexpected Error: {ex.Message}");
            throw;
        }
    }

    /// <summary>
    /// Get the database schemas schema from the server.
    /// </summary>
    /// <param name="options">RPC-layer hints for this call.</param>
    /// <returns>The SchemaResult describing the schema of the database schemas.</returns>
    public async Task<Schema> GetDbSchemasSchemaAsync(FlightCallOptions options)
    {
        if (options == null)
        {
            throw new ArgumentNullException(nameof(options));
        }

        try
        {
            var command = new CommandGetDbSchemas();
            var descriptor = FlightDescriptor.CreateCommandDescriptor(command.PackAndSerialize());

            var schemaResultCall = _client.GetSchema(descriptor, options.Headers);
            var schemaResult = await schemaResultCall.ResponseAsync.ConfigureAwait(false);

            return schemaResult;
        }
        catch (RpcException ex)
        {
            Console.WriteLine($@"gRPC Error: {ex.Status.Detail}");
            throw new InvalidOperationException("Failed to get database schemas schema", ex);
        }
        catch (Exception ex)
        {
            Console.WriteLine($@"Unexpected Error: {ex.Message}");
            throw;
        }
    }

    /// <summary>
    /// Given a flight ticket and schema, request to be sent the stream. Returns record batch stream reader.
    /// </summary>
    /// <param name="options">Per-RPC options</param>
    /// <param name="ticket">The flight ticket to use</param>
    /// <returns>The returned RecordBatchReader</returns>
    public async IAsyncEnumerable<RecordBatch> DoGetAsync(FlightCallOptions options, FlightTicket ticket)
    {
        if (options == null)
        {
            throw new ArgumentNullException(nameof(options));
        }

        if (ticket == null)
        {
            throw new ArgumentNullException(nameof(ticket));
        }

        var call = _client.GetStream(ticket, options.Headers);
        await foreach (var recordBatch in call.ResponseStream.ReadAllAsync())
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
    public Task<DoPutResult> DoPut(FlightCallOptions options, FlightDescriptor descriptor, Schema schema)
    {
        if (descriptor is null)
            throw new ArgumentNullException(nameof(descriptor));

        if (schema is null)
            throw new ArgumentNullException(nameof(schema));
        try
        {
            var doPutResult = _client.StartPut(descriptor, options.Headers);
            // Get the writer and reader
            var writer = doPutResult.RequestStream;
            var reader = doPutResult.ResponseStream;

            // TODO: After Re-Check it with Jeremy
            // Create an empty RecordBatch to begin the writer with the schema
            // var emptyRecordBatch = new RecordBatch(schema, new List<IArrowArray>(), 0);
            // await writer.WriteAsync(emptyRecordBatch);

            // Begin the writer with the schema
            return Task.FromResult(new DoPutResult(writer, reader));
        }
        catch (RpcException ex)
        {
            // Handle gRPC exceptions
            Console.WriteLine($@"gRPC Error: {ex.Status.Detail}");
            throw new InvalidOperationException("Failed to perform DoPut operation", ex);
        }
        catch (Exception ex)
        {
            // Handle other exceptions
            Console.WriteLine($@"Unexpected Error: {ex.Message}");
            throw;
        }
    }

    /// <summary>
    /// Upload data to a Flight described by the given descriptor. The caller must call Close() on the returned stream
    /// once they are done writing. Uses default options.
    /// </summary>
    /// <param name="descriptor">The descriptor of the stream.</param>
    /// <param name="schema">The schema for the data to upload.</param>
    /// <returns>A Task representing the asynchronous operation. The task result contains a DoPutResult struct holding a reader and a writer.</returns>
    public Task<DoPutResult> DoPutAsync(FlightDescriptor descriptor, Schema schema)
    {
        return DoPut(new FlightCallOptions(), descriptor, schema);
    }

    /// <summary>
    /// Request the primary keys for a table.
    /// </summary>
    /// <param name="options">RPC-layer hints for this call.</param>
    /// <param name="tableRef">The table reference.</param>
    /// <returns>The FlightInfo describing where to access the dataset.</returns>
    public async Task<FlightInfo> GetPrimaryKeys(FlightCallOptions options, TableRef tableRef)
    {
        if (tableRef == null)
            throw new ArgumentNullException(nameof(tableRef));

        try
        {
            var getPrimaryKeysRequest = new CommandGetPrimaryKeys
            {
                Catalog = tableRef.Catalog ?? string.Empty, DbSchema = tableRef.DbSchema, Table = tableRef.Table
            };
            var action = new FlightAction("GetPrimaryKeys", getPrimaryKeysRequest.PackAndSerialize());
            var doActionResult = DoActionAsync(options, action);
            await foreach (var result in doActionResult)
            {
                var getPrimaryKeysResponse =
                    result.Body.ParseAndUnpack<ActionCreatePreparedStatementResult>();
                var command = new CommandPreparedStatementQuery
                {
                    PreparedStatementHandle = getPrimaryKeysResponse.PreparedStatementHandle
                };

                var descriptor = FlightDescriptor.CreateCommandDescriptor(command.PackAndSerialize());
                var flightInfo = await GetFlightInfoAsync(options, descriptor);
                return flightInfo;
            }

            throw new InvalidOperationException("Failed to retrieve primary keys information.");
        }
        catch (RpcException ex)
        {
            Console.WriteLine($@"gRPC Error: {ex.Status.Detail}");
            throw new InvalidOperationException("Failed to get primary keys", ex);
        }
        catch (Exception ex)
        {
            Console.WriteLine($@"Unexpected Error: {ex.Message}");
            throw;
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
    public async Task<IEnumerable<FlightInfo>> GetTablesAsync(FlightCallOptions options,
        string? catalog = null,
        string? dbSchemaFilterPattern = null,
        string? tableFilterPattern = null,
        bool includeSchema = false,
        IEnumerable<string>? tableTypes = null)
    {
        if (options == null)
            throw new ArgumentNullException(nameof(options));

        var command = new CommandGetTables
        {
            Catalog = catalog ?? string.Empty,
            DbSchemaFilterPattern = dbSchemaFilterPattern ?? string.Empty,
            TableNameFilterPattern = tableFilterPattern ?? string.Empty,
            IncludeSchema = includeSchema
        };
        command.TableTypes.AddRange(tableTypes);

        var descriptor = FlightDescriptor.CreateCommandDescriptor(command.PackAndSerialize());
        var flightInfoCall = GetFlightInfoAsync(options, descriptor);
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
    public async Task<FlightInfo> GetExportedKeysAsync(FlightCallOptions options, TableRef tableRef)
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
            var flightInfo = await GetFlightInfoAsync(options, descriptor);
            return flightInfo;
        }
        catch (RpcException ex)
        {
            Console.WriteLine($@"gRPC Error: {ex.Status.Detail}");
            throw new InvalidOperationException("Failed to get exported keys", ex);
        }
        catch (Exception ex)
        {
            Console.WriteLine($@"Unexpected Error: {ex.Message}");
            throw;
        }
    }

    /// <summary>
    /// Get the exported keys schema from the server.
    /// </summary>
    /// <param name="options">RPC-layer hints for this call.</param>
    /// <returns>The SchemaResult describing the schema of the exported keys.</returns>
    public async Task<Schema> GetExportedKeysSchemaAsync(FlightCallOptions options)
    {
        if (options == null)
        {
            throw new ArgumentNullException(nameof(options));
        }

        try
        {
            var commandGetExportedKeysSchema = new CommandGetExportedKeys();
            var descriptor = FlightDescriptor.CreateCommandDescriptor(commandGetExportedKeysSchema.PackAndSerialize());
            var schemaResult = await GetSchemaAsync(options, descriptor);
            return schemaResult;
        }
        catch (RpcException ex)
        {
            Console.WriteLine($@"gRPC Error: {ex.Status.Detail}");
            throw new InvalidOperationException("Failed to get exported keys schema", ex);
        }
        catch (Exception ex)
        {
            Console.WriteLine($@"Unexpected Error: {ex.Message}");
            throw;
        }
    }

    /// <summary>
    /// Retrieves the foreign key columns for the given table.
    /// </summary>
    /// <param name="options">RPC-layer hints for this call.</param>
    /// <param name="tableRef">The table reference.</param>
    /// <returns>The FlightInfo describing where to access the dataset.</returns>
    public async Task<FlightInfo> GetImportedKeysAsync(FlightCallOptions options, TableRef tableRef)
    {
        if (tableRef == null)
            throw new ArgumentNullException(nameof(tableRef));

        try
        {
            var getImportedKeysRequest = new CommandGetImportedKeys
            {
                Catalog = tableRef.Catalog ?? string.Empty, DbSchema = tableRef.DbSchema, Table = tableRef.Table
            };

            var action =
                new FlightAction("GetImportedKeys",
                    getImportedKeysRequest.PackAndSerialize()); // check: whether using SqlAction.Enum
            var doActionResult = DoActionAsync(options, action);

            await foreach (var result in doActionResult)
            {
                var getImportedKeysResponse =
                    result.Body.ParseAndUnpack<ActionCreatePreparedStatementResult>();
                var command = new CommandPreparedStatementQuery
                {
                    PreparedStatementHandle = getImportedKeysResponse.PreparedStatementHandle
                };

                var descriptor = FlightDescriptor.CreateCommandDescriptor(command.PackAndSerialize());
                var flightInfo = await GetFlightInfoAsync(options, descriptor);
                return flightInfo;
            }

            throw new InvalidOperationException("Failed to retrieve imported keys information.");
        }
        catch (RpcException ex)
        {
            Console.WriteLine($@"gRPC Error: {ex.Status.Detail}");
            throw new InvalidOperationException("Failed to get imported keys", ex);
        }
        catch (Exception ex)
        {
            Console.WriteLine($@"Unexpected Error: {ex.Message}");
            throw;
        }
    }

    /// <summary>
    /// Get the imported keys schema from the server.
    /// </summary>
    /// <param name="options">RPC-layer hints for this call.</param>
    /// <returns>The SchemaResult describing the schema of the imported keys.</returns>
    public async Task<Schema> GetImportedKeysSchemaAsync(FlightCallOptions options)
    {
        if (options == null)
        {
            throw new ArgumentNullException(nameof(options));
        }

        try
        {
            var commandGetImportedKeysSchema = new CommandGetImportedKeys();
            var descriptor = FlightDescriptor.CreateCommandDescriptor(commandGetImportedKeysSchema.PackAndSerialize());
            var schemaResult = await GetSchemaAsync(options, descriptor);
            return schemaResult;
        }
        catch (RpcException ex)
        {
            Console.WriteLine($@"gRPC Error: {ex.Status.Detail}");
            throw new InvalidOperationException("Failed to get imported keys schema", ex);
        }
        catch (Exception ex)
        {
            Console.WriteLine($@"Unexpected Error: {ex.Message}");
            throw;
        }
    }

    /// <summary>
    /// Retrieves a description of the foreign key columns in the given foreign key table that reference the primary key or the columns representing a unique constraint of the parent table.
    /// </summary>
    /// <param name="options">RPC-layer hints for this call.</param>
    /// <param name="pkTableRef">The table reference that exports the key.</param>
    /// <param name="fkTableRef">The table reference that imports the key.</param>
    /// <returns>The FlightInfo describing where to access the dataset.</returns>
    public async Task<FlightInfo> GetCrossReferenceAsync(FlightCallOptions options, TableRef pkTableRef,
        TableRef fkTableRef)
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
            var flightInfo = await GetFlightInfoAsync(options, descriptor);

            return flightInfo;
        }
        catch (RpcException ex)
        {
            Console.WriteLine($@"gRPC Error: {ex.Status.Detail}");
            throw new InvalidOperationException("Failed to get cross reference", ex);
        }
        catch (Exception ex)
        {
            Console.WriteLine($@"Unexpected Error: {ex.Message}");
            throw;
        }
    }

    /// <summary>
    /// Get the cross-reference schema from the server.
    /// </summary>
    /// <param name="options">RPC-layer hints for this call.</param>
    /// <returns>The SchemaResult describing the schema of the cross-reference.</returns>
    public async Task<Schema> GetCrossReferenceSchemaAsync(FlightCallOptions options)
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
            var schemaResultCall = GetSchemaAsync(options, descriptor);
            var schemaResult = await schemaResultCall.ConfigureAwait(false);

            return schemaResult;
        }
        catch (RpcException ex)
        {
            Console.WriteLine($@"gRPC Error: {ex.Status.Detail}");
            throw new InvalidOperationException("Failed to get cross-reference schema", ex);
        }
        catch (Exception ex)
        {
            Console.WriteLine($@"Unexpected Error: {ex.Message}");
            throw;
        }
    }

    /// <summary>
    /// Request a list of table types.
    /// </summary>
    /// <param name="options">RPC-layer hints for this call.</param>
    /// <returns>The FlightInfo describing where to access the dataset.</returns>
    public async Task<FlightInfo> GetTableTypesAsync(FlightCallOptions options)
    {
        if (options == null)
        {
            throw new ArgumentNullException(nameof(options));
        }

        try
        {
            var command = new CommandGetTableTypes();
            var descriptor = FlightDescriptor.CreateCommandDescriptor(command.PackAndSerialize());
            var flightInfo = await GetFlightInfoAsync(options, descriptor);
            return flightInfo;
        }
        catch (RpcException ex)
        {
            Console.WriteLine($@"gRPC Error: {ex.Status.Detail}");
            throw new InvalidOperationException("Failed to get table types", ex);
        }
        catch (Exception ex)
        {
            Console.WriteLine($@"Unexpected Error: {ex.Message}");
            throw;
        }
    }

    /// <summary>
    /// Get the table types schema from the server.
    /// </summary>
    /// <param name="options">RPC-layer hints for this call.</param>
    /// <returns>The SchemaResult describing the schema of the table types.</returns>
    public async Task<Schema> GetTableTypesSchemaAsync(FlightCallOptions options)
    {
        if (options == null)
        {
            throw new ArgumentNullException(nameof(options));
        }

        try
        {
            var command = new CommandGetTableTypes();
            var descriptor = FlightDescriptor.CreateCommandDescriptor(command.PackAndSerialize());
            var schemaResult = await GetSchemaAsync(options, descriptor);
            return schemaResult;
        }
        catch (RpcException ex)
        {
            Console.WriteLine($@"gRPC Error: {ex.Status.Detail}");
            throw new InvalidOperationException("Failed to get table types schema", ex);
        }
        catch (Exception ex)
        {
            Console.WriteLine($@"Unexpected Error: {ex.Message}");
            throw;
        }
    }

    /// <summary>
    /// Request the information about all the data types supported with filtering by data type.
    /// </summary>
    /// <param name="options">RPC-layer hints for this call.</param>
    /// <param name="dataType">The data type to search for as filtering.</param>
    /// <returns>The FlightInfo describing where to access the dataset.</returns>
    public async Task<FlightInfo> GetXdbcTypeInfoAsync(FlightCallOptions options, int dataType)
    {
        if (options == null)
        {
            throw new ArgumentNullException(nameof(options));
        }

        try
        {
            var command = new CommandGetXdbcTypeInfo { DataType = dataType };
            var descriptor = FlightDescriptor.CreateCommandDescriptor(command.PackAndSerialize());
            var flightInfo = await GetFlightInfoAsync(options, descriptor);
            return flightInfo;
        }
        catch (RpcException ex)
        {
            Console.WriteLine($@"gRPC Error: {ex.Status.Detail}");
            throw new InvalidOperationException("Failed to get XDBC type info", ex);
        }
        catch (Exception ex)
        {
            Console.WriteLine($@"Unexpected Error: {ex.Message}");
            throw;
        }
    }

    /// <summary>
    /// Request the information about all the data types supported.
    /// </summary>
    /// <param name="options">RPC-layer hints for this call.</param>
    /// <returns>The FlightInfo describing where to access the dataset.</returns>
    public async Task<FlightInfo> GetXdbcTypeInfoAsync(FlightCallOptions options)
    {
        if (options == null)
        {
            throw new ArgumentNullException(nameof(options));
        }

        try
        {
            var command = new CommandGetXdbcTypeInfo();
            var descriptor = FlightDescriptor.CreateCommandDescriptor(command.PackAndSerialize());
            var flightInfo = await GetFlightInfoAsync(options, descriptor);
            return flightInfo;
        }
        catch (RpcException ex)
        {
            Console.WriteLine($@"gRPC Error: {ex.Status.Detail}");
            throw new InvalidOperationException("Failed to get XDBC type info", ex);
        }
        catch (Exception ex)
        {
            Console.WriteLine($@"Unexpected Error: {ex.Message}");
            throw;
        }
    }

    /// <summary>
    /// Get the type info schema from the server.
    /// </summary>
    /// <param name="options">RPC-layer hints for this call.</param>
    /// <returns>The SchemaResult describing the schema of the type info.</returns>
    public async Task<Schema> GetXdbcTypeInfoSchemaAsync(FlightCallOptions options)
    {
        if (options == null)
        {
            throw new ArgumentNullException(nameof(options));
        }

        try
        {
            var command = new CommandGetXdbcTypeInfo();
            var descriptor = FlightDescriptor.CreateCommandDescriptor(command.PackAndSerialize());
            var schemaResult = await GetSchemaAsync(options, descriptor);
            return schemaResult;
        }
        catch (RpcException ex)
        {
            Console.WriteLine($@"gRPC Error: {ex.Status.Detail}");
            throw new InvalidOperationException("Failed to get XDBC type info schema", ex);
        }
        catch (Exception ex)
        {
            Console.WriteLine($@"Unexpected Error: {ex.Message}");
            throw;
        }
    }

    /// <summary>
    /// Request a list of SQL information.
    /// </summary>
    /// <param name="options">RPC-layer hints for this call.</param>
    /// <param name="sqlInfo">The SQL info required.</param>
    /// <returns>The FlightInfo describing where to access the dataset.</returns>
    public async Task<FlightInfo> GetSqlInfoAsync(FlightCallOptions options, List<int> sqlInfo)
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
            var flightInfo = await GetFlightInfoAsync(options, descriptor);
            return flightInfo;
        }
        catch (RpcException ex)
        {
            Console.WriteLine($@"gRPC Error: {ex.Status.Detail}");
            throw new InvalidOperationException("Failed to get SQL info", ex);
        }
        catch (Exception ex)
        {
            Console.WriteLine($@"Unexpected Error: {ex.Message}");
            throw;
        }
    }

    /// <summary>
    /// Get the SQL information schema from the server.
    /// </summary>
    /// <param name="options">RPC-layer hints for this call.</param>
    /// <returns>The SchemaResult describing the schema of the SQL information.</returns>
    public async Task<Schema> GetSqlInfoSchemaAsync(FlightCallOptions options)
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
            Console.WriteLine($@"gRPC Error: {ex.Status.Detail}");
            throw new InvalidOperationException("Failed to get SQL info schema", ex);
        }
        catch (Exception ex)
        {
            Console.WriteLine($@"Unexpected Error: {ex.Message}");
            throw;
        }
    }

    /// <summary>
    /// Explicitly cancel a FlightInfo.
    /// </summary>
    /// <param name="options">RPC-layer hints for this call.</param>
    /// <param name="request">The CancelFlightInfoRequest.</param>
    /// <returns>A Task representing the asynchronous operation. The task result contains the CancelFlightInfoResult describing the canceled result.</returns>
    public async Task<CancelFlightInfoResult> CancelFlightInfoAsync(FlightCallOptions options,
        CancelFlightInfoRequest request)
    {
        if (options == null) throw new ArgumentNullException(nameof(options));
        if (request == null) throw new ArgumentNullException(nameof(request));

        try
        {
            var action = new FlightAction("CancelFlightInfo", request.PackAndSerialize());
            var call = _client.DoAction(action, options.Headers);
            await foreach (var result in call.ResponseStream.ReadAllAsync())
            {
                var cancelResult = FlightSqlUtils.ParseAndUnpack<CancelFlightInfoResult>(result.Body);
                return cancelResult;
            }

            throw new InvalidOperationException("No response received for the CancelFlightInfo request.");
        }
        catch (RpcException ex)
        {
            // Handle gRPC exceptions
            Console.WriteLine($@"gRPC Error: {ex.Status.Detail}");
            throw new InvalidOperationException("Failed to cancel flight info", ex);
        }
        catch (Exception ex)
        {
            // Handle other exceptions
            Console.WriteLine($@"Unexpected Error: {ex.Message}");
            throw;
        }
    }

    /// <summary>
    /// Begin a new transaction.
    /// </summary>
    /// <param name="options">RPC-layer hints for this call.</param>
    /// <returns>A Task representing the asynchronous operation. The task result contains the Transaction object representing the new transaction.</returns>
    public async Task<Transaction> BeginTransactionAsync(FlightCallOptions options)
    {
        if (options == null)
        {
            throw new ArgumentNullException(nameof(options));
        }

        try
        {
            var actionBeginTransaction = new ActionBeginTransactionRequest();
            var action = new FlightAction("BeginTransaction", actionBeginTransaction.PackAndSerialize());
            var responseStream = _client.DoAction(action, options.Headers);

            await foreach (var result in responseStream.ResponseStream.ReadAllAsync())
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
    public AsyncServerStreamingCall<FlightResult> CommitAsync(FlightCallOptions options, Transaction transaction)
    {
        if (options == null)
        {
            throw new ArgumentNullException(nameof(options));
        }

        if (transaction == null)
        {
            throw new ArgumentNullException(nameof(transaction));
        }

        try
        {
            var actionCommit = new FlightAction("Commit", transaction.TransactionId);
            return _client.DoAction(actionCommit, options.Headers);
        }
        catch (RpcException ex)
        {
            Console.WriteLine($@"gRPC Error: {ex.Status.Detail}");
            throw new InvalidOperationException("Failed to commit transaction", ex);
        }
        catch (Exception ex)
        {
            Console.WriteLine($@"Unexpected Error: {ex.Message}");
            throw;
        }
    }

    /// <summary>
    /// Rollback a transaction.
    /// After this, the transaction and all associated savepoints will be invalidated.
    /// </summary>
    /// <param name="options">RPC-layer hints for this call.</param>
    /// <param name="transaction">The transaction to rollback.</param>
    /// <returns>A Task representing the asynchronous operation.</returns>
    public AsyncServerStreamingCall<FlightResult> RollbackAsync(FlightCallOptions options, Transaction transaction)
    {
        if (options == null)
        {
            throw new ArgumentNullException(nameof(options));
        }

        if (transaction == null)
        {
            throw new ArgumentNullException(nameof(transaction));
        }

        try
        {
            var actionRollback = new FlightAction("Rollback", transaction.TransactionId);
            return _client.DoAction(actionRollback, options.Headers);
        }
        catch (RpcException ex)
        {
            throw new InvalidOperationException("Failed to rollback transaction", ex);
        }
    }


    /// <summary>
    /// Explicitly cancel a query.
    /// </summary>
    /// <param name="options">RPC-layer hints for this call.</param>
    /// <param name="info">The FlightInfo of the query to cancel.</param>
    /// <returns>A Task representing the asynchronous operation.</returns>
    public async Task<CancelStatus> CancelQueryAsync(FlightCallOptions options, FlightInfo info)
    {
        if (options == null)
        {
            throw new ArgumentNullException(nameof(options));
        }

        if (info == null)
        {
            throw new ArgumentNullException(nameof(info));
        }

        try
        {
            var cancelRequest = new CancelFlightInfoRequest(info);
            var action = new FlightAction("CancelFlightInfo", cancelRequest.ToByteString());
            var call = _client.DoAction(action, options.Headers);
            await foreach (var result in call.ResponseStream.ReadAllAsync())
            {
                var cancelResult = Any.Parser.ParseFrom(result.Body);
                if (cancelResult.TryUnpack(out CancelFlightInfoResult cancelFlightInfoResult))
                {
                    return cancelFlightInfoResult.CancelStatus switch
                    {
                        CancelStatus.Cancelled => CancelStatus.Cancelled,
                        CancelStatus.Cancelling => CancelStatus.Cancelling,
                        CancelStatus.NotCancellable => CancelStatus.NotCancellable,
                        _ => CancelStatus.Unspecified
                    };
                }
            }

            throw new InvalidOperationException("Failed to cancel query: No response received.");
        }
        catch (RpcException ex)
        {
            Console.WriteLine($@"gRPC Error: {ex.Status.Detail}");
            throw new InvalidOperationException("Failed to cancel query", ex);
        }
        catch (Exception ex)
        {
            Console.WriteLine($@"Unexpected Error: {ex.Message}");
            throw;
        }
    }

    /// <summary>
    /// Create a prepared statement object.
    /// </summary>
    /// <param name="options">RPC-layer hints for this call.</param>
    /// <param name="query">The query that will be executed.</param>
    /// <param name="transaction">A transaction to associate this query with.</param>
    /// <returns>The created prepared statement.</returns>
    public async Task<PreparedStatement> PrepareAsync(FlightCallOptions options, string query,
        Transaction? transaction = null)
    {
        transaction ??= NoTransaction();

        if (options == null)
        {
            throw new ArgumentNullException(nameof(options));
        }

        if (string.IsNullOrEmpty(query))
        {
            throw new ArgumentException("Query cannot be null or empty", nameof(query));
        }

        try
        {
            var preparedStatementRequest = new ActionCreatePreparedStatementRequest
            {
                Query = query,
                TransactionId = transaction is null
                    ? ByteString.CopyFromUtf8(transaction?.TransactionId)
                    : ByteString.Empty
            };

            var action = new FlightAction(SqlAction.CreateRequest, preparedStatementRequest.PackAndSerialize());
            var call = _client.DoAction(action, options.Headers);

            await foreach (var result in call.ResponseStream.ReadAllAsync())
            {
                var preparedStatementResponse =
                    FlightSqlUtils.ParseAndUnpack<ActionClosePreparedStatementRequest>(result.Body);

                var commandSqlCall = new CommandPreparedStatementQuery
                {
                    PreparedStatementHandle = preparedStatementResponse.PreparedStatementHandle
                };
                byte[] commandSqlCallPackedAndSerialized = commandSqlCall.PackAndSerialize();
                var descriptor = FlightDescriptor.CreateCommandDescriptor(commandSqlCallPackedAndSerialized);
                var flightInfo = await GetFlightInfoAsync(options, descriptor);
                return new PreparedStatement(this, flightInfo, query);
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
