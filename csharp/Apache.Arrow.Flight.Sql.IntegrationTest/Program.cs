/*using Apache.Arrow;
using Apache.Arrow.Flight;
using Apache.Arrow.Flight.Client;
using Grpc.Core;
using Grpc.Net.Client;

namespace FlightClientExample
{
    public class Program
    {
        public static async Task Main(string[] args)
        {
            string host = args.Length > 0 ? args[0] : "localhost";
            string port = args.Length > 1 ? args[1] : "5000";

            // Create client
            // (In production systems, you should use https not http)
            var address = $"http://{host}:{port}";
            Console.WriteLine($"Connecting to: {address}");
            var channel = GrpcChannel.ForAddress(address);
            var client = new FlightClient(channel);

            var recordBatches = new[] { CreateTestBatch(0, 2000), CreateTestBatch(50, 9000) };

            // Particular flights are identified by a descriptor. This might be a name,
            // a SQL query, or a path. Here, just using the name "test".
            var descriptor = FlightDescriptor.CreatePathDescriptor("//SYSDB/Info"); //.CreateCommandDescriptor("SELECT * FROM SYSDB.`Info` ");

            // Upload data with StartPut
            // var batchStreamingCall = client.StartPut(descriptor);
            // foreach (var batch in recordBatches)
            // {
            //     await batchStreamingCall.RequestStream.WriteAsync(batch);
            // }
            //
            // // Signal we are done sending record batches
            // await batchStreamingCall.RequestStream.CompleteAsync();
            // // Retrieve final response
            // await batchStreamingCall.ResponseStream.MoveNext();
            // Console.WriteLine(batchStreamingCall.ResponseStream.Current.ApplicationMetadata.ToStringUtf8());
            // Console.WriteLine($"Wrote {recordBatches.Length} batches to server.");

            // Request information:
            //var schema = await client.GetSchema(descriptor).ResponseAsync;
            //Console.WriteLine($"Schema saved as: \n {schema}");

            var info = await client.GetInfo(descriptor).ResponseAsync;
            Console.WriteLine($"Info provided: \n {info.TotalRecords}");

            Console.WriteLine($"Available flights:");
            // var flights_call = client.ListFlights();
            //
            // while (await flights_call.ResponseStream.MoveNext())
            // {
            //     Console.WriteLine("  " + flights_call.ResponseStream.Current);
            // }

            // // Download data
            // await foreach (var batch in StreamRecordBatches(info))
            // {
            //     Console.WriteLine($"Read batch from flight server: \n {batch}");
            // }

            // See available commands on this server
            // var action_stream = client.ListActions();
            // Console.WriteLine("Actions:");
            // while (await action_stream.ResponseStream.MoveNext())
            // {
            //     var action = action_stream.ResponseStream.Current;
            //     Console.WriteLine($"  {action.Type}: {action.Description}");
            // }
            //
            // // Send clear command to drop all data from the server.
            // var clear_result = client.DoAction(new FlightAction("clear"));
            // await clear_result.ResponseStream.MoveNext(default);
        }

        public static async IAsyncEnumerable<RecordBatch> StreamRecordBatches(
            FlightInfo info
        )
        {
            // There might be multiple endpoints hosting part of the data. In simple services,
            // the only endpoint might be the same server we initially queried.
            foreach (var endpoint in info.Endpoints)
            {
                // We may have multiple locations to choose from. Here we choose the first.
                var download_channel = GrpcChannel.ForAddress(endpoint.Locations.First().Uri);
                var download_client = new FlightClient(download_channel);

                var stream = download_client.GetStream(endpoint.Ticket);

                while (await stream.ResponseStream.MoveNext())
                {
                    yield return stream.ResponseStream.Current;
                }
            }
        }

        public static RecordBatch CreateTestBatch(int start, int length)
        {
            return new RecordBatch.Builder()
                .Append("Column A", false,
                    col => col.Int32(array => array.AppendRange(Enumerable.Range(start, start + length))))
                .Append("Column B", false,
                    col => col.Float(array =>
                        array.AppendRange(Enumerable.Range(start, start + length)
                            .Select(x => Convert.ToSingle(x * 2)))))
                .Append("Column C", false,
                    col => col.String(array =>
                        array.AppendRange(Enumerable.Range(start, start + length).Select(x => $"Item {x + 1}"))))
                .Append("Column D", false,
                    col => col.Boolean(array =>
                        array.AppendRange(Enumerable.Range(start, start + length).Select(x => x % 2 == 0))))
                .Build();
        }
    }
}*/

using Apache.Arrow.Flight.Client;
using Apache.Arrow.Flight.Sql.Client;
using Apache.Arrow.Types;
using Arrow.Flight.Protocol.Sql;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Grpc.Net.Client;

namespace Apache.Arrow.Flight.Sql.IntegrationTest;

class Program
{
    static async Task Main(string[] args)
    {
        var httpHandler = new SocketsHttpHandler
        {
            PooledConnectionIdleTimeout = TimeSpan.FromMinutes(1),
            KeepAlivePingDelay = TimeSpan.FromSeconds(60),
            KeepAlivePingTimeout = TimeSpan.FromSeconds(30),
            EnableMultipleHttp2Connections = true
        };
        // Initialize the gRPC channel to connect to the Flight server
        using var channel = GrpcChannel.ForAddress("http://localhost:5000",
            new GrpcChannelOptions { HttpHandler = httpHandler, Credentials = ChannelCredentials.Insecure });

        // Initialize the Flight client
        var flightClient = new FlightClient(channel);
        var sqlClient = new FlightSqlClient(flightClient);

        // Define the SQL query
        string query = "SELECT * FROM SYSDB.`Info`";

        try
        {
            // ExecuteAsync
            Console.WriteLine("ExecuteAsync:");
            var flightInfo = await sqlClient.ExecuteAsync(new FlightCallOptions(), query);
            // Handle the ExecuteAsync result
            Console.WriteLine($@"Query executed successfully. Records count: {flightInfo.TotalRecords}");

            // ExecuteUpdate
            Console.WriteLine("ExecuteUpdate:");
            string updateQuery = "UPDATE SYSDB.`Info` SET Key = 1, Val=10 WHERE Id=1";
            long affectedRows = await sqlClient.ExecuteUpdateAsync(new FlightCallOptions(), updateQuery);
            // Handle the ExecuteUpdate result
            Console.WriteLine($@"Number of affected d rows: {affectedRows}");

            // GetExecuteSchema
            Console.WriteLine("GetExecuteSchema:");
            var schemaResult = await sqlClient.GetExecuteSchemaAsync(new FlightCallOptions(), query);
            // Process the schemaResult as needed
            Console.WriteLine($"Schema retrieved successfully:{schemaResult}");

            // ExecuteIngest

            // GetCatalogs
            Console.WriteLine("GetCatalogs:");
            var catalogsInfo = await sqlClient.GetCatalogs(new FlightCallOptions());
            // Print catalog details
            Console.WriteLine("Catalogs retrieved:");
            foreach (var endpoint in catalogsInfo.Endpoints)
            {
                var ticket = endpoint.Ticket;
                Console.WriteLine($"- Ticket: {ticket}");
            }

            // GetCatalogsSchema
            // Console.WriteLine("GetCatalogsSchema:");
            // Schema schemaCatalogResult = await sqlClient.GetCatalogsSchema(new FlightCallOptions());
            // Console.WriteLine("Catalogs Schema retrieved:");
            // Console.WriteLine(schemaCatalogResult);

            // GetDbSchemasAsync
            // Console.WriteLine("GetDbSchemasAsync:");
            // FlightInfo flightInfoDbSchemas =
            //     await sqlClient.GetDbSchemasAsync(new FlightCallOptions(), "default_catalog", "public");
            // // Process the FlightInfoDbSchemas
            // Console.WriteLine("Database schemas retrieved:");
            // Console.WriteLine(flightInfoDbSchemas);

            // GetDbSchemasSchemaAsync
            // Console.WriteLine("GetDbSchemasSchemaAsync:");
            // Schema schema = await sqlClient.GetDbSchemasSchemaAsync(new FlightCallOptions());
            // // Process the Schema
            // Console.WriteLine("Database schemas schema retrieved:");
            // Console.WriteLine(schema);

            // DoPut
            // Console.WriteLine("DoPut:");
            // await PutExample(sqlClient, query);

            // GetPrimaryKeys
            // Console.WriteLine("GetPrimaryKeys:");
            // var tableRef = new TableRef
            // {
            //     DbSchema = "SYSDB",
            //     Table = "Info"
            // };
            // var getPrimaryKeysInfo = await sqlClient.GetPrimaryKeys(new FlightCallOptions(), tableRef);
            // Console.WriteLine("Primary keys information retrieved successfully.");


            // Call GetTablesAsync method
            Console.WriteLine("GetTablesAsync:");
            IEnumerable<FlightInfo> tables = await sqlClient.GetTablesAsync(
                new FlightCallOptions(),
                catalog: "",
                dbSchemaFilterPattern: "public",
                tableFilterPattern: "SYSDB",
                includeSchema: true,
                tableTypes: new List<string> { "TABLE", "VIEW" });
            foreach (var table in tables)
            {
                Console.WriteLine($"Table URI: {table.Descriptor.Paths}");
                foreach (var endpoint in table.Endpoints)
                {
                    Console.WriteLine($"Endpoint Ticket: {endpoint.Ticket}");
                }
            }

            var tableRef = new TableRef { Catalog = "", DbSchema = "SYSDB", Table = "Info" };

            // Get exported keys
            // Console.WriteLine("GetExportedKeysAsync:");
            // var tableRef = new TableRef { Catalog = "", DbSchema = "SYSDB", Table = "Info" };
            // var flightInfoExportedKeys = await sqlClient.GetExportedKeysAsync(new FlightCallOptions(), tableRef);
            // Console.WriteLine("FlightInfo obtained:");
            // Console.WriteLine($"  FlightDescriptor: {flightInfoExportedKeys.Descriptor}");
            // Console.WriteLine($"  Total records: {flightInfoExportedKeys.TotalRecords}");
            // Console.WriteLine($"  Total bytes: {flightInfoExportedKeys.TotalBytes}");

            // Get exported keys schema
            // var schema = await sqlClient.GetExportedKeysSchemaAsync(new FlightCallOptions());
            // Console.WriteLine("Schema obtained:");
            // Console.WriteLine($"  Fields: {string.Join(", ", schema.FieldsList)}");

            // Get imported keys
            // Console.WriteLine("GetImportedKeys");
            // var flightInfoGetImportedKeys = sqlClient.GetImportedKeysAsync(new FlightCallOptions(), tableRef);
            // Console.WriteLine("FlightInfo obtained:");
            // Console.WriteLine($@"  Location: {flightInfoGetImportedKeys.Result.Endpoints[0]}");

            // Get imported keys schema
            // Console.WriteLine("GetImportedKeysSchemaAsync:");
            // var schema = await sqlClient.GetImportedKeysSchemaAsync(new FlightCallOptions());
            // Console.WriteLine("Imported Keys Schema obtained:");
            // Console.WriteLine($"Schema Fields: {string.Join(", ", schema.FieldsList)}");

            // Get cross reference
            // Console.WriteLine("GetCrossReferenceAsync:");
            // var flightInfoGetCrossReference = await sqlClient.GetCrossReferenceAsync(new FlightCallOptions(), tableRef, new TableRef
            // {
            //     Catalog = "catalog2",
            //     DbSchema = "schema2",
            //     Table = "table2"
            // });
            // Console.WriteLine("Cross Reference Information obtained:");
            // Console.WriteLine($"Flight Descriptor: {flightInfoGetCrossReference.Descriptor}");
            // Console.WriteLine($"Endpoints: {string.Join(", ", flightInfoGetCrossReference.Endpoints)}");

            // Get cross-reference schema
            // Console.WriteLine("GetCrossReferenceSchemaAsync:");
            // var schema = await sqlClient.GetCrossReferenceSchemaAsync(new FlightCallOptions());
            // Console.WriteLine("Cross Reference Schema obtained:");
            // Console.WriteLine($"Schema: {schema}");


            // Get table types
            // Console.WriteLine("GetTableTypesAsync:");
            // var tableTypesInfo = await sqlClient.GetTableTypesAsync(new FlightCallOptions());
            // Console.WriteLine("Table Types Info obtained:");
            // Console.WriteLine($"FlightInfo: {tableTypesInfo}");

            // Get table types schema
            // Console.WriteLine("GetTableTypesSchemaAsync:");
            // var tableTypesSchema = await sqlClient.GetTableTypesSchemaAsync(new FlightCallOptions());
            // Console.WriteLine("Table Types Schema obtained:");
            // Console.WriteLine($"Schema: {tableTypesSchema}");

            // Get XDBC type info (with DataType)
            Console.WriteLine("GetXdbcTypeInfoAsync: (With DataType)");
            var flightInfoGetXdbcTypeInfoWithoutDataType =
                await sqlClient.GetXdbcTypeInfoAsync(new FlightCallOptions(), 4);
            Console.WriteLine("XDBC With DataType Info obtained:");
            Console.WriteLine($"FlightInfo: {flightInfoGetXdbcTypeInfoWithoutDataType}");

            // Get XDBC type info
            Console.WriteLine("GetXdbcTypeInfoAsync:");
            var flightInfoGetXdbcTypeInfo = await sqlClient.GetXdbcTypeInfoAsync(new FlightCallOptions());
            Console.WriteLine("XDBC Type Info obtained:");
            Console.WriteLine($"FlightInfo: {flightInfoGetXdbcTypeInfo}");

            // Get XDBC type info schema
            // Console.WriteLine("GetXdbcTypeInfoSchemaAsync:");
            // var flightInfoGetXdbcTypeSchemaInfo = await sqlClient.GetXdbcTypeInfoSchemaAsync(new FlightCallOptions());
            // Console.WriteLine("XDBC Type Info obtained:");
            // Console.WriteLine($"FlightInfo: {flightInfoGetXdbcTypeSchemaInfo}");

            // Get SQL info
            Console.WriteLine("GetSqlInfoAsync:");
            // Define SQL info list
            var sqlInfo = new List<int> { 1, 2, 3 };
            var flightInfoGetSqlInfo = sqlClient.GetSqlInfoAsync(new FlightCallOptions(), sqlInfo);
            Console.WriteLine("SQL Info obtained:");
            Console.WriteLine($"FlightInfo: {flightInfoGetSqlInfo}");

            // Get SQL info schema
            // Console.WriteLine("GetSqlInfoSchemaAsync:");
            // var schema = await sqlClient.GetSqlInfoSchemaAsync(new FlightCallOptions());
            // Console.WriteLine("SQL Info Schema obtained:");
            // Console.WriteLine($"Schema: {schema}");

            // Prepare a SQL statement
            Console.WriteLine("PrepareAsync:");
            var preparedStatement = await sqlClient.PrepareAsync(new FlightCallOptions(), query);
            Console.WriteLine("Prepared statement created successfully.");


            // Cancel FlightInfo Request
            // Console.WriteLine("CancelFlightInfoRequest:");
            // var cancelRequest = new CancelFlightInfoRequest(flightInfo);
            // var cancelResult = await sqlClient.CancelFlightInfoAsync(new FlightCallOptions(), cancelRequest);
            // Console.WriteLine($"Cancellation Status: {cancelResult.CancelStatus}");

            // Begin Transaction
            // Console.WriteLine("BeginTransaction:");
            // Transaction transaction = await sqlClient.BeginTransactionAsync(new FlightCallOptions());
            // Console.WriteLine($"Transaction started with ID: {transaction.TransactionId}");
            // FlightInfo flightInfoBeginTransaction =
            //     await sqlClient.ExecuteAsync(new FlightCallOptions(), query, transaction);
            // Console.WriteLine("Query executed within transaction");
            //
            // // Commit Transaction
            // Console.WriteLine("CommitTransaction:");
            // await sqlClient.CommitAsync(new FlightCallOptions(), new Transaction("transaction-id"));
            // Console.WriteLine("Transaction committed successfully.");
            //
            // // Rollback Transaction
            // Console.WriteLine("RollbackTransaction");
            // await sqlClient.RollbackAsync(new FlightCallOptions(), new Transaction("transaction-id"));
            // Console.WriteLine("Transaction rolled back successfully.");

            // Cancel Query
            Console.WriteLine("CancelQuery:");
            var cancelResult = await sqlClient.CancelQueryAsync(new FlightCallOptions(), flightInfo);
            Console.WriteLine($"Cancellation Status: {cancelResult}");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error executing query: {ex.Message}");
        }
    }

    static async Task PutExample(FlightSqlClient client, string query)
    {
        // TODO: Talk with Jeremy about the implementation: DoPut - seems that needed to resolve missing part
        var options = new FlightCallOptions();
        var body = new ActionCreatePreparedStatementRequest { Query = query }.PackAndSerialize();
        var action = new FlightAction(SqlAction.CreateRequest, body);
        await foreach (FlightResult flightResult in client.DoActionAsync(action))
        {
            var preparedStatementResponse =
                FlightSqlUtils.ParseAndUnpack<ActionCreatePreparedStatementResult>(flightResult.Body);

            var command = new CommandPreparedStatementUpdate
            {
                PreparedStatementHandle = preparedStatementResponse.PreparedStatementHandle
            };

            var descriptor = FlightDescriptor.CreateCommandDescriptor(command.PackAndSerialize());
            // Define schema
            var fields = new List<Field>
            {
                new("id", Int32Type.Default, nullable: false), new("name", StringType.Default, nullable: false)
            };
            var metadata =
                new List<KeyValuePair<string, string>> { new("db_name", "SYSDB"), new("table_name", "Info") };
            var schema = new Schema(fields, metadata);
            var doPutResult = await client.DoPut(options, descriptor, schema).ConfigureAwait(false);

            // Example data to write
            var col1 = new Int32Array.Builder().AppendRange(new[] { 8, 9, 10, 11 }).Build();
            var col2 = new StringArray.Builder().AppendRange(new[] { "a", "b", "c", "d" }).Build();
            var col3 = new StringArray.Builder().AppendRange(new[] { "x", "y", "z", "q" }).Build();
            var batch = new RecordBatch(schema, new IArrowArray[] { col1, col2, col3 }, 4);

            await doPutResult.Writer.WriteAsync(batch);
            await doPutResult.Writer.CompleteAsync();

            // Handle metadata response (if any)
            while (await doPutResult.Reader.MoveNext())
            {
                var receivedMetadata = doPutResult.Reader.Current.ApplicationMetadata;
                if (receivedMetadata != null)
                {
                    Console.WriteLine("Received metadata: " + receivedMetadata.ToStringUtf8());
                }
            }
        }
    }
}

internal static class FlightDescriptorExtensions
{
    public static byte[] PackAndSerialize(this IMessage command)
    {
        return Any.Pack(command).Serialize().ToByteArray();
    }
}
