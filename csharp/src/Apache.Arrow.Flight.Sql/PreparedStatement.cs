using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Flight.Server;
using Apache.Arrow.Flight.Sql.Client;
using Apache.Arrow.Ipc;
using Arrow.Flight.Protocol.Sql;
using Google.Protobuf;
using Grpc.Core;
using System.Threading.Channels;

namespace Apache.Arrow.Flight.Sql;

public class PreparedStatement : IDisposable
{
    private readonly FlightSqlClient _client;
    private readonly string _handle;
    private Schema _datasetSchema;
    private Schema _parameterSchema;
    private bool _isClosed;
    public bool IsClosed => _isClosed;
    public string Handle => _handle;
    private FlightServerRecordBatchStreamReader? _parameterReader;
    public FlightServerRecordBatchStreamReader? ParameterReader => _parameterReader;

    public PreparedStatement(FlightSqlClient client, string handle, Schema datasetSchema, Schema parameterSchema)
    {
        _client = client ?? throw new ArgumentNullException(nameof(client));
        _handle = handle ?? throw new ArgumentNullException(nameof(handle));
        _datasetSchema = datasetSchema ?? throw new ArgumentNullException(nameof(datasetSchema));
        _parameterSchema = parameterSchema ?? throw new ArgumentNullException(nameof(parameterSchema));
        _isClosed = false;
    }

    /// <summary>
    /// Retrieves the schema associated with the prepared statement asynchronously.
    /// </summary>
    /// <param name="options">The FlightCallOptions for the operation.</param>
    /// <returns>A Task representing the asynchronous operation. The task result contains the SchemaResult object.</returns>
    public async Task<Schema> GetSchemaAsync(FlightCallOptions options)
    {
        EnsureStatementIsNotClosed();

        try
        {
            var command = new CommandPreparedStatementQuery
            {
                PreparedStatementHandle = ByteString.CopyFrom(_handle, Encoding.UTF8)
            };
            var descriptor = FlightDescriptor.CreateCommandDescriptor(command.PackAndSerialize());
            var schema = await _client.GetSchemaAsync(options, descriptor).ConfigureAwait(false);
            if (schema == null || !schema.FieldsList.Any())
            {
                throw new InvalidOperationException("Schema is empty or invalid.");
            }
            return schema;
        }
        catch (RpcException ex)
        {
            throw new InvalidOperationException("Failed to retrieve the schema for the prepared statement", ex);
        }
    }


    /// <summary>
    /// Closes the prepared statement asynchronously.
    /// </summary>
    /// <param name="options">The FlightCallOptions for the operation.</param>
    /// <returns>A Task representing the asynchronous operation.</returns>
    public async Task CloseAsync(FlightCallOptions options)
    {
        EnsureStatementIsNotClosed();
        try
        {
            var closeRequest = new ActionClosePreparedStatementRequest
            {
                PreparedStatementHandle = ByteString.CopyFrom(_handle, Encoding.UTF8)
            };

            var action = new FlightAction(SqlAction.CloseRequest, closeRequest.ToByteArray());
            await foreach (var result in _client.DoActionAsync(options, action).ConfigureAwait(false))
            {
                // Just drain the results to complete the operation
            }

            _isClosed = true;
        }
        catch (RpcException ex)
        {
            throw new InvalidOperationException("Failed to close the prepared statement", ex);
        }
    }

    /// <summary>
    /// Reads the result from an asynchronous stream of FlightData and populates the provided Protobuf message.
    /// </summary>
    /// <param name="results">The async enumerable stream of FlightData results.</param>
    /// <param name="message">The Protobuf message to populate from the results.</param>
    /// <returns>A task that represents the asynchronous read operation.</returns>
    public async Task ReadResultAsync(IAsyncEnumerable<FlightData> results, IMessage message)
    {
        if (results == null) throw new ArgumentNullException(nameof(results));
        if (message == null) throw new ArgumentNullException(nameof(message));

        await foreach (var flightData in results.ConfigureAwait(false))
        {
            // Ensure that the data received is valid and non-empty.
            if (flightData.DataBody == null || flightData.DataBody.Length == 0)
                throw new InvalidOperationException("Received empty or invalid FlightData.");

            try
            {
                // Merge the flight data's body into the provided message.
                message.MergeFrom(message.PackAndSerialize());
            }
            catch (InvalidProtocolBufferException ex)
            {
                throw new InvalidOperationException(
                    "Failed to parse the received FlightData into the specified message.", ex);
            }
        }
    }

    public async Task<PreparedStatement> ParseResponseAsync(FlightSqlClient client, IAsyncEnumerable<FlightData> results)
    {
        if (client == null)
            throw new ArgumentNullException(nameof(client));

        if (results == null)
            throw new ArgumentNullException(nameof(results));

        var preparedStatementResult = new ActionCreatePreparedStatementResult();
        await foreach (var flightData in results.ConfigureAwait(false))
        {
            if (flightData.DataBody == null || flightData.DataBody.Length == 0)
            {
                continue;
            }

            try
            {
                preparedStatementResult.MergeFrom(flightData.DataBody.ToByteArray());
            }
            catch (InvalidProtocolBufferException ex)
            {
                throw new InvalidOperationException(
                    "Failed to parse FlightData into ActionCreatePreparedStatementResult.", ex);
            }
        }

        // If the response is empty or incomplete
        if (preparedStatementResult.PreparedStatementHandle.Length == 0)
        {
            throw new InvalidOperationException("Received an empty or invalid PreparedStatementHandle.");
        }

        // Parse dataset and parameter schemas from the response
        Schema datasetSchema = null!;
        Schema parameterSchema = null!;

        if (preparedStatementResult.DatasetSchema.Length > 0)
        {
            datasetSchema = SchemaExtensions.DeserializeSchema(preparedStatementResult.DatasetSchema.ToByteArray());
        }

        if (preparedStatementResult.ParameterSchema.Length > 0)
        {
            parameterSchema = SchemaExtensions.DeserializeSchema(preparedStatementResult.ParameterSchema.ToByteArray());
        }

        // Create and return the PreparedStatement object
        return new PreparedStatement(client, preparedStatementResult.PreparedStatementHandle.ToStringUtf8(),
            datasetSchema, parameterSchema);
    }

    public Status SetParameters(RecordBatch parameterBatch, CancellationToken cancellationToken = default)
    {
        EnsureStatementIsNotClosed();

        if (parameterBatch == null)
            throw new ArgumentNullException(nameof(parameterBatch));

        var channel = Channel.CreateUnbounded<FlightData>();
        var task = Task.Run(async () =>
        {
            try
            {
                using (var memoryStream = new MemoryStream())
                {
                    var writer = new ArrowStreamWriter(memoryStream, parameterBatch.Schema);

                    cancellationToken.ThrowIfCancellationRequested();
                    await writer.WriteRecordBatchAsync(parameterBatch, cancellationToken).ConfigureAwait(false);
                    await writer.WriteEndAsync(cancellationToken).ConfigureAwait(false);

                    memoryStream.Position = 0;

                    cancellationToken.ThrowIfCancellationRequested();

                    var flightData = new FlightData(
                        FlightDescriptor.CreateCommandDescriptor(_handle),
                        ByteString.CopyFrom(memoryStream.ToArray()),
                        ByteString.Empty,
                        ByteString.Empty
                    );
                    await channel.Writer.WriteAsync(flightData, cancellationToken).ConfigureAwait(false);
                }

                channel.Writer.Complete();
            }
            catch (OperationCanceledException)
            {
                channel.Writer.TryComplete(new OperationCanceledException("Task was canceled"));
            }
            catch (Exception ex)
            {
                channel.Writer.TryComplete(ex);
            }
        }, cancellationToken);

        _parameterReader = new FlightServerRecordBatchStreamReader(new ChannelReaderStreamAdapter<FlightData>(channel.Reader));
        if (task.IsCanceled || cancellationToken.IsCancellationRequested)
        {
            return Status.DefaultCancelled;
        }

        return Status.DefaultSuccess;
    }



    /// <summary>
    /// Executes the prepared statement asynchronously and retrieves the query results as <see cref="FlightInfo"/>.
    /// </summary>
    /// <param name="options">The <see cref="FlightCallOptions"/> for the operation, which may include timeouts, headers, and other options for the call.</param>
    /// <param name="parameterBatch">Optional <see cref="RecordBatch"/> containing parameters to bind before executing the statement.</param>
    /// <param name="cancellationToken">Optional <see cref="CancellationToken"/> to observe while waiting for the task to complete. The task will be canceled if the token is canceled.</param>
    /// <returns>A <see cref="Task{FlightInfo}"/> representing the asynchronous operation. The task result contains the <see cref="FlightInfo"/> describing the executed query results.</returns>
    /// <exception cref="InvalidOperationException">Thrown if the prepared statement is closed or if there is an error during execution.</exception>
    /// <exception cref="OperationCanceledException">Thrown if the operation is canceled by the <paramref name="cancellationToken"/>.</exception>
    public async Task<FlightInfo> ExecuteAsync(FlightCallOptions options, RecordBatch parameterBatch, CancellationToken cancellationToken = default)
    {
        EnsureStatementIsNotClosed();

        var descriptor = FlightDescriptor.CreateCommandDescriptor(_handle);
        cancellationToken.ThrowIfCancellationRequested();
        
        if (parameterBatch != null)
        {
            var boundParametersAsync = await BindParametersAsync(options, descriptor, parameterBatch, cancellationToken).ConfigureAwait(false);
        }
        cancellationToken.ThrowIfCancellationRequested();
        return await _client.GetFlightInfoAsync(options, descriptor).ConfigureAwait(false);
    }

    
    /// <summary>
    /// Binds parameters to the prepared statement by streaming the given RecordBatch to the server asynchronously.
    /// </summary>
    /// <param name="options">The <see cref="FlightCallOptions"/> for the operation, which may include timeouts, headers, and other options for the call.</param>
    /// <param name="descriptor">The <see cref="FlightDescriptor"/> that identifies the statement or command being executed.</param>
    /// <param name="parameterBatch">The <see cref="RecordBatch"/> containing the parameters to bind to the prepared statement.</param>
    /// <param name="cancellationToken">Optional <see cref="CancellationToken"/> to observe while waiting for the task to complete. The task will be canceled if the token is canceled.</param>
    /// <returns>A <see cref="Task{ByteString}"/> that represents the asynchronous operation. The task result contains the metadata from the server after binding the parameters.</returns>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="parameterBatch"/> is null.</exception>
    /// <exception cref="InvalidOperationException">Thrown if the operation is canceled or if there is an error during the DoPut operation.</exception>
    public async Task<ByteString> BindParametersAsync(FlightCallOptions options, FlightDescriptor descriptor, RecordBatch parameterBatch, CancellationToken cancellationToken = default)
    {
        if (parameterBatch == null)
        {
            throw new ArgumentNullException(nameof(parameterBatch), "Parameter batch cannot be null.");
        }

        var putResult = await _client.DoPutAsync(options, descriptor, parameterBatch.Schema).ConfigureAwait(false);

        try
        {
            var metadata = await putResult.ReadMetadataAsync().ConfigureAwait(false);
            await putResult.CompleteAsync().ConfigureAwait(false);
            return metadata;
        }
        catch (OperationCanceledException)
        {
            throw new InvalidOperationException("Parameter binding was canceled.");
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException("Failed to bind parameters to the prepared statement.", ex);
        }
    }

    /// <summary>
    /// Ensures that the statement is not already closed.
    /// </summary>
    private void EnsureStatementIsNotClosed()
    {
        if (_isClosed)
            throw new InvalidOperationException("Cannot execute a closed statement.");
    }

    /// <summary>
    /// Protected implementation of the dispose pattern.
    /// </summary>
    /// <param name="disposing">True if called from Dispose, false if called from the finalizer.</param>
    protected virtual void Dispose(bool disposing)
    {
        if (_isClosed) return;

        if (disposing)
        {
            // Close the statement if it's not already closed.
            CloseAsync(new FlightCallOptions()).GetAwaiter().GetResult();
        }

        _isClosed = true;
    }

    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }
}

public static class SchemaExtensions
{
    /// <summary>
    /// Deserializes a schema from a byte array.
    /// </summary>
    /// <param name="serializedSchema">The byte array representing the serialized schema.</param>
    /// <returns>The deserialized Schema object.</returns>
    public static Schema DeserializeSchema(byte[] serializedSchema)
    {
        if (serializedSchema == null || serializedSchema.Length == 0)
        {
            throw new ArgumentException("Invalid serialized schema");
        }

        using var stream = new MemoryStream(serializedSchema);
        var reader = new ArrowStreamReader(stream);
        return reader.Schema;
    }
}

//
// public class PreparedStatement : IDisposable
// {
//     private readonly FlightSqlClient _client;
//     private readonly FlightInfo _flightInfo;
//     private readonly string _query;
//     private bool _isClosed;
//     private readonly FlightDescriptor _descriptor;
//     private RecordBatch? _parameterBatch;
//
//     public PreparedStatement(FlightSqlClient client, FlightInfo flightInfo, string query)
//     {
//         _client = client ?? throw new ArgumentNullException(nameof(client));
//         _flightInfo = flightInfo ?? throw new ArgumentNullException(nameof(flightInfo));
//         _query = query ?? throw new ArgumentNullException(nameof(query));
//         _descriptor = flightInfo.Descriptor ?? throw new ArgumentNullException(nameof(flightInfo.Descriptor));
//         _isClosed = false;
//     }
//
//     /// <summary>
//     /// Set parameters for the prepared statement
//     /// </summary>
//     /// <param name="parameterBatch">The batch of parameters to bind</param>
//     public Task SetParameters(RecordBatch parameterBatch)
//     {
//         EnsureStatementIsNotClosed();
//         if (parameterBatch == null)
//         {
//             throw new ArgumentNullException(nameof(parameterBatch));
//         }
//
//         _parameterBatch = parameterBatch ?? throw new ArgumentNullException(nameof(parameterBatch));
//         return Task.CompletedTask;
//     }
//
//     /// <summary>
//     /// Execute the prepared statement, returning the number of affected rows
//     /// </summary>
//     /// <param name="options">The FlightCallOptions for the execution</param>
//     /// <returns>Task representing the asynchronous operation</returns>
//     public async Task<long> ExecuteUpdateAsync(FlightCallOptions options)
//     {
//         EnsureStatementIsNotClosed();
//         EnsureParametersAreSet();
//         try
//         {
//             var prepareStatementRequest = new ActionCreatePreparedStatementRequest { Query = _query };
//             var command = new CommandPreparedStatementQuery
//             {
//                 PreparedStatementHandle = prepareStatementRequest.ToByteString()
//             };
//             var descriptor = FlightDescriptor.CreateCommandDescriptor(command.PackAndSerialize());
//             var metadata = await BindParametersAsync(options, descriptor).ConfigureAwait(false);
//             await _client.ExecuteUpdateAsync(options, _query);
//
//             return ParseUpdateResult(metadata);
//         }
//         catch (RpcException ex)
//         {
//             throw new InvalidOperationException("Failed to execute update query", ex);
//         }
//     }
//
//     /// <summary>
//     /// Binds parameters to the server using DoPut and retrieves metadata.
//     /// </summary>
//     /// <param name="options">The FlightCallOptions for the execution.</param>
//     /// <param name="descriptor">The FlightDescriptor for the command.</param>
//     /// <returns>A ByteString containing metadata from the server response.</returns>
//     public async Task<ByteString> BindParametersAsync(FlightCallOptions options, FlightDescriptor descriptor)
//     {
//         if (_parameterBatch == null)
//             throw new InvalidOperationException("Parameters have not been set.");
//
//         // Start the DoPut operation
//         var doPutResult = await _client.DoPutAsync(options, descriptor, _parameterBatch.Schema);
//         var writer = doPutResult.Writer;
//
//         // Write the record batch to the stream
//         await writer.WriteAsync(_parameterBatch).ConfigureAwait(false);
//         await writer.CompleteAsync().ConfigureAwait(false);
//
//         // Read metadata from response
//         var metadata = await doPutResult.ReadMetadataAsync().ConfigureAwait(false);
//
//         // Close the writer and reader streams
//         await writer.CompleteAsync().ConfigureAwait(false);
//         await doPutResult.CompleteAsync().ConfigureAwait(false);
//         return metadata;
//     }
//
//     /// <summary>
//     /// Closes the prepared statement
//     /// </summary>
//     public async Task CloseAsync(FlightCallOptions options)
//     {
//         EnsureStatementIsNotClosed();
//         try
//         {
//             var actionClose = new FlightAction(SqlAction.CloseRequest, _flightInfo.Descriptor.Command);
//             await foreach (var result in _client.DoActionAsync(options, actionClose).ConfigureAwait(false))
//             {
//             }
//
//             _isClosed = true;
//         }
//         catch (RpcException ex)
//         {
//             throw new InvalidOperationException("Failed to close the prepared statement", ex);
//         }
//     }
//
//     /// <summary>
//     /// Parses the metadata returned from the server to get the number of affected rows.
//     /// </summary>
//     private long ParseUpdateResult(ByteString metadata)
//     {
//         var updateResult = new DoPutUpdateResult();
//         updateResult.MergeFrom(metadata);
//         return updateResult.RecordCount;
//     }
//
//     /// <summary>
//     /// Helper method to ensure the statement is not closed.
//     /// </summary>
//     private void EnsureStatementIsNotClosed()
//     {
//         if (_isClosed)
//             throw new InvalidOperationException("Cannot execute a closed statement.");
//     }
//
//     private void EnsureParametersAreSet()
//     {
//         if (_parameterBatch == null || _parameterBatch.Length == 0)
//         {
//             throw new InvalidOperationException("Prepared statement parameters have not been set.");
//         }
//     }
//
//     public void Dispose()
//     {
//         _parameterBatch?.Dispose();
//
//         if (!_isClosed)
//         {
//             _isClosed = true;
//         }
//     }
// }
//
public static class RecordBatchExtensions
{
    /// <summary>
    /// Converts a RecordBatch into an asynchronous stream of FlightData.
    /// </summary>
    /// <param name="recordBatch">The RecordBatch to convert.</param>
    /// <param name="flightDescriptor">The FlightDescriptor describing the Flight data.</param>
    /// <returns>An asynchronous stream of FlightData objects.</returns>
    public static async IAsyncEnumerable<FlightData> ToFlightDataStreamAsync(this RecordBatch recordBatch,
        FlightDescriptor flightDescriptor)
    {
        if (recordBatch == null)
        {
            throw new ArgumentNullException(nameof(recordBatch));
        }

        // Use a memory stream to write the Arrow RecordBatch into FlightData format
        using var memoryStream = new MemoryStream();
        var writer = new ArrowStreamWriter(memoryStream, recordBatch.Schema);

        // Write the RecordBatch to the stream
        await writer.WriteRecordBatchAsync(recordBatch).ConfigureAwait(false);
        await writer.WriteEndAsync().ConfigureAwait(false);

        // Reset the memory stream position
        memoryStream.Position = 0;

        // Read back the data to create FlightData
        var flightData = new FlightData(flightDescriptor, ByteString.CopyFrom(memoryStream.ToArray()),
            ByteString.CopyFrom(memoryStream.ToArray()));
        yield return flightData;
    }

    /// <summary>
    /// Converts a RecordBatch into an IAsyncStreamReader<FlightData>.
    /// </summary>
    /// <param name="recordBatch">The RecordBatch to convert.</param>
    /// <param name="flightDescriptor">The FlightDescriptor describing the Flight data.</param>
    /// <returns>An IAsyncStreamReader of FlightData.</returns>
    public static IAsyncStreamReader<FlightData> ToFlightDataStream(this RecordBatch recordBatch, FlightDescriptor flightDescriptor)
    {
        if (recordBatch == null) throw new ArgumentNullException(nameof(recordBatch));
        if (flightDescriptor == null) throw new ArgumentNullException(nameof(flightDescriptor));

        var channel = Channel.CreateUnbounded<FlightData>();

        try
        {
            if (recordBatch.Schema == null || !recordBatch.Schema.FieldsList.Any())
            {
                throw new InvalidOperationException("The record batch has an invalid or empty schema.");
            }

            using var memoryStream = new MemoryStream();
            using var writer = new ArrowStreamWriter(memoryStream, recordBatch.Schema);
            writer.WriteRecordBatch(recordBatch);
            writer.WriteEnd();
            memoryStream.Position = 0;
            var flightData = new FlightData(flightDescriptor, ByteString.CopyFrom(memoryStream.ToArray()), ByteString.Empty, ByteString.Empty);
            if (flightData.DataBody.IsEmpty)
            {
                throw new InvalidOperationException(
                    "The generated FlightData is empty. Check the RecordBatch content.");
            }

            channel.Writer.TryWrite(flightData);
        }
        finally
        {
            // Mark the channel as complete once done
            channel.Writer.Complete();
        }
        return new ChannelFlightDataReader(channel.Reader);
    }

    /*public static IAsyncStreamReader<FlightData> ToFlightDataStream(this RecordBatch recordBatch,
        FlightDescriptor flightDescriptor)
    {
        if (recordBatch == null) throw new ArgumentNullException(nameof(recordBatch));
        if (flightDescriptor == null) throw new ArgumentNullException(nameof(flightDescriptor));

        // Create a channel to act as the data stream.
        var channel = Channel.CreateUnbounded<FlightData>();

        // Start a background task to generate the FlightData asynchronously.
        _ = Task.Run(async () =>
        {
            try
            {
                // Check if the schema is set and there are fields in the RecordBatch
                if (recordBatch.Schema == null || !recordBatch.Schema.FieldsList.Any())
                {
                    throw new InvalidOperationException("The record batch has an invalid or empty schema.");
                }

                // Use a memory stream to convert the RecordBatch to FlightData
                await using var memoryStream = new MemoryStream();
                var writer = new ArrowStreamWriter(memoryStream, recordBatch.Schema);

                // Write the RecordBatch to the memory stream
                await writer.WriteRecordBatchAsync(recordBatch).ConfigureAwait(false);
                await writer.WriteEndAsync().ConfigureAwait(false);

                // Reset the memory stream position to read from it
                memoryStream.Position = 0;

                // Read back the data from the stream and create FlightData
                var flightData = new FlightData(
                    flightDescriptor,
                    ByteString.CopyFrom(memoryStream.ToArray()), // Use the data from memory stream
                    ByteString.Empty // Empty application metadata for now
                );

                // Check if flightData has valid data before writing
                if (flightData.DataBody.IsEmpty)
                {
                    throw new InvalidOperationException(
                        "The generated FlightData is empty. Check the RecordBatch content.");
                }

                // Write the FlightData to the channel
                await channel.Writer.WriteAsync(flightData).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                // Log any exceptions for debugging purposes
                Console.WriteLine($"Error generating FlightData: {ex.Message}");
            }
            finally
            {
                // Mark the channel as complete once done
                channel.Writer.Complete();
            }
        });

        // Return a custom IAsyncStreamReader implementation.
        return new ChannelFlightDataReader(channel.Reader);
    }*/


    /// <summary>
    /// Custom IAsyncStreamReader<FlightData> implementation to read from a ChannelReader<FlightData>.
    /// </summary>
    private class ChannelFlightDataReader : IAsyncStreamReader<FlightData>
    {
        private readonly ChannelReader<FlightData> _channelReader;

        public ChannelFlightDataReader(ChannelReader<FlightData> channelReader)
        {
            _channelReader = channelReader ?? throw new ArgumentNullException(nameof(channelReader));
            Current = default!;
        }

        public FlightData Current { get; private set; }

        public async Task<bool> MoveNext(CancellationToken cancellationToken)
        {
            if (await _channelReader.WaitToReadAsync(cancellationToken).ConfigureAwait(false))
            {
                if (_channelReader.TryRead(out var flightData))
                {
                    Current = flightData;
                    return true;
                }
            }

            return false;
        }

        public void Dispose()
        {
            // No additional cleanup is required here since we're not managing external resources.
        }
    }
}

public class ChannelReaderStreamAdapter<T> : IAsyncStreamReader<T>
{
    private readonly ChannelReader<T> _channelReader;

    public ChannelReaderStreamAdapter(ChannelReader<T> channelReader)
    {
        _channelReader = channelReader ?? throw new ArgumentNullException(nameof(channelReader));
        Current = default!;
    }

    public T Current { get; private set; }

    public async Task<bool> MoveNext(CancellationToken cancellationToken)
    {
        if (await _channelReader.WaitToReadAsync(cancellationToken).ConfigureAwait(false))
        {
            if (_channelReader.TryRead(out var item))
            {
                Current = item;
                return true;
            }
        }

        return false;
    }

    public void Dispose()
    {
        // No additional cleanup is required here since we are using a channel
    }
}