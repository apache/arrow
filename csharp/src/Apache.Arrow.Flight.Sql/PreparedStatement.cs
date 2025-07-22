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
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Flight.Sql.Client;
using Arrow.Flight.Protocol.Sql;
using Google.Protobuf;
using Grpc.Core;

namespace Apache.Arrow.Flight.Sql;

public class PreparedStatement : IDisposable, IAsyncDisposable
{
    private readonly FlightSqlClient _client;
    private readonly string _handle;
    private RecordBatch? _recordsBatch;
    private bool _isClosed;
    public Schema DatasetSchema { get; }
    public Schema ParameterSchema { get; }
    public bool IsClosed => _isClosed;
    public string Handle => _handle;
    public RecordBatch? ParametersBatch => _recordsBatch;

    /// <summary>
    /// Initializes a new instance of the <see cref="PreparedStatement"/> class.
    /// </summary>
    /// <param name="client">The Flight SQL client used for executing SQL operations.</param>
    /// <param name="handle">The handle representing the prepared statement.</param>
    /// <param name="datasetSchema">The schema of the result dataset.</param>
    /// <param name="parameterSchema">The schema of the parameters for this prepared statement.</param>
    public PreparedStatement(FlightSqlClient client, string handle, Schema datasetSchema, Schema parameterSchema)
    {
        _client = client ?? throw new ArgumentNullException(nameof(client));
        _handle = handle ?? throw new ArgumentNullException(nameof(handle));
        DatasetSchema = datasetSchema ?? throw new ArgumentNullException(nameof(datasetSchema));
        ParameterSchema = parameterSchema ?? throw new ArgumentNullException(nameof(parameterSchema));
        _isClosed = false;
    }

    /// <summary>
    /// Retrieves the schema associated with the prepared statement asynchronously.
    /// </summary>
    /// <param name="options">The options used to configure the Flight call.</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>A task representing the asynchronous operation, which returns the schema of the result set.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the schema is empty or invalid.</exception>
    public async Task<Schema> GetSchemaAsync(FlightCallOptions? options = default, CancellationToken cancellationToken = default)
    {
        EnsureStatementIsNotClosed();

        try
        {
            var command = new CommandPreparedStatementQuery
            {
                PreparedStatementHandle = ByteString.CopyFrom(_handle, Encoding.UTF8)
            };
            var descriptor = FlightDescriptor.CreateCommandDescriptor(command.PackAndSerialize());
            var schema = await _client.GetSchemaAsync(descriptor, options, cancellationToken).ConfigureAwait(false);
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
    /// <param name="options">The options used to configure the Flight call.</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    /// <exception cref="InvalidOperationException">Thrown if closing the prepared statement fails.</exception>
    public async Task CloseAsync(FlightCallOptions? options = default, CancellationToken cancellationToken = default)
    {
        EnsureStatementIsNotClosed();
        try
        {
            var closeRequest = new ActionClosePreparedStatementRequest
            {
                PreparedStatementHandle = ByteString.CopyFrom(_handle, Encoding.UTF8)
            };

            var action = new FlightAction(SqlAction.CloseRequest, closeRequest.PackAndSerialize());
            await foreach (var result in _client.DoActionAsync(action, options, cancellationToken).ConfigureAwait(false))
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
    /// <param name="results">The asynchronous stream of <see cref="FlightData"/> objects.</param>
    /// <param name="message">The Protobuf message to populate with the data from the stream.</param>
    /// <returns>A task that represents the asynchronous read operation.</returns>
    /// <exception cref="ArgumentNullException">Thrown if <paramref name="results"/> or <paramref name="message"/> is null.</exception>
    /// <exception cref="InvalidOperationException">Thrown if parsing the data fails.</exception>
    public async Task ReadResultAsync(IAsyncEnumerable<FlightData> results, IMessage message)
    {
        if (results == null) throw new ArgumentNullException(nameof(results));
        if (message == null) throw new ArgumentNullException(nameof(message));

        await foreach (var flightData in results.ConfigureAwait(false))
        {
            if (flightData.DataBody == null || flightData.DataBody.Length == 0)
                continue;

            try
            {
                message.MergeFrom(message.PackAndSerialize());
            }
            catch (InvalidProtocolBufferException ex)
            {
                throw new InvalidOperationException("Failed to parse the received FlightData into the specified message.", ex);
            }
        }
    }

    /// <summary>
    /// Parses the response of a prepared statement execution from the FlightData stream.
    /// </summary>
    /// <param name="client">The Flight SQL client.</param>
    /// <param name="results">The asynchronous stream of <see cref="FlightData"/> objects.</param>
    /// <returns>A task representing the asynchronous operation, which returns the populated <see cref="PreparedStatement"/>.</returns>
    /// <exception cref="ArgumentNullException">Thrown if <paramref name="client"/> or <paramref name="results"/> is null.</exception>
    /// <exception cref="InvalidOperationException">Thrown if the prepared statement handle or data is invalid.</exception>
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
                throw new InvalidOperationException("Failed to parse FlightData into ActionCreatePreparedStatementResult.", ex);
            }
        }

        if (preparedStatementResult.PreparedStatementHandle.Length == 0)
        {
            throw new InvalidOperationException("Received an empty or invalid PreparedStatementHandle.");
        }

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

    /// <summary>
    /// Binds the specified parameter batch to the prepared statement and returns the status.
    /// </summary>
    /// <param name="parameterBatch">The <see cref="RecordBatch"/> containing parameters to bind to the statement.</param>
    /// <param name="cancellationToken">A cancellation token for the binding operation.</param>
    /// <returns>A <see cref="Status"/> indicating success or failure.</returns>
    /// <exception cref="ArgumentNullException">Thrown if <paramref name="parameterBatch"/> is null.</exception>
    public void SetParameters(RecordBatch parameterBatch)
    {
        _recordsBatch = parameterBatch ?? throw new ArgumentNullException(nameof(parameterBatch));
    }

    /// <summary>
    /// Executes the prepared statement asynchronously and retrieves the query results as <see cref="FlightInfo"/>.
    /// </summary>
    /// <param name="options">Optional <see cref="FlightCallOptions"/>The <see cref="FlightCallOptions"/> for the operation, which may include timeouts, headers, and other options for the call.</param>
    /// <param name="cancellationToken">Optional <see cref="CancellationToken"/> to observe while waiting for the task to complete. The task will be canceled if the token is canceled.</param>
    /// <returns>A <see cref="Task{FlightInfo}"/> representing the asynchronous operation. The task result contains the <see cref="FlightInfo"/> describing the executed query results.</returns>
    /// <exception cref="InvalidOperationException">Thrown if the prepared statement is closed or if there is an error during execution.</exception>
    /// <exception cref="OperationCanceledException">Thrown if the operation is canceled by the <paramref name="cancellationToken"/>.</exception>
    public async Task<FlightInfo> ExecuteAsync(FlightCallOptions? options = default, CancellationToken cancellationToken = default)
    {
        EnsureStatementIsNotClosed();

        var command = new CommandPreparedStatementQuery
        {
            PreparedStatementHandle = ByteString.CopyFrom(_handle, Encoding.UTF8),
        };
        
        var descriptor = FlightDescriptor.CreateCommandDescriptor(command.PackAndSerialize());
        cancellationToken.ThrowIfCancellationRequested();
        
        if (_recordsBatch != null)
        {
            await BindParametersAsync(descriptor, _recordsBatch, options, cancellationToken).ConfigureAwait(false);
        }
        cancellationToken.ThrowIfCancellationRequested();
        return await _client.GetFlightInfoAsync(descriptor, options, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Executes a prepared update statement asynchronously with the provided parameter batch.
    /// </summary>
    /// <remarks>
    /// This method executes an update operation using a prepared statement. The provided <paramref name="parameterBatch"/> 
    /// is bound to the statement, and the operation is sent to the server. The server processes the update and returns 
    /// metadata indicating the number of affected rows.
    /// 
    /// This operation is asynchronous and can be canceled via the provided <paramref name="cancellationToken"/>.
    /// </remarks>
    /// <param name="parameterBatch">
    /// A <see cref="RecordBatch"/> containing the parameters to be bound to the update statement. 
    /// This batch should match the schema expected by the prepared statement.
    /// </param>
    /// <param name="options">The <see cref="FlightCallOptions"/> for this execution, containing headers and other options.</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>
    /// A <see cref="Task{TResult}"/> representing the asynchronous operation. 
    /// The task result contains the number of rows affected by the update.
    /// </returns>
    /// <exception cref="ArgumentNullException">
    /// Thrown if <paramref name="parameterBatch"/> is null, as a valid parameter batch is required for execution.
    /// </exception>
    /// <exception cref="InvalidOperationException">
    /// Thrown if the update operation fails for any reason, including when the server returns invalid or empty metadata, 
    /// or if the operation is canceled via the <paramref name="cancellationToken"/>.
    /// </exception>
    /// <example>
    /// The following example demonstrates how to use the <see cref="ExecuteUpdateAsync"/> method to execute an update operation:
    /// <code>
    /// var parameterBatch = CreateParameterBatch();
    /// var affectedRows = await preparedStatement.ExecuteUpdateAsync(new FlightCallOptions(), parameterBatch);
    /// Console.WriteLine($"Rows affected: {affectedRows}");
    /// </code>
    /// </example>
    public async Task<long> ExecuteUpdateAsync(
        RecordBatch parameterBatch,
        FlightCallOptions? options = default,
        CancellationToken cancellationToken = default)
    {
        if (parameterBatch == null)
        {
            throw new ArgumentNullException(nameof(parameterBatch), "Parameter batch cannot be null.");
        }
        
        var command = new CommandPreparedStatementQuery
        {
            PreparedStatementHandle = ByteString.CopyFrom(_handle, Encoding.UTF8),
        };

        var descriptor = FlightDescriptor.CreateCommandDescriptor(command.PackAndSerialize());
        var metadata = await BindParametersAsync(descriptor, parameterBatch, options, cancellationToken).ConfigureAwait(false);

        try
        {
            return ParseAffectedRows(metadata);
        }
        catch (OperationCanceledException)
        {
            throw new InvalidOperationException("Update operation was canceled.");
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException("Failed to execute the prepared update statement.", ex);
        }
    }

    private long ParseAffectedRows(ByteString metadata)
    {
        if (metadata == null || metadata.Length == 0)
        {
            throw new InvalidOperationException("Server returned empty metadata, unable to determine affected row count.");
        }

        var updateResult = new DoPutUpdateResult();
        updateResult.MergeFrom(metadata);
        return updateResult.RecordCount;
    }

    /// <summary>
    /// Binds parameters to the prepared statement by streaming the given RecordBatch to the server asynchronously.
    /// </summary>
    /// <param name="descriptor">The <see cref="FlightDescriptor"/> that identifies the statement or command being executed.</param>
    /// <param name="parameterBatch">The <see cref="RecordBatch"/> containing the parameters to bind to the prepared statement.</param>
    /// <param name="options">The <see cref="FlightCallOptions"/> for the operation, which may include timeouts, headers, and other options for the call.</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>A <see cref="Task{ByteString}"/> that represents the asynchronous operation. The task result contains the metadata from the server after binding the parameters.</returns>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="parameterBatch"/> is null.</exception>
    /// <exception cref="InvalidOperationException">Thrown if the operation is canceled or if there is an error during the DoPut operation.</exception>
    public async Task<ByteString> BindParametersAsync(
        FlightDescriptor descriptor,
        RecordBatch parameterBatch,
        FlightCallOptions? options = default,
        CancellationToken cancellationToken = default)
    {
        if (parameterBatch == null)
        {
            throw new ArgumentNullException(nameof(parameterBatch), @"Parameter batch cannot be null.");
        }
        var putResult = await _client.DoPutAsync(descriptor, parameterBatch, options, cancellationToken).ConfigureAwait(false);
        try
        {
            var metadata = putResult.ApplicationMetadata;
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
    /// Disposes of the resources used by the prepared statement.
    /// </summary>
    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }

    /// <summary>
    /// Disposes of the resources used by the prepared statement.
    /// </summary>
    /// <param name="disposing">Whether the method is called from <see cref="Dispose()"/>.</param>
    protected virtual void Dispose(bool disposing)
    {
        if (_isClosed) return;

        if (disposing)
        {
            DisposeAsync().GetAwaiter().GetResult();
        }

        _isClosed = true;
    }

    public async ValueTask DisposeAsync()
    {
        if (!_isClosed)
        {
            await CloseAsync(new FlightCallOptions());
            _isClosed = true;
        }
    }
}
