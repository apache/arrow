using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Apache.Arrow.Ipc;
using Google.Protobuf;
using Grpc.Core;

namespace Apache.Arrow.Flight.Sql;

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