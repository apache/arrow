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

using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Flight.Client;
using Grpc.Core;

namespace Apache.Arrow.Flight.Sql;

public class DoPutResult
{
    public FlightClientRecordBatchStreamWriter Writer { get; }
    public IAsyncStreamReader<FlightPutResult> Reader { get; }

    public DoPutResult(FlightClientRecordBatchStreamWriter writer, IAsyncStreamReader<FlightPutResult> reader)
    {
        Writer = writer;
        Reader = reader;
    }
    
    /// <summary>
    /// Reads the metadata asynchronously from the reader.
    /// </summary>
    /// <returns>A ByteString containing the metadata read from the reader.</returns>
    public async Task<Google.Protobuf.ByteString> ReadMetadataAsync(CancellationToken cancellationToken = default)
    {
        if (await Reader.MoveNext(cancellationToken).ConfigureAwait(false))
        {
            return Reader.Current.ApplicationMetadata;
        }
        throw new RpcException(new Status(StatusCode.Internal, "No metadata available in the response stream."));
    }
    
    /// <summary>
    /// Completes the writer by signaling the end of the writing process.
    /// </summary>
    /// <returns>A Task representing the completion of the writer.</returns>
    public async Task CompleteAsync()
    {
        await Writer.CompleteAsync().ConfigureAwait(false);
    }
}
