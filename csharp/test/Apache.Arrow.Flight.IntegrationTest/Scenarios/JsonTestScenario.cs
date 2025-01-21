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
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Apache.Arrow.Flight.Client;
using Apache.Arrow.Flight.Server;
using Apache.Arrow.IntegrationTest;
using Apache.Arrow.Tests;
using Apache.Arrow.Types;
using Google.Protobuf;
using Grpc.Net.Client;
using Grpc.Core;
using Grpc.Net.Client.Balancer;
using Microsoft.Extensions.DependencyInjection;

namespace Apache.Arrow.Flight.IntegrationTest.Scenarios;

/// <summary>
/// A test scenario defined using a JSON data file
/// </summary>
internal class JsonTestScenario : IScenario
{
    private readonly FileInfo _jsonFile;
    private readonly ServiceProvider _serviceProvider;

    public JsonTestScenario(FileInfo jsonFile)
    {
        if (!(jsonFile?.Exists ?? false))
        {
            throw new Exception($"Invalid JSON file path '{jsonFile?.FullName}'");
        }

        _jsonFile = jsonFile;

        var services = new ServiceCollection();
        services.AddSingleton<ResolverFactory>(new GrpcTcpResolverFactory());
        _serviceProvider = services.BuildServiceProvider();
    }

    public FlightServer MakeServer()
    {
        throw new NotImplementedException();
    }

    public async Task RunClient(int serverPort)
    {
        var address = $"grpc+tcp://localhost:{serverPort}";
        using var channel = GrpcChannel.ForAddress(
            address,
            new GrpcChannelOptions
            {
                ServiceProvider = _serviceProvider,
                Credentials = ChannelCredentials.Insecure
            });
        var client = new FlightClient(channel);

        var descriptor = FlightDescriptor.CreatePathDescriptor(_jsonFile.FullName);

        var jsonFile = await JsonFile.ParseAsync(_jsonFile).ConfigureAwait(false);
        var schema = jsonFile.GetSchemaAndDictionaries(out Func<DictionaryType, IArrowArray> dictionaries);
        var batches = jsonFile.Batches.Select(batch => batch.ToArrow(schema, dictionaries)).ToArray();

        // 1. Put the data to the server.
        await UploadBatches(client, descriptor, schema, batches).ConfigureAwait(false);

        // 2. Get the ticket for the data.
        var info = await client.GetInfo(descriptor).ConfigureAwait(false);
        if (info.Endpoints.Count == 0)
        {
            throw new Exception("No endpoints received");
        }

        // 3. Stream data from the server, comparing individual batches.
        foreach (var endpoint in info.Endpoints)
        {
            var locations = endpoint.Locations.ToArray();
            if (locations.Length == 0)
            {
                // Can read with existing client
                await ConsumeFlightLocation(client, endpoint.Ticket, batches).ConfigureAwait(false);
            }
            else
            {
                foreach (var location in locations)
                {
                    using var readChannel = GrpcChannel.ForAddress(
                        location.Uri,
                        new GrpcChannelOptions
                        {
                            ServiceProvider = _serviceProvider,
                            Credentials = ChannelCredentials.Insecure
                        });
                    var readClient = new FlightClient(readChannel);
                    await ConsumeFlightLocation(readClient, endpoint.Ticket, batches).ConfigureAwait(false);
                }
            }
        }
    }

    private static async Task UploadBatches(
        FlightClient client, FlightDescriptor descriptor, Schema schema, RecordBatch[] batches)
    {
        using var putCall = await client.StartPut(descriptor, schema);
        using var writer = putCall.RequestStream;

        try
        {
            var counter = 0;
            foreach (var batch in batches)
            {
                var metadata = $"{counter}";

                await writer.WriteAsync(batch, ByteString.CopyFromUtf8(metadata)).ConfigureAwait(false);

                // Verify server has acknowledged the write request
                await putCall.ResponseStream.MoveNext().ConfigureAwait(false);
                var responseString = putCall.ResponseStream.Current.ApplicationMetadata.ToStringUtf8();

                if (responseString != metadata)
                {
                    throw new Exception($"Response metadata '{responseString}' does not match expected metadata '{metadata}'");
                }

                counter++;
            }
        }
        finally
        {
            await writer.CompleteAsync().ConfigureAwait(false);
        }

        // Drain the response stream to ensure the server has stored the data
        var hasMore = await putCall.ResponseStream.MoveNext().ConfigureAwait(false);
        if (hasMore)
        {
            throw new Exception("Expected to have reached the end of the response stream");
        }
    }

    private static async Task ConsumeFlightLocation(FlightClient client, FlightTicket ticket, RecordBatch[] batches)
    {
        using var readStream = client.GetStream(ticket);
        var counter = 0;
        foreach (var originalBatch in batches)
        {
            if (!await readStream.ResponseStream.MoveNext().ConfigureAwait(false))
            {
                throw new Exception($"Expected {batches.Length} batches but received {counter}");
            }

            var batch = readStream.ResponseStream.Current;
            ArrowReaderVerifier.CompareBatches(originalBatch, batch, strictCompare: false);

            counter++;
        }

        if (await readStream.ResponseStream.MoveNext().ConfigureAwait(false))
        {
            throw new Exception($"Expected to reach the end of the response stream after {batches.Length} batches");
        }
    }
}
