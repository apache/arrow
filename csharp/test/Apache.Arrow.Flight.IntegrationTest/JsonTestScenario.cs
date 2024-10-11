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
using Apache.Arrow.IntegrationTest;
using Apache.Arrow.Tests;
using Apache.Arrow.Types;
using Google.Protobuf;
using Grpc.Net.Client;
using Grpc.Core;
using Grpc.Net.Client.Balancer;
using Microsoft.Extensions.DependencyInjection;

namespace Apache.Arrow.Flight.IntegrationTest;

/// <summary>
/// A test scenario defined using a JSON file
/// </summary>
internal class JsonTestScenario : Scenario
{
    private readonly int _port;
    private readonly FileInfo _jsonFile;
    private readonly ServiceProvider _serviceProvider;

    public JsonTestScenario(int port, FileInfo jsonFile)
    {
        _port = port;
        _jsonFile = jsonFile;

        var services = new ServiceCollection();
        services.AddSingleton<ResolverFactory>(new GrpcTcpResolverFactory());
        _serviceProvider = services.BuildServiceProvider();
    }

    public override async Task RunClient()
    {
        var address = $"grpc+tcp://localhost:{_port}";
        using var channel = GrpcChannel.ForAddress(
            address,
            new GrpcChannelOptions
            {
                ServiceProvider = _serviceProvider,
                Credentials = ChannelCredentials.Insecure
            });
        var client = new FlightClient(channel);

        var descriptor = FlightDescriptor.CreatePathDescriptor(_jsonFile.FullName);

        var jsonFile = await JsonFile.ParseAsync(_jsonFile);
        var schema = jsonFile.GetSchemaAndDictionaries(out Func<DictionaryType, IArrowArray> dictionaries);
        var batches = jsonFile.Batches.Select(batch => batch.ToArrow(schema, dictionaries)).ToArray();

        // 1. Put the data to the server.
        await UploadBatches(client, descriptor, batches);

        // 2. Get the ticket for the data.
        var info = await client.GetInfo(descriptor);
        if (info.Endpoints.Count == 0)
        {
            throw new Exception("No endpoints received");
        }

        foreach (var endpoint in info.Endpoints)
        {
            var locations = endpoint.Locations.ToArray();
            if (locations.Length == 0)
            {
                // Can read with existing client
                await ConsumeFlightLocation(client, endpoint.Ticket, batches);
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
                    await ConsumeFlightLocation(readClient, endpoint.Ticket, batches);
                }
            }
        }
    }

    private static async Task UploadBatches(FlightClient client, FlightDescriptor descriptor, RecordBatch[] batches)
    {
        using var putCall = client.StartPut(descriptor);
        using var writer = putCall.RequestStream;

        try
        {
            var counter = 0;
            foreach (var batch in batches)
            {
                var metadata = $"{counter}";

                await writer.WriteAsync(batch, ByteString.CopyFromUtf8(metadata));

                // Verify server has acknowledged the write request
                await putCall.ResponseStream.MoveNext();
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
            await writer.CompleteAsync();
        }
    }

    private static async Task ConsumeFlightLocation(FlightClient client, FlightTicket ticket, RecordBatch[] batches)
    {
        using var readStream = client.GetStream(ticket);
        var counter = 0;
        foreach (var originalBatch in batches)
        {
            if (!await readStream.ResponseStream.MoveNext())
            {
                throw new Exception($"Expected {batches.Length} batches but received {counter}");
            }

            var batch = readStream.ResponseStream.Current;
            ArrowReaderVerifier.CompareBatches(originalBatch, batch, strictCompare: false);

            counter++;
        }
    }
}
