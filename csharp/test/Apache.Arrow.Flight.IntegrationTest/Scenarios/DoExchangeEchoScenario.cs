using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Flight.Client;
using Apache.Arrow.Flight.Server;
using Google.Protobuf;
using Grpc.Core;
using Grpc.Net.Client;
using Grpc.Net.Client.Balancer;
using Microsoft.Extensions.DependencyInjection;

namespace Apache.Arrow.Flight.IntegrationTest.Scenarios;

internal class DoExchangeServer : FlightServer
{
    public override async Task DoExchange(
        FlightServerRecordBatchStreamReader requestStream,
        FlightServerRecordBatchStreamWriter responseStream,
        ServerCallContext context)
    {
        var descriptor = await requestStream.FlightDescriptor;
        var command = descriptor.Command?.ToStringUtf8();
        if (command != "echo")
        {
            throw new Exception($"Unsupported command: '{command}'");
        }

        while (await requestStream.MoveNext())
        {
            await responseStream.WriteAsync(
                requestStream.Current, requestStream.ApplicationMetadata.FirstOrDefault());
        }
    }
}

internal class DoExchangeEchoScenario : IScenario
{
    public FlightServer MakeServer() => new DoExchangeServer();

    public async Task RunClient(int serverPort)
    {
        var services = new ServiceCollection();
        services.AddSingleton<ResolverFactory>(new GrpcTcpResolverFactory());
        var serviceProvider = services.BuildServiceProvider();

        var address = $"grpc+tcp://localhost:{serverPort}";
        using var channel = GrpcChannel.ForAddress(
            address,
            new GrpcChannelOptions
            {
                ServiceProvider = serviceProvider,
                Credentials = ChannelCredentials.Insecure
            });

        var client = new FlightClient(channel);
        var descriptor = FlightDescriptor.CreateCommandDescriptor("echo");
        using var exchange = client.DoExchange(descriptor);

        using var writer = exchange.RequestStream;
        using var reader = exchange.ResponseStream;

        for (var batchIdx = 0; batchIdx < 4; batchIdx++)
        {
            using var batch = new RecordBatch.Builder()
                .Append(
                    "x",
                    nullable: false,
                    array: new Int32Array.Builder().AppendRange(Enumerable.Range(batchIdx, 10)).Build())
                .Build();

            var expectedMetadata = $"{batchIdx}";
            var writeMetadata = batchIdx % 2 == 0;
            if (writeMetadata)
            {
                await writer.WriteAsync(batch, ByteString.CopyFromUtf8(expectedMetadata));
            }
            else
            {
                await writer.WriteAsync(batch);
            }

            if (!await reader.MoveNext(CancellationToken.None))
            {
                throw new Exception("Unexpected end of read stream");
            }

            var readMetadata = reader.ApplicationMetadata?.FirstOrDefault()?.ToStringUtf8();

            if (writeMetadata && readMetadata != expectedMetadata)
            {
                throw new Exception($"Expected metadata '{expectedMetadata}' but received '{readMetadata}'");
            }
            if (!writeMetadata && readMetadata != null)
            {
                throw new Exception($"Unexpected metadata received: '{readMetadata}'");
            }
        }

        await writer.CompleteAsync();

        if (await reader.MoveNext(CancellationToken.None))
        {
            throw new Exception("Expected end of read stream");
        }
    }
}
