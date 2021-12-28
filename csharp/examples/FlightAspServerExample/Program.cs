using FlightAspServerExample.Services;
using Microsoft.AspNetCore.Server.Kestrel.Core;

var builder = WebApplication.CreateBuilder(args);

// Additional configuration is required to successfully run gRPC on macOS.
// For instructions on how to configure Kestrel and gRPC clients on macOS, visit https://go.microsoft.com/fwlink/?linkid=2099682
if (builder.Environment.IsDevelopment())
{
    builder.WebHost.ConfigureKestrel(options =>
    {
        // Setup a HTTP/2 endpoint without TLS.
        options.ListenLocalhost(5000, o => o.Protocols =
            HttpProtocols.Http2);
    });
}

// There may be multiple instances of InMemoryFlightServer, so we need a singleton instance
// of the data store.
builder.Services.AddSingleton<FlightData>();

// Add services to the container.
var grpcBuilder = builder.Services.AddGrpc();
grpcBuilder.AddFlightServer<InMemoryFlightServer>();

var app = builder.Build();
// Configure the HTTP request pipeline.
app.MapGrpcService<GreeterService>();
app.MapFlightEndpoint();
app.MapGet("/", () => "Communication with gRPC endpoints must be made through a gRPC client. To learn how to create a client, visit: https://go.microsoft.com/fwlink/?linkid=2086909");

app.Run();
