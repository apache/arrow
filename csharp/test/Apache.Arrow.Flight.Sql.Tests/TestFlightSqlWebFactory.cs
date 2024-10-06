using System;
using System.Linq;
using Apache.Arrow.Flight.TestWeb;
using Grpc.Net.Client;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace Apache.Arrow.Flight.Sql.Tests;

public class TestFlightSqlWebFactory : IDisposable
{
    readonly IHost host;
    private int _port;

    public TestFlightSqlWebFactory(FlightStore flightStore)
    {
        host = WebHostBuilder(flightStore).Build();
        host.Start();
        var addressInfo = host.Services.GetRequiredService<IServer>().Features.Get<IServerAddressesFeature>();
        if (addressInfo == null)
        {
            throw new Exception("No address info could be found for configured server");
        }

        var address = addressInfo.Addresses.First();
        var addressUri = new Uri(address);
        _port = addressUri.Port;
        AppContext.SetSwitch(
            "System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true);
    }

    private IHostBuilder WebHostBuilder(FlightStore flightStore)
    {
        return Host.CreateDefaultBuilder()
            .ConfigureWebHostDefaults(webBuilder =>
            {
                webBuilder
                    .ConfigureKestrel(c => { c.ListenAnyIP(0, l => l.Protocols = HttpProtocols.Http2); })
                    .UseStartup<StartupFlightSql>()
                    .ConfigureServices(services => { services.AddSingleton(flightStore); });
            });
    }

    public string GetAddress()
    {
        return $"http://127.0.0.1:{_port}";
    }

    public GrpcChannel GetChannel()
    {
        return GrpcChannel.ForAddress(GetAddress());
    }

    public void Stop()
    {
        host.StopAsync().Wait();
    }

    public void Dispose()
    {
        Stop();
    }
}