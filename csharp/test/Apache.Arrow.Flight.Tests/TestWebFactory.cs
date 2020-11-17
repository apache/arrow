using System;
using System.Collections.Generic;
using System.Net;
using System.Text;
using Apache.Arrow.Flight.TestWeb;
using Grpc.Net.Client;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace Apache.Arrow.Flight.Tests
{
    public class TestWebFactory : IDisposable
    {
        readonly IHost host;

        public TestWebFactory(FlightStore flightStore)
        {
            host = WebHostBuilder(flightStore).Build(); //Create the server
            host.Start();
            AppContext.SetSwitch(
                "System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true);
        }

        private IHostBuilder WebHostBuilder(FlightStore flightStore)
        {
            return Host.CreateDefaultBuilder()
                        .ConfigureWebHostDefaults(webBuilder =>
                        {
                            webBuilder
                            .ConfigureKestrel(c =>
                            {
                                c.Listen(IPEndPoint.Parse("0.0.0.0:5001"), l => l.Protocols = HttpProtocols.Http2);
                            })
                            .UseStartup<Startup>()
                            .ConfigureServices(services =>
                            {
                                services.AddSingleton(flightStore);
                            });
                        });
        }

        public GrpcChannel GetChannel()
        {
            return GrpcChannel.ForAddress("http://127.0.0.1:5001");
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
}
