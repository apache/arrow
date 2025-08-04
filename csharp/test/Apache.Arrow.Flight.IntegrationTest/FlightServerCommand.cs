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
using System.Net;
using System.Threading.Tasks;
using Apache.Arrow.Flight.IntegrationTest.Scenarios;
using Apache.Arrow.Flight.Server;
using Apache.Arrow.Flight.TestWeb;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Console;

namespace Apache.Arrow.Flight.IntegrationTest;

public class FlightServerCommand
{
    private readonly string _scenario;

    public FlightServerCommand(string scenario)
    {
        _scenario = scenario;
    }

    public async Task Execute()
    {
        IScenario scenario = _scenario switch
        {
            null => null,
            "do_exchange:echo" => new DoExchangeEchoScenario(),
            _ => throw new NotSupportedException($"Scenario {_scenario} is not supported")
        };

        var host = Host.CreateDefaultBuilder()
            .ConfigureWebHostDefaults(webBuilder =>
            {
                webBuilder
                    .ConfigureKestrel(options =>
                    {
                        options.Listen(IPEndPoint.Parse("127.0.0.1:0"), l => l.Protocols = HttpProtocols.Http2);
                    })
                    .ConfigureServices(services =>
                    {
                        if (scenario == null)
                        {
                            // Use the TestFlightServer for JSON based integration tests
                            services.AddGrpc().AddFlightServer<TestFlightServer>();
                            services.AddSingleton(new FlightStore());
                        }
                        else
                        {
                            // Use a scenario-specific server implementation
                            services.AddGrpc().Services.AddScoped<FlightServer>(_ => scenario.MakeServer());
                        }

                        // The integration tests rely on the port being written to the first line of stdout,
                        // so send all logging to stderr.
                        services.Configure<ConsoleLoggerOptions>(
                            o => o.LogToStandardErrorThreshold = LogLevel.Debug);

                    })
                    .UseStartup<Startup>();
            })
            .Build();

        await host.StartAsync().ConfigureAwait(false);

        var addresses = host.Services.GetService<IServer>().Features.Get<IServerAddressesFeature>().Addresses;
        foreach (var address in addresses)
        {
            Console.WriteLine($"Server listening on {address}");
        }

        await host.WaitForShutdownAsync().ConfigureAwait(false);
    }
}
