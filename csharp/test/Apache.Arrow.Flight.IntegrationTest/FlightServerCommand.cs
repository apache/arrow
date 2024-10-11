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
using Apache.Arrow.Flight.TestWeb;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

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
        if (!string.IsNullOrEmpty(_scenario))
        {
            // No named scenarios are currently implemented
            throw new Exception($"Scenario '{_scenario}' is not supported.");
        }

        var host = Host.CreateDefaultBuilder()
            .ConfigureWebHostDefaults(webBuilder =>
            {
                webBuilder
                    .ConfigureKestrel(options =>
                    {
                        options.Listen(IPEndPoint.Parse("127.0.0.1:0"), l => l.Protocols = HttpProtocols.Http2);
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
