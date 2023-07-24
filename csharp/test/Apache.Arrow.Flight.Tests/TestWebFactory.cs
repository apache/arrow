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
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using Apache.Arrow.Flight.TestWeb;
using Grpc.Net.Client;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace Apache.Arrow.Flight.Tests
{
    public class TestWebFactory : IDisposable
    {
        readonly IHost host;
        private int _port;

        public TestWebFactory(FlightStore flightStore)
        {
            host = WebHostBuilder(flightStore).Build(); //Create the server
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
                            .ConfigureKestrel(c =>
                            {
                                c.ListenAnyIP(0, l => l.Protocols = HttpProtocols.Http2);
                            })
                            .UseStartup<Startup>()
                            .ConfigureServices(services =>
                            {
                                services.AddSingleton(flightStore);
                            });
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
}
