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

using System.CommandLine;
using System.IO;
using System.Threading.Tasks;

namespace Apache.Arrow.Flight.IntegrationTest;

public static class Program
{
    public static async Task<int> Main(string[] args)
    {
        var portOption = new Option<int>(
            new[] { "--port", "-p" },
            description: "Port the Flight server is listening on");
        var scenarioOption = new Option<string>(
            new[] { "--scenario", "-s" },
            "The name of the scenario to run");
        var pathOption = new Option<FileInfo>(
            new[] { "--path", "-j" },
            "Path to a JSON file of test data");

        var rootCommand = new RootCommand(
            "Integration test application for Apache.Arrow .NET Flight.");

        var clientCommand = new Command("client", "Run the Flight client")
        {
            portOption,
            scenarioOption,
            pathOption,
        };
        rootCommand.AddCommand(clientCommand);

        clientCommand.SetHandler(async (port, scenario, jsonFile) =>
        {
            var command = new FlightClientCommand(port, scenario, jsonFile);
            await command.Execute().ConfigureAwait(false);
        }, portOption, scenarioOption, pathOption);

        var serverCommand = new Command("server", "Run the Flight server")
        {
            scenarioOption,
        };
        rootCommand.AddCommand(serverCommand);

        serverCommand.SetHandler(async scenario =>
        {
            var command = new FlightServerCommand(scenario);
            await command.Execute().ConfigureAwait(false);
        }, scenarioOption);

        return await rootCommand.InvokeAsync(args).ConfigureAwait(false);
    }
}
