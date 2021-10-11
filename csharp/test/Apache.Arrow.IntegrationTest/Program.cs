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

using Apache.Arrow.Types;
using System;
using System.Collections.Generic;
using System.CommandLine;
using System.CommandLine.Invocation;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace Apache.Arrow.IntegrationTest
{
    public class Program
    {
        public static async Task<int> Main(string[] args)
        {
            var integrationTestCommand = new RootCommand
            {
                new Option<string>(
                    "--mode",
                    description: "Which command to run"),
                new Option<FileInfo>(
                    new[] { "--json-file", "-j" },
                    "The JSON file to interact with"),
                new Option<FileInfo>(
                    new[] { "--arrow-file", "-a" },
                    "The arrow file to interact with")
            };

            integrationTestCommand.Description = "Integration test app for Apache.Arrow .NET Library.";

            integrationTestCommand.Handler = CommandHandler.Create<string, FileInfo, FileInfo>(async (mode, j, a) =>
            {
                var integrationCommand = new IntegrationCommand(mode, j, a);
                await integrationCommand.Execute();
            });
            return await integrationTestCommand.InvokeAsync(args);
        }
    }
}
