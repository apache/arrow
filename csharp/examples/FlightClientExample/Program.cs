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

using System.Threading.Tasks;
using Grpc.Net.Client;
using Grpc.Core;
using Apache.Arrow.Flight.Client;
using Apache.Arrow.Flight;
using Apache.Arrow;
using System.Linq;
using System;
using System.Collections.Generic;

namespace FlightClientExample
{
    public class Program
    {
        public static async Task Main(string[] args)
        {
            string host = args.Length > 0 ? args[0] : "localhost";
            string port = args.Length > 1 ? args[1] : "5000";

            // Create client
            // (In production systems, you should use https not http)
            var address = $"http://{host}:{port}";
            Console.WriteLine($"Connecting to: {address}");
            var channel = GrpcChannel.ForAddress(address);
            var client = new FlightClient(channel);

            var recordBatches = new RecordBatch[] {
                CreateTestBatch(0, 2000), CreateTestBatch(50, 9000)
            };

            // Particular flights are identified by a descriptor. This might be a name,
            // a SQL query, or a path. Here, just using the name "test".
            var descriptor = FlightDescriptor.CreateCommandDescriptor("test");

            // Upload data with StartPut
            var batchStreamingCall = client.StartPut(descriptor);
            foreach (var batch in recordBatches)
            {
                await batchStreamingCall.RequestStream.WriteAsync(batch);
            }
            // Signal we are done sending record batches
            await batchStreamingCall.RequestStream.CompleteAsync();
            // Retrieve final response
            await batchStreamingCall.ResponseStream.MoveNext();
            Console.WriteLine(batchStreamingCall.ResponseStream.Current.ApplicationMetadata.ToStringUtf8());
            Console.WriteLine($"Wrote {recordBatches.Length} batches to server.");

            // Request information:
            var schema = await client.GetSchema(descriptor).ResponseAsync;
            Console.WriteLine($"Schema saved as: \n {schema}");

            var info = await client.GetInfo(descriptor).ResponseAsync;
            Console.WriteLine($"Info provided: \n {info}");

            Console.WriteLine($"Available flights:");
            var flights_call = client.ListFlights();

            while (await flights_call.ResponseStream.MoveNext())
            {   
                Console.WriteLine("  " + flights_call.ResponseStream.Current.ToString());
            }

            // Download data
            await foreach (var batch in StreamRecordBatches(info))
            {
                Console.WriteLine($"Read batch from flight server: \n {batch}")  ;
            }

            // See available commands on this server
            var action_stream = client.ListActions();
            Console.WriteLine("Actions:");
            while (await action_stream.ResponseStream.MoveNext())
            {
                var action = action_stream.ResponseStream.Current;
                Console.WriteLine($"  {action.Type}: {action.Description}");
            }

            // Send clear command to drop all data from the server.
            var clear_result = client.DoAction(new FlightAction("clear"));
            await clear_result.ResponseStream.MoveNext(default);
        }

        public static async IAsyncEnumerable<RecordBatch> StreamRecordBatches(
            FlightInfo info
        )
        {
            // There might be multiple endpoints hosting part of the data. In simple services,
            // the only endpoint might be the same server we initially queried.
            foreach (var endpoint in info.Endpoints)
            {
                // We may have multiple locations to choose from. Here we choose the first.
                var download_channel = GrpcChannel.ForAddress(endpoint.Locations.First().Uri);
                var download_client = new FlightClient(download_channel);

                var stream = download_client.GetStream(endpoint.Ticket);

                while (await stream.ResponseStream.MoveNext())
                { 
                    yield return stream.ResponseStream.Current;
                }
            }
        }

        public static RecordBatch CreateTestBatch(int start, int length)
        {
            return new RecordBatch.Builder()
                .Append("Column A", false, col => col.Int32(array => array.AppendRange(Enumerable.Range(start, start + length))))
                .Append("Column B", false, col => col.Float(array => array.AppendRange(Enumerable.Range(start, start + length).Select(x => Convert.ToSingle(x * 2)))))
                .Append("Column C", false, col => col.String(array => array.AppendRange(Enumerable.Range(start, start + length).Select(x => $"Item {x+1}"))))
                .Append("Column D", false, col => col.Boolean(array => array.AppendRange(Enumerable.Range(start, start + length).Select(x => x % 2 == 0))))
                .Build();
        }
    }
}