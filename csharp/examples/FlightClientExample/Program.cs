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
using Apache.Arrow.Flight.Client;
using Apache.Arrow.Flight;
using Apache.Arrow;
using System.Linq;
using System;
using System.Threading;

namespace FlightClientExample
{
    public class Program
    {
        public static async Task Main(string[] args)
        {
            string host = args.Length > 0 ? args[0] : "localhost";
            string port = args.Length > 1 ? args[1] : "433";

            // Create client
            var channel = GrpcChannel.ForAddress($"http://${host}:${port}");
            var client = new FlightClient(channel);

            // Upload data
            var recordBatches = new RecordBatch[] {
                CreateTestBatch(0, 20), CreateTestBatch(50, 90)
            };
            
            var descriptor = FlightDescriptor.CreateCommandDescriptor("test");
            var batchStreamingCall = client.StartPut(descriptor);
            foreach (var batch in recordBatches) {
                await batchStreamingCall.RequestStream.WriteAsync(batch);
            }
            // We need this?
            batchStreamingCall.Dispose();

            Console.WriteLine($"Wrote {recordBatches.Length} batches to server.");

            // Request information:
            var schema_call = client.GetSchema(descriptor);
            var schema = await schema_call.ResponseAsync;
            Console.WriteLine($"Schema saved as: \n {schema.ToString()}");

            var infoCall = client.GetInfo(descriptor);
            var info = await infoCall.ResponseAsync;
            Console.WriteLine($"Info provided: \n {info.ToString()}");

            Console.WriteLine($"Available flights:");
            var flights_call = client.ListFlights();
            CancellationToken token; // Why is token required??
            while (await flights_call.ResponseStream.MoveNext(token))
            {   
                Console.WriteLine("Flight: " + flights_call.ResponseStream.Current.ToString());
            }

            // Download data
            var ticket = info.Endpoints.First().Ticket;
            // Are we requesting from the correct server? we may need to create a client
            // for that endpoint...
            var stream = client.GetStream(ticket);
            
            while (await stream.ResponseStream.MoveNext(token))
            { 
                RecordBatch batch = stream.ResponseStream.Current;
                Console.WriteLine($"Read batch from flight server: \n {batch}")  ;
            }
            
        }

        public static RecordBatch CreateTestBatch(Int32 start, Int32 end)
        {
            return new RecordBatch.Builder()
                .Append("Column A", false, col => col.Int32(array => array.AppendRange(Enumerable.Range(0, 10))))
                .Append("Column B", false, col => col.Float(array => array.AppendRange(Enumerable.Range(0, 10).Select(x => Convert.ToSingle(x * 2)))))
                .Append("Column C", false, col => col.String(array => array.AppendRange(Enumerable.Range(0, 10).Select(x => $"Item {x+1}"))))
                .Append("Column D", false, col => col.Boolean(array => array.AppendRange(Enumerable.Range(0, 10).Select(x => x % 2 == 0))))
                .Build();
        }
    }
}