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
using System.Text;
using System.Threading.Tasks;
using Apache.Arrow.Flight.Protocol;
using Apache.Arrow.Flight.Internal;
using Grpc.Core;

namespace Apache.Arrow.Flight.Client
{
    public class FlightClientRecordBatchStreamWriter : FlightRecordBatchStreamWriter, IClientStreamWriter<RecordBatch>
    {
        private readonly IClientStreamWriter<FlightData> _clientStreamWriter;
        private bool _completed = false;
        internal FlightClientRecordBatchStreamWriter(IClientStreamWriter<FlightData> clientStreamWriter, FlightDescriptor flightDescriptor) : base(clientStreamWriter, flightDescriptor)
        {
            _clientStreamWriter = clientStreamWriter;
        }

        protected override void Dispose(bool disposing)
        {
            if (!_completed)
            {
                throw new InvalidOperationException("Dispose called before completing the stream.");
            }

            base.Dispose(disposing);
        }

        public async Task CompleteAsync()
        {
            if (_completed)
            {
                return;
            }

            await _clientStreamWriter.CompleteAsync().ConfigureAwait(false);
            _completed = true;
        }
    }
}
