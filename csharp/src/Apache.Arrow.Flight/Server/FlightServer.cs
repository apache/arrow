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
using Grpc.Core;

namespace Apache.Arrow.Flight.Server
{
    public abstract class FlightServer
    {
        public virtual Task DoPut(FlightServerRecordBatchStreamReader requestStream, IAsyncStreamWriter<FlightPutResult> responseStream, ServerCallContext context)
        {
            throw new NotImplementedException();
        }

        public virtual Task DoGet(FlightTicket ticket, FlightServerRecordBatchStreamWriter responseStream, ServerCallContext context)
        {
            throw new NotImplementedException();
        }

        public virtual Task ListFlights(FlightCriteria request, IAsyncStreamWriter<FlightInfo> responseStream, ServerCallContext context)
        {
            throw new NotImplementedException();
        }

        public virtual Task ListActions(IAsyncStreamWriter<FlightActionType> responseStream, ServerCallContext context)
        {
            throw new NotImplementedException();
        }

        public virtual Task DoAction(FlightAction request, IAsyncStreamWriter<FlightResult> responseStream, ServerCallContext context)
        {
            throw new NotImplementedException();
        }

        public virtual Task<Schema> GetSchema(FlightDescriptor request, ServerCallContext context)
        {
            throw new NotImplementedException();
        }

        public virtual Task<FlightInfo> GetFlightInfo(FlightDescriptor request, ServerCallContext context)
        {
            throw new NotImplementedException();
        }
    }
}
