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

using System.Collections.Generic;
using Apache.Arrow.Flight.Middleware.Interfaces;
using Grpc.Core;

namespace Apache.Arrow.Flight.Tests.MiddlewareTests.Stubs;

public class CapturingMiddleware : IFlightClientMiddleware
{ 
    public Dictionary<string, string> CapturedHeaders { get; } = new();
    
    public bool BeforeHeadersCalled { get; private set; }
    public bool HeadersReceivedCalled { get; private set; }
    public bool CallCompletedCalled { get; private set; }
    public void OnBeforeSendingHeaders(ICallHeaders outgoingHeaders)
    {
        BeforeHeadersCalled = true;
        outgoingHeaders.Insert("x-test-header", "test-value");
        outgoingHeaders.Insert("cookie", "sessionId=abc123; token=xyz789");
        CaptureHeaders(outgoingHeaders);
    }
    public void OnHeadersReceived(ICallHeaders incomingHeaders)
    {
        HeadersReceivedCalled = true;
        CaptureHeaders(incomingHeaders);
    }

    public void OnCallCompleted(Status status, Metadata trailers)
    {
        CallCompletedCalled = true;
    }
    
    private void CaptureHeaders(ICallHeaders headers)
    {
        foreach (var key in headers.Keys)
        {
            var value = headers[key];
            if (value != null)
            {
                CapturedHeaders[key] = value;
            }
        }
    }
}