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
using System.Linq;
using System.Threading.Tasks;
using Apache.Arrow.Flight.Client;
using Apache.Arrow.Flight.Middleware.Interceptors;
using Apache.Arrow.Flight.Sql.Tests.Stubs;
using Apache.Arrow.Flight.Tests.MiddlewareTests.Stubs;
using Grpc.Core;
using Grpc.Core.Interceptors;
using Xunit;

namespace Apache.Arrow.Flight.Tests.MiddlewareTests;

public class ClientInterceptorAdapterTests
{
    private readonly TestWebFactory _testWebFactory;
    private readonly FlightClient _client;
    private readonly CapturingMiddlewareFactory _middlewareFactory;

    public ClientInterceptorAdapterTests()
    {
        _testWebFactory = new TestWebFactory(new InMemoryFlightStore());

        _middlewareFactory = new CapturingMiddlewareFactory();
        var interceptor = new ClientInterceptorAdapter([_middlewareFactory]);

        _client = new FlightClient(_testWebFactory.GetChannel().Intercept(interceptor));
    }

    [Fact]
    public async Task MiddlewareFlowIsCalledCorrectly()
    {
        // Arrange
        var descriptor = FlightDescriptor.CreatePathDescriptor("test");

        // Act
        var info = await _client.GetInfo(descriptor);
        var middleware = _middlewareFactory.Instance;
        
        // Assert
        Assert.NotNull(info);
        Assert.True(middleware.BeforeHeadersCalled, "BeforeHeaders not called");
        Assert.True(middleware.HeadersReceivedCalled, "HeadersReceived not called");
        Assert.True(middleware.CallCompletedCalled, "CallCompleted not called");
    }

    [Fact]
    public async Task CookieAndHeaderValuesArePersistedThroughMiddleware()
    {
        // Arrange
        var descriptor = FlightDescriptor.CreatePathDescriptor("test");

        // Act
        try
        {
            await _client.GetInfo(descriptor);
        }
        catch (RpcException)
        {
            // Expected: Flight not found, but middleware should have run
        }
        
        // Assert Middleware captured the headers and cookies correctly
        var middleware = _middlewareFactory.Instance;
        
        Assert.True(middleware.BeforeHeadersCalled, "OnBeforeSendingHeaders not called");
        Assert.True(middleware.HeadersReceivedCalled, "OnHeadersReceived not called");
        Assert.True(middleware.CallCompletedCalled, "OnCallCompleted not called");
        
        // Validate Cookies captured correctly
        Assert.True(middleware.CapturedHeaders.ContainsKey("cookie"));
        var cookies = ParseCookies(middleware.CapturedHeaders["cookie"]);
        
        Assert.Equal("abc123", cookies["sessionId"]);
        Assert.Equal("xyz789", cookies["token"]);
    }
    
    private static IDictionary<string, string> ParseCookies(string cookieHeader)
    {
        return cookieHeader.Split(';')
            .Select(pair => pair.Split('='))
            .ToDictionary(parts => parts[0].Trim(), parts => parts[1].Trim());
    }
}
    
 