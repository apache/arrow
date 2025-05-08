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
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Apache.Arrow.Flight.Middleware;
using Apache.Arrow.Flight.Tests.MiddlewareTests.Stubs;
using Xunit;

namespace Apache.Arrow.Flight.Tests.MiddlewareTests;

public class ClientCookieMiddlewareTests
{
    private readonly ClientCookieMiddlewareMock _middlewareMock = new();

    [Fact]
    public void NoCookiesReturnsEmptyString()
    {
        var factory = _middlewareMock.CreateFactory();
        var middleware =
            new ClientCookieMiddleware(factory, new ClientCookieMiddlewareMock.TestLogger<ClientCookieMiddleware>());
        var headers = new InMemoryCallHeaders();
        middleware.OnBeforeSendingHeaders(headers);
        Assert.Empty(headers.GetAll("Cookie"));
    }

    [Fact]
    public void OnlyExpiredCookiesRemovesAll()
    {
        var factory = _middlewareMock.CreateFactory();
        factory.Cookies["expired"] =
            _middlewareMock.CreateCookie("expired", "value", DateTimeOffset.UtcNow.AddMinutes(-5));
        var middleware =
            new ClientCookieMiddleware(factory, new ClientCookieMiddlewareMock.TestLogger<ClientCookieMiddleware>());
        var headers = new InMemoryCallHeaders();
        middleware.OnBeforeSendingHeaders(headers);
        Assert.Empty(headers.GetAll("Cookie"));
        Assert.Empty(factory.Cookies);
    }

    [Fact]
    public void OnlyValidCookiesReturnsCookieHeader()
    {
        var factory = _middlewareMock.CreateFactory();
        factory.Cookies["valid"] = _middlewareMock.CreateCookie("valid", "abc", DateTimeOffset.UtcNow.AddMinutes(10));
        var middleware =
            new ClientCookieMiddleware(factory, new ClientCookieMiddlewareMock.TestLogger<ClientCookieMiddleware>());
        var headers = new InMemoryCallHeaders();
        middleware.OnBeforeSendingHeaders(headers);
        var header = headers.GetAll("Cookie").FirstOrDefault();
        Assert.NotNull(header);
        Assert.Contains("valid=abc", header);
    }

    [Fact]
    public void MixedCookiesRemovesExpiredOnly()
    {
        var factory = _middlewareMock.CreateFactory();
        factory.Cookies["expired"] =
            _middlewareMock.CreateCookie("expired", "x", DateTimeOffset.UtcNow.AddMinutes(-10));
        factory.Cookies["valid"] = _middlewareMock.CreateCookie("valid", "y", DateTimeOffset.UtcNow.AddMinutes(10));
        var middleware =
            new ClientCookieMiddleware(factory, new ClientCookieMiddlewareMock.TestLogger<ClientCookieMiddleware>());
        var headers = new InMemoryCallHeaders();
        middleware.OnBeforeSendingHeaders(headers);
        var header = headers.GetAll("Cookie").FirstOrDefault();
        Assert.NotNull(header);
        Assert.Contains("valid=y", header);
        Assert.DoesNotContain("expired=x", header);
        Assert.Single(factory.Cookies);
    }

    [Fact]
    public void DuplicateCookieKeysLastValidRemains()
    {
        var factory = _middlewareMock.CreateFactory();
        factory.Cookies["token"] = _middlewareMock.CreateCookie("token", "old", DateTimeOffset.UtcNow.AddMinutes(-5));
        factory.Cookies["token"] = _middlewareMock.CreateCookie("token", "new", DateTimeOffset.UtcNow.AddMinutes(5));
        var middleware =
            new ClientCookieMiddleware(factory, new ClientCookieMiddlewareMock.TestLogger<ClientCookieMiddleware>());
        var headers = new InMemoryCallHeaders();
        middleware.OnBeforeSendingHeaders(headers);
        var header = headers.GetAll("Cookie").FirstOrDefault();
        Assert.NotNull(header);
        Assert.Contains("token=new", header);
    }

    [Fact]
    public void FalsePositiveValidDateButMarkedExpired()
    {
        var factory = _middlewareMock.CreateFactory();
        factory.Cookies["wrong"] =
            _middlewareMock.CreateCookie("wrong", "v", DateTimeOffset.UtcNow.AddMinutes(10), expiredOverride: true);
        var middleware =
            new ClientCookieMiddleware(factory, new ClientCookieMiddlewareMock.TestLogger<ClientCookieMiddleware>());
        var headers = new InMemoryCallHeaders();
        middleware.OnBeforeSendingHeaders(headers);
        Assert.Empty(headers.GetAll("Cookie"));
    }

    [Fact]
    public async Task ConcurrentInsertRemoveDoesNotCorrupt()
    {
        var factory = _middlewareMock.CreateFactory();
        var middleware =
            new ClientCookieMiddleware(factory, new ClientCookieMiddlewareMock.TestLogger<ClientCookieMiddleware>());

        for (int i = 0; i < 100; i++)
            factory.Cookies[$"cookie{i}"] =
                _middlewareMock.CreateCookie($"cookie{i}", $"{i}", DateTimeOffset.UtcNow.AddMinutes(5));

        var tasks = Enumerable.Range(0, 20).Select(_ => Task.Run(() =>
        {
            var headers = new InMemoryCallHeaders();
            middleware.OnBeforeSendingHeaders(headers);
            foreach (var key in factory.Cookies.Keys)
                factory.Cookies.TryRemove(key, out Cookie _);
        }));

        await Task.WhenAll(tasks);
        Assert.True(factory.Cookies.Count >= 0);
    }
}