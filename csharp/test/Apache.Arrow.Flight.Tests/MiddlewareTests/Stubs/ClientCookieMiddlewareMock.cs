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
using System.Net;
using Apache.Arrow.Flight.Middleware;
using Microsoft.Extensions.Logging;

namespace Apache.Arrow.Flight.Tests.MiddlewareTests.Stubs;

internal class ClientCookieMiddlewareMock
{
    public Cookie CreateCookie(string name, string value, DateTimeOffset? expires = null, bool? expiredOverride = null)
    {
        return new Cookie
        {
            Name = name,
            Value = value,
            Expires = expires!.Value.UtcDateTime,
            Expired = expiredOverride ?? (expires.HasValue && expires.Value < DateTimeOffset.UtcNow)
        };
    }

    public ClientCookieMiddlewareFactory CreateFactory()
    {
        return new ClientCookieMiddlewareFactory(new TestLoggerFactory());
    }

    public class TestLogger<T> : ILogger<T>
    {
        public IDisposable BeginScope<TState>(TState state) => null;
        public bool IsEnabled(LogLevel logLevel) => false;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception exception,
            Func<TState, Exception, string> formatter)
        {
        }
    }

    internal class TestLoggerFactory : ILoggerFactory
    {
        public void AddProvider(ILoggerProvider provider)
        {
        }

        public ILogger CreateLogger(string categoryName) => new TestLogger<object>();

        public void Dispose()
        {
        }
    }
}