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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.Net;
using Apache.Arrow.Flight.Middleware.Extensions;
using Apache.Arrow.Flight.Middleware.Interfaces;
using Microsoft.Extensions.Logging;
using CallInfo = Apache.Arrow.Flight.Middleware.Models.CallInfo;

namespace Apache.Arrow.Flight.Middleware;

public class ClientCookieMiddlewareFactory : IFlightClientMiddlewareFactory
{
    public readonly ConcurrentDictionary<string, Cookie> Cookies = new(StringComparer.OrdinalIgnoreCase);
    private readonly ILoggerFactory _loggerFactory;

    public ClientCookieMiddlewareFactory(ILoggerFactory loggerFactory)
    {
        _loggerFactory = loggerFactory;
    }

    public IFlightClientMiddleware OnCallStarted(CallInfo callInfo)
    {
        var logger = _loggerFactory.CreateLogger<ClientCookieMiddleware>();
        return new ClientCookieMiddleware(this, logger);
    }
    
    internal void UpdateCookies(IEnumerable<string> newCookieHeaderValues)
    {
        foreach (var headerValue in newCookieHeaderValues)
        {
            try
            {
                var parsedCookies = headerValue.ParseHeader();
                foreach (var parsedCookie in parsedCookies)
                {
                    var nameLc = parsedCookie.Name.ToLower(CultureInfo.InvariantCulture);
                    if (parsedCookie.Expired)
                    {
                        Cookies.TryRemove(nameLc, out _);
                    }
                    else
                    {
                        Cookies[nameLc] = parsedCookie;
                    }
                }
            }
            catch (FormatException ex)
            {
                var logger = _loggerFactory.CreateLogger<ClientCookieMiddleware>();
                logger.LogWarning(ex, "Skipping malformed Set-Cookie header: '{HeaderValue}'", headerValue);
            }
        }
    }
}