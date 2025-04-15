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
using Apache.Arrow.Flight.Sql.Middleware.Extensions;
using Apache.Arrow.Flight.Sql.Middleware.Interfaces;
using Apache.Arrow.Flight.Sql.Middleware.Models;
using Microsoft.Extensions.Logging;

namespace Apache.Arrow.Flight.Sql.Middleware.Middleware;

public class ClientCookieMiddleware : IFlightClientMiddleware
{
    private readonly ClientCookieMiddlewareFactory _factory;
    private readonly ILogger<ClientCookieMiddleware> _logger;
    private const string SET_COOKIE_HEADER = "Set-cookie";
    private const string COOKIE_HEADER = "Cookie";

    private readonly ConcurrentDictionary<string, Cookie> _cookies = new();

    public ClientCookieMiddleware(ClientCookieMiddlewareFactory factory,
        ILogger<ClientCookieMiddleware> logger)
    {
        _factory = factory;
        _logger = logger;
    }

    public void OnBeforeSendingHeaders(ICallHeaders outgoingHeaders)
    {
        var cookieValue = GetValidCookiesAsString();
        if (!string.IsNullOrEmpty(cookieValue))
        {
            outgoingHeaders.Insert(COOKIE_HEADER, cookieValue);
        }

        _logger.LogInformation("Sending Headers: " + string.Join(", ", outgoingHeaders.Keys));
    }

    public void OnHeadersReceived(ICallHeaders incomingHeaders)
    {
        var setCookieHeaders = incomingHeaders.GetAll(SET_COOKIE_HEADER);
        _factory.UpdateCookies(setCookieHeaders);
        _logger.LogInformation("Received Headers: " + string.Join(", ", incomingHeaders.Keys));
    }

    public void OnCallCompleted(CallStatus status)
    {
        _logger.LogInformation($"Call completed with: {status.Code} ({status.Description})");
    }

    private string GetValidCookiesAsString()
    {
        var cookieList = new List<string>();
        foreach (var entry in _factory.Cookies)
        {
            if (entry.Value.Expired)
            {
                _factory.Cookies.TryRemove(entry.Key, out _);
            }
            else
            {
                cookieList.Add(entry.Value.ToString());
            }
        }

        return string.Join("; ", cookieList);
    }

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
}