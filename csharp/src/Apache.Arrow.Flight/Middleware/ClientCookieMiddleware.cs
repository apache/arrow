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
using Microsoft.Extensions.Logging;

namespace Apache.Arrow.Flight.Middleware;

public class ClientCookieMiddleware : IFlightClientMiddleware
{
    private readonly ClientCookieMiddlewareFactory _factory;
    private readonly ILogger<ClientCookieMiddleware> _logger;
    private const string SET_COOKIE_HEADER = "Set-Cookie";
    private const string COOKIE_HEADER = "Cookie";

    public ClientCookieMiddleware(ClientCookieMiddlewareFactory factory,
        ILogger<ClientCookieMiddleware> logger)
    {
        _factory = factory;
        _logger = logger;
    }

    public void OnBeforeSendingHeaders(ICallHeaders outgoingHeaders)
    {
        if (_factory.Cookies.IsEmpty)
            return;
        var cookieValue = GetValidCookiesAsString();
        if (!string.IsNullOrEmpty(cookieValue))
        {
            outgoingHeaders.Insert(COOKIE_HEADER, cookieValue);
        }
        _logger.LogInformation("Sending Headers: " + string.Join(", ", outgoingHeaders));
    }

    public void OnHeadersReceived(ICallHeaders incomingHeaders)
    {
        var setCookies = incomingHeaders.GetAll(SET_COOKIE_HEADER);
        _factory.UpdateCookies(setCookies);
        _logger.LogInformation("Received Headers: " + string.Join(", ", incomingHeaders));
    }

    public void OnCallCompleted(Status status, Metadata trailers)
    {
        _logger.LogInformation($"Call completed with: {status.StatusCode} ({status.Detail})");
    }

    private string GetValidCookiesAsString()
    {
        var cookieList = new List<string>();
        foreach (var entry in _factory.Cookies)
        {
            _logger.LogInformation($"Before remove cookie: {entry.Key} Expired: ({entry.Value.Expired})");
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
}