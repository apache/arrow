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
    private const string SetCookieHeader = "Set-Cookie";
    private const string CookieHeader = "Cookie";

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
            outgoingHeaders.Insert(CookieHeader, cookieValue);
        }
    }

    public void OnHeadersReceived(ICallHeaders incomingHeaders)
    {
        var setCookies = incomingHeaders.GetAll(SetCookieHeader);
        _factory.UpdateCookies(setCookies);
    }

    public void OnCallCompleted(Status status, Metadata trailers)
    {
        // ingest: status and/or metadata trailers
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
}
