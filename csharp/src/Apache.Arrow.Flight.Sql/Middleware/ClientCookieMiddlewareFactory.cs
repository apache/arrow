using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.Net;
using System.Threading;
using Apache.Arrow.Flight.Sql.Middleware.Extensions;
using Apache.Arrow.Flight.Sql.Middleware.Interfaces;
using Apache.Arrow.Flight.Sql.Middleware.Models;
using Microsoft.Extensions.Logging;

namespace Apache.Arrow.Flight.Sql.Middleware;

public class ClientCookieMiddlewareFactory : IFlightClientMiddlewareFactory
{
    public readonly ConcurrentDictionary<string, Cookie> Cookies = new(StringComparer.OrdinalIgnoreCase);
    private readonly ILoggerFactory _loggerFactory;
    private int _createdInstances = 0;
    public int CreatedInstances => _createdInstances;

    public ClientCookieMiddlewareFactory(ILoggerFactory loggerFactory)
    {
        _loggerFactory = loggerFactory;
    }

    public IFlightClientMiddleware OnCallStarted(CallInfo callInfo)
    {
        int callNumber = Interlocked.Increment(ref _createdInstances);
        var logger = _loggerFactory.CreateLogger<ClientCookieMiddleware>();
        logger.LogInformation($"Creating ClientCookieMiddleware #{callNumber} for {callInfo.MethodName}");
        return new ClientCookieMiddleware(this, logger);
    }

    public void UpdateCookies(IEnumerable<string> newCookieHeaderValues)
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