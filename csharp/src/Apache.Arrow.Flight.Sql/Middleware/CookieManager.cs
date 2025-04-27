/*using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Net;
using Apache.Arrow.Flight.Client;
using Apache.Arrow.Flight.Sql.Middleware.Extensions;
using Apache.Arrow.Flight.Sql.Middleware.Interceptors;
using Apache.Arrow.Flight.Sql.Middleware.Interfaces;
using Apache.Arrow.Flight.Sql.Middleware.Models;
using Grpc.Core;
using Grpc.Core.Interceptors;
using Grpc.Net.Client;
using Microsoft.Extensions.Logging;

namespace Apache.Arrow.Flight.Sql.Middleware;

public class CookieManager
{
    private readonly ILogger<ClientCookieMiddleware> _logger;
    public ConcurrentDictionary<string, Cookie> Cookies { get; } = new(StringComparer.OrdinalIgnoreCase);

    public CookieManager(ILoggerFactory loggerFactory)
    {
        _logger = loggerFactory.CreateLogger<ClientCookieMiddleware>();
    }

    public void UpdateCookies(IEnumerable<string> cookieHeaders)
    {
        foreach (var header in cookieHeaders)
        {
            try
            {
                var cookies = header.ParseHeader();
                foreach (var cookie in cookies)
                {
                    if (cookie.Expired)
                        Cookies.TryRemove(cookie.Name.ToLowerInvariant(), out _);
                    else
                        Cookies[cookie.Name.ToLowerInvariant()] = cookie;
                }
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Skipping malformed Set-Cookie header: '{Header}'", header);
            }
        }
    }

    public string GetCookieHeader()
    {
        var validCookies =
            Cookies.Values.Where(c => !c.Expired).Select(c => $"{c.Name}={c.Value}");
        return string.Join("; ", validCookies);
    }
}

public class ClientCookieMiddleware : IFlightClientMiddleware
{
    private readonly CookieManager _cookieManager;
    private readonly ILogger<ClientCookieMiddleware> _logger;

    public ClientCookieMiddleware(CookieManager cookieManager, ILogger<ClientCookieMiddleware> logger)
    {
        _cookieManager = cookieManager;
        _logger = logger;
    }

    public void OnBeforeSendingHeaders(ICallHeaders outgoingHeaders)
    {
        var cookieHeader = _cookieManager.GetCookieHeader();
        if (!string.IsNullOrEmpty(cookieHeader))
        {
            outgoingHeaders.Insert("cookie", cookieHeader);
        }

        _logger.LogInformation("Sending cookies: {CookieHeader}", cookieHeader);
    }

    public void OnHeadersReceived(ICallHeaders incomingHeaders)
    {
        var setCookie = incomingHeaders.GetAll("set-cookie");
        var xCookie = incomingHeaders.GetAll("x-cookie");

        _cookieManager.UpdateCookies(setCookie.Concat(xCookie));

        _logger.LogInformation("Received Headers: {Keys}", string.Join(", ", incomingHeaders.Keys));
    }

    public void OnCallCompleted(CallStatus status)
    {
        _logger.LogInformation("Call completed: {Status}", status);
    }
}

public class ClientCookieMiddlewareFactory : IFlightClientMiddlewareFactory
{
    private readonly CookieManager _cookieManager;
    private readonly ILoggerFactory _loggerFactory;

    public ClientCookieMiddlewareFactory(CookieManager cookieManager, ILoggerFactory loggerFactory)
    {
        _cookieManager = cookieManager;
        _loggerFactory = loggerFactory;
    }

    public IFlightClientMiddleware OnCallStarted(CallInfo callInfo)
    {
        return new ClientCookieMiddleware(_cookieManager, _loggerFactory.CreateLogger<ClientCookieMiddleware>());
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
                        _cookieManager.Cookies.TryRemove(nameLc, out _);
                    }
                    else
                    {
                        _cookieManager.Cookies[nameLc] = parsedCookie;
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

public static class FlightClientFactory
{
    public static FlightClient Create(string address, CookieManager cookieManager, ILoggerFactory loggerFactory)
    {
        var channel = GrpcChannel.ForAddress(address, new GrpcChannelOptions
        {
            Credentials = ChannelCredentials.Insecure,
            MaxReceiveMessageSize = 100 * 1024 * 1024
        });

        var invoker = channel.Intercept(
            new ClientInterceptorAdapter([
                new ClientCookieMiddlewareFactory(cookieManager, loggerFactory)
            ]));

        return new FlightClient(invoker);
    }
}*/