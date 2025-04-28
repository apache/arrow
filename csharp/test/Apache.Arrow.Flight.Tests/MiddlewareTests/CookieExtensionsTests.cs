using System;
using Apache.Arrow.Flight.Middleware.Extensions;
using Xunit;
using static System.Linq.Enumerable;

namespace Apache.Arrow.Flight.Tests.MiddlewareTests;

public class CookieExtensionsTests
{
    [Fact]
    public void ParseHeaderShouldParseSimpleCookie()
    {
        // Arrange
        var header = "sessionId=abc123";

        // Act
        var cookies = header.ParseHeader().ToList();

        // Assert
        Assert.Single(cookies);
        Assert.Equal("sessionId", cookies[0].Name);
        Assert.Equal("abc123", cookies[0].Value);
        Assert.False(cookies[0].Expired);
    }
    
    [Fact]
    public void ParseHeaderShouldParseCookieWithExpires()
    {
        // Arrange
        var futureDate = DateTimeOffset.UtcNow.AddDays(7);
        var header = $"userId=789; Expires={futureDate:R}";

        // Act
        var cookies = header.ParseHeader().ToList();

        // Assert
        Assert.Single(cookies);
        Assert.Equal("userId", cookies[0].Name);
        Assert.Equal("789", cookies[0].Value);
        Assert.True(Math.Abs((cookies[0].Expires - futureDate.UtcDateTime).TotalSeconds) < 5);
    }

    [Fact]
    public void ParseHeaderShouldReturnEmptyWhenMalformed()
    {
        // Arrange
        var header = "this_is_wrong";

        // Act
        var cookies = header.ParseHeader().ToList();

        // Assert
        Assert.Empty(cookies);
    }
    
    [Fact]
    public void ParseHeaderShouldReturnEmptyWhenEmptyString()
    {
        // Arrange
        var header = string.Empty;

        // Act
        var cookies = header.ParseHeader().ToList();

        // Assert
        Assert.Empty(cookies);
    }
    
    [Fact]
    public void ParseHeaderShouldReturnEmptyWhenNullString()
    {
        // Arrange
        string header = null;

        // Act
        var cookies = header.ParseHeader().ToList();

        // Assert
        Assert.Empty(cookies);
    }

    [Fact]
    public void ParseHeaderShouldParseCookieIgnoringAttributes()
    {
        // Arrange
        var header = "token=xyz; Path=/; HttpOnly";

        // Act
        var cookies = header.ParseHeader().ToList();

        // Assert
        Assert.Single(cookies);
        Assert.Equal("token", cookies[0].Name);
        Assert.Equal("xyz", cookies[0].Value);
    }

    [Fact]
    public void ParseHeaderShouldIgnoreInvalidExpires()
    {
        // Arrange
        var header = "name=value; Expires=invalid-date";

        // Act
        var cookies = header.ParseHeader().ToList();

        // Assert
        Assert.Single(cookies);
        Assert.Equal("name", cookies[0].Name);
        Assert.Equal("value", cookies[0].Value);
        Assert.False(cookies[0].Expired);
    }
}