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
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Net;

namespace Apache.Arrow.Flight.Middleware.Extensions;

public static class CookieExtensions
{
    public static IEnumerable<Cookie> ParseHeader(this string setCookieHeader)
    {
        if (string.IsNullOrWhiteSpace(setCookieHeader))
            return System.Array.Empty<Cookie>();

        var cookies = new List<Cookie>();

        var segments = setCookieHeader.Split([';'], StringSplitOptions.RemoveEmptyEntries);
        if (segments.Length == 0)
            return cookies;

        var nameValue = segments[0].Split(['='], 2);
        if (nameValue.Length != 2 || string.IsNullOrWhiteSpace(nameValue[0]))
            return cookies;

        var name = nameValue[0].Trim();
        var value = nameValue[1].Trim();
        var cookie = new Cookie(name, value);

        foreach (var segment in segments.Skip(1))
        {
            var kv = segment.Split(['='], 2, StringSplitOptions.RemoveEmptyEntries);
            var key = kv[0].Trim().ToLowerInvariant();
            var val = kv.Length > 1 ? kv[1] : null;

            switch (key)
            {
                case "expires":
                    if (!string.IsNullOrWhiteSpace(val))
                    {
                        if (DateTimeOffset.TryParseExact(val, "R", CultureInfo.InvariantCulture, DateTimeStyles.None, out var expiresRfc))
                            cookie.Expires = expiresRfc.UtcDateTime;
                        else if (DateTimeOffset.TryParse(val, out var expiresFallback))
                            cookie.Expires = expiresFallback.UtcDateTime;
                    }
                    break;

                case "max-age":
                    if (int.TryParse(val, out var seconds))
                        cookie.Expires = DateTime.UtcNow.AddSeconds(seconds);
                    break;

                case "domain":
                    cookie.Domain = val;
                    break;

                case "path":
                    cookie.Path = val;
                    break;

                case "secure":
                    cookie.Secure = true;
                    break;

                case "httponly":
                    cookie.HttpOnly = true;
                    break;
            }
        }

        cookies.Add(cookie);
        return cookies;
    }
    
    public static bool IsExpired(this Cookie cookie, string rawHeader)
    {
        if (string.IsNullOrWhiteSpace(cookie?.Value))
            return true;

        // If raw header has Max-Age=0, consider it deleted
        if (rawHeader?.IndexOf("Max-Age=0", StringComparison.OrdinalIgnoreCase) >= 0)
            return true;
        
        if (cookie.Expires != DateTime.MinValue && cookie.Expires <= DateTime.UtcNow)
            return true;

        return false;
    }
}
