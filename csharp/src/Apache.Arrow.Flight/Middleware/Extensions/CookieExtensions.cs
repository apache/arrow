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
using System.Linq;
using System.Net;

namespace Apache.Arrow.Flight.Middleware.Extensions;

public static class CookieExtensions
{
    public static IEnumerable<Cookie> ParseHeader(this string headers)
    {
        if (string.IsNullOrEmpty(headers))
            return [];
        
        var cookies = new List<Cookie>();
        var segments = headers.Split([';'], StringSplitOptions.RemoveEmptyEntries);

        if (segments.Length == 0) return cookies;

        var nameValue = segments[0].Split(['='], 2);
        if (nameValue.Length == 2)
        {
            var cookie = new Cookie(nameValue[0].Trim(), nameValue[1].Trim());

            foreach (var segment in segments.Skip(1))
            {
                var trimmedSegment = segment.Trim();
                if (trimmedSegment.StartsWith("Expires=", StringComparison.OrdinalIgnoreCase))
                {
                    var value = trimmedSegment.Substring("Expires=".Length).Trim();

                    if (!DateTimeOffset.TryParseExact(
                            value,
                            "R",
                            System.Globalization.CultureInfo.InvariantCulture,
                            System.Globalization.DateTimeStyles.None,
                            out var expires))
                    {
                        if (DateTimeOffset.TryParse(value, out expires))
                        {
                            cookie.Expires = expires.UtcDateTime;
                        }
                    }
                    else
                    {
                        cookie.Expires = expires.UtcDateTime;
                    }
                }
            }

            cookies.Add(cookie);
        }

        return cookies;
    }
}