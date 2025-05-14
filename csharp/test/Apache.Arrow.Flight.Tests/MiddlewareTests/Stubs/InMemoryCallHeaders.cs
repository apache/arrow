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
using Apache.Arrow.Flight.Middleware;
using Apache.Arrow.Flight.Middleware.Interfaces;
using Grpc.Core;

namespace Apache.Arrow.Flight.Tests.MiddlewareTests.Stubs;

public class InMemoryCallHeaders : ICallHeaders
{
    private readonly CallHeaders _stringHeaders;
    private readonly Dictionary<string, List<byte[]>> _byteHeaders;

    public InMemoryCallHeaders()
    {
        _stringHeaders = new CallHeaders(new Metadata());
        _byteHeaders = new Dictionary<string, List<byte[]>>(StringComparer.OrdinalIgnoreCase);
    }

    private static string NormalizeKey(string key) => key.ToLowerInvariant();

    public string this[string key] => Get(key);

    public string Get(string key)
    {
        key = NormalizeKey(key);
        return _stringHeaders.ContainsKey(key) ? _stringHeaders[key] : null;
    }

    public byte[] GetBytes(string key)
    {
        key = NormalizeKey(key);
        return _byteHeaders.TryGetValue(key, out var values) ? values.LastOrDefault() : null;
    }

    public IEnumerable<string> GetAll(string key)
    {
        key = NormalizeKey(key);
        return _stringHeaders.Where(h => string.Equals(h.Key, key, StringComparison.OrdinalIgnoreCase))
                             .Select(h => h.Value);
    }

    public IEnumerable<byte[]> GetAllBytes(string key)
    {
        key = NormalizeKey(key);
        return _byteHeaders.TryGetValue(key, out var values) ? values : Enumerable.Empty<byte[]>();
    }

    public void Insert(string key, string value)
    {
        key = NormalizeKey(key);
        _stringHeaders.Add(key, value);
    }

    public void Insert(string key, byte[] value)
    {
        key = NormalizeKey(key);
        if (!_byteHeaders.TryGetValue(key, out var list))
            _byteHeaders[key] = list = new List<byte[]>();
        list.Add(value);
    }

    public ISet<string> Keys =>
        new HashSet<string>(
            _stringHeaders.Select(h => h.Key.ToLowerInvariant())
                          .Concat(_byteHeaders.Keys),
            StringComparer.OrdinalIgnoreCase);

    public bool ContainsKey(string key)
    {
        key = NormalizeKey(key);
        return _stringHeaders.ContainsKey(key) || _byteHeaders.ContainsKey(key);
    }
}