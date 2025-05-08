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
using System.Linq;
using Apache.Arrow.Flight.Middleware.Interfaces;

namespace Apache.Arrow.Flight.Tests.MiddlewareTests.Stubs;

public class InMemoryCallHeaders : ICallHeaders
{
    private readonly Dictionary<string, List<string>> _stringHeaders = new();
    private readonly Dictionary<string, List<byte[]>> _byteHeaders = new();

    public string this[string key] => Get(key);

    public string Get(string key) =>
        _stringHeaders.TryGetValue(key, out var values) ? values.LastOrDefault() : null;

    public byte[] GetBytes(string key) =>
        _byteHeaders.TryGetValue(key, out var values)
            ? values.LastOrDefault()
            : null;

    public IEnumerable<string> GetAll(string key) =>
        _stringHeaders.TryGetValue(key, out var values)
            ? values
            : Enumerable.Empty<string>();

    public IEnumerable<byte[]> GetAllBytes(string key) =>
        _byteHeaders.TryGetValue(key, out var values)
            ? values
            : Enumerable.Empty<byte[]>();

    public void Insert(string key, string value)
    {
        if (!_stringHeaders.TryGetValue(key, out var list))
            _stringHeaders[key] = list = new List<string>();
        list.Add(value);
    }

    public void Insert(string key, byte[] value)
    {
        if (!_byteHeaders.TryGetValue(key, out var list))
            _byteHeaders[key] = list = new List<byte[]>();
        list.Add(value);
    }

    public ISet<string> Keys =>
        (HashSet<string>) [.._stringHeaders.Keys.Concat(_byteHeaders.Keys)];

    public bool ContainsKey(string key) =>
        _stringHeaders.ContainsKey(key) || _byteHeaders.ContainsKey(key);
}