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
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using Apache.Arrow.Flight.Middleware.Interfaces;
using Grpc.Core;

namespace Apache.Arrow.Flight.Middleware;

public class CallHeaders : ICallHeaders, IEnumerable<KeyValuePair<string, string>>
{
    private readonly Metadata _metadata;

    public CallHeaders(Metadata metadata)
    {
        _metadata = metadata;
    }

    public void Add(string key, string value) => _metadata.Add(key, value);

    public bool ContainsKey(string key) => _metadata.Any(h => KeyEquals(h.Key, key));

    public IEnumerator<KeyValuePair<string, string>> GetEnumerator()
    {
        foreach (var entry in _metadata)
            yield return new KeyValuePair<string, string>(entry.Key, entry.Value);
    }

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

    public string this[string key]
    {
        get
        {
            var entry = _metadata.FirstOrDefault(h =>  KeyEquals(h.Key, key));
            return entry?.Value;
        }
        set
        {
            var entry = _metadata.FirstOrDefault(h =>  KeyEquals(h.Key, key));
            if (entry != null) _metadata.Remove(entry);
            _metadata.Add(key, value);
        }
    }

    public string Get(string key) => this[key];
  
    public byte[] GetBytes(string key) =>
        _metadata.FirstOrDefault(h => KeyEquals(h.Key, key))?.ValueBytes;

    public IEnumerable<string> GetAll(string key) =>
        _metadata.Where(h => KeyEquals(h.Key, key)).Select(h => h.Value);

    public IEnumerable<byte[]> GetAllBytes(string key) =>
        _metadata.Where(h => KeyEquals(h.Key, key)).Select(h => h.ValueBytes);

    public void Insert(string key, string value) => Add(key, value);

    public void Insert(string key, byte[] value) => _metadata.Add(key, value);

    public ISet<string> Keys => new HashSet<string>(_metadata.Select(h => h.Key));
    
    private static bool KeyEquals(string a, string b) =>
        string.Equals(a, b, StringComparison.OrdinalIgnoreCase);
}