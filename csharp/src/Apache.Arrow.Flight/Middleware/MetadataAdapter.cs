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
using Apache.Arrow.Flight.Middleware.Interfaces;
using Grpc.Core;

namespace Apache.Arrow.Flight.Middleware;

public class MetadataAdapter : ICallHeaders
{
    private readonly Metadata _metadata;

    public MetadataAdapter(Metadata metadata)
    {
        _metadata = metadata ?? throw new ArgumentNullException(nameof(metadata));
    }

    public string this[string key] => Get(key);

    public string Get(string key)
    {
        return _metadata.FirstOrDefault(e =>
            !e.IsBinary && e.Key.Equals(key, StringComparison.OrdinalIgnoreCase))?.Value;
    }

    public byte[] GetBytes(string key)
    {
        return _metadata.FirstOrDefault(e =>
            e.IsBinary && e.Key.Equals(NormalizeBinaryKey(key), StringComparison.OrdinalIgnoreCase))?.ValueBytes;
    }

    public IEnumerable<string> GetAll(string key)
    {
        return _metadata
            .Where(e => !e.IsBinary && e.Key.Equals(key, StringComparison.OrdinalIgnoreCase))
            .Select(e => e.Value);
    }

    public IEnumerable<byte[]> GetAllBytes(string key)
    {
        var binaryKey = NormalizeBinaryKey(key);
        return _metadata
            .Where(e => e.IsBinary && e.Key.Equals(binaryKey, StringComparison.OrdinalIgnoreCase))
            .Select(e => e.ValueBytes);
    }

    public void Insert(string key, string value)
    {
        _metadata.Add(key, value);
    }

    public void Insert(string key, byte[] value)
    {
        _metadata.Add(NormalizeBinaryKey(key), value);
    }

    public ISet<string> Keys =>
        new HashSet<string>(_metadata.Select(e =>
                e.IsBinary ? DenormalizeBinaryKey(e.Key) : e.Key),
            StringComparer.OrdinalIgnoreCase);

    public bool ContainsKey(string key)
    {
        return _metadata.Any(e =>
            e.Key.Equals(key, StringComparison.OrdinalIgnoreCase) ||
            e.Key.Equals(NormalizeBinaryKey(key), StringComparison.OrdinalIgnoreCase));
    }

    private static string NormalizeBinaryKey(string key)
        => key.EndsWith(Metadata.BinaryHeaderSuffix, StringComparison.OrdinalIgnoreCase)
            ? key
            : key + Metadata.BinaryHeaderSuffix;

    private static string DenormalizeBinaryKey(string key)
        => key.EndsWith(Metadata.BinaryHeaderSuffix, StringComparison.OrdinalIgnoreCase)
            ? key.Substring(0, key.Length - Metadata.BinaryHeaderSuffix.Length)
            : key;
}

public static class MetadataAdapterExtensions
{
    public static bool TryGet(this ICallHeaders headers, string key, out string value)
    {
        value = headers.Get(key);
        return value is not null;
    }
}