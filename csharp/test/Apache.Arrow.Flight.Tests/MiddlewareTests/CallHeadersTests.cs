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

using System.Linq;
using Apache.Arrow.Flight.Tests.MiddlewareTests.Stubs;
using Xunit;

namespace Apache.Arrow.Flight.Tests.MiddlewareTests;

public class CallHeadersTests
{
    private readonly InMemoryCallHeaders _headers = new();

    [Fact]
    public void InsertAndGetStringValue()
    {
        _headers.Insert("Auth", "Bearer 123");
        Assert.Equal("Bearer 123", _headers.Get("Auth"));
        Assert.Equal("Bearer 123", _headers["Auth"]);
    }

    [Fact]
    public void InsertAndGetByteArrayValue()
    {
        var bytes = new byte[] { 1, 2, 3, 4, 5 };
        _headers.Insert("Data", bytes);
        Assert.Equal(bytes, _headers.GetBytes("Data"));
    }

    [Fact]
    public void InsertMultipleValuesAndGetLast()
    {
        _headers.Insert("User", "Alice");
        _headers.Insert("User", "Bob");
        Assert.Equal("Alice", _headers.Get("User"));
    }

    [Fact]
    public void GetAllShouldReturnAllStringValues()
    {
        _headers.Insert("Header", "v1");
        _headers.Insert("Header", "v2");
        var all = _headers.GetAll("Header").ToList();
        Assert.Contains("v1", all);
        Assert.Contains("v2", all);
        Assert.Equal(2, all.Count);
    }

    [Fact]
    public void GetAllBytesShouldReturnAllByteArrayValues()
    {
        var a = new byte[] { 1 };
        var b = new byte[] { 2 };
        _headers.Insert("Binary", a);
        _headers.Insert("Binary", b);
        var all = _headers.GetAllBytes("Binary").ToList();
        Assert.Contains(a, all);
        Assert.Contains(b, all);
        Assert.Equal(2, all.Count);
    }

    [Fact]
    public void KeysShouldReturnAllKeys()
    {
        _headers.Insert("A", "x");
        _headers.Insert("B", "y");
        Assert.Contains("A", _headers.Keys);
        Assert.Contains("B", _headers.Keys);
    }

    [Fact]
    public void ContainsKeyShouldWork()
    {
        _headers.Insert("Check", "yes");
        Assert.True(_headers.ContainsKey("Check"));
        Assert.False(_headers.ContainsKey("Missing"));
    }

    [Fact]
    public void GetNonExistentKeyShouldReturnNull()
    {
        Assert.Null(_headers.Get("MissingKey"));
        Assert.Null(_headers.GetBytes("MissingKey"));
        Assert.Empty(_headers.GetAll("MissingKey"));
        Assert.Empty(_headers.GetAllBytes("MissingKey"));
    }

    [Fact]
    public void ContainsKeyShouldBeFalseForMissingKey()
    {
        Assert.False(_headers.ContainsKey("DefinitelyMissing"));
    }

    [Fact]
    public void KeysShouldBeEmptyWhenNoHeaders()
    {
        Assert.Empty(_headers.Keys);
    }

    [Fact]
    public void IndexerShouldReturnNullForMissingKey()
    {
        string value = _headers["nonexistent"];
        Assert.Null(value);
    }

    [Fact]
    public void InsertEmptyStringsShouldStillStore()
    {
        _headers.Insert("Empty", "");
        Assert.Equal("", _headers.Get("Empty"));
        Assert.Single(_headers.GetAll("Empty"));
    }

    [Fact]
    public void InsertEmptyByteArrayShouldStillStore()
    {
        var empty = System.Array.Empty<byte>();
        _headers.Insert("BinaryEmpty", empty);
        Assert.Equal(empty, _headers.GetBytes("BinaryEmpty"));
        Assert.Single(_headers.GetAllBytes("BinaryEmpty"));
    }
}