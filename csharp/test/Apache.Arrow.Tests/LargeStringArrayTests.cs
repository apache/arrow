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
using Xunit;

namespace Apache.Arrow.Tests;

public class LargeStringArrayTests
{
    [Fact]
    public void GetStringReturnsCorrectValue()
    {
        var strings = new string[]
        {
            "abc",
            "defg",
            "",
            null,
            "123",
        };
        var array = BuildArray(strings);

        Assert.Equal(array.Length, strings.Length);
        for (var i = 0; i < strings.Length; ++i)
        {
            Assert.Equal(strings[i], array.GetString(i));
        }
    }

    [Fact]
    public void GetStringChecksForOffsetOverflow()
    {
        var valueBuffer = new ArrowBuffer.Builder<byte>();
        var offsetBuffer = new ArrowBuffer.Builder<long>();
        var validityBuffer = new ArrowBuffer.BitmapBuilder();

        offsetBuffer.Append(0);
        offsetBuffer.Append((long)int.MaxValue + 1);
        validityBuffer.Append(true);

        var array = new LargeStringArray(
            length: 1, offsetBuffer.Build(), valueBuffer.Build(), validityBuffer.Build(),
            validityBuffer.UnsetBitCount);

        Assert.Throws<OverflowException>(() => array.GetString(0));
    }

    private static LargeStringArray BuildArray(IReadOnlyCollection<string> strings)
    {
        var valueBuffer = new ArrowBuffer.Builder<byte>();
        var offsetBuffer = new ArrowBuffer.Builder<long>();
        var validityBuffer = new ArrowBuffer.BitmapBuilder();

        long offset = 0;
        offsetBuffer.Append(offset);
        foreach (var value in strings)
        {
            if (value == null)
            {
                validityBuffer.Append(false);
                offsetBuffer.Append(offset);
            }
            else
            {
                var bytes = LargeStringArray.DefaultEncoding.GetBytes(value);
                valueBuffer.Append(bytes);
                offset += value.Length;
                offsetBuffer.Append(offset);
                validityBuffer.Append(true);
            }
        }

        return new LargeStringArray(
            strings.Count, offsetBuffer.Build(), valueBuffer.Build(), validityBuffer.Build(),
            validityBuffer.UnsetBitCount);
    }
}
