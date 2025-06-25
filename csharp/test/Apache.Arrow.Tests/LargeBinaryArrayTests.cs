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
using Apache.Arrow.Types;
using Xunit;

namespace Apache.Arrow.Tests;

public class LargeBinaryArrayTests
{
    [Fact]
    public void GetBytesReturnsCorrectValue()
    {
        var byteArrays = new byte[][]
        {
            new byte[] {0, 1, 2, 255},
            new byte[] {3, 4, 5},
            new byte[] {},
            null,
            new byte[] {254, 253, 252},
        };
        var array = BuildArray(byteArrays);

        Assert.Equal(array.Length, byteArrays.Length);
        for (var i = 0; i < byteArrays.Length; ++i)
        {
            var byteSpan = array.GetBytes(i, out var isNull);
            var byteArray = isNull ? null : byteSpan.ToArray();
            Assert.Equal(byteArrays[i], byteArray);
        }
    }

    [Fact]
    public void GetBytesChecksForOffsetOverflow()
    {
        var valueBuffer = new ArrowBuffer.Builder<byte>();
        var offsetBuffer = new ArrowBuffer.Builder<long>();
        var validityBuffer = new ArrowBuffer.BitmapBuilder();

        offsetBuffer.Append(0);
        offsetBuffer.Append((long)int.MaxValue + 1);
        validityBuffer.Append(true);

        var array = new LargeBinaryArray(
            LargeBinaryType.Default, length: 1,
            offsetBuffer.Build(), valueBuffer.Build(), validityBuffer.Build(),
            validityBuffer.UnsetBitCount);

        Assert.Throws<OverflowException>(() => array.GetBytes(0));
    }

    private static LargeBinaryArray BuildArray(IReadOnlyCollection<byte[]> byteArrays)
    {
        var valueBuffer = new ArrowBuffer.Builder<byte>();
        var offsetBuffer = new ArrowBuffer.Builder<long>();
        var validityBuffer = new ArrowBuffer.BitmapBuilder();

        long offset = 0;
        offsetBuffer.Append(offset);
        foreach (var bytes in byteArrays)
        {
            if (bytes == null)
            {
                validityBuffer.Append(false);
                offsetBuffer.Append(offset);
            }
            else
            {
                valueBuffer.Append(bytes);
                offset += bytes.Length;
                offsetBuffer.Append(offset);
                validityBuffer.Append(true);
            }
        }

        return new LargeBinaryArray(
            LargeBinaryType.Default, byteArrays.Count,
            offsetBuffer.Build(), valueBuffer.Build(), validityBuffer.Build(),
            validityBuffer.UnsetBitCount);
    }
}
