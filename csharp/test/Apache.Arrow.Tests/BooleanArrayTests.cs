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
using Xunit;

namespace Apache.Arrow.Tests
{
    public class BooleanArrayTests
    {
        public class Builder
        {
            public class Append
            {
                [Theory]
                [InlineData(1)]
                [InlineData(3)]
                public void IncrementsLength(int count)
                {
                    var builder = new BooleanArray.Builder();

                    for (var i = 0; i < count; i++)
                    {
                        builder.Append(true);
                    }

                    var array = builder.Build();

                    Assert.Equal(count, array.Length);
                }

                [Fact]
                public void AppendsExpectedBit()
                {
                    var array1 = new BooleanArray.Builder()
                        .Append(false)
                        .Build();

                    Assert.False(array1.GetBoolean(0));

                    var array2 = new BooleanArray.Builder()
                        .Append(true)
                        .Build();

                    Assert.True(array2.GetBoolean(0));
                }
            }

            public class Clear
            {
                [Fact]
                public void SetsAllBitsToDefault()
                {
                    var array = new BooleanArray.Builder()
                        .Resize(8)
                        .Set(0, true)
                        .Set(7, true)
                        .Clear()
                        .Build();

                    for (var i = 0; i < array.Length; i++)
                    {
                        Assert.False(array.GetBoolean(i));
                    }
                }
            }

            public class Toggle
            {
                [Theory]
                [InlineData(8, 1)]
                [InlineData(16, 13)]
                public void TogglesExpectedBitToFalse(int length, int index)
                {
                    var array = new BooleanArray.Builder()
                        .Resize(length)
                        .Set(index, true)
                        .Toggle(index)
                        .Build();

                    Assert.False(array.GetBoolean(index));
                }

                [Theory]
                [InlineData(8, 1)]
                [InlineData(16, 13)]
                public void TogglesExpectedBitToTreu(int length, int index)
                {
                    var array = new BooleanArray.Builder()
                        .Resize(length)
                        .Set(index, false)
                        .Toggle(index)
                        .Build();

                    Assert.True(array.GetBoolean(index));
                }

                [Fact]
                public void ThrowsWhenIndexOutOfRange()
                {
                    Assert.Throws<ArgumentOutOfRangeException>(() =>
                    {
                        var builder = new BooleanArray.Builder();
                        builder.Toggle(8);
                    });
                }
            }

            public class Swap
            {
                [Fact]
                public void SwapsExpectedBits()
                {
                    var array = new BooleanArray.Builder()
                        .Resize(8)
                        .Set(0, true)
                        .Swap(0, 7)
                        .Build();

                    Assert.False(array.GetBoolean(0));
                    Assert.True(array.GetBoolean(7));
                }

                [Fact]
                public void ThrowsWhenIndexOutOfRange()
                {
                    Assert.Throws<ArgumentOutOfRangeException>(() =>
                    {
                        var builder = new BooleanArray.Builder();
                        builder.Swap(0, 1);
                    });
                }
            }

            public class Set
            {
                [Theory]
                [InlineData(8, 0)]
                [InlineData(8, 4)]
                [InlineData(8, 7)]
                [InlineData(16, 8)]
                [InlineData(16, 15)]
                public void SetsExpectedBitToTrue(int length, int index)
                {
                    var array = new BooleanArray.Builder()
                        .Resize(length)
                        .Set(index, true)
                        .Build();

                    Assert.True(array.GetBoolean(index));
                }

                [Theory]
                [InlineData(8, 0)]
                [InlineData(8, 4)]
                [InlineData(8, 7)]
                [InlineData(16, 8)]
                [InlineData(16, 15)]
                public void SetsExpectedBitsToFalse(int length, int index)
                {
                    var array = new BooleanArray.Builder()
                        .Resize(length)
                        .Set(index, false)
                        .Build();

                    Assert.False(array.GetBoolean(index));
                }

                [Theory]
                [InlineData(4)]
                public void UnsetBitsAreUnchanged(int index)
                {
                    var array = new BooleanArray.Builder()
                        .Resize(8)
                        .Set(index, true)
                        .Build();

                    for (var i = 0; i < 8; i++)
                    {
                        if (i != index)
                        {
                            Assert.False(array.GetBoolean(i));
                        }
                    }
                }

                [Fact]
                public void ThrowsWhenIndexOutOfRange()
                {
                    Assert.Throws<ArgumentOutOfRangeException>(() =>
                    {
                        var builder = new BooleanArray.Builder();
                        builder.Set(builder.Length, false);
                    });
                }
            }
        }
    }
}
