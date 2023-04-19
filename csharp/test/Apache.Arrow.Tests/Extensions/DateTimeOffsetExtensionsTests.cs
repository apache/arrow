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
    public class DateTimeOffsetExtensionsTests
    {
        [Fact]
        public void UnixEpoch_ShoudleBe_Right()
        {
            // Assert
            Assert.Equal(DateTimeOffsetExtensions.UnixEpoch, new DateTimeOffset(1970, 1, 1, 0, 0, 0, TimeSpan.Zero));
        }

        [Fact]
        public void FromUnixTimeSeconds_ReturnsExpectedDateTimeOffset()
        {
            // Assert
            Assert.Equal(DateTimeOffsetExtensions.UnixEpoch.AddSeconds(1), DateTimeOffsetExtensions.FromUnixTimeSeconds(1));
            Assert.Equal(DateTimeOffsetExtensions.UnixEpoch.AddSeconds(-1), DateTimeOffsetExtensions.FromUnixTimeSeconds(-1));
        }

        [Fact]
        public void FromUnixTimeMilliseconds_ReturnsExpectedDateTimeOffset()
        {
            // Assert
            Assert.Equal(DateTimeOffsetExtensions.UnixEpoch.AddSeconds(1), DateTimeOffsetExtensions.FromUnixTimeMilliseconds(1000));
            Assert.Equal(DateTimeOffsetExtensions.UnixEpoch.AddSeconds(-1), DateTimeOffsetExtensions.FromUnixTimeMilliseconds(-1000));
        }

        [Fact]
        public void FromUnixTimeMicroseconds_ReturnsExpectedDateTimeOffset()
        {
            // Assert
            Assert.Equal(DateTimeOffsetExtensions.UnixEpoch.AddSeconds(1), DateTimeOffsetExtensions.FromUnixTimeMicroseconds(1000000));
            Assert.Equal(DateTimeOffsetExtensions.UnixEpoch.AddSeconds(-1), DateTimeOffsetExtensions.FromUnixTimeMicroseconds(-1000000));
        }

        [Fact]
        public void FromUnixTimeNanoseconds_ReturnsExpectedDateTimeOffset()
        {
            // Assert
            Assert.Equal(DateTimeOffsetExtensions.UnixEpoch.AddSeconds(1), DateTimeOffsetExtensions.FromUnixTimeNanoseconds(1000000000));
            Assert.Equal(DateTimeOffsetExtensions.UnixEpoch.AddSeconds(-1), DateTimeOffsetExtensions.FromUnixTimeNanoseconds(-1000000000));
        }

        [Fact]
        public void ToUnixTimeMicroseconds_ReturnsExpectedValue()
        {
            // Assert
            Assert.Equal(123000000, DateTimeOffsetExtensions.UnixEpoch.AddSeconds(123).ToUnixTimeMicroseconds());
            Assert.Equal(-123000000, DateTimeOffsetExtensions.UnixEpoch.AddSeconds(-123).ToUnixTimeMicroseconds());
        }

        [Fact]
        public void ToUnixTimeNanoseconds_ReturnsExpectedValue()
        {
            // Assert
            Assert.Equal(123000000000, DateTimeOffsetExtensions.UnixEpoch.AddSeconds(123).ToUnixTimeNanoseconds());
            Assert.Equal(-123000000000, DateTimeOffsetExtensions.UnixEpoch.AddSeconds(-123).ToUnixTimeNanoseconds());
        }
    }
}
