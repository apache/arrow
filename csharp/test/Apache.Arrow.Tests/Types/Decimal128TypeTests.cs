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
using Apache.Arrow.Types;
using Xunit;

namespace Apache.Arrow.Tests.Types
{
    public class Decimal128TypeTests
    {
        [Fact]
        public void Default_Should_Match()
        {
            Decimal128Type type = Decimal128Type.Default;

            // Assert
            Assert.Equal(13, type.ByteWidth);
            Assert.Equal(29, type.Precision);
            Assert.Equal(15, type.Scale);
        }

        [Fact]
        public void Constructor_Should_ComputeByteWidth()
        {
            // Assert
            Assert.Equal(9, new Decimal128Type(15, 5).ByteWidth);
            Assert.Equal(5, new Decimal128Type(10, 0).ByteWidth);
            Assert.Equal(16, new Decimal128Type(29, 8).ByteWidth);
            Assert.Equal(16, new Decimal128Type(29, 9).ByteWidth);
            Assert.Throws<OverflowException>(() => new Decimal128Type(29, 10));
            Assert.Throws<OverflowException>(() => new Decimal128Type(39, 0));
        }
    }
}
