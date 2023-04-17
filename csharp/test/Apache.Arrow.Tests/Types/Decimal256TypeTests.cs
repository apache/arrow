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
    public class Decimal256TypeTests
    {
        [Fact]
        public void Default_Should_Match()
        {
            Decimal256Type type = Decimal256Type.Default;

            // Assert
            Assert.Equal(24, type.ByteWidth);
            Assert.Equal(38, type.Precision);
            Assert.Equal(18, type.Scale);
        }

        [Fact]
        public void Constructor_Should_ComputeByteWidth()
        {
            // Assert
            Assert.Equal(9, new Decimal256Type(15, 5).ByteWidth);
            Assert.Equal(17, new Decimal256Type(29, 10).ByteWidth);
            Assert.Equal(32, new Decimal256Type(46, 29).ByteWidth);
            Assert.Equal(32, new Decimal256Type(46, 30).ByteWidth);
            Assert.Throws<OverflowException>(() => new Decimal256Type(46, 31));
            Assert.Throws<OverflowException>(() => new Decimal256Type(77, 10));
        }
    }
}
