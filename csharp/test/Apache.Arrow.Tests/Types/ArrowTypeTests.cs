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

using Apache.Arrow.Types;
using Xunit;

namespace Apache.Arrow.Tests
{
    public class ArrowTypeTests
    {
#if NETCOREAPP2_0_OR_GREATER
        // Hash32 of string only implemented for .net >= 2.0
        [Fact]
        public void ArrowType_Should_GetHashCode()
        {
            Assert.Equal(-950003445, new BooleanType().GetHashCode());
        }

        [Fact]
        public void NestedType_Should_GetHashCode()
        {
            Assert.Equal(2016456901, new ListType(new Int32Type()).GetHashCode());
        }
#endif
        [Fact]
        public void ArrowType_Should_ComparePrimitiveTypes()
        {
            Assert.Equal(new BooleanType(), new BooleanType());
            Assert.NotEqual(new BooleanType() as IArrowType, new BinaryType() as IArrowType);
            Assert.NotEqual(new BooleanType() as IArrowType, new ListType(new BooleanType()) as IArrowType);
        }

        [Fact]
        public void ArrowType_Should_CompareNestedTypes()
        {
            Assert.Equal((NestedType)new ListType(new BooleanType()), (NestedType)new ListType(new BooleanType()));
            Assert.NotEqual((NestedType)new ListType(new BooleanType()), (NestedType)new ListType(new BinaryType()));
        }

        [Fact]
        public void ArrowType_Should_CompareListTypes()
        {
            Assert.Equal(new ListType(new Int32Type()), new ListType(new Int32Type()));
            Assert.NotEqual(new ListType(new Int32Type()), new ListType(new FloatType()));
        }

        [Fact]
        public void ArrowType_Should_CompareStructTypes()
        {
            Assert.Equal(
                new StructType(new Field[] { new Field("test", new Int32Type(), false) }),
                new StructType(new Field[] { new Field("test", new Int32Type(), false) })
            );
            Assert.NotEqual(
                new StructType(new Field[] { new Field("test", new Int32Type(), false) }),
                new StructType(new Field[] { new Field("test", new Int8Type(), false) })
            );
            Assert.NotEqual(
                new StructType(new Field[] { new Field("_", new Int32Type(), false) }),
                new StructType(new Field[] { new Field("test", new Int32Type(), false) })
            );
            Assert.NotEqual(
                new StructType(new Field[] { new Field("test", new Int32Type(), true) }),
                new StructType(new Field[] { new Field("test", new Int32Type(), false) })
            );
        }

        [Fact]
        public void TimeType_Should_Compare()
        {
            Assert.Equal(new Time32Type(TimeUnit.Microsecond), new Time32Type(TimeUnit.Microsecond));
            Assert.NotEqual(new Time32Type(TimeUnit.Microsecond), new Time32Type(TimeUnit.Second));
        }

        [Fact]
        public void Decimal128Type_Should_Compare()
        {
            Assert.Equal(new Decimal128Type(15, 2), new Decimal128Type(15, 2));
            Assert.NotEqual(new Decimal128Type(15, 2), new Decimal128Type(15, 3));
            Assert.NotEqual(new Decimal128Type(15, 2), new Decimal128Type(1, 2));
        }

        [Fact]
        public void Decimal256Type_Should_Compare()
        {
            Assert.Equal(new Decimal256Type(15, 2), new Decimal256Type(15, 2));
            Assert.NotEqual(new Decimal256Type(15, 2), new Decimal256Type(15, 3));
        }

        [Fact]
        public void TimestampType_Should_Compare()
        {
            Assert.Equal(new TimestampType(TimeUnit.Second, "+00:00"), new TimestampType(TimeUnit.Second, "+00:00"));
            Assert.NotEqual(new TimestampType(TimeUnit.Second, "+00:00"), new TimestampType(TimeUnit.Second, "+00:01"));
            Assert.NotEqual(new TimestampType(TimeUnit.Second, "+00:00"), new TimestampType(TimeUnit.Millisecond, "+00:00"));
            Assert.NotEqual(new TimestampType(TimeUnit.Second, "+00:00"), new TimestampType(TimeUnit.Millisecond, "+00:12"));
        }

        [Fact]
        public void FloatingType_Should_Compare()
        {
            Assert.Equal(new FloatType(), new FloatType());
            Assert.NotEqual(new FloatType(), new DoubleType() as IArrowType);
        }

        [Fact]
        public void NumericType_Should_Compare()
        {
            Assert.NotEqual(new FloatType(), new Decimal128Type(15, 2) as IArrowType);
        }
    }
}
