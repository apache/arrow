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
using System;

namespace Apache.Arrow
{
    public class Date32Array: PrimitiveArray<int>
    {
        private const int MillisecondsPerDay = 86400000;

        public class Builder : PrimitiveArrayBuilder<DateTimeOffset, int, Date32Array, Builder>
        {
            internal class DateBuilder : PrimitiveArrayBuilder<int, Date32Array, DateBuilder>
            {
                protected override Date32Array Build(
                    ArrowBuffer valueBuffer, ArrowBuffer nullBitmapBuffer,
                    int length, int nullCount, int offset) =>
                    new Date32Array(valueBuffer, nullBitmapBuffer, length, nullCount, offset);
            }

            public Builder() : base(new DateBuilder()) { }

            protected override int ConvertTo(DateTimeOffset value)
            {
                return (int) (value.ToUnixTimeMilliseconds() / MillisecondsPerDay);
            }
        }

        public Date32Array(
            ArrowBuffer valueBuffer, ArrowBuffer nullBitmapBuffer,
            int length, int nullCount, int offset)
            : this(new ArrayData(Date32Type.Default, length, nullCount, offset,
                new[] { nullBitmapBuffer, valueBuffer }))
        { }

        public Date32Array(ArrayData data) 
            : base(data)
        {
            data.EnsureDataType(ArrowTypeId.Date32);
        }

        public override void Accept(IArrowArrayVisitor visitor) => Accept(this, visitor);

        public DateTimeOffset? GetDate(int index)
        {
            var values = ValueBuffer.Span.CastTo<int>().Slice(0, Length);
            var value = values[index];

            return DateTimeOffset.FromUnixTimeMilliseconds(value * MillisecondsPerDay);
        }
    }
}
