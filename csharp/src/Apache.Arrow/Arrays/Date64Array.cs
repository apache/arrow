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
using System.Collections.Generic;
using Apache.Arrow.Memory;

namespace Apache.Arrow
{
    public class Date64Array: PrimitiveArray<long>
    {
        public Date64Array(
            ArrowBuffer valueBuffer, ArrowBuffer nullBitmapBuffer,
            int length, int nullCount, int offset)
            : this(new ArrayData(Date64Type.Default, length, nullCount, offset,
                new[] { nullBitmapBuffer, valueBuffer }))
        { }

        public class Builder : PrimitiveArrayBuilder<DateTimeOffset, long, Date64Array, Builder>
        {
            internal class DateBuilder: PrimitiveArrayBuilder<long, Date64Array, DateBuilder>
            {
                protected override Date64Array Build(
                    ArrowBuffer valueBuffer, ArrowBuffer nullBitmapBuffer,
                    int length, int nullCount, int offset) =>
                    new Date64Array(valueBuffer, nullBitmapBuffer, length, nullCount, offset);
            } 

            public Builder() : base(new DateBuilder()) { }

            protected override long ConvertTo(DateTimeOffset value)
            {
                return value.ToUnixTimeMilliseconds();
            }
        }

        public Date64Array(ArrayData data) 
            : base(data)
        {
            data.EnsureDataType(ArrowTypeId.Date64);
        }

        public override void Accept(IArrowArrayVisitor visitor) => Accept(this, visitor);

        public DateTimeOffset? GetDate(int index)
        {
            if (!IsValid(index))
            {
                return null;
            }

            var values = ValueBuffer.Span.CastTo<long>().Slice(0, Length);
            var value = values[index];

            return DateTimeOffset.FromUnixTimeMilliseconds(value);
        }
    }
}
