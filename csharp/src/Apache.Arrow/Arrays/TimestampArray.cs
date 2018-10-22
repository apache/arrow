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
using System.IO;

namespace Apache.Arrow
{
    public class TimestampArray: PrimitiveArray<long>
    {
        public TimestampArray(
            ArrowBuffer valueBuffer, ArrowBuffer nullBitmapBuffer,
            int length, int nullCount, int offset)
            : this(new ArrayData(TimestampType.Default, length, nullCount, offset,
                new[] { nullBitmapBuffer, valueBuffer }))
        { }

        public TimestampArray(ArrayData data)
            : base(data)
        {
            data.EnsureDataType(ArrowTypeId.Timestamp);
        }

        public override void Accept(IArrowArrayVisitor visitor) => Accept(this, visitor);

        public DateTimeOffset? GetTimestamp(int index)
        {
            var span = GetSpan();

            if (IsNull(index))
            {
                return null;
            }

            var value = span[index];
            var type = Data.DataType as TimestampType;

            switch (type.Unit)
            {
                case TimeUnit.Nanosecond:
                    return DateTimeOffset.FromUnixTimeMilliseconds(value / 1000000);
                case TimeUnit.Microsecond:
                    return DateTimeOffset.FromUnixTimeMilliseconds(value / 1000);
                case TimeUnit.Millisecond:
                    return DateTimeOffset.FromUnixTimeMilliseconds(value);
                case TimeUnit.Second:
                    return DateTimeOffset.FromUnixTimeSeconds(value);
                default:
                    throw new InvalidDataException(
                        string.Format("Unsupported timestamp unit <{0}>", type.Unit));
            }
        }
    }
}
