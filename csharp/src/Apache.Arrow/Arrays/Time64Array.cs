// Licensed to the Apache Software Foundation (ASF) under one or moreDate32Array
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
using System.IO;
using System.Linq;
using Apache.Arrow.Types;

namespace Apache.Arrow
{
    /// <summary>
    /// The <see cref="Time64Array"/> class holds an array of <see cref="Int64" />, where each value is
    /// stored as the number of microseconds/nanoseconds (depending on the Time64Type) since midnight.
    /// </summary>
    public class Time64Array : PrimitiveArray<long>
    {
        /// <summary>
        /// The <see cref="Builder"/> class can be used to fluently build <see cref="Time64Array"/> objects.
        /// </summary>
        public class Builder : PrimitiveArrayBuilder<long, Time64Array, Builder>
        {
            protected override Time64Array Build(
                ArrowBuffer valueBuffer, ArrowBuffer nullBitmapBuffer,
                int length, int nullCount, int offset) =>
                new Time64Array(DataType, valueBuffer, nullBitmapBuffer, length, nullCount, offset);

            protected Time64Type DataType { get; }

            public Builder()
                : this(Time64Type.Default) { }

            public Builder(TimeUnit unit)
                : this(new Time64Type(unit)) { }

            /// <summary>
            /// Construct a new instance of the <see cref="Builder"/> class.
            /// </summary>
            public Builder(Time64Type type)
                : base()
            {
                DataType = type;
            }

            public Builder AppendRange(IEnumerable<TimeSpan> values)
            {
                switch (DataType.Unit)
                {
                    case TimeUnit.Second:
                        AppendRange(values.Select(value => (long)value.TotalSeconds));
                        break;
                    case TimeUnit.Millisecond:
                        AppendRange(values.Select(value => (long)value.TotalMilliseconds));
                        break;
                    case TimeUnit.Microsecond:
                        AppendRange(values.Select(value => value.TotalMicroseconds()));
                        break;
                    case TimeUnit.Nanosecond:
                        AppendRange(values.Select(value => value.TotalNanoseconds()));
                        break;
                    default:
                        throw new InvalidDataException($"Unsupported time unit for TimestampType: {DataType.Unit}");
                }

                return this;
            }

            public Builder AppendRange(IEnumerable<TimeSpan?> values)
            {
                switch (DataType.Unit)
                {
                    case TimeUnit.Second:
                        foreach (TimeSpan? value in values)
                        {
                            if (value.HasValue)
                                Append((long)value.Value.TotalSeconds);
                            else
                                AppendNull();
                        }
                        break;
                    case TimeUnit.Millisecond:
                        foreach (TimeSpan? value in values)
                        {
                            if (value.HasValue)
                                Append((long)value.Value.TotalMilliseconds);
                            else
                                AppendNull();
                        }
                        break;
                    case TimeUnit.Microsecond:
                        foreach (TimeSpan? value in values)
                        {
                            if (value.HasValue)
                                Append(value.Value.TotalMicroseconds());
                            else
                                AppendNull();
                        }
                        break;
                    case TimeUnit.Nanosecond:
                        foreach (TimeSpan? value in values)
                        {
                            if (value.HasValue)
                                Append(value.Value.TotalNanoseconds());
                            else
                                AppendNull();
                        }
                        break;
                    default:
                        throw new InvalidDataException($"Unsupported time unit for TimestampType: {DataType.Unit}");
                }

                return this;
            }
        }

        public Time64Array(
            Time64Type type,
            ArrowBuffer valueBuffer, ArrowBuffer nullBitmapBuffer,
            int length, int nullCount, int offset)
            : this(new ArrayData(type, length, nullCount, offset,
                new[] { nullBitmapBuffer, valueBuffer }))
        { }

        public Time64Array(ArrayData data)
            : base(data)
        {
            data.EnsureDataType(ArrowTypeId.Time64);
        }

        private Time64Type TimeType => (Time64Type)Data.DataType;

        public override void Accept(IArrowArrayVisitor visitor) => Accept(this, visitor);

        /// <summary>
        /// Get the time at the specified index as microseconds
        /// </summary>
        /// <param name="index">Index at which to get the time.</param>
        /// <returns>Returns a <see cref="Int64" />, or <c>null</c> if there is no object at that index.
        /// </returns>
        public long? GetMicroSeconds(int index)
        {
            long? value = GetValue(index);
            if (value == null)
            {
                return null;
            }

            var unit = ((Time64Type)Data.DataType).Unit;
            return unit switch
            {
                TimeUnit.Microsecond => value,
                TimeUnit.Nanosecond => value / 1_000,
                _ => throw new InvalidDataException($"Unsupported time unit for Time64Type: {unit}")
            };
        }

        /// <summary>
        /// Get the time at the specified index as nanoseconds
        /// </summary>
        /// <param name="index">Index at which to get the time.</param>
        /// <returns>Returns a <see cref="Int64" />, or <c>null</c> if there is no object at that index.
        /// </returns>
        public long? GetNanoSeconds(int index)
        {
            long? value = GetValue(index);
            if (value == null)
            {
                return null;
            }

            var unit = ((Time64Type)Data.DataType).Unit;
            return unit switch
            {
                TimeUnit.Microsecond => value * 1_000,
                TimeUnit.Nanosecond => value,
                _ => throw new InvalidDataException($"Unsupported time unit for Time64Type: {unit}")
            };
        }

        public new TimeSpan?[] ToArray()
        {
            TimeSpan?[] alloc = new TimeSpan?[Length];

            // Initialize the values
            switch (TimeType.Unit)
            {
                case TimeUnit.Microsecond:
                    for (int i = 0; i < Length; i++)
                    {
                        alloc[i] = IsValid(i) ? TimeSpanExtensions.FromMicroseconds(Values[i]) : null;
                    }
                    break;
                case TimeUnit.Nanosecond:
                    for (int i = 0; i < Length; i++)
                    {
                        alloc[i] = IsValid(i) ? TimeSpanExtensions.FromNanoseconds(Values[i]) : null;
                    }
                    break;
                default:
                    throw new InvalidDataException($"Unsupported time unit for Time64Type: {TimeType.Unit}");
            }

            return alloc;
        }

        public new TimeSpan[] ToArray(bool nullable = false)
        {
            TimeSpan[] alloc = new TimeSpan[Length];

            // Initialize the values
            switch (TimeType.Unit)
            {
                case TimeUnit.Microsecond:
                    for (int i = 0; i < Length; i++)
                    {
                        alloc[i] = TimeSpanExtensions.FromMicroseconds(Values[i]);
                    }
                    break;
                case TimeUnit.Nanosecond:
                    for (int i = 0; i < Length; i++)
                    {
                        alloc[i] = TimeSpanExtensions.FromNanoseconds(Values[i]);
                    }
                    break;
                default:
                    throw new InvalidDataException($"Unsupported time unit for Time64Type: {TimeType.Unit}");
            }

            return alloc;
        }
    }
}
