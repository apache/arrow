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
using System.Linq;
using System.Collections.Generic;
using System.IO;
using Apache.Arrow.Types;

namespace Apache.Arrow
{
    /// <summary>
    /// The <see cref="Time32Array"/> class holds an array of <see cref="Int32" />, where each value is
    /// stored as the number of seconds/ milliseconds (depending on the Time32Type) since midnight.
    /// </summary>
    public class Time32Array : PrimitiveArray<int>
    {
        /// <summary>
        /// The <see cref="Builder"/> class can be used to fluently build <see cref="Time32Array"/> objects.
        /// </summary>
        public class Builder : PrimitiveArrayBuilder<int, Time32Array, Builder>
        {
            protected override Time32Array Build(
                ArrowBuffer valueBuffer, ArrowBuffer nullBitmapBuffer,
                int length, int nullCount, int offset) =>
                new Time32Array(DataType, valueBuffer, nullBitmapBuffer, length, nullCount, offset);
            
            protected Time32Type DataType { get; }

            public Builder()
                : this(Time32Type.Default) { }

            public Builder(TimeUnit unit)
                : this(new Time32Type(unit)) { }

            /// <summary>
            /// Construct a new instance of the <see cref="Builder"/> class.
            /// </summary>
            public Builder(Time32Type type)
                : base()
            {
                DataType = type;
            }

            public Builder AppendRange(IEnumerable<TimeSpan> values)
            {
                switch (DataType.Unit)
                {
                    case TimeUnit.Second:
                        AppendRange(values.Select(value => (int)value.TotalSeconds));
                        break;
                    case TimeUnit.Millisecond:
                        AppendRange(values.Select(value => (int)value.TotalMilliseconds));
                        break;
                    case TimeUnit.Microsecond:
                        AppendRange(values.Select(value => (int)value.TotalMicroseconds()));
                        break;
                    case TimeUnit.Nanosecond:
                        AppendRange(values.Select(value => (int)value.TotalNanoseconds()));
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
                                Append((int)value.Value.TotalSeconds);
                            else
                                AppendNull();
                        }
                        break;
                    case TimeUnit.Millisecond:
                        foreach (TimeSpan? value in values)
                        {
                            if (value.HasValue)
                                Append((int)value.Value.TotalMilliseconds);
                            else
                                AppendNull();
                        }
                        break;
                    case TimeUnit.Microsecond:
                        foreach (TimeSpan? value in values)
                        {
                            if (value.HasValue)
                                Append((int)value.Value.TotalMicroseconds());
                            else
                                AppendNull();
                        }
                        break;
                    case TimeUnit.Nanosecond:
                        foreach (TimeSpan? value in values)
                        {
                            if (value.HasValue)
                                Append((int)value.Value.TotalNanoseconds());
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

        public Time32Array(
            Time32Type type,
            ArrowBuffer valueBuffer, ArrowBuffer nullBitmapBuffer,
            int length, int nullCount, int offset)
            : this(new ArrayData(type, length, nullCount, offset,
                new[] { nullBitmapBuffer, valueBuffer }))
        { }

        public Time32Array(ArrayData data)
            : base(data)
        {
            data.EnsureDataType(ArrowTypeId.Time32);
        }

        private Time32Type TimeType => (Time32Type)Data.DataType;

        public override void Accept(IArrowArrayVisitor visitor) => Accept(this, visitor);

        /// <summary>
        /// Get the time at the specified index as seconds
        /// </summary>
        /// <param name="index">Index at which to get the time.</param>
        /// <returns>Returns an <see cref="Int32" />, or <c>null</c> if there is no object at that index.
        /// </returns>
        public int? GetSeconds(int index)
        {
            int? value = GetValue(index);
            if (value == null)
            {
                return null;
            }

            return TimeType.Unit switch
            {
                TimeUnit.Second => value,
                TimeUnit.Millisecond => value / 1_000,
                _ => throw new InvalidDataException($"Unsupported time unit for Time32Type: {TimeType.Unit}")
            };
        }

        /// <summary>
        /// Get the time at the specified index as milliseconds
        /// </summary>
        /// <param name="index">Index at which to get the time.</param>
        /// <returns>Returns an <see cref="Int32" />, or <c>null</c> if there is no object at that index.
        /// </returns>
        public int? GetMilliSeconds(int index)
        {
            int? value = GetValue(index);
            if (value == null)
            {
                return null;
            }

            return TimeType.Unit switch
            {
                TimeUnit.Second => value * 1_000,
                TimeUnit.Millisecond => value,
                _ => throw new InvalidDataException($"Unsupported time unit for Time32Type: {TimeType.Unit}")
            };
        }

        public new TimeSpan?[] ToArray()
        {
            TimeSpan?[] alloc = new TimeSpan?[Length];

            // Initialize the values
            switch (TimeType.Unit)
            {
                case TimeUnit.Second:
                    for (int i = 0; i < Length; i++)
                    {
                        alloc[i] = IsValid(i) ? TimeSpan.FromSeconds(Values[i]) : null;
                    }
                    break;
                case TimeUnit.Millisecond:
                    for (int i = 0; i < Length; i++)
                    {
                        alloc[i] = IsValid(i) ? TimeSpan.FromMilliseconds(Values[i]) : null;
                    }
                    break;
                default:
                    throw new InvalidDataException($"Unsupported time unit for Time32Type: {TimeType.Unit}");
            }

            return alloc;
        }

        public new TimeSpan[] ToArray(bool notNull = true)
        {
            TimeSpan[] alloc = new TimeSpan[Length];

            // Initialize the values
            switch (TimeType.Unit)
            {
                case TimeUnit.Second:
                    for (int i = 0; i < Length; i++)
                    {
                        alloc[i] = TimeSpan.FromSeconds(Values[i]);
                    }
                    break;
                case TimeUnit.Millisecond:
                    for (int i = 0; i < Length; i++)
                    {
                        alloc[i] = TimeSpan.FromMilliseconds(Values[i]);
                    }
                    break;
                default:
                    throw new InvalidDataException($"Unsupported time unit for Time32Type: {TimeType.Unit}");
            }

            return alloc;
        }
    }
}
