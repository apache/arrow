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
using System.Collections;
using System.Collections.Generic;
using Apache.Arrow.Types;

namespace Apache.Arrow
{
    public class DurationArray : PrimitiveArray<long>, IReadOnlyList<TimeSpan?>
    {
        public class Builder : PrimitiveArrayBuilder<long, DurationArray, Builder>
        {
            public DurationType DataType { get; }

            public Builder(DurationType dataType)
            {
                DataType = dataType;
            }

            protected override DurationArray Build(
                ArrowBuffer valueBuffer, ArrowBuffer nullBitmapBuffer,
                int length, int nullCount, int offset) =>
                new DurationArray(DataType, valueBuffer, nullBitmapBuffer, length, nullCount, offset);

            /// <summary>
            /// Append a duration in the form of a <see cref="TimeSpan"/> object to the array.
            /// </summary>
            /// <param name="value">TimeSpan to add.</param>
            /// <returns>Returns the builder (for fluent-style composition).</returns>
            public Builder Append(TimeSpan value)
            {
                Append(DataType.Unit.ConvertFromTicks(value.Ticks));
                return this;
            }

            /// <summary>
            /// Append a duration in the form of a <see cref="TimeSpan"/> object to the array.
            /// </summary>
            /// <param name="value">TimeSpan to add.</param>
            /// <returns>Returns the builder (for fluent-style composition).</returns>
            public Builder Append(TimeSpan? value) =>
                (value == null) ? AppendNull() : Append(value.Value);
        }

        public DurationArray(
            DurationType type,
            ArrowBuffer valueBuffer, ArrowBuffer nullBitmapBuffer,
            int length, int nullCount, int offset)
            : this(new ArrayData(type, length, nullCount, offset,
                new[] { nullBitmapBuffer, valueBuffer }))
        { }

        public DurationArray(ArrayData data)
            : base(data)
        {
            data.EnsureDataType(ArrowTypeId.Duration);
        }

        public DurationType DataType => (DurationType)this.Data.DataType;

        public TimeSpan? GetTimeSpan(int index)
        {
            if (index < 0 || index >= Length)
            {
                throw new ArgumentOutOfRangeException(nameof(index));
            }
            return IsValid(index) ? new TimeSpan(DataType.Unit.ConvertToTicks(Values[index])) : null;
        }

        public override void Accept(IArrowArrayVisitor visitor) => Accept(this, visitor);

        int IReadOnlyCollection<TimeSpan?>.Count => Length;
        TimeSpan? IReadOnlyList<TimeSpan?>.this[int index] => GetTimeSpan(index);

        IEnumerator<TimeSpan?> IEnumerable<TimeSpan?>.GetEnumerator()
        {
            for (int index = 0; index < Length; index++)
            {
                yield return GetTimeSpan(index);
            }
        }

        IEnumerator IEnumerable.GetEnumerator() => ((IEnumerable<TimeSpan?>)this).GetEnumerator();
    }
}
