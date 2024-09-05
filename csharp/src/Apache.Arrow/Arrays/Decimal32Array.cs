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
using System.Diagnostics;
using Apache.Arrow.Arrays;
using Apache.Arrow.Types;

namespace Apache.Arrow
{
    public class Decimal32Array : FixedSizeBinaryArray, IReadOnlyList<decimal?>
    {
        public class Builder : BuilderBase<Decimal32Array, Builder>
        {
            public Builder(Decimal32Type type) : base(type, 4)
            {
                DataType = type;
            }

            protected new Decimal32Type DataType { get; }

            protected override Decimal32Array Build(ArrayData data)
            {
                return new Decimal32Array(data);
            }

            public Builder Append(decimal value)
            {
                Span<byte> bytes = stackalloc byte[DataType.ByteWidth];
                DecimalUtility.GetBytes(value, DataType.Precision, DataType.Scale, DataType.ByteWidth, bytes);

                return Append(bytes);
            }

            public Builder AppendRange(IEnumerable<decimal> values)
            {
                if (values == null)
                {
                    throw new ArgumentNullException(nameof(values));
                }

                foreach (decimal d in values)
                {
                    Append(d);
                }

                return Instance;
            }

            public Builder Append(string value)
            {
                if (value == null)
                {
                    AppendNull();
                }
                else
                {
                    Span<byte> bytes = stackalloc byte[DataType.ByteWidth];
                    DecimalUtility.GetBytes(value, DataType.Precision, DataType.Scale, ByteWidth, bytes);
                    Append(bytes);
                }

                return Instance;
            }

            public Builder AppendRange(IEnumerable<string> values)
            {
                if (values == null)
                {
                    throw new ArgumentNullException(nameof(values));
                }

                foreach (string s in values)
                {
                    Append(s);
                }

                return Instance;
            }

            public Builder Set(int index, decimal value)
            {
                Span<byte> bytes = stackalloc byte[DataType.ByteWidth];
                DecimalUtility.GetBytes(value, DataType.Precision, DataType.Scale, DataType.ByteWidth, bytes);

                return Set(index, bytes);
            }
        }

        public Decimal32Array(ArrayData data)
            : base(ArrowTypeId.Decimal32, data)
        {
            data.EnsureDataType(ArrowTypeId.Decimal32);
            data.EnsureBufferCount(2);
            Debug.Assert(Data.DataType is Decimal32Type);
        }
        public override void Accept(IArrowArrayVisitor visitor) => Accept(this, visitor);

        public int Scale => ((Decimal32Type)Data.DataType).Scale;
        public int Precision => ((Decimal32Type)Data.DataType).Precision;
        public int ByteWidth => ((Decimal32Type)Data.DataType).ByteWidth;

        public decimal? GetValue(int index)
        {
            if (IsNull(index))
            {
                return null;
            }
            return DecimalUtility.GetDecimal(ValueBuffer, Offset + index, Scale, ByteWidth);
        }

        public IList<decimal?> ToList(bool includeNulls = false)
        {
            var list = new List<decimal?>(Length);

            for (int i = 0; i < Length; i++)
            {
                decimal? value = GetValue(i);

                if (value.HasValue)
                {
                    list.Add(value.Value);
                }
                else
                {
                    if (includeNulls)
                    {
                        list.Add(null);
                    }
                }
            }

            return list;
        }

        public string GetString(int index)
        {
            if (IsNull(index))
            {
                return null;
            }
            return DecimalUtility.GetString(ValueBuffer, Offset + index, Precision, Scale, ByteWidth);
        }

        public decimal? GetDecimal(int index)
        {
            if (IsNull(index))
            {
                return null;
            }

            return DecimalUtility.GetDecimal(ValueBuffer, Offset + index, Scale, ByteWidth);
        }

        int IReadOnlyCollection<decimal?>.Count => Length;
        decimal? IReadOnlyList<decimal?>.this[int index] => GetDecimal(index);

        IEnumerator<decimal?> IEnumerable<decimal?>.GetEnumerator()
        {
            for (int index = 0; index < Length; index++)
            {
                yield return GetDecimal(index);
            }
        }

        IEnumerator IEnumerable.GetEnumerator() => ((IEnumerable<decimal>)this).GetEnumerator();
    }
}
