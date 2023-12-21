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
using System.Data.SqlTypes;
using System.Diagnostics;
using Apache.Arrow.Arrays;
using Apache.Arrow.Types;

namespace Apache.Arrow
{
    public class Decimal256Array : FixedSizeBinaryArray, IReadOnlyList<SqlDecimal?>, IReadOnlyList<string>
    {
        public class Builder : BuilderBase<Decimal256Array, Builder>
        {
            public Builder(Decimal256Type type) : base(type, 32)
            {
                DataType = type;
            }

            protected new Decimal256Type DataType { get; }

            protected override Decimal256Array Build(ArrayData data)
            {
                return new Decimal256Array(data);
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

            public Builder Append(SqlDecimal value)
            {
                Span<byte> bytes = stackalloc byte[DataType.ByteWidth];
                DecimalUtility.GetBytes(value, DataType.Precision, DataType.Scale, bytes);
                if (!value.IsPositive)
                {
                    var span = bytes.CastTo<long>();
                    span[2] = -1;
                    span[3] = -1;
                }

                return Append(bytes);
            }

            public Builder AppendRange(IEnumerable<SqlDecimal> values)
            {
                if (values == null)
                {
                    throw new ArgumentNullException(nameof(values));
                }

                foreach (SqlDecimal d in values)
                {
                    Append(d);
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

        public Decimal256Array(ArrayData data)
            : base(ArrowTypeId.Decimal256, data)
        {
            data.EnsureDataType(ArrowTypeId.Decimal256);
            data.EnsureBufferCount(2);
            Debug.Assert(Data.DataType is Decimal256Type);
        }
        public override void Accept(IArrowArrayVisitor visitor) => Accept(this, visitor);

        public int Scale => ((Decimal256Type)Data.DataType).Scale;
        public int Precision => ((Decimal256Type)Data.DataType).Precision;
        public int ByteWidth => ((Decimal256Type)Data.DataType).ByteWidth;

        public decimal? GetValue(int index)
        {
            if (IsNull(index))
            {
                return null;
            }

            return DecimalUtility.GetDecimal(ValueBuffer, index, Scale, ByteWidth);
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
            return DecimalUtility.GetString(ValueBuffer, index, Precision, Scale, ByteWidth);
        }

        public bool TryGetSqlDecimal(int index, out SqlDecimal? value)
        {
            if (IsNull(index))
            {
                value = null;
                return true;
            }

            const int longWidth = 4;
            var span = ValueBuffer.Span.CastTo<long>().Slice(index * longWidth);
            if ((span[2] == 0 && span[3] == 0) ||
                (span[2] == -1 && span[3] == -1))
            {
                value = DecimalUtility.GetSqlDecimal128(ValueBuffer, 2 * index, Precision, Scale);
                return true;
            }

            value = null;
            return false;
        }

        private SqlDecimal? GetSqlDecimal(int index)
        {
            SqlDecimal? value;
            if (TryGetSqlDecimal(index, out value))
            {
                return value;
            }

            throw new OverflowException("decimal256 value out of range of SqlDecimal");
        }

        int IReadOnlyCollection<SqlDecimal?>.Count => Length;
        SqlDecimal? IReadOnlyList<SqlDecimal?>.this[int index] => GetSqlDecimal(index);

        IEnumerator<SqlDecimal?> IEnumerable<SqlDecimal?>.GetEnumerator()
        {
            for (int index = 0; index < Length; index++)
            {
                yield return GetSqlDecimal(index);
            }
        }

        int IReadOnlyCollection<string>.Count => Length;
        string? IReadOnlyList<string>.this[int index] => GetString(index);

        IEnumerator<string> IEnumerable<string>.GetEnumerator()
        {
            for (int index = 0; index < Length; index++)
            {
                yield return GetString(index);
            }
        }

        IEnumerator IEnumerable.GetEnumerator() => ((IEnumerable<string>)this).GetEnumerator();
    }
}
