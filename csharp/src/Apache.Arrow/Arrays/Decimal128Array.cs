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
    public class Decimal128Array : FixedSizeBinaryArray, IReadOnlyList<SqlDecimal?>
    {
        public class Builder : BuilderBase<Decimal128Array, Builder>
        {
            public Builder(Decimal128Type type) : base(type, 16)
            {
                DataType = type;
            }

            protected new Decimal128Type DataType { get; }

            protected override Decimal128Array Build(ArrayData data)
            {
                return new Decimal128Array(data);
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

        public Decimal128Array(ArrayData data)
            : base(ArrowTypeId.Decimal128, data)
        {
            data.EnsureDataType(ArrowTypeId.Decimal128);
            data.EnsureBufferCount(2);
            Debug.Assert(Data.DataType is Decimal128Type);
        }
        public override void Accept(IArrowArrayVisitor visitor) => Accept(this, visitor);

        public int Scale => ((Decimal128Type)Data.DataType).Scale;
        public int Precision => ((Decimal128Type)Data.DataType).Precision;
        public int ByteWidth => ((Decimal128Type)Data.DataType).ByteWidth;

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

        public SqlDecimal? GetSqlDecimal(int index)
        {
            if (IsNull(index))
            {
                return null;
            }

            return DecimalUtility.GetSqlDecimal128(ValueBuffer, index, Precision, Scale);
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

        IEnumerator IEnumerable.GetEnumerator() => ((IEnumerable<SqlDecimal>)this).GetEnumerator();
    }
}
