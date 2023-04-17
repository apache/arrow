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
using System.Numerics;

namespace Apache.Arrow.Types
{
    public sealed class Decimal128Type : FixedSizeBinaryType
    {
        public static readonly Decimal128Type Default = new(29, 9, 16);
        public override ArrowTypeId TypeId => ArrowTypeId.Decimal128;
        public override string Name => "decimal128";

        public int Precision { get; }
        public int Scale { get; }

        public Decimal128Type(int precision, int scale)
            : this(precision, scale, DecimalUtility.GetByteWidth(BigInteger.Parse(new string('9', precision + scale))))
        {
        }

        public Decimal128Type(int precision, int scale, int byteWidth)
            : base(byteWidth)
        {
            if (byteWidth > 16)
            {
                throw new OverflowException($"Cannot create Decimal128Type({precision}, {scale}, {byteWidth}), byteWidth should be between 1 and 16");
            }
            if (precision > 38)
            {
                throw new OverflowException($"Cannot create Decimal128Type({precision}, {scale}, {byteWidth}), precision should be between 1 and 38");
            }

            Precision = precision;
            Scale = scale;
        }

        public override void Accept(IArrowTypeVisitor visitor) => Accept(this, visitor);
    }
}
