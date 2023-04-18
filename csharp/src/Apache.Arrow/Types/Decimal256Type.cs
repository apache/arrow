﻿// Licensed to the Apache Software Foundation (ASF) under one or more
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

namespace Apache.Arrow.Types
{
    public sealed class Decimal256Type: FixedSizeBinaryType
    {
        // max int decimal 79228162514264337593543950335M : precision = 29
        // max scaled decimal 1.2345678901234567890123456789M : scale = 28
        public static readonly Decimal256Type SystemDefault = new(29, 28, 32);
        public override ArrowTypeId TypeId => ArrowTypeId.Decimal256;
        public override string Name => "decimal256";

        public int Precision { get; }
        public int Scale { get; }

        public Decimal256Type(int precision, int scale)
            : this(precision, scale, 32)
        {
        }

        public Decimal256Type(int precision, int scale, int byteWidth)
            : base(byteWidth)
        {
            Precision = precision;
            Scale = scale;
        }

        public override void Accept(IArrowTypeVisitor visitor) => Accept(this, visitor);
    }
}
