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

using System;

namespace Apache.Arrow.Types
{
    public sealed class Decimal128Type : FixedSizeBinaryType
    {
        public override ArrowTypeId TypeId => ArrowTypeId.Decimal128;
        public override string Name => "decimal128";

        public int Precision { get; }
        public int Scale { get; }

        public Decimal128Type(int precision, int scale)
            : base(16)
        {
            Precision = precision;
            Scale = scale;
        }

        public override void Accept(IArrowTypeVisitor visitor) => Accept(this, visitor);

        // Equality
        public override bool Equals(object obj)
        {
            if (obj == null || obj is not ArrowType other)
            {
                return false;
            }
            return Equals(other);
        }

        public new bool Equals(IArrowType other)
            => base.Equals(other) && other is Decimal128Type _other && Precision == _other.Precision && Scale == _other.Scale;

        public override int GetHashCode() => Tuple.Create(base.GetHashCode(), Precision, Scale).GetHashCode();
    }
}
