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
    public abstract class FloatingPointType: NumberType
    {
        public enum PrecisionKind
        {
            Half,
            Single,
            Double
        }

        public abstract PrecisionKind Precision { get; }

        // Equality
        public override bool Equals(object obj)
        {
            if (obj == null || obj is not ArrowType other)
            {
                return false;
            }
            return Equals(other);
        }

        public new bool Equals(ArrowType other)
            => base.Equals(other) && other is FloatingPointType _other && Precision == _other.Precision;

        public override int GetHashCode() => Tuple.Create(base.GetHashCode(), Precision).GetHashCode();
    }
}
