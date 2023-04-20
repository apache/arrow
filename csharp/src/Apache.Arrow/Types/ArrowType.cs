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


using Apache.Arrow.Util;

namespace Apache.Arrow.Types
{
    public abstract class ArrowType: IArrowType
    {
        public abstract ArrowTypeId TypeId { get; }

        public abstract string Name { get; }

        public virtual bool IsFixedWidth => false;

        // C# object methods
        public override bool Equals(object obj)
        {
            if (obj == null || obj is not ArrowType other)
            {
                return false;
            }

            return Equals(other);
        }

        public bool Equals(IArrowType other)
        {
            return TypeId == other.TypeId
                && IsFixedWidth == other.IsFixedWidth
                && Name == other.Name;
        }

        public override int GetHashCode()
        {
            checked
            {
                return HashUtil.CombineHash32(TypeId.GetHashCode(), IsFixedWidth.GetHashCode(), HashUtil.Hash32(Name));
            }
        }

        // Instance methods
        public abstract void Accept(IArrowTypeVisitor visitor);

        internal static void Accept<T>(T type, IArrowTypeVisitor visitor)
            where T: class, IArrowType
        {
            switch (visitor)
            {
                case IArrowTypeVisitor<T> typedVisitor:
                    typedVisitor.Visit(type);
                    break;
                default:
                    visitor.Visit(type);
                    break;
            }
        }
    }
}
