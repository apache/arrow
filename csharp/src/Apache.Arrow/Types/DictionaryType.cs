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

namespace Apache.Arrow.Types
{
    public sealed class DictionaryType : FixedWidthType
    {
        [Obsolete]
        public static readonly DictionaryType Default = new DictionaryType(Int64Type.Default, Int64Type.Default, false);

        public DictionaryType(IArrowType indexType, IArrowType valueType, bool ordered)
        {
            if (!(indexType is IntegerType))
            {
                throw new ArgumentException($"{nameof(indexType)} must be integer");
            }

            IndexType = indexType;
            ValueType = valueType;
            Ordered = ordered;
        }

        public override ArrowTypeId TypeId => ArrowTypeId.Dictionary;
        public override string Name => "dictionary";
        public override int BitWidth => ((IntegerType)IndexType).BitWidth;
        public override void Accept(IArrowTypeVisitor visitor) => Accept(this, visitor);

        public IArrowType IndexType { get; private set; }
        public IArrowType ValueType { get; private set; }
        public bool Ordered { get; private set; }
    }
}
