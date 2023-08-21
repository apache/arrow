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
    public sealed class FixedSizeListType : NestedType
    {
        public override ArrowTypeId TypeId => ArrowTypeId.FixedSizeList;
        public override string Name => "fixed_size_list";
        public int ListSize { get; }

        public Field ValueField => Fields[0];

        public IArrowType ValueDataType => Fields[0].DataType;

        public FixedSizeListType(Field valueField, int listSize)
           : base(valueField)
        {
            if (listSize <= 0)
                throw new ArgumentOutOfRangeException(nameof(listSize));

            ListSize = listSize;
        }

        public FixedSizeListType(IArrowType valueDataType, int listSize)
            : this(new Field("item", valueDataType, true), listSize) { }

        public override void Accept(IArrowTypeVisitor visitor) => Accept(this, visitor);
    }
}
