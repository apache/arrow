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
using System.Collections.Generic;
using System.Text;

namespace Apache.Arrow.Types
{
    public class FixedSizeBinaryType: FixedWidthType
    {
        public override ArrowTypeId TypeId => ArrowTypeId.FixedSizedBinary;
        public override string Name => "fixed_size_binary";
        public int ByteWidth { get; }
        public override int BitWidth => ByteWidth * 8;

        public FixedSizeBinaryType(int byteWidth)
        {
            if (byteWidth <= 0)
                throw new ArgumentOutOfRangeException(nameof(byteWidth));

            ByteWidth = byteWidth;
        }

        public override void Accept(IArrowTypeVisitor visitor)
        {
            if (visitor is IArrowTypeVisitor<FixedSizeBinaryType> v)
                v.Visit(this);
        }

        
    }
}
