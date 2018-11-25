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
    public class Int8Type: NumberType
    {
        public static readonly Int8Type Default = new Int8Type();

        public override ArrowTypeId TypeId => ArrowTypeId.Int8;
        public override string Name => "int8";
        public override int BitWidth => 8;
        public override bool IsSigned => true;

        public override void Accept(IArrowTypeVisitor visitor)
        {
            if (visitor is IArrowTypeVisitor<Int8Type> v)
                v.Visit(this);
        }
    }
}
