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
    public class Date32Type: DateType
    {
        public static readonly Date32Type Default = new Date32Type();

        public override ArrowTypeId TypeId => ArrowTypeId.Date32;
        public override string Name => "date32";
        public override int BitWidth => 32;
        public override DateUnit Unit => DateUnit.Day;

        public override void Accept(IArrowTypeVisitor visitor)
        {
            if (visitor is IArrowTypeVisitor<Date32Type> v)
                v.Visit(this);
        }
    }
}
