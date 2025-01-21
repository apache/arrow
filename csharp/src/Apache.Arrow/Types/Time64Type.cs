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
    public sealed class Time64Type : TimeType
    {
        public static Time64Type Default => Nanosecond;

        public override ArrowTypeId TypeId => ArrowTypeId.Time64;
        public override string Name => "time64";
        public override int BitWidth => 64;

        public Time64Type(TimeUnit unit = TimeUnit.Nanosecond)
            : base(unit) { }

        public override void Accept(IArrowTypeVisitor visitor) => Accept(this, visitor);
    }
}
