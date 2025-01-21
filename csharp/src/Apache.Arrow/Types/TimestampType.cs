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
    public sealed class TimestampType : TimeBasedType
    {
        public static readonly TimestampType Default = new TimestampType(TimeUnit.Millisecond, "+00:00");

        public override ArrowTypeId TypeId => ArrowTypeId.Timestamp;
        public override string Name => "timestamp";
        public override int BitWidth => 64;

        public string Timezone { get; }

        public bool IsTimeZoneAware => !string.IsNullOrWhiteSpace(Timezone);

        public TimestampType(
            TimeUnit unit = TimeUnit.Millisecond,
            string timezone = default)
            : base(unit)
        {
            Timezone = timezone;
        }

        public TimestampType(
            TimeUnit unit = TimeUnit.Millisecond,
            TimeZoneInfo timezone = default)
            : base(unit)
        {
            Timezone = timezone?.BaseUtcOffset.ToTimeZoneOffsetString();
        }

        public override void Accept(IArrowTypeVisitor visitor) => Accept(this, visitor);
    }
}
