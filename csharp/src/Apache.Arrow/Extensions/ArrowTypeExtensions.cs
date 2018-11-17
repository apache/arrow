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

using System.Collections.Generic;
using Apache.Arrow.Types;

namespace Apache.Arrow
{
    public static class ArrowTypeExtensions
    {
        private static readonly ISet<ArrowTypeId> IntegralTypes = 
            new HashSet<ArrowTypeId>(new[]
            {
                ArrowTypeId.Int8, ArrowTypeId.Int16, ArrowTypeId.Int32, ArrowTypeId.Int64,
                ArrowTypeId.UInt8, ArrowTypeId.UInt16, ArrowTypeId.UInt32, ArrowTypeId.UInt64,
            });

        private static readonly ISet<ArrowTypeId> FloatingPointTypes =
            new HashSet<ArrowTypeId>(new[]
            {
                ArrowTypeId.HalfFloat, ArrowTypeId.Float, ArrowTypeId.Double
            });

        public static bool IsIntegral(this IArrowType type) 
            => IntegralTypes.Contains(type.TypeId);

        public static bool IsFloatingPoint(this IArrowType type)
            => FloatingPointTypes.Contains(type.TypeId);
    }
}
