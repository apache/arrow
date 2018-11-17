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

using Apache.Arrow.Types;
using System;

namespace Apache.Arrow
{
    internal static class ArrayDataExtensions
    {
        public static void EnsureBufferCount(this ArrayData data, int count)
        {
            if (data.Buffers.Length != count)
            {
                // TODO: Use localizable string resource
                throw new ArgumentException(
                    $"Buffer count <{data.Buffers.Length}> must be at least <{count}>",
                    nameof(data.Buffers.Length));
            }
        }

        public static void EnsureDataType(this ArrayData data, params ArrowTypeId[] ids)
        {
            var valid = true;

            foreach (var id in ids)
            {
                if (data.DataType.TypeId != id)
                    valid = false;
            }

            if (!valid)
            {
                // TODO: Use localizable string resource
                throw new ArgumentException(
                    $"Specified array type <{data.DataType.TypeId}> does not match expected type(s) <{string.Join(",", ids)}>",
                    nameof(data.DataType.TypeId));
            }
        }
    }
}
