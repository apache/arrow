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

namespace Apache.Arrow
{
    // Helpers to Deconstruct Tuples on netstandard
    internal static partial class TupleExtensions
    {
        public static void Deconstruct<T1, T2>(this Tuple<T1, T2> value, out T1 item1, out T2 item2)
        {
            item1 = value.Item1;
            item2 = value.Item2;
        }

        public static void Deconstruct<T1, T2, T3>(this Tuple<T1, T2, T3> value, out T1 item1, out T2 item2, out T3 item3)
        {
            item1 = value.Item1;
            item2 = value.Item2;
            item3 = value.Item3;
        }
    }
}
