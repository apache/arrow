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
    public static class DetaTimeExtensions
    {
        /// <summary>
        /// Number of days from 1970-01-01
        /// </summary>
        /// <param name="dt">DateTime to double</param>
        /// <returns>number of days</returns>
        public static double ToUnixDays(this DateTime dt)
        {
            return (dt.Date - Types.Convert.EpochDateTime).TotalDays;
        }
    }
}
