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

using Apache.Arrow.Flatbuf;
using System;
using System.Collections.Generic;
using System.Text;

namespace Apache.Arrow
{
    internal static class Utility
    {
        public static IList<T> DeleteListElement<T>(IList<T> values, int index)
        {
            if (index < 0 || index >= values.Count)
            {
                throw new ArgumentException("Invalid index", nameof(index));
            }

            List<T> newList = new List<T>(values.Count - 1);
            for (int i = 0; i < index; i++)
            {
                newList.Add(values[i]);
            }
            for (int i = index + 1; i < values.Count; i++)
            {
                newList.Add(values[i]);
            }

            return newList;
        }

        public static IList<T> AddListElement<T>(IList<T> values, int index, T newElement)
        {
            if (index < 0 || index > values.Count)
            {
                throw new ArgumentException("Invalid index", nameof(index));
            }

            List<T> newList = new List<T>(values.Count + 1);
            for (int i = 0; i < index; i++)
            {
                newList.Add(values[i]);
            }
            newList.Add(newElement);
            for (int i = index; i < values.Count; i++)
            {
                newList.Add(values[i]);
            }

            return newList;
        }

        public static IList<T> SetListElement<T>(IList<T> values, int index, T newElement)
        {
            if (index < 0 || index >= values.Count)
            {
                throw new ArgumentException("Invalid index", nameof(index));
            }

            List<T> newList = new List<T>(values.Count);
            for (int i = 0; i < index; i++)
            {
                newList.Add(values[i]);
            }
            newList.Add(newElement);
            for (int i = index + 1; i < values.Count; i++)
            {
                newList.Add(values[i]);
            }

            return newList;
        }
    }
}
