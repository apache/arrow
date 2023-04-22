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
using System.Linq;

namespace Apache.Arrow.Types
{
    public abstract class NestedType : ArrowType
    {
        [Obsolete("Use `Fields` instead")]
        public IReadOnlyList<Field> Children => Fields;

        public IReadOnlyList<Field> Fields { get; }

        protected NestedType(IReadOnlyList<Field> fields)
        {
            if (fields == null || fields.Count == 0)
            {
                throw new ArgumentNullException(nameof(fields));
            }
            Fields = fields;
        }

        protected NestedType(Field field)
        {
            if (field == null)
            {
                throw new ArgumentNullException(nameof(field));
            }
            Fields = new Field[] { field };
        }

        // Equality
        public override bool Equals(object obj)
        {
            if (obj == null || obj is not ArrowType other)
            {
                return false;
            }
            return Equals(other);
        }

        public new bool Equals(ArrowType other)
            => base.Equals(other) && other is NestedType _other && Fields.SequenceEqual(_other.Fields);

        public override int GetHashCode() => Tuple.Create(base.GetHashCode(), Hash32Array(Fields.Select(f => f.GetHashCode()).ToArray())).GetHashCode();

        private static int Hash32Array(int[] array)
        {
            int length = array.Length;

            switch (length)
            {
                case 0:
                    return 0;
                case 1:
                    return Tuple.Create(array[0]).GetHashCode();
                case 2:
                    return Tuple.Create(array[0], array[1]).GetHashCode();
                case 3:
                    return Tuple.Create(array[0], array[1], array[2]).GetHashCode();
                case 4:
                    return Tuple.Create(array[0], array[1], array[2], array[3]).GetHashCode();
                case 5:
                    return Tuple.Create(array[0], array[1], array[2], array[3], array[4]).GetHashCode();
                case 6:
                    return Tuple.Create(array[0], array[1], array[2], array[3], array[4], array[5]).GetHashCode();
                case 7:
                    return Tuple.Create(array[0], array[1], array[2], array[3], array[4], array[5], array[6]).GetHashCode();
                case 8:
                    return Tuple.Create(array[0], array[1], array[2], array[3], array[4], array[5], array[6], array[7]).GetHashCode();
                default:
                    IEnumerable<int[]> int8s = Enumerable.Range(0, length)
                        .Where(i => i % 8 == 0) // select starting indices of each block
                        .Select(i => array.Skip(i).Take(Math.Min(length - i, 8)).ToArray());
                    return Hash32Array(int8s.Select(Hash32Array).ToArray());
            }
        }
    }
}
