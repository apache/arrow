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

namespace Apache.Arrow
{
    public class ArrayTypeComparer
    {
        private readonly IArrowType _expectedType;

        public ArrayTypeComparer(IArrowType expectedType)
        {
            //Debug.Assert(expectedType != null);
            _expectedType = expectedType;
        }

        private static void CompareNested(NestedType expectedType, NestedType actualType)
        {
            if (expectedType.Fields.Count != actualType.Fields.Count)
            {
                return;
            }

            for (int i = 0; i < expectedType.Fields.Count; i++)
            {
                FieldComparer.Compare(expectedType.Fields[i], actualType.Fields[i]);
            }
        }

        public void Visit(IArrowType actualType)
        {
            Assert.IsAssignableFrom(actualType.GetType(), _expectedType);
        }
    }
}
