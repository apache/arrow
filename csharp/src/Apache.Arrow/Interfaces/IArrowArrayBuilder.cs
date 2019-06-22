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

using Apache.Arrow.Memory;
using System;
using System.Collections.Generic;

namespace Apache.Arrow
{
    public interface IArrowArrayBuilder { }

    public interface IArrowArrayBuilder<out TArray> : IArrowArrayBuilder
        where TArray: IArrowArray
    {
        TArray Build(MemoryAllocator allocator);
    }

    public interface IArrowArrayBuilder<T, out TArray> : IArrowArrayBuilder<TArray>
        where TArray: IArrowArray { }

    public interface IArrowArrayBuilder<T, out TArray, out TBuilder> : IArrowArrayBuilder<T, TArray>
        where TArray : IArrowArray
        where TBuilder : IArrowArrayBuilder<TArray>
    {
        TBuilder Append(T value);
        TBuilder Append(ReadOnlySpan<T> span);
        TBuilder AppendRange(IEnumerable<T> values);
        TBuilder Reserve(int capacity);
        TBuilder Resize(int length);
        TBuilder Swap(int i, int j);
        TBuilder Set(int index, T value);
        TBuilder Clear();
    }
}