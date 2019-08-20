/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.arrow.vector.compare;

import org.apache.arrow.vector.BaseFixedWidthVector;
import org.apache.arrow.vector.BaseVariableWidthVector;
import org.apache.arrow.vector.ZeroVector;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.NonNullableStructVector;
import org.apache.arrow.vector.complex.UnionVector;

/**
 * Generic visitor to visit a {@link org.apache.arrow.vector.ValueVector}.
 * @param <OUT> the output result type.
 * @param <IN> the input data together with visitor.
 */
public interface VectorVisitor<OUT, IN> {

  OUT visit(BaseFixedWidthVector left, IN value);

  OUT visit(BaseVariableWidthVector left, IN value);

  OUT visit(ListVector left, IN value);

  OUT visit(FixedSizeListVector left, IN value);

  OUT visit(NonNullableStructVector left, IN value);

  OUT visit(UnionVector left, IN value);

  OUT visit(ZeroVector left, IN value);
}
