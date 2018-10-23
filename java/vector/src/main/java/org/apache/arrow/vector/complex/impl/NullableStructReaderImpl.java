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

package org.apache.arrow.vector.complex.impl;

import org.apache.arrow.vector.complex.NonNullableStructVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.writer.BaseWriter.StructWriter;
import org.apache.arrow.vector.types.pojo.Field;

public class NullableStructReaderImpl extends SingleStructReaderImpl {

  private StructVector nullableStructVector;

  public NullableStructReaderImpl(NonNullableStructVector vector) {
    super(vector);
    this.nullableStructVector = (StructVector) vector;
  }

  @Override
  public Field getField() {
    return nullableStructVector.getField();
  }

  @Override
  public void copyAsValue(StructWriter writer) {
    NullableStructWriter impl = (NullableStructWriter) writer;
    impl.container.copyFromSafe(idx(), impl.idx(), nullableStructVector);
  }

  @Override
  public void copyAsField(String name, StructWriter writer) {
    NullableStructWriter impl = (NullableStructWriter) writer.struct(name);
    impl.container.copyFromSafe(idx(), impl.idx(), nullableStructVector);
  }

  @Override
  public boolean isSet() {
    return !nullableStructVector.isNull(idx());
  }
}
