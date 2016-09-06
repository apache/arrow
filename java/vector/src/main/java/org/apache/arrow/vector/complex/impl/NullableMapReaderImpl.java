/*******************************************************************************

 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package org.apache.arrow.vector.complex.impl;

import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.NullableMapVector;
import org.apache.arrow.vector.complex.writer.BaseWriter.MapWriter;

public class NullableMapReaderImpl extends SingleMapReaderImpl {

  private NullableMapVector nullableMapVector;

  public NullableMapReaderImpl(MapVector vector) {
    super((NullableMapVector)vector);
    this.nullableMapVector = (NullableMapVector)vector;
  }

  @Override
  public void copyAsValue(MapWriter writer){
    NullableMapWriter impl = (NullableMapWriter) writer;
    impl.container.copyFromSafe(idx(), impl.idx(), nullableMapVector);
  }

  @Override
  public void copyAsField(String name, MapWriter writer){
    NullableMapWriter impl = (NullableMapWriter) writer.map(name);
    impl.container.copyFromSafe(idx(), impl.idx(), nullableMapVector);
  }
}
