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


import java.util.HashMap;
import java.util.Map;

import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.complex.NonNullableStructVector;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.complex.writer.BaseWriter.StructWriter;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.Field;

@SuppressWarnings("unused")
public class SingleStructReaderImpl extends AbstractFieldReader {

  private final NonNullableStructVector vector;
  private final Map<String, FieldReader> fields = new HashMap<>();

  public SingleStructReaderImpl(NonNullableStructVector vector) {
    this.vector = vector;
  }

  private void setChildrenPosition(int index) {
    for (FieldReader r : fields.values()) {
      r.setPosition(index);
    }
  }

  @Override
  public Field getField() {
    return vector.getField();
  }

  @Override
  public FieldReader reader(String name) {
    FieldReader reader = fields.get(name);
    if (reader == null) {
      ValueVector child = vector.getChild(name);
      if (child == null) {
        reader = NullReader.INSTANCE;
      } else {
        reader = child.getReader();
      }
      fields.put(name, reader);
      reader.setPosition(idx());
    }
    return reader;
  }

  @Override
  public void setPosition(int index) {
    super.setPosition(index);
    for (FieldReader r : fields.values()) {
      r.setPosition(index);
    }
  }

  @Override
  public Object readObject() {
    return vector.getObject(idx());
  }

  @Override
  public MinorType getMinorType() {
    return MinorType.STRUCT;
  }

  @Override
  public boolean isSet() {
    return true;
  }

  @Override
  public java.util.Iterator<String> iterator() {
    return vector.fieldNameIterator();
  }

  @Override
  public void copyAsValue(StructWriter writer) {
    SingleStructWriter impl = (SingleStructWriter) writer;
    impl.container.copyFromSafe(idx(), impl.idx(), vector);
  }

  @Override
  public void copyAsField(String name, StructWriter writer) {
    SingleStructWriter impl = (SingleStructWriter) writer.struct(name);
    impl.container.copyFromSafe(idx(), impl.idx(), vector);
  }


}

