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


import java.util.Map;

import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.NonNullableMapVector;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.complex.writer.BaseWriter.MapWriter;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.Field;

import com.google.common.collect.Maps;

@SuppressWarnings("unused")
public class SingleMapReaderImpl extends AbstractFieldReader {

  private final NonNullableMapVector vector;
  private final Map<String, FieldReader> fields = Maps.newHashMap();

  public SingleMapReaderImpl(NonNullableMapVector vector) {
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
    return MinorType.MAP;
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
  public void copyAsValue(MapWriter writer) {
    SingleMapWriter impl = (SingleMapWriter) writer;
    impl.container.copyFromSafe(idx(), impl.idx(), vector);
  }

  @Override
  public void copyAsField(String name, MapWriter writer) {
    SingleMapWriter impl = (SingleMapWriter) writer.map(name);
    impl.container.copyFromSafe(idx(), impl.idx(), vector);
  }


}

