
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


import org.apache.arrow.vector.complex.AbstractContainerVector;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.complex.writer.BaseWriter.ListWriter;
import org.apache.arrow.vector.complex.writer.BaseWriter.MapWriter;
import org.apache.arrow.vector.types.Types.MinorType;

@SuppressWarnings("unused")
public class SingleListReaderImpl extends AbstractFieldReader{

  private final String name;
  private final AbstractContainerVector container;
  private FieldReader reader;

  public SingleListReaderImpl(String name, AbstractContainerVector container) {
    super();
    this.name = name;
    this.container = container;
  }

  @Override
  public void setPosition(int index) {
    super.setPosition(index);
    if (reader != null) {
      reader.setPosition(index);
    }
  }

  @Override
  public Object readObject() {
    return reader.readObject();
  }

  @Override
  public FieldReader reader() {
    if (reader == null) {
      reader = container.getChild(name).getReader();
      setPosition(idx());
    }
    return reader;
  }

  @Override
  public MinorType getMinorType() {
    return MinorType.LIST;
  }

  @Override
  public boolean isSet() {
    return false;
  }

  @Override
  public void copyAsValue(ListWriter writer) {
    throw new UnsupportedOperationException("Generic list copying not yet supported.  Please resolve to typed list.");
  }

  @Override
  public void copyAsField(String name, MapWriter writer) {
    throw new UnsupportedOperationException("Generic list copying not yet supported.  Please resolve to typed list.");
  }

}
