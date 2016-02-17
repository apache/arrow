/**
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
 */

package org.apache.arrow.vector.complex.impl;

import java.util.Iterator;

import org.apache.arrow.vector.complex.RepeatedMapVector;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.complex.writer.BaseWriter.MapWriter;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.Types.MajorType;
import org.apache.arrow.vector.types.Types.MinorType;

public class SingleLikeRepeatedMapReaderImpl extends AbstractFieldReader{

  private RepeatedMapReaderImpl delegate;

  public SingleLikeRepeatedMapReaderImpl(RepeatedMapVector vector, FieldReader delegate) {
    this.delegate = (RepeatedMapReaderImpl) delegate;
  }

  @Override
  public int size() {
    throw new UnsupportedOperationException("You can't call size on a single map reader.");
  }

  @Override
  public boolean next() {
    throw new UnsupportedOperationException("You can't call next on a single map reader.");
  }

  @Override
  public MajorType getType() {
    return Types.required(MinorType.MAP);
  }


  @Override
  public void copyAsValue(MapWriter writer) {
    delegate.copyAsValueSingle(writer);
  }

  public void copyAsValueSingle(MapWriter writer){
    delegate.copyAsValueSingle(writer);
  }

  @Override
  public FieldReader reader(String name) {
    return delegate.reader(name);
  }

  @Override
  public void setPosition(int index) {
    delegate.setPosition(index);
  }

  @Override
  public Object readObject() {
    return delegate.readObject();
  }

  @Override
  public Iterator<String> iterator() {
    return delegate.iterator();
  }

  @Override
  public boolean isSet() {
    return ! delegate.isNull();
  }


}
