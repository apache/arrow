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

import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.NullableMapVector;
import org.apache.arrow.vector.complex.StateTool;
import org.apache.arrow.vector.complex.writer.BaseWriter.ComplexWriter;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.Field;

import com.google.common.base.Preconditions;

public class ComplexWriterImpl extends AbstractFieldWriter implements ComplexWriter {
//  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ComplexWriterImpl.class);

  private NullableMapWriter mapRoot;
  private UnionListWriter listRoot;
  private final MapVector container;

  Mode mode = Mode.INIT;
  private final String name;
  private final boolean unionEnabled;
  private final NullableMapWriterFactory nullableMapWriterFactory;

  private enum Mode { INIT, MAP, LIST };

  public ComplexWriterImpl(String name, MapVector container, boolean unionEnabled, boolean caseSensitive){
    this.name = name;
    this.container = container;
    this.unionEnabled = unionEnabled;
    nullableMapWriterFactory = caseSensitive? NullableMapWriterFactory.getNullableCaseSensitiveMapWriterFactoryInstance() :
        NullableMapWriterFactory.getNullableMapWriterFactoryInstance();
  }

  public ComplexWriterImpl(String name, MapVector container, boolean unionEnabled) {
    this(name, container, unionEnabled, false);
  }

  public ComplexWriterImpl(String name, MapVector container){
    this(name, container, false);
  }

  @Override
  public Field getField() {
    return container.getField();
  }

  @Override
  public int getValueCapacity() {
    return container.getValueCapacity();
  }

  private void check(Mode... modes){
    StateTool.check(mode, modes);
  }

  @Override
  public void reset(){
    setPosition(0);
  }

  @Override
  public void close() throws Exception {
    clear();
    mapRoot.close();
    if (listRoot != null) {
      listRoot.close();
    }
  }

  @Override
  public void clear(){
    switch(mode){
    case MAP:
      mapRoot.clear();
      break;
    case LIST:
      listRoot.clear();
      break;
    }
  }

  @Override
  public void setValueCount(int count){
    switch(mode){
    case MAP:
      mapRoot.setValueCount(count);
      break;
    case LIST:
      listRoot.setValueCount(count);
      break;
    }
  }

  @Override
  public void setPosition(int index){
    super.setPosition(index);
    switch(mode){
    case MAP:
      mapRoot.setPosition(index);
      break;
    case LIST:
      listRoot.setPosition(index);
      break;
    }
  }


  public MapWriter directMap(){
    Preconditions.checkArgument(name == null);

    switch(mode){

    case INIT:
      mapRoot = nullableMapWriterFactory.build((NullableMapVector) container);
      mapRoot.setPosition(idx());
      mode = Mode.MAP;
      break;

    case MAP:
      break;

    default:
        check(Mode.INIT, Mode.MAP);
    }

    return mapRoot;
  }

  @Override
  public MapWriter rootAsMap() {
    switch(mode){

    case INIT:
      // TODO allow dictionaries in complex types
      NullableMapVector map = container.addOrGet(name, MinorType.MAP, NullableMapVector.class, null);
      mapRoot = nullableMapWriterFactory.build(map);
      mapRoot.setPosition(idx());
      mode = Mode.MAP;
      break;

    case MAP:
      break;

    default:
        check(Mode.INIT, Mode.MAP);
    }

    return mapRoot;
  }

  @Override
  public void allocate() {
    if(mapRoot != null) {
      mapRoot.allocate();
    } else if(listRoot != null) {
      listRoot.allocate();
    }
  }

  @Override
  public ListWriter rootAsList() {
    switch(mode){

    case INIT:
      int vectorCount = container.size();
      // TODO allow dictionaries in complex types
      ListVector listVector = container.addOrGet(name, MinorType.LIST, ListVector.class, null);
      if (container.size() > vectorCount) {
        listVector.allocateNew();
      }
      listRoot = new UnionListWriter(listVector, nullableMapWriterFactory);
      listRoot.setPosition(idx());
      mode = Mode.LIST;
      break;

    case LIST:
      break;

    default:
        check(Mode.INIT, Mode.MAP);
    }

    return listRoot;
  }


}
