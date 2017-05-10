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

import org.apache.arrow.vector.types.pojo.ArrowType.Null;
import org.apache.arrow.vector.types.pojo.Field;

<@pp.dropOutputFile />
<@pp.changeOutputFile name="/org/apache/arrow/vector/complex/impl/NullReader.java" />


<#include "/@includes/license.ftl" />

package org.apache.arrow.vector.complex.impl;

<#include "/@includes/vv_imports.ftl" />

/**
 * Source code generated using FreeMarker template ${.template_name}
 */
@SuppressWarnings("unused")
public class NullReader extends AbstractBaseReader implements FieldReader{
  
  public static final NullReader INSTANCE = new NullReader();
  public static final NullReader EMPTY_LIST_INSTANCE = new NullReader(MinorType.NULL);
  public static final NullReader EMPTY_MAP_INSTANCE = new NullReader(MinorType.MAP);
  private MinorType type;
  
  private NullReader(){
    super();
    type = MinorType.NULL;
  }

  private NullReader(MinorType type){
    super();
    this.type = type;
  }

  @Override
  public MinorType getMinorType() {
    return type;
  }

  @Override
  public Field getField() {
    return new Field("", FieldType.nullable(new Null()), null);
  }

  public void copyAsValue(MapWriter writer) {}

  public void copyAsValue(ListWriter writer) {}

  public void copyAsValue(UnionWriter writer) {}

  <#list vv.types as type><#list type.minor as minor><#assign name = minor.class?cap_first />
  public void read(${name}Holder holder){
    throw new UnsupportedOperationException("NullReader cannot write into non-nullable holder");
  }

  public void read(Nullable${name}Holder holder){
    holder.isSet = 0;
  }

  public void read(int arrayIndex, ${name}Holder holder){
    throw new ArrayIndexOutOfBoundsException();
  }
  
  public void copyAsValue(${minor.class}Writer writer){}
  public void copyAsField(String name, ${minor.class}Writer writer){}

  public void read(int arrayIndex, Nullable${name}Holder holder){
    throw new ArrayIndexOutOfBoundsException();
  }
  </#list></#list>
  
  public int size(){
    return 0;
  }
  
  public boolean isSet(){
    return false;
  }
  
  public boolean next(){
    return false;
  }
  
  public RepeatedMapReader map(){
    return this;
  }
  
  public RepeatedListReader list(){
    return this;
  }
  
  public MapReader map(String name){
    return this;
  }
  
  public ListReader list(String name){
    return this;
  }
  
  public FieldReader reader(String name){
    return this;
  }
  
  public FieldReader reader(){
    return this;
  }
  
  private void fail(String name){
    throw new IllegalArgumentException(String.format("You tried to read a %s type when you are using a ValueReader of type %s.", name, this.getClass().getSimpleName()));
  }
  
  <#list ["Object", "BigDecimal", "Integer", "Long", "Boolean", 
          "Character", "LocalDateTime", "Period", "Double", "Float",
          "Text", "String", "Byte", "Short", "byte[]"] as friendlyType>
  <#assign safeType=friendlyType />
  <#if safeType=="byte[]"><#assign safeType="ByteArray" /></#if>
  
  public ${friendlyType} read${safeType}(int arrayIndex){
    return null;
  }
  
  public ${friendlyType} read${safeType}(){
    return null;
  }
  </#list>
  
}



