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

import java.lang.Override;
import java.util.List;

import org.apache.arrow.record.TransferPair;
import org.apache.arrow.vector.complex.IndexHolder;
import org.apache.arrow.vector.complex.writer.IntervalWriter;
import org.apache.arrow.vector.complex.writer.BaseWriter.MapWriter;

<@pp.dropOutputFile />
<#list vv.types as type>
<#list type.minor as minor>
<#list ["", "Repeated"] as mode>
<#assign lowerName = minor.class?uncap_first />
<#if lowerName == "int" ><#assign lowerName = "integer" /></#if>
<#assign name = mode + minor.class?cap_first />
<#assign javaType = (minor.javaType!type.javaType) />
<#assign friendlyType = (minor.friendlyType!minor.boxedType!type.boxedType) />
<#assign safeType=friendlyType />
<#if safeType=="byte[]"><#assign safeType="ByteArray" /></#if>

<#assign hasFriendly = minor.friendlyType!"no" == "no" />

<#list ["", "Nullable"] as nullMode>
<#if (mode == "Repeated" && nullMode  == "") || mode == "" >
<@pp.changeOutputFile name="/org/apache/arrow/vector/complex/impl/${nullMode}${name}ReaderImpl.java" />
<#include "/@includes/license.ftl" />

package org.apache.arrow.vector.complex.impl;

<#include "/@includes/vv_imports.ftl" />

@SuppressWarnings("unused")
public class ${nullMode}${name}ReaderImpl extends AbstractFieldReader {
  
  private final ${nullMode}${name}Vector vector;
  
  public ${nullMode}${name}ReaderImpl(${nullMode}${name}Vector vector){
    super();
    this.vector = vector;
  }

  public MajorType getType(){
    return vector.getField().getType();
  }

  public MaterializedField getField(){
    return vector.getField();
  }
  
  public boolean isSet(){
    <#if nullMode == "Nullable">
    return !vector.getAccessor().isNull(idx());
    <#else>
    return true;
    </#if>
  }


  
  
  <#if mode == "Repeated">

  public void copyAsValue(${minor.class?cap_first}Writer writer){
    Repeated${minor.class?cap_first}WriterImpl impl = (Repeated${minor.class?cap_first}WriterImpl) writer;
    impl.vector.copyFromSafe(idx(), impl.idx(), vector);
  }
  
  public void copyAsField(String name, MapWriter writer){
    Repeated${minor.class?cap_first}WriterImpl impl = (Repeated${minor.class?cap_first}WriterImpl)  writer.list(name).${lowerName}();
    impl.vector.copyFromSafe(idx(), impl.idx(), vector);
  }
  
  public int size(){
    return vector.getAccessor().getInnerValueCountAt(idx());
  }
  
  public void read(int arrayIndex, ${minor.class?cap_first}Holder h){
    vector.getAccessor().get(idx(), arrayIndex, h);
  }
  public void read(int arrayIndex, Nullable${minor.class?cap_first}Holder h){
    vector.getAccessor().get(idx(), arrayIndex, h);
  }
  
  public ${friendlyType} read${safeType}(int arrayIndex){
    return vector.getAccessor().getSingleObject(idx(), arrayIndex);
  }

  
  public List<Object> readObject(){
    return (List<Object>) (Object) vector.getAccessor().getObject(idx());
  }
  
  <#else>
  
  public void copyAsValue(${minor.class?cap_first}Writer writer){
    ${nullMode}${minor.class?cap_first}WriterImpl impl = (${nullMode}${minor.class?cap_first}WriterImpl) writer;
    impl.vector.copyFromSafe(idx(), impl.idx(), vector);
  }
  
  public void copyAsField(String name, MapWriter writer){
    ${nullMode}${minor.class?cap_first}WriterImpl impl = (${nullMode}${minor.class?cap_first}WriterImpl) writer.${lowerName}(name);
    impl.vector.copyFromSafe(idx(), impl.idx(), vector);
  }

  <#if nullMode != "Nullable">
  public void read(${minor.class?cap_first}Holder h){
    vector.getAccessor().get(idx(), h);
  }
  </#if>

  public void read(Nullable${minor.class?cap_first}Holder h){
    vector.getAccessor().get(idx(), h);
  }
  
  public ${friendlyType} read${safeType}(){
    return vector.getAccessor().getObject(idx());
  }
  
  public void copyValue(FieldWriter w){
    
  }
  
  public Object readObject(){
    return vector.getAccessor().getObject(idx());
  }

  
  </#if>
}
</#if>
</#list>
<@pp.changeOutputFile name="/org/apache/arrow/vector/complex/reader/${name}Reader.java" />
<#include "/@includes/license.ftl" />

package org.apache.arrow.vector.complex.reader;

<#include "/@includes/vv_imports.ftl" />
@SuppressWarnings("unused")
public interface ${name}Reader extends BaseReader{
  
  <#if mode == "Repeated">
  public int size();
  public void read(int arrayIndex, ${minor.class?cap_first}Holder h);
  public void read(int arrayIndex, Nullable${minor.class?cap_first}Holder h);
  public Object readObject(int arrayIndex);
  public ${friendlyType} read${safeType}(int arrayIndex);
  <#else>
  public void read(${minor.class?cap_first}Holder h);
  public void read(Nullable${minor.class?cap_first}Holder h);
  public Object readObject();
  public ${friendlyType} read${safeType}();
  </#if>  
  public boolean isSet();
  public void copyAsValue(${minor.class}Writer writer);
  public void copyAsField(String name, ${minor.class}Writer writer);
  
}



</#list>
</#list>
</#list>


