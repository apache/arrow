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
import org.apache.arrow.vector.complex.writer.BaseWriter.StructWriter;

<@pp.dropOutputFile />
<#list vv.types as type>
<#list type.minor as minor>
<#list [""] as mode>
<#assign lowerName = minor.class?uncap_first />
<#if lowerName == "int" ><#assign lowerName = "integer" /></#if>
<#assign name = minor.class?cap_first />
<#assign javaType = (minor.javaType!type.javaType) />
<#assign friendlyType = (minor.friendlyType!minor.boxedType!type.boxedType) />
<#assign safeType=friendlyType />
<#if safeType=="byte[]"><#assign safeType="ByteArray" /></#if>

<#assign hasFriendly = minor.friendlyType!"no" == "no" />

<#list ["Nullable"] as nullMode>
<#if mode == "" >
<@pp.changeOutputFile name="/org/apache/arrow/vector/complex/impl/${name}ReaderImpl.java" />
<#include "/@includes/license.ftl" />

package org.apache.arrow.vector.complex.impl;

<#include "/@includes/vv_imports.ftl" />

/**
 * Source code generated using FreeMarker template ${.template_name}
 */
@SuppressWarnings("unused")
public class ${name}ReaderImpl extends AbstractFieldReader {
  
  private final ${name}Vector vector;
  
  public ${name}ReaderImpl(${name}Vector vector){
    super();
    this.vector = vector;
  }

  public MinorType getMinorType(){
    return vector.getMinorType();
  }

  public Field getField(){
    return vector.getField();
  }
  
  public boolean isSet(){
    return !vector.isNull(idx());
  }

  public void copyAsValue(${minor.class?cap_first}Writer writer){
    ${minor.class?cap_first}WriterImpl impl = (${minor.class?cap_first}WriterImpl) writer;
    impl.vector.copyFromSafe(idx(), impl.idx(), vector);
  }
  
  public void copyAsField(String name, StructWriter writer){
    ${minor.class?cap_first}WriterImpl impl = (${minor.class?cap_first}WriterImpl) writer.${lowerName}(name);
    impl.vector.copyFromSafe(idx(), impl.idx(), vector);
  }

  <#if nullMode != "Nullable">
  public void read(${minor.class?cap_first}Holder h){
    vector.get(idx(), h);
  }
  </#if>

  public void read(Nullable${minor.class?cap_first}Holder h){
    vector.get(idx(), h);
  }
  
  public ${friendlyType} read${safeType}(){
    return vector.getObject(idx());
  }

  <#if minor.class == "TimeStampSec" ||
       minor.class == "TimeStampMilli" ||
       minor.class == "TimeStampMicro" ||
       minor.class == "TimeStampNano">
  @Override
  public ${minor.boxedType} read${minor.boxedType}(){
    return vector.get(idx());
  }
  </#if>
  
  public void copyValue(FieldWriter w){
    
  }
  
  public Object readObject(){
    return (Object)vector.getObject(idx());
  }
}
</#if>
</#list>
<@pp.changeOutputFile name="/org/apache/arrow/vector/complex/reader/${name}Reader.java" />
<#include "/@includes/license.ftl" />

package org.apache.arrow.vector.complex.reader;

<#include "/@includes/vv_imports.ftl" />
/**
 * Source code generated using FreeMarker template ${.template_name}
 */
@SuppressWarnings("unused")
public interface ${name}Reader extends BaseReader{
  
  public void read(${minor.class?cap_first}Holder h);
  public void read(Nullable${minor.class?cap_first}Holder h);
  public Object readObject();
  // read friendly type
  public ${friendlyType} read${safeType}();
  public boolean isSet();
  public void copyAsValue(${minor.class}Writer writer);
  public void copyAsField(String name, ${minor.class}Writer writer);
  
}



</#list>
</#list>
</#list>


