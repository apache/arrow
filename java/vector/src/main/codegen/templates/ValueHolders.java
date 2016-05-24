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
<@pp.dropOutputFile />
<#list vv.modes as mode>
<#list vv.types as type>
<#list type.minor as minor>

<#assign className="${mode.prefix}${minor.class}Holder" />
<@pp.changeOutputFile name="/org/apache/arrow/vector/holders/${className}.java" />

<#include "/@includes/license.ftl" />

package org.apache.arrow.vector.holders;

<#include "/@includes/vv_imports.ftl" />

public final class ${className} implements ValueHolder{
  
    <#if mode.name == "Repeated">
    
    /** The first index (inclusive) into the Vector. **/
    public int start;
    
    /** The last index (exclusive) into the Vector. **/
    public int end;
    
    /** The Vector holding the actual values. **/
    public ${minor.class}Vector vector;
    
    <#else>
    public static final int WIDTH = ${type.width};
    
    <#if mode.name == "Optional">public int isSet;
    <#else>public final int isSet = 1;</#if>
    <#assign fields = minor.fields!type.fields />
    <#list fields as field>
    public ${field.type} ${field.name};
    </#list>
    
    @Deprecated
    public int hashCode(){
      throw new UnsupportedOperationException();
    }

    /*
     * Reason for deprecation is that ValueHolders are potential scalar replacements
     * and hence we don't want any methods to be invoked on them.
     */
    @Deprecated
    public String toString(){
      throw new UnsupportedOperationException();
    }
    </#if>
    
    
    
    
}

</#list>
</#list>
</#list>