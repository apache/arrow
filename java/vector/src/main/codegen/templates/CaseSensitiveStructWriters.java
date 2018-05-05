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
<#list ["Nullable", "Single"] as mode>
<@pp.changeOutputFile name="/org/apache/arrow/vector/complex/impl/${mode}CaseSensitiveStructWriter.java" />
<#assign index = "idx()">
<#if mode == "Single">
<#assign containerClass = "NonNullableStructVector" />
<#else>
<#assign containerClass = "StructVector" />
</#if>

<#include "/@includes/license.ftl" />

package org.apache.arrow.vector.complex.impl;

<#include "/@includes/vv_imports.ftl" />
/*
 * This class is generated using FreeMarker and the ${.template_name} template.
 */
@SuppressWarnings("unused")
public class ${mode}CaseSensitiveStructWriter extends ${mode}StructWriter {
  public ${mode}CaseSensitiveStructWriter(${containerClass} container) {
    super(container);
  }

  @Override
  protected String handleCase(final String input){
    return input;
  }

  @Override
  protected NullableStructWriterFactory getNullableStructWriterFactory() {
    return NullableStructWriterFactory.getNullableCaseSensitiveStructWriterFactoryInstance();
  }

}
</#list>
