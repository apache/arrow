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

import org.apache.arrow.vector.complex.ListViewVector;
import org.apache.arrow.vector.complex.impl.UnionListWriter;

<@pp.dropOutputFile />
<@pp.changeOutputFile name="/org/apache/arrow/vector/complex/impl/UnionListViewWriter.java" />


<#include "/@includes/license.ftl" />

package org.apache.arrow.vector.complex.impl;

<#include "/@includes/vv_imports.ftl" />

/*
 * This class is generated using freemarker and the ${.template_name} template.
 */

/**
 * <p>Writer for ListViewVector. This extends UnionListWriter to simplify writing listview entries to a list
 * </p>
 */
@SuppressWarnings("unused")
public class UnionListViewWriter extends UnionListWriter {
  public UnionListViewWriter(ListViewVector vector) {
    super(vector);
  }

  public void startList(int offset) {
    ((ListViewVector) vector).startNewValue(idx(), offset);
    writer.setPosition(((ListViewVector) vector).getOffsetBuffer().getInt((idx()) * 4));
  }

  public void endList(int size) {
    ((ListViewVector) vector).endValue(idx(), size);
    setPosition(idx() + 1);
  }

  public void setValueCount(int count) {
    ((ListViewVector) vector).setValueCount(count);
  }
}
