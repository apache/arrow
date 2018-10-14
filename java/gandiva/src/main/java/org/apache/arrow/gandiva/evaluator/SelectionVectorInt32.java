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

package org.apache.arrow.gandiva.evaluator;

import org.apache.arrow.gandiva.ipc.GandivaTypes.SelectionVectorType;

import io.netty.buffer.ArrowBuf;

/*
 * Selection vector with records of arrow type INT32.
 */
public class SelectionVectorInt32 extends SelectionVector {

  public SelectionVectorInt32(ArrowBuf buffer) {
    super(buffer);
  }

  @Override
  public int getRecordSize() {
    return 4;
  }

  @Override
  public SelectionVectorType getType() {
    return SelectionVectorType.SV_INT32;
  }

  @Override
  public int getIndex(int index) {
    checkReadBounds(index);

    return getBuffer().getInt(index * getRecordSize());
  }
}
