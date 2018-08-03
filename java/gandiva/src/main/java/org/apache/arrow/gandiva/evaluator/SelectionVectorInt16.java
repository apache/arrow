/*
 * Copyright (C) 2017-2018 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.arrow.gandiva.evaluator;

import io.netty.buffer.ArrowBuf;
import org.apache.arrow.gandiva.ipc.GandivaTypes.SelectionVectorType;

/*
 * Selection vector with records of arrow type INT16.
 */
public class SelectionVectorInt16 extends SelectionVector {

  public SelectionVectorInt16(ArrowBuf buffer) {
    super(buffer);
  }

  @Override
  public int getRecordSize() {
    return 2;
  }

  @Override
  public SelectionVectorType getType() {
    return SelectionVectorType.SV_INT16;
  }

  @Override
  public int getIndex(int index) {
    checkReadBounds(index);

    char value = getBuffer().getChar(index * getRecordSize());
    return (int)value;
  }
}
