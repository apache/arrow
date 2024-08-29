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

package org.apache.arrow.vector.complex.impl;

import org.apache.arrow.vector.complex.writer.FieldWriter;


/**
 * Base class providing common functionality for {@link FieldWriter} implementations.
 *
 * <p>Currently this only includes index tracking.
 */
abstract class AbstractBaseWriter implements FieldWriter {
  //private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AbstractBaseWriter.class);

  private int index;

  @Override
  public String toString() {
    return super.toString() + "[index = " + index + "]";
  }

  int idx() {
    return index;
  }

  @Override
  public int getPosition() {
    return index;
  }

  @Override
  public void setPosition(int index) {
    this.index = index;
  }

  @Override
  public void end() {
  }
}
