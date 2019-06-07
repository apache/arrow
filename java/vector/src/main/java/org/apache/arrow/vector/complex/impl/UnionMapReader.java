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

import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.types.Types.MinorType;

/**
 * Reader for a MapVector.
 */
public class UnionMapReader extends UnionListReader {

  private String keyName = MapVector.KEY_NAME;
  private String valueName = MapVector.VALUE_NAME;

  /**
   * Construct a new reader for the given vector.
   *
   * @param vector Vector to read from.
   */
  public UnionMapReader(MapVector vector) {
    super(vector);
  }

  /**
   * Set the key, value field names to read.
   *
   * @param key Field name for key.
   * @param value Field name for value.
   */
  public void setKeyValueNames(String key, String value) {
    keyName = key;
    valueName = value;
  }

  /**
   * Start reading a key from the map entry.
   *
   * @return reader that can be used to read the key.
   */
  public FieldReader key() {
    return reader().reader(keyName);
  }

  /**
   * Start reading a value element from the map entry.
   *
   * @return reader that can be used to read the value.
   */
  public FieldReader value() {
    return reader().reader(valueName);
  }

  /**
   * Return the MinorType of the reader as MAP.
   */
  @Override
  public MinorType getMinorType() {
    return MinorType.MAP;
  }
}
