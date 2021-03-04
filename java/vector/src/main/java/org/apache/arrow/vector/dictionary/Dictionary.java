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

package org.apache.arrow.vector.dictionary;

import java.util.Objects;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.compare.VectorEqualsVisitor;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;

/**
 * A dictionary (integer to Value mapping) that is used to facilitate
 * dictionary encoding compression.
 */
public class Dictionary {

  private final DictionaryEncoding encoding;
  private final FieldVector dictionary;

  public Dictionary(FieldVector dictionary, DictionaryEncoding encoding) {
    this.dictionary = dictionary;
    this.encoding = encoding;
  }

  public FieldVector getVector() {
    return dictionary;
  }

  public DictionaryEncoding getEncoding() {
    return encoding;
  }

  public ArrowType getVectorType() {
    return dictionary.getField().getType();
  }

  @Override
  public String toString() {
    return "Dictionary " + encoding + " " + dictionary;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Dictionary that = (Dictionary) o;
    return Objects.equals(encoding, that.encoding) &&
        new VectorEqualsVisitor().vectorEquals(that.dictionary, dictionary);
  }

  @Override
  public int hashCode() {
    return Objects.hash(encoding, dictionary);
  }
}
