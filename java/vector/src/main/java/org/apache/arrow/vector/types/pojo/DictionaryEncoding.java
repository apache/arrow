/*******************************************************************************

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
 ******************************************************************************/

package org.apache.arrow.vector.types.pojo;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.arrow.vector.types.pojo.ArrowType.Int;

public class DictionaryEncoding {

  private final long id;
  private final boolean ordered;
  private final Int indexType;

  @JsonCreator
  public DictionaryEncoding(
      @JsonProperty("id") long id,
      @JsonProperty("isOrdered") boolean ordered,
      @JsonProperty("indexType") Int indexType) {
    this.id = id;
    this.ordered = ordered;
    this.indexType = indexType == null ? new Int(32, true) : indexType;
  }

  public long getId() {
    return id;
  }

  @JsonGetter("isOrdered")
  public boolean isOrdered() {
    return ordered;
  }

  public Int getIndexType() {
    return indexType;
  }

  @Override
  public String toString() {
    return "DictionaryEncoding[id=" + id + ",ordered=" + ordered + ",indexType=" + indexType + "]";
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    } else if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DictionaryEncoding that = (DictionaryEncoding) o;
    return id == that.id && ordered == that.ordered && Objects.equals(indexType, that.indexType);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, ordered, indexType);
  }
}
