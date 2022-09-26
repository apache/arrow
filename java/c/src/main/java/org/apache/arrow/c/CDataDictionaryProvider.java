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

package org.apache.arrow.c;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryProvider;

/**
 * A DictionaryProvider that is used in C Data Interface for imports.
 * <p>
 * CDataDictionaryProvider is similar to
 * {@link DictionaryProvider.MapDictionaryProvider} with a key difference that
 * the dictionaries are owned by the provider so it must eventually be closed.
 * <p>
 * The typical usage is to create the CDataDictionaryProvider and pass it to
 * {@link Data#importField} or {@link Data#importSchema} to allocate empty
 * dictionaries based on the information in {@link ArrowSchema}. Then you can
 * re-use the same dictionary provider in any function that imports an
 * {@link ArrowArray} that has the same schema.
 */
public class CDataDictionaryProvider implements DictionaryProvider, AutoCloseable {

  private final Map<Long, Dictionary> map;

  public CDataDictionaryProvider() {
    this.map = new HashMap<>();
  }

  void put(Dictionary dictionary) {
    Dictionary previous = map.put(dictionary.getEncoding().getId(), dictionary);
    if (previous != null) {
      previous.getVector().close();
    }
  }

  @Override
  public final Set<Long> getDictionaryIds() {
    return map.keySet();
  }

  @Override
  public Dictionary lookup(long id) {
    return map.get(id);
  }

  @Override
  public void close() {
    for (Dictionary dictionary : map.values()) {
      dictionary.getVector().close();
    }
    map.clear();
  }

}
