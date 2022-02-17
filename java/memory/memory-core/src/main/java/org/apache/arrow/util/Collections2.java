/**
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

package org.apache.arrow.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Utility methods for manipulating {@link java.util.Collections} and their subclasses/implementations.
 */
public final class Collections2 {
  private Collections2() {}

  /**
   * Creates a {@link List} from the elements remaining in iterator.
   */
  public static <T> List<T> toList(Iterator<T> iterator) {
    List<T> target = new ArrayList<>();
    iterator.forEachRemaining(target::add);
    return target;
  }

  /**
   * Converts the iterable into a new {@link List}.
   */
  public static <T> List<T> toList(Iterable<T> iterable) {
    if (iterable instanceof Collection<?>) {
      // If iterable is a collection, take advantage of it for a more efficient copy
      return new ArrayList<T>((Collection<T>) iterable);
    }
    return toList(iterable.iterator());
  }

  /**
   * Converts the iterable into a new immutable {@link List}.
   */
  public static <T> List<T> toImmutableList(Iterable<T> iterable) {
    return Collections.unmodifiableList(toList(iterable));
  }


  /** Copies the elements of <code>map</code> to a new unmodifiable map. */
  public static <K, V> Map<K, V> immutableMapCopy(Map<K, V> map) {
    return Collections.unmodifiableMap(new HashMap<>(map));
  }

  /** Copies the elements of list to a new unmodifiable list. */
  public static <V> List<V> immutableListCopy(List<V> list) {
    return Collections.unmodifiableList(new ArrayList<>(list));
  }

  /** Copies the values to a new unmodifiable list. */
  public static <V> List<V> asImmutableList(V...values) {
    return Collections.unmodifiableList(Arrays.asList(values));
  }

  /**
   * Creates a human readable string from the remaining elements in iterator.
   *
   * The output should be similar to {@code Arrays#toString(Object[])}
   */
  public static String toString(Iterator<?> iterator) {
    return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED), false)
        .map(String::valueOf)
        .collect(Collectors.joining(", ", "[", "]"));
  }
}
