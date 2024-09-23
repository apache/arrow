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
package org.apache.arrow.algorithm.dictionary;

import java.util.TreeSet;
import org.apache.arrow.algorithm.sort.VectorValueComparator;
import org.apache.arrow.vector.ValueVector;

/**
 * This class builds the dictionary based on a binary search tree. Each add operation can be
 * finished in O(log(n)) time, where n is the current dictionary size.
 *
 * @param <V> the dictionary vector type.
 */
public class SearchTreeBasedDictionaryBuilder<V extends ValueVector>
    implements DictionaryBuilder<V> {

  /** The dictionary to be built. */
  private final V dictionary;

  /** The criteria for sorting in the search tree. */
  protected final VectorValueComparator<V> comparator;

  /** If null should be encoded. */
  private final boolean encodeNull;

  /** The search tree for storing the value index. */
  private TreeSet<Integer> searchTree;

  /**
   * Construct a search tree-based dictionary builder.
   *
   * @param dictionary the dictionary vector.
   * @param comparator the criteria for value equality.
   */
  public SearchTreeBasedDictionaryBuilder(V dictionary, VectorValueComparator<V> comparator) {
    this(dictionary, comparator, false);
  }

  /**
   * Construct a search tree-based dictionary builder.
   *
   * @param dictionary the dictionary vector.
   * @param comparator the criteria for value equality.
   * @param encodeNull if null values should be added to the dictionary.
   */
  public SearchTreeBasedDictionaryBuilder(
      V dictionary, VectorValueComparator<V> comparator, boolean encodeNull) {
    this.dictionary = dictionary;
    this.comparator = comparator;
    this.encodeNull = encodeNull;
    this.comparator.attachVector(dictionary);

    searchTree = new TreeSet<>((index1, index2) -> comparator.compare(index1, index2));
  }

  /**
   * Gets the dictionary built. Please note that the dictionary is not in sorted order. Instead, its
   * order is determined by the order of element insertion. To get the dictionary in sorted order,
   * please use {@link SearchTreeBasedDictionaryBuilder#populateSortedDictionary(ValueVector)}.
   *
   * @return the dictionary.
   */
  @Override
  public V getDictionary() {
    return dictionary;
  }

  /**
   * Try to add all values from the target vector to the dictionary.
   *
   * @param targetVector the target vector containing values to probe.
   * @return the number of values actually added to the dictionary.
   */
  @Override
  public int addValues(V targetVector) {
    int oldDictSize = dictionary.getValueCount();
    for (int i = 0; i < targetVector.getValueCount(); i++) {
      if (!encodeNull && targetVector.isNull(i)) {
        continue;
      }
      addValue(targetVector, i);
    }
    return dictionary.getValueCount() - oldDictSize;
  }

  /**
   * Try to add an element from the target vector to the dictionary.
   *
   * @param targetVector the target vector containing new element.
   * @param targetIndex the index of the new element in the target vector.
   * @return the index of the new element in the dictionary.
   */
  @Override
  public int addValue(V targetVector, int targetIndex) {
    // first copy the value to the end of the dictionary
    int dictSize = dictionary.getValueCount();
    dictionary.copyFromSafe(targetIndex, dictSize, targetVector);

    // try to add the value to the dictionary,
    // if an equal element does not exist.
    // this operation can be done in O(log(n)) time.
    if (searchTree.add(dictSize)) {
      // the element is successfully added
      dictionary.setValueCount(dictSize + 1);
      return dictSize;
    } else {
      // the element is already in the dictionary
      // find its index in O(log(n)) time.
      return searchTree.ceiling(dictSize);
    }
  }

  /**
   * Gets the sorted dictionary. Note that given the binary search tree, the sort can finish in
   * O(n).
   */
  public void populateSortedDictionary(V sortedDictionary) {
    int idx = 0;
    for (Integer dictIdx : searchTree) {
      sortedDictionary.copyFromSafe(dictIdx, idx++, dictionary);
    }

    sortedDictionary.setValueCount(dictionary.getValueCount());
  }
}
