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

package org.apache.arrow.memory.util;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;

/**
 * A node in the tree which has ArrowBufPointers as leaf nodes.
 */
public final class ArrowBufPointerNode implements Comparable<ArrowBufPointerNode>, Iterable<ArrowBufPointer> {

  /**
   * A flag indicating if this is a leaf node.
   */
  private final boolean leafNode;

  /**
   * Reference to an ArrowBufPointer.
   * It is valid only if it is a leaf node.
   */
  private final ArrowBufPointer pointer;

  /**
   * Reference to child nodes.
   * It is valid only if it is a non-leaf node.
   */
  private final ArrowBufPointerNode[] children;

  /**
   * Constructor for leaf nodes.
   * @param pointer pointer to an ArrowBufPointer.
   */
  public ArrowBufPointerNode(ArrowBufPointer pointer) {
    this.leafNode = true;
    this.pointer = pointer;
    this.children = null;
  }

  /**
   * Constructor for non-leaf nodes.
   * @param children child nodes.
   */
  public ArrowBufPointerNode(ArrowBufPointerNode[] children) {
    this.leafNode = false;
    this.children = children;
    this.pointer = null;
  }

  @Override
  public int compareTo(ArrowBufPointerNode o) {
    Iterator<ArrowBufPointer> iter1 = this.iterator();
    Iterator<ArrowBufPointer> iter2 = o.iterator();

    while (iter1.hasNext() && iter2.hasNext()) {
      ArrowBufPointer pointer1 = iter1.next();
      ArrowBufPointer pointer2 = iter2.next();

      int result = pointer1.compareTo(pointer2);
      if (result != 0) {
        return result;
      }
    }

    if (iter1.hasNext() || iter2.hasNext()) {
      // the two iterators have different lengths.
      if (iter1.hasNext()) {
        return 1;
      } else {
        return -1;
      }
    }
    return 0;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof ArrowBufPointerNode)) {
      return false;
    }

    ArrowBufPointerNode other = (ArrowBufPointerNode) o;
    if (this.leafNode != other.leafNode) {
      return false;
    }

    if (this.leafNode) {
      return this.pointer.equals(other.pointer);
    } else {
      if (this.children.length != other.children.length) {
        return false;
      }

      for (int i = 0; i < this.children.length; i++) {
        if (!this.children[i].equals(other.children[i])) {
          return false;
        }
      }
      return true;
    }
  }

  @Override
  public int hashCode() {
    if (leafNode) {
      return pointer.hashCode();
    } else {
      int ret = 0;
      for (int i = 0; i < children.length; i++) {
        ret = ByteFunctionHelpers.combineHash(ret, children[i].hashCode());
      }
      return ret;
    }
  }

  /**
   * Checks if the node is a leaf node.
   */
  public boolean isLeafNode() {
    return leafNode;
  }

  /**
   * Gets the underlying pointer for the node.
   * Valid only if it is a leaf node.
   */
  public ArrowBufPointer getArrowBufPointer() {
    if (!leafNode) {
      throw new UnsupportedOperationException("This is supported only for leaf nodes.");
    }
    return pointer;
  }

  /**
   * Gets the child nodes.
   * Valid only if it is a non-leaf node.
   */
  public ArrowBufPointerNode[] getChildNodes() {
    if (leafNode) {
      throw new UnsupportedOperationException("This is supported only for non-leaf nodes.");
    }
    return children;
  }

  @Override
  public Iterator<ArrowBufPointer> iterator() {
    return new ArrowBufPointerIterator();
  }

  private class ArrowBufPointerIterator implements Iterator<ArrowBufPointer> {

    private ArrowBufPointer nextToReturn = null;

    private Queue<ArrowBufPointerNode> nodesToCheck = new LinkedList<>();

    ArrowBufPointerIterator() {
      nodesToCheck.add(ArrowBufPointerNode.this);
      findNext();
    }

    @Override
    public boolean hasNext() {
      return nextToReturn != null;
    }

    @Override
    public ArrowBufPointer next() {
      ArrowBufPointer ret = nextToReturn;

      nextToReturn = null;
      findNext();

      return ret;
    }

    private void findNext() {
      // traverse the tree in breadth-first order
      while (!nodesToCheck.isEmpty()) {
        ArrowBufPointerNode node = nodesToCheck.poll();
        if (node.leafNode) {
          nextToReturn = node.pointer;
          return;
        } else {
          for (ArrowBufPointerNode child : node.children) {
            nodesToCheck.add(child);
          }
        }
      }
    }
  }
}
