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

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.netty.buffer.ArrowBuf;

public class TestArrowBufPointerNode {

  private BufferAllocator allocator;

  private ArrowBuf intVector;

  @Before
  public void prepare() {
    allocator = new RootAllocator(1024 * 1024);
    intVector = allocator.buffer(4 * 10);

    for (int i = 0; i < 10; i++) {
      intVector.setInt(i * 4, i);
    }
  }

  @After
  public void shutdown() {
    intVector.close();
    allocator.close();
  }

  private ArrowBufPointerNode createTree1() {
    // create a tree with structure:
    //            root
    //            /    \
    //     node    node
    //     /      \    /     \
    //    1      2  3     4

    ArrowBufPointerNode leaf1 = new ArrowBufPointerNode(new ArrowBufPointer(intVector, 1 * 4, 4));
    ArrowBufPointerNode leaf2 = new ArrowBufPointerNode(new ArrowBufPointer(intVector, 2 * 4, 4));
    ArrowBufPointerNode leaf3 = new ArrowBufPointerNode(new ArrowBufPointer(intVector, 3 * 4, 4));
    ArrowBufPointerNode leaf4 = new ArrowBufPointerNode(new ArrowBufPointer(intVector, 4 * 4, 4));

    ArrowBufPointerNode interNode1 = new ArrowBufPointerNode(new ArrowBufPointerNode[] {leaf1, leaf2});
    ArrowBufPointerNode interNode2 = new ArrowBufPointerNode(new ArrowBufPointerNode[] {leaf3, leaf4});

    return new ArrowBufPointerNode(new ArrowBufPointerNode[] {interNode1, interNode2});
  }

  private ArrowBufPointerNode createTree2() {
    // create a tree with structure:
    //            root
    //            /    \
    //     node    node
    //     /      \    /     \
    //    5      6  7     8

    ArrowBufPointerNode leaf5 = new ArrowBufPointerNode(new ArrowBufPointer(intVector, 5 * 4, 4));
    ArrowBufPointerNode leaf6 = new ArrowBufPointerNode(new ArrowBufPointer(intVector, 6 * 4, 4));
    ArrowBufPointerNode leaf7 = new ArrowBufPointerNode(new ArrowBufPointer(intVector, 7 * 4, 4));
    ArrowBufPointerNode leaf8 = new ArrowBufPointerNode(new ArrowBufPointer(intVector, 8 * 4, 4));

    ArrowBufPointerNode interNode1 = new ArrowBufPointerNode(new ArrowBufPointerNode[] {leaf5, leaf6});
    ArrowBufPointerNode interNode2 = new ArrowBufPointerNode(new ArrowBufPointerNode[] {leaf7, leaf8});

    return new ArrowBufPointerNode(new ArrowBufPointerNode[] {interNode1, interNode2});
  }

  private ArrowBufPointerNode createTree3() {
    // create a tree with structure:
    //            root
    //            /    \
    //     node    node
    //     /      \       |
    //    1      2     3

    ArrowBufPointerNode leaf1 = new ArrowBufPointerNode(new ArrowBufPointer(intVector, 1 * 4, 4));
    ArrowBufPointerNode leaf2 = new ArrowBufPointerNode(new ArrowBufPointer(intVector, 2 * 4, 4));
    ArrowBufPointerNode leaf3 = new ArrowBufPointerNode(new ArrowBufPointer(intVector, 3 * 4, 4));

    ArrowBufPointerNode interNode1 = new ArrowBufPointerNode(new ArrowBufPointerNode[] {leaf1, leaf2});
    ArrowBufPointerNode interNode2 = new ArrowBufPointerNode(new ArrowBufPointerNode[] {leaf3});

    return new ArrowBufPointerNode(new ArrowBufPointerNode[] {interNode1, interNode2});
  }

  @Test
  public void testTreeStructure() {
    ArrowBufPointerNode root = createTree1();

    assertFalse(root.isLeafNode());
    ArrowBufPointerNode[] children = root.getChildNodes();
    assertEquals(2, children.length);

    assertFalse(children[0].isLeafNode());
    assertFalse(children[1].isLeafNode());

    ArrowBufPointerNode[] grandChildren0 = children[0].getChildNodes();
    assertEquals(2, grandChildren0.length);

    assertTrue(grandChildren0[0].isLeafNode());
    assertTrue(grandChildren0[1].isLeafNode());

    assertEquals(1, grandChildren0[0].getArrowBufPointer().getBuf().getInt(
            grandChildren0[0].getArrowBufPointer().getOffset()));
    assertEquals(2, grandChildren0[1].getArrowBufPointer().getBuf().getInt(
            grandChildren0[1].getArrowBufPointer().getOffset()));

    ArrowBufPointerNode[] grandChildren1 = children[1].getChildNodes();
    assertEquals(2, grandChildren1.length);

    assertTrue(grandChildren1[0].isLeafNode());
    assertTrue(grandChildren1[1].isLeafNode());

    assertEquals(3, grandChildren1[0].getArrowBufPointer().getBuf().getInt(
            grandChildren1[0].getArrowBufPointer().getOffset()));
    assertEquals(4, grandChildren1[1].getArrowBufPointer().getBuf().getInt(
            grandChildren1[1].getArrowBufPointer().getOffset()));
  }

  @Test
  public void testNodeComparison() {
    ArrowBufPointerNode tree1 = createTree1();
    ArrowBufPointerNode tree2 = createTree2();
    ArrowBufPointerNode tree3 = createTree3();

    assertTrue(tree1.compareTo(tree2) < 0);
    assertTrue(tree2.compareTo(tree1) > 0);

    assertTrue(tree1.compareTo(tree3) > 0);
    assertTrue(tree3.compareTo(tree1) < 0);

    assertTrue(tree2.compareTo(tree3) > 0);
    assertTrue(tree3.compareTo(tree2) < 0);

    ArrowBufPointerNode tree1Copy = createTree1();
    assertTrue(tree1.compareTo(tree1Copy) == 0);
    assertTrue(tree1Copy.compareTo(tree1) == 0);
  }

  @Test
  public void testNodeEquals() {
    ArrowBufPointerNode tree1 = createTree1();
    ArrowBufPointerNode tree2 = createTree2();
    ArrowBufPointerNode tree3 = createTree3();

    assertFalse(tree1.equals(tree2));
    assertFalse(tree2.equals(tree1));

    assertFalse(tree1.equals(tree3));
    assertFalse(tree3.equals(tree1));

    assertFalse(tree2.equals(tree3));
    assertFalse(tree3.equals(tree2));

    ArrowBufPointerNode tree1Copy = createTree1();
    assertTrue(tree1.equals(tree1));
    assertTrue(tree1.equals(tree1Copy));
  }

  @Test
  public void testNodeIterator() {
    ArrowBufPointerNode tree1 = createTree1();
    Iterator<ArrowBufPointer> it = tree1.iterator();

    List<ArrowBufPointer> pointers = new ArrayList<>();
    it.forEachRemaining(pointers::add);

    assertEquals(4, pointers.size());

    ArrowBufPointer pointer1 = pointers.get(0);
    assertNotNull(pointer1.getBuf());
    assertEquals(1, pointer1.getBuf().getInt(pointer1.getOffset()));

    ArrowBufPointer pointer2 = pointers.get(1);
    assertNotNull(pointer2.getBuf());
    assertEquals(2, pointer2.getBuf().getInt(pointer2.getOffset()));

    ArrowBufPointer pointer3 = pointers.get(2);
    assertNotNull(pointer3.getBuf());
    assertEquals(3, pointer3.getBuf().getInt(pointer3.getOffset()));

    ArrowBufPointer pointer4 = pointers.get(3);
    assertNotNull(pointer4.getBuf());
    assertEquals(4, pointer4.getBuf().getInt(pointer4.getOffset()));
  }
}
