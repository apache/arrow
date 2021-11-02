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

package org.apache.arrow.util;

import static org.junit.Assert.fail;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.junit.Test;

/**
 * Tests for {@code Collections2} class.
 */
public class TestCollections2 {


  @Test
  public void testToImmutableListFromIterable() {
    final List<String> source = new ArrayList<>(Arrays.asList("foo", "bar", "baz"));

    final List<String> copy = Collections2.toImmutableList(source);
    assertEquals(source, copy);

    try {
      copy.add("unexpected");
      fail("add operation should not be supported");
    } catch (UnsupportedOperationException ignored) {
    }

    try {
      copy.set(0, "unexpected");
      fail("set operation should not be supported");
    } catch (UnsupportedOperationException ignored) {
    }

    try {
      copy.remove(0);
      fail("remove operation should not be supported");
    } catch (UnsupportedOperationException ignored) {
    }

    source.set(1, "newvalue");
    source.add("anothervalue");

    assertEquals("bar", copy.get(1));
    assertEquals(3, copy.size());
  }


  @Test
  public void testStringFromEmptyIterator() {
    assertEquals("[]", Collections2.toString(Collections.emptyIterator()));
  }

  @Test
  public void testStringFromIterator() {
    Iterator<String> iterator = Arrays.asList("foo", "bar", "baz").iterator();
    iterator.next();

    assertEquals("[bar, baz]", Collections2.toString(iterator));
    assertEquals(false, iterator.hasNext());
  }
}
