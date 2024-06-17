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
package org.apache.arrow.memory.util.hash;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.charset.StandardCharsets;
import java.util.stream.Stream;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/** Test cases for {@link ArrowBufHasher} and its subclasses. */
public class TestArrowBufHasher {

  private final int BUFFER_LENGTH = 1024;

  private BufferAllocator allocator;

  @BeforeEach
  public void prepare() {
    allocator = new RootAllocator(1024 * 1024);
  }

  @AfterEach
  public void shutdown() {
    allocator.close();
  }

  @ParameterizedTest(name = "hasher = {0}")
  @MethodSource("getHasher")
  void testHasher(String name, ArrowBufHasher hasher) {
    try (ArrowBuf buf1 = allocator.buffer(BUFFER_LENGTH);
        ArrowBuf buf2 = allocator.buffer(BUFFER_LENGTH)) {
      // prepare data
      for (int i = 0; i < BUFFER_LENGTH / 4; i++) {
        buf1.setFloat(i * 4L, i / 10.0f);
        buf2.setFloat(i * 4L, i / 10.0f);
      }

      verifyHashCodesEqual(hasher, buf1, 0, 100, buf2, 0, 100);
      verifyHashCodesEqual(hasher, buf1, 1, 5, buf2, 1, 5);
      verifyHashCodesEqual(hasher, buf1, 10, 17, buf2, 10, 17);
      verifyHashCodesEqual(hasher, buf1, 33, 25, buf2, 33, 25);
      verifyHashCodesEqual(hasher, buf1, 22, 22, buf2, 22, 22);
      verifyHashCodesEqual(hasher, buf1, 123, 333, buf2, 123, 333);
      verifyHashCodesEqual(hasher, buf1, 374, 1, buf2, 374, 1);
      verifyHashCodesEqual(hasher, buf1, 11, 0, buf2, 11, 0);
      verifyHashCodesEqual(hasher, buf1, 75, 25, buf2, 75, 25);
      verifyHashCodesEqual(hasher, buf1, 0, 1024, buf2, 0, 1024);
    }
  }

  private void verifyHashCodesEqual(
      ArrowBufHasher hasher,
      ArrowBuf buf1,
      int offset1,
      int length1,
      ArrowBuf buf2,
      int offset2,
      int length2) {
    int hashCode1 = hasher.hashCode(buf1, offset1, length1);
    int hashCode2 = hasher.hashCode(buf2, offset2, length2);
    assertEquals(hashCode1, hashCode2);
  }

  @ParameterizedTest(name = "hasher = {0}")
  @MethodSource("getHasher")
  public void testHasherNegative(String name, ArrowBufHasher hasher) {
    try (ArrowBuf buf = allocator.buffer(BUFFER_LENGTH)) {
      // prepare data
      for (int i = 0; i < BUFFER_LENGTH / 4; i++) {
        buf.setFloat(i * 4L, i / 10.0f);
      }

      assertThrows(IllegalArgumentException.class, () -> hasher.hashCode(buf, 0, -1));

      assertThrows(IndexOutOfBoundsException.class, () -> hasher.hashCode(buf, 0, 1028));

      assertThrows(IndexOutOfBoundsException.class, () -> hasher.hashCode(buf, 500, 1000));
    }
  }

  @ParameterizedTest(name = "hasher = {0}")
  @MethodSource("getHasher")
  public void testHasherLessThanInt(String name, ArrowBufHasher hasher) {
    try (ArrowBuf buf1 = allocator.buffer(4);
        ArrowBuf buf2 = allocator.buffer(4)) {
      buf1.writeBytes("foo1".getBytes(StandardCharsets.UTF_8));
      buf2.writeBytes("bar2".getBytes(StandardCharsets.UTF_8));

      for (int i = 1; i <= 4; i++) {
        verifyHashCodeNotEqual(hasher, buf1, i, buf2, i);
      }
    }
  }

  private void verifyHashCodeNotEqual(
      ArrowBufHasher hasher, ArrowBuf buf1, int length1, ArrowBuf buf2, int length2) {
    int hashCode1 = hasher.hashCode(buf1, 0, length1);
    int hashCode2 = hasher.hashCode(buf2, 0, length2);
    assertNotEquals(hashCode1, hashCode2);
  }

  private static Stream<Arguments> getHasher() {
    return Stream.of(
        Arguments.of(SimpleHasher.class.getSimpleName(), SimpleHasher.INSTANCE),
        Arguments.of(MurmurHasher.class.getSimpleName(), new MurmurHasher()));
  }
}
