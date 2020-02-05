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

import static org.junit.Assert.assertEquals;

import org.junit.Test;

/**
 * Test cases for {@link DataSizeRoundingUtil}.
 */
public class TestDataSizeRoundingUtil {

  @Test
  public void testRoundUpTo8MultipleInt() {
    assertEquals(0, DataSizeRoundingUtil.roundUpTo8Multiple(0));
    assertEquals(16, DataSizeRoundingUtil.roundUpTo8Multiple(9));
    assertEquals(24, DataSizeRoundingUtil.roundUpTo8Multiple(20));
    assertEquals(128, DataSizeRoundingUtil.roundUpTo8Multiple(128));
  }

  @Test
  public void testRoundUpTo8MultipleLong() {
    assertEquals(0L, DataSizeRoundingUtil.roundUpTo8Multiple(0L));
    assertEquals(40L, DataSizeRoundingUtil.roundUpTo8Multiple(37L));
    assertEquals(32L, DataSizeRoundingUtil.roundUpTo8Multiple(29L));
    assertEquals(512L, DataSizeRoundingUtil.roundUpTo8Multiple(512L));
  }

  @Test
  public void testRoundDownTo8MultipleInt() {
    assertEquals(0, DataSizeRoundingUtil.roundDownTo8Multiple(0));
    assertEquals(16, DataSizeRoundingUtil.roundDownTo8Multiple(23));
    assertEquals(24, DataSizeRoundingUtil.roundDownTo8Multiple(27));
    assertEquals(128, DataSizeRoundingUtil.roundDownTo8Multiple(128));
  }

  @Test
  public void testRoundDownTo8MultipleLong() {
    assertEquals(0L, DataSizeRoundingUtil.roundDownTo8Multiple(0L));
    assertEquals(40L, DataSizeRoundingUtil.roundDownTo8Multiple(45L));
    assertEquals(32L, DataSizeRoundingUtil.roundDownTo8Multiple(39L));
    assertEquals(512L, DataSizeRoundingUtil.roundDownTo8Multiple(512L));
  }

  @Test
  public void testDivideBy8CeilInt() {
    assertEquals(0, DataSizeRoundingUtil.divideBy8Ceil(0));
    assertEquals(3, DataSizeRoundingUtil.divideBy8Ceil(23));
    assertEquals(5, DataSizeRoundingUtil.divideBy8Ceil(35));
    assertEquals(24, DataSizeRoundingUtil.divideBy8Ceil(192));
  }

  @Test
  public void testDivideBy8CeilLong() {
    assertEquals(0L, DataSizeRoundingUtil.divideBy8Ceil(0L));
    assertEquals(5L, DataSizeRoundingUtil.divideBy8Ceil(37L));
    assertEquals(10L, DataSizeRoundingUtil.divideBy8Ceil(73L));
    assertEquals(25L, DataSizeRoundingUtil.divideBy8Ceil(200L));
  }
}
