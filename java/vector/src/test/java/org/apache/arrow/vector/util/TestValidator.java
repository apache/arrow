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

package org.apache.arrow.vector.util;

import static org.apache.arrow.vector.util.Validator.equalEnough;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class TestValidator {

  @Test
  public void testFloatComp() {
    assertTrue(equalEnough(912.4140000000002F, 912.414F));
    assertTrue(equalEnough(912.4140000000002D, 912.414D));
    assertTrue(equalEnough(912.414F, 912.4140000000002F));
    assertTrue(equalEnough(912.414D, 912.4140000000002D));
    assertFalse(equalEnough(912.414D, 912.4140001D));
    assertFalse(equalEnough(null, 912.414D));
    assertTrue(equalEnough((Float) null, null));
    assertTrue(equalEnough((Double) null, null));
    assertFalse(equalEnough(912.414D, null));
    assertFalse(equalEnough(Double.MAX_VALUE, Double.MIN_VALUE));
    assertFalse(equalEnough(Double.MIN_VALUE, Double.MAX_VALUE));
    assertTrue(equalEnough(Double.MAX_VALUE, Double.MAX_VALUE));
    assertTrue(equalEnough(Double.MIN_VALUE, Double.MIN_VALUE));
    assertTrue(equalEnough(Double.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY));
    assertFalse(equalEnough(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY));
    assertTrue(equalEnough(Double.NaN, Double.NaN));
    assertFalse(equalEnough(1.0, Double.NaN));
    assertFalse(equalEnough(Float.MAX_VALUE, Float.MIN_VALUE));
    assertFalse(equalEnough(Float.MIN_VALUE, Float.MAX_VALUE));
    assertTrue(equalEnough(Float.MAX_VALUE, Float.MAX_VALUE));
    assertTrue(equalEnough(Float.MIN_VALUE, Float.MIN_VALUE));
    assertTrue(equalEnough(Float.NEGATIVE_INFINITY, Float.NEGATIVE_INFINITY));
    assertFalse(equalEnough(Float.NEGATIVE_INFINITY, Float.POSITIVE_INFINITY));
    assertTrue(equalEnough(Float.NaN, Float.NaN));
    assertFalse(equalEnough(1.0F, Float.NaN));
  }
}
