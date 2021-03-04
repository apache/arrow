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

package org.apache.arrow.vector.testing;

import java.util.Random;
import java.util.function.Supplier;

/**
 * Utility for generating random data.
 */
public class RandomDataGenerator {

  static final Random random = new Random(0);

  public static final Supplier<Byte> TINY_INT_GENERATOR = () -> (byte) random.nextInt();

  public static final Supplier<Short> SMALL_INT_GENERATOR = () -> (short) random.nextInt();

  public static final Supplier<Integer> INT_GENERATOR = () -> random.nextInt();

  public static final Supplier<Long> LONG_GENERATOR = () -> random.nextLong();

  public static final Supplier<Float> FLOAT_GENERATOR = () -> random.nextFloat();

  public static final Supplier<Double> DOUBLE_GENERATOR = () -> random.nextDouble();

  private RandomDataGenerator() {
  }
}
