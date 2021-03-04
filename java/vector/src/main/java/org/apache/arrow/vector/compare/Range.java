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

package org.apache.arrow.vector.compare;

/**
 * Wrapper for the parameters of comparing a range of values in two vectors.
 */
public class Range {

  /**
   * Start position in the left vector.
   */
  private int leftStart = -1;

  /**
   * Start position in the right vector.
   */
  private int rightStart = -1;

  /**
   * Length of the range.
   */
  private int length = -1;


  /**
   * Constructs a new instance.
   */
  public Range() {}

  /**
   * Constructs a new instance.
   *
   * @param leftStart start index in left vector
   * @param rightStart start index in right vector
   * @param length length of range
   */
  public Range(int leftStart, int rightStart, int length) {
    this.leftStart = leftStart;
    this.rightStart = rightStart;
    this.length = length;
  }

  public int getLeftStart() {
    return leftStart;
  }

  public int getRightStart() {
    return rightStart;
  }

  public int getLength() {
    return length;
  }

  public Range setLeftStart(int leftStart) {
    this.leftStart = leftStart;
    return this;
  }

  public Range setRightStart(int rightStart) {
    this.rightStart = rightStart;
    return this;
  }

  public Range setLength(int length) {
    this.length = length;
    return this;
  }
}
