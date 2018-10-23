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

import java.util.Arrays;

/**
 * Convenient way of obtaining and manipulating stack traces for debugging.
 */
public class StackTrace {

  private final StackTraceElement[] stackTraceElements;

  /**
   * Constructor. Captures the current stack trace.
   */
  public StackTrace() {
    // skip over the first element so that we don't include this constructor call
    final StackTraceElement[] stack = Thread.currentThread().getStackTrace();
    stackTraceElements = Arrays.copyOfRange(stack, 1, stack.length - 1);
  }

  /**
   * Write the stack trace to a StringBuilder.
   *
   * @param sb     where to write it
   * @param indent how many double spaces to indent each line
   */
  public void writeToBuilder(final StringBuilder sb, final int indent) {
    // create the indentation string
    final char[] indentation = new char[indent * 2];
    Arrays.fill(indentation, ' ');

    // write the stack trace in standard Java format
    for (StackTraceElement ste : stackTraceElements) {
      sb.append(indentation)
          .append("at ")
          .append(ste.getClassName())
          .append('.')
          .append(ste.getMethodName())
          .append('(')
          .append(ste.getFileName())
          .append(':')
          .append(Integer.toString(ste.getLineNumber()))
          .append(")\n");
    }
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    writeToBuilder(sb, 0);
    return sb.toString();
  }
}
