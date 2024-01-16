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

package org.apache.arrow.flight;

/**
 * A helper to facilitate easier anonymous subclass declaration.
 *
 * Implementations need only override callbacks for types they wish to do something with.
 */
public class NoOpSessionOptionValueVisitor implements SessionOptionValueVisitor<Object> {
  /**
   * A callback to handle SessionOptionValue containing a String.
   */
  public Object visit(String value) {
    return null;
  }

  /**
   * A callback to handle SessionOptionValue containing a boolean.
   */
  public Object visit(boolean value) {
    return null;
  }

  /**
   * A callback to handle SessionOptionValue containing a long.
   */
  public Object visit(long value) {
    return null;
  }

  /**
   * A callback to handle SessionOptionValue containing a double.
   */
  public Object visit(double value) {
    return null;
  }

  /**
   * A callback to handle SessionOptionValue containing an array of String.
   */
  public Object visit(String[] value) {
    return null;
  }

  /**
   * A callback to handle SessionOptionValue containing no value.
   *
   * By convention, an attempt to set a valueless SessionOptionValue should
   * attempt to unset or clear the named option value on the server.
   */
  public Object visit(Void value) {
    return null;
  }
}
