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
 * A visitor interface to access SessionOptionValue's contained value.
 */
public interface SessionOptionValueVisitor {
    /**
     * Callback to handle SessionOptionValue containing a String.
     * @param value The contained value being accessed.
     */
    void visit(String value);

    /**
     * Callback to handle SessionOptionValue containing a boolean.
     * @param value The contained value being accessed.
     */
    void visit(boolean value);

    /**
     * Callback to handle SessionOptionValue containing an int.
     * @param value The contained value being accessed.
     */
    void visit(int value);

    /**
     * Callback to handle SessionOptionValue containing a long.
     * @param value The contained value being accessed.
     */
    void visit(long value);

    /**
     * Callback to handle SessionOptionValue containing a float.
     * @param value The contained value being accessed.
     */
    void visit(float value);

    /**
     * Callback to handle SessionOptionValue containing a double.
     * @param value The contained value being accessed.
     */
    void visit(double value);

    /**
     * Callback to handle SessionOptionValue containing an array of String.
     * @param value The contained value being accessed.
     */
    void visit(String[] value);
}
