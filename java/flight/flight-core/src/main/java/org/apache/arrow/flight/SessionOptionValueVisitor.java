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
     * A callback to handle SessionOptionValue containing a String.
     */
    void visit(String value);

    /**
     * A callback to handle SessionOptionValue containing a boolean.
     */
    void visit(boolean value);

    /**
     * A callback to handle SessionOptionValue containing an int.
     */
    void visit(int value);

    /**
     * A callback to handle SessionOptionValue containing a long.
     */
    void visit(long value);

    /**
     * A callback to handle SessionOptionValue containing a float.
     */
    void visit(float value);

    /**
     * A callback to handle SessionOptionValue containing a double.
     */
    void visit(double value);

    /**
     * A callback to handle SessionOptionValue containing an array of String.
     */
    void visit(String[] value);
}
