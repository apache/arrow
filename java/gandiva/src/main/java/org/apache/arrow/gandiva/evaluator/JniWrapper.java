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

package org.apache.arrow.gandiva.evaluator;

import org.apache.arrow.gandiva.exceptions.GandivaException;

/**
 * This class is implemented in JNI. This provides the Java interface
 * to invoke functions in JNI.
 * This file is used to generated the .h files required for jni. Avoid all
 * external dependencies in this file.
 */
public class JniWrapper {

  /**
   * Generates the projector module to evaluate the expressions with
   * custom configuration.
   *
   * @param schemaBuf   The schema serialized as a protobuf. See Types.proto
   *                    to see the protobuf specification
   * @param exprListBuf The serialized protobuf of the expression vector. Each
   *                    expression is created using TreeBuilder::MakeExpression
   * @param configId    Configuration to gandiva.
   * @return A moduleId that is passed to the evaluateProjector() and closeProjector() methods
   *
   */
  native long buildProjector(byte[] schemaBuf, byte[] exprListBuf,
                             long configId) throws GandivaException;

  /**
   * Evaluate the expressions represented by the moduleId on a record batch
   * and store the output in ValueVectors. Throws an exception in case of errors
   *
   * @param moduleId moduleId representing expressions. Created using a call to
   *                 buildNativeCode
   * @param numRows Number of rows in the record batch
   * @param bufAddrs An array of memory addresses. Each memory address points to
   *                 a validity vector or a data vector (will add support for offset
   *                 vectors later).
   * @param bufSizes An array of buffer sizes. For each memory address in bufAddrs,
   *                 the size of the buffer is present in bufSizes
   * @param outAddrs An array of output buffers, including the validity and data
   *                 addresses.
   * @param outSizes The allocated size of the output buffers. On successful evaluation,
   *                 the result is stored in the output buffers
   */
  native void evaluateProjector(long moduleId, int numRows,
                                long[] bufAddrs, long[] bufSizes,
                                int selectionVectorType, int selectionVectorSize,
                                long selectionVectorBufferAddr, long selectionVectorBufferSize,
                                long[] outAddrs, long[] outSizes) throws GandivaException;

  /**
   * Closes the projector referenced by moduleId.
   *
   * @param moduleId moduleId that needs to be closed
   */
  native void closeProjector(long moduleId);

  /**
   * Generates the filter module to evaluate the condition expression with
   * custom configuration.
   *
   * @param schemaBuf    The schema serialized as a protobuf. See Types.proto
   *                     to see the protobuf specification
   * @param conditionBuf The serialized protobuf of the condition expression. Each
   *                     expression is created using TreeBuilder::MakeCondition
   * @param configId     Configuration to gandiva.
   * @return A moduleId that is passed to the evaluateFilter() and closeFilter() methods
   *
   */
  native long buildFilter(byte[] schemaBuf, byte[] conditionBuf,
                          long configId) throws GandivaException;

  /**
   * Evaluate the filter represented by the moduleId on a record batch
   * and store the output in buffer 'outAddr'. Throws an exception in case of errors
   *
   * @param moduleId moduleId representing expressions. Created using a call to
   *                 buildNativeCode
   * @param numRows Number of rows in the record batch
   * @param bufAddrs An array of memory addresses. Each memory address points to
   *                 a validity vector or a data vector (will add support for offset
   *                 vectors later).
   * @param bufSizes An array of buffer sizes. For each memory address in bufAddrs,
   *                 the size of the buffer is present in bufSizes
   * @param selectionVectorType type of selection vector
   * @param outAddr  output buffer, whose type is represented by selectionVectorType
   * @param outSize The allocated size of the output buffer. On successful evaluation,
   *                the result is stored in the output buffer
   */
  native int evaluateFilter(long moduleId, int numRows, long[] bufAddrs, long[] bufSizes,
                            int selectionVectorType,
                            long outAddr, long outSize) throws GandivaException;

  /**
   * Closes the filter referenced by moduleId.
   *
   * @param moduleId moduleId that needs to be closed
   */
  native void closeFilter(long moduleId);
}
