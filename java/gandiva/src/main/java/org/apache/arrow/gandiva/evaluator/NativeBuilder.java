/*
 * Copyright (C) 2017-2018 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.arrow.gandiva.evaluator;

/**
 * This class is implemented in JNI. This provides the Java interface
 * to invoke functions in JNI
 */
class NativeBuilder {
  private static final String LIBRARY_NAME = "gandiva_jni";

  static {
    try {
      // TODO: Load the gandiva_jni lib dynamically from a jar
      System.loadLibrary(LIBRARY_NAME);
    } catch (UnsatisfiedLinkError unsatisfiedLinkError) {
      String errMsg = "Unable to load native library: "
              + LIBRARY_NAME
              + " due to "
              + unsatisfiedLinkError.toString();
      System.err.println(errMsg);
      // TODO: Need to handle this better. Gandiva cannot exit in case of a failure
      System.exit(1);
    }
  }

  /**
   * Generates the LLVM module to evaluate the expressions.
   *
   * @param schemaBuf   The schema serialized as a protobuf. See Types.proto
   *                    to see the protobuf specification
   * @param exprListBuf The serialized protobuf of the expression vector. Each
   *                    expression is created using TreeBuilder::MakeExpression
   * @return A moduleId that is passed to the evaluate() and close() methods
   */
  static native long buildNativeCode(byte[] schemaBuf, byte[] exprListBuf);

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
  static native void evaluate(long moduleId, int numRows,
                              long[] bufAddrs, long[] bufSizes,
                              long[] outAddrs, long[] outSizes);

  /**
   * Closes the LLVM module referenced by moduleId.
   *
   * @param moduleId moduleId that needs to be closed
   */
  static native void close(long moduleId);
}
