/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
    private final  static String LIBRARY_NAME = "JniGandiva";

    static {
        try {
            System.loadLibrary(LIBRARY_NAME);
        } catch (UnsatisfiedLinkError e) {
            System.err.println("Native code library failed to load.\n" + e);
            System.exit(1);
        }
    }

    static native long BuildNativeCode(byte[] schemaBuf, byte[] exprListBuf);

    static native void Evaluate(long moduleID,
                                byte[] recordBatchBuf, int recordBatchOffset,
                                long[] inputBufAddrs,
                                long[] outValidityAddrs, long[] outValueAddrs);

}
