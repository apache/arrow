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

package org.apache.arrow.c.jni;

/**
 * JniWrapper for C Data Interface API implementation.
 */
public class JniWrapper {
  private static final JniWrapper INSTANCE = new JniWrapper();

  public static JniWrapper get() {
    JniLoader.get().ensureLoaded();
    return INSTANCE;
  }

  private JniWrapper() {
    // A best effort to error on 32-bit systems
    String dataModel = System.getProperty("sun.arch.data.model");
    if (dataModel != null && dataModel.equals("32")) {
      throw new UnsupportedOperationException(
          "The Java C Data Interface implementation is currently only supported on 64-bit systems");
    }
  }

  public native void releaseSchema(long memoryAddress);

  public native void releaseArray(long memoryAddress);

  public native void getNextArrayStream(long streamAddress, long arrayAddress) throws CDataJniException;

  public native void getSchemaArrayStream(long streamAddress, long arrayAddress) throws CDataJniException;

  public native void releaseArrayStream(long memoryAddress);

  public native void exportSchema(long memoryAddress, PrivateData privateData);

  public native void exportArray(long memoryAddress, PrivateData data);

  public native void exportArrayStream(long memoryAddress, PrivateData data);
}
