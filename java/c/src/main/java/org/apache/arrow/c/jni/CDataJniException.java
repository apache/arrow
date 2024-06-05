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
 * An exception raised by the JNI side of the C Data bridge.
 */
public final class CDataJniException extends Exception {
  private final int errno;

  public CDataJniException(int errno, String message) {
    super(message);
    this.errno = errno;
  }

  /**
   * The original error code returned from C.
   */
  public int getErrno() {
    return errno;
  }

  @Override
  public String getMessage() {
    return "CDataJniException{" +
        "errno=" + errno +
        ", message=" + super.getMessage() +
        '}';
  }
}
