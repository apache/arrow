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

package org.apache.arrow.dataset.jni;

import java.io.PrintWriter;
import java.io.StringWriter;

/**
 * For native code to invoke to convert a java/lang/Throwable to jstring.
 */
class JniExceptionDescriber {
  private JniExceptionDescriber() {
  }

  /**
   * Convert a java/lang/Throwable to jstring. See codes in arrow::dataset::jni::CheckException
   * for more details.
   *
   * @param throwable the exception instance.
   * @return a String including error message and stack trace of the exception.
   */
  static String describe(Throwable throwable) {
    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw, true);
    throwable.printStackTrace(pw);
    return sw.getBuffer().toString();
  }
}
