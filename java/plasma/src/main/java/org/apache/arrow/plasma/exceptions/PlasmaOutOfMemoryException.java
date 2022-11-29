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

package org.apache.arrow.plasma.exceptions;

/**
 * Indicates no more memory is available in Plasma.
 *
 * @deprecated Plasma is deprecated since 10.0.0. Plasma will not be released from Apache Arrow 12.0.0 or so.
 */
@Deprecated
public class PlasmaOutOfMemoryException extends RuntimeException {

  public PlasmaOutOfMemoryException(String message) {
    super("The plasma store ran out of memory." + message);
  }

  public PlasmaOutOfMemoryException(String message, Throwable t) {
    super("The plasma store ran out of memory." + message, t);
  }

  public PlasmaOutOfMemoryException() {
    super("The plasma store ran out of memory.");
  }

  public PlasmaOutOfMemoryException(Throwable t) {
    super("The plasma store ran out of memory.", t);
  }
}
