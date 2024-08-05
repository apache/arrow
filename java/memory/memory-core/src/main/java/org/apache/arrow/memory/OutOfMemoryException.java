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
package org.apache.arrow.memory;

import java.util.Optional;

/**
 * Indicates memory could not be allocated for Arrow buffers.
 *
 * <p>This is different from {@linkplain OutOfMemoryError} which indicates the JVM is out of memory.
 * This error indicates that static limit of one of Arrow's allocators (e.g. {@linkplain
 * BaseAllocator}) has been exceeded.
 */
public class OutOfMemoryException extends RuntimeException {

  static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(OutOfMemoryException.class);
  private static final long serialVersionUID = -6858052345185793382L;
  private Optional<AllocationOutcomeDetails> outcomeDetails = Optional.empty();

  public OutOfMemoryException() {
    super();
  }

  public OutOfMemoryException(
      String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }

  public OutOfMemoryException(String message, Throwable cause) {
    super(message, cause);
  }

  public OutOfMemoryException(String message) {
    super(message);
  }

  public OutOfMemoryException(String message, Optional<AllocationOutcomeDetails> details) {
    super(message);
    this.outcomeDetails = details;
  }

  public OutOfMemoryException(Throwable cause) {
    super(cause);
  }

  public Optional<AllocationOutcomeDetails> getOutcomeDetails() {
    return outcomeDetails;
  }
}
