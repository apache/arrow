/**
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
package org.apache.arrow.memory.util;

import java.util.concurrent.locks.Lock;

/**
 * Simple wrapper class that allows Locks to be released via an try-with-resources block.
 */
public class AutoCloseableLock implements AutoCloseable {

  private final Lock lock;

  public AutoCloseableLock(Lock lock) {
    this.lock = lock;
  }

  public AutoCloseableLock open() {
    lock.lock();
    return this;
  }

  @Override
  public void close() {
    lock.unlock();
  }

}
