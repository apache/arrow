/**
 * Portions Copyright (C) 2017-2018 Dremio Corporation
 *
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

package org.apache.arrow.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Utilities for AutoCloseable classes.
 */
public final class AutoCloseables {
  // Utility class. Should not be instantiated
  private AutoCloseables() {
  }

  public static AutoCloseable all(final Collection<? extends AutoCloseable> autoCloseables) {
    return new AutoCloseable() {
      @Override
      public void close() throws Exception {
        AutoCloseables.close(autoCloseables);
      }
    };
  }

  /**
   * Closes all autoCloseables if not null and suppresses exceptions by adding them to t.
   * @param t the throwable to add suppressed exception to
   * @param autoCloseables the closeables to close
   */
  public static void close(Throwable t, AutoCloseable... autoCloseables) {
    close(t, Arrays.asList(autoCloseables));
  }

  /**
   * Closes all autoCloseables if not null and suppresses exceptions by adding them to t.
   * @param t the throwable to add suppressed exception to
   * @param autoCloseables the closeables to close
   */
  public static void close(Throwable t, Iterable<? extends AutoCloseable> autoCloseables) {
    try {
      close(autoCloseables);
    } catch (Exception e) {
      t.addSuppressed(e);
    }
  }

  /**
   * Closes all autoCloseables if not null and suppresses subsequent exceptions if more than one.
   * @param autoCloseables the closeables to close
   */
  public static void close(AutoCloseable... autoCloseables) throws Exception {
    close(Arrays.asList(autoCloseables));
  }

  /**
   * Closes all autoCloseables if not null and suppresses subsequent exceptions if more than one.
   * @param ac the closeables to close
   */
  public static void close(Iterable<? extends AutoCloseable> ac) throws Exception {
    // this method can be called on a single object if it implements Iterable<AutoCloseable>
    // like for example VectorContainer make sure we handle that properly
    if (ac == null) {
      return;
    } else if (ac instanceof AutoCloseable) {
      ((AutoCloseable) ac).close();
      return;
    }

    Exception topLevelException = null;
    for (AutoCloseable closeable : ac) {
      try {
        if (closeable != null) {
          closeable.close();
        }
      } catch (Exception e) {
        if (topLevelException == null) {
          topLevelException = e;
        } else if (e != topLevelException) {
          topLevelException.addSuppressed(e);
        }
      }
    }
    if (topLevelException != null) {
      throw topLevelException;
    }
  }

  @SafeVarargs
  public static void close(Iterable<? extends AutoCloseable>...closeables) throws Exception {
    close(Arrays.asList(closeables).stream()
        .flatMap((Iterable<? extends AutoCloseable> t) -> Collections2.toList(t).stream())
        .collect(Collectors.toList()));
  }

  public static Iterable<AutoCloseable> iter(AutoCloseable... ac) {
    if (ac.length == 0) {
      return Collections.emptyList();
    } else {
      final List<AutoCloseable> nonNullAc = new ArrayList<>();
      for (AutoCloseable autoCloseable : ac) {
        if (autoCloseable != null) {
          nonNullAc.add(autoCloseable);
        }
      }
      return nonNullAc;
    }
  }

  /**
   * A closeable wrapper that will close the underlying closeables if a commit does not occur.
   */
  public static class RollbackCloseable implements AutoCloseable {

    private boolean commit = false;
    private List<AutoCloseable> closeables;

    public RollbackCloseable(AutoCloseable... closeables) {
      this.closeables = new ArrayList<>(Arrays.asList(closeables));
    }

    public <T extends AutoCloseable> T add(T t) {
      closeables.add(t);
      return t;
    }

    public void addAll(AutoCloseable... list) {
      closeables.addAll(Arrays.asList(list));
    }

    public void addAll(Iterable<? extends AutoCloseable> list) {
      for (AutoCloseable ac : list) {
        closeables.add(ac);
      }
    }

    public void commit() {
      commit = true;
    }

    @Override
    public void close() throws Exception {
      if (!commit) {
        AutoCloseables.close(closeables);
      }
    }

  }

  public static RollbackCloseable rollbackable(AutoCloseable... closeables) {
    return new RollbackCloseable(closeables);
  }

  /**
   * close() an {@see java.lang.AutoCloseable} without throwing a (checked)
   * {@see java.lang.Exception}. This wraps the close() call with a
   * try-catch that will rethrow an Exception wrapped with a
   * {@see java.lang.RuntimeException}, providing a way to call close()
   * without having to do the try-catch everywhere or propagate the Exception.
   *
   * @param autoCloseable the AutoCloseable to close; may be null
   * @throws RuntimeException if an Exception occurs; the Exception is
   *         wrapped by the RuntimeException
   */
  public static void closeNoChecked(final AutoCloseable autoCloseable) {
    if (autoCloseable != null) {
      try {
        autoCloseable.close();
      } catch (final Exception e) {
        throw new RuntimeException("Exception while closing: " + e.getMessage(), e);
      }
    }
  }

  private static final AutoCloseable noOpAutocloseable = new AutoCloseable() {
    @Override
    public void close() {
    }
  };

  /**
   * Get an AutoCloseable that does nothing.
   *
   * @return A do-nothing autocloseable
   */
  public static AutoCloseable noop() {
    return noOpAutocloseable;
  }
}

