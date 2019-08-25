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

package org.apache.arrow.log;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.helpers.FormattingTuple;
import org.slf4j.helpers.MessageFormatter;
import org.slf4j.spi.LocationAwareLogger;

/**
 * Use slf4j logger to implement Logger.
 *
 */
public class SLF4JLogger implements org.apache.arrow.log.Logger {
  private Logger slf4jLogger;

  private boolean isLocationAwareLogger;

  private static final String FQCN = SLF4JLogger.class.getName();

  public SLF4JLogger(Class<?> clazz) {
    slf4jLogger = LoggerFactory.getLogger(clazz);
    isLocationAwareLogger = slf4jLogger instanceof LocationAwareLogger;
  }

  public boolean isDebugEnabled() {
    return this.slf4jLogger.isDebugEnabled();
  }

  public boolean isErrorEnabled() {
    return this.slf4jLogger.isErrorEnabled();
  }

  public boolean isInfoEnabled()  {
    return this.slf4jLogger.isInfoEnabled();
  }

  public boolean isTraceEnabled() {
    return this.slf4jLogger.isTraceEnabled();
  }

  public boolean isWarnEnabled() {
    return this.slf4jLogger.isWarnEnabled();
  }

  /**
   * Log a message at the DEBUG level.
   *
   * @param msg the message string to be logged
   */
  public void debug(String msg) {
    if (isLocationAwareLogger) {
      ((LocationAwareLogger) slf4jLogger).log(null, FQCN,
          LocationAwareLogger.DEBUG_INT, msg, null, null);
    } else {
      slf4jLogger.debug(msg);
    }
  }

  /**
   * Log a message at the DEBUG level according to the specified format
   * and argument.
   * <p/>
   * <p>This form avoids superfluous object creation when the logger
   * is disabled for the DEBUG level. </p>
   *
   * @param format the format string
   * @param arg    the argument
   */
  public void debug(String format, Object... arg) {
    if (isDebugEnabled()) {
      FormattingTuple ft = MessageFormatter.arrayFormat(
          format, evaluateLambdaArgs(arg));
      this.debug(ft.getMessage());
    }
  }

  /**
   * Log an exception (throwable) at the DEBUG level with an
   * accompanying message.
   *
   * @param msg the message accompanying the exception
   * @param t   the exception (throwable) to log
   */
  public void debug(String msg, Throwable t) {
    if (isLocationAwareLogger) {
      ((LocationAwareLogger) slf4jLogger).log(null, FQCN,
          LocationAwareLogger.DEBUG_INT, msg, null, t);
    } else {
      slf4jLogger.debug(msg, t);
    }
  }

  /**
   * Log a message at the ERROR level.
   *
   * @param msg the message string to be logged
   */
  public void error(String msg) {
    if (isLocationAwareLogger) {
      ((LocationAwareLogger) slf4jLogger).log(null, FQCN,
          LocationAwareLogger.ERROR_INT, msg, null, null);
    } else {
      slf4jLogger.error(msg);
    }
  }

  /**
   * Log a message at the ERROR level according to the specified format
   * and argument.
   * <p/>
   * <p>This form avoids superfluous object creation when the logger
   * is disabled for the ERROR level. </p>
   *
   * @param format the format string
   * @param arg    the argument
   */
  public void error(String format, Object... arg) {
    if (isErrorEnabled()) {
      FormattingTuple ft = MessageFormatter.arrayFormat(
          format, evaluateLambdaArgs(arg));
      this.error(ft.getMessage());
    }
  }

  /**
   * Log an exception (throwable) at the ERROR level with an
   * accompanying message.
   *
   * @param msg the message accompanying the exception
   * @param t   the exception (throwable) to log
   */
  public void error(String msg, Throwable t) {
    if (isLocationAwareLogger) {
      ((LocationAwareLogger) slf4jLogger).log(null, FQCN,
          LocationAwareLogger.ERROR_INT, msg, null, t);
    } else {
      slf4jLogger.error(msg, t);
    }
  }

  /**
   * Log a message at the INFO level.
   *
   * @param msg the message string to be logged
   */
  public void info(String msg) {
    if (isLocationAwareLogger) {
      ((LocationAwareLogger) slf4jLogger).log(null, FQCN,
          LocationAwareLogger.INFO_INT, msg, null, null);
    } else {
      slf4jLogger.info(msg);
    }
  }

  /**
   * Log a message at the INFO level according to the specified format
   * and argument.
   * <p/>
   * <p>This form avoids superfluous object creation when the logger
   * is disabled for the INFO level. </p>
   *
   * @param format the format string
   * @param arg    the argument
   */
  public void info(String format, Object... arg) {
    if (isInfoEnabled()) {
      FormattingTuple ft = MessageFormatter.arrayFormat(
          format, evaluateLambdaArgs(arg));
      this.info(ft.getMessage());
    }
  }

  /**
   * Log an exception (throwable) at the INFO level with an
   * accompanying message.
   *
   * @param msg the message accompanying the exception
   * @param t   the exception (throwable) to log
   */
  public void info(String msg, Throwable t) {
    if (isLocationAwareLogger) {
      ((LocationAwareLogger) slf4jLogger).log(null, FQCN,
          LocationAwareLogger.INFO_INT, msg, null, t);
    } else {
      slf4jLogger.error(msg, t);
    }
  }

  /**
   * Log a message at the TRACE level.
   *
   * @param msg the message string to be logged
   * @since 1.4
   */
  public void trace(String msg) {
    if (isLocationAwareLogger) {
      ((LocationAwareLogger) slf4jLogger).log(null, FQCN,
          LocationAwareLogger.TRACE_INT, msg, null, null);
    } else {
      slf4jLogger.trace(msg);
    }
  }

  /**
   * Log a message at the TRACE level according to the specified format
   * and argument.
   * <p/>
   * <p>This form avoids superfluous object creation when the logger
   * is disabled for the TRACE level. </p>
   *
   * @param format the format string
   * @param arg    the argument
   * @since 1.4
   */
  public void trace(String format, Object... arg) {
    if (isTraceEnabled()) {
      FormattingTuple ft = MessageFormatter.arrayFormat(
          format, evaluateLambdaArgs(arg));
      this.trace(ft.getMessage());
    }
  }

  /**
   * Log an exception (throwable) at the TRACE level with an
   * accompanying message.
   *
   * @param msg the message accompanying the exception
   * @param t   the exception (throwable) to log
   * @since 1.4
   */
  public void trace(String msg, Throwable t) {
    if (isLocationAwareLogger) {
      ((LocationAwareLogger) slf4jLogger).log(null, FQCN,
          LocationAwareLogger.TRACE_INT, msg, null, t);
    } else {
      slf4jLogger.trace(msg, t);
    }
  }

  /**
   * Log a message at the WARN level.
   *
   * @param msg the message string to be logged
   */
  public void warn(String msg) {
    if (isLocationAwareLogger) {
      ((LocationAwareLogger) slf4jLogger).log(null, FQCN,
          LocationAwareLogger.WARN_INT, msg, null, null);
    } else {
      slf4jLogger.error(msg);
    }
  }

  /**
   * Log a message at the WARN level according to the specified format
   * and argument.
   * <p/>
   * <p>This form avoids superfluous object creation when the logger
   * is disabled for the WARN level. </p>
   *
   * @param format the format string
   * @param arg    the argument
   */
  public void warn(String format, Object... arg) {
    if (isWarnEnabled()) {
      FormattingTuple ft = MessageFormatter.arrayFormat(
          format, evaluateLambdaArgs(arg));
      this.warn(ft.getMessage());
    }
  }

  /**
   * Log an exception (throwable) at the WARN level with an
   * accompanying message.
   *
   * @param msg the message accompanying the exception
   * @param t   the exception (throwable) to log
   */
  public void warn(String msg, Throwable t) {
    if (isLocationAwareLogger) {
      ((LocationAwareLogger) slf4jLogger).log(null, FQCN,
          LocationAwareLogger.WARN_INT, msg, null, t);
    } else {
      slf4jLogger.error(msg, t);
    }
  }

  private static Object[] evaluateLambdaArgs(Object... args) {
    final Object[] result = new Object[args.length];

    for (int i = 0; i < args.length; i++) {
      result[i] = args[i] instanceof ArgSupplier ? ((ArgSupplier) args[i]).get()
                  : args[i];
    }

    return result;
  }
}
