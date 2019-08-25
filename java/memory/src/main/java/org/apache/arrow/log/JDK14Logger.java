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



import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Use java.util.logging to implement Logger.
 *
 * <p>The mapping of log levels from Logger to java.util.logging:
 * ERROR -- SEVERE
 * WARN  -- WARNING
 * INFO  -- INFO
 * DEBUG -- FINE
 * TRACE -- FINEST
 */
public class JDK14Logger implements org.apache.arrow.log.Logger {
  public static final String CLASS_NAME_PREFIX =
      JDK14Logger.class.getPackage().getName().substring(0,
          JDK14Logger.class.getPackage().getName().lastIndexOf('.'));

  private Logger jdkLogger;

  private Set<String> logMethods = new HashSet<>(Arrays.asList(
      "debug", "error", "info", "trace", "warn"));

  public JDK14Logger(String name) {
    this.jdkLogger = Logger.getLogger(name);
  }

  public boolean isDebugEnabled() {
    return this.jdkLogger.isLoggable(Level.FINE);
  }

  public boolean isErrorEnabled() {
    return this.jdkLogger.isLoggable(Level.SEVERE);
  }

  public boolean isInfoEnabled() {
    return this.jdkLogger.isLoggable(Level.INFO);
  }

  public boolean isTraceEnabled() {
    return this.jdkLogger.isLoggable(Level.FINEST);
  }

  public boolean isWarnEnabled() {
    return this.jdkLogger.isLoggable(Level.WARNING);
  }

  public void debug(String msg) {
    logInternal(Level.FINE, msg);
  }

  public void debug(String msg, Object... arguments) {
    logInternal(Level.FINE, msg, arguments);
  }

  public void debug(String msg, Throwable t) {
    logInternal(Level.FINE, msg, t);
  }

  public void error(String msg) {
    logInternal(Level.SEVERE, msg);
  }

  public void error(String msg, Object... arguments) {
    logInternal(Level.SEVERE, msg, arguments);
  }

  public void error(String msg, Throwable t) {
    logInternal(Level.SEVERE, msg, t);
  }

  public void info(String msg) {
    logInternal(Level.INFO, msg);
  }

  public void info(String msg, Object... arguments) {
    logInternal(Level.INFO, msg, arguments);
  }

  public void info(String msg, Throwable t) {
    logInternal(Level.INFO, msg, t);
  }

  public void trace(String msg) {
    logInternal(Level.FINEST, msg);
  }

  public void trace(String msg, Object... arguments) {
    logInternal(Level.FINEST, msg, arguments);
  }

  public void trace(String msg, Throwable t) {
    logInternal(Level.FINEST, msg, t);
  }

  public void warn(String msg) {
    logInternal(Level.WARNING, msg);
  }

  public void warn(String msg, Object... arguments) {
    logInternal(Level.WARNING, msg, arguments);
  }

  public void warn(String msg, Throwable t) {
    logInternal(Level.WARNING, msg, t);
  }

  private void logInternal(Level level, String msg) {
    if (jdkLogger.isLoggable(level)) {
      String[] source = findSourceInStack();
      jdkLogger.logp(level, source[0], source[1], msg);
    }
  }

  private void logInternal(Level level, String msg, Object... arguments) {
    if (jdkLogger.isLoggable(level)) {
      String[] source = findSourceInStack();
      jdkLogger.logp(
          level,
          source[0],
          source[1],
          refactorString(msg),
          evaluateLambdaArgs(arguments));
    }
  }

  private void logInternal(Level level, String msg, Throwable t) {
    if (jdkLogger.isLoggable(level)) {
      String[] source = findSourceInStack();
      jdkLogger.logp(level, source[0], source[1], msg, t);
    }
  }

  public static void addHandler(Handler handler) {
    Logger snowflakeLogger = Logger.getLogger(CLASS_NAME_PREFIX);
    snowflakeLogger.addHandler(handler);
  }

  public static void setLevel(Level level) {
    Logger snowflakeLogger = Logger.getLogger(CLASS_NAME_PREFIX);
    snowflakeLogger.setLevel(level);
  }

  /**
   * Since we use SLF4J ways of formatting string we need to refactor message string
   * if we have arguments.
   * For example, in sl4j, this string can be formatted with 2 arguments
   *
   * <p>ex.1: Error happened in {} on {}
   *
   * <p>And if two arguments are provided, error message can be formatted.
   *
   * <p>However, in java.util.logging, to achieve formatted error message,
   * Same string should be converted to
   *
   * <p>ex.2: Error happened in {0} on {1}
   *
   * <p>Which represented first arguments and second arguments will be replaced in the
   * corresponding places.
   *
   * <p>This method will convert string in ex.1 to ex.2
   */
  private String refactorString(String original) {
    StringBuilder sb = new StringBuilder();
    int argCount = 0;
    for (int i = 0; i < original.length(); i++) {
      if (original.charAt(i) == '{' && i < original.length() - 1 && original.charAt(i + 1) == '}') {
        sb.append(String.format("{%d}", argCount));
        argCount++;
        i++;
      } else {
        sb.append(original.charAt(i));
      }
    }
    return sb.toString();
  }

  /**
   * Used to find the index of the source class/method in current stack.
   * This method will locate the source as the first method after logMethods
   *
   * @return an array of size two, first element is className and second is
   *     methodName
   */
  private String[] findSourceInStack() {
    StackTraceElement[] stackTraces = Thread.currentThread().getStackTrace();
    String[] results = new String[2];
    for (int i = 0; i < stackTraces.length; i++) {
      if (logMethods.contains(stackTraces[i].getMethodName())) {
        // since already find the highest logMethods, find the first method after this one
        // and is not a logMethods. This is done to avoid multiple wrapper over log methods
        for (int j = i; j < stackTraces.length; j++) {
          if (!logMethods.contains(stackTraces[j].getMethodName())) {
            results[0] = stackTraces[j].getClassName();
            results[1] = stackTraces[j].getMethodName();
            return results;
          }
        }
      }
    }
    return results;
  }

  private static Object[] evaluateLambdaArgs(Object... args) {
    final Object[] result = new Object[args.length];

    for (int i = 0; i < args.length; i++) {
      result[i] = args[i] instanceof ArgSupplier ?
          ((ArgSupplier) args[i]).get() : args[i];
    }

    return result;
  }
}
