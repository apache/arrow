/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.arrow.memory.util;

import java.util.Arrays;
import java.util.LinkedList;

import org.slf4j.Logger;

/**
 * Utility class that can be used to log activity within a class
 * for later logging and debugging. Supports recording events and
 * recording the stack at the time they occur.
 */
public class HistoricalLog {

  private final LinkedList<Event> history = new LinkedList<>();
  private final String idString; // the formatted id string
  private final int limit; // the limit on the number of events kept
  private Event firstEvent; // the first stack trace recorded

  /**
   * Constructor. The format string will be formatted and have its arguments
   * substituted at the time this is called.
   *
   * @param idStringFormat {@link String#format} format string that can be used to identify this
   *                       object in a log. Including some kind of unique identifier that can be
   *                       associated with the object instance is best.
   * @param args           for the format string, or nothing if none are required
   */
  public HistoricalLog(final String idStringFormat, Object... args) {
    this(Integer.MAX_VALUE, idStringFormat, args);
  }

  /**
   * Constructor. The format string will be formatted and have its arguments
   * substituted at the time this is called.
   * <p>
   * <p>This form supports the specification of a limit that will limit the
   * number of historical entries kept (which keeps down the amount of memory
   * used). With the limit, the first entry made is always kept (under the
   * assumption that this is the creation site of the object, which is usually
   * interesting), and then up to the limit number of entries are kept after that.
   * Each time a new entry is made, the oldest that is not the first is dropped.
   * </p>
   *
   * @param limit          the maximum number of historical entries that will be kept, not including
   *                       the first entry made
   * @param idStringFormat {@link String#format} format string that can be used to identify this
   *                       object in a log. Including some kind of unique identifier that can be
   *                       associated with the object instance is best.
   * @param args           for the format string, or nothing if none are required
   */
  public HistoricalLog(final int limit, final String idStringFormat, Object... args) {
    this.limit = limit;
    this.idString = String.format(idStringFormat, args);
  }

  /**
   * Record an event. Automatically captures the stack trace at the time this is
   * called. The format string will be formatted and have its arguments substituted
   * at the time this is called.
   *
   * @param noteFormat {@link String#format} format string that describes the event
   * @param args       for the format string, or nothing if none are required
   */
  public synchronized void recordEvent(final String noteFormat, Object... args) {
    final String note = String.format(noteFormat, args);
    final Event event = new Event(note);
    if (firstEvent == null) {
      firstEvent = event;
    }
    if (history.size() == limit) {
      history.removeFirst();
    }
    history.add(event);
  }

  /**
   * Write the history of this object to the given {@link StringBuilder}. The history
   * includes the identifying string provided at construction time, and all the recorded
   * events with their stack traces.
   *
   * @param sb {@link StringBuilder} to write to
   * @param includeStackTrace whether to include the stacktrace of each event in the history
   */
  public void buildHistory(final StringBuilder sb, boolean includeStackTrace) {
    buildHistory(sb, 0, includeStackTrace);
  }

  /**
   * build the history and write it to sb
   * @param sb output
   * @param indent starting indent (usually "")
   * @param includeStackTrace whether to include the stacktrace of each event.
   */
  public synchronized void buildHistory(
      final StringBuilder sb, int indent, boolean includeStackTrace) {
    final char[] indentation = new char[indent];
    final char[] innerIndentation = new char[indent + 2];
    Arrays.fill(indentation, ' ');
    Arrays.fill(innerIndentation, ' ');

    sb.append(indentation)
        .append("event log for: ")
        .append(idString)
        .append('\n');

    if (firstEvent != null) {
      sb.append(innerIndentation)
          .append(firstEvent.time)
          .append(' ')
          .append(firstEvent.note)
          .append('\n');
      if (includeStackTrace) {
        firstEvent.stackTrace.writeToBuilder(sb, indent + 2);
      }

      for (final Event event : history) {
        if (event == firstEvent) {
          continue;
        }
        sb.append(innerIndentation)
            .append("  ")
            .append(event.time)
            .append(' ')
            .append(event.note)
            .append('\n');

        if (includeStackTrace) {
          event.stackTrace.writeToBuilder(sb, indent + 2);
          sb.append('\n');
        }
      }
    }
  }

  /**
   * Write the history of this object to the given {@link Logger}. The history
   * includes the identifying string provided at construction time, and all the recorded
   * events with their stack traces.
   *
   * @param logger {@link Logger} to write to
   */
  public void logHistory(final Logger logger) {
    final StringBuilder sb = new StringBuilder();
    buildHistory(sb, 0, true);
    logger.debug(sb.toString());
  }

  private static class Event {

    private final String note; // the event text
    private final StackTrace stackTrace; // where the event occurred
    private final long time;

    public Event(final String note) {
      this.note = note;
      this.time = System.nanoTime();
      stackTrace = new StackTrace();
    }
  }
}
