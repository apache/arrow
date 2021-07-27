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

package org.apache.arrow.flight.sql;

import java.io.Serializable;
import java.sql.Statement;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nullable;

import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.util.Preconditions;

/**
 * Context for {@link T} to be persisted in memory in between {@link FlightSqlProducer} calls.
 *
 * @param <T> the {@link Statement} to be persisted.
 */
public final class StatementContext<T extends Statement> implements AutoCloseable, Serializable {

  private static final long serialVersionUID = 1344967087502630673L;

  private final T statement;
  private final String query;

  public StatementContext(final T statement, final @Nullable String query) {
    this.statement = Preconditions.checkNotNull(statement);
    this.query = query;
  }

  public StatementContext(final T statement) {
    this(statement, null);
  }

  /**
   * Gets the statement wrapped by this {@link StatementContext}.
   *
   * @return the inner statement.
   */
  public T getStatement() {
    return statement;
  }

  /**
   * Gets the optional SQL query wrapped by this {@link StatementContext}.
   *
   * @return the SQL query if present; empty otherwise.
   */
  public Optional<String> getQuery() {
    return Optional.ofNullable(query);
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(statement, statement.getConnection());
  }

  @Override
  public boolean equals(final Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof StatementContext)) {
      return false;
    }
    final StatementContext<?> that = (StatementContext<?>) other;
    return getStatement().equals(that.getStatement());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getStatement());
  }
}
