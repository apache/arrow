/*
 * Licensed to Cloudera, Inc. under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Cloudera, Inc. licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Interface for interacting with Beeswax Server
 */

namespace java com.cloudera.beeswax.api
namespace py beeswaxd
namespace cpp beeswax

include "hive_metastore.thrift"

// A Query
struct Query {
  1: string query;
  // A list of HQL commands to execute before the query.
  // This is typically defining UDFs, setting settings, and loading resources.
  3: list<string> configuration;

  // User and groups to "act as" for purposes of Hadoop.
  4: string hadoop_user;
}

typedef string LogContextId

enum QueryState {
  CREATED,
  INITIALIZED,
  COMPILED,
  RUNNING,
  FINISHED,
  EXCEPTION
}

struct QueryHandle {
  1: string id;
  2: LogContextId log_context;
}

struct QueryExplanation {
  1: string textual
}

struct Results {
  // If set, data is valid.  Otherwise, results aren't ready yet.
  1: bool ready,
  // Columns for the results
  2: list<string> columns,
  // A set of results
  3: list<string> data,
  // The starting row of the results
  4: i64 start_row,
  // Whether there are more results to fetch
  5: bool has_more
}

/**
 * Metadata information about the results.
 * Applicable only for SELECT.
 */
struct ResultsMetadata {
  /** The schema of the results */
  1: hive_metastore.Schema schema,
  /** The directory containing the results. Not applicable for partition table. */
  2: string table_dir,
  /** If the results are straight from an existing table, the table name. */
  3: string in_tablename,
  /** Field delimiter */
  4: string delim,
}

exception BeeswaxException {
  1: string message,
  // Use get_log(log_context) to retrieve any log related to this exception
  2: LogContextId log_context,
  // (Optional) The QueryHandle that caused this exception
  3: QueryHandle handle,
  4: optional i32 errorCode = 0,
  5: optional string SQLState = "     "
}

exception QueryNotFoundException {
} 

/** Represents a Hadoop-style configuration variable. */
struct ConfigVariable {
  1: string key,
  2: string value,
  3: string description
}

service BeeswaxService {
  /**
   * Submit a query and return a handle (QueryHandle). The query runs asynchronously.
   */
  QueryHandle query(1:Query query) throws(1:BeeswaxException error),

  /**
   * run a query synchronously and return a handle (QueryHandle).
   */
  QueryHandle executeAndWait(1:Query query, 2:LogContextId clientCtx) 
                        throws(1:BeeswaxException error),

  /**
   * Get the query plan for a query.
   */
  QueryExplanation explain(1:Query query)
                        throws(1:BeeswaxException error),

  /**
   * Get the results of a query. This is non-blocking. Caller should check
   * Results.ready to determine if the results are in yet. The call requests
   * the batch size of fetch.
   */
  Results fetch(1:QueryHandle query_id, 2:bool start_over, 3:i32 fetch_size=-1) 
              throws(1:QueryNotFoundException error, 2:BeeswaxException error2),

  /**
   * Get the state of the query
   */
  QueryState get_state(1:QueryHandle handle) throws(1:QueryNotFoundException error),

  /**
   * Get the result metadata
   */
  ResultsMetadata get_results_metadata(1:QueryHandle handle)
                                    throws(1:QueryNotFoundException error),

  /**
   * Used to test connection to server.  A "noop" command.
   */
  string echo(1:string s)

  /**
   * Returns a string representation of the configuration object being used.
   * Handy for debugging.
   */
  string dump_config()

  /**
   * Get the log messages related to the given context.
   */
  string get_log(1:LogContextId context) throws(1:QueryNotFoundException error)

  /*
   * Returns "default" configuration.
   */
  list<ConfigVariable> get_default_configuration(1:bool include_hadoop)

  /*
   * closes the query with given handle
   */
  void close(1:QueryHandle handle) throws(1:QueryNotFoundException error, 
                            2:BeeswaxException error2)

  /*
   * clean the log context for given id 
   */
  void clean(1:LogContextId log_context)
}
