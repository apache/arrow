// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

namespace cpp impala
namespace java com.cloudera.impala.thrift

include "ExecStats.thrift"
include "Status.thrift"
include "Types.thrift"
include "beeswax.thrift"
include "TCLIService.thrift"

// ImpalaService accepts query execution options through beeswax.Query.configuration in
// key:value form. For example, the list of strings could be:
//     "num_nodes:1", "abort_on_error:false"
// The valid keys are listed in this enum. They map to TQueryOptions.
// Note: If you add an option or change the default, you also need to update:
// - ImpalaInternalService.thrift: TQueryOptions
// - SetQueryOption(), SetQueryOptions()
// - TQueryOptionsToMap()
enum TImpalaQueryOptions {
  // if true, abort execution on the first error
  ABORT_ON_ERROR,

  // maximum # of errors to be reported; Unspecified or 0 indicates backend default
  MAX_ERRORS,

  // if true, disable llvm codegen
  DISABLE_CODEGEN,

  // batch size to be used by backend; Unspecified or a size of 0 indicates backend
  // default
  BATCH_SIZE,

  // a per-machine approximate limit on the memory consumption of this query;
  // unspecified or a limit of 0 means no limit;
  // otherwise specified either as:
  // a) an int (= number of bytes);
  // b) a float followed by "M" (MB) or "G" (GB)
  MEM_LIMIT,

  // specifies the degree of parallelism with which to execute the query;
  // 1: single-node execution
  // NUM_NODES_ALL: executes on all nodes that contain relevant data
  // NUM_NODES_ALL_RACKS: executes on one node per rack that holds relevant data
  // > 1: executes on at most that many nodes at any point in time (ie, there can be
  //      more nodes than numNodes with plan fragments for this query, but at most
  //      numNodes would be active at any point in time)
  // Constants (NUM_NODES_ALL, NUM_NODES_ALL_RACKS) are defined in JavaConstants.thrift.
  NUM_NODES,

  // maximum length of the scan range; only applicable to HDFS scan range; Unspecified or
  // a length of 0 indicates backend default;
  MAX_SCAN_RANGE_LENGTH,

  // Maximum number of io buffers (per disk)
  MAX_IO_BUFFERS,

  // Number of scanner threads.
  NUM_SCANNER_THREADS,

  // If true, Impala will try to execute on file formats that are not fully supported yet
  ALLOW_UNSUPPORTED_FORMATS,

  // if set and > -1, specifies the default limit applied to a top-level SELECT statement
  // with an ORDER BY but without a LIMIT clause (ie, if the SELECT statement also has
  // a LIMIT clause, this default is ignored)
  DEFAULT_ORDER_BY_LIMIT,

  // DEBUG ONLY:
  // If set to
  //   "[<backend number>:]<node id>:<TExecNodePhase>:<TDebugAction>",
  // the exec node with the given id will perform the specified action in the given
  // phase. If the optional backend number (starting from 0) is specified, only that
  // backend instance will perform the debug action, otherwise all backends will behave
  // in that way.
  // If the string doesn't have the required format or if any of its components is
  // invalid, the option is ignored.
  DEBUG_ACTION,

  // If true, raise an error when the DEFAULT_ORDER_BY_LIMIT has been reached.
  ABORT_ON_DEFAULT_LIMIT_EXCEEDED,

  // Compression codec when inserting into tables.
  // Valid values are "snappy", "gzip", "bzip2" and "none"
  // Leave blank to use default.
  COMPRESSION_CODEC,

  // Mode for compressing sequence files; either BLOCK, RECORD, or DEFAULT
  SEQ_COMPRESSION_MODE,

  // HBase scan query option. If set and > 0, HBASE_CACHING is the value for
  // "hbase.client.Scan.setCaching()" when querying HBase table. Otherwise, use backend
  // default.
  // If the value is too high, then the hbase region server will have a hard time (GC
  // pressure and long response times). If the value is too small, then there will be
  // extra trips to the hbase region server.
  HBASE_CACHING,

  // HBase scan query option. If set, HBase scan will always set
  // "hbase.client.setCacheBlocks" to CACHE_BLOCKS. Default is false.
  // If the table is large and the query is doing big scan, set it to false to
  // avoid polluting the cache in the hbase region server.
  // If the table is small and the table is used several time, set it to true to improve
  // performance.
  HBASE_CACHE_BLOCKS,

  // Target file size for inserts into parquet tables. 0 uses the default.
  PARQUET_FILE_SIZE,

  // Level of detail for explain output (NORMAL, VERBOSE).
  EXPLAIN_LEVEL,

  // If true, waits for the result of all catalog operations to be processed by all
  // active impalad in the cluster before completing.
  SYNC_DDL,

  // Request pool this request should be submitted to. If not set
  // the pool is determined based on the user.
  REQUEST_POOL,

  // Per-host virtual CPU cores required for query (only relevant with RM).
  V_CPU_CORES,

  // Max time in milliseconds the resource broker should wait for
  // a resource request to be granted by Llama/Yarn (only relevant with RM).
  RESERVATION_REQUEST_TIMEOUT,

  // if true, disables cached reads. This option has no effect if REPLICA_PREFERENCE is
  // configured.
  // TODO: Retire in C6
  DISABLE_CACHED_READS,

  // Temporary testing flag
  DISABLE_OUTERMOST_TOPN,

  // Size of initial memory reservation when RM is enabled
  RM_INITIAL_MEM,

  // Time, in s, before a query will be timed out if it is inactive. May not exceed
  // --idle_query_timeout if that flag > 0.
  QUERY_TIMEOUT_S,

  // Test hook for spill to disk operators
  MAX_BLOCK_MGR_MEMORY,

  // Transforms all count(distinct) aggregations into NDV()
  APPX_COUNT_DISTINCT,

  // If true, allows Impala to internally disable spilling for potentially
  // disastrous query plans. Impala will excercise this option if a query
  // has no plan hints, and at least one table is missing relevant stats.
  DISABLE_UNSAFE_SPILLS,

  // If the number of rows that are processed for a single query is below the
  // threshold, it will be executed on the coordinator only with codegen disabled
  EXEC_SINGLE_NODE_ROWS_THRESHOLD,

  // If true, use the table's metadata to produce the partition columns instead of table
  // scans whenever possible. This option is opt-in by default as this optimization may
  // produce different results than the scan based approach in some edge cases.
  OPTIMIZE_PARTITION_KEY_SCANS,

  // Prefered memory distance of replicas. This parameter determines the pool of replicas
  // among which scans will be scheduled in terms of the distance of the replica storage
  // from the impalad.
  REPLICA_PREFERENCE,

  // Determines tie breaking policy when picking locations.
  RANDOM_REPLICA,

  // For scan nodes with any conjuncts, use codegen to evaluate the conjuncts if
  // the number of rows * number of operators in the conjuncts exceeds this threshold.
  SCAN_NODE_CODEGEN_THRESHOLD,

  // If true, the planner will not generate plans with streaming preaggregations.
  DISABLE_STREAMING_PREAGGREGATIONS,

  RUNTIME_FILTER_MODE,

  // Size (in bytes) of a runtime Bloom Filter. Will be rounded up to nearest power of
  // two.
  RUNTIME_BLOOM_FILTER_SIZE,

  // Time (in ms) to wait in scans for partition filters to arrive.
  RUNTIME_FILTER_WAIT_TIME_MS,

  // If true, disable application of runtime filters to individual rows.
  DISABLE_ROW_RUNTIME_FILTERING,

  // Maximum number of runtime filters allowed per query.
  MAX_NUM_RUNTIME_FILTERS
}

// The summary of an insert.
struct TInsertResult {
  // Number of appended rows per modified partition. Only applies to HDFS tables.
  // The keys represent partitions to create, coded as k1=v1/k2=v2/k3=v3..., with the
  // root in an unpartitioned table being the empty string.
  1: required map<string, i64> rows_appended
}

// Response from a call to PingImpalaService
struct TPingImpalaServiceResp {
  // The Impala service's version string.
  1: string version
}

// Parameters for a ResetTable request which will invalidate a table's metadata.
// DEPRECATED.
struct TResetTableReq {
  // Name of the table's parent database.
  1: required string db_name

  // Name of the table.
  2: required string table_name
}

// For all rpc that return a TStatus as part of their result type,
// if the status_code field is set to anything other than OK, the contents
// of the remainder of the result type is undefined (typically not set)
service ImpalaService extends beeswax.BeeswaxService {
  // Cancel execution of query. Returns RUNTIME_ERROR if query_id
  // unknown.
  // This terminates all threads running on behalf of this query at
  // all nodes that were involved in the execution.
  // Throws BeeswaxException if the query handle is invalid (this doesn't
  // necessarily indicate an error: the query might have finished).
  Status.TStatus Cancel(1:beeswax.QueryHandle query_id)
      throws(1:beeswax.BeeswaxException error);

  // Invalidates all catalog metadata, forcing a reload
  // DEPRECATED; execute query "invalidate metadata" to refresh metadata
  Status.TStatus ResetCatalog();

  // Invalidates a specific table's catalog metadata, forcing a reload on the next access
  // DEPRECATED; execute query "refresh <table>" to refresh metadata
  Status.TStatus ResetTable(1:TResetTableReq request)

  // Returns the runtime profile string for the given query handle.
  string GetRuntimeProfile(1:beeswax.QueryHandle query_id)
      throws(1:beeswax.BeeswaxException error);

  // Closes the query handle and return the result summary of the insert.
  TInsertResult CloseInsert(1:beeswax.QueryHandle handle)
      throws(1:beeswax.QueryNotFoundException error, 2:beeswax.BeeswaxException error2);

  // Client calls this RPC to verify that the server is an ImpalaService. Returns the
  // server version.
  TPingImpalaServiceResp PingImpalaService();

  // Returns the summary of the current execution.
  ExecStats.TExecSummary GetExecSummary(1:beeswax.QueryHandle handle)
      throws(1:beeswax.QueryNotFoundException error, 2:beeswax.BeeswaxException error2);
}

// Impala HiveServer2 service

struct TGetExecSummaryReq {
  1: optional TCLIService.TOperationHandle operationHandle

  2: optional TCLIService.TSessionHandle sessionHandle
}

struct TGetExecSummaryResp {
  1: required TCLIService.TStatus status

  2: optional ExecStats.TExecSummary summary
}

struct TGetRuntimeProfileReq {
  1: optional TCLIService.TOperationHandle operationHandle

  2: optional TCLIService.TSessionHandle sessionHandle
}

struct TGetRuntimeProfileResp {
  1: required TCLIService.TStatus status

  2: optional string profile
}

service ImpalaHiveServer2Service extends TCLIService.TCLIService {
  // Returns the exec summary for the given query
  TGetExecSummaryResp GetExecSummary(1:TGetExecSummaryReq req);

  // Returns the runtime profile string for the given query
  TGetRuntimeProfileResp GetRuntimeProfile(1:TGetRuntimeProfileReq req);
}
