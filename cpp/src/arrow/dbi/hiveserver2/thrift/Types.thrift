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

typedef i64 TTimestamp
typedef i32 TPlanNodeId
typedef i32 TTupleId
typedef i32 TSlotId
typedef i32 TTableId

// TODO: Consider moving unrelated enums to better locations.

enum TPrimitiveType {
  INVALID_TYPE,
  NULL_TYPE,
  BOOLEAN,
  TINYINT,
  SMALLINT,
  INT,
  BIGINT,
  FLOAT,
  DOUBLE,
  DATE,
  DATETIME,
  TIMESTAMP,
  STRING,
  // Unsupported types
  BINARY,
  DECIMAL,
  // CHAR(n). Currently only supported in UDAs
  CHAR,
  VARCHAR
}

enum TTypeNodeType {
  SCALAR,
  ARRAY,
  MAP,
  STRUCT
}

struct TScalarType {
  1: required TPrimitiveType type

  // Only set if type == CHAR or type == VARCHAR
  2: optional i32 len

  // Only set for DECIMAL
  3: optional i32 precision
  4: optional i32 scale
}

// Represents a field in a STRUCT type.
// TODO: Model column stats for struct fields.
struct TStructField {
  1: required string name
  2: optional string comment
}

struct TTypeNode {
  1: required TTypeNodeType type

  // only set for scalar types
  2: optional TScalarType scalar_type

  // only used for structs; has struct_fields.size() corresponding child types
  3: optional list<TStructField> struct_fields
}

// A flattened representation of a tree of column types obtained by depth-first
// traversal. Complex types such as map, array and struct have child types corresponding
// to the map key/value, array item type, and struct fields, respectively.
// For scalar types the list contains only a single node.
// Note: We cannot rename this to TType because it conflicts with Thrift's internal TType
// and the generated Python thrift files will not work.
struct TColumnType {
  1: list<TTypeNode> types
}

enum TStmtType {
  QUERY,
  DDL, // Data definition, e.g. CREATE TABLE (includes read-only functions e.g. SHOW)
  DML, // Data modification e.g. INSERT
  EXPLAIN,
  LOAD, // Statement type for LOAD commands
  SET
}

// Level of verboseness for "explain" output.
enum TExplainLevel {
  MINIMAL,
  STANDARD,
  EXTENDED,
  VERBOSE
}

enum TRuntimeFilterMode {
  // No filters are computed in the FE or the BE.
  OFF,

  // Only broadcast filters are computed in the BE, and are only published to the local
  // fragment.
  LOCAL,

  // All fiters are computed in the BE, and are published globally.
  GLOBAL
}

// A TNetworkAddress is the standard host, port representation of a
// network address. The hostname field must be resolvable to an IPv4
// address.
struct TNetworkAddress {
  1: required string hostname
  2: required i32 port
}

// Wire format for UniqueId
struct TUniqueId {
  1: required i64 hi
  2: required i64 lo
}

enum TFunctionCategory {
  SCALAR,
  AGGREGATE,
  ANALYTIC
}

enum TFunctionBinaryType {
  // Impala builtin. We can either run this interpreted or via codegen
  // depending on the query option.
  BUILTIN,

  // Java UDFs, loaded from *.jar
  JAVA,

  // Native-interface, precompiled UDFs loaded from *.so
  NATIVE,

  // Native-interface, precompiled to IR; loaded from *.ll
  IR,
}

// Represents a fully qualified function name.
struct TFunctionName {
  // Name of the function's parent database. Not set if in global
  // namespace (e.g. builtins)
  1: optional string db_name

  // Name of the function
  2: required string function_name
}

struct TScalarFunction {
  1: required string symbol;
  2: optional string prepare_fn_symbol
  3: optional string close_fn_symbol
}

struct TAggregateFunction {
  1: required TColumnType intermediate_type
  2: required string update_fn_symbol
  3: required string init_fn_symbol
  4: optional string serialize_fn_symbol
  5: optional string merge_fn_symbol
  6: optional string finalize_fn_symbol
  8: optional string get_value_fn_symbol
  9: optional string remove_fn_symbol

  7: optional bool ignores_distinct
}

// Represents a function in the Catalog.
struct TFunction {
  // Fully qualified function name.
  1: required TFunctionName name

  // Type of the udf. e.g. hive, native, ir
  2: required TFunctionBinaryType binary_type

  // The types of the arguments to the function
  3: required list<TColumnType> arg_types

  // Return type for the function.
  4: required TColumnType ret_type

  // If true, this function takes var args.
  5: required bool has_var_args

  // Optional comment to attach to the function
  6: optional string comment

  7: optional string signature

  // HDFS path for the function binary. This binary must exist at the time the
  // function is created.
  8: optional string hdfs_location

  // One of these should be set.
  9: optional TScalarFunction scalar_fn
  10: optional TAggregateFunction aggregate_fn

  // True for builtins or user-defined functions persisted by the catalog
  11: required bool is_persistent
}
