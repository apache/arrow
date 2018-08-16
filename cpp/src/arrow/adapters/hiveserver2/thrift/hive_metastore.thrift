#!/usr/local/bin/thrift -java

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
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
 */

#
# Thrift Service that the MetaStore is built on
#

include "fb303.thrift"

namespace java org.apache.hadoop.hive.metastore.api
namespace php metastore
namespace cpp Apache.Hadoop.Hive

const string DDL_TIME = "transient_lastDdlTime"

struct Version {
  1: string version,
  2: string comments
}

struct FieldSchema {
  1: string name, // name of the field
  2: string type, // type of the field. primitive types defined above, specify list<TYPE_NAME>, map<TYPE_NAME, TYPE_NAME> for lists & maps
  3: string comment
}

struct Type {
  1: string          name,             // one of the types in PrimitiveTypes or CollectionTypes or User defined types
  2: optional string type1,            // object type if the name is 'list' (LIST_TYPE), key type if the name is 'map' (MAP_TYPE)
  3: optional string type2,            // val type if the name is 'map' (MAP_TYPE)
  4: optional list<FieldSchema> fields // if the name is one of the user defined types
}

enum HiveObjectType {
  GLOBAL = 1,
  DATABASE = 2,
  TABLE = 3,
  PARTITION = 4,
  COLUMN = 5,
}

enum PrincipalType {
  USER = 1,
  ROLE = 2,
  GROUP = 3,
}

const string HIVE_FILTER_FIELD_OWNER = "hive_filter_field_owner__"
const string HIVE_FILTER_FIELD_PARAMS = "hive_filter_field_params__"
const string HIVE_FILTER_FIELD_LAST_ACCESS = "hive_filter_field_last_access__"

enum PartitionEventType {
  LOAD_DONE = 1,
}

// Enums for transaction and lock management 
enum TxnState {
    COMMITTED = 1,
    ABORTED = 2,
    OPEN = 3,
}

enum LockLevel {
    DB = 1,
    TABLE = 2,
    PARTITION = 3,
}

enum LockState {
    ACQUIRED = 1,       // requester has the lock
    WAITING = 2,        // requester is waiting for the lock and should call checklock at a later point to see if the lock has been obtained.
    ABORT = 3,          // the lock has been aborted, most likely due to timeout
    NOT_ACQUIRED = 4,   // returned only with lockNoWait, indicates the lock was not available and was not acquired
}

enum LockType {
    SHARED_READ = 1,
    SHARED_WRITE = 2,
    EXCLUSIVE = 3,
}

enum CompactionType {
    MINOR = 1,
    MAJOR = 2,
}

enum GrantRevokeType {
    GRANT = 1,
    REVOKE = 2,
}

struct HiveObjectRef{
  1: HiveObjectType objectType,
  2: string dbName,
  3: string objectName,
  4: list<string> partValues,
  5: string columnName,
}

struct PrivilegeGrantInfo {
  1: string privilege,
  2: i32 createTime,
  3: string grantor,
  4: PrincipalType grantorType,
  5: bool grantOption,
}

struct HiveObjectPrivilege {
  1: HiveObjectRef  hiveObject,
  2: string principalName,
  3: PrincipalType principalType,
  4: PrivilegeGrantInfo grantInfo,
}

struct PrivilegeBag {
  1: list<HiveObjectPrivilege> privileges,
}

struct PrincipalPrivilegeSet {
  1: map<string, list<PrivilegeGrantInfo>> userPrivileges, // user name -> privilege grant info
  2: map<string, list<PrivilegeGrantInfo>> groupPrivileges, // group name -> privilege grant info
  3: map<string, list<PrivilegeGrantInfo>> rolePrivileges, //role name -> privilege grant info
}

struct GrantRevokePrivilegeRequest {
  1: GrantRevokeType requestType;
  2: PrivilegeBag privileges;
  3: optional bool revokeGrantOption;  // Only for revoke request
}

struct GrantRevokePrivilegeResponse {
  1: optional bool success;
}

struct Role {
  1: string roleName,
  2: i32 createTime,
  3: string ownerName,
}

// Representation of a grant for a principal to a role
struct RolePrincipalGrant {
  1: string roleName,
  2: string principalName,
  3: PrincipalType principalType,
  4: bool grantOption,
  5: i32 grantTime,
  6: string grantorName,
  7: PrincipalType grantorPrincipalType
}

struct GetRoleGrantsForPrincipalRequest {
  1: required string principal_name,
  2: required PrincipalType principal_type
}

struct GetRoleGrantsForPrincipalResponse {
  1: required list<RolePrincipalGrant> principalGrants;
}

struct GetPrincipalsInRoleRequest {
  1: required string roleName;
}

struct GetPrincipalsInRoleResponse {
  1: required list<RolePrincipalGrant> principalGrants;
}

struct GrantRevokeRoleRequest {
  1: GrantRevokeType requestType;
  2: string roleName;
  3: string principalName;
  4: PrincipalType principalType;
  5: optional string grantor;            // Needed for grant
  6: optional PrincipalType grantorType; // Needed for grant
  7: optional bool grantOption;
}

struct GrantRevokeRoleResponse {
  1: optional bool success;
}

// namespace for tables
struct Database {
  1: string name,
  2: string description,
  3: string locationUri,
  4: map<string, string> parameters, // properties associated with the database
  5: optional PrincipalPrivilegeSet privileges,
  6: optional string ownerName,
  7: optional PrincipalType ownerType
}

// This object holds the information needed by SerDes
struct SerDeInfo {
  1: string name,                   // name of the serde, table name by default
  2: string serializationLib,       // usually the class that implements the extractor & loader
  3: map<string, string> parameters // initialization parameters
}

// sort order of a column (column name along with asc(1)/desc(0))
struct Order {
  1: string col,  // sort column name
  2: i32    order // asc(1) or desc(0)
}

// this object holds all the information about skewed table
struct SkewedInfo {
  1: list<string> skewedColNames, // skewed column names
  2: list<list<string>> skewedColValues, //skewed values
  3: map<list<string>, string> skewedColValueLocationMaps, //skewed value to location mappings
}

// this object holds all the information about physical storage of the data belonging to a table
struct StorageDescriptor {
  1: list<FieldSchema> cols,  // required (refer to types defined above)
  2: string location,         // defaults to <warehouse loc>/<db loc>/tablename
  3: string inputFormat,      // SequenceFileInputFormat (binary) or TextInputFormat`  or custom format
  4: string outputFormat,     // SequenceFileOutputFormat (binary) or IgnoreKeyTextOutputFormat or custom format
  5: bool   compressed,       // compressed or not
  6: i32    numBuckets,       // this must be specified if there are any dimension columns
  7: SerDeInfo    serdeInfo,  // serialization and deserialization information
  8: list<string> bucketCols, // reducer grouping columns and clustering columns and bucketing columns`
  9: list<Order>  sortCols,   // sort order of the data in each bucket
  10: map<string, string> parameters, // any user supplied key value hash
  11: optional SkewedInfo skewedInfo, // skewed information
  12: optional bool   storedAsSubDirectories       // stored as subdirectories or not
}

// table information
struct Table {
  1: string tableName,                // name of the table
  2: string dbName,                   // database name ('default')
  3: string owner,                    // owner of this table
  4: i32    createTime,               // creation time of the table
  5: i32    lastAccessTime,           // last access time (usually this will be filled from HDFS and shouldn't be relied on)
  6: i32    retention,                // retention time
  7: StorageDescriptor sd,            // storage descriptor of the table
  8: list<FieldSchema> partitionKeys, // partition keys of the table. only primitive types are supported
  9: map<string, string> parameters,   // to store comments or any other user level parameters
  10: string viewOriginalText,         // original view text, null for non-view
  11: string viewExpandedText,         // expanded view text, null for non-view
  12: string tableType,                 // table type enum, e.g. EXTERNAL_TABLE
  13: optional PrincipalPrivilegeSet privileges,
  14: optional bool temporary=false
}

struct Partition {
  1: list<string> values // string value is converted to appropriate partition key type
  2: string       dbName,
  3: string       tableName,
  4: i32          createTime,
  5: i32          lastAccessTime,
  6: StorageDescriptor   sd,
  7: map<string, string> parameters,
  8: optional PrincipalPrivilegeSet privileges
}

struct PartitionWithoutSD {
  1: list<string> values // string value is converted to appropriate partition key type
  2: i32          createTime,
  3: i32          lastAccessTime,
  4: string       relativePath,
  5: map<string, string> parameters,
  6: optional PrincipalPrivilegeSet privileges
}

struct PartitionSpecWithSharedSD {
  1: list<PartitionWithoutSD> partitions,
  2: StorageDescriptor sd,
}

struct PartitionListComposingSpec {
  1: list<Partition> partitions
}

struct PartitionSpec {
  1: string dbName,
  2: string tableName,
  3: string rootPath,
  4: optional PartitionSpecWithSharedSD sharedSDPartitionSpec,
  5: optional PartitionListComposingSpec partitionList
}

struct Index {
  1: string       indexName, // unique with in the whole database namespace
  2: string       indexHandlerClass, // reserved
  3: string       dbName,
  4: string       origTableName,
  5: i32          createTime,
  6: i32          lastAccessTime,
  7: string       indexTableName,
  8: StorageDescriptor   sd,
  9: map<string, string> parameters,
  10: bool         deferredRebuild
}

// column statistics
struct BooleanColumnStatsData {
1: required i64 numTrues,
2: required i64 numFalses,
3: required i64 numNulls
}

struct DoubleColumnStatsData {
1: optional double lowValue,
2: optional double highValue,
3: required i64 numNulls,
4: required i64 numDVs
}

struct LongColumnStatsData {
1: optional i64 lowValue,
2: optional i64 highValue,
3: required i64 numNulls,
4: required i64 numDVs
}

struct StringColumnStatsData {
1: required i64 maxColLen,
2: required double avgColLen,
3: required i64 numNulls,
4: required i64 numDVs
}

struct BinaryColumnStatsData {
1: required i64 maxColLen,
2: required double avgColLen,
3: required i64 numNulls
}


struct Decimal {
1: required binary unscaled,
3: required i16 scale
}

struct DecimalColumnStatsData {
1: optional Decimal lowValue,
2: optional Decimal highValue,
3: required i64 numNulls,
4: required i64 numDVs
}

union ColumnStatisticsData {
1: BooleanColumnStatsData booleanStats,
2: LongColumnStatsData longStats,
3: DoubleColumnStatsData doubleStats,
4: StringColumnStatsData stringStats,
5: BinaryColumnStatsData binaryStats,
6: DecimalColumnStatsData decimalStats
}

struct ColumnStatisticsObj {
1: required string colName,
2: required string colType,
3: required ColumnStatisticsData statsData
}

struct ColumnStatisticsDesc {
1: required bool isTblLevel,
2: required string dbName,
3: required string tableName,
4: optional string partName,
5: optional i64 lastAnalyzed
}

struct ColumnStatistics {
1: required ColumnStatisticsDesc statsDesc,
2: required list<ColumnStatisticsObj> statsObj;
}

struct AggrStats {
1: required list<ColumnStatisticsObj> colStats,
2: required i64 partsFound // number of partitions for which stats were found
}

struct SetPartitionsStatsRequest {
1: required list<ColumnStatistics> colStats
}

// schema of the table/query results etc.
struct Schema {
 // column names, types, comments
 1: list<FieldSchema> fieldSchemas,  // delimiters etc
 2: map<string, string> properties
}

// Key-value store to be used with selected
// Metastore APIs (create, alter methods).
// The client can pass environment properties / configs that can be
// accessed in hooks.
struct EnvironmentContext {
  1: map<string, string> properties
}

// Return type for get_partitions_by_expr
struct PartitionsByExprResult {
  1: required list<Partition> partitions,
  // Whether the results has any (currently, all) partitions which may or may not match
  2: required bool hasUnknownPartitions
}

struct PartitionsByExprRequest {
  1: required string dbName,
  2: required string tblName,
  3: required binary expr,
  4: optional string defaultPartitionName,
  5: optional i16 maxParts=-1
}

struct TableStatsResult {
  1: required list<ColumnStatisticsObj> tableStats
}

struct PartitionsStatsResult {
  1: required map<string, list<ColumnStatisticsObj>> partStats
}

struct TableStatsRequest {
 1: required string dbName,
 2: required string tblName,
 3: required list<string> colNames
}

struct PartitionsStatsRequest {
 1: required string dbName,
 2: required string tblName,
 3: required list<string> colNames,
 4: required list<string> partNames
}

// Return type for add_partitions_req
struct AddPartitionsResult {
  1: optional list<Partition> partitions,
}

// Request type for add_partitions_req
struct AddPartitionsRequest {
  1: required string dbName,
  2: required string tblName,
  3: required list<Partition> parts,
  4: required bool ifNotExists,
  5: optional bool needResult=true
}

// Return type for drop_partitions_req
struct DropPartitionsResult {
  1: optional list<Partition> partitions,
}

struct DropPartitionsExpr {
  1: required binary expr;
  2: optional i32 partArchiveLevel;
}

union RequestPartsSpec {
  1: list<string> names;
  2: list<DropPartitionsExpr> exprs;
}

// Request type for drop_partitions_req
// TODO: we might want to add "bestEffort" flag; where a subset can fail
struct DropPartitionsRequest {
  1: required string dbName,
  2: required string tblName,
  3: required RequestPartsSpec parts,
  4: optional bool deleteData,
  5: optional bool ifExists=true, // currently verified on client
  6: optional bool ignoreProtection,
  7: optional EnvironmentContext environmentContext,
  8: optional bool needResult=true
}

enum FunctionType {
  JAVA = 1,
}

enum ResourceType {
  JAR     = 1,
  FILE    = 2,
  ARCHIVE = 3,
}

struct ResourceUri {
  1: ResourceType resourceType,
  2: string       uri,
}

// User-defined function
struct Function {
  1: string           functionName,
  2: string           dbName,
  3: string           className,
  4: string           ownerName,
  5: PrincipalType    ownerType,
  6: i32              createTime,
  7: FunctionType     functionType,
  8: list<ResourceUri> resourceUris,
}

// Structs for transaction and locks
struct TxnInfo {
    1: required i64 id,
    2: required TxnState state,
    3: required string user,        // used in 'show transactions' to help admins find who has open transactions
    4: required string hostname,    // used in 'show transactions' to help admins find who has open transactions
}

struct GetOpenTxnsInfoResponse {
    1: required i64 txn_high_water_mark,
    2: required list<TxnInfo> open_txns,
}

struct GetOpenTxnsResponse {
    1: required i64 txn_high_water_mark,
    2: required set<i64> open_txns,
}

struct OpenTxnRequest {
    1: required i32 num_txns,
    2: required string user,
    3: required string hostname,
}

struct OpenTxnsResponse {
    1: required list<i64> txn_ids,
}

struct AbortTxnRequest {
    1: required i64 txnid,
}

struct CommitTxnRequest {
    1: required i64 txnid,
}

struct LockComponent {
    1: required LockType type,
    2: required LockLevel level,
    3: required string dbname,
    4: optional string tablename,
    5: optional string partitionname,
}

struct LockRequest {
    1: required list<LockComponent> component,
    2: optional i64 txnid,
    3: required string user,     // used in 'show locks' to help admins find who has open locks
    4: required string hostname, // used in 'show locks' to help admins find who has open locks
}

struct LockResponse {
    1: required i64 lockid,
    2: required LockState state,
}

struct CheckLockRequest {
    1: required i64 lockid,
}

struct UnlockRequest {
    1: required i64 lockid,
}

struct ShowLocksRequest {
}

struct ShowLocksResponseElement {
    1: required i64 lockid,
    2: required string dbname,
    3: optional string tablename,
    4: optional string partname,
    5: required LockState state,
    6: required LockType type,
    7: optional i64 txnid,
    8: required i64 lastheartbeat,
    9: optional i64 acquiredat,
    10: required string user,
    11: required string hostname,
}

struct ShowLocksResponse {
    1: list<ShowLocksResponseElement> locks,
}

struct HeartbeatRequest {
    1: optional i64 lockid,
    2: optional i64 txnid
}

struct HeartbeatTxnRangeRequest {
    1: required i64 min,
    2: required i64 max
}

struct HeartbeatTxnRangeResponse {
    1: required set<i64> aborted,
    2: required set<i64> nosuch
}

struct CompactionRequest {
    1: required string dbname,
    2: required string tablename,
    3: optional string partitionname,
    4: required CompactionType type,
    5: optional string runas,
}

struct ShowCompactRequest {
}

struct ShowCompactResponseElement {
    1: required string dbname,
    2: required string tablename,
    3: optional string partitionname,
    4: required CompactionType type,
    5: required string state,
    6: optional string workerid,
    7: optional i64 start,
    8: optional string runAs,
}

struct ShowCompactResponse {
    1: required list<ShowCompactResponseElement> compacts,
}

struct NotificationEventRequest {
    1: required i64 lastEvent,
    2: optional i32 maxEvents,
}

struct NotificationEvent {
    1: required i64 eventId,
    2: required i32 eventTime,
    3: required string eventType,
    4: optional string dbName,
    5: optional string tableName,
    6: required string message,
}

struct NotificationEventResponse {
    1: required list<NotificationEvent> events,
}

struct CurrentNotificationEventId {
    1: required i64 eventId,
}

struct InsertEventRequestData {
    1: required list<string> filesAdded
}

union FireEventRequestData {
    1: InsertEventRequestData insertData
}

struct FireEventRequest {
    1: required bool successful,
    2: required FireEventRequestData data
    // dbname, tablename, and partition vals are included as optional in the top level event rather than placed in each type of
    // subevent as I assume they'll be used across most event types.
    3: optional string dbName,
    4: optional string tableName,
    5: optional list<string> partitionVals,
}

struct FireEventResponse {
    // NOP for now, this is just a place holder for future responses
}
    

struct GetAllFunctionsResponse {
  1: optional list<Function> functions
}

struct TableMeta {
  1: required string dbName;
  2: required string tableName;
  3: required string tableType;
  4: optional string comments;
}

exception MetaException {
  1: string message
}

exception UnknownTableException {
  1: string message
}

exception UnknownDBException {
  1: string message
}

exception AlreadyExistsException {
  1: string message
}

exception InvalidPartitionException {
  1: string message
}

exception UnknownPartitionException {
  1: string message
}

exception InvalidObjectException {
  1: string message
}

exception NoSuchObjectException {
  1: string message
}

exception IndexAlreadyExistsException {
  1: string message
}

exception InvalidOperationException {
  1: string message
}

exception ConfigValSecurityException {
  1: string message
}

exception InvalidInputException {
  1: string message
}

// Transaction and lock exceptions
exception NoSuchTxnException {
    1: string message
}

exception TxnAbortedException {
    1: string message
}

exception TxnOpenException {
    1: string message
}

exception NoSuchLockException {
    1: string message
}

/**
* This interface is live.
*/
service ThriftHiveMetastore extends fb303.FacebookService
{
  string getMetaConf(1:string key) throws(1:MetaException o1)
  void setMetaConf(1:string key, 2:string value) throws(1:MetaException o1)

  void create_database(1:Database database) throws(1:AlreadyExistsException o1, 2:InvalidObjectException o2, 3:MetaException o3)
  Database get_database(1:string name) throws(1:NoSuchObjectException o1, 2:MetaException o2)
  void drop_database(1:string name, 2:bool deleteData, 3:bool cascade) throws(1:NoSuchObjectException o1, 2:InvalidOperationException o2, 3:MetaException o3)
  list<string> get_databases(1:string pattern) throws(1:MetaException o1)
  list<string> get_all_databases() throws(1:MetaException o1)
  void alter_database(1:string dbname, 2:Database db) throws(1:MetaException o1, 2:NoSuchObjectException o2)

  // returns the type with given name (make seperate calls for the dependent types if needed)
  Type get_type(1:string name)  throws(1:MetaException o1, 2:NoSuchObjectException o2)
  bool create_type(1:Type type) throws(1:AlreadyExistsException o1, 2:InvalidObjectException o2, 3:MetaException o3)
  bool drop_type(1:string type) throws(1:MetaException o1, 2:NoSuchObjectException o2)
  map<string, Type> get_type_all(1:string name)
                                throws(1:MetaException o2)

  // Gets a list of FieldSchemas describing the columns of a particular table
  list<FieldSchema> get_fields(1: string db_name, 2: string table_name) throws (1: MetaException o1, 2: UnknownTableException o2, 3: UnknownDBException o3),
  list<FieldSchema> get_fields_with_environment_context(1: string db_name, 2: string table_name, 3:EnvironmentContext environment_context) throws (1: MetaException o1, 2: UnknownTableException o2, 3: UnknownDBException o3)

  // Gets a list of FieldSchemas describing both the columns and the partition keys of a particular table
  list<FieldSchema> get_schema(1: string db_name, 2: string table_name) throws (1: MetaException o1, 2: UnknownTableException o2, 3: UnknownDBException o3)
  list<FieldSchema> get_schema_with_environment_context(1: string db_name, 2: string table_name, 3:EnvironmentContext environment_context) throws (1: MetaException o1, 2: UnknownTableException o2, 3: UnknownDBException o3)

  // create a Hive table. Following fields must be set
  // tableName
  // database        (only 'default' for now until Hive QL supports databases)
  // owner           (not needed, but good to have for tracking purposes)
  // sd.cols         (list of field schemas)
  // sd.inputFormat  (SequenceFileInputFormat (binary like falcon tables or u_full) or TextInputFormat)
  // sd.outputFormat (SequenceFileInputFormat (binary) or TextInputFormat)
  // sd.serdeInfo.serializationLib (SerDe class name eg org.apache.hadoop.hive.serde.simple_meta.MetadataTypedColumnsetSerDe
  // * See notes on DDL_TIME
  void create_table(1:Table tbl) throws(1:AlreadyExistsException o1, 2:InvalidObjectException o2, 3:MetaException o3, 4:NoSuchObjectException o4)
  void create_table_with_environment_context(1:Table tbl,
      2:EnvironmentContext environment_context)
      throws (1:AlreadyExistsException o1,
              2:InvalidObjectException o2, 3:MetaException o3,
              4:NoSuchObjectException o4)
  // drops the table and all the partitions associated with it if the table has partitions
  // delete data (including partitions) if deleteData is set to true
  void drop_table(1:string dbname, 2:string name, 3:bool deleteData)
                       throws(1:NoSuchObjectException o1, 2:MetaException o3)
  void drop_table_with_environment_context(1:string dbname, 2:string name, 3:bool deleteData,
      4:EnvironmentContext environment_context)
                       throws(1:NoSuchObjectException o1, 2:MetaException o3)
  list<string> get_tables(1: string db_name, 2: string pattern) throws (1: MetaException o1)
  list<TableMeta> get_table_meta(1: string db_patterns, 2: string tbl_patterns, 3: list<string> tbl_types)
                       throws (1: MetaException o1)
  list<string> get_all_tables(1: string db_name) throws (1: MetaException o1)

  Table get_table(1:string dbname, 2:string tbl_name)
                       throws (1:MetaException o1, 2:NoSuchObjectException o2)
  list<Table> get_table_objects_by_name(1:string dbname, 2:list<string> tbl_names)
				   throws (1:MetaException o1, 2:InvalidOperationException o2, 3:UnknownDBException o3)

  // Get a list of table names that match a filter.
  // The filter operators are LIKE, <, <=, >, >=, =, <>
  //
  // In the filter statement, values interpreted as strings must be enclosed in quotes,
  // while values interpreted as integers should not be.  Strings and integers are the only
  // supported value types.
  //
  // The currently supported key names in the filter are:
  // Constants.HIVE_FILTER_FIELD_OWNER, which filters on the tables' owner's name
  //   and supports all filter operators
  // Constants.HIVE_FILTER_FIELD_LAST_ACCESS, which filters on the last access times
  //   and supports all filter operators except LIKE
  // Constants.HIVE_FILTER_FIELD_PARAMS, which filters on the tables' parameter keys and values
  //   and only supports the filter operators = and <>.
  //   Append the parameter key name to HIVE_FILTER_FIELD_PARAMS in the filter statement.
  //   For example, to filter on parameter keys called "retention", the key name in the filter
  //   statement should be Constants.HIVE_FILTER_FIELD_PARAMS + "retention"
  //   Also, = and <> only work for keys that exist
  //   in the tables. E.g., if you are looking for tables where key1 <> value, it will only
  //   look at tables that have a value for the parameter key1.
  // Some example filter statements include:
  // filter = Constants.HIVE_FILTER_FIELD_OWNER + " like \".*test.*\" and " +
  //   Constants.HIVE_FILTER_FIELD_LAST_ACCESS + " = 0";
  // filter = Constants.HIVE_FILTER_FIELD_PARAMS + "retention = \"30\" or " +
  //   Constants.HIVE_FILTER_FIELD_PARAMS + "retention = \"90\""
  // @param dbName
  //          The name of the database from which you will retrieve the table names
  // @param filterType
  //          The type of filter
  // @param filter
  //          The filter string
  // @param max_tables
  //          The maximum number of tables returned
  // @return  A list of table names that match the desired filter
  list<string> get_table_names_by_filter(1:string dbname, 2:string filter, 3:i16 max_tables=-1)
                       throws (1:MetaException o1, 2:InvalidOperationException o2, 3:UnknownDBException o3)

  // alter table applies to only future partitions not for existing partitions
  // * See notes on DDL_TIME
  void alter_table(1:string dbname, 2:string tbl_name, 3:Table new_tbl)
                       throws (1:InvalidOperationException o1, 2:MetaException o2)
  void alter_table_with_environment_context(1:string dbname, 2:string tbl_name,
      3:Table new_tbl, 4:EnvironmentContext environment_context)
      throws (1:InvalidOperationException o1, 2:MetaException o2)
  // alter table not only applies to future partitions but also cascade to existing partitions
  void alter_table_with_cascade(1:string dbname, 2:string tbl_name, 3:Table new_tbl, 4:bool cascade)
                       throws (1:InvalidOperationException o1, 2:MetaException o2)
  // the following applies to only tables that have partitions
  // * See notes on DDL_TIME
  Partition add_partition(1:Partition new_part)
                       throws(1:InvalidObjectException o1, 2:AlreadyExistsException o2, 3:MetaException o3)
  Partition add_partition_with_environment_context(1:Partition new_part,
      2:EnvironmentContext environment_context)
      throws (1:InvalidObjectException o1, 2:AlreadyExistsException o2,
      3:MetaException o3)
  i32 add_partitions(1:list<Partition> new_parts)
                       throws(1:InvalidObjectException o1, 2:AlreadyExistsException o2, 3:MetaException o3)
  i32 add_partitions_pspec(1:list<PartitionSpec> new_parts)
                       throws(1:InvalidObjectException o1, 2:AlreadyExistsException o2, 3:MetaException o3)
  Partition append_partition(1:string db_name, 2:string tbl_name, 3:list<string> part_vals)
                       throws (1:InvalidObjectException o1, 2:AlreadyExistsException o2, 3:MetaException o3)
  AddPartitionsResult add_partitions_req(1:AddPartitionsRequest request)
                       throws(1:InvalidObjectException o1, 2:AlreadyExistsException o2, 3:MetaException o3)
  Partition append_partition_with_environment_context(1:string db_name, 2:string tbl_name,
      3:list<string> part_vals, 4:EnvironmentContext environment_context)
                       throws (1:InvalidObjectException o1, 2:AlreadyExistsException o2, 3:MetaException o3)
  Partition append_partition_by_name(1:string db_name, 2:string tbl_name, 3:string part_name)
                       throws (1:InvalidObjectException o1, 2:AlreadyExistsException o2, 3:MetaException o3)
  Partition append_partition_by_name_with_environment_context(1:string db_name, 2:string tbl_name,
      3:string part_name, 4:EnvironmentContext environment_context)
                       throws (1:InvalidObjectException o1, 2:AlreadyExistsException o2, 3:MetaException o3)
  bool drop_partition(1:string db_name, 2:string tbl_name, 3:list<string> part_vals, 4:bool deleteData)
                       throws(1:NoSuchObjectException o1, 2:MetaException o2)
  bool drop_partition_with_environment_context(1:string db_name, 2:string tbl_name,
      3:list<string> part_vals, 4:bool deleteData, 5:EnvironmentContext environment_context)
                       throws(1:NoSuchObjectException o1, 2:MetaException o2)
  bool drop_partition_by_name(1:string db_name, 2:string tbl_name, 3:string part_name, 4:bool deleteData)
                       throws(1:NoSuchObjectException o1, 2:MetaException o2)
  bool drop_partition_by_name_with_environment_context(1:string db_name, 2:string tbl_name,
      3:string part_name, 4:bool deleteData, 5:EnvironmentContext environment_context)
                       throws(1:NoSuchObjectException o1, 2:MetaException o2)
  DropPartitionsResult drop_partitions_req(1: DropPartitionsRequest req)
                       throws(1:NoSuchObjectException o1, 2:MetaException o2)

  Partition get_partition(1:string db_name, 2:string tbl_name, 3:list<string> part_vals)
                       throws(1:MetaException o1, 2:NoSuchObjectException o2)
  Partition exchange_partition(1:map<string, string> partitionSpecs, 2:string source_db,
      3:string source_table_name, 4:string dest_db, 5:string dest_table_name)
      throws(1:MetaException o1, 2:NoSuchObjectException o2, 3:InvalidObjectException o3,
      4:InvalidInputException o4)

  Partition get_partition_with_auth(1:string db_name, 2:string tbl_name, 3:list<string> part_vals,
      4: string user_name, 5: list<string> group_names) throws(1:MetaException o1, 2:NoSuchObjectException o2)

  Partition get_partition_by_name(1:string db_name 2:string tbl_name, 3:string part_name)
                       throws(1:MetaException o1, 2:NoSuchObjectException o2)

  // returns all the partitions for this table in reverse chronological order.
  // If max parts is given then it will return only that many.
  list<Partition> get_partitions(1:string db_name, 2:string tbl_name, 3:i16 max_parts=-1)
                       throws(1:NoSuchObjectException o1, 2:MetaException o2)
  list<Partition> get_partitions_with_auth(1:string db_name, 2:string tbl_name, 3:i16 max_parts=-1,
     4: string user_name, 5: list<string> group_names) throws(1:NoSuchObjectException o1, 2:MetaException o2)

  list<PartitionSpec> get_partitions_pspec(1:string db_name, 2:string tbl_name, 3:i32 max_parts=-1)
                       throws(1:NoSuchObjectException o1, 2:MetaException o2)

  list<string> get_partition_names(1:string db_name, 2:string tbl_name, 3:i16 max_parts=-1)
                       throws(1:MetaException o2)

  // get_partition*_ps methods allow filtering by a partial partition specification,
  // as needed for dynamic partitions. The values that are not restricted should
  // be empty strings. Nulls were considered (instead of "") but caused errors in
  // generated Python code. The size of part_vals may be smaller than the
  // number of partition columns - the unspecified values are considered the same
  // as "".
  list<Partition> get_partitions_ps(1:string db_name 2:string tbl_name
  	3:list<string> part_vals, 4:i16 max_parts=-1)
                       throws(1:MetaException o1, 2:NoSuchObjectException o2)
  list<Partition> get_partitions_ps_with_auth(1:string db_name, 2:string tbl_name, 3:list<string> part_vals, 4:i16 max_parts=-1,
     5: string user_name, 6: list<string> group_names) throws(1:NoSuchObjectException o1, 2:MetaException o2)

  list<string> get_partition_names_ps(1:string db_name,
  	2:string tbl_name, 3:list<string> part_vals, 4:i16 max_parts=-1)
  	                   throws(1:MetaException o1, 2:NoSuchObjectException o2)

  // get the partitions matching the given partition filter
  list<Partition> get_partitions_by_filter(1:string db_name 2:string tbl_name
    3:string filter, 4:i16 max_parts=-1)
                       throws(1:MetaException o1, 2:NoSuchObjectException o2)

  // List partitions as PartitionSpec instances.
  list<PartitionSpec> get_part_specs_by_filter(1:string db_name 2:string tbl_name
    3:string filter, 4:i32 max_parts=-1)
                       throws(1:MetaException o1, 2:NoSuchObjectException o2)

  // get the partitions matching the given partition filter
  // unlike get_partitions_by_filter, takes serialized hive expression, and with that can work
  // with any filter (get_partitions_by_filter only works if the filter can be pushed down to JDOQL.
  PartitionsByExprResult get_partitions_by_expr(1:PartitionsByExprRequest req)
                       throws(1:MetaException o1, 2:NoSuchObjectException o2)

  // get partitions give a list of partition names
  list<Partition> get_partitions_by_names(1:string db_name 2:string tbl_name 3:list<string> names)
                       throws(1:MetaException o1, 2:NoSuchObjectException o2)

  // changes the partition to the new partition object. partition is identified from the part values
  // in the new_part
  // * See notes on DDL_TIME
  void alter_partition(1:string db_name, 2:string tbl_name, 3:Partition new_part)
                       throws (1:InvalidOperationException o1, 2:MetaException o2)

  // change a list of partitions. All partitions are altered atomically and all
  // prehooks are fired together followed by all post hooks
  void alter_partitions(1:string db_name, 2:string tbl_name, 3:list<Partition> new_parts)
                       throws (1:InvalidOperationException o1, 2:MetaException o2)

  void alter_partition_with_environment_context(1:string db_name,
      2:string tbl_name, 3:Partition new_part,
      4:EnvironmentContext environment_context)
      throws (1:InvalidOperationException o1, 2:MetaException o2)

  // rename the old partition to the new partition object by changing old part values to the part values
  // in the new_part. old partition is identified from part_vals.
  // partition keys in new_part should be the same as those in old partition.
  void rename_partition(1:string db_name, 2:string tbl_name, 3:list<string> part_vals, 4:Partition new_part)
                       throws (1:InvalidOperationException o1, 2:MetaException o2)

  // returns whether or not the partition name is valid based on the value of the config
  // hive.metastore.partition.name.whitelist.pattern
  bool partition_name_has_valid_characters(1:list<string> part_vals, 2:bool throw_exception)
 	throws(1: MetaException o1)

  // gets the value of the configuration key in the metastore server. returns
  // defaultValue if the key does not exist. if the configuration key does not
  // begin with "hive", "mapred", or "hdfs", a ConfigValSecurityException is
  // thrown.
  string get_config_value(1:string name, 2:string defaultValue)
                          throws(1:ConfigValSecurityException o1)

  // converts a partition name into a partition values array
  list<string> partition_name_to_vals(1: string part_name)
                          throws(1: MetaException o1)
  // converts a partition name into a partition specification (a mapping from
  // the partition cols to the values)
  map<string, string> partition_name_to_spec(1: string part_name)
                          throws(1: MetaException o1)

  void markPartitionForEvent(1:string db_name, 2:string tbl_name, 3:map<string,string> part_vals,
                  4:PartitionEventType eventType) throws (1: MetaException o1, 2: NoSuchObjectException o2,
                  3: UnknownDBException o3, 4: UnknownTableException o4, 5: UnknownPartitionException o5,
                  6: InvalidPartitionException o6)
  bool isPartitionMarkedForEvent(1:string db_name, 2:string tbl_name, 3:map<string,string> part_vals,
                  4: PartitionEventType eventType) throws (1: MetaException o1, 2:NoSuchObjectException o2,
                  3: UnknownDBException o3, 4: UnknownTableException o4, 5: UnknownPartitionException o5,
                  6: InvalidPartitionException o6)

  //index
  Index add_index(1:Index new_index, 2: Table index_table)
                       throws(1:InvalidObjectException o1, 2:AlreadyExistsException o2, 3:MetaException o3)
  void alter_index(1:string dbname, 2:string base_tbl_name, 3:string idx_name, 4:Index new_idx)
                       throws (1:InvalidOperationException o1, 2:MetaException o2)
  bool drop_index_by_name(1:string db_name, 2:string tbl_name, 3:string index_name, 4:bool deleteData)
                       throws(1:NoSuchObjectException o1, 2:MetaException o2)
  Index get_index_by_name(1:string db_name 2:string tbl_name, 3:string index_name)
                       throws(1:MetaException o1, 2:NoSuchObjectException o2)

  list<Index> get_indexes(1:string db_name, 2:string tbl_name, 3:i16 max_indexes=-1)
                       throws(1:NoSuchObjectException o1, 2:MetaException o2)
  list<string> get_index_names(1:string db_name, 2:string tbl_name, 3:i16 max_indexes=-1)
                       throws(1:MetaException o2)

  // column statistics interfaces

  // update APIs persist the column statistics object(s) that are passed in. If statistics already
  // exists for one or more columns, the existing statistics will be overwritten. The update APIs
  // validate that the dbName, tableName, partName, colName[] passed in as part of the ColumnStatistics
  // struct are valid, throws InvalidInputException/NoSuchObjectException if found to be invalid
  bool update_table_column_statistics(1:ColumnStatistics stats_obj) throws (1:NoSuchObjectException o1,
              2:InvalidObjectException o2, 3:MetaException o3, 4:InvalidInputException o4)
  bool update_partition_column_statistics(1:ColumnStatistics stats_obj) throws (1:NoSuchObjectException o1,
              2:InvalidObjectException o2, 3:MetaException o3, 4:InvalidInputException o4)

  // get APIs return the column statistics corresponding to db_name, tbl_name, [part_name], col_name if
  // such statistics exists. If the required statistics doesn't exist, get APIs throw NoSuchObjectException
  // For instance, if get_table_column_statistics is called on a partitioned table for which only
  // partition level column stats exist, get_table_column_statistics will throw NoSuchObjectException
  ColumnStatistics get_table_column_statistics(1:string db_name, 2:string tbl_name, 3:string col_name) throws
              (1:NoSuchObjectException o1, 2:MetaException o2, 3:InvalidInputException o3, 4:InvalidObjectException o4)
  ColumnStatistics get_partition_column_statistics(1:string db_name, 2:string tbl_name, 3:string part_name,
               4:string col_name) throws (1:NoSuchObjectException o1, 2:MetaException o2,
               3:InvalidInputException o3, 4:InvalidObjectException o4)
  TableStatsResult get_table_statistics_req(1:TableStatsRequest request) throws
              (1:NoSuchObjectException o1, 2:MetaException o2)
  PartitionsStatsResult get_partitions_statistics_req(1:PartitionsStatsRequest request) throws
              (1:NoSuchObjectException o1, 2:MetaException o2)
  AggrStats get_aggr_stats_for(1:PartitionsStatsRequest request) throws
              (1:NoSuchObjectException o1, 2:MetaException o2)
  bool set_aggr_stats_for(1:SetPartitionsStatsRequest request) throws
              (1:NoSuchObjectException o1, 2:InvalidObjectException o2, 3:MetaException o3, 4:InvalidInputException o4)


  // delete APIs attempt to delete column statistics, if found, associated with a given db_name, tbl_name, [part_name]
  // and col_name. If the delete API doesn't find the statistics record in the metastore, throws NoSuchObjectException
  // Delete API validates the input and if the input is invalid throws InvalidInputException/InvalidObjectException.
  bool delete_partition_column_statistics(1:string db_name, 2:string tbl_name, 3:string part_name, 4:string col_name) throws
              (1:NoSuchObjectException o1, 2:MetaException o2, 3:InvalidObjectException o3,
               4:InvalidInputException o4)
  bool delete_table_column_statistics(1:string db_name, 2:string tbl_name, 3:string col_name) throws
              (1:NoSuchObjectException o1, 2:MetaException o2, 3:InvalidObjectException o3,
               4:InvalidInputException o4)

  //
  // user-defined functions
  //

  void create_function(1:Function func)
      throws (1:AlreadyExistsException o1,
              2:InvalidObjectException o2,
              3:MetaException o3,
              4:NoSuchObjectException o4)

  void drop_function(1:string dbName, 2:string funcName)
      throws (1:NoSuchObjectException o1, 2:MetaException o3)

  void alter_function(1:string dbName, 2:string funcName, 3:Function newFunc)
      throws (1:InvalidOperationException o1, 2:MetaException o2)

  list<string> get_functions(1:string dbName, 2:string pattern)
      throws (1:MetaException o1)
  Function get_function(1:string dbName, 2:string funcName)
      throws (1:MetaException o1, 2:NoSuchObjectException o2)

  GetAllFunctionsResponse get_all_functions() throws (1:MetaException o1)

  //authorization privileges

  bool create_role(1:Role role) throws(1:MetaException o1)
  bool drop_role(1:string role_name) throws(1:MetaException o1)
  list<string> get_role_names() throws(1:MetaException o1)
  // Deprecated, use grant_revoke_role()
  bool grant_role(1:string role_name, 2:string principal_name, 3:PrincipalType principal_type,
    4:string grantor, 5:PrincipalType grantorType, 6:bool grant_option) throws(1:MetaException o1)
  // Deprecated, use grant_revoke_role()
  bool revoke_role(1:string role_name, 2:string principal_name, 3:PrincipalType principal_type)
                        throws(1:MetaException o1)
  list<Role> list_roles(1:string principal_name, 2:PrincipalType principal_type) throws(1:MetaException o1)
  GrantRevokeRoleResponse grant_revoke_role(1:GrantRevokeRoleRequest request) throws(1:MetaException o1)

  // get all role-grants for users/roles that have been granted the given role
  // Note that in the returned list of RolePrincipalGrants, the roleName is
  // redundant as it would match the role_name argument of this function
  GetPrincipalsInRoleResponse get_principals_in_role(1: GetPrincipalsInRoleRequest request) throws(1:MetaException o1)

  // get grant information of all roles granted to the given principal
  // Note that in the returned list of RolePrincipalGrants, the principal name,type is
  // redundant as it would match the principal name,type arguments of this function
  GetRoleGrantsForPrincipalResponse get_role_grants_for_principal(1: GetRoleGrantsForPrincipalRequest request) throws(1:MetaException o1)

  PrincipalPrivilegeSet get_privilege_set(1:HiveObjectRef hiveObject, 2:string user_name,
    3: list<string> group_names) throws(1:MetaException o1)
  list<HiveObjectPrivilege> list_privileges(1:string principal_name, 2:PrincipalType principal_type,
    3: HiveObjectRef hiveObject) throws(1:MetaException o1)

  // Deprecated, use grant_revoke_privileges()
  bool grant_privileges(1:PrivilegeBag privileges) throws(1:MetaException o1)
  // Deprecated, use grant_revoke_privileges()
  bool revoke_privileges(1:PrivilegeBag privileges) throws(1:MetaException o1)
  GrantRevokePrivilegeResponse grant_revoke_privileges(1:GrantRevokePrivilegeRequest request) throws(1:MetaException o1);

  // this is used by metastore client to send UGI information to metastore server immediately
  // after setting up a connection.
  list<string> set_ugi(1:string user_name, 2:list<string> group_names) throws (1:MetaException o1)

  //Authentication (delegation token) interfaces

  // get metastore server delegation token for use from the map/reduce tasks to authenticate
  // to metastore server
  string get_delegation_token(1:string token_owner, 2:string renewer_kerberos_principal_name)
    throws (1:MetaException o1)

  // method to renew delegation token obtained from metastore server
  i64 renew_delegation_token(1:string token_str_form) throws (1:MetaException o1)

  // method to cancel delegation token obtained from metastore server
  void cancel_delegation_token(1:string token_str_form) throws (1:MetaException o1)

  // Transaction and lock management calls
  // Get just list of open transactions
  GetOpenTxnsResponse get_open_txns()
  // Get list of open transactions with state (open, aborted)
  GetOpenTxnsInfoResponse get_open_txns_info()
  OpenTxnsResponse open_txns(1:OpenTxnRequest rqst)
  void abort_txn(1:AbortTxnRequest rqst) throws (1:NoSuchTxnException o1)
  void commit_txn(1:CommitTxnRequest rqst) throws (1:NoSuchTxnException o1, 2:TxnAbortedException o2)
  LockResponse lock(1:LockRequest rqst) throws (1:NoSuchTxnException o1, 2:TxnAbortedException o2)
  LockResponse check_lock(1:CheckLockRequest rqst)
    throws (1:NoSuchTxnException o1, 2:TxnAbortedException o2, 3:NoSuchLockException o3)
  void unlock(1:UnlockRequest rqst) throws (1:NoSuchLockException o1, 2:TxnOpenException o2)
  ShowLocksResponse show_locks(1:ShowLocksRequest rqst)
  void heartbeat(1:HeartbeatRequest ids) throws (1:NoSuchLockException o1, 2:NoSuchTxnException o2, 3:TxnAbortedException o3)
  HeartbeatTxnRangeResponse heartbeat_txn_range(1:HeartbeatTxnRangeRequest txns)
  void compact(1:CompactionRequest rqst) 
  ShowCompactResponse show_compact(1:ShowCompactRequest rqst)

  // Notification logging calls
  NotificationEventResponse get_next_notification(1:NotificationEventRequest rqst) 
  CurrentNotificationEventId get_current_notificationEventId()
}

// * Note about the DDL_TIME: When creating or altering a table or a partition,
// if the DDL_TIME is not set, the current time will be used.

// For storing info about archived partitions in parameters

// Whether the partition is archived
const string IS_ARCHIVED = "is_archived",
// The original location of the partition, before archiving. After archiving,
// this directory will contain the archive. When the partition
// is dropped, this directory will be deleted
const string ORIGINAL_LOCATION = "original_location",

// Whether or not the table is considered immutable - immutable tables can only be
// overwritten or created if unpartitioned, or if partitioned, partitions inside them
// can only be overwritten or created. Immutability supports write-once and replace
// semantics, but not append.
const string IS_IMMUTABLE = "immutable",

// these should be needed only for backward compatibility with filestore
const string META_TABLE_COLUMNS   = "columns",
const string META_TABLE_COLUMN_TYPES   = "columns.types",
const string BUCKET_FIELD_NAME    = "bucket_field_name",
const string BUCKET_COUNT         = "bucket_count",
const string FIELD_TO_DIMENSION   = "field_to_dimension",
const string META_TABLE_NAME      = "name",
const string META_TABLE_DB        = "db",
const string META_TABLE_LOCATION  = "location",
const string META_TABLE_SERDE     = "serde",
const string META_TABLE_PARTITION_COLUMNS = "partition_columns",
const string META_TABLE_PARTITION_COLUMN_TYPES = "partition_columns.types",
const string FILE_INPUT_FORMAT    = "file.inputformat",
const string FILE_OUTPUT_FORMAT   = "file.outputformat",
const string META_TABLE_STORAGE   = "storage_handler",
const string TABLE_IS_TRANSACTIONAL = "transactional",
const string TABLE_NO_AUTO_COMPACT = "no_auto_compaction",


