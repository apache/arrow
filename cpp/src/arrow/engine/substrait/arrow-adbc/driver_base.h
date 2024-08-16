// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <cstring>
#include <memory>
#include <sstream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "adbc.h"

// This file defines a developer-friendly way to create an ADBC driver, currently intended
// for testing the R driver manager. It handles errors, option getting/setting, and
// managing the export of the many C callables that compose an AdbcDriver. In general,
// functions or methods intended to be called from C are prefixed with "C" and are private
// (i.e., the public and protected methods are the only ones that driver authors should
// ever interact with).
//
// Example:
// class MyDatabase: public DatabaseObjectBase {};
// class MyConnection: public ConnectionObjectBase {};
// class MyStatement: public StatementObjectbase {};
// AdbcStatusCode VoidDriverInitFunc(int version, void* raw_driver, AdbcError* error) {
//   return Driver<MyDatabase, MyConnection, MyStatement>::Init(
//     version, raw_driver, error);
// }

namespace adbc {

namespace common {

class Error {
 public:
  explicit Error(std::string message) : message_(std::move(message)) {
    std::memset(sql_state_, 0, sizeof(sql_state_));
  }

  explicit Error(const char* message) : Error(std::string(message)) {}

  Error(std::string message, std::vector<std::pair<std::string, std::string>> details)
      : message_(std::move(message)), details_(std::move(details)) {
    std::memset(sql_state_, 0, sizeof(sql_state_));
  }

  void AddDetail(std::string key, std::string value) {
    details_.push_back({std::move(key), std::move(value)});
  }

  void ToAdbc(AdbcError* adbc_error, AdbcDriver* driver = nullptr) {
    if (adbc_error == nullptr) {
      return;
    }

    if (adbc_error->vendor_code == ADBC_ERROR_VENDOR_CODE_PRIVATE_DATA) {
      auto error_owned_by_adbc_error =
          new Error(std::move(message_), std::move(details_));
      adbc_error->message =
          const_cast<char*>(error_owned_by_adbc_error->message_.c_str());
      adbc_error->private_data = error_owned_by_adbc_error;
      adbc_error->private_driver = driver;
    } else {
      adbc_error->message = reinterpret_cast<char*>(std::malloc(message_.size() + 1));
      if (adbc_error->message != nullptr) {
        std::memcpy(adbc_error->message, message_.c_str(), message_.size() + 1);
      }
    }

    std::memcpy(adbc_error->sqlstate, sql_state_, sizeof(sql_state_));
    adbc_error->release = &CRelease;
  }

 private:
  std::string message_;
  std::vector<std::pair<std::string, std::string>> details_;
  char sql_state_[5];

  // Let the Driver use these to expose C callables wrapping option setters/getters
  template <typename DatabaseT, typename ConnectionT, typename StatementT>
  friend class Driver;

  int CDetailCount() const { return static_cast<int>(details_.size()); }

  AdbcErrorDetail CDetail(int index) const {
    const auto& detail = details_[index];
    return {detail.first.c_str(), reinterpret_cast<const uint8_t*>(detail.second.data()),
            detail.second.size() + 1};
  }

  static void CRelease(AdbcError* error) {
    if (error->vendor_code == ADBC_ERROR_VENDOR_CODE_PRIVATE_DATA) {
      auto error_obj = reinterpret_cast<Error*>(error->private_data);
      delete error_obj;
    } else {
      std::free(error->message);
    }

    std::memset(error, 0, sizeof(AdbcError));
  }
};

// Variant that handles the option types that can be get/set by databases,
// connections, and statements. It currently does not attempt conversion
// (i.e., getting a double option as a string).
class Option {
 public:
  enum Type { TYPE_MISSING, TYPE_STRING, TYPE_BYTES, TYPE_INT, TYPE_DOUBLE };

  Option() : type_(TYPE_MISSING) {}
  explicit Option(const std::string& value) : type_(TYPE_STRING), value_string_(value) {}
  explicit Option(const std::vector<uint8_t>& value)
      : type_(TYPE_BYTES), value_bytes_(value) {}
  explicit Option(double value) : type_(TYPE_DOUBLE), value_double_(value) {}
  explicit Option(int64_t value) : type_(TYPE_INT), value_int_(value) {}

  Type type() const { return type_; }

  const std::string& GetStringUnsafe() const { return value_string_; }

  const std::vector<uint8_t>& GetBytesUnsafe() const { return value_bytes_; }

  int64_t GetIntUnsafe() const { return value_int_; }

  double GetDoubleUnsafe() const { return value_double_; }

 private:
  Type type_;
  std::string value_string_;
  std::vector<uint8_t> value_bytes_;
  double value_double_;
  int64_t value_int_;

  // Methods used by trampolines to export option values in C below
  friend class ObjectBase;

  AdbcStatusCode CGet(char* out, size_t* length) const {
    switch (type_) {
      case TYPE_STRING: {
        const std::string& value = GetStringUnsafe();
        size_t value_size_with_terminator = value.size() + 1;
        if (*length < value_size_with_terminator) {
          *length = value_size_with_terminator;
        } else {
          memcpy(out, value.data(), value_size_with_terminator);
        }

        return ADBC_STATUS_OK;
      }
      default:
        return ADBC_STATUS_NOT_FOUND;
    }
  }

  AdbcStatusCode CGet(uint8_t* out, size_t* length) const {
    switch (type_) {
      case TYPE_BYTES: {
        const std::vector<uint8_t>& value = GetBytesUnsafe();
        if (*length < value.size()) {
          *length = value.size();
        } else {
          memcpy(out, value.data(), value.size());
        }

        return ADBC_STATUS_OK;
      }
      default:
        return ADBC_STATUS_NOT_FOUND;
    }
  }

  AdbcStatusCode CGet(int64_t* value) const {
    switch (type_) {
      case TYPE_INT:
        *value = GetIntUnsafe();
        return ADBC_STATUS_OK;
      default:
        return ADBC_STATUS_NOT_FOUND;
    }
  }

  AdbcStatusCode CGet(double* value) const {
    switch (type_) {
      case TYPE_DOUBLE:
        *value = GetDoubleUnsafe();
        return ADBC_STATUS_OK;
      default:
        return ADBC_STATUS_NOT_FOUND;
    }
  }
};

// Base class for private_data of AdbcDatabase, AdbcConnection, and AdbcStatement
// This class handles option setting and getting.
class ObjectBase {
 public:
  ObjectBase() : driver_(nullptr) {}

  virtual ~ObjectBase() {}

  // Driver authors can override this method to reject options that are not supported or
  // that are set at a time not supported by the driver (e.g., to reject options that are
  // set after Init() is called if this is not supported).
  virtual AdbcStatusCode SetOption(const std::string& key, const Option& value) {
    options_[key] = value;
    return ADBC_STATUS_OK;
  }

  // Called After zero or more SetOption() calls. The parent is the private_data of
  // the AdbcDriver, AdbcDatabase, or AdbcConnection when initializing a subclass of
  // DatabaseObjectBase, ConnectionObjectBase, and StatementObjectBase (respectively).
  // For example, if you have defined Driver<MyDatabase, MyConnection, MyStatement>,
  // you can reinterpret_cast<MyDatabase>(parent) in MyConnection::Init().
  virtual AdbcStatusCode Init(void* parent, AdbcError* error) { return ADBC_STATUS_OK; }

  // Called when the corresponding AdbcXXXRelease() function is invoked from C.
  // Driver authors can override this method to return an error if the object is
  // not in a valid state (e.g., if a connection has open statements) or to clean
  // up resources when resource cleanup could fail. Resource cleanup that cannot fail
  // (e.g., releasing memory) should generally be handled in the deleter.
  virtual AdbcStatusCode Release(AdbcError* error) { return ADBC_STATUS_OK; }

  // Get an option that was previously set, providing an optional default value.
  virtual const Option& GetOption(const std::string& key,
                                  const Option& default_value = Option()) const {
    auto result = options_.find(key);
    if (result == options_.end()) {
      return default_value;
    } else {
      return result->second;
    }
  }

 protected:
  // Needed to export errors using Error::ToAdbc() that use 1.1.0 extensions
  // (i.e., error details). This will be nullptr before Init() is called.
  AdbcDriver* driver() const { return driver_; }

 private:
  AdbcDriver* driver_;
  std::unordered_map<std::string, Option> options_;

  // Let the Driver use these to expose C callables wrapping option setters/getters
  template <typename DatabaseT, typename ConnectionT, typename StatementT>
  friend class Driver;

  // The AdbcDriver* struct is set right before Init() is called by the Driver
  // trampoline.
  void set_driver(AdbcDriver* driver) { driver_ = driver; }

  template <typename T>
  AdbcStatusCode CSetOption(const char* key, T value, AdbcError* error) {
    Option option(value);
    return SetOption(key, option);
  }

  AdbcStatusCode CSetOptionBytes(const char* key, const uint8_t* value, size_t length,
                                 AdbcError* error) {
    std::vector<uint8_t> cppvalue(value, value + length);
    Option option(cppvalue);
    return SetOption(key, option);
  }

  template <typename T>
  AdbcStatusCode CGetOptionStringLike(const char* key, T* value, size_t* length,
                                      AdbcError* error) const {
    Option result = GetOption(key);
    if (result.type() == Option::TYPE_MISSING) {
      InitErrorNotFound(key, error);
      return ADBC_STATUS_NOT_FOUND;
    } else {
      AdbcStatusCode status = result.CGet(value, length);
      if (status != ADBC_STATUS_OK) {
        InitErrorWrongType(key, error);
      }

      return status;
    }
  }

  template <typename T>
  AdbcStatusCode CGetOptionNumeric(const char* key, T* value, AdbcError* error) const {
    Option result = GetOption(key);
    if (result.type() == Option::TYPE_MISSING) {
      InitErrorNotFound(key, error);
      return ADBC_STATUS_NOT_FOUND;
    } else {
      AdbcStatusCode status = result.CGet(value);
      if (status != ADBC_STATUS_OK) {
        InitErrorWrongType(key, error);
      }

      return status;
    }
  }

  void InitErrorNotFound(const char* key, AdbcError* error) const {
    std::stringstream msg_builder;
    msg_builder << "Option not found for key '" << key << "'";
    Error cpperror(msg_builder.str());
    cpperror.AddDetail("adbc.driver_base.option_key", key);
    cpperror.ToAdbc(error, driver());
  }

  void InitErrorWrongType(const char* key, AdbcError* error) const {
    std::stringstream msg_builder;
    msg_builder << "Wrong type requested for option key '" << key << "'";
    Error cpperror(msg_builder.str());
    cpperror.AddDetail("adbc.driver_base.option_key", key);
    cpperror.ToAdbc(error, driver());
  }
};

// Driver authors can subclass DatabaseObjectBase to track driver-specific
// state pertaining to the AdbcDatbase. The private_data member of an
// AdbcDatabase initialized by the driver will be a pointer to the
// subclass of DatbaseObjectBase.
class DatabaseObjectBase : public ObjectBase {
 public:
  // (there are no database functions other than option getting/setting)
};

// Driver authors can subclass ConnectionObjectBase to track driver-specific
// state pertaining to the AdbcConnection. The private_data member of an
// AdbcConnection initialized by the driver will be a pointer to the
// subclass of ConnectionObjectBase. Driver authors can override methods to
// implement the corresponding ConnectionXXX driver methods.
class ConnectionObjectBase : public ObjectBase {
 public:
  virtual AdbcStatusCode Commit(AdbcError* error) { return ADBC_STATUS_NOT_IMPLEMENTED; }

  virtual AdbcStatusCode GetInfo(const uint32_t* info_codes, size_t info_codes_length,
                                 ArrowArrayStream* out, AdbcError* error) {
    return ADBC_STATUS_NOT_IMPLEMENTED;
  }

  virtual AdbcStatusCode GetObjects(int depth, const char* catalog, const char* db_schema,
                                    const char* table_name, const char** table_type,
                                    const char* column_name, ArrowArrayStream* out,
                                    AdbcError* error) {
    return ADBC_STATUS_NOT_IMPLEMENTED;
  }

  virtual AdbcStatusCode GetTableSchema(const char* catalog, const char* db_schema,
                                        const char* table_name, ArrowSchema* schema,
                                        AdbcError* error) {
    return ADBC_STATUS_NOT_IMPLEMENTED;
  }

  virtual AdbcStatusCode GetTableTypes(ArrowArrayStream* out, AdbcError* error) {
    return ADBC_STATUS_NOT_IMPLEMENTED;
  }

  virtual AdbcStatusCode ReadPartition(const uint8_t* serialized_partition,
                                       size_t serialized_length, ArrowArrayStream* out,
                                       AdbcError* error) {
    return ADBC_STATUS_NOT_IMPLEMENTED;
  }

  virtual AdbcStatusCode Rollback(AdbcError* error) {
    return ADBC_STATUS_NOT_IMPLEMENTED;
  }

  virtual AdbcStatusCode Cancel(AdbcError* error) { return ADBC_STATUS_NOT_IMPLEMENTED; }

  virtual AdbcStatusCode GetStatistics(const char* catalog, const char* db_schema,
                                       const char* table_name, char approximate,
                                       ArrowArrayStream* out, AdbcError* error) {
    return ADBC_STATUS_NOT_IMPLEMENTED;
  }

  virtual AdbcStatusCode GetStatisticNames(ArrowArrayStream* out, AdbcError* error) {
    return ADBC_STATUS_NOT_IMPLEMENTED;
  }
};

// Driver authors can subclass StatementObjectBase to track driver-specific
// state pertaining to the AdbcStatement. The private_data member of an
// AdbcStatement initialized by the driver will be a pointer to the
// subclass of StatementObjectBase. Driver authors can override methods to
// implement the corresponding StatementXXX driver methods.
class StatementObjectBase : public ObjectBase {
 public:
  virtual AdbcStatusCode ExecuteQuery(ArrowArrayStream* stream, int64_t* rows_affected,
                                      AdbcError* error) {
    return ADBC_STATUS_NOT_IMPLEMENTED;
  }

  virtual AdbcStatusCode ExecuteSchema(ArrowSchema* schema, AdbcError* error) {
    return ADBC_STATUS_NOT_IMPLEMENTED;
  }

  virtual AdbcStatusCode Prepare(AdbcError* error) { return ADBC_STATUS_NOT_IMPLEMENTED; }

  virtual AdbcStatusCode SetSqlQuery(const char* query, AdbcError* error) {
    return ADBC_STATUS_NOT_IMPLEMENTED;
  }

  virtual AdbcStatusCode SetSubstraitPlan(const uint8_t* plan, size_t length,
                                          AdbcError* error) {
    return ADBC_STATUS_NOT_IMPLEMENTED;
  }

  virtual AdbcStatusCode Bind(ArrowArray* values, ArrowSchema* schema, AdbcError* error) {
    return ADBC_STATUS_NOT_IMPLEMENTED;
  }

  virtual AdbcStatusCode BindStream(ArrowArrayStream* stream, AdbcError* error) {
    return ADBC_STATUS_NOT_IMPLEMENTED;
  }

  virtual AdbcStatusCode Cancel(AdbcError* error) { return ADBC_STATUS_NOT_IMPLEMENTED; }
};

// Driver authors can declare a template specialization of the Driver class
// and use it to provide their driver init function. It is possible, but
// rarely useful, to subclass a driver.
template <typename DatabaseT, typename ConnectionT, typename StatementT>
class Driver {
 public:
  static AdbcStatusCode Init(int version, void* raw_driver, AdbcError* error) {
    if (version != ADBC_VERSION_1_1_0) return ADBC_STATUS_NOT_IMPLEMENTED;
    AdbcDriver* driver = (AdbcDriver*)raw_driver;
    std::memset(driver, 0, sizeof(AdbcDriver));

    // Driver lifecycle
    driver->private_data = new Driver();
    driver->release = &CDriverRelease;

    // Driver functions
    driver->ErrorGetDetailCount = &CErrorGetDetailCount;
    driver->ErrorGetDetail = &CErrorGetDetail;

    // Database lifecycle
    driver->DatabaseNew = &CNew<AdbcDatabase, DatabaseT>;
    driver->DatabaseInit = &CDatabaseInit;
    driver->DatabaseRelease = &CRelease<AdbcDatabase, DatabaseT>;

    // Database functions
    driver->DatabaseSetOption = &CSetOption<AdbcDatabase, DatabaseT>;
    driver->DatabaseSetOptionBytes = &CSetOptionBytes<AdbcDatabase, DatabaseT>;
    driver->DatabaseSetOptionInt = &CSetOptionInt<AdbcDatabase, DatabaseT>;
    driver->DatabaseSetOptionDouble = &CSetOptionDouble<AdbcDatabase, DatabaseT>;
    driver->DatabaseGetOption = &CGetOption<AdbcDatabase, DatabaseT>;
    driver->DatabaseGetOptionBytes = &CGetOptionBytes<AdbcDatabase, DatabaseT>;
    driver->DatabaseGetOptionInt = &CGetOptionInt<AdbcDatabase, DatabaseT>;
    driver->DatabaseGetOptionDouble = &CGetOptionDouble<AdbcDatabase, DatabaseT>;

    // Connection lifecycle
    driver->ConnectionNew = &CNew<AdbcConnection, ConnectionT>;
    driver->ConnectionInit = &CConnectionInit;
    driver->ConnectionRelease = &CRelease<AdbcConnection, ConnectionT>;

    // Connection functions
    driver->ConnectionSetOption = &CSetOption<AdbcConnection, ConnectionT>;
    driver->ConnectionSetOptionBytes = &CSetOptionBytes<AdbcConnection, ConnectionT>;
    driver->ConnectionSetOptionInt = &CSetOptionInt<AdbcConnection, ConnectionT>;
    driver->ConnectionSetOptionDouble = &CSetOptionDouble<AdbcConnection, ConnectionT>;
    driver->ConnectionGetOption = &CGetOption<AdbcConnection, ConnectionT>;
    driver->ConnectionGetOptionBytes = &CGetOptionBytes<AdbcConnection, ConnectionT>;
    driver->ConnectionGetOptionInt = &CGetOptionInt<AdbcConnection, ConnectionT>;
    driver->ConnectionGetOptionDouble = &CGetOptionDouble<AdbcConnection, ConnectionT>;
    driver->ConnectionCommit = &CConnectionCommit;
    driver->ConnectionGetInfo = &CConnectionGetInfo;
    driver->ConnectionGetObjects = &CConnectionGetObjects;
    driver->ConnectionGetTableSchema = &CConnectionGetTableSchema;
    driver->ConnectionGetTableTypes = &CConnectionGetTableTypes;
    driver->ConnectionReadPartition = &CConnectionReadPartition;
    driver->ConnectionRollback = &CConnectionRollback;
    driver->ConnectionCancel = &CConnectionCancel;
    driver->ConnectionGetStatistics = &CConnectionGetStatistics;
    driver->ConnectionGetStatisticNames = &CConnectionGetStatisticNames;

    // Statement lifecycle
    driver->StatementNew = &CStatementNew;
    driver->StatementRelease = &CRelease<AdbcStatement, StatementT>;

    // Statement functions
    driver->StatementSetOption = &CSetOption<AdbcStatement, StatementT>;
    driver->StatementSetOptionBytes = &CSetOptionBytes<AdbcStatement, StatementT>;
    driver->StatementSetOptionInt = &CSetOptionInt<AdbcStatement, StatementT>;
    driver->StatementSetOptionDouble = &CSetOptionDouble<AdbcStatement, StatementT>;
    driver->StatementGetOption = &CGetOption<AdbcStatement, StatementT>;
    driver->StatementGetOptionBytes = &CGetOptionBytes<AdbcStatement, StatementT>;
    driver->StatementGetOptionInt = &CGetOptionInt<AdbcStatement, StatementT>;
    driver->StatementGetOptionDouble = &CGetOptionDouble<AdbcStatement, StatementT>;

    driver->StatementExecuteQuery = &CStatementExecuteQuery;
    driver->StatementExecuteSchema = &CStatementExecuteSchema;
    driver->StatementPrepare = &CStatementPrepare;
    driver->StatementSetSqlQuery = &CStatementSetSqlQuery;
    driver->StatementSetSubstraitPlan = &CStatementSetSubstraitPlan;
    driver->StatementBind = &CStatementBind;
    driver->StatementBindStream = &CStatementBindStream;
    driver->StatementCancel = &CStatementCancel;

    return ADBC_STATUS_OK;
  }

 private:
  // Driver trampolines
  static AdbcStatusCode CDriverRelease(AdbcDriver* driver, AdbcError* error) {
    auto driver_private = reinterpret_cast<Driver*>(driver->private_data);
    delete driver_private;
    driver->private_data = nullptr;
    return ADBC_STATUS_OK;
  }

  static int CErrorGetDetailCount(const AdbcError* error) {
    if (error->vendor_code != ADBC_ERROR_VENDOR_CODE_PRIVATE_DATA) {
      return 0;
    }

    auto error_obj = reinterpret_cast<Error*>(error->private_data);
    return error_obj->CDetailCount();
  }

  static AdbcErrorDetail CErrorGetDetail(const AdbcError* error, int index) {
    auto error_obj = reinterpret_cast<Error*>(error->private_data);
    return error_obj->CDetail(index);
  }

  // Templatable trampolines
  template <typename T, typename ObjectT>
  static AdbcStatusCode CNew(T* obj, AdbcError* error) {
    auto private_data = new ObjectT();
    obj->private_data = private_data;
    return ADBC_STATUS_OK;
  }

  template <typename T, typename ObjectT>
  static AdbcStatusCode CRelease(T* obj, AdbcError* error) {
    auto private_data = reinterpret_cast<ObjectT*>(obj->private_data);
    AdbcStatusCode result = private_data->Release(error);
    if (result != ADBC_STATUS_OK) {
      return result;
    }

    delete private_data;
    obj->private_data = nullptr;
    return ADBC_STATUS_OK;
  }

  template <typename T, typename ObjectT>
  static AdbcStatusCode CSetOption(T* obj, const char* key, const char* value,
                                   AdbcError* error) {
    auto private_data = reinterpret_cast<ObjectT*>(obj->private_data);
    return private_data->template CSetOption<>(key, value, error);
  }

  template <typename T, typename ObjectT>
  static AdbcStatusCode CSetOptionBytes(T* obj, const char* key, const uint8_t* value,
                                        size_t length, AdbcError* error) {
    auto private_data = reinterpret_cast<ObjectT*>(obj->private_data);
    return private_data->CSetOptionBytes(key, value, length, error);
  }

  template <typename T, typename ObjectT>
  static AdbcStatusCode CSetOptionInt(T* obj, const char* key, int64_t value,
                                      AdbcError* error) {
    auto private_data = reinterpret_cast<ObjectT*>(obj->private_data);
    return private_data->template CSetOption<>(key, value, error);
  }

  template <typename T, typename ObjectT>
  static AdbcStatusCode CSetOptionDouble(T* obj, const char* key, double value,
                                         AdbcError* error) {
    auto private_data = reinterpret_cast<ObjectT*>(obj->private_data);
    return private_data->template CSetOption<>(key, value, error);
  }

  template <typename T, typename ObjectT>
  static AdbcStatusCode CGetOption(T* obj, const char* key, char* value, size_t* length,
                                   AdbcError* error) {
    auto private_data = reinterpret_cast<ObjectT*>(obj->private_data);
    return private_data->template CGetOptionStringLike<>(key, value, length, error);
  }

  template <typename T, typename ObjectT>
  static AdbcStatusCode CGetOptionBytes(T* obj, const char* key, uint8_t* value,
                                        size_t* length, AdbcError* error) {
    auto private_data = reinterpret_cast<ObjectT*>(obj->private_data);
    return private_data->template CGetOptionStringLike<>(key, value, length, error);
  }

  template <typename T, typename ObjectT>
  static AdbcStatusCode CGetOptionInt(T* obj, const char* key, int64_t* value,
                                      AdbcError* error) {
    auto private_data = reinterpret_cast<ObjectT*>(obj->private_data);
    return private_data->template CGetOptionNumeric<>(key, value, error);
  }

  template <typename T, typename ObjectT>
  static AdbcStatusCode CGetOptionDouble(T* obj, const char* key, double* value,
                                         AdbcError* error) {
    auto private_data = reinterpret_cast<ObjectT*>(obj->private_data);
    return private_data->template CGetOptionNumeric<>(key, value, error);
  }

  // Database trampolines
  static AdbcStatusCode CDatabaseInit(AdbcDatabase* database, AdbcError* error) {
    auto private_data = reinterpret_cast<DatabaseT*>(database->private_data);
    private_data->set_driver(database->private_driver);
    return private_data->Init(database->private_driver->private_data, error);
  }

  // Connection trampolines
  static AdbcStatusCode CConnectionInit(AdbcConnection* connection,
                                        AdbcDatabase* database, AdbcError* error) {
    auto private_data = reinterpret_cast<ConnectionT*>(connection->private_data);
    private_data->set_driver(connection->private_driver);
    return private_data->Init(database->private_data, error);
  }

  static AdbcStatusCode CConnectionCancel(AdbcConnection* connection, AdbcError* error) {
    auto private_data = reinterpret_cast<ConnectionT*>(connection->private_data);
    return private_data->Cancel(error);
  }

  static AdbcStatusCode CConnectionGetInfo(AdbcConnection* connection,
                                           const uint32_t* info_codes,
                                           size_t info_codes_length,
                                           ArrowArrayStream* out, AdbcError* error) {
    auto private_data = reinterpret_cast<ConnectionT*>(connection->private_data);
    return private_data->GetInfo(info_codes, info_codes_length, out, error);
  }

  static AdbcStatusCode CConnectionGetObjects(AdbcConnection* connection, int depth,
                                              const char* catalog, const char* db_schema,
                                              const char* table_name,
                                              const char** table_type,
                                              const char* column_name,
                                              ArrowArrayStream* out, AdbcError* error) {
    auto private_data = reinterpret_cast<ConnectionT*>(connection->private_data);
    return private_data->GetObjects(depth, catalog, db_schema, table_name, table_type,
                                    column_name, out, error);
  }

  static AdbcStatusCode CConnectionGetStatistics(
      AdbcConnection* connection, const char* catalog, const char* db_schema,
      const char* table_name, char approximate, ArrowArrayStream* out, AdbcError* error) {
    auto private_data = reinterpret_cast<ConnectionT*>(connection->private_data);
    return private_data->GetStatistics(catalog, db_schema, table_name, approximate, out,
                                       error);
  }

  static AdbcStatusCode CConnectionGetStatisticNames(AdbcConnection* connection,
                                                     ArrowArrayStream* out,
                                                     AdbcError* error) {
    auto private_data = reinterpret_cast<ConnectionT*>(connection->private_data);
    return private_data->GetStatisticNames(out, error);
  }

  static AdbcStatusCode CConnectionGetTableSchema(AdbcConnection* connection,
                                                  const char* catalog,
                                                  const char* db_schema,
                                                  const char* table_name,
                                                  ArrowSchema* schema, AdbcError* error) {
    auto private_data = reinterpret_cast<ConnectionT*>(connection->private_data);
    return private_data->GetTableSchema(catalog, db_schema, table_name, schema, error);
  }

  static AdbcStatusCode CConnectionGetTableTypes(AdbcConnection* connection,
                                                 ArrowArrayStream* out,
                                                 AdbcError* error) {
    auto private_data = reinterpret_cast<ConnectionT*>(connection->private_data);
    return private_data->GetTableTypes(out, error);
  }

  static AdbcStatusCode CConnectionReadPartition(AdbcConnection* connection,
                                                 const uint8_t* serialized_partition,
                                                 size_t serialized_length,
                                                 ArrowArrayStream* out,
                                                 AdbcError* error) {
    auto private_data = reinterpret_cast<ConnectionT*>(connection->private_data);
    return private_data->ReadPartition(serialized_partition, serialized_length, out,
                                       error);
  }

  static AdbcStatusCode CConnectionCommit(AdbcConnection* connection, AdbcError* error) {
    auto private_data = reinterpret_cast<ConnectionT*>(connection->private_data);
    return private_data->Commit(error);
  }

  static AdbcStatusCode CConnectionRollback(AdbcConnection* connection,
                                            AdbcError* error) {
    auto private_data = reinterpret_cast<ConnectionT*>(connection->private_data);
    return private_data->Rollback(error);
  }

  // Statement trampolines
  static AdbcStatusCode CStatementNew(AdbcConnection* connection,
                                      AdbcStatement* statement, AdbcError* error) {
    auto private_data = new StatementT();
    private_data->set_driver(connection->private_driver);
    AdbcStatusCode status = private_data->Init(connection->private_data, error);
    if (status != ADBC_STATUS_OK) {
      delete private_data;
    }

    statement->private_data = private_data;
    return ADBC_STATUS_OK;
  }

  static AdbcStatusCode CStatementExecuteQuery(AdbcStatement* statement,
                                               ArrowArrayStream* stream,
                                               int64_t* rows_affected, AdbcError* error) {
    auto private_data = reinterpret_cast<StatementT*>(statement->private_data);
    return private_data->ExecuteQuery(stream, rows_affected, error);
  }

  static AdbcStatusCode CStatementExecuteSchema(AdbcStatement* statement,
                                                ArrowSchema* schema, AdbcError* error) {
    auto private_data = reinterpret_cast<StatementT*>(statement->private_data);
    return private_data->ExecuteSchema(schema, error);
  }

  static AdbcStatusCode CStatementPrepare(AdbcStatement* statement, AdbcError* error) {
    auto private_data = reinterpret_cast<StatementT*>(statement->private_data);
    return private_data->Prepare(error);
  }

  static AdbcStatusCode CStatementSetSqlQuery(AdbcStatement* statement, const char* query,
                                              AdbcError* error) {
    auto private_data = reinterpret_cast<StatementT*>(statement->private_data);
    return private_data->SetSqlQuery(query, error);
  }

  static AdbcStatusCode CStatementSetSubstraitPlan(AdbcStatement* statement,
                                                   const uint8_t* plan, size_t length,
                                                   AdbcError* error) {
    auto private_data = reinterpret_cast<StatementT*>(statement->private_data);
    return private_data->SetSubstraitPlan(plan, length, error);
  }

  static AdbcStatusCode CStatementBind(AdbcStatement* statement, ArrowArray* values,
                                       ArrowSchema* schema, AdbcError* error) {
    auto private_data = reinterpret_cast<StatementT*>(statement->private_data);
    return private_data->Bind(values, schema, error);
  }

  static AdbcStatusCode CStatementBindStream(AdbcStatement* statement,
                                             ArrowArrayStream* stream, AdbcError* error) {
    auto private_data = reinterpret_cast<StatementT*>(statement->private_data);
    return private_data->BindStream(stream, error);
  }

  static AdbcStatusCode CStatementCancel(AdbcStatement* statement, AdbcError* error) {
    auto private_data = reinterpret_cast<StatementT*>(statement->private_data);
    return private_data->Cancel(error);
  }
};

}  // namespace common

}  // namespace adbc
