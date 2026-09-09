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

#include <sql.h>
#include <sqlext.h>

#include <cstdlib>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>

namespace {

std::string GetRequiredEnv(const char* name) {
  const char* value = std::getenv(name);
  if (value == nullptr || value[0] == '\0') {
    std::cerr << "missing required environment variable: " << name << '\n';
    std::exit(2);
  }
  return value;
}

std::string ReadSecret(const char* file_env_name, const char* value_env_name) {
  const char* path = std::getenv(file_env_name);
  if (path != nullptr && path[0] != '\0') {
    std::ifstream input(path);
    if (!input) {
      std::cerr << "cannot read " << file_env_name << '\n';
      std::exit(2);
    }
    std::ostringstream contents;
    contents << input.rdbuf();
    std::string password = contents.str();
    while (!password.empty() && (password.back() == '\n' || password.back() == '\r')) {
      password.pop_back();
    }
    if (password.empty()) {
      std::cerr << file_env_name << " is empty\n";
      std::exit(2);
    }
    return password;
  }
  return GetRequiredEnv(value_env_name);
}

std::string Redact(std::string message, const std::string& uid,
                   const std::string& password) {
  for (const auto& secret : {uid, password}) {
    if (secret.empty()) continue;
    for (std::string::size_type position = 0;
         (position = message.find(secret, position)) != std::string::npos;) {
      message.replace(position, secret.size(), "<redacted>");
      position += sizeof("<redacted>") - 1;
    }
  }
  return message;
}

void PrintDiagnostics(SQLSMALLINT handle_type, SQLHANDLE handle, const std::string& uid,
                      const std::string& password) {
  SQLCHAR state[6] = {};
  SQLCHAR message[2048] = {};
  SQLINTEGER native_error = 0;
  SQLSMALLINT message_length = 0;
  for (SQLSMALLINT record = 1;
       SQL_SUCCEEDED(SQLGetDiagRec(handle_type, handle, record, state, &native_error,
                                   message, sizeof(message), &message_length));
       ++record) {
    std::string safe_message(reinterpret_cast<char*>(message), message_length);
    std::cerr << "ODBC diagnostic: state=" << state << " native=" << native_error
              << " message=" << Redact(safe_message, uid, password) << '\n';
  }
}

bool Check(SQLRETURN result, const char* operation, SQLSMALLINT handle_type,
           SQLHANDLE handle, const std::string& uid, const std::string& password) {
  if (SQL_SUCCEEDED(result)) {
    if (result == SQL_SUCCESS_WITH_INFO) {
      PrintDiagnostics(handle_type, handle, uid, password);
    }
    return true;
  }
  std::cerr << operation << " failed with SQLRETURN=" << result << '\n';
  if (handle != SQL_NULL_HANDLE) {
    PrintDiagnostics(handle_type, handle, uid, password);
  }
  return false;
}

bool Cleanup(SQLHENV env, SQLHDBC connection, SQLHSTMT statement, bool connected,
             const std::string& uid, const std::string& password) {
  bool ok = true;
  if (statement != SQL_NULL_HSTMT) {
    SQLRETURN result = SQLCloseCursor(statement);
    if (result != SQL_SUCCESS && result != SQL_SUCCESS_WITH_INFO &&
        result != SQL_NO_DATA) {
      ok = Check(result, "SQLCloseCursor", SQL_HANDLE_STMT, statement, uid, password) &&
           ok;
    } else {
      std::cout << "cleanup: close cursor: success\n";
    }
    result = SQLFreeHandle(SQL_HANDLE_STMT, statement);
    if (!SQL_SUCCEEDED(result)) {
      std::cerr << "cleanup: free statement: failed with SQLRETURN=" << result << '\n';
      ok = false;
    } else {
      std::cout << "cleanup: free statement: success\n";
    }
  }
  if (connection != SQL_NULL_HDBC) {
    if (connected) {
      SQLRETURN result = SQLDisconnect(connection);
      if (!SQL_SUCCEEDED(result)) {
        ok = Check(result, "SQLDisconnect", SQL_HANDLE_DBC, connection, uid, password) &&
             ok;
      } else {
        std::cout << "cleanup: disconnect: success\n";
      }
    }
    SQLRETURN result = SQLFreeHandle(SQL_HANDLE_DBC, connection);
    if (!SQL_SUCCEEDED(result)) {
      std::cerr << "cleanup: free connection: failed with SQLRETURN=" << result << '\n';
      ok = false;
    } else {
      std::cout << "cleanup: free connection: success\n";
    }
  }
  if (env != SQL_NULL_HENV) {
    SQLRETURN result = SQLFreeHandle(SQL_HANDLE_ENV, env);
    if (!SQL_SUCCEEDED(result)) {
      std::cerr << "cleanup: free environment: failed with SQLRETURN=" << result << '\n';
      ok = false;
    } else {
      std::cout << "cleanup: free environment: success\n";
    }
  }
  return ok;
}

}  // namespace

int main() {
  const char* token_file = std::getenv("DREMIO_ODBC_TOKEN_FILE");
  const char* token_value = std::getenv("DREMIO_ODBC_TOKEN");
  const bool token_auth = (token_file != nullptr && token_file[0] != '\0') ||
                          (token_value != nullptr && token_value[0] != '\0');
  const std::string uid = token_auth ? "" : GetRequiredEnv("DREMIO_ODBC_UID");
  const std::string password =
      token_auth ? ReadSecret("DREMIO_ODBC_TOKEN_FILE", "DREMIO_ODBC_TOKEN")
                 : ReadSecret("DREMIO_ODBC_PASSWORD_FILE", "DREMIO_ODBC_PASSWORD");
  const char* host_value = std::getenv("DREMIO_ODBC_HOST");
  const char* port_value = std::getenv("DREMIO_ODBC_PORT");
  const std::string host = host_value != nullptr ? host_value : "data.eu.dremio.cloud";
  const std::string port = port_value != nullptr ? port_value : "443";

  const std::string authentication =
      token_auth ? ";Token=" + password : ";UID=" + uid + ";PWD=" + password;
  const std::string connection_string =
      "Driver={Apache Arrow Flight SQL ODBC Driver};Host=" + host + ";Port=" + port +
      authentication +
      ";useEncryption=true;disableCertificateVerification=false;"
      "useSystemTrustStore=true;useWideChar=false;";

  SQLHENV env = SQL_NULL_HENV;
  SQLHDBC connection = SQL_NULL_HDBC;
  SQLHSTMT statement = SQL_NULL_HSTMT;

  if (!Check(SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env), "SQLAllocHandle(env)",
             SQL_HANDLE_ENV, env, uid, password) ||
      !Check(SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION,
                           reinterpret_cast<SQLPOINTER>(SQL_OV_ODBC3), 0),
             "SQLSetEnvAttr", SQL_HANDLE_ENV, env, uid, password) ||
      !Check(SQLAllocHandle(SQL_HANDLE_DBC, env, &connection),
             "SQLAllocHandle(connection)", SQL_HANDLE_ENV, env, uid, password)) {
    Cleanup(env, connection, statement, false, uid, password);
    return 1;
  }

  SQLCHAR completed_connection[4096] = {};
  SQLSMALLINT completed_length = 0;
  SQLRETURN result = SQLDriverConnect(
      connection, nullptr,
      reinterpret_cast<SQLCHAR*>(const_cast<char*>(connection_string.c_str())), SQL_NTS,
      completed_connection, sizeof(completed_connection), &completed_length,
      SQL_DRIVER_NOPROMPT);
  if (!Check(result, "SQLDriverConnect", SQL_HANDLE_DBC, connection, uid, password)) {
    Cleanup(env, connection, statement, false, uid, password);
    return 1;
  }
  std::cout << "connection: success (TLS certificate verification enabled)\n";

  if (!Check(SQLAllocHandle(SQL_HANDLE_STMT, connection, &statement),
             "SQLAllocHandle(statement)", SQL_HANDLE_DBC, connection, uid, password) ||
      !Check(SQLExecDirect(statement,
                           reinterpret_cast<SQLCHAR*>(const_cast<char*>("SELECT 1")),
                           SQL_NTS),
             "SQLExecDirect", SQL_HANDLE_STMT, statement, uid, password) ||
      !Check(SQLFetch(statement), "SQLFetch", SQL_HANDLE_STMT, statement, uid,
             password)) {
    Cleanup(env, connection, statement, true, uid, password);
    return 1;
  }

  SQLINTEGER value = 0;
  SQLLEN indicator = 0;
  if (!Check(SQLGetData(statement, 1, SQL_C_SLONG, &value, sizeof(value), &indicator),
             "SQLGetData", SQL_HANDLE_STMT, statement, uid, password)) {
    Cleanup(env, connection, statement, true, uid, password);
    return 1;
  }
  std::cout << "query: SELECT 1\nresult: " << value << '\n';
  if (indicator == SQL_NULL_DATA || value != 1) {
    std::cerr << "unexpected query result\n";
    Cleanup(env, connection, statement, true, uid, password);
    return 1;
  }

  const bool cleanup_ok = Cleanup(env, connection, statement, true, uid, password);
  std::cout << "smoke test: " << (cleanup_ok ? "PASS" : "FAIL") << '\n';
  return cleanup_ok ? 0 : 1;
}
