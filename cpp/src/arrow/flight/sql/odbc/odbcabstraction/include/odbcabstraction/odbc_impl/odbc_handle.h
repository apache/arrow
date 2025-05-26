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

#pragma once

#include <arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/diagnostics.h>
#include <arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/platform.h>
#include <sql.h>
#include <sqltypes.h>
#include <functional>
#include <mutex>

/**
 * @brief An abstraction over a generic ODBC handle.
 */
namespace ODBC {

template <typename Derived>
class ODBCHandle {
 public:
  inline driver::odbcabstraction::Diagnostics& GetDiagnostics() {
    return static_cast<Derived*>(this)->GetDiagnostics_Impl();
  }

  inline driver::odbcabstraction::Diagnostics& GetDiagnostics_Impl() {
    throw std::runtime_error("Illegal state -- diagnostics requested on invalid handle");
  }

  template <typename Function>
  inline SQLRETURN execute(SQLRETURN rc, Function function) {
    try {
      GetDiagnostics().Clear();
      rc = function();
    } catch (const driver::odbcabstraction::DriverException& ex) {
      GetDiagnostics().AddError(ex);
    } catch (const std::bad_alloc& ex) {
      GetDiagnostics().AddError(driver::odbcabstraction::DriverException(
          "A memory allocation error occurred.", "HY001"));
    } catch (const std::exception& ex) {
      GetDiagnostics().AddError(driver::odbcabstraction::DriverException(ex.what()));
    } catch (...) {
      GetDiagnostics().AddError(
          driver::odbcabstraction::DriverException("An unknown error occurred."));
    }

    if (GetDiagnostics().HasError()) {
      return SQL_ERROR;
    }
    if (SQL_SUCCEEDED(rc) && GetDiagnostics().HasWarning()) {
      return SQL_SUCCESS_WITH_INFO;
    }
    return rc;
  }

  template <typename Function>
  inline SQLRETURN executeWithLock(SQLRETURN rc, Function function) {
    const std::lock_guard<std::mutex> lock(mtx_);
    return execute(rc, function);
  }

  template <typename Function, bool SHOULD_LOCK = true>
  static inline SQLRETURN ExecuteWithDiagnostics(SQLHANDLE handle, SQLRETURN rc,
                                                 Function func) {
    if (!handle) {
      return SQL_INVALID_HANDLE;
    }
    if (SHOULD_LOCK) {
      return reinterpret_cast<Derived*>(handle)->executeWithLock(rc, func);
    } else {
      return reinterpret_cast<Derived*>(handle)->execute(rc, func);
    }
  }

  static Derived* of(SQLHANDLE handle) { return reinterpret_cast<Derived*>(handle); }

 private:
  std::mutex mtx_;
};
}  // namespace ODBC
