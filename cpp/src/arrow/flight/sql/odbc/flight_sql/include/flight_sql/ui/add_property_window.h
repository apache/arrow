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

// platform.h needs to be included before custom_window.h due to windows.h conflicts
#include "arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/platform.h"

#include "arrow/flight/sql/odbc/flight_sql/include/flight_sql/ui/custom_window.h"

namespace driver {
namespace flight_sql {
namespace config {
/**
 * Add property window class.
 */
class AddPropertyWindow : public CustomWindow {
  /**
   * Children windows ids.
   */
  struct ChildId {
    enum Type {
      KEY_EDIT = 100,
      KEY_LABEL,
      VALUE_EDIT,
      VALUE_LABEL,
      OK_BUTTON,
      CANCEL_BUTTON
    };
  };

 public:
  /**
   * Constructor.
   *
   * @param parent Parent window handle.
   */
  explicit AddPropertyWindow(Window* parent);

  /**
   * Destructor.
   */
  virtual ~AddPropertyWindow();

  /**
   * Create window in the center of the parent window.
   */
  void Create();

  void OnCreate() override;

  bool OnMessage(UINT msg, WPARAM wparam, LPARAM lparam) override;

  /**
   * Get the property from the dialog.
   *
   * @return true if the dialog was OK'd, false otherwise.
   */
  bool GetProperty(std::wstring& key, std::wstring& value);

 private:
  /**
   * Create property edit boxes.
   *
   * @param pos_x X position.
   * @param pos_y Y position.
   * @param size_x Width.
   * @return Size by Y.
   */
  int CreateEdits(int pos_x, int pos_y, int size_x);

  void CheckEnableOk();

  std::vector<std::unique_ptr<Window> > labels_;

  /** Ok button. */
  std::unique_ptr<Window> ok_button_;

  /** Cancel button. */
  std::unique_ptr<Window> cancel_button_;

  std::unique_ptr<Window> key_edit_;

  std::unique_ptr<Window> value_edit_;

  std::wstring key_;

  std::wstring value_;

  /** Window width. */
  int width_;

  /** Window height. */
  int height_;

  /** Flag indicating whether OK option was selected. */
  bool accepted_;

  bool is_initialized_;
};

}  // namespace config
}  // namespace flight_sql
}  // namespace driver
