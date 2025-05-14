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

  bool OnMessage(UINT msg, WPARAM wParam, LPARAM lParam) override;

  /**
   * Get the property from the dialog.
   *
   * @return true if the dialog was OK'd, false otherwise.
   */
  bool GetProperty(std::string& key, std::string& value);

 private:
  /**
   * Create property edit boxes.
   *
   * @param posX X position.
   * @param posY Y position.
   * @param sizeX Width.
   * @return Size by Y.
   */
  int CreateEdits(int posX, int posY, int sizeX);

  void CheckEnableOk();

  std::vector<std::unique_ptr<Window> > labels;

  /** Ok button. */
  std::unique_ptr<Window> okButton;

  /** Cancel button. */
  std::unique_ptr<Window> cancelButton;

  std::unique_ptr<Window> keyEdit;

  std::unique_ptr<Window> valueEdit;

  std::string key;

  std::string value;

  /** Window width. */
  int width;

  /** Window height. */
  int height;

  /** Flag indicating whether OK option was selected. */
  bool accepted;

  bool isInitialized;
};

}  // namespace config
}  // namespace flight_sql
}  // namespace driver
