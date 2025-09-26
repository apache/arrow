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

#include "config/configuration.h"
#include "ui/custom_window.h"

namespace arrow::flight::sql::odbc {
namespace config {
/**
 * DSN configuration window class.
 */
class DsnConfigurationWindow : public CustomWindow {
  /**
   * Children windows ids.
   */
  struct ChildId {
    enum Type {
      CONNECTION_SETTINGS_GROUP_BOX = 100,
      AUTH_SETTINGS_GROUP_BOX,
      ENCRYPTION_SETTINGS_GROUP_BOX,
      NAME_EDIT,
      NAME_LABEL,
      SERVER_EDIT,
      SERVER_LABEL,
      PORT_EDIT,
      PORT_LABEL,
      AUTH_TYPE_LABEL,
      AUTH_TYPE_COMBOBOX,
      USER_LABEL,
      USER_EDIT,
      PASSWORD_LABEL,
      PASSWORD_EDIT,
      AUTH_TOKEN_LABEL,
      AUTH_TOKEN_EDIT,
      ENABLE_ENCRYPTION_LABEL,
      ENABLE_ENCRYPTION_CHECKBOX,
      CERTIFICATE_LABEL,
      CERTIFICATE_EDIT,
      CERTIFICATE_BROWSE_BUTTON,
      USE_SYSTEM_CERT_STORE_LABEL,
      USE_SYSTEM_CERT_STORE_CHECKBOX,
      DISABLE_CERT_VERIFICATION_LABEL,
      DISABLE_CERT_VERIFICATION_CHECKBOX,
      PROPERTY_GROUP_BOX,
      PROPERTY_LIST,
      ADD_BUTTON,
      DELETE_BUTTON,
      TAB_CONTROL,
      TEST_CONNECTION_BUTTON,
      OK_BUTTON,
      CANCEL_BUTTON
    };
  };

 public:
  /**
   * Constructor.
   *
   * @param parent Parent window handle.
   * @param config Configuration
   */
  DsnConfigurationWindow(Window* parent, config::Configuration& config);

  /**
   * Destructor.
   */
  virtual ~DsnConfigurationWindow();

  /**
   * Create window in the center of the parent window.
   */
  void Create();

  void OnCreate() override;

  bool OnMessage(UINT msg, WPARAM wparam, LPARAM lparam) override;

 private:
  /**
   * Create connection settings group box.
   *
   * @param pos_x X position.
   * @param pos_y Y position.
   * @param size_x Width.
   * @return Size by Y.
   */
  int CreateConnectionSettingsGroup(int pos_x, int pos_y, int size_x);

  /**
   * Create aythentication settings group box.
   *
   * @param pos_x X position.
   * @param pos_y Y position.
   * @param size_x Width.
   * @return Size by Y.
   */
  int CreateAuthSettingsGroup(int pos_x, int pos_y, int size_x);

  /**
   * Create Encryption settings group box.
   *
   * @param pos_x X position.
   * @param pos_y Y position.
   * @param size_x Width.
   * @return Size by Y.
   */
  int CreateEncryptionSettingsGroup(int pos_x, int pos_y, int size_x);

  /**
   * Create advanced properties group box.
   *
   * @param pos_x X position.
   * @param pos_y Y position.
   * @param size_x Width.
   * @return Size by Y.
   */
  int CreatePropertiesGroup(int pos_x, int pos_y, int size_x);

  void SelectTab(int tab_index);

  void CheckEnableOk();

  void CheckAuthType();

  void SaveParameters(Configuration& target_config);

  /** Window width. */
  int width_;

  /** Window height. */
  int height_;

  std::unique_ptr<Window> tab_control_;

  /** Connection settings group box. */
  std::unique_ptr<Window> connection_settings_group_box_;

  /** Authentication settings group box. */
  std::unique_ptr<Window> auth_settings_group_box_;

  /** Encryption settings group box. */
  std::unique_ptr<Window> encryption_settings_group_box_;

  std::vector<std::unique_ptr<Window> > labels_;

  /** Test button. */
  std::unique_ptr<Window> test_button_;

  /** Ok button. */
  std::unique_ptr<Window> ok_button_;

  /** Cancel button. */
  std::unique_ptr<Window> cancel_button_;

  /** DSN name edit field. */
  std::unique_ptr<Window> name_edit_;

  std::unique_ptr<Window> server_edit_;

  std::unique_ptr<Window> port_edit_;

  std::unique_ptr<Window> auth_type_combo_box_;

  /** User edit. */
  std::unique_ptr<Window> user_edit_;

  /** Password edit. */
  std::unique_ptr<Window> password_edit_;

  std::unique_ptr<Window> auth_token_edit_;

  std::unique_ptr<Window> enable_encryption_check_box_;

  std::unique_ptr<Window> certificate_edit_;

  std::unique_ptr<Window> certificate_browse_button_;

  std::unique_ptr<Window> use_system_cert_store_check_box_;

  std::unique_ptr<Window> disable_cert_verification_check_box_;

  std::unique_ptr<Window> property_group_box_;

  std::unique_ptr<Window> property_list_;

  std::unique_ptr<Window> add_button_;

  std::unique_ptr<Window> delete_button_;

  /** Configuration. */
  Configuration& config_;

  /** Flag indicating whether OK option was selected. */
  bool accepted_;

  bool is_initialized_;
};

}  // namespace config
}  // namespace arrow::flight::sql::odbc
