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

namespace driver {
namespace flight_sql {
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

  bool OnMessage(UINT msg, WPARAM wParam, LPARAM lParam) override;

 private:
  /**
   * Create connection settings group box.
   *
   * @param posX X position.
   * @param posY Y position.
   * @param sizeX Width.
   * @return Size by Y.
   */
  int CreateConnectionSettingsGroup(int posX, int posY, int sizeX);

  /**
   * Create aythentication settings group box.
   *
   * @param posX X position.
   * @param posY Y position.
   * @param sizeX Width.
   * @return Size by Y.
   */
  int CreateAuthSettingsGroup(int posX, int posY, int sizeX);

  /**
   * Create Encryption settings group box.
   *
   * @param posX X position.
   * @param posY Y position.
   * @param sizeX Width.
   * @return Size by Y.
   */
  int CreateEncryptionSettingsGroup(int posX, int posY, int sizeX);

  /**
   * Create advanced properties group box.
   *
   * @param posX X position.
   * @param posY Y position.
   * @param sizeX Width.
   * @return Size by Y.
   */
  int CreatePropertiesGroup(int posX, int posY, int sizeX);

  void SelectTab(int tabIndex);

  void CheckEnableOk();

  void CheckAuthType();

  void SaveParameters(Configuration& targetConfig);

  /** Window width. */
  int width;

  /** Window height. */
  int height;

  std::unique_ptr<Window> tabControl;

  std::unique_ptr<Window> commonContent;

  std::unique_ptr<Window> advancedContent;

  /** Connection settings group box. */
  std::unique_ptr<Window> connectionSettingsGroupBox;

  /** Authentication settings group box. */
  std::unique_ptr<Window> authSettingsGroupBox;

  /** Encryption settings group box. */
  std::unique_ptr<Window> encryptionSettingsGroupBox;

  std::vector<std::unique_ptr<Window> > labels;

  /** Test button. */
  std::unique_ptr<Window> testButton;

  /** Ok button. */
  std::unique_ptr<Window> okButton;

  /** Cancel button. */
  std::unique_ptr<Window> cancelButton;

  /** DSN name edit field. */
  std::unique_ptr<Window> nameEdit;

  std::unique_ptr<Window> serverEdit;

  std::unique_ptr<Window> portEdit;

  std::unique_ptr<Window> authTypeComboBox;

  /** User edit. */
  std::unique_ptr<Window> userEdit;

  /** Password edit. */
  std::unique_ptr<Window> passwordEdit;

  std::unique_ptr<Window> authTokenEdit;

  std::unique_ptr<Window> enableEncryptionCheckBox;

  std::unique_ptr<Window> certificateEdit;

  std::unique_ptr<Window> certificateBrowseButton;

  std::unique_ptr<Window> useSystemCertStoreCheckBox;

  std::unique_ptr<Window> disableCertVerificationCheckBox;

  std::unique_ptr<Window> propertyGroupBox;

  std::unique_ptr<Window> propertyList;

  std::unique_ptr<Window> addButton;

  std::unique_ptr<Window> deleteButton;

  /** Configuration. */
  Configuration& config;

  /** Flag indicating whether OK option was selected. */
  bool accepted;

  bool isInitialized;
};

}  // namespace config
}  // namespace flight_sql
}  // namespace driver
