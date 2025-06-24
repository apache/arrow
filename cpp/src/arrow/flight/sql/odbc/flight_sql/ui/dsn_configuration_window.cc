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

#include "arrow/flight/sql/odbc/flight_sql/include/flight_sql/ui/dsn_configuration_window.h"
#include "arrow/flight/sql/odbc/flight_sql/flight_sql_connection.h"

#include <Shlwapi.h>
#include <Windowsx.h>
#include <arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/utils.h>
#include <commctrl.h>
#include <commdlg.h>
#include <sql.h>
#include <sstream>

#include "arrow/flight/sql/odbc/flight_sql/include/flight_sql/ui/add_property_window.h"

#define COMMON_TAB 0
#define ADVANCED_TAB 1

namespace {
std::string TestConnection(const driver::flight_sql::config::Configuration& config) {
  std::unique_ptr<driver::flight_sql::FlightSqlConnection> flightSqlConn(
      new driver::flight_sql::FlightSqlConnection(driver::odbcabstraction::V_3));

  std::vector<std::string_view> missingProperties;
  flightSqlConn->Connect(config.GetProperties(), missingProperties);

  // This should have been checked before enabling the Test button.
  assert(missingProperties.empty());
  std::string serverName =
      boost::get<std::string>(flightSqlConn->GetInfo(SQL_SERVER_NAME));
  std::string serverVersion =
      boost::get<std::string>(flightSqlConn->GetInfo(SQL_DBMS_VER));
  return "Server Name: " + serverName + "\n" + "Server Version: " + serverVersion;
}
}  // namespace

namespace driver {
namespace flight_sql {
namespace config {

DsnConfigurationWindow::DsnConfigurationWindow(Window* parent,
                                               config::Configuration& config)
    : CustomWindow(parent, "FlightConfigureDSN", "Configure Apache Arrow Flight SQL"),
      width(480),
      height(375),
      config(config),
      accepted(false),
      isInitialized(false) {
  // No-op.
}

DsnConfigurationWindow::~DsnConfigurationWindow() {
  // No-op.
}

void DsnConfigurationWindow::Create() {
  // Finding out parent position.
  RECT parentRect;
  GetWindowRect(parent->GetHandle(), &parentRect);

  // Positioning window to the center of parent window.
  const int posX = parentRect.left + (parentRect.right - parentRect.left - width) / 2;
  const int posY = parentRect.top + (parentRect.bottom - parentRect.top - height) / 2;

  RECT desiredRect = {posX, posY, posX + width, posY + height};
  AdjustWindowRect(&desiredRect, WS_BORDER | WS_CAPTION | WS_SYSMENU | WS_THICKFRAME,
                   FALSE);

  Window::Create(WS_OVERLAPPED | WS_SYSMENU, desiredRect.left, desiredRect.top,
                 desiredRect.right - desiredRect.left,
                 desiredRect.bottom - desiredRect.top, 0);

  if (!handle) {
    std::stringstream buf;
    buf << "Can not create window, error code: " << GetLastError();
    throw odbcabstraction::DriverException(buf.str());
  }
}

void DsnConfigurationWindow::OnCreate() {
  tabControl = CreateTabControl(ChildId::TAB_CONTROL);
  tabControl->AddTab("Common", COMMON_TAB);
  tabControl->AddTab("Advanced", ADVANCED_TAB);

  int groupPosY = 3 * MARGIN;
  int groupSizeY = width - 2 * MARGIN;

  int commonGroupPosY = groupPosY;
  commonGroupPosY +=
      INTERVAL + CreateConnectionSettingsGroup(MARGIN, commonGroupPosY, groupSizeY);
  commonGroupPosY +=
      INTERVAL + CreateAuthSettingsGroup(MARGIN, commonGroupPosY, groupSizeY);

  int advancedGroupPosY = groupPosY;
  advancedGroupPosY +=
      INTERVAL + CreateEncryptionSettingsGroup(MARGIN, advancedGroupPosY, groupSizeY);
  advancedGroupPosY +=
      INTERVAL + CreatePropertiesGroup(MARGIN, advancedGroupPosY, groupSizeY);

  int testPosX = MARGIN;
  int cancelPosX = width - MARGIN - BUTTON_WIDTH;
  int okPosX = cancelPosX - INTERVAL - BUTTON_WIDTH;

  int buttonPosY = std::max(commonGroupPosY, advancedGroupPosY);
  testButton = CreateButton(testPosX, buttonPosY, BUTTON_WIDTH + 20, BUTTON_HEIGHT,
                            "Test Connection", ChildId::TEST_CONNECTION_BUTTON);
  okButton = CreateButton(okPosX, buttonPosY, BUTTON_WIDTH, BUTTON_HEIGHT, "Ok",
                          ChildId::OK_BUTTON);
  cancelButton = CreateButton(cancelPosX, buttonPosY, BUTTON_WIDTH, BUTTON_HEIGHT,
                              "Cancel", ChildId::CANCEL_BUTTON);
  isInitialized = true;
  CheckEnableOk();
  SelectTab(COMMON_TAB);
}

int DsnConfigurationWindow::CreateConnectionSettingsGroup(int posX, int posY, int sizeX) {
  enum { LABEL_WIDTH = 100 };

  const int labelPosX = posX + INTERVAL;

  const int editSizeX = sizeX - LABEL_WIDTH - 3 * INTERVAL;
  const int editPosX = labelPosX + LABEL_WIDTH + INTERVAL;

  int rowPos = posY + 2 * INTERVAL;

  const char* val = config.Get(FlightSqlConnection::DSN).c_str();
  labels.push_back(CreateLabel(labelPosX, rowPos, LABEL_WIDTH, ROW_HEIGHT,
                               "Data Source Name:", ChildId::NAME_LABEL));
  nameEdit = CreateEdit(editPosX, rowPos, editSizeX, ROW_HEIGHT, val, ChildId::NAME_EDIT);

  rowPos += INTERVAL + ROW_HEIGHT;

  val = config.Get(FlightSqlConnection::HOST).c_str();
  labels.push_back(CreateLabel(labelPosX, rowPos, LABEL_WIDTH, ROW_HEIGHT,
                               "Host Name:", ChildId::SERVER_LABEL));
  serverEdit =
      CreateEdit(editPosX, rowPos, editSizeX, ROW_HEIGHT, val, ChildId::SERVER_EDIT);

  rowPos += INTERVAL + ROW_HEIGHT;

  val = config.Get(FlightSqlConnection::PORT).c_str();
  labels.push_back(CreateLabel(labelPosX, rowPos, LABEL_WIDTH, ROW_HEIGHT,
                               "Port:", ChildId::PORT_LABEL));
  portEdit = CreateEdit(editPosX, rowPos, editSizeX, ROW_HEIGHT, val, ChildId::PORT_EDIT,
                        ES_NUMBER);

  rowPos += INTERVAL + ROW_HEIGHT;

  connectionSettingsGroupBox =
      CreateGroupBox(posX, posY, sizeX, rowPos - posY, "Connection settings",
                     ChildId::CONNECTION_SETTINGS_GROUP_BOX);

  return rowPos - posY;
}

int DsnConfigurationWindow::CreateAuthSettingsGroup(int posX, int posY, int sizeX) {
  enum { LABEL_WIDTH = 120 };

  const int labelPosX = posX + INTERVAL;

  const int editSizeX = sizeX - LABEL_WIDTH - 3 * INTERVAL;
  const int editPosX = labelPosX + LABEL_WIDTH + INTERVAL;

  int rowPos = posY + 2 * INTERVAL;

  labels.push_back(CreateLabel(labelPosX, rowPos, LABEL_WIDTH, ROW_HEIGHT,
                               "Authentication Type:", ChildId::AUTH_TYPE_LABEL));
  authTypeComboBox = CreateComboBox(editPosX, rowPos, editSizeX, ROW_HEIGHT,
                                    "Authentication Type:", ChildId::AUTH_TYPE_COMBOBOX);
  authTypeComboBox->AddString("Basic Authentication");
  authTypeComboBox->AddString("Token Authentication");

  rowPos += INTERVAL + ROW_HEIGHT;

  const char* val = config.Get(FlightSqlConnection::UID).c_str();

  labels.push_back(CreateLabel(labelPosX, rowPos, LABEL_WIDTH, ROW_HEIGHT,
                               "User:", ChildId::USER_LABEL));
  userEdit = CreateEdit(editPosX, rowPos, editSizeX, ROW_HEIGHT, val, ChildId::USER_EDIT);

  rowPos += INTERVAL + ROW_HEIGHT;

  val = config.Get(FlightSqlConnection::PWD).c_str();
  labels.push_back(CreateLabel(labelPosX, rowPos, LABEL_WIDTH, ROW_HEIGHT,
                               "Password:", ChildId::PASSWORD_LABEL));
  passwordEdit = CreateEdit(editPosX, rowPos, editSizeX, ROW_HEIGHT, val,
                            ChildId::USER_EDIT, ES_PASSWORD);

  rowPos += INTERVAL + ROW_HEIGHT;

  const auto& token = config.Get(FlightSqlConnection::TOKEN);
  val = token.c_str();
  labels.push_back(CreateLabel(labelPosX, rowPos, LABEL_WIDTH, ROW_HEIGHT,
                               "Authentication Token:", ChildId::AUTH_TOKEN_LABEL));
  authTokenEdit =
      CreateEdit(editPosX, rowPos, editSizeX, ROW_HEIGHT, val, ChildId::AUTH_TOKEN_EDIT);
  authTokenEdit->SetEnabled(false);

  // Ensure the right elements are selected.
  authTypeComboBox->SetSelection(token.empty() ? 0 : 1);
  CheckAuthType();

  rowPos += INTERVAL + ROW_HEIGHT;

  authSettingsGroupBox =
      CreateGroupBox(posX, posY, sizeX, rowPos - posY, "Authentication settings",
                     ChildId::AUTH_SETTINGS_GROUP_BOX);

  return rowPos - posY;
}

int DsnConfigurationWindow::CreateEncryptionSettingsGroup(int posX, int posY, int sizeX) {
  enum { LABEL_WIDTH = 120 };

  const int labelPosX = posX + INTERVAL;

  const int editSizeX = sizeX - LABEL_WIDTH - 3 * INTERVAL;
  const int editPosX = labelPosX + LABEL_WIDTH + INTERVAL;

  int rowPos = posY + 2 * INTERVAL;

  const char* val = config.Get(FlightSqlConnection::USE_ENCRYPTION).c_str();

  const bool enableEncryption = driver::odbcabstraction::AsBool(val).value_or(true);
  labels.push_back(CreateLabel(labelPosX, rowPos, LABEL_WIDTH, ROW_HEIGHT,
                               "Use Encryption:", ChildId::ENABLE_ENCRYPTION_LABEL));
  enableEncryptionCheckBox =
      CreateCheckBox(editPosX, rowPos - 2, editSizeX, ROW_HEIGHT, "",
                     ChildId::ENABLE_ENCRYPTION_CHECKBOX, enableEncryption);

  rowPos += INTERVAL + ROW_HEIGHT;

  val = config.Get(FlightSqlConnection::TRUSTED_CERTS).c_str();

  labels.push_back(CreateLabel(labelPosX, rowPos, LABEL_WIDTH, ROW_HEIGHT,
                               "Certificate:", ChildId::CERTIFICATE_LABEL));
  certificateEdit = CreateEdit(editPosX, rowPos, editSizeX - MARGIN - BUTTON_WIDTH,
                               ROW_HEIGHT, val, ChildId::CERTIFICATE_EDIT);
  certificateBrowseButton =
      CreateButton(editPosX + editSizeX - BUTTON_WIDTH, rowPos - 2, BUTTON_WIDTH,
                   BUTTON_HEIGHT, "Browse", ChildId::CERTIFICATE_BROWSE_BUTTON);

  rowPos += INTERVAL + ROW_HEIGHT;

  val = config.Get(FlightSqlConnection::USE_SYSTEM_TRUST_STORE).c_str();

  const bool useSystemCertStore = driver::odbcabstraction::AsBool(val).value_or(true);
  labels.push_back(
      CreateLabel(labelPosX, rowPos, LABEL_WIDTH, 2 * ROW_HEIGHT,
                  "Use System Certificate Store:", ChildId::USE_SYSTEM_CERT_STORE_LABEL));
  useSystemCertStoreCheckBox =
      CreateCheckBox(editPosX, rowPos - 2, 20, 2 * ROW_HEIGHT, "",
                     ChildId::USE_SYSTEM_CERT_STORE_CHECKBOX, useSystemCertStore);

  val = config.Get(FlightSqlConnection::DISABLE_CERTIFICATE_VERIFICATION).c_str();

  const int rightPosX = labelPosX + (sizeX - (2 * INTERVAL)) / 2;
  const int rightCheckPosX = rightPosX + (editPosX - labelPosX);
  const bool disableCertVerification =
      driver::odbcabstraction::AsBool(val).value_or(false);
  labels.push_back(CreateLabel(
      rightPosX, rowPos, LABEL_WIDTH, 2 * ROW_HEIGHT,
      "Disable Certificate Verification:", ChildId::DISABLE_CERT_VERIFICATION_LABEL));
  disableCertVerificationCheckBox = CreateCheckBox(
      rightCheckPosX, rowPos - 2, 20, 2 * ROW_HEIGHT, "",
      ChildId::DISABLE_CERT_VERIFICATION_CHECKBOX, disableCertVerification);

  rowPos += INTERVAL + static_cast<int>(1.5 * ROW_HEIGHT);

  encryptionSettingsGroupBox =
      CreateGroupBox(posX, posY, sizeX, rowPos - posY, "Encryption settings",
                     ChildId::AUTH_SETTINGS_GROUP_BOX);

  return rowPos - posY;
}

int DsnConfigurationWindow::CreatePropertiesGroup(int posX, int posY, int sizeX) {
  enum { LABEL_WIDTH = 120 };

  const int labelPosX = posX + INTERVAL;
  const int listSize = sizeX - 2 * INTERVAL;
  const int columnSize = listSize / 2;

  int rowPos = posY + 2 * INTERVAL;
  const int listHeight = 5 * ROW_HEIGHT;

  propertyList =
      CreateList(labelPosX, rowPos, listSize, listHeight, ChildId::PROPERTY_LIST);
  propertyList->ListAddColumn("Key", 0, columnSize);
  propertyList->ListAddColumn("Value", 1, columnSize);

  const auto keys = config.GetCustomKeys();
  for (const auto& key : keys) {
    propertyList->ListAddItem({std::string(key), config.Get(key)});
  }

  SendMessage(propertyList->GetHandle(), LVM_SETEXTENDEDLISTVIEWSTYLE,
              LVS_EX_FULLROWSELECT, LVS_EX_FULLROWSELECT);

  rowPos += INTERVAL + listHeight;

  int deletePosX = width - INTERVAL - MARGIN - BUTTON_WIDTH;
  int addPosX = deletePosX - INTERVAL - BUTTON_WIDTH;
  addButton = CreateButton(addPosX, rowPos, BUTTON_WIDTH, BUTTON_HEIGHT, "Add",
                           ChildId::ADD_BUTTON);
  deleteButton = CreateButton(deletePosX, rowPos, BUTTON_WIDTH, BUTTON_HEIGHT, "Delete",
                              ChildId::DELETE_BUTTON);

  rowPos += INTERVAL + BUTTON_HEIGHT;

  propertyGroupBox = CreateGroupBox(posX, posY, sizeX, rowPos - posY,
                                    "Advanced properties", ChildId::PROPERTY_GROUP_BOX);

  return rowPos - posY;
}

void DsnConfigurationWindow::SelectTab(int tabIndex) {
  if (!isInitialized) {
    return;
  }

  connectionSettingsGroupBox->SetVisible(COMMON_TAB == tabIndex);
  authSettingsGroupBox->SetVisible(COMMON_TAB == tabIndex);
  nameEdit->SetVisible(COMMON_TAB == tabIndex);
  serverEdit->SetVisible(COMMON_TAB == tabIndex);
  portEdit->SetVisible(COMMON_TAB == tabIndex);
  authTypeComboBox->SetVisible(COMMON_TAB == tabIndex);
  userEdit->SetVisible(COMMON_TAB == tabIndex);
  passwordEdit->SetVisible(COMMON_TAB == tabIndex);
  authTokenEdit->SetVisible(COMMON_TAB == tabIndex);
  for (size_t i = 0; i < 7; ++i) {
    labels[i]->SetVisible(COMMON_TAB == tabIndex);
  }

  encryptionSettingsGroupBox->SetVisible(ADVANCED_TAB == tabIndex);
  enableEncryptionCheckBox->SetVisible(ADVANCED_TAB == tabIndex);
  certificateEdit->SetVisible(ADVANCED_TAB == tabIndex);
  certificateBrowseButton->SetVisible(ADVANCED_TAB == tabIndex);
  useSystemCertStoreCheckBox->SetVisible(ADVANCED_TAB == tabIndex);
  disableCertVerificationCheckBox->SetVisible(ADVANCED_TAB == tabIndex);
  propertyGroupBox->SetVisible(ADVANCED_TAB == tabIndex);
  propertyList->SetVisible(ADVANCED_TAB == tabIndex);
  addButton->SetVisible(ADVANCED_TAB == tabIndex);
  deleteButton->SetVisible(ADVANCED_TAB == tabIndex);
  for (size_t i = 7; i < labels.size(); ++i) {
    labels[i]->SetVisible(ADVANCED_TAB == tabIndex);
  }
}

void DsnConfigurationWindow::CheckEnableOk() {
  if (!isInitialized) {
    return;
  }

  bool enableOk = !nameEdit->IsTextEmpty();
  enableOk = enableOk && !serverEdit->IsTextEmpty();
  enableOk = enableOk && !portEdit->IsTextEmpty();
  if (authTokenEdit->IsEnabled()) {
    enableOk = enableOk && !authTokenEdit->IsTextEmpty();
  } else {
    enableOk = enableOk && !userEdit->IsTextEmpty();
    enableOk = enableOk && !passwordEdit->IsTextEmpty();
  }

  testButton->SetEnabled(enableOk);
  okButton->SetEnabled(enableOk);
}

void DsnConfigurationWindow::SaveParameters(Configuration& targetConfig) {
  targetConfig.Clear();

  std::string text;
  nameEdit->GetText(text);
  targetConfig.Set(FlightSqlConnection::DSN, text);
  serverEdit->GetText(text);
  targetConfig.Set(FlightSqlConnection::HOST, text);
  portEdit->GetText(text);
  try {
    const int portInt = std::stoi(text);
    if (0 > portInt || USHRT_MAX < portInt) {
      throw odbcabstraction::DriverException("Invalid port value.");
    }
    targetConfig.Set(FlightSqlConnection::PORT, text);
  } catch (odbcabstraction::DriverException&) {
    throw;
  } catch (std::exception&) {
    throw odbcabstraction::DriverException("Invalid port value.");
  }

  if (0 == authTypeComboBox->GetSelection()) {
    userEdit->GetText(text);
    targetConfig.Set(FlightSqlConnection::UID, text);
    passwordEdit->GetText(text);
    targetConfig.Set(FlightSqlConnection::PWD, text);
  } else {
    authTokenEdit->GetText(text);
    targetConfig.Set(FlightSqlConnection::TOKEN, text);
  }

  if (enableEncryptionCheckBox->IsChecked()) {
    targetConfig.Set(FlightSqlConnection::USE_ENCRYPTION, TRUE_STR);
    certificateEdit->GetText(text);
    targetConfig.Set(FlightSqlConnection::TRUSTED_CERTS, text);
    targetConfig.Set(FlightSqlConnection::USE_SYSTEM_TRUST_STORE,
                     useSystemCertStoreCheckBox->IsChecked() ? TRUE_STR : FALSE_STR);
    targetConfig.Set(FlightSqlConnection::DISABLE_CERTIFICATE_VERIFICATION,
                     disableCertVerificationCheckBox->IsChecked() ? TRUE_STR : FALSE_STR);
  } else {
    targetConfig.Set(FlightSqlConnection::USE_ENCRYPTION, FALSE_STR);
  }

  // Get all the list properties.
  const auto properties = propertyList->ListGetAll();
  for (const auto& property : properties) {
    targetConfig.Set(property[0], property[1]);
  }
}

void DsnConfigurationWindow::CheckAuthType() {
  const bool isBasic = COMMON_TAB == authTypeComboBox->GetSelection();
  userEdit->SetEnabled(isBasic);
  passwordEdit->SetEnabled(isBasic);
  authTokenEdit->SetEnabled(!isBasic);
}

bool DsnConfigurationWindow::OnMessage(UINT msg, WPARAM wParam, LPARAM lParam) {
  switch (msg) {
    case WM_NOTIFY: {
      switch (((LPNMHDR)lParam)->code) {
        case TCN_SELCHANGING: {
          // Return FALSE to allow the selection to change.
          return FALSE;
        }

        case TCN_SELCHANGE: {
          SelectTab(TabCtrl_GetCurSel(tabControl->GetHandle()));
          break;
        }
      }
      break;
    }

    case WM_COMMAND: {
      switch (LOWORD(wParam)) {
        case ChildId::TEST_CONNECTION_BUTTON: {
          try {
            Configuration testConfig;
            SaveParameters(testConfig);
            std::string testMessage = TestConnection(testConfig);

            MessageBox(NULL, testMessage.c_str(), "Test Connection Success", MB_OK);
          } catch (odbcabstraction::DriverException& err) {
            MessageBox(NULL, err.GetMessageText().c_str(), "Error!",
                       MB_ICONEXCLAMATION | MB_OK);
          }

          break;
        }
        case ChildId::OK_BUTTON: {
          try {
            SaveParameters(config);
            accepted = true;
            PostMessage(GetHandle(), WM_CLOSE, 0, 0);
          } catch (odbcabstraction::DriverException& err) {
            MessageBox(NULL, err.GetMessageText().c_str(), "Error!",
                       MB_ICONEXCLAMATION | MB_OK);
          }

          break;
        }

        case IDCANCEL:
        case ChildId::CANCEL_BUTTON: {
          PostMessage(GetHandle(), WM_CLOSE, 0, 0);
          break;
        }

        case ChildId::AUTH_TOKEN_EDIT:
        case ChildId::NAME_EDIT:
        case ChildId::PASSWORD_EDIT:
        case ChildId::PORT_EDIT:
        case ChildId::SERVER_EDIT:
        case ChildId::USER_EDIT: {
          if (HIWORD(wParam) == EN_CHANGE) {
            CheckEnableOk();
          }
          break;
        }

        case ChildId::AUTH_TYPE_COMBOBOX: {
          CheckAuthType();
          CheckEnableOk();
          break;
        }

        case ChildId::ENABLE_ENCRYPTION_CHECKBOX: {
          const bool toggle = !enableEncryptionCheckBox->IsChecked();
          enableEncryptionCheckBox->SetChecked(toggle);
          certificateEdit->SetEnabled(toggle);
          certificateBrowseButton->SetEnabled(toggle);
          useSystemCertStoreCheckBox->SetEnabled(toggle);
          disableCertVerificationCheckBox->SetEnabled(toggle);
          break;
        }

        case ChildId::CERTIFICATE_BROWSE_BUTTON: {
          OPENFILENAME openFileName;
          char fileName[FILENAME_MAX];

          ZeroMemory(&openFileName, sizeof(openFileName));
          openFileName.lStructSize = sizeof(openFileName);
          openFileName.hwndOwner = NULL;
          openFileName.lpstrFile = fileName;
          openFileName.lpstrFile[0] = '\0';
          openFileName.nMaxFile = FILENAME_MAX;
          // TODO: What type should this be?
          openFileName.lpstrFilter = "All\0*.*";
          openFileName.nFilterIndex = 1;
          openFileName.lpstrFileTitle = NULL;
          openFileName.nMaxFileTitle = 0;
          openFileName.lpstrInitialDir = NULL;
          openFileName.Flags = OFN_PATHMUSTEXIST | OFN_FILEMUSTEXIST;

          if (GetOpenFileName(&openFileName)) {
            certificateEdit->SetText(fileName);
          }
          break;
        }

        case ChildId::USE_SYSTEM_CERT_STORE_CHECKBOX: {
          useSystemCertStoreCheckBox->SetChecked(
              !useSystemCertStoreCheckBox->IsChecked());
          break;
        }

        case ChildId::DISABLE_CERT_VERIFICATION_CHECKBOX: {
          disableCertVerificationCheckBox->SetChecked(
              !disableCertVerificationCheckBox->IsChecked());
          break;
        }

        case ChildId::DELETE_BUTTON: {
          propertyList->ListDeleteSelectedItem();
          break;
        }

        case ChildId::ADD_BUTTON: {
          AddPropertyWindow addWindow(this);
          addWindow.Create();
          addWindow.Show();
          addWindow.Update();

          if (ProcessMessages(addWindow) == Result::OK) {
            std::string key;
            std::string value;
            addWindow.GetProperty(key, value);
            propertyList->ListAddItem({key, value});
          }
          break;
        }

        default:
          return false;
      }

      break;
    }

    case WM_DESTROY: {
      PostQuitMessage(accepted ? Result::OK : Result::CANCEL);

      break;
    }

    default:
      return false;
  }

  return true;
}

}  // namespace config
}  // namespace flight_sql
}  // namespace driver
