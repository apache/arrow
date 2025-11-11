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

#include "arrow/result.h"
#include "arrow/util/utf8.h"

#include "arrow/flight/sql/odbc/odbc_impl/ui/dsn_configuration_window.h"

#include "arrow/flight/sql/odbc/odbc_impl/flight_sql_connection.h"
#include "arrow/flight/sql/odbc/odbc_impl/util.h"

#include <Shlwapi.h>
#include <Windowsx.h>
#include <commctrl.h>
#include <commdlg.h>
#include <sql.h>
#include <sstream>

#include "arrow/flight/sql/odbc/odbc_impl/ui/add_property_window.h"

#define COMMON_TAB 0
#define ADVANCED_TAB 1

namespace arrow::flight::sql::odbc {
namespace {
std::string TestConnection(const config::Configuration& config) {
  std::unique_ptr<FlightSqlConnection> flight_sql_conn(
      new FlightSqlConnection(OdbcVersion::V_3));

  std::vector<std::string_view> missing_properties;
  flight_sql_conn->Connect(config.GetProperties(), missing_properties);

  // This should have been checked before enabling the Test button.
  assert(missing_properties.empty());
  std::string server_name =
      boost::get<std::string>(flight_sql_conn->GetInfo(SQL_SERVER_NAME));
  std::string server_version =
      boost::get<std::string>(flight_sql_conn->GetInfo(SQL_DBMS_VER));
  return "Server Name: " + server_name + "\n" + "Server Version: " + server_version;
}
}  // namespace

namespace config {

DsnConfigurationWindow::DsnConfigurationWindow(Window* parent, Configuration& config)
    : CustomWindow(parent, L"FlightConfigureDSN", L"Configure Apache Arrow Flight SQL"),
      width_(480),
      height_(375),
      config_(config),
      accepted_(false),
      is_initialized_(false) {
  // No-op.
}

DsnConfigurationWindow::~DsnConfigurationWindow() {
  // No-op.
}

void DsnConfigurationWindow::Create() {
  // Finding out parent position.
  RECT parent_rect;
  GetWindowRect(parent_->GetHandle(), &parent_rect);

  // Positioning window to the center of parent window.
  const int pos_x =
      parent_rect.left + (parent_rect.right - parent_rect.left - width_) / 2;
  const int pos_y =
      parent_rect.top + (parent_rect.bottom - parent_rect.top - height_) / 2;

  RECT desired_rect = {pos_x, pos_y, pos_x + width_, pos_y + height_};
  AdjustWindowRect(&desired_rect, WS_BORDER | WS_CAPTION | WS_SYSMENU | WS_THICKFRAME,
                   FALSE);

  Window::Create(WS_OVERLAPPED | WS_SYSMENU, desired_rect.left, desired_rect.top,
                 desired_rect.right - desired_rect.left,
                 desired_rect.bottom - desired_rect.top, 0);

  if (!handle_) {
    std::stringstream buf;
    buf << "Can not create window, error code: " << GetLastError();
    throw DriverException(buf.str());
  }
}

void DsnConfigurationWindow::OnCreate() {
  tab_control_ = CreateTabControl(ChildId::TAB_CONTROL);
  tab_control_->AddTab(L"Common", COMMON_TAB);
  tab_control_->AddTab(L"Advanced", ADVANCED_TAB);

  int group_pos_y = 3 * MARGIN;
  int group_size_y = width_ - 2 * MARGIN;

  int common_group_pos_y = group_pos_y;
  common_group_pos_y +=
      INTERVAL + CreateConnectionSettingsGroup(MARGIN, common_group_pos_y, group_size_y);
  common_group_pos_y +=
      INTERVAL + CreateAuthSettingsGroup(MARGIN, common_group_pos_y, group_size_y);

  int advanced_group_pos_y = group_pos_y;
  advanced_group_pos_y += INTERVAL + CreateEncryptionSettingsGroup(
                                         MARGIN, advanced_group_pos_y, group_size_y);
  advanced_group_pos_y +=
      INTERVAL + CreatePropertiesGroup(MARGIN, advanced_group_pos_y, group_size_y);

  int test_pos_x = MARGIN;
  int cancel_pos_x = width_ - MARGIN - BUTTON_WIDTH;
  int ok_pos_x = cancel_pos_x - INTERVAL - BUTTON_WIDTH;

  int button_pos_y = std::max(common_group_pos_y, advanced_group_pos_y);
  test_button_ = CreateButton(test_pos_x, button_pos_y, BUTTON_WIDTH + 20, BUTTON_HEIGHT,
                              L"Test Connection", ChildId::TEST_CONNECTION_BUTTON);
  ok_button_ = CreateButton(ok_pos_x, button_pos_y, BUTTON_WIDTH, BUTTON_HEIGHT, L"Ok",
                            ChildId::OK_BUTTON);
  cancel_button_ = CreateButton(cancel_pos_x, button_pos_y, BUTTON_WIDTH, BUTTON_HEIGHT,
                                L"Cancel", ChildId::CANCEL_BUTTON);
  is_initialized_ = true;
  CheckEnableOk();
  SelectTab(COMMON_TAB);
}

int DsnConfigurationWindow::CreateConnectionSettingsGroup(int pos_x, int pos_y,
                                                          int size_x) {
  enum { LABEL_WIDTH = 100 };

  const int label_pos_x = pos_x + INTERVAL;

  const int edit_size_x = size_x - LABEL_WIDTH - 3 * INTERVAL;
  const int edit_pos_x = label_pos_x + LABEL_WIDTH + INTERVAL;

  int row_pos = pos_y + 2 * INTERVAL;

  std::string dsn = config_.Get(FlightSqlConnection::DSN);
  CONVERT_WIDE_STR(const std::wstring wdsn, dsn);
  labels_.push_back(CreateLabel(label_pos_x, row_pos, LABEL_WIDTH, ROW_HEIGHT,
                                L"Data Source Name:", ChildId::NAME_LABEL));
  name_edit_ = CreateEdit(edit_pos_x, row_pos, edit_size_x, ROW_HEIGHT, wdsn.c_str(),
                          ChildId::NAME_EDIT);

  row_pos += INTERVAL + ROW_HEIGHT;

  std::string host = config_.Get(FlightSqlConnection::HOST);
  CONVERT_WIDE_STR(const std::wstring whost, host);
  labels_.push_back(CreateLabel(label_pos_x, row_pos, LABEL_WIDTH, ROW_HEIGHT,
                                L"Host Name:", ChildId::SERVER_LABEL));
  server_edit_ = CreateEdit(edit_pos_x, row_pos, edit_size_x, ROW_HEIGHT, whost.c_str(),
                            ChildId::SERVER_EDIT);

  row_pos += INTERVAL + ROW_HEIGHT;

  std::string port = config_.Get(FlightSqlConnection::PORT);
  CONVERT_WIDE_STR(const std::wstring wport, port);
  labels_.push_back(CreateLabel(label_pos_x, row_pos, LABEL_WIDTH, ROW_HEIGHT, L"Port:",
                                ChildId::PORT_LABEL));
  port_edit_ = CreateEdit(edit_pos_x, row_pos, edit_size_x, ROW_HEIGHT, wport.c_str(),
                          ChildId::PORT_EDIT, ES_NUMBER);

  row_pos += INTERVAL + ROW_HEIGHT;

  connection_settings_group_box_ =
      CreateGroupBox(pos_x, pos_y, size_x, row_pos - pos_y, L"Connection settings",
                     ChildId::CONNECTION_SETTINGS_GROUP_BOX);

  return row_pos - pos_y;
}

int DsnConfigurationWindow::CreateAuthSettingsGroup(int pos_x, int pos_y, int size_x) {
  enum { LABEL_WIDTH = 120 };

  const int label_pos_x = pos_x + INTERVAL;

  const int edit_size_x = size_x - LABEL_WIDTH - 3 * INTERVAL;
  const int edit_pos_x = label_pos_x + LABEL_WIDTH + INTERVAL;

  int row_pos = pos_y + 2 * INTERVAL;

  labels_.push_back(CreateLabel(label_pos_x, row_pos, LABEL_WIDTH, ROW_HEIGHT,
                                L"Authentication Type:", ChildId::AUTH_TYPE_LABEL));
  auth_type_combo_box_ =
      CreateComboBox(edit_pos_x, row_pos, edit_size_x, ROW_HEIGHT,
                     L"Authentication Type:", ChildId::AUTH_TYPE_COMBOBOX);
  auth_type_combo_box_->AddString(L"Basic Authentication");
  auth_type_combo_box_->AddString(L"Token Authentication");

  row_pos += INTERVAL + ROW_HEIGHT;

  std::string uid = config_.Get(FlightSqlConnection::UID);
  CONVERT_WIDE_STR(const std::wstring wuid, uid);

  labels_.push_back(CreateLabel(label_pos_x, row_pos, LABEL_WIDTH, ROW_HEIGHT, L"User:",
                                ChildId::USER_LABEL));
  user_edit_ = CreateEdit(edit_pos_x, row_pos, edit_size_x, ROW_HEIGHT, wuid.c_str(),
                          ChildId::USER_EDIT);

  row_pos += INTERVAL + ROW_HEIGHT;

  std::string pwd = config_.Get(FlightSqlConnection::PWD);
  CONVERT_WIDE_STR(const std::wstring wpwd, pwd);
  labels_.push_back(CreateLabel(label_pos_x, row_pos, LABEL_WIDTH, ROW_HEIGHT,
                                L"Password:", ChildId::PASSWORD_LABEL));
  password_edit_ = CreateEdit(edit_pos_x, row_pos, edit_size_x, ROW_HEIGHT, wpwd.c_str(),
                              ChildId::USER_EDIT, ES_PASSWORD);

  row_pos += INTERVAL + ROW_HEIGHT;

  const auto& token = config_.Get(FlightSqlConnection::TOKEN);
  CONVERT_WIDE_STR(const std::wstring wtoken, token);
  labels_.push_back(CreateLabel(label_pos_x, row_pos, LABEL_WIDTH, ROW_HEIGHT,
                                L"Authentication Token:", ChildId::AUTH_TOKEN_LABEL));
  auth_token_edit_ = CreateEdit(edit_pos_x, row_pos, edit_size_x, ROW_HEIGHT,
                                wtoken.c_str(), ChildId::AUTH_TOKEN_EDIT);
  auth_token_edit_->SetEnabled(false);

  // Ensure the right elements are selected.
  auth_type_combo_box_->SetSelection(token.empty() ? 0 : 1);
  CheckAuthType();

  row_pos += INTERVAL + ROW_HEIGHT;

  auth_settings_group_box_ =
      CreateGroupBox(pos_x, pos_y, size_x, row_pos - pos_y, L"Authentication settings",
                     ChildId::AUTH_SETTINGS_GROUP_BOX);

  return row_pos - pos_y;
}

int DsnConfigurationWindow::CreateEncryptionSettingsGroup(int pos_x, int pos_y,
                                                          int size_x) {
  enum { LABEL_WIDTH = 120 };

  const int label_pos_x = pos_x + INTERVAL;

  const int edit_size_x = size_x - LABEL_WIDTH - 3 * INTERVAL;
  const int edit_pos_x = label_pos_x + LABEL_WIDTH + INTERVAL;

  int row_pos = pos_y + 2 * INTERVAL;

  std::string val = config_.Get(FlightSqlConnection::USE_ENCRYPTION);

  const bool enable_encryption = util::AsBool(val).value_or(true);
  labels_.push_back(CreateLabel(label_pos_x, row_pos, LABEL_WIDTH, ROW_HEIGHT,
                                L"Use Encryption:", ChildId::ENABLE_ENCRYPTION_LABEL));
  enable_encryption_check_box_ =
      CreateCheckBox(edit_pos_x, row_pos - 2, edit_size_x, ROW_HEIGHT, L"",
                     ChildId::ENABLE_ENCRYPTION_CHECKBOX, enable_encryption);

  row_pos += INTERVAL + ROW_HEIGHT;

  std::string trusted_certs = config_.Get(FlightSqlConnection::TRUSTED_CERTS);
  CONVERT_WIDE_STR(const std::wstring wtrusted_certs, trusted_certs);

  labels_.push_back(CreateLabel(label_pos_x, row_pos, LABEL_WIDTH, ROW_HEIGHT,
                                L"Certificate:", ChildId::CERTIFICATE_LABEL));
  certificate_edit_ =
      CreateEdit(edit_pos_x, row_pos, edit_size_x - MARGIN - BUTTON_WIDTH, ROW_HEIGHT,
                 wtrusted_certs.c_str(), ChildId::CERTIFICATE_EDIT);
  certificate_browse_button_ =
      CreateButton(edit_pos_x + edit_size_x - BUTTON_WIDTH, row_pos - 2, BUTTON_WIDTH,
                   BUTTON_HEIGHT, L"Browse", ChildId::CERTIFICATE_BROWSE_BUTTON);

  row_pos += INTERVAL + ROW_HEIGHT;

  val = config_.Get(FlightSqlConnection::USE_SYSTEM_TRUST_STORE).c_str();

  const bool use_system_cert_store = util::AsBool(val).value_or(true);
  labels_.push_back(CreateLabel(label_pos_x, row_pos, LABEL_WIDTH, 2 * ROW_HEIGHT,
                                L"Use System Certificate Store:",
                                ChildId::USE_SYSTEM_CERT_STORE_LABEL));
  use_system_cert_store_check_box_ =
      CreateCheckBox(edit_pos_x, row_pos - 2, 20, 2 * ROW_HEIGHT, L"",
                     ChildId::USE_SYSTEM_CERT_STORE_CHECKBOX, use_system_cert_store);

  val = config_.Get(FlightSqlConnection::DISABLE_CERTIFICATE_VERIFICATION).c_str();

  const int right_pos_x = label_pos_x + (size_x - (2 * INTERVAL)) / 2;
  const int right_check_pos_x = right_pos_x + (edit_pos_x - label_pos_x);
  const bool disable_cert_verification = util::AsBool(val).value_or(false);
  labels_.push_back(CreateLabel(right_pos_x, row_pos, LABEL_WIDTH, 2 * ROW_HEIGHT,
                                L"Disable Certificate Verification:",
                                ChildId::DISABLE_CERT_VERIFICATION_LABEL));
  disable_cert_verification_check_box_ = CreateCheckBox(
      right_check_pos_x, row_pos - 2, 20, 2 * ROW_HEIGHT, L"",
      ChildId::DISABLE_CERT_VERIFICATION_CHECKBOX, disable_cert_verification);

  row_pos += INTERVAL + static_cast<int>(1.5 * static_cast<int>(ROW_HEIGHT));

  encryption_settings_group_box_ =
      CreateGroupBox(pos_x, pos_y, size_x, row_pos - pos_y, L"Encryption settings",
                     ChildId::AUTH_SETTINGS_GROUP_BOX);

  certificate_edit_->SetEnabled(enable_encryption);
  certificate_browse_button_->SetEnabled(enable_encryption);
  use_system_cert_store_check_box_->SetEnabled(enable_encryption);
  disable_cert_verification_check_box_->SetEnabled(enable_encryption);

  return row_pos - pos_y;
}

int DsnConfigurationWindow::CreatePropertiesGroup(int pos_x, int pos_y, int size_x) {
  enum { LABEL_WIDTH = 120 };

  const int label_pos_x = pos_x + INTERVAL;
  const int list_size = size_x - 2 * INTERVAL;
  const int column_size = list_size / 2;

  int row_pos = pos_y + 2 * INTERVAL;
  const int list_height = 5 * ROW_HEIGHT;

  property_list_ =
      CreateList(label_pos_x, row_pos, list_size, list_height, ChildId::PROPERTY_LIST);
  property_list_->ListAddColumn(L"Key", 0, column_size);
  property_list_->ListAddColumn(L"Value", 1, column_size);

  const auto keys = config_.GetCustomKeys();
  for (const auto& key : keys) {
    CONVERT_WIDE_STR(const std::wstring wkey, key);
    CONVERT_WIDE_STR(const std::wstring wval, config_.Get(key));

    property_list_->ListAddItem({wkey, wval});
  }

  SendMessage(property_list_->GetHandle(), LVM_SETEXTENDEDLISTVIEWSTYLE,
              LVS_EX_FULLROWSELECT, LVS_EX_FULLROWSELECT);

  row_pos += INTERVAL + list_height;

  int delete_pos_x = width_ - INTERVAL - MARGIN - BUTTON_WIDTH;
  int add_pos_x = delete_pos_x - INTERVAL - BUTTON_WIDTH;
  add_button_ = CreateButton(add_pos_x, row_pos, BUTTON_WIDTH, BUTTON_HEIGHT, L"Add",
                             ChildId::ADD_BUTTON);
  delete_button_ = CreateButton(delete_pos_x, row_pos, BUTTON_WIDTH, BUTTON_HEIGHT,
                                L"Delete", ChildId::DELETE_BUTTON);

  row_pos += INTERVAL + BUTTON_HEIGHT;

  property_group_box_ =
      CreateGroupBox(pos_x, pos_y, size_x, row_pos - pos_y, L"Advanced properties",
                     ChildId::PROPERTY_GROUP_BOX);

  return row_pos - pos_y;
}

void DsnConfigurationWindow::SelectTab(int tab_index) {
  if (!is_initialized_) {
    return;
  }

  connection_settings_group_box_->SetVisible(COMMON_TAB == tab_index);
  auth_settings_group_box_->SetVisible(COMMON_TAB == tab_index);
  name_edit_->SetVisible(COMMON_TAB == tab_index);
  server_edit_->SetVisible(COMMON_TAB == tab_index);
  port_edit_->SetVisible(COMMON_TAB == tab_index);
  auth_type_combo_box_->SetVisible(COMMON_TAB == tab_index);
  user_edit_->SetVisible(COMMON_TAB == tab_index);
  password_edit_->SetVisible(COMMON_TAB == tab_index);
  auth_token_edit_->SetVisible(COMMON_TAB == tab_index);
  for (size_t i = 0; i < 7; ++i) {
    labels_[i]->SetVisible(COMMON_TAB == tab_index);
  }

  encryption_settings_group_box_->SetVisible(ADVANCED_TAB == tab_index);
  enable_encryption_check_box_->SetVisible(ADVANCED_TAB == tab_index);
  certificate_edit_->SetVisible(ADVANCED_TAB == tab_index);
  certificate_browse_button_->SetVisible(ADVANCED_TAB == tab_index);
  use_system_cert_store_check_box_->SetVisible(ADVANCED_TAB == tab_index);
  disable_cert_verification_check_box_->SetVisible(ADVANCED_TAB == tab_index);
  property_group_box_->SetVisible(ADVANCED_TAB == tab_index);
  property_list_->SetVisible(ADVANCED_TAB == tab_index);
  add_button_->SetVisible(ADVANCED_TAB == tab_index);
  delete_button_->SetVisible(ADVANCED_TAB == tab_index);
  for (size_t i = 7; i < labels_.size(); ++i) {
    labels_[i]->SetVisible(ADVANCED_TAB == tab_index);
  }
}

void DsnConfigurationWindow::CheckEnableOk() {
  if (!is_initialized_) {
    return;
  }

  bool enable_ok = !name_edit_->IsTextEmpty();
  enable_ok = enable_ok && !server_edit_->IsTextEmpty();
  enable_ok = enable_ok && !port_edit_->IsTextEmpty();
  if (auth_token_edit_->IsEnabled()) {
    enable_ok = enable_ok && !auth_token_edit_->IsTextEmpty();
  } else {
    enable_ok = enable_ok && !user_edit_->IsTextEmpty();
    enable_ok = enable_ok && !password_edit_->IsTextEmpty();
  }

  test_button_->SetEnabled(enable_ok);
  ok_button_->SetEnabled(enable_ok);
}

void DsnConfigurationWindow::SaveParameters(Configuration& target_config) {
  target_config.Clear();

  std::wstring text;
  name_edit_->GetText(text);
  target_config.Set(FlightSqlConnection::DSN, text);
  server_edit_->GetText(text);
  target_config.Set(FlightSqlConnection::HOST, text);
  port_edit_->GetText(text);
  try {
    const int port_int = std::stoi(text);
    if (0 > port_int || USHRT_MAX < port_int) {
      throw DriverException("Invalid port value.");
    }
    target_config.Set(FlightSqlConnection::PORT, text);
  } catch (DriverException&) {
    throw;
  } catch (std::exception&) {
    throw DriverException("Invalid port value.");
  }

  if (0 == auth_type_combo_box_->GetSelection()) {
    user_edit_->GetText(text);
    target_config.Set(FlightSqlConnection::UID, text);
    password_edit_->GetText(text);
    target_config.Set(FlightSqlConnection::PWD, text);
  } else {
    auth_token_edit_->GetText(text);
    target_config.Set(FlightSqlConnection::TOKEN, text);
  }

  if (enable_encryption_check_box_->IsChecked()) {
    target_config.Set(FlightSqlConnection::USE_ENCRYPTION, TRUE_STR);
    certificate_edit_->GetText(text);
    target_config.Set(FlightSqlConnection::TRUSTED_CERTS, text);
    target_config.Set(
        FlightSqlConnection::USE_SYSTEM_TRUST_STORE,
        use_system_cert_store_check_box_->IsChecked() ? TRUE_STR : FALSE_STR);
    target_config.Set(
        FlightSqlConnection::DISABLE_CERTIFICATE_VERIFICATION,
        disable_cert_verification_check_box_->IsChecked() ? TRUE_STR : FALSE_STR);
  } else {
    // System trust store verification requires encryption
    target_config.Set(FlightSqlConnection::USE_ENCRYPTION, FALSE_STR);
    target_config.Set(FlightSqlConnection::USE_SYSTEM_TRUST_STORE, FALSE_STR);
  }

  // Get all the list properties.
  const auto properties = property_list_->ListGetAll();
  for (const auto& property : properties) {
    std::string property_key = arrow::util::WideStringToUTF8(property[0]).ValueOr("");
    std::string property_value = arrow::util::WideStringToUTF8(property[1]).ValueOr("");
    target_config.Set(property_key, property_value);
  }
}

void DsnConfigurationWindow::CheckAuthType() {
  const bool is_basic = COMMON_TAB == auth_type_combo_box_->GetSelection();
  user_edit_->SetEnabled(is_basic);
  password_edit_->SetEnabled(is_basic);
  auth_token_edit_->SetEnabled(!is_basic);
}

bool DsnConfigurationWindow::OnMessage(UINT msg, WPARAM wparam, LPARAM lparam) {
  switch (msg) {
    case WM_NOTIFY: {
      switch (((LPNMHDR)lparam)->code) {
        case TCN_SELCHANGING: {
          // Return FALSE to allow the selection to change.
          return FALSE;
        }

        case TCN_SELCHANGE: {
          SelectTab(TabCtrl_GetCurSel(tab_control_->GetHandle()));
          break;
        }
      }
      break;
    }

    case WM_COMMAND: {
      switch (LOWORD(wparam)) {
        case ChildId::TEST_CONNECTION_BUTTON: {
          try {
            Configuration test_config;
            SaveParameters(test_config);
            std::string test_message = TestConnection(test_config);

            CONVERT_WIDE_STR(const std::wstring w_test_message, test_message);

            MessageBox(NULL, w_test_message.c_str(), L"Test Connection Success", MB_OK);
          } catch (DriverException& err) {
            const std::wstring w_message_text =
                arrow::util::UTF8ToWideString(err.GetMessageText())
                    .ValueOr(L"Test failed");
            MessageBox(NULL, w_message_text.c_str(), L"Error!",
                       MB_ICONEXCLAMATION | MB_OK);
          }

          break;
        }
        case ChildId::OK_BUTTON: {
          try {
            SaveParameters(config_);
            accepted_ = true;
            PostMessage(GetHandle(), WM_CLOSE, 0, 0);
          } catch (DriverException& err) {
            const std::wstring w_message_text =
                arrow::util::UTF8ToWideString(err.GetMessageText())
                    .ValueOr(L"Error when saving DSN");
            MessageBox(NULL, w_message_text.c_str(), L"Error!",
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
          if (HIWORD(wparam) == EN_CHANGE) {
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
          const bool toggle = !enable_encryption_check_box_->IsChecked();
          enable_encryption_check_box_->SetChecked(toggle);
          certificate_edit_->SetEnabled(toggle);
          certificate_browse_button_->SetEnabled(toggle);
          use_system_cert_store_check_box_->SetEnabled(toggle);
          disable_cert_verification_check_box_->SetEnabled(toggle);
          break;
        }

        case ChildId::CERTIFICATE_BROWSE_BUTTON: {
          OPENFILENAME open_file_name;
          wchar_t file_name[FILENAME_MAX];

          ZeroMemory(&open_file_name, sizeof(open_file_name));
          open_file_name.lStructSize = sizeof(open_file_name);
          open_file_name.hwndOwner = NULL;
          open_file_name.lpstrFile = file_name;
          open_file_name.lpstrFile[0] = '\0';
          open_file_name.nMaxFile = FILENAME_MAX;
          // TODO: What type should this be?
          open_file_name.lpstrFilter = L"All\0*.*";
          open_file_name.nFilterIndex = 1;
          open_file_name.lpstrFileTitle = NULL;
          open_file_name.nMaxFileTitle = 0;
          open_file_name.lpstrInitialDir = NULL;
          open_file_name.Flags = OFN_PATHMUSTEXIST | OFN_FILEMUSTEXIST;

          if (GetOpenFileName(&open_file_name)) {
            certificate_edit_->SetText(file_name);
          }
          break;
        }

        case ChildId::USE_SYSTEM_CERT_STORE_CHECKBOX: {
          use_system_cert_store_check_box_->SetChecked(
              !use_system_cert_store_check_box_->IsChecked());
          break;
        }

        case ChildId::DISABLE_CERT_VERIFICATION_CHECKBOX: {
          disable_cert_verification_check_box_->SetChecked(
              !disable_cert_verification_check_box_->IsChecked());
          break;
        }

        case ChildId::DELETE_BUTTON: {
          property_list_->ListDeleteSelectedItem();
          break;
        }

        case ChildId::ADD_BUTTON: {
          AddPropertyWindow add_window(this);
          add_window.Create();
          add_window.Show();
          add_window.Update();

          if (ProcessMessages(add_window) == Result::OK) {
            std::wstring key;
            std::wstring value;
            add_window.GetProperty(key, value);
            property_list_->ListAddItem({key, value});
          }
          break;
        }

        default:
          return false;
      }

      break;
    }

    case WM_DESTROY: {
      PostQuitMessage(accepted_ ? Result::OK : Result::CANCEL);

      break;
    }

    default:
      return false;
  }

  return true;
}

}  // namespace config
}  // namespace arrow::flight::sql::odbc
