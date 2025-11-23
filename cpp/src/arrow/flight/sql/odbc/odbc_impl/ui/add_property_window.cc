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

#include "ui/add_property_window.h"

#include <Windowsx.h>

#include <Shlwapi.h>
#include <sstream>

#include <commctrl.h>

#include "arrow/flight/sql/odbc/odbc_impl/exceptions.h"
#include "ui/custom_window.h"
#include "ui/window.h"

namespace arrow::flight::sql::odbc {
namespace config {

AddPropertyWindow::AddPropertyWindow(Window* parent)
    : CustomWindow(parent, L"AddProperty", L"Add Property"),
      width_(300),
      height_(120),
      accepted_(false),
      is_initialized_(false) {
  // No-op.
}

AddPropertyWindow::~AddPropertyWindow() {
  // No-op.
}

void AddPropertyWindow::Create() {
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

bool AddPropertyWindow::GetProperty(std::wstring& key, std::wstring& value) {
  if (accepted_) {
    key = this->key_;
    value = this->value_;
    return true;
  }
  return false;
}

void AddPropertyWindow::OnCreate() {
  int group_pos_y = MARGIN;
  int group_size_y = width_ - 2 * MARGIN;

  group_pos_y += INTERVAL + CreateEdits(MARGIN, group_pos_y, group_size_y);

  int cancel_pos_x = width_ - MARGIN - BUTTON_WIDTH;
  int ok_pos_x = cancel_pos_x - INTERVAL - BUTTON_WIDTH;

  ok_button_ = CreateButton(ok_pos_x, group_pos_y, BUTTON_WIDTH, BUTTON_HEIGHT, L"Ok",
                            ChildId::OK_BUTTON, BS_DEFPUSHBUTTON);
  cancel_button_ = CreateButton(cancel_pos_x, group_pos_y, BUTTON_WIDTH, BUTTON_HEIGHT,
                                L"Cancel", ChildId::CANCEL_BUTTON);
  is_initialized_ = true;
  CheckEnableOk();
}

int AddPropertyWindow::CreateEdits(int pos_x, int pos_y, int size_x) {
  enum { LABEL_WIDTH = 30 };

  const int edit_size_x = size_x - LABEL_WIDTH - INTERVAL;
  const int edit_pos_x = pos_x + LABEL_WIDTH + INTERVAL;

  int row_pos = pos_y;

  labels_.push_back(
      CreateLabel(pos_x, row_pos, LABEL_WIDTH, ROW_HEIGHT, L"Key:", ChildId::KEY_LABEL));
  key_edit_ =
      CreateEdit(edit_pos_x, row_pos, edit_size_x, ROW_HEIGHT, L"", ChildId::KEY_EDIT);

  row_pos += INTERVAL + ROW_HEIGHT;

  labels_.push_back(CreateLabel(pos_x, row_pos, LABEL_WIDTH, ROW_HEIGHT, L"Value:",
                                ChildId::VALUE_LABEL));
  value_edit_ =
      CreateEdit(edit_pos_x, row_pos, edit_size_x, ROW_HEIGHT, L"", ChildId::VALUE_EDIT);

  row_pos += INTERVAL + ROW_HEIGHT;

  return row_pos - pos_y;
}

void AddPropertyWindow::CheckEnableOk() {
  if (!is_initialized_) {
    return;
  }

  ok_button_->SetEnabled(!key_edit_->IsTextEmpty() && !value_edit_->IsTextEmpty());
}

bool AddPropertyWindow::OnMessage(UINT msg, WPARAM wparam, LPARAM lparam) {
  switch (msg) {
    case WM_COMMAND: {
      switch (LOWORD(wparam)) {
        case ChildId::OK_BUTTON: {
          key_edit_->GetText(key_);
          value_edit_->GetText(value_);
          accepted_ = true;
          PostMessage(GetHandle(), WM_CLOSE, 0, 0);

          break;
        }

        case IDCANCEL:
        case ChildId::CANCEL_BUTTON: {
          PostMessage(GetHandle(), WM_CLOSE, 0, 0);
          break;
        }

        case ChildId::KEY_EDIT:
        case ChildId::VALUE_EDIT: {
          if (HIWORD(wparam) == EN_CHANGE) {
            CheckEnableOk();
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
