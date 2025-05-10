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

#include <arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/exceptions.h>
#include "ui/custom_window.h"
#include "ui/window.h"

namespace driver {
namespace flight_sql {
namespace config {

AddPropertyWindow::AddPropertyWindow(Window* parent)
    : CustomWindow(parent, "AddProperty", "Add Property"),
      width(300),
      height(120),
      accepted(false),
      isInitialized(false) {
  // No-op.
}

AddPropertyWindow::~AddPropertyWindow() {
  // No-op.
}

void AddPropertyWindow::Create() {
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

bool AddPropertyWindow::GetProperty(std::string& key, std::string& value) {
  if (accepted) {
    key = this->key;
    value = this->value;
    return true;
  }
  return false;
}

void AddPropertyWindow::OnCreate() {
  int groupPosY = MARGIN;
  int groupSizeY = width - 2 * MARGIN;

  groupPosY += INTERVAL + CreateEdits(MARGIN, groupPosY, groupSizeY);

  int cancelPosX = width - MARGIN - BUTTON_WIDTH;
  int okPosX = cancelPosX - INTERVAL - BUTTON_WIDTH;

  okButton = CreateButton(okPosX, groupPosY, BUTTON_WIDTH, BUTTON_HEIGHT, "Ok",
                          ChildId::OK_BUTTON, BS_DEFPUSHBUTTON);
  cancelButton = CreateButton(cancelPosX, groupPosY, BUTTON_WIDTH, BUTTON_HEIGHT,
                              "Cancel", ChildId::CANCEL_BUTTON);
  isInitialized = true;
  CheckEnableOk();
}

int AddPropertyWindow::CreateEdits(int posX, int posY, int sizeX) {
  enum { LABEL_WIDTH = 30 };

  const int editSizeX = sizeX - LABEL_WIDTH - INTERVAL;
  const int editPosX = posX + LABEL_WIDTH + INTERVAL;

  int rowPos = posY;

  labels.push_back(
      CreateLabel(posX, rowPos, LABEL_WIDTH, ROW_HEIGHT, "Key:", ChildId::KEY_LABEL));
  keyEdit = CreateEdit(editPosX, rowPos, editSizeX, ROW_HEIGHT, "", ChildId::KEY_EDIT);

  rowPos += INTERVAL + ROW_HEIGHT;

  labels.push_back(
      CreateLabel(posX, rowPos, LABEL_WIDTH, ROW_HEIGHT, "Value:", ChildId::VALUE_LABEL));
  valueEdit =
      CreateEdit(editPosX, rowPos, editSizeX, ROW_HEIGHT, "", ChildId::VALUE_EDIT);

  rowPos += INTERVAL + ROW_HEIGHT;

  return rowPos - posY;
}

void AddPropertyWindow::CheckEnableOk() {
  if (!isInitialized) {
    return;
  }

  okButton->SetEnabled(!keyEdit->IsTextEmpty() && !valueEdit->IsTextEmpty());
}

bool AddPropertyWindow::OnMessage(UINT msg, WPARAM wParam, LPARAM lParam) {
  switch (msg) {
    case WM_COMMAND: {
      switch (LOWORD(wParam)) {
        case ChildId::OK_BUTTON: {
          keyEdit->GetText(key);
          valueEdit->GetText(value);
          accepted = true;
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
          if (HIWORD(wParam) == EN_CHANGE) {
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
