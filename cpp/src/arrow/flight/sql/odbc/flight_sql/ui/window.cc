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

// platform.h includes windows.h, so it needs to be included
// before Windowsx.h and commctrl.h
#include "arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/platform.h"

#include <Windowsx.h>
#include <commctrl.h>
#include <winuser.h>
#include <string>

#include <boost/algorithm/string/trim.hpp>
#include <sstream>
#include "arrow/flight/sql/odbc/flight_sql/include/flight_sql/ui/window.h"
#include "arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/exceptions.h"

namespace driver {
namespace flight_sql {
namespace config {

HINSTANCE GetHInstance() {
  TCHAR szFileName[MAX_PATH];
  GetModuleFileName(NULL, szFileName, MAX_PATH);

  // TODO: This needs to be the module name.
  HINSTANCE hInstance = GetModuleHandle(szFileName);

  if (hInstance == NULL) {
    std::stringstream buf;
    buf << "Can not get hInstance for the module, error code: " << GetLastError();
    throw odbcabstraction::DriverException(buf.str());
  }

  return hInstance;
}

Window::Window(Window* parent, const char* className, const char* title)
    : className(className), title(title), handle(NULL), parent(parent), created(false) {
  // No-op.
}

Window::Window(HWND handle)
    : className(), title(), handle(handle), parent(0), created(false) {
  // No-op.
}

Window::~Window() {
  if (created) Destroy();
}

void Window::Create(DWORD style, int posX, int posY, int width, int height, int id) {
  if (handle) {
    std::stringstream buf;
    buf << "Window already created, error code: " << GetLastError();
    throw odbcabstraction::DriverException(buf.str());
  }

  handle = CreateWindow(className.c_str(), title.c_str(), style, posX, posY, width,
                        height, parent ? parent->GetHandle() : NULL,
                        reinterpret_cast<HMENU>(static_cast<ptrdiff_t>(id)),
                        GetHInstance(), this);

  if (!handle) {
    std::stringstream buf;
    buf << "Can not create window, error code: " << GetLastError();
    throw odbcabstraction::DriverException(buf.str());
  }

  created = true;

  const HGDIOBJ hfDefault = GetStockObject(DEFAULT_GUI_FONT);
  SendMessage(GetHandle(), WM_SETFONT, (WPARAM)hfDefault, MAKELPARAM(FALSE, 0));
}

std::unique_ptr<Window> Window::CreateTabControl(int id) {
  std::unique_ptr<Window> child(new Window(this, WC_TABCONTROL, ""));

  // Get the dimensions of the parent window's client area, and
  // create a tab control child window of that size.
  RECT rcClient;
  GetClientRect(handle, &rcClient);

  child->Create(WS_CHILD | WS_CLIPSIBLINGS | WS_VISIBLE | WS_TABSTOP, 0, 0,
                rcClient.right, 20, id);

  return child;
}

std::unique_ptr<Window> Window::CreateList(int posX, int posY, int sizeX, int sizeY,
                                           int id) {
  std::unique_ptr<Window> child(new Window(this, WC_LISTVIEW, ""));

  child->Create(
      WS_CHILD | WS_VISIBLE | WS_BORDER | LVS_REPORT | LVS_EDITLABELS | WS_TABSTOP, posX,
      posY, sizeX, sizeY, id);

  return child;
}

std::unique_ptr<Window> Window::CreateGroupBox(int posX, int posY, int sizeX, int sizeY,
                                               const char* title, int id) {
  std::unique_ptr<Window> child(new Window(this, "Button", title));

  child->Create(WS_CHILD | WS_VISIBLE | BS_GROUPBOX, posX, posY, sizeX, sizeY, id);

  return child;
}

std::unique_ptr<Window> Window::CreateLabel(int posX, int posY, int sizeX, int sizeY,
                                            const char* title, int id) {
  std::unique_ptr<Window> child(new Window(this, "Static", title));

  child->Create(WS_CHILD | WS_VISIBLE, posX, posY, sizeX, sizeY, id);

  return child;
}

std::unique_ptr<Window> Window::CreateEdit(int posX, int posY, int sizeX, int sizeY,
                                           const char* title, int id, int style) {
  std::unique_ptr<Window> child(new Window(this, "Edit", title));

  child->Create(WS_CHILD | WS_VISIBLE | WS_BORDER | ES_AUTOHSCROLL | WS_TABSTOP | style,
                posX, posY, sizeX, sizeY, id);

  return child;
}

std::unique_ptr<Window> Window::CreateButton(int posX, int posY, int sizeX, int sizeY,
                                             const char* title, int id, int style) {
  std::unique_ptr<Window> child(new Window(this, "Button", title));

  child->Create(WS_CHILD | WS_VISIBLE | WS_TABSTOP | style, posX, posY, sizeX, sizeY, id);

  return child;
}

std::unique_ptr<Window> Window::CreateCheckBox(int posX, int posY, int sizeX, int sizeY,
                                               const char* title, int id, bool state) {
  std::unique_ptr<Window> child(new Window(this, "Button", title));

  child->Create(WS_CHILD | WS_VISIBLE | BS_CHECKBOX | WS_TABSTOP, posX, posY, sizeX,
                sizeY, id);

  child->SetChecked(state);

  return child;
}

std::unique_ptr<Window> Window::CreateComboBox(int posX, int posY, int sizeX, int sizeY,
                                               const char* title, int id) {
  std::unique_ptr<Window> child(new Window(this, "Combobox", title));

  child->Create(WS_CHILD | WS_VISIBLE | CBS_DROPDOWNLIST | WS_TABSTOP, posX, posY, sizeX,
                sizeY, id);

  return child;
}

void Window::Show() { ShowWindow(handle, SW_SHOW); }

void Window::Update() { UpdateWindow(handle); }

void Window::Destroy() {
  if (handle) DestroyWindow(handle);

  handle = NULL;
}

void Window::SetVisible(bool isVisible) {
  ShowWindow(handle, isVisible ? SW_SHOW : SW_HIDE);
}

bool Window::IsTextEmpty() const {
  if (!IsEnabled()) {
    return true;
  }
  int len = GetWindowTextLength(handle);

  return (len <= 0);
}

void Window::ListAddColumn(const std::string& name, int index, int width) {
  LVCOLUMN lvc;
  lvc.mask = LVCF_FMT | LVCF_WIDTH | LVCF_TEXT | LVCF_SUBITEM;
  lvc.fmt = LVCFMT_LEFT;
  lvc.cx = width;
  lvc.pszText = const_cast<char*>(name.c_str());
  lvc.iSubItem = index;

  if (ListView_InsertColumn(handle, index, &lvc) == -1) {
    std::stringstream buf;
    buf << "Can not add list column, error code: " << GetLastError();
    throw odbcabstraction::DriverException(buf.str());
  }
}

void Window::ListAddItem(const std::vector<std::string>& items) {
  LVITEM lvi = {0};
  lvi.mask = LVIF_TEXT;
  lvi.pszText = const_cast<char*>(items[0].c_str());

  int ret = ListView_InsertItem(handle, &lvi);
  if (ret < 0) {
    std::stringstream buf;
    buf << "Can not add list item, error code: " << GetLastError();
    throw odbcabstraction::DriverException(buf.str());
  }

  for (size_t i = 1; i < items.size(); ++i) {
    ListView_SetItemText(handle, ret, static_cast<int>(i),
                         const_cast<char*>(items[i].c_str()));
  }
}

void Window::ListDeleteSelectedItem() {
  const int rowIndex = ListView_GetSelectionMark(handle);
  if (rowIndex >= 0) {
    if (ListView_DeleteItem(handle, rowIndex) == -1) {
      std::stringstream buf;
      buf << "Can not delete list item, error code: " << GetLastError();
      throw odbcabstraction::DriverException(buf.str());
    }
  }
}

std::vector<std::vector<std::string> > Window::ListGetAll() {
#define BUF_LEN 1024
  char buf[BUF_LEN];

  std::vector<std::vector<std::string> > values;
  const int numColumns = Header_GetItemCount(ListView_GetHeader(handle));
  const int numItems = ListView_GetItemCount(handle);
  for (int i = 0; i < numItems; ++i) {
    std::vector<std::string> row;
    for (int j = 0; j < numColumns; ++j) {
      ListView_GetItemText(handle, i, j, buf, BUF_LEN);
      row.emplace_back(buf);
    }
    values.push_back(row);
  }

  return values;
}

void Window::AddTab(const std::string& name, int index) {
  TCITEM tabControlItem;
  tabControlItem.mask = TCIF_TEXT | TCIF_IMAGE;
  tabControlItem.iImage = -1;
  tabControlItem.pszText = const_cast<char*>(name.c_str());
  if (TabCtrl_InsertItem(handle, index, &tabControlItem) == -1) {
    std::stringstream buf;
    buf << "Can not add tab, error code: " << GetLastError();
    throw odbcabstraction::DriverException(buf.str());
  }
}

void Window::GetText(std::string& text) const {
  if (!IsEnabled()) {
    text.clear();

    return;
  }

  int len = GetWindowTextLength(handle);

  if (len <= 0) {
    text.clear();

    return;
  }

  text.resize(len + 1);

  if (!GetWindowText(handle, &text[0], len + 1)) text.clear();

  text.resize(len);
  boost::algorithm::trim(text);
}

void Window::SetText(const std::string& text) const {
  SNDMSG(handle, WM_SETTEXT, 0, reinterpret_cast<LPARAM>(text.c_str()));
}

bool Window::IsChecked() const {
  return IsEnabled() && Button_GetCheck(handle) == BST_CHECKED;
}

void Window::SetChecked(bool state) {
  Button_SetCheck(handle, state ? BST_CHECKED : BST_UNCHECKED);
}

void Window::AddString(const std::string& str) {
  SNDMSG(handle, CB_ADDSTRING, 0, reinterpret_cast<LPARAM>(str.c_str()));
}

void Window::SetSelection(int idx) {
  SNDMSG(handle, CB_SETCURSEL, static_cast<WPARAM>(idx), 0);
}

int Window::GetSelection() const {
  return static_cast<int>(SNDMSG(handle, CB_GETCURSEL, 0, 0));
}

void Window::SetEnabled(bool enabled) { EnableWindow(GetHandle(), enabled); }

bool Window::IsEnabled() const { return IsWindowEnabled(GetHandle()) != 0; }

}  // namespace config
}  // namespace flight_sql
}  // namespace driver
