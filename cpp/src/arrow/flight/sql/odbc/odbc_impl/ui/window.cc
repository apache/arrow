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
#include "arrow/flight/sql/odbc/odbc_impl/platform.h"

#include <Windowsx.h>
#include <commctrl.h>
#include <winuser.h>
#include <string>

#include <boost/algorithm/string/trim.hpp>
#include <sstream>
#include "arrow/flight/sql/odbc/odbc_impl/exceptions.h"
#include "arrow/flight/sql/odbc/odbc_impl/ui/window.h"

namespace arrow::flight::sql::odbc {
namespace config {

HINSTANCE GetHInstance() {
  TCHAR sz_file_name[MAX_PATH];
  GetModuleFileName(NULL, sz_file_name, MAX_PATH);

  HINSTANCE h_instance = GetModuleHandle(sz_file_name);

  if (h_instance == NULL) {
    std::stringstream buf;
    buf << "Can not get hInstance for the module, error code: " << GetLastError();
    throw DriverException(buf.str());
  }

  return h_instance;
}

Window::Window(Window* parent, const wchar_t* class_name, const wchar_t* title)
    : class_name_(class_name),
      title_(title),
      handle_(NULL),
      parent_(parent),
      created_(false) {
  // No-op.
}

Window::Window(HWND handle)
    : class_name_(), title_(), handle_(handle), parent_(0), created_(false) {
  // No-op.
}

Window::~Window() {
  if (created_) Destroy();
}

void Window::Create(DWORD style, int pos_x, int pos_y, int width, int height, int id) {
  if (handle_) {
    std::stringstream buf;
    buf << "Window already created, error code: " << GetLastError();
    throw DriverException(buf.str());
  }

  handle_ = CreateWindow(class_name_.c_str(), title_.c_str(), style, pos_x, pos_y, width,
                         height, parent_ ? parent_->GetHandle() : NULL,
                         reinterpret_cast<HMENU>(static_cast<ptrdiff_t>(id)),
                         GetHInstance(), this);

  if (!handle_) {
    std::stringstream buf;
    buf << "Can not create window, error code: " << GetLastError();
    throw DriverException(buf.str());
  }

  created_ = true;

  const HGDIOBJ hf_default = GetStockObject(DEFAULT_GUI_FONT);
  SendMessage(GetHandle(), WM_SETFONT, (WPARAM)hf_default, MAKELPARAM(FALSE, 0));
}

std::unique_ptr<Window> Window::CreateTabControl(int id) {
  std::unique_ptr<Window> child(new Window(this, WC_TABCONTROL, L""));

  // Get the dimensions of the parent window's client area, and
  // create a tab control child window of that size.
  RECT rc_client;
  GetClientRect(handle_, &rc_client);

  child->Create(WS_CHILD | WS_CLIPSIBLINGS | WS_VISIBLE | WS_TABSTOP, 0, 0,
                rc_client.right, 20, id);

  return child;
}

std::unique_ptr<Window> Window::CreateList(int pos_x, int pos_y, int size_x, int size_y,
                                           int id) {
  std::unique_ptr<Window> child(new Window(this, WC_LISTVIEW, L""));

  child->Create(
      WS_CHILD | WS_VISIBLE | WS_BORDER | LVS_REPORT | LVS_EDITLABELS | WS_TABSTOP, pos_x,
      pos_y, size_x, size_y, id);

  return child;
}

std::unique_ptr<Window> Window::CreateGroupBox(int pos_x, int pos_y, int size_x,
                                               int size_y, const wchar_t* title, int id) {
  std::unique_ptr<Window> child(new Window(this, L"Button", title));

  child->Create(WS_CHILD | WS_VISIBLE | BS_GROUPBOX, pos_x, pos_y, size_x, size_y, id);

  return child;
}

std::unique_ptr<Window> Window::CreateLabel(int pos_x, int pos_y, int size_x, int size_y,
                                            const wchar_t* title, int id) {
  std::unique_ptr<Window> child(new Window(this, L"Static", title));

  child->Create(WS_CHILD | WS_VISIBLE, pos_x, pos_y, size_x, size_y, id);

  return child;
}

std::unique_ptr<Window> Window::CreateEdit(int pos_x, int pos_y, int size_x, int size_y,
                                           const wchar_t* title, int id, int style) {
  std::unique_ptr<Window> child(new Window(this, L"Edit", title));

  child->Create(WS_CHILD | WS_VISIBLE | WS_BORDER | ES_AUTOHSCROLL | WS_TABSTOP | style,
                pos_x, pos_y, size_x, size_y, id);

  return child;
}

std::unique_ptr<Window> Window::CreateButton(int pos_x, int pos_y, int size_x, int size_y,
                                             const wchar_t* title, int id, int style) {
  std::unique_ptr<Window> child(new Window(this, L"Button", title));

  child->Create(WS_CHILD | WS_VISIBLE | WS_TABSTOP | style, pos_x, pos_y, size_x, size_y,
                id);

  return child;
}

std::unique_ptr<Window> Window::CreateCheckBox(int pos_x, int pos_y, int size_x,
                                               int size_y, const wchar_t* title, int id,
                                               bool state) {
  std::unique_ptr<Window> child(new Window(this, L"Button", title));

  child->Create(WS_CHILD | WS_VISIBLE | BS_CHECKBOX | WS_TABSTOP, pos_x, pos_y, size_x,
                size_y, id);

  child->SetChecked(state);

  return child;
}

std::unique_ptr<Window> Window::CreateComboBox(int pos_x, int pos_y, int size_x,
                                               int size_y, const wchar_t* title, int id) {
  std::unique_ptr<Window> child(new Window(this, L"Combobox", title));

  child->Create(WS_CHILD | WS_VISIBLE | CBS_DROPDOWNLIST | WS_TABSTOP, pos_x, pos_y,
                size_x, size_y, id);

  return child;
}

void Window::Show() { ShowWindow(handle_, SW_SHOW); }

void Window::Update() { UpdateWindow(handle_); }

void Window::Destroy() {
  if (handle_) DestroyWindow(handle_);

  handle_ = NULL;
}

void Window::SetVisible(bool is_visible) {
  ShowWindow(handle_, is_visible ? SW_SHOW : SW_HIDE);
}

bool Window::IsTextEmpty() const {
  if (!IsEnabled()) {
    return true;
  }
  int len = GetWindowTextLength(handle_);

  return (len <= 0);
}

void Window::ListAddColumn(const std::wstring& name, int index, int width) {
  LVCOLUMN lvc;
  lvc.mask = LVCF_FMT | LVCF_WIDTH | LVCF_TEXT | LVCF_SUBITEM;
  lvc.fmt = LVCFMT_LEFT;
  lvc.cx = width;
  lvc.pszText = const_cast<wchar_t*>(name.c_str());
  lvc.iSubItem = index;

  if (ListView_InsertColumn(handle_, index, &lvc) == -1) {
    std::stringstream buf;
    buf << "Can not add list column, error code: " << GetLastError();
    throw DriverException(buf.str());
  }
}

void Window::ListAddItem(const std::vector<std::wstring>& items) {
  LVITEM lvi = {0};
  lvi.mask = LVIF_TEXT;
  lvi.pszText = const_cast<wchar_t*>(items[0].c_str());

  int ret = ListView_InsertItem(handle_, &lvi);
  if (ret < 0) {
    std::stringstream buf;
    buf << "Can not add list item, error code: " << GetLastError();
    throw DriverException(buf.str());
  }

  for (size_t i = 1; i < items.size(); ++i) {
    ListView_SetItemText(handle_, ret, static_cast<int>(i),
                         const_cast<wchar_t*>(items[i].c_str()));
  }
}

void Window::ListDeleteSelectedItem() {
  const int row_index = ListView_GetSelectionMark(handle_);
  if (row_index >= 0) {
    if (ListView_DeleteItem(handle_, row_index) == -1) {
      std::stringstream buf;
      buf << "Can not delete list item, error code: " << GetLastError();
      throw DriverException(buf.str());
    }
  }
}

std::vector<std::vector<std::wstring> > Window::ListGetAll() {
#define BUF_LEN 1024
  wchar_t buf[BUF_LEN];

  std::vector<std::vector<std::wstring> > values;
  const int num_columns = Header_GetItemCount(ListView_GetHeader(handle_));
  const int num_items = ListView_GetItemCount(handle_);
  for (int i = 0; i < num_items; ++i) {
    std::vector<std::wstring> row;
    for (int j = 0; j < num_columns; ++j) {
      ListView_GetItemText(handle_, i, j, buf, BUF_LEN);
      row.emplace_back(buf);
    }
    values.push_back(row);
  }

  return values;
}

void Window::AddTab(const std::wstring& name, int index) {
  TCITEM tab_control_item;
  tab_control_item.mask = TCIF_TEXT | TCIF_IMAGE;
  tab_control_item.iImage = -1;
  tab_control_item.pszText = const_cast<wchar_t*>(name.c_str());
  if (TabCtrl_InsertItem(handle_, index, &tab_control_item) == -1) {
    std::stringstream buf;
    buf << "Can not add tab, error code: " << GetLastError();
    throw DriverException(buf.str());
  }
}

void Window::GetText(std::wstring& text) const {
  if (!IsEnabled()) {
    text.clear();

    return;
  }

  int len = GetWindowTextLength(handle_);

  if (len <= 0) {
    text.clear();

    return;
  }

  text.resize(len + 1);

  if (!GetWindowText(handle_, &text[0], len + 1)) text.clear();

  text.resize(len);
  boost::algorithm::trim(text);
}

void Window::SetText(const std::wstring& text) const {
  SNDMSG(handle_, WM_SETTEXT, 0, reinterpret_cast<LPARAM>(text.c_str()));
}

bool Window::IsChecked() const {
  return IsEnabled() && Button_GetCheck(handle_) == BST_CHECKED;
}

void Window::SetChecked(bool state) {
  Button_SetCheck(handle_, state ? BST_CHECKED : BST_UNCHECKED);
}

void Window::AddString(const std::wstring& str) {
  SNDMSG(handle_, CB_ADDSTRING, 0, reinterpret_cast<LPARAM>(str.c_str()));
}

void Window::SetSelection(int idx) {
  SNDMSG(handle_, CB_SETCURSEL, static_cast<WPARAM>(idx), 0);
}

int Window::GetSelection() const {
  return static_cast<int>(SNDMSG(handle_, CB_GETCURSEL, 0, 0));
}

void Window::SetEnabled(bool enabled) { EnableWindow(GetHandle(), enabled); }

bool Window::IsEnabled() const { return IsWindowEnabled(GetHandle()) != 0; }

}  // namespace config
}  // namespace arrow::flight::sql::odbc
