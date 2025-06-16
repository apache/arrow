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

#include <wtypes.h>
#include <sstream>

#include "arrow/flight/sql/odbc/odbc_impl/exceptions.h"
#include "ui/custom_window.h"

namespace arrow::flight::sql::odbc {
namespace config {

Result::Type ProcessMessages(Window& window) {
  MSG msg;

  while (GetMessage(&msg, NULL, 0, 0) > 0) {
    if (!IsDialogMessage(window.GetHandle(), &msg)) {
      TranslateMessage(&msg);

      DispatchMessage(&msg);
    }
  }

  return static_cast<Result::Type>(msg.wParam);
}

LRESULT CALLBACK CustomWindow::WndProc(HWND hwnd, UINT msg, WPARAM wparam,
                                       LPARAM lparam) {
  CustomWindow* window =
      reinterpret_cast<CustomWindow*>(GetWindowLongPtr(hwnd, GWLP_USERDATA));

  switch (msg) {
    case WM_NCCREATE: {
      _ASSERT(lparam != NULL);

      CREATESTRUCT* create_struct = reinterpret_cast<CREATESTRUCT*>(lparam);

      LONG_PTR long_self_ptr = reinterpret_cast<LONG_PTR>(create_struct->lpCreateParams);

      SetWindowLongPtr(hwnd, GWLP_USERDATA, long_self_ptr);

      return DefWindowProc(hwnd, msg, wparam, lparam);
    }

    case WM_CREATE: {
      _ASSERT(window != NULL);

      window->SetHandle(hwnd);

      window->OnCreate();

      return 0;
    }

    default:
      break;
  }

  if (window && window->OnMessage(msg, wparam, lparam)) return 0;

  return DefWindowProc(hwnd, msg, wparam, lparam);
}

CustomWindow::CustomWindow(Window* parent, const wchar_t* class_name, const wchar_t* title)
    : Window(parent, class_name, title) {
  WNDCLASS wcx;

  wcx.style = CS_HREDRAW | CS_VREDRAW;
  wcx.lpfnWndProc = WndProc;
  wcx.cbClsExtra = 0;
  wcx.cbWndExtra = 0;
  wcx.hInstance = GetHInstance();
  wcx.hIcon = NULL;
  wcx.hCursor = LoadCursor(NULL, IDC_ARROW);
  wcx.hbrBackground = (HBRUSH)COLOR_WINDOW;
  wcx.lpszMenuName = NULL;
  wcx.lpszClassName = class_name;

  if (!RegisterClass(&wcx)) {
    std::stringstream buf;
    buf << "Can not register window class, error code: " << GetLastError();
    throw DriverException(buf.str());
  }
}

CustomWindow::~CustomWindow() { UnregisterClass(class_name_.c_str(), GetHInstance()); }

}  // namespace config
}  // namespace arrow::flight::sql::odbc
