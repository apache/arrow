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
#include <arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/platform.h>

#include <Windowsx.h>
#include <commctrl.h>

#include <wtypes.h>
#include <sstream>

#include <arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/exceptions.h>
#include "ui/custom_window.h"

namespace driver {
namespace flight_sql {
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

LRESULT CALLBACK CustomWindow::WndProc(HWND hwnd, UINT msg, WPARAM wParam,
                                       LPARAM lParam) {
  CustomWindow* window =
      reinterpret_cast<CustomWindow*>(GetWindowLongPtr(hwnd, GWLP_USERDATA));

  switch (msg) {
    case WM_NCCREATE: {
      _ASSERT(lParam != NULL);

      CREATESTRUCT* createStruct = reinterpret_cast<CREATESTRUCT*>(lParam);

      LONG_PTR longSelfPtr = reinterpret_cast<LONG_PTR>(createStruct->lpCreateParams);

      SetWindowLongPtr(hwnd, GWLP_USERDATA, longSelfPtr);

      return DefWindowProc(hwnd, msg, wParam, lParam);
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

  if (window && window->OnMessage(msg, wParam, lParam)) return 0;

  return DefWindowProc(hwnd, msg, wParam, lParam);
}

CustomWindow::CustomWindow(Window* parent, const char* className, const char* title)
    : Window(parent, className, title) {
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
  wcx.lpszClassName = className;

  if (!RegisterClass(&wcx)) {
    std::stringstream buf;
    buf << "Can not register window class, error code: " << GetLastError();
    throw odbcabstraction::DriverException(buf.str());
  }
}

CustomWindow::~CustomWindow() { UnregisterClass(className.c_str(), GetHInstance()); }

}  // namespace config
}  // namespace flight_sql
}  // namespace driver
