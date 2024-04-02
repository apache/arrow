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

#include "ui/window.h"

namespace driver {
namespace flight_sql {
namespace config {
/**
 * Application execution result.
 */
struct Result {
  enum Type { OK, CANCEL };
};

/**
 * Process UI messages in current thread.
 * Blocks until quit message has been received.
 *
 * @param window Main window.
 * @return Application execution result.
 */
Result::Type ProcessMessages(Window& window);

/**
 * Window class.
 */
class CustomWindow : public Window {
 public:
  // Window margin size.
  enum { MARGIN = 10 };

  // Standard interval between UI elements.
  enum { INTERVAL = 10 };

  // Standard row height.
  enum { ROW_HEIGHT = 20 };

  // Standard button width.
  enum { BUTTON_WIDTH = 80 };

  // Standard button height.
  enum { BUTTON_HEIGHT = 25 };

  /**
   * Constructor.
   *
   * @param parent Parent window.
   * @param className Window class name.
   * @param title Window title.
   */
  CustomWindow(Window* parent, const char* className, const char* title);

  /**
   * Destructor.
   */
  virtual ~CustomWindow();

  /**
   * Callback which is called upon receiving new message.
   * Pure virtual. Should be defined by user.
   *
   * @param msg Message.
   * @param wParam Word-sized parameter.
   * @param lParam Long parameter.
   * @return Should return true if the message has been
   *     processed by the handler and false otherwise.
   */
  virtual bool OnMessage(UINT msg, WPARAM wParam, LPARAM lParam) = 0;

  /**
   * Callback that is called upon window creation.
   */
  virtual void OnCreate() = 0;

 private:
  //       IGNITE_NO_COPY_ASSIGNMENT(CustomWindow)

  /**
   * Static callback.
   *
   * @param hwnd Window handle.
   * @param msg Message.
   * @param wParam Word-sized parameter.
   * @param lParam Long parameter.
   * @return Operation result.
   */
  static LRESULT CALLBACK WndProc(HWND hwnd, UINT msg, WPARAM wParam, LPARAM lParam);
};

}  // namespace config
}  // namespace flight_sql
}  // namespace driver
