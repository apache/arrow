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

#include <memory>
#include <string>
#include <vector>

namespace arrow::flight::sql::odbc {
namespace config {

/**
 * Get handle for the current module.
 *
 * @return Handle for the current module.
 */
HINSTANCE GetHInstance();

/**
 * Window class.
 */
class Window {
 public:
  /**
   * Constructor for a new window that is going to be created.
   *
   * @param parent Parent window handle.
   * @param class_name Window class name.
   * @param title Window title.
   */
  Window(Window* parent, const wchar_t* class_name, const wchar_t* title);

  /**
   * Constructor for the existing window.
   *
   * @param handle Window handle.
   */
  explicit Window(HWND handle);

  /**
   * Destructor.
   */
  virtual ~Window();

  /**
   * Create window.
   *
   * @param style Window style.
   * @param pos_x Window x position.
   * @param pos_y Window y position.
   * @param width Window width.
   * @param height Window height.
   * @param id ID for child window.
   */
  void Create(DWORD style, int pos_x, int pos_y, int width, int height, int id);

  /**
   * Create child tab controlwindow.
   *
   * @param id ID to be assigned to the created window.
   * @return Auto pointer containing new window.
   */
  std::unique_ptr<Window> CreateTabControl(int id);

  /**
   * Create child list view window.
   *
   * @param pos_x Position by X coordinate.
   * @param pos_y Position by Y coordinate.
   * @param size_x Size by X coordinate.
   * @param size_y Size by Y coordinate.
   * @param id ID to be assigned to the created window.
   * @return Auto pointer containing new window.
   */
  std::unique_ptr<Window> CreateList(int pos_x, int pos_y, int size_x, int size_y,
                                     int id);

  /**
   * Create child group box window.
   *
   * @param pos_x Position by X coordinate.
   * @param pos_y Position by Y coordinate.
   * @param size_x Size by X coordinate.
   * @param size_y Size by Y coordinate.
   * @param title Title.
   * @param id ID to be assigned to the created window.
   * @return Auto pointer containing new window.
   */
  std::unique_ptr<Window> CreateGroupBox(int pos_x, int pos_y, int size_x, int size_y,
                                         const wchar_t* title, int id);

  /**
   * Create child label window.
   *
   * @param pos_x Position by X coordinate.
   * @param pos_y Position by Y coordinate.
   * @param size_x Size by X coordinate.
   * @param size_y Size by Y coordinate.
   * @param title Title.
   * @param id ID to be assigned to the created window.
   * @return Auto pointer containing new window.
   */
  std::unique_ptr<Window> CreateLabel(int pos_x, int pos_y, int size_x, int size_y,
                                      const wchar_t* title, int id);

  /**
   * Create child Edit window.
   *
   * @param pos_x Position by X coordinate.
   * @param pos_y Position by Y coordinate.
   * @param size_x Size by X coordinate.
   * @param size_y Size by Y coordinate.
   * @param title Title.
   * @param id ID to be assigned to the created window.
   * @param style Window style.
   * @return Auto pointer containing new window.
   */
  std::unique_ptr<Window> CreateEdit(int pos_x, int pos_y, int size_x, int size_y,
                                     const wchar_t* title, int id, int style = 0);

  /**
   * Create child button window.
   *
   * @param pos_x Position by X coordinate.
   * @param pos_y Position by Y coordinate.
   * @param size_x Size by X coordinate.
   * @param size_y Size by Y coordinate.
   * @param title Title.
   * @param id ID to be assigned to the created window.
   * @param style Window style.
   * @return Auto pointer containing new window.
   */
  std::unique_ptr<Window> CreateButton(int pos_x, int pos_y, int size_x, int size_y,
                                       const wchar_t* title, int id, int style = 0);

  /**
   * Create child CheckBox window.
   *
   * @param pos_x Position by X coordinate.
   * @param pos_y Position by Y coordinate.
   * @param size_x Size by X coordinate.
   * @param size_y Size by Y coordinate.
   * @param title Title.
   * @param id ID to be assigned to the created window.
   * @param state Checked state of checkbox
   * @return Auto pointer containing new window.
   */
  std::unique_ptr<Window> CreateCheckBox(int pos_x, int pos_y, int size_x, int size_y,
                                         const wchar_t* title, int id, bool state);

  /**
   * Create child ComboBox window.
   *
   * @param pos_x Position by X coordinate.
   * @param pos_y Position by Y coordinate.
   * @param size_x Size by X coordinate.
   * @param size_y Size by Y coordinate.
   * @param title Title.
   * @param id ID to be assigned to the created window.
   * @return Auto pointer containing new window.
   */
  std::unique_ptr<Window> CreateComboBox(int pos_x, int pos_y, int size_x, int size_y,
                                         const wchar_t* title, int id);

  /**
   * Show window.
   */
  void Show();

  /**
   * Update window.
   */
  void Update();

  /**
   * Destroy window.
   */
  void Destroy();

  /**
   * Get window handle.
   *
   * @return Window handle.
   */
  HWND GetHandle() const { return handle_; }

  void SetVisible(bool isVisible);

  void ListAddColumn(const std::wstring& name, int index, int width);

  void ListAddItem(const std::vector<std::wstring>& items);

  void ListDeleteSelectedItem();

  std::vector<std::vector<std::wstring> > ListGetAll();

  void AddTab(const std::wstring& name, int index);

  bool IsTextEmpty() const;

  /**
   * Get window text.
   *
   * @param text Text.
   */
  void GetText(std::wstring& text) const;

  /**
   * Set window text.
   *
   * @param text Text.
   */
  void SetText(const std::wstring& text) const;

  /**
   * Get CheckBox state.
   *
   * @return True if checked.
   */
  bool IsChecked() const;

  /**
   * Set CheckBox state.
   *
   * @param state True if checked.
   */
  void SetChecked(bool state);

  /**
   * Add string.
   *
   * @param str String.
   */
  void AddString(const std::wstring& str);

  /**
   * Set current ComboBox selection.
   *
   * @param idx List index.
   */
  void SetSelection(int idx);

  /**
   * Get current ComboBox selection.
   *
   * @return idx List index.
   */
  int GetSelection() const;

  /**
   * Set enabled.
   *
   * @param enabled Enable flag.
   */
  void SetEnabled(bool enabled);

  /**
   * Check if the window is enabled.
   *
   * @return True if enabled.
   */
  bool IsEnabled() const;

 protected:
  /**
   * Set window handle.
   *
   * @param value Window handle.
   */
  void SetHandle(HWND value) { handle_ = value; }

  /** Window class name. */
  std::wstring class_name_;

  /** Window title. */
  std::wstring title_;

  /** Window handle. */
  HWND handle_;

  /** Window parent. */
  Window* parent_;

  /** Specifies whether window has been created by the thread and needs destruction. */
  bool created_;

 private:
  Window(const Window& window) = delete;
};

}  // namespace config
}  // namespace arrow::flight::sql::odbc
