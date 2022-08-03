<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

This directory contains a vendored version of Flatbuffers
(unknown changeset), with two patches: the first patch
for ARROW-15388 and the second patch for ARROW-17280.

```diff
diff --git a/cpp/thirdparty/flatbuffers/include/flatbuffers/base.h b/cpp/thirdparty/flatbuffers/include/flatbuffers/base.h
index 955738067..fccce42f6 100644
--- a/cpp/thirdparty/flatbuffers/include/flatbuffers/base.h
+++ b/cpp/thirdparty/flatbuffers/include/flatbuffers/base.h
@@ -212,13 +212,6 @@ namespace flatbuffers {
         typedef std::experimental::string_view string_view;
       }
       #define FLATBUFFERS_HAS_STRING_VIEW 1
-    // Check for absl::string_view
-    #elif __has_include("absl/strings/string_view.h")
-      #include "absl/strings/string_view.h"
-      namespace flatbuffers {
-        typedef absl::string_view string_view;
-      }
-      #define FLATBUFFERS_HAS_STRING_VIEW 1
     #endif
   #endif // __has_include
 #endif // !FLATBUFFERS_HAS_STRING_VIEW
```

```diff
diff --git a/cpp/thirdparty/flatbuffers/include/flatbuffers/base.h b/cpp/thirdparty/flatbuffers/include/flatbuffers/base.h
index fccce42f6..a00d5b0fd 100644
--- a/cpp/thirdparty/flatbuffers/include/flatbuffers/base.h
+++ b/cpp/thirdparty/flatbuffers/include/flatbuffers/base.h
@@ -1,6 +1,14 @@
 #ifndef FLATBUFFERS_BASE_H_
 #define FLATBUFFERS_BASE_H_
 
+// Move this vendored copy of flatbuffers to a private namespace,
+// but continue to access it through the "flatbuffers" alias.
+namespace arrow_vendored_private {
+namespace flatbuffers {
+}
+}
+namespace flatbuffers = arrow_vendored_private::flatbuffers;
+
 // clang-format off
 
 // If activate should be declared and included first.
@@ -144,10 +152,12 @@
 #define FLATBUFFERS_VERSION_REVISION 0
 #define FLATBUFFERS_STRING_EXPAND(X) #X
 #define FLATBUFFERS_STRING(X) FLATBUFFERS_STRING_EXPAND(X)
+namespace arrow_vendored_private {
 namespace flatbuffers {
   // Returns version as string  "MAJOR.MINOR.REVISION".
   const char* FLATBUFFERS_VERSION();
 }
+}
 
 #if (!defined(_MSC_VER) || _MSC_VER > 1600) && \
     (!defined(__GNUC__) || (__GNUC__ * 100 + __GNUC_MINOR__ >= 407)) || \
@@ -201,16 +211,20 @@ namespace flatbuffers {
     // Check for std::string_view (in c++17)
     #if __has_include(<string_view>) && (__cplusplus >= 201606 || (defined(_HAS_CXX17) && _HAS_CXX17))
       #include <string_view>
+      namespace arrow_vendored_private {
       namespace flatbuffers {
         typedef std::string_view string_view;
       }
+      }
       #define FLATBUFFERS_HAS_STRING_VIEW 1
     // Check for std::experimental::string_view (in c++14, compiler-dependent)
     #elif __has_include(<experimental/string_view>) && (__cplusplus >= 201411)
       #include <experimental/string_view>
+      namespace arrow_vendored_private {
       namespace flatbuffers {
         typedef std::experimental::string_view string_view;
       }
+      }
       #define FLATBUFFERS_HAS_STRING_VIEW 1
     #endif
   #endif // __has_include
@@ -278,6 +292,7 @@ template<typename T> FLATBUFFERS_CONSTEXPR inline bool IsConstTrue(T t) {
 /// @endcond
 
 /// @file
+namespace arrow_vendored_private {
 namespace flatbuffers {
 
 /// @cond FLATBUFFERS_INTERNAL
@@ -388,4 +403,5 @@ inline size_t PaddingBytes(size_t buf_size, size_t scalar_size) {
 }
 
 }  // namespace flatbuffers
+}  // namespace arrow_vendored_private
 #endif  // FLATBUFFERS_BASE_H_
diff --git a/cpp/thirdparty/flatbuffers/include/flatbuffers/flatbuffers.h b/cpp/thirdparty/flatbuffers/include/flatbuffers/flatbuffers.h
index c4dc5bcd0..2f7eb5fcf 100644
--- a/cpp/thirdparty/flatbuffers/include/flatbuffers/flatbuffers.h
+++ b/cpp/thirdparty/flatbuffers/include/flatbuffers/flatbuffers.h
@@ -23,6 +23,15 @@
 #  include <cmath>
 #endif
 
+// Move this vendored copy of flatbuffers to a private namespace,
+// but continue to access it through the "flatbuffers" alias.
+namespace arrow_vendored_private {
+namespace flatbuffers {
+}
+}
+namespace flatbuffers = arrow_vendored_private::flatbuffers;
+
+namespace arrow_vendored_private {
 namespace flatbuffers {
 // Generic 'operator==' with conditional specialisations.
 // T e - new value of a scalar field.
@@ -2777,6 +2786,7 @@ volatile __attribute__((weak)) const char *flatbuffer_version_string =
     }
 /// @endcond
 }  // namespace flatbuffers
+}  // namespace arrow_vendored_private
 
 // clang-format on
 
diff --git a/cpp/thirdparty/flatbuffers/include/flatbuffers/stl_emulation.h b/cpp/thirdparty/flatbuffers/include/flatbuffers/stl_emulation.h
index 8bae61bfd..7e5a95233 100644
--- a/cpp/thirdparty/flatbuffers/include/flatbuffers/stl_emulation.h
+++ b/cpp/thirdparty/flatbuffers/include/flatbuffers/stl_emulation.h
@@ -25,6 +25,14 @@
 #include <memory>
 #include <limits>
 
+// Move this vendored copy of flatbuffers to a private namespace,
+// but continue to access it through the "flatbuffers" alias.
+namespace arrow_vendored_private {
+namespace flatbuffers {
+}
+}
+namespace flatbuffers = arrow_vendored_private::flatbuffers;
+
 #if defined(_STLPORT_VERSION) && !defined(FLATBUFFERS_CPP98_STL)
   #define FLATBUFFERS_CPP98_STL
 #endif  // defined(_STLPORT_VERSION) && !defined(FLATBUFFERS_CPP98_STL)
@@ -44,6 +52,7 @@
 #endif
 
 // This header provides backwards compatibility for C++98 STLs like stlport.
+namespace arrow_vendored_private {
 namespace flatbuffers {
 
 // Retrieve ::back() from a string in a way that is compatible with pre C++11
@@ -303,5 +312,6 @@ inline void vector_emplace_back(std::vector<T> *vector, V &&data) {
 #endif  // !FLATBUFFERS_CPP98_STL
 
 }  // namespace flatbuffers
+}  // namespace arrow_vendored_private
 
 #endif  // FLATBUFFERS_STL_EMULATION_H_
-- 
2.25.1
```
