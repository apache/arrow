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
(unknown changeset), with the following patch for ARROW-15388:

```diff
diff --git a/cpp/thirdparty/flatbuffers/include/flatbuffers/base.h b/cpp/thirdparty/flatbuffers/include/flatbuffers/base.h
index 955738067..f8277b3e9 100644
--- a/cpp/thirdparty/flatbuffers/include/flatbuffers/base.h
+++ b/cpp/thirdparty/flatbuffers/include/flatbuffers/base.h
@@ -212,11 +212,11 @@ namespace flatbuffers {
         typedef std::experimental::string_view string_view;
       }
       #define FLATBUFFERS_HAS_STRING_VIEW 1
-    // Check for absl::string_view
-    #elif __has_include("absl/strings/string_view.h")
-      #include "absl/strings/string_view.h"
+    // Check for arrow::util::string_view
+    #elif __has_include("arrow/util/string_view.h")
+      #include "arrow/util/string_view.h"
       namespace flatbuffers {
-        typedef absl::string_view string_view;
+        typedef arrow::util::string_view string_view;
       }
       #define FLATBUFFERS_HAS_STRING_VIEW 1
     #endif
```
