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
(v23.5.26), with a patch for ARROW-17280.

```diff
diff --git a/cpp/thirdparty/flatbuffers/include/flatbuffers/allocator.h b/cpp/thirdparty/flatbuffers/include/flatbuffers/allocator.h
index 30427190b..f0e91d0a3 100644
--- a/cpp/thirdparty/flatbuffers/include/flatbuffers/allocator.h
+++ b/cpp/thirdparty/flatbuffers/include/flatbuffers/allocator.h
@@ -17,9 +17,15 @@
 #ifndef FLATBUFFERS_ALLOCATOR_H_
 #define FLATBUFFERS_ALLOCATOR_H_
 
+// Move this vendored copy of flatbuffers to a private namespace,
+// but continue to access it through the "flatbuffers" alias.
+namespace arrow_vendored_private::flatbuffers {
+}
+namespace flatbuffers = arrow_vendored_private::flatbuffers;
+
 #include "flatbuffers/base.h"
 
-namespace flatbuffers {
+namespace arrow_vendored_private::flatbuffers {
 
 // Allocator interface. This is flatbuffers-specific and meant only for
 // `vector_downward` usage.
@@ -63,6 +69,6 @@ class Allocator {
   }
 };
 
-}  // namespace flatbuffers
+}  // namespace arrow_vendored_private
 
 #endif  // FLATBUFFERS_ALLOCATOR_H_
diff --git a/cpp/thirdparty/flatbuffers/include/flatbuffers/array.h b/cpp/thirdparty/flatbuffers/include/flatbuffers/array.h
index f4bfbf054..f8e78243a 100644
--- a/cpp/thirdparty/flatbuffers/include/flatbuffers/array.h
+++ b/cpp/thirdparty/flatbuffers/include/flatbuffers/array.h
@@ -17,6 +17,12 @@
 #ifndef FLATBUFFERS_ARRAY_H_
 #define FLATBUFFERS_ARRAY_H_
 
+// Move this vendored copy of flatbuffers to a private namespace,
+// but continue to access it through the "flatbuffers" alias.
+namespace arrow_vendored_private::flatbuffers {
+}
+namespace flatbuffers = arrow_vendored_private::flatbuffers;
+
 #include <cstdint>
 #include <memory>
 
@@ -24,7 +30,7 @@
 #include "flatbuffers/stl_emulation.h"
 #include "flatbuffers/vector.h"
 
-namespace flatbuffers {
+namespace arrow_vendored_private::flatbuffers {
 
 // This is used as a helper type for accessing arrays.
 template<typename T, uint16_t length> class Array {
@@ -251,6 +257,6 @@ bool operator==(const Array<T, length> &lhs,
           std::memcmp(lhs.Data(), rhs.Data(), rhs.size() * sizeof(T)) == 0);
 }
 
-}  // namespace flatbuffers
+}  // namespace arrow_vendored_private
 
 #endif  // FLATBUFFERS_ARRAY_H_
diff --git a/cpp/thirdparty/flatbuffers/include/flatbuffers/base.h b/cpp/thirdparty/flatbuffers/include/flatbuffers/base.h
index 5c4cae791..1b0c7d987 100644
--- a/cpp/thirdparty/flatbuffers/include/flatbuffers/base.h
+++ b/cpp/thirdparty/flatbuffers/include/flatbuffers/base.h
@@ -1,6 +1,12 @@
 #ifndef FLATBUFFERS_BASE_H_
 #define FLATBUFFERS_BASE_H_
 
+// Move this vendored copy of flatbuffers to a private namespace,
+// but continue to access it through the "flatbuffers" alias.
+namespace arrow_vendored_private::flatbuffers {
+}
+namespace flatbuffers = arrow_vendored_private::flatbuffers;
+
 // clang-format off
 
 // If activate should be declared and included first.
@@ -144,10 +150,13 @@
 #define FLATBUFFERS_VERSION_REVISION 26
 #define FLATBUFFERS_STRING_EXPAND(X) #X
 #define FLATBUFFERS_STRING(X) FLATBUFFERS_STRING_EXPAND(X)
+
+namespace arrow_vendored_private {
 namespace flatbuffers {
   // Returns version as string  "MAJOR.MINOR.REVISION".
   const char* FLATBUFFERS_VERSION();
 }
+}
 
 #if (!defined(_MSC_VER) || _MSC_VER > 1600) && \
     (!defined(__GNUC__) || (__GNUC__ * 100 + __GNUC_MINOR__ >= 407)) || \
@@ -222,14 +231,14 @@ namespace flatbuffers {
     // Check for std::string_view (in c++17)
     #if __has_include(<string_view>) && (__cplusplus >= 201606 || (defined(_HAS_CXX17) && _HAS_CXX17))
       #include <string_view>
-      namespace flatbuffers {
+      namespace arrow_vendored_private::flatbuffers {
         typedef std::string_view string_view;
       }
       #define FLATBUFFERS_HAS_STRING_VIEW 1
     // Check for std::experimental::string_view (in c++14, compiler-dependent)
     #elif __has_include(<experimental/string_view>) && (__cplusplus >= 201411)
       #include <experimental/string_view>
-      namespace flatbuffers {
+      namespace arrow_vendored_private::flatbuffers {
         typedef std::experimental::string_view string_view;
       }
       #define FLATBUFFERS_HAS_STRING_VIEW 1
@@ -240,7 +249,7 @@ namespace flatbuffers {
       #include "absl/base/config.h"
       #if !defined(ABSL_USES_STD_STRING_VIEW)
         #include "absl/strings/string_view.h"
-        namespace flatbuffers {
+        namespace arrow_vendored_private::flatbuffers {
           typedef absl::string_view string_view;
         }
         #define FLATBUFFERS_HAS_STRING_VIEW 1
@@ -317,6 +326,7 @@ template<typename T> FLATBUFFERS_CONSTEXPR inline bool IsConstTrue(T t) {
 /// @endcond
 
 /// @file
+namespace arrow_vendored_private {
 namespace flatbuffers {
 
 /// @cond FLATBUFFERS_INTERNAL
@@ -492,4 +502,6 @@ inline bool IsInRange(const T &v, const T &low, const T &high) {
 }
 
 }  // namespace flatbuffers
+}  // namespace arrow_vendored_private
+
 #endif  // FLATBUFFERS_BASE_H_
diff --git a/cpp/thirdparty/flatbuffers/include/flatbuffers/buffer.h b/cpp/thirdparty/flatbuffers/include/flatbuffers/buffer.h
index 94d4f7903..e791c3007 100644
--- a/cpp/thirdparty/flatbuffers/include/flatbuffers/buffer.h
+++ b/cpp/thirdparty/flatbuffers/include/flatbuffers/buffer.h
@@ -17,11 +17,17 @@
 #ifndef FLATBUFFERS_BUFFER_H_
 #define FLATBUFFERS_BUFFER_H_
 
+// Move this vendored copy of flatbuffers to a private namespace,
+// but continue to access it through the "flatbuffers" alias.
+namespace arrow_vendored_private::flatbuffers {
+}
+namespace flatbuffers = arrow_vendored_private::flatbuffers;
+
 #include <algorithm>
 
 #include "flatbuffers/base.h"
 
-namespace flatbuffers {
+namespace arrow_vendored_private::flatbuffers {
 
 // Wrapper for uoffset_t to allow safe template specialization.
 // Value is allowed to be 0 to indicate a null object (see e.g. AddOffset).
@@ -194,6 +200,6 @@ const T *GetSizePrefixedRoot(const void *buf) {
   return GetRoot<T>(reinterpret_cast<const uint8_t *>(buf) + sizeof(SizeT));
 }
 
-}  // namespace flatbuffers
+}  // namespace arrow_vendored_private
 
 #endif  // FLATBUFFERS_BUFFER_H_
diff --git a/cpp/thirdparty/flatbuffers/include/flatbuffers/buffer_ref.h b/cpp/thirdparty/flatbuffers/include/flatbuffers/buffer_ref.h
index f70941fc6..8a4a43f81 100644
--- a/cpp/thirdparty/flatbuffers/include/flatbuffers/buffer_ref.h
+++ b/cpp/thirdparty/flatbuffers/include/flatbuffers/buffer_ref.h
@@ -17,10 +17,16 @@
 #ifndef FLATBUFFERS_BUFFER_REF_H_
 #define FLATBUFFERS_BUFFER_REF_H_
 
+// Move this vendored copy of flatbuffers to a private namespace,
+// but continue to access it through the "flatbuffers" alias.
+namespace arrow_vendored_private::flatbuffers {
+}
+namespace flatbuffers = arrow_vendored_private::flatbuffers;
+
 #include "flatbuffers/base.h"
 #include "flatbuffers/verifier.h"
 
-namespace flatbuffers {
+namespace arrow_vendored_private::flatbuffers {
 
 // Convenient way to bundle a buffer and its length, to pass it around
 // typed by its root.
@@ -48,6 +54,6 @@ template<typename T> struct BufferRef : BufferRefBase {
   bool must_free;
 };
 
-}  // namespace flatbuffers
+}  // namespace arrow_vendored_private
 
 #endif  // FLATBUFFERS_BUFFER_REF_H_
diff --git a/cpp/thirdparty/flatbuffers/include/flatbuffers/default_allocator.h b/cpp/thirdparty/flatbuffers/include/flatbuffers/default_allocator.h
index d4724122c..fd2dc60a9 100644
--- a/cpp/thirdparty/flatbuffers/include/flatbuffers/default_allocator.h
+++ b/cpp/thirdparty/flatbuffers/include/flatbuffers/default_allocator.h
@@ -17,10 +17,16 @@
 #ifndef FLATBUFFERS_DEFAULT_ALLOCATOR_H_
 #define FLATBUFFERS_DEFAULT_ALLOCATOR_H_
 
+// Move this vendored copy of flatbuffers to a private namespace,
+// but continue to access it through the "flatbuffers" alias.
+namespace arrow_vendored_private::flatbuffers {
+}
+namespace flatbuffers = arrow_vendored_private::flatbuffers;
+
 #include "flatbuffers/allocator.h"
 #include "flatbuffers/base.h"
 
-namespace flatbuffers {
+namespace arrow_vendored_private::flatbuffers {
 
 // DefaultAllocator uses new/delete to allocate memory regions
 class DefaultAllocator : public Allocator {
@@ -59,6 +65,6 @@ inline uint8_t *ReallocateDownward(Allocator *allocator, uint8_t *old_p,
                          old_p, old_size, new_size, in_use_back, in_use_front);
 }
 
-}  // namespace flatbuffers
+}  // namespace arrow_vendored_private
 
 #endif  // FLATBUFFERS_DEFAULT_ALLOCATOR_H_
diff --git a/cpp/thirdparty/flatbuffers/include/flatbuffers/detached_buffer.h b/cpp/thirdparty/flatbuffers/include/flatbuffers/detached_buffer.h
index 5e900baeb..4779545b3 100644
--- a/cpp/thirdparty/flatbuffers/include/flatbuffers/detached_buffer.h
+++ b/cpp/thirdparty/flatbuffers/include/flatbuffers/detached_buffer.h
@@ -17,11 +17,17 @@
 #ifndef FLATBUFFERS_DETACHED_BUFFER_H_
 #define FLATBUFFERS_DETACHED_BUFFER_H_
 
+// Move this vendored copy of flatbuffers to a private namespace,
+// but continue to access it through the "flatbuffers" alias.
+namespace arrow_vendored_private::flatbuffers {
+}
+namespace flatbuffers = arrow_vendored_private::flatbuffers;
+
 #include "flatbuffers/allocator.h"
 #include "flatbuffers/base.h"
 #include "flatbuffers/default_allocator.h"
 
-namespace flatbuffers {
+namespace arrow_vendored_private::flatbuffers {
 
 // DetachedBuffer is a finished flatbuffer memory region, detached from its
 // builder. The original memory region and allocator are also stored so that
@@ -109,6 +115,6 @@ class DetachedBuffer {
   }
 };
 
-}  // namespace flatbuffers
+}  // namespace arrow_vendored_private
 
 #endif  // FLATBUFFERS_DETACHED_BUFFER_H_
diff --git a/cpp/thirdparty/flatbuffers/include/flatbuffers/flatbuffer_builder.h b/cpp/thirdparty/flatbuffers/include/flatbuffers/flatbuffer_builder.h
index 0a38b4ac3..5825cc05c 100644
--- a/cpp/thirdparty/flatbuffers/include/flatbuffers/flatbuffer_builder.h
+++ b/cpp/thirdparty/flatbuffers/include/flatbuffers/flatbuffer_builder.h
@@ -17,6 +17,12 @@
 #ifndef FLATBUFFERS_FLATBUFFER_BUILDER_H_
 #define FLATBUFFERS_FLATBUFFER_BUILDER_H_
 
+// Move this vendored copy of flatbuffers to a private namespace,
+// but continue to access it through the "flatbuffers" alias.
+namespace arrow_vendored_private::flatbuffers {
+}
+namespace flatbuffers = arrow_vendored_private::flatbuffers;
+
 #include <algorithm>
 #include <cstdint>
 #include <functional>
@@ -38,6 +44,7 @@
 #include "flatbuffers/vector_downward.h"
 #include "flatbuffers/verifier.h"
 
+namespace arrow_vendored_private {
 namespace flatbuffers {
 
 // Converts a Field ID to a virtual table offset.
@@ -1461,5 +1468,6 @@ const T *GetTemporaryPointer(FlatBufferBuilder &fbb, Offset<T> offset) {
 }
 
 }  // namespace flatbuffers
+}  // namespace arrow_vendored_private
 
 #endif  // FLATBUFFERS_FLATBUFFER_BUILDER_H_
diff --git a/cpp/thirdparty/flatbuffers/include/flatbuffers/flatbuffers.h b/cpp/thirdparty/flatbuffers/include/flatbuffers/flatbuffers.h
index bc828a313..8aa19a65c 100644
--- a/cpp/thirdparty/flatbuffers/include/flatbuffers/flatbuffers.h
+++ b/cpp/thirdparty/flatbuffers/include/flatbuffers/flatbuffers.h
@@ -17,6 +17,12 @@
 #ifndef FLATBUFFERS_H_
 #define FLATBUFFERS_H_
 
+// Move this vendored copy of flatbuffers to a private namespace,
+// but continue to access it through the "flatbuffers" alias.
+namespace arrow_vendored_private::flatbuffers {
+}
+namespace flatbuffers = arrow_vendored_private::flatbuffers;
+
 #include <algorithm>
 
 // TODO: These includes are for mitigating the pains of users editing their
@@ -35,6 +41,7 @@
 #include "flatbuffers/vector_downward.h"
 #include "flatbuffers/verifier.h"
 
+namespace arrow_vendored_private {
 namespace flatbuffers {
 
 /// @brief This can compute the start of a FlatBuffer from a root pointer, i.e.
@@ -278,6 +285,7 @@ inline const char *flatbuffers_version_string() {
     }
 /// @endcond
 }  // namespace flatbuffers
+}  // namespace arrow_vendored_private
 
 // clang-format on
 
diff --git a/cpp/thirdparty/flatbuffers/include/flatbuffers/stl_emulation.h b/cpp/thirdparty/flatbuffers/include/flatbuffers/stl_emulation.h
index fd3a8cda7..dcbe23347 100644
--- a/cpp/thirdparty/flatbuffers/include/flatbuffers/stl_emulation.h
+++ b/cpp/thirdparty/flatbuffers/include/flatbuffers/stl_emulation.h
@@ -26,6 +26,12 @@
 #include <memory>
 #include <limits>
 
+// Move this vendored copy of flatbuffers to a private namespace,
+// but continue to access it through the "flatbuffers" alias.
+namespace arrow_vendored_private::flatbuffers {
+}
+namespace flatbuffers = arrow_vendored_private::flatbuffers;
+
 #ifndef FLATBUFFERS_USE_STD_OPTIONAL
   // Detect C++17 compatible compiler.
   // __cplusplus >= 201703L - a compiler has support of 'static inline' variables.
@@ -64,6 +70,7 @@
 #endif // defined(FLATBUFFERS_USE_STD_SPAN)
 
 // This header provides backwards compatibility for older versions of the STL.
+namespace arrow_vendored_private {
 namespace flatbuffers {
 
 #if defined(FLATBUFFERS_TEMPLATES_ALIASES)
@@ -509,5 +516,6 @@ flatbuffers::span<const ElementType, dynamic_extent> make_span(const ElementType
 #endif // !defined(FLATBUFFERS_SPAN_MINIMAL)
 
 }  // namespace flatbuffers
+}  // namespace arrow_vendored_private
 
 #endif  // FLATBUFFERS_STL_EMULATION_H_
diff --git a/cpp/thirdparty/flatbuffers/include/flatbuffers/string.h b/cpp/thirdparty/flatbuffers/include/flatbuffers/string.h
index 97e399fd6..1cdba4588 100644
--- a/cpp/thirdparty/flatbuffers/include/flatbuffers/string.h
+++ b/cpp/thirdparty/flatbuffers/include/flatbuffers/string.h
@@ -17,9 +17,16 @@
 #ifndef FLATBUFFERS_STRING_H_
 #define FLATBUFFERS_STRING_H_
 
+// Move this vendored copy of flatbuffers to a private namespace,
+// but continue to access it through the "flatbuffers" alias.
+namespace arrow_vendored_private::flatbuffers {
+}
+namespace flatbuffers = arrow_vendored_private::flatbuffers;
+
 #include "flatbuffers/base.h"
 #include "flatbuffers/vector.h"
 
+namespace arrow_vendored_private {
 namespace flatbuffers {
 
 struct String : public Vector<char> {
@@ -60,5 +67,6 @@ static inline flatbuffers::string_view GetStringView(const String *str) {
 #endif  // FLATBUFFERS_HAS_STRING_VIEW
 
 }  // namespace flatbuffers
+}  // namespace arrow_vendored_private
 
 #endif  // FLATBUFFERS_STRING_H_
diff --git a/cpp/thirdparty/flatbuffers/include/flatbuffers/struct.h b/cpp/thirdparty/flatbuffers/include/flatbuffers/struct.h
index abacc8a9a..92a8e80bf 100644
--- a/cpp/thirdparty/flatbuffers/include/flatbuffers/struct.h
+++ b/cpp/thirdparty/flatbuffers/include/flatbuffers/struct.h
@@ -17,8 +17,15 @@
 #ifndef FLATBUFFERS_STRUCT_H_
 #define FLATBUFFERS_STRUCT_H_
 
+// Move this vendored copy of flatbuffers to a private namespace,
+// but continue to access it through the "flatbuffers" alias.
+namespace arrow_vendored_private::flatbuffers {
+}
+namespace flatbuffers = arrow_vendored_private::flatbuffers;
+
 #include "flatbuffers/base.h"
 
+namespace arrow_vendored_private {
 namespace flatbuffers {
 
 // "structs" are flat structures that do not have an offset table, thus
@@ -49,5 +56,6 @@ class Struct FLATBUFFERS_FINAL_CLASS {
 };
 
 }  // namespace flatbuffers
+}  // namespace arrow_vendored_private
 
 #endif  // FLATBUFFERS_STRUCT_H_
diff --git a/cpp/thirdparty/flatbuffers/include/flatbuffers/table.h b/cpp/thirdparty/flatbuffers/include/flatbuffers/table.h
index e92d8ae8e..762563daf 100644
--- a/cpp/thirdparty/flatbuffers/include/flatbuffers/table.h
+++ b/cpp/thirdparty/flatbuffers/include/flatbuffers/table.h
@@ -17,9 +17,16 @@
 #ifndef FLATBUFFERS_TABLE_H_
 #define FLATBUFFERS_TABLE_H_
 
+// Move this vendored copy of flatbuffers to a private namespace,
+// but continue to access it through the "flatbuffers" alias.
+namespace arrow_vendored_private::flatbuffers {
+}
+namespace flatbuffers = arrow_vendored_private::flatbuffers;
+
 #include "flatbuffers/base.h"
 #include "flatbuffers/verifier.h"
 
+namespace arrow_vendored_private {
 namespace flatbuffers {
 
 // "tables" use an offset table (possibly shared) that allows fields to be
@@ -184,5 +191,6 @@ inline flatbuffers::Optional<bool> Table::GetOptional<uint8_t, bool>(
 }
 
 }  // namespace flatbuffers
+}  // namespace arrow_vendored_private
 
 #endif  // FLATBUFFERS_TABLE_H_
diff --git a/cpp/thirdparty/flatbuffers/include/flatbuffers/vector.h b/cpp/thirdparty/flatbuffers/include/flatbuffers/vector.h
index ae52b9382..94591859a 100644
--- a/cpp/thirdparty/flatbuffers/include/flatbuffers/vector.h
+++ b/cpp/thirdparty/flatbuffers/include/flatbuffers/vector.h
@@ -17,10 +17,17 @@
 #ifndef FLATBUFFERS_VECTOR_H_
 #define FLATBUFFERS_VECTOR_H_
 
+// Move this vendored copy of flatbuffers to a private namespace,
+// but continue to access it through the "flatbuffers" alias.
+namespace arrow_vendored_private::flatbuffers {
+}
+namespace flatbuffers = arrow_vendored_private::flatbuffers;
+
 #include "flatbuffers/base.h"
 #include "flatbuffers/buffer.h"
 #include "flatbuffers/stl_emulation.h"
 
+namespace arrow_vendored_private {
 namespace flatbuffers {
 
 struct String;
@@ -393,5 +400,6 @@ template<typename T> static inline size_t VectorLength(const Vector<T> *v) {
 }
 
 }  // namespace flatbuffers
+}  // namespace arrow_vendored_private
 
 #endif  // FLATBUFFERS_VERIFIER_H_
diff --git a/cpp/thirdparty/flatbuffers/include/flatbuffers/vector_downward.h b/cpp/thirdparty/flatbuffers/include/flatbuffers/vector_downward.h
index 2b5a92cf1..a87b17b66 100644
--- a/cpp/thirdparty/flatbuffers/include/flatbuffers/vector_downward.h
+++ b/cpp/thirdparty/flatbuffers/include/flatbuffers/vector_downward.h
@@ -17,6 +17,12 @@
 #ifndef FLATBUFFERS_VECTOR_DOWNWARD_H_
 #define FLATBUFFERS_VECTOR_DOWNWARD_H_
 
+// Move this vendored copy of flatbuffers to a private namespace,
+// but continue to access it through the "flatbuffers" alias.
+namespace arrow_vendored_private::flatbuffers {
+}
+namespace flatbuffers = arrow_vendored_private::flatbuffers;
+
 #include <algorithm>
 #include <cstdint>
 
@@ -24,6 +30,7 @@
 #include "flatbuffers/default_allocator.h"
 #include "flatbuffers/detached_buffer.h"
 
+namespace arrow_vendored_private {
 namespace flatbuffers {
 
 // This is a minimal replication of std::vector<uint8_t> functionality,
@@ -285,5 +292,6 @@ template<typename SizeT = uoffset_t> class vector_downward {
 };
 
 }  // namespace flatbuffers
+}  // namespace arrow_vendored_private
 
 #endif  // FLATBUFFERS_VECTOR_DOWNWARD_H_
diff --git a/cpp/thirdparty/flatbuffers/include/flatbuffers/verifier.h b/cpp/thirdparty/flatbuffers/include/flatbuffers/verifier.h
index de1146be9..f6f5ac7f2 100644
--- a/cpp/thirdparty/flatbuffers/include/flatbuffers/verifier.h
+++ b/cpp/thirdparty/flatbuffers/include/flatbuffers/verifier.h
@@ -17,9 +17,16 @@
 #ifndef FLATBUFFERS_VERIFIER_H_
 #define FLATBUFFERS_VERIFIER_H_
 
+// Move this vendored copy of flatbuffers to a private namespace,
+// but continue to access it through the "flatbuffers" alias.
+namespace arrow_vendored_private::flatbuffers {
+}
+namespace flatbuffers = arrow_vendored_private::flatbuffers;
+
 #include "flatbuffers/base.h"
 #include "flatbuffers/vector.h"
 
+namespace arrow_vendored_private {
 namespace flatbuffers {
 
 // Helper class to verify the integrity of a FlatBuffer
@@ -328,5 +335,6 @@ inline size_t Verifier::VerifyOffset<uoffset64_t>(const size_t start) const {
 }
 
 }  // namespace flatbuffers
+}  // namespace arrow_vendored_private
 
 #endif  // FLATBUFFERS_VERIFIER_H_
-- 
2.34.1

```
