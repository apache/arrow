# 特点

## 总体

* 跨平台
 * 编译器：Visual Studio、gcc、clang 等
 * 架构：x86、x64、ARM 等
 * 操作系统：Windows、Mac OS X、Linux、iOS、Android 等
* 容易安装
 * 只有头文件的库。只需把头文件复制至你的项目中。
* 独立、最小依赖
 * 不需依赖 STL、BOOST 等。
 * 只包含 `<cstdio>`, `<cstdlib>`, `<cstring>`, `<inttypes.h>`, `<new>`, `<stdint.h>`。 
* 没使用 C++ 异常、RTTI
* 高性能
 * 使用模版及内联函数去降低函数调用开销。
 * 内部经优化的 Grisu2 及浮点数解析实现。
 * 可选的 SSE2/SSE4.2 支持。

## 符合标准

* RapidJSON 应完全符合 RFC4627/ECMA-404 标准。
* 支持 JSON Pointer (RFC6901).
* 支持 JSON Schema Draft v4.
* 支持 Unicode 代理对（surrogate pair）。
* 支持空字符（`"\u0000"`）。
 * 例如，可以优雅地解析及处理 `["Hello\u0000World"]`。含读写字符串长度的 API。
* 支持可选的放宽语法
 * 单行（`// ...`）及多行（`/* ... */`） 注释 (`kParseCommentsFlag`)。
 * 在对象和数组结束前含逗号 (`kParseTrailingCommasFlag`)。
 * `NaN`、`Inf`、`Infinity`、`-Inf` 及 `-Infinity` 作为 `double` 值 (`kParseNanAndInfFlag`)
* [NPM 兼容](https://github.com/Tencent/rapidjson/blob/master/doc/npm.md).

## Unicode

* 支持 UTF-8、UTF-16、UTF-32 编码，包括小端序和大端序。
 * 这些编码用于输入输出流，以及内存中的表示。
* 支持从输入流自动检测编码。
* 内部支持编码的转换。
 * 例如，你可以读取一个 UTF-8 文件，让 RapidJSON 把 JSON 字符串转换至 UTF-16 的 DOM。
* 内部支持编码校验。
 * 例如，你可以读取一个 UTF-8 文件，让 RapidJSON 检查是否所有 JSON 字符串是合法的 UTF-8 字节序列。
* 支持自定义的字符类型。
 * 预设的字符类型是：UTF-8 为 `char`，UTF-16 为 `wchar_t`，UTF32 为 `uint32_t`。
* 支持自定义的编码。

## API 风格

* SAX（Simple API for XML）风格 API
 * 类似于 [SAX](http://en.wikipedia.org/wiki/Simple_API_for_XML), RapidJSON 提供一个事件循序访问的解析器 API（`rapidjson::GenericReader`）。RapidJSON 也提供一个生成器 API（`rapidjson::Writer`），可以处理相同的事件集合。
* DOM（Document Object Model）风格 API
 * 类似于 HTML／XML 的 [DOM](http://en.wikipedia.org/wiki/Document_Object_Model)，RapidJSON 可把 JSON 解析至一个 DOM 表示方式（`rapidjson::GenericDocument`），以方便操作。如有需要，可把 DOM 转换（stringify）回 JSON。
 * DOM 风格 API（`rapidjson::GenericDocument`）实际上是由 SAX 风格 API（`rapidjson::GenericReader`）实现的。SAX 更快，但有时 DOM 更易用。用户可根据情况作出选择。

## 解析

* 递归式（预设）及迭代式解析器
 * 递归式解析器较快，但在极端情况下可出现堆栈溢出。
 * 迭代式解析器使用自定义的堆栈去维持解析状态。
* 支持原位（*in situ*）解析。
 * 把 JSON 字符串的值解析至原 JSON 之中，然后让 DOM 指向那些字符串。
 * 比常规分析更快：不需字符串的内存分配、不需复制（如字符串不含转义符）、缓存友好。
* 对于 JSON 数字类型，支持 32-bit/64-bit 的有号／无号整数，以及 `double`。
* 错误处理
 * 支持详尽的解析错误代号。
 * 支持本地化错误信息。

## DOM (Document)

* RapidJSON 在类型转换时会检查数值的范围。
* 字符串字面量的优化
 * 只储存指针，不作复制
* 优化“短”字符串
 * 在 `Value` 内储存短字符串，无需额外分配。
 * 对 UTF-8 字符串来说，32 位架构下可存储最多 11 字符，64 位下 21 字符（x86-64 下 13 字符）。
* 可选地支持 `std::string`（定义 `RAPIDJSON_HAS_STDSTRING=1`）

## 生成

* 支持 `rapidjson::PrettyWriter` 去加入换行及缩进。

## 输入输出流

* 支持 `rapidjson::GenericStringBuffer`，把输出的 JSON 储存于字符串内。
* 支持 `rapidjson::FileReadStream` 及 `rapidjson::FileWriteStream`，使用 `FILE` 对象作输入输出。
* 支持自定义输入输出流。

## 内存

* 最小化 DOM 的内存开销。
 * 对大部分 32／64 位机器而言，每个 JSON 值只占 16 或 20 字节（不包含字符串）。
* 支持快速的预设分配器。
 * 它是一个堆栈形式的分配器（顺序分配，不容许单独释放，适合解析过程之用）。
 * 使用者也可提供一个预分配的缓冲区。（有可能达至无需 CRT 分配就能解析多个 JSON）
* 支持标准 CRT（C-runtime）分配器。
* 支持自定义分配器。

## 其他

* 一些 C++11 的支持（可选）
 * 右值引用（rvalue reference）
 * `noexcept` 修饰符
 * 范围 for 循环
