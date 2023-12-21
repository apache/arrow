# Pointer

（本功能于 v1.1.0 发布）

JSON Pointer 是一个标准化（[RFC6901]）的方式去选取一个 JSON Document（DOM）中的值。这类似于 XML 的 XPath。然而，JSON Pointer 简单得多，而且每个 JSON Pointer 仅指向单个值。

使用 RapidJSON 的 JSON Pointer 实现能简化一些 DOM 的操作。

[TOC]

# JSON Pointer {#JsonPointer}

一个 JSON Pointer 由一串（零至多个）token 所组成，每个 token 都有 `/` 前缀。每个 token 可以是一个字符串或数字。例如，给定一个 JSON：
~~~javascript
{
    "foo" : ["bar", "baz"],
    "pi" : 3.1416
}
~~~

以下的 JSON Pointer 解析为：

1. `"/foo"` → `[ "bar", "baz" ]`
2. `"/foo/0"` → `"bar"`
3. `"/foo/1"` → `"baz"`
4. `"/pi"` → `3.1416`

要注意，一个空 JSON Pointer `""` （零个 token）解析为整个 JSON。

# 基本使用方法 {#BasicUsage}

以下的代码范例不解自明。

~~~cpp
#include "rapidjson/pointer.h"

// ...
Document d;

// 使用 Set() 创建 DOM
Pointer("/project").Set(d, "RapidJSON");
Pointer("/stars").Set(d, 10);

// { "project" : "RapidJSON", "stars" : 10 }

// 使用 Get() 访问 DOM。若该值不存在则返回 nullptr。
if (Value* stars = Pointer("/stars").Get(d))
    stars->SetInt(stars->GetInt() + 1);

// { "project" : "RapidJSON", "stars" : 11 }

// Set() 和 Create() 自动生成父值（如果它们不存在）。
Pointer("/a/b/0").Create(d);

// { "project" : "RapidJSON", "stars" : 11, "a" : { "b" : [ null ] } }

// GetWithDefault() 返回引用。若该值不存在则会深拷贝缺省值。
Value& hello = Pointer("/hello").GetWithDefault(d, "world");

// { "project" : "RapidJSON", "stars" : 11, "a" : { "b" : [ null ] }, "hello" : "world" }

// Swap() 和 Set() 相似
Value x("C++");
Pointer("/hello").Swap(d, x);

// { "project" : "RapidJSON", "stars" : 11, "a" : { "b" : [ null ] }, "hello" : "C++" }
// x 变成 "world"

// 删去一个成员或元素，若值存在返回 true
bool success = Pointer("/a").Erase(d);
assert(success);

// { "project" : "RapidJSON", "stars" : 10 }
~~~

# 辅助函数 {#HelperFunctions}

由于面向对象的调用习惯可能不符直觉，RapidJSON 也提供了一些辅助函数，它们把成员函数包装成自由函数。

以下的例子与上面例子所做的事情完全相同。

~~~cpp
Document d;

SetValueByPointer(d, "/project", "RapidJSON");
SetValueByPointer(d, "/stars", 10);

if (Value* stars = GetValueByPointer(d, "/stars"))
    stars->SetInt(stars->GetInt() + 1);

CreateValueByPointer(d, "/a/b/0");

Value& hello = GetValueByPointerWithDefault(d, "/hello", "world");

Value x("C++");
SwapValueByPointer(d, "/hello", x);

bool success = EraseValueByPointer(d, "/a");
assert(success);
~~~

以下对比 3 种调用方式：

1. `Pointer(source).<Method>(root, ...)`
2. `<Method>ValueByPointer(root, Pointer(source), ...)`
3. `<Method>ValueByPointer(root, source, ...)`

# 解析 Pointer {#ResolvingPointer}

`Pointer::Get()` 或 `GetValueByPointer()` 函数并不修改 DOM。若那些 token 不能匹配 DOM 里的值，这些函数便返回 `nullptr`。使用者可利用这个方法来检查一个值是否存在。

注意，数值 token 可表示数组索引或成员名字。解析过程中会按值的类型来匹配。

~~~javascript
{
    "0" : 123,
    "1" : [456]
}
~~~

1. `"/0"` → `123`
2. `"/1/0"` → `456`

Token `"0"` 在第一个 pointer 中被当作成员名字。它在第二个 pointer 中被当作成数组索引。

其他函数会改变 DOM，包括 `Create()`、`GetWithDefault()`、`Set()`、`Swap()`。这些函数总是成功的。若一些父值不存在，就会创建它们。若父值类型不匹配 token，也会强行改变其类型。改变类型也意味着完全移除其 DOM 子树的内容。

例如，把上面的 JSON 解译至 `d` 之后，

~~~cpp
SetValueByPointer(d, "1/a", 789); // { "0" : 123, "1" : { "a" : 789 } }
~~~

## 解析负号 token

另外，[RFC6901] 定义了一个特殊 token `-` （单个负号），用于表示数组最后元素的下一个元素。 `Get()` 只会把此 token 当作成员名字 '"-"'。而其他函数则会以此解析数组，等同于对数组调用 `Value::PushBack()` 。

~~~cpp
Document d;
d.Parse("{\"foo\":[123]}");
SetValueByPointer(d, "/foo/-", 456); // { "foo" : [123, 456] }
SetValueByPointer(d, "/-", 789);    // { "foo" : [123, 456], "-" : 789 }
~~~

## 解析 Document 及 Value

当使用 `p.Get(root)` 或 `GetValueByPointer(root, p)`，`root` 是一个（常数） `Value&`。这意味着，它也可以是 DOM 里的一个子树。

其他函数有两组签名。一组使用 `Document& document` 作为参数，另一组使用 `Value& root`。第一组使用 `document.GetAllocator()` 去创建值，而第二组则需要使用者提供一个 allocator，如同 DOM 里的函数。

以上例子都不需要 allocator 参数，因为它的第一个参数是 `Document&`。但如果你需要对一个子树进行解析，就需要如下面的例子般提供 allocator：

~~~cpp
class Person {
public:
    Person() {
        document_ = new Document();
        // CreateValueByPointer() here no need allocator
        SetLocation(CreateValueByPointer(*document_, "/residence"), ...);
        SetLocation(CreateValueByPointer(*document_, "/office"), ...);
    };

private:
    void SetLocation(Value& location, const char* country, const char* addresses[2]) {
        Value::Allocator& a = document_->GetAllocator();
        // SetValueByPointer() here need allocator
        SetValueByPointer(location, "/country", country, a);
        SetValueByPointer(location, "/address/0", address[0], a);
        SetValueByPointer(location, "/address/1", address[1], a);
    }

    // ...

    Document* document_;
};
~~~

`Erase()` 或 `EraseValueByPointer()` 不需要 allocator。而且它们成功删除值之后会返回 `true`。

# 错误处理 {#ErrorHandling}

`Pointer` 在其建构函数里会解译源字符串。若有解析错误，`Pointer::IsValid()` 返回 `false`。你可使用 `Pointer::GetParseErrorCode()` 和 `GetParseErrorOffset()` 去获取错信息。

要注意的是，所有解析函数都假设 pointer 是合法的。对一个非法 pointer 解析会造成断言失败。

# URI 片段表示方式 {#URIFragment}

除了我们一直在使用的字符串方式表示 JSON pointer，[RFC6901] 也定义了一个 JSON Pointer 的 URI 片段（fragment）表示方式。URI 片段是定义于 [RFC3986] "Uniform Resource Identifier (URI): Generic Syntax"。

URI 片段的主要分别是必然以 `#` （pound sign）开头，而一些字符也会以百分比编码成 UTF-8 序列。例如，以下的表展示了不同表示法下的 C/C++ 字符串常数。

字符串表示方式 | URI 片段表示方式 | Pointer Tokens （UTF-8）
----------------------|-----------------------------|------------------------
`"/foo/0"`            | `"#/foo/0"`                 | `{"foo", 0}`
`"/a~1b"`             | `"#/a~1b"`                  | `{"a/b"}`
`"/m~0n"`             | `"#/m~0n"`                  | `{"m~n"}`
`"/ "`                | `"#/%20"`                   | `{" "}`
`"/\0"`               | `"#/%00"`                   | `{"\0"}`
`"/€"`                | `"#/%E2%82%AC"`             | `{"€"}`

RapidJSON 完全支持 URI 片段表示方式。它在解译时会自动检测 `#` 号。

# 字符串化

你也可以把一个 `Pointer` 字符串化，储存于字符串或其他输出流。例如：

~~~
Pointer p(...);
StringBuffer sb;
p.Stringify(sb);
std::cout << sb.GetString() << std::endl;
~~~

使用 `StringifyUriFragment()` 可以把 pointer 字符串化为 URI 片段表示法。

# 使用者提供的 tokens {#UserSuppliedTokens}

若一个 pointer 会用于多次解析，它应该只被创建一次，然后再施于不同的 DOM ，或在不同时间做解析。这样可以避免多次创键 `Pointer`，节省时间和内存分配。

我们甚至可以再更进一步，完全消去解析过程及动态内存分配。我们可以直接生成 token 数组：

~~~cpp
#define NAME(s) { s, sizeof(s) / sizeof(s[0]) - 1, kPointerInvalidIndex }
#define INDEX(i) { #i, sizeof(#i) - 1, i }

static const Pointer::Token kTokens[] = { NAME("foo"), INDEX(123) };
static const Pointer p(kTokens, sizeof(kTokens) / sizeof(kTokens[0]));
// Equivalent to static const Pointer p("/foo/123");
~~~

这种做法可能适合内存受限的系统。

[RFC3986]: https://tools.ietf.org/html/rfc3986
[RFC6901]: https://tools.ietf.org/html/rfc6901
