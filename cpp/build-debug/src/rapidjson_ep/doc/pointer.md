# Pointer

(This feature was released in v1.1.0)

JSON Pointer is a standardized ([RFC6901]) way to select a value inside a JSON Document (DOM). This can be analogous to XPath for XML document. However, JSON Pointer is much simpler, and a single JSON Pointer only pointed to a single value.

Using RapidJSON's implementation of JSON Pointer can simplify some manipulations of the DOM.

[TOC]

# JSON Pointer {#JsonPointer}

A JSON Pointer is a list of zero-to-many tokens, each prefixed by `/`. Each token can be a string or a number. For example, given a JSON:
~~~javascript
{
    "foo" : ["bar", "baz"],
    "pi" : 3.1416
}
~~~

The following JSON Pointers resolve this JSON as:

1. `"/foo"` → `[ "bar", "baz" ]`
2. `"/foo/0"` → `"bar"`
3. `"/foo/1"` → `"baz"`
4. `"/pi"` → `3.1416`

Note that, an empty JSON Pointer `""` (zero token) resolves to the whole JSON.

# Basic Usage {#BasicUsage}

The following example code is self-explanatory.

~~~cpp
#include "rapidjson/pointer.h"

// ...
Document d;

// Create DOM by Set()
Pointer("/project").Set(d, "RapidJSON");
Pointer("/stars").Set(d, 10);

// { "project" : "RapidJSON", "stars" : 10 }

// Access DOM by Get(). It return nullptr if the value does not exist.
if (Value* stars = Pointer("/stars").Get(d))
    stars->SetInt(stars->GetInt() + 1);

// { "project" : "RapidJSON", "stars" : 11 }

// Set() and Create() automatically generate parents if not exist.
Pointer("/a/b/0").Create(d);

// { "project" : "RapidJSON", "stars" : 11, "a" : { "b" : [ null ] } }

// GetWithDefault() returns reference. And it deep clones the default value.
Value& hello = Pointer("/hello").GetWithDefault(d, "world");

// { "project" : "RapidJSON", "stars" : 11, "a" : { "b" : [ null ] }, "hello" : "world" }

// Swap() is similar to Set()
Value x("C++");
Pointer("/hello").Swap(d, x);

// { "project" : "RapidJSON", "stars" : 11, "a" : { "b" : [ null ] }, "hello" : "C++" }
// x becomes "world"

// Erase a member or element, return true if the value exists
bool success = Pointer("/a").Erase(d);
assert(success);

// { "project" : "RapidJSON", "stars" : 10 }
~~~

# Helper Functions {#HelperFunctions}

Since object-oriented calling convention may be non-intuitive, RapidJSON also provides helper functions, which just wrap the member functions with free-functions.

The following example does exactly the same as the above one.

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

The conventions are shown here for comparison:

1. `Pointer(source).<Method>(root, ...)`
2. `<Method>ValueByPointer(root, Pointer(source), ...)`
3. `<Method>ValueByPointer(root, source, ...)`

# Resolving Pointer {#ResolvingPointer}

`Pointer::Get()` or `GetValueByPointer()` function does not modify the DOM. If the tokens cannot match a value in the DOM, it returns `nullptr`. User can use this to check whether a value exists.

Note that, numerical tokens can represent an array index or member name. The resolving process will match the values according to the types of value.

~~~javascript
{
    "0" : 123,
    "1" : [456]
}
~~~

1. `"/0"` → `123`
2. `"/1/0"` → `456`

The token `"0"` is treated as member name in the first pointer. It is treated as an array index in the second pointer.

The other functions, including `Create()`, `GetWithDefault()`, `Set()` and `Swap()`, will change the DOM. These functions will always succeed. They will create the parent values if they do not exist. If the parent values do not match the tokens, they will also be forced to change their type. Changing the type also mean fully removal of that DOM subtree.

Parsing the above JSON into `d`, 

~~~cpp
SetValueByPointer(d, "1/a", 789); // { "0" : 123, "1" : { "a" : 789 } }
~~~

## Resolving Minus Sign Token

Besides, [RFC6901] defines a special token `-` (single minus sign), which represents the pass-the-end element of an array. `Get()` only treats this token as a member name '"-"'. Yet the other functions can resolve this for array, equivalent to calling `Value::PushBack()` to the array.

~~~cpp
Document d;
d.Parse("{\"foo\":[123]}");
SetValueByPointer(d, "/foo/-", 456); // { "foo" : [123, 456] }
SetValueByPointer(d, "/-", 789);    // { "foo" : [123, 456], "-" : 789 }
~~~

## Resolving Document and Value

When using `p.Get(root)` or `GetValueByPointer(root, p)`, `root` is a (const) `Value&`. That means, it can be a subtree of the DOM.

The other functions have two groups of signature. One group uses `Document& document` as parameter, another one uses `Value& root`. The first group uses `document.GetAllocator()` for creating values. And the second group needs user to supply an allocator, like the functions in DOM.

All examples above do not require an allocator parameter, because the first parameter is a `Document&`. But if you want to resolve a pointer to a subtree, you need to supply the allocator as in the following example:

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

`Erase()` or `EraseValueByPointer()` does not need allocator. And they return `true` if the value is erased successfully.

# Error Handling {#ErrorHandling}

A `Pointer` parses a source string in its constructor. If there is parsing error, `Pointer::IsValid()` returns `false`. And you can use `Pointer::GetParseErrorCode()` and `GetParseErrorOffset()` to retrieve the error information.

Note that, all resolving functions assumes valid pointer. Resolving with an invalid pointer causes assertion failure.

# URI Fragment Representation {#URIFragment}

In addition to the string representation of JSON pointer that we are using till now, [RFC6901] also defines the URI fragment representation of JSON pointer. URI fragment is specified in [RFC3986] "Uniform Resource Identifier (URI): Generic Syntax".

The main differences are that a the URI fragment always has a `#` (pound sign) in the beginning, and some characters are encoded by percent-encoding in UTF-8 sequence. For example, the following table shows different C/C++ string literals of different representations.

String Representation | URI Fragment Representation | Pointer Tokens (UTF-8)
----------------------|-----------------------------|------------------------
`"/foo/0"`            | `"#/foo/0"`                 | `{"foo", 0}`
`"/a~1b"`             | `"#/a~1b"`                  | `{"a/b"}`
`"/m~0n"`             | `"#/m~0n"`                  | `{"m~n"}`
`"/ "`                | `"#/%20"`                   | `{" "}`
`"/\0"`               | `"#/%00"`                   | `{"\0"}`
`"/€"`                | `"#/%E2%82%AC"`             | `{"€"}`

RapidJSON fully support URI fragment representation. It automatically detects the pound sign during parsing.

# Stringify

You may also stringify a `Pointer` to a string or other output streams. This can be done by:

~~~
Pointer p(...);
StringBuffer sb;
p.Stringify(sb);
std::cout << sb.GetString() << std::endl;
~~~

It can also stringify to URI fragment representation by `StringifyUriFragment()`.

# User-Supplied Tokens {#UserSuppliedTokens}

If a pointer will be resolved multiple times, it should be constructed once, and then apply it to different DOMs or in different times. This reduce time and memory allocation for constructing `Pointer` multiple times.

We can go one step further, to completely eliminate the parsing process and dynamic memory allocation, we can establish the token array directly:

~~~cpp
#define NAME(s) { s, sizeof(s) / sizeof(s[0]) - 1, kPointerInvalidIndex }
#define INDEX(i) { #i, sizeof(#i) - 1, i }

static const Pointer::Token kTokens[] = { NAME("foo"), INDEX(123) };
static const Pointer p(kTokens, sizeof(kTokens) / sizeof(kTokens[0]));
// Equivalent to static const Pointer p("/foo/123");
~~~

This may be useful for memory constrained systems.

[RFC3986]: https://tools.ietf.org/html/rfc3986
[RFC6901]: https://tools.ietf.org/html/rfc6901
