# Schema

（本功能于 v1.1.0 发布）

JSON Schema 是描述 JSON 格式的一个标准草案。一个 schema 本身也是一个 JSON。使用 JSON Schema 去校验 JSON，可以让你的代码安全地访问 DOM，而无须检查类型或键值是否存在等。这也能确保输出的 JSON 是符合指定的 schema。

RapidJSON 实现了一个 [JSON Schema Draft v4](http://json-schema.org/documentation.html) 的校验器。若你不熟悉 JSON Schema，可以参考 [Understanding JSON Schema](http://spacetelescope.github.io/understanding-json-schema/)。

[TOC]

# 基本用法 {#BasicUsage}

首先，你要把 JSON Schema 解析成 `Document`，再把它编译成一个 `SchemaDocument`。

然后，利用该 `SchemaDocument` 创建一个 `SchemaValidator`。它与 `Writer` 相似，都是能够处理 SAX 事件的。因此，你可以用 `document.Accept(validator)` 去校验一个 JSON，然后再获取校验结果。

~~~cpp
#include "rapidjson/schema.h"

// ...

Document sd;
if (sd.Parse(schemaJson).HasParseError()) {
    // 此 schema 不是合法的 JSON
    // ...       
}
SchemaDocument schema(sd); // 把一个 Document 编译至 SchemaDocument
// 之后不再需要 sd

Document d;
if (d.Parse(inputJson).HasParseError()) {
    // 输入不是一个合法的 JSON
    // ...       
}

SchemaValidator validator(schema);
if (!d.Accept(validator)) {
    // 输入的 JSON 不合乎 schema
    // 打印诊断信息
    StringBuffer sb;
    validator.GetInvalidSchemaPointer().StringifyUriFragment(sb);
    printf("Invalid schema: %s\n", sb.GetString());
    printf("Invalid keyword: %s\n", validator.GetInvalidSchemaKeyword());
    sb.Clear();
    validator.GetInvalidDocumentPointer().StringifyUriFragment(sb);
    printf("Invalid document: %s\n", sb.GetString());
}
~~~

一些注意点：

* 一个 `SchemaDocment` 能被多个 `SchemaValidator` 引用。它不会被 `SchemaValidator` 修改。
* 可以重复使用一个 `SchemaValidator` 来校验多个文件。在校验其他文件前，须先调用 `validator.Reset()`。

# 在解析／生成时进行校验 {#ParsingSerialization}

与大部分 JSON Schema 校验器有所不同，RapidJSON 提供了一个基于 SAX 的 schema 校验器实现。因此，你可以在输入流解析 JSON 的同时进行校验。若校验器遇到一个与 schema 不符的值，就会立即终止解析。这设计对于解析大型 JSON 文件时特别有用。

## DOM 解析 {#DomParsing}

在使用 DOM 进行解析时，`Document` 除了接收 SAX 事件外，还需做一些准备及结束工作，因此，为了连接 `Reader`、`SchemaValidator` 和 `Document` 要做多一点事情。`SchemaValidatingReader` 是一个辅助类去做那些工作。

~~~cpp
#include "rapidjson/filereadstream.h"

// ...
SchemaDocument schema(sd); // 把一个 Document 编译至 SchemaDocument

// 使用 reader 解析 JSON
FILE* fp = fopen("big.json", "r");
FileReadStream is(fp, buffer, sizeof(buffer));

// 用 reader 解析 JSON，校验它的 SAX 事件，并存储至 d
Document d;
SchemaValidatingReader<kParseDefaultFlags, FileReadStream, UTF8<> > reader(is, schema);
d.Populate(reader);

if (!reader.GetParseResult()) {
    // 不是一个合法的 JSON
    // 当 reader.GetParseResult().Code() == kParseErrorTermination,
    // 它可能是被以下原因中止：
    // (1) 校验器发现 JSON 不合乎 schema；或
    // (2) 输入流有 I/O 错误。

    // 检查校验结果
    if (!reader.IsValid()) {
        // 输入的 JSON 不合乎 schema
        // 打印诊断信息
        StringBuffer sb;
        reader.GetInvalidSchemaPointer().StringifyUriFragment(sb);
        printf("Invalid schema: %s\n", sb.GetString());
        printf("Invalid keyword: %s\n", reader.GetInvalidSchemaKeyword());
        sb.Clear();
        reader.GetInvalidDocumentPointer().StringifyUriFragment(sb);
        printf("Invalid document: %s\n", sb.GetString());
    }
}
~~~

## SAX 解析 {#SaxParsing}

使用 SAX 解析时，情况就简单得多。若只需要校验 JSON 而无需进一步处理，那么仅需要：

~~~
SchemaValidator validator(schema);
Reader reader;
if (!reader.Parse(stream, validator)) {
    if (!validator.IsValid()) {
        // ...    
    }
}
~~~

这种方式和 [schemavalidator](example/schemavalidator/schemavalidator.cpp) 例子完全相同。这带来的独特优势是，无论 JSON 多巨大，永远维持低内存用量（内存用量只与 Schema 的复杂度相关）。

若你需要进一步处理 SAX 事件，便可使用模板类 `GenericSchemaValidator` 去设置校验器的输出 `Handler`：

~~~
MyHandler handler;
GenericSchemaValidator<SchemaDocument, MyHandler> validator(schema, handler);
Reader reader;
if (!reader.Parse(ss, validator)) {
    if (!validator.IsValid()) {
        // ...    
    }
}
~~~

## 生成 {#Serialization}

我们也可以在生成（serialization）的时候进行校验。这能确保输出的 JSON 符合一个 JSON Schema。

~~~
StringBuffer sb;
Writer<StringBuffer> writer(sb);
GenericSchemaValidator<SchemaDocument, Writer<StringBuffer> > validator(s, writer);
if (!d.Accept(validator)) {
    // Some problem during Accept(), it may be validation or encoding issues.
    if (!validator.IsValid()) {
        // ...
    }
}
~~~

当然，如果你的应用仅需要 SAX 风格的生成，那么只需要把 SAX 事件由原来发送到 `Writer`，改为发送到 `SchemaValidator`。

# 远程 Schema {#RemoteSchema}

JSON Schema 支持 [`$ref` 关键字](http://spacetelescope.github.io/understanding-json-schema/structuring.html)，它是一个 [JSON pointer](doc/pointer.zh-cn.md) 引用至一个本地（local）或远程（remote） schema。本地指针的首字符是 `#`，而远程指针是一个相对或绝对 URI。例如：

~~~js
{ "$ref": "definitions.json#/address" }
~~~

由于 `SchemaDocument` 并不知道如何处理那些 URI，它需要使用者提供一个 `IRemoteSchemaDocumentProvider` 的实例去处理。

~~~
class MyRemoteSchemaDocumentProvider : public IRemoteSchemaDocumentProvider {
public:
    virtual const SchemaDocument* GetRemoteDocument(const char* uri, SizeType length) {
        // Resolve the uri and returns a pointer to that schema.
    }
};

// ...

MyRemoteSchemaDocumentProvider provider;
SchemaDocument schema(sd, &provider);
~~~

# 标准的符合程度 {#Conformance}

RapidJSON 通过了 [JSON Schema Test Suite](https://github.com/json-schema/JSON-Schema-Test-Suite) (Json Schema draft 4) 中 263 个测试的 262 个。

没通过的测试是 `refRemote.json` 中的 "change resolution scope" - "changed scope ref invalid"。这是由于未实现 `id` schema 关键字及 URI 合并功能。

除此以外，关于字符串类型的 `format` schema 关键字也会被忽略，因为标准中并没需求必须实现。

## 正则表达式 {#RegEx}

`pattern` 及 `patternProperties` 这两个 schema 关键字使用了正则表达式去匹配所需的模式。

RapidJSON 实现了一个简单的 NFA 正则表达式引擎，并预设使用。它支持以下语法。

|语法|描述|
|------|-----------|
|`ab`    | 串联 |
|<code>a&#124;b</code>   | 交替 |
|`a?`    | 零或一次 |
|`a*`    | 零或多次 |
|`a+`    | 一或多次 |
|`a{3}`  | 刚好 3 次 |
|`a{3,}` | 至少 3 次 |
|`a{3,5}`| 3 至 5 次 |
|`(ab)`  | 分组 |
|`^a`    | 在开始处 |
|`a$`    | 在结束处 |
|`.`     | 任何字符 |
|`[abc]` | 字符组 |
|`[a-c]` | 字符组范围 |
|`[a-z0-9_]` | 字符组组合 |
|`[^abc]` | 字符组取反 |
|`[^a-c]` | 字符组范围取反 |
|`[\b]`   | 退格符 (U+0008) |
|<code>\\&#124;</code>, `\\`, ...  | 转义字符 |
|`\f` | 馈页 (U+000C) |
|`\n` | 馈行 (U+000A) |
|`\r` | 回车 (U+000D) |
|`\t` | 制表 (U+0009) |
|`\v` | 垂直制表 (U+000B) |

对于使用 C++11 编译器的使用者，也可使用 `std::regex`，只需定义 `RAPIDJSON_SCHEMA_USE_INTERNALREGEX=0` 及 `RAPIDJSON_SCHEMA_USE_STDREGEX=1`。若你的 schema 无需使用 `pattern` 或 `patternProperties`，可以把两个宏都设为零，以禁用此功能，这样做可节省一些代码体积。

# 性能 {#Performance}

大部分 C++ JSON 库都未支持 JSON Schema。因此我们尝试按照 [json-schema-benchmark](https://github.com/ebdrup/json-schema-benchmark) 去评估 RapidJSON 的 JSON Schema 校验器。该评测测试了 11 个运行在 node.js 上的 JavaScript 库。

该评测校验 [JSON Schema Test Suite](https://github.com/json-schema/JSON-Schema-Test-Suite) 中的测试，当中排除了一些测试套件及个别测试。我们在 [`schematest.cpp`](test/perftest/schematest.cpp) 实现了相同的评测。

在 MacBook Pro (2.8 GHz Intel Core i7) 上收集到以下结果。

|校验器|相对速度|每秒执行的测试数目|
|---------|:------------:|:----------------------------:|
|RapidJSON|155%|30682|
|[`ajv`](https://github.com/epoberezkin/ajv)|100%|19770 (± 1.31%)|
|[`is-my-json-valid`](https://github.com/mafintosh/is-my-json-valid)|70%|13835 (± 2.84%)|
|[`jsen`](https://github.com/bugventure/jsen)|57.7%|11411 (± 1.27%)|
|[`schemasaurus`](https://github.com/AlexeyGrishin/schemasaurus)|26%|5145 (± 1.62%)|
|[`themis`](https://github.com/playlyfe/themis)|19.9%|3935 (± 2.69%)|
|[`z-schema`](https://github.com/zaggino/z-schema)|7%|1388 (± 0.84%)|
|[`jsck`](https://github.com/pandastrike/jsck#readme)|3.1%|606 (± 2.84%)|
|[`jsonschema`](https://github.com/tdegrunt/jsonschema#readme)|0.9%|185 (± 1.01%)|
|[`skeemas`](https://github.com/Prestaul/skeemas#readme)|0.8%|154 (± 0.79%)|
|tv4|0.5%|93 (± 0.94%)|
|[`jayschema`](https://github.com/natesilva/jayschema)|0.1%|21 (± 1.14%)|

换言之，RapidJSON 比最快的 JavaScript 库（ajv）快约 1.5x。比最慢的快 1400x。
