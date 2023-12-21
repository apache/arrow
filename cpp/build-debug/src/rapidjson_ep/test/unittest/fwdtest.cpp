// Tencent is pleased to support the open source community by making RapidJSON available.
// 
// Copyright (C) 2015 THL A29 Limited, a Tencent company, and Milo Yip.
//
// Licensed under the MIT License (the "License"); you may not use this file except
// in compliance with the License. You may obtain a copy of the License at
//
// http://opensource.org/licenses/MIT
//
// Unless required by applicable law or agreed to in writing, software distributed 
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR 
// CONDITIONS OF ANY KIND, either express or implied. See the License for the 
// specific language governing permissions and limitations under the License.

#include "unittest.h"

// Using forward declared types here.

#include "rapidjson/fwd.h"

#ifdef __GNUC__
RAPIDJSON_DIAG_PUSH
RAPIDJSON_DIAG_OFF(effc++)
#endif

using namespace rapidjson;

struct Foo {
    Foo();
    ~Foo();

    // encodings.h
    UTF8<char>* utf8;
    UTF16<wchar_t>* utf16;
    UTF16BE<wchar_t>* utf16be;
    UTF16LE<wchar_t>* utf16le;
    UTF32<unsigned>* utf32;
    UTF32BE<unsigned>* utf32be;
    UTF32LE<unsigned>* utf32le;
    ASCII<char>* ascii;
    AutoUTF<unsigned>* autoutf;
    Transcoder<UTF8<char>, UTF8<char> >* transcoder;

    // allocators.h
    CrtAllocator* crtallocator;
    MemoryPoolAllocator<CrtAllocator>* memorypoolallocator;

    // stream.h
    StringStream* stringstream;
    InsituStringStream* insitustringstream;

    // stringbuffer.h
    StringBuffer* stringbuffer;

    // // filereadstream.h
    // FileReadStream* filereadstream;

    // // filewritestream.h
    // FileWriteStream* filewritestream;

    // memorybuffer.h
    MemoryBuffer* memorybuffer;

    // memorystream.h
    MemoryStream* memorystream;

    // reader.h
    BaseReaderHandler<UTF8<char>, void>* basereaderhandler;
    Reader* reader;

    // writer.h
    Writer<StringBuffer, UTF8<char>, UTF8<char>, CrtAllocator, 0>* writer;

    // prettywriter.h
    PrettyWriter<StringBuffer, UTF8<char>, UTF8<char>, CrtAllocator, 0>* prettywriter;

    // document.h
    Value* value;
    Document* document;

    // pointer.h
    Pointer* pointer;

    // schema.h
    SchemaDocument* schemadocument;
    SchemaValidator* schemavalidator;

    // char buffer[16];
};

// Using type definitions here.

#include "rapidjson/stringbuffer.h"
#include "rapidjson/filereadstream.h"
#include "rapidjson/filewritestream.h"
#include "rapidjson/memorybuffer.h"
#include "rapidjson/memorystream.h"
#include "rapidjson/document.h" // -> reader.h
#include "rapidjson/writer.h"
#include "rapidjson/prettywriter.h"
#include "rapidjson/schema.h"   // -> pointer.h

typedef Transcoder<UTF8<>, UTF8<> > TranscoderUtf8ToUtf8;
typedef BaseReaderHandler<UTF8<>, void> BaseReaderHandlerUtf8Void;

Foo::Foo() : 
    // encodings.h
    utf8(RAPIDJSON_NEW(UTF8<>)),
    utf16(RAPIDJSON_NEW(UTF16<>)),
    utf16be(RAPIDJSON_NEW(UTF16BE<>)),
    utf16le(RAPIDJSON_NEW(UTF16LE<>)),
    utf32(RAPIDJSON_NEW(UTF32<>)),
    utf32be(RAPIDJSON_NEW(UTF32BE<>)),
    utf32le(RAPIDJSON_NEW(UTF32LE<>)),
    ascii(RAPIDJSON_NEW(ASCII<>)),
    autoutf(RAPIDJSON_NEW(AutoUTF<unsigned>)),
    transcoder(RAPIDJSON_NEW(TranscoderUtf8ToUtf8)),

    // allocators.h
    crtallocator(RAPIDJSON_NEW(CrtAllocator)),
    memorypoolallocator(RAPIDJSON_NEW(MemoryPoolAllocator<>)),

    // stream.h
    stringstream(RAPIDJSON_NEW(StringStream)(NULL)),
    insitustringstream(RAPIDJSON_NEW(InsituStringStream)(NULL)),

    // stringbuffer.h
    stringbuffer(RAPIDJSON_NEW(StringBuffer)),

    // // filereadstream.h
    // filereadstream(RAPIDJSON_NEW(FileReadStream)(stdout, buffer, sizeof(buffer))),

    // // filewritestream.h
    // filewritestream(RAPIDJSON_NEW(FileWriteStream)(stdout, buffer, sizeof(buffer))),

    // memorybuffer.h
    memorybuffer(RAPIDJSON_NEW(MemoryBuffer)),

    // memorystream.h
    memorystream(RAPIDJSON_NEW(MemoryStream)(NULL, 0)),

    // reader.h
    basereaderhandler(RAPIDJSON_NEW(BaseReaderHandlerUtf8Void)),
    reader(RAPIDJSON_NEW(Reader)),

    // writer.h
    writer(RAPIDJSON_NEW(Writer<StringBuffer>)),

    // prettywriter.h
    prettywriter(RAPIDJSON_NEW(PrettyWriter<StringBuffer>)),

    // document.h
    value(RAPIDJSON_NEW(Value)),
    document(RAPIDJSON_NEW(Document)),

    // pointer.h
    pointer(RAPIDJSON_NEW(Pointer)),

    // schema.h
    schemadocument(RAPIDJSON_NEW(SchemaDocument)(*document)),
    schemavalidator(RAPIDJSON_NEW(SchemaValidator)(*schemadocument))
{

}

Foo::~Foo() {
    // encodings.h
    RAPIDJSON_DELETE(utf8);
    RAPIDJSON_DELETE(utf16);
    RAPIDJSON_DELETE(utf16be);
    RAPIDJSON_DELETE(utf16le);
    RAPIDJSON_DELETE(utf32);
    RAPIDJSON_DELETE(utf32be);
    RAPIDJSON_DELETE(utf32le);
    RAPIDJSON_DELETE(ascii);
    RAPIDJSON_DELETE(autoutf);
    RAPIDJSON_DELETE(transcoder);

    // allocators.h
    RAPIDJSON_DELETE(crtallocator);
    RAPIDJSON_DELETE(memorypoolallocator);

    // stream.h
    RAPIDJSON_DELETE(stringstream);
    RAPIDJSON_DELETE(insitustringstream);

    // stringbuffer.h
    RAPIDJSON_DELETE(stringbuffer);

    // // filereadstream.h
    // RAPIDJSON_DELETE(filereadstream);

    // // filewritestream.h
    // RAPIDJSON_DELETE(filewritestream);

    // memorybuffer.h
    RAPIDJSON_DELETE(memorybuffer);

    // memorystream.h
    RAPIDJSON_DELETE(memorystream);

    // reader.h
    RAPIDJSON_DELETE(basereaderhandler);
    RAPIDJSON_DELETE(reader);

    // writer.h
    RAPIDJSON_DELETE(writer);

    // prettywriter.h
    RAPIDJSON_DELETE(prettywriter);

    // document.h
    RAPIDJSON_DELETE(value);
    RAPIDJSON_DELETE(document);

    // pointer.h
    RAPIDJSON_DELETE(pointer);

    // schema.h
    RAPIDJSON_DELETE(schemadocument);
    RAPIDJSON_DELETE(schemavalidator);
}

TEST(Fwd, Fwd) {
    Foo f;
}

#ifdef __GNUC__
RAPIDJSON_DIAG_POP
#endif
