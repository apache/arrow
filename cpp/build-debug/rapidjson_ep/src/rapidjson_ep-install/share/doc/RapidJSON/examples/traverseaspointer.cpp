#include "rapidjson/document.h"
#include "rapidjson/filereadstream.h"
#include "rapidjson/pointer.h"
#include "rapidjson/stringbuffer.h"
#include <iostream>

using namespace rapidjson;

void traverse(const Value& v, const Pointer& p) {
    StringBuffer sb;
    p.Stringify(sb);
    std::cout << sb.GetString() << std::endl;

    switch (v.GetType()) {
    case kArrayType:
        for (SizeType i = 0; i != v.Size(); ++i)
            traverse(v[i], p.Append(i));
        break;
    case kObjectType:
        for (Value::ConstMemberIterator m = v.MemberBegin(); m != v.MemberEnd(); ++m) 
            traverse(m->value, p.Append(m->name.GetString(), m->name.GetStringLength()));
        break;
    default:
        break;
    }
}

int main(int, char*[]) {
    char readBuffer[65536];
    FileReadStream is(stdin, readBuffer, sizeof(readBuffer));

    Document d;
    d.ParseStream(is);

    Pointer root;
    traverse(d, root);

    return 0;
}
