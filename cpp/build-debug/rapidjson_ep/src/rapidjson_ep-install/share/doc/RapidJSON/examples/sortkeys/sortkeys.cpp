#include "rapidjson/document.h"
#include "rapidjson/filewritestream.h"
#include <rapidjson/prettywriter.h>

#include <algorithm>
#include <iostream>

using namespace rapidjson;
using namespace std;

static void printIt(const Value &doc) {
    char writeBuffer[65536];
    FileWriteStream os(stdout, writeBuffer, sizeof(writeBuffer));
    PrettyWriter<FileWriteStream> writer(os);
    doc.Accept(writer);
    cout << endl;
}

struct NameComparator {
    bool operator()(const Value::Member &lhs, const Value::Member &rhs) const {
        return (strcmp(lhs.name.GetString(), rhs.name.GetString()) < 0);
    }
};

int main() {
    Document d(kObjectType);
    Document::AllocatorType &allocator = d.GetAllocator();

    d.AddMember("zeta", Value().SetBool(false), allocator);
    d.AddMember("gama", Value().SetString("test string", allocator), allocator);
    d.AddMember("delta", Value().SetInt(123), allocator);
    d.AddMember("alpha", Value(kArrayType).Move(), allocator);

    printIt(d);

/*
{
    "zeta": false,
    "gama": "test string",
    "delta": 123,
    "alpha": []
}
*/

// C++11 supports std::move() of Value so it always have no problem for std::sort().
// Some C++03 implementations of std::sort() requires copy constructor which causes compilation error.
// Needs a sorting function only depends on std::swap() instead.
#if __cplusplus >= 201103L || (!defined(__GLIBCXX__) && (!defined(_MSC_VER) || _MSC_VER >= 1900))
    std::sort(d.MemberBegin(), d.MemberEnd(), NameComparator());

    printIt(d);

/*
{
  "alpha": [],
  "delta": 123,
  "gama": "test string",
  "zeta": false
}
*/
#endif
}
