#include "rapidjson/reader.h"
#include <iostream>
#include <sstream>

using namespace rapidjson;
using namespace std;

// If you can require C++11, you could use std::to_string here
template <typename T> std::string stringify(T x) {
    std::stringstream ss;
    ss << x;
    return ss.str();
}

struct MyHandler {
    const char* type;
    std::string data;
    
    MyHandler() : type(), data() {}

    bool Null() { type = "Null"; data.clear(); return true; }
    bool Bool(bool b) { type = "Bool:"; data = b? "true": "false"; return true; }
    bool Int(int i) { type = "Int:"; data = stringify(i); return true; }
    bool Uint(unsigned u) { type = "Uint:"; data = stringify(u); return true; }
    bool Int64(int64_t i) { type = "Int64:"; data = stringify(i); return true; }
    bool Uint64(uint64_t u) { type = "Uint64:"; data = stringify(u); return true; }
    bool Double(double d) { type = "Double:"; data = stringify(d); return true; }
    bool RawNumber(const char* str, SizeType length, bool) { type = "Number:"; data = std::string(str, length); return true; }
    bool String(const char* str, SizeType length, bool) { type = "String:"; data = std::string(str, length); return true; }
    bool StartObject() { type = "StartObject"; data.clear(); return true; }
    bool Key(const char* str, SizeType length, bool) { type = "Key:"; data = std::string(str, length); return true; }
    bool EndObject(SizeType memberCount) { type = "EndObject:"; data = stringify(memberCount); return true; }
    bool StartArray() { type = "StartArray"; data.clear(); return true; }
    bool EndArray(SizeType elementCount) { type = "EndArray:"; data = stringify(elementCount); return true; }
private:
    MyHandler(const MyHandler& noCopyConstruction);
    MyHandler& operator=(const MyHandler& noAssignment);
};

int main() {
    const char json[] = " { \"hello\" : \"world\", \"t\" : true , \"f\" : false, \"n\": null, \"i\":123, \"pi\": 3.1416, \"a\":[1, 2, 3, 4] } ";

    MyHandler handler;
    Reader reader;
    StringStream ss(json);
    reader.IterativeParseInit();
    while (!reader.IterativeParseComplete()) {
        reader.IterativeParseNext<kParseDefaultFlags>(ss, handler);
        cout << handler.type << handler.data << endl;
    }

    return 0;
}
