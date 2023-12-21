// JSON to JSONx conversion example, using SAX API.
// JSONx is an IBM standard format to represent JSON as XML.
// https://www-01.ibm.com/support/knowledgecenter/SS9H2Y_7.1.0/com.ibm.dp.doc/json_jsonx.html
// This example parses JSON text from stdin with validation, 
// and convert to JSONx format to stdout.
// Need compile with -D__STDC_FORMAT_MACROS for defining PRId64 and PRIu64 macros.

#include "rapidjson/reader.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/filereadstream.h"
#include "rapidjson/filewritestream.h"
#include "rapidjson/error/en.h"
#include <cstdio>

using namespace rapidjson;

// For simplicity, this example only read/write in UTF-8 encoding
template <typename OutputStream>
class JsonxWriter {
public:
    JsonxWriter(OutputStream& os) : os_(os), name_(), level_(0), hasName_(false) {
    }

    bool Null() {
        return WriteStartElement("null", true);
    }
    
    bool Bool(bool b) {
        return 
            WriteStartElement("boolean") &&
            WriteString(b ? "true" : "false") &&
            WriteEndElement("boolean");
    }
    
    bool Int(int i) {
        char buffer[12];
        return WriteNumberElement(buffer, sprintf(buffer, "%d", i));
    }
    
    bool Uint(unsigned i) {
        char buffer[11];
        return WriteNumberElement(buffer, sprintf(buffer, "%u", i));
    }
    
    bool Int64(int64_t i) {
        char buffer[21];
        return WriteNumberElement(buffer, sprintf(buffer, "%" PRId64, i));
    }
    
    bool Uint64(uint64_t i) {
        char buffer[21];
        return WriteNumberElement(buffer, sprintf(buffer, "%" PRIu64, i));
    }
    
    bool Double(double d) {
        char buffer[30];
        return WriteNumberElement(buffer, sprintf(buffer, "%.17g", d));
    }

    bool RawNumber(const char* str, SizeType length, bool) {
        return
            WriteStartElement("number") &&
            WriteEscapedText(str, length) &&
            WriteEndElement("number");
    }

    bool String(const char* str, SizeType length, bool) {
        return
            WriteStartElement("string") &&
            WriteEscapedText(str, length) &&
            WriteEndElement("string");
    }

    bool StartObject() {
        return WriteStartElement("object");
    }

    bool Key(const char* str, SizeType length, bool) {
        // backup key to name_
        name_.Clear();
        for (SizeType i = 0; i < length; i++)
            name_.Put(str[i]);
        hasName_ = true;
        return true;
    }

    bool EndObject(SizeType) {
        return WriteEndElement("object");
    }

    bool StartArray() {
        return WriteStartElement("array");
    }

    bool EndArray(SizeType) {
        return WriteEndElement("array");
    }

private:
    bool WriteString(const char* s) {
        while (*s)
            os_.Put(*s++);
        return true;
    }

    bool WriteEscapedAttributeValue(const char* s, size_t length) {
        for (size_t i = 0; i < length; i++) {
            switch (s[i]) {
                case '&': WriteString("&amp;"); break;
                case '<': WriteString("&lt;"); break;
                case '"': WriteString("&quot;"); break;
                default: os_.Put(s[i]); break;
            }
        }
        return true;
    }

    bool WriteEscapedText(const char* s, size_t length) {
        for (size_t i = 0; i < length; i++) {
            switch (s[i]) {
                case '&': WriteString("&amp;"); break;
                case '<': WriteString("&lt;"); break;
                default: os_.Put(s[i]); break;
            }
        }
        return true;
    }

    bool WriteStartElement(const char* type, bool emptyElement = false) {
        if (level_ == 0)
            if (!WriteString("<?xml version=\"1.0\" encoding=\"UTF-8\"?>"))
                return false;

        if (!WriteString("<json:") || !WriteString(type))
            return false;

        // For root element, need to add declarations
        if (level_ == 0) {
            if (!WriteString(
                " xsi:schemaLocation=\"http://www.datapower.com/schemas/json jsonx.xsd\""
                " xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\""
                " xmlns:json=\"http://www.ibm.com/xmlns/prod/2009/jsonx\""))
                return false;
        }

        if (hasName_) {
            hasName_ = false;
            if (!WriteString(" name=\"") ||
                !WriteEscapedAttributeValue(name_.GetString(), name_.GetSize()) ||
                !WriteString("\""))
                return false;
        }

        if (emptyElement)
            return WriteString("/>");
        else {
            level_++;
            return WriteString(">");
        }
    }

    bool WriteEndElement(const char* type) {
        if (!WriteString("</json:") ||
            !WriteString(type) ||
            !WriteString(">"))
            return false;

        // For the last end tag, flush the output stream.
        if (--level_ == 0)
            os_.Flush();

        return true;
    }

    bool WriteNumberElement(const char* buffer, int length) {
        if (!WriteStartElement("number"))
            return false;
        for (int j = 0; j < length; j++)
            os_.Put(buffer[j]);
        return WriteEndElement("number");
    }

    OutputStream& os_;
    StringBuffer name_;
    unsigned level_;
    bool hasName_;
};

int main(int, char*[]) {
    // Prepare JSON reader and input stream.
    Reader reader;
    char readBuffer[65536];
    FileReadStream is(stdin, readBuffer, sizeof(readBuffer));

    // Prepare JSON writer and output stream.
    char writeBuffer[65536];
    FileWriteStream os(stdout, writeBuffer, sizeof(writeBuffer));
    JsonxWriter<FileWriteStream> writer(os);

    // JSON reader parse from the input stream and let writer generate the output.
    if (!reader.Parse(is, writer)) {
        fprintf(stderr, "\nError(%u): %s\n", static_cast<unsigned>(reader.GetErrorOffset()), GetParseError_En(reader.GetParseErrorCode()));
        return 1;
    }

    return 0;
}
