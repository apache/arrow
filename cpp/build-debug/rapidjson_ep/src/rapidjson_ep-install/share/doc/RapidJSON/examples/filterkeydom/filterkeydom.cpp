// JSON filterkey example which populates filtered SAX events into a Document.

// This example parses JSON text from stdin with validation.
// During parsing, specified key will be filtered using a SAX handler.
// And finally the filtered events are used to populate a Document.
// As an example, the document is written to standard output.

#include "rapidjson/document.h"
#include "rapidjson/writer.h"
#include "rapidjson/filereadstream.h"
#include "rapidjson/filewritestream.h"
#include "rapidjson/error/en.h"
#include <stack>

using namespace rapidjson;

// This handler forwards event into an output handler, with filtering the descendent events of specified key.
template <typename OutputHandler>
class FilterKeyHandler {
public:
    typedef char Ch;

    FilterKeyHandler(OutputHandler& outputHandler, const Ch* keyString, SizeType keyLength) : 
        outputHandler_(outputHandler), keyString_(keyString), keyLength_(keyLength), filterValueDepth_(), filteredKeyCount_()
    {}

    bool Null()             { return filterValueDepth_ > 0 ? EndValue() : outputHandler_.Null()    && EndValue(); }
    bool Bool(bool b)       { return filterValueDepth_ > 0 ? EndValue() : outputHandler_.Bool(b)   && EndValue(); }
    bool Int(int i)         { return filterValueDepth_ > 0 ? EndValue() : outputHandler_.Int(i)    && EndValue(); }
    bool Uint(unsigned u)   { return filterValueDepth_ > 0 ? EndValue() : outputHandler_.Uint(u)   && EndValue(); }
    bool Int64(int64_t i)   { return filterValueDepth_ > 0 ? EndValue() : outputHandler_.Int64(i)  && EndValue(); }
    bool Uint64(uint64_t u) { return filterValueDepth_ > 0 ? EndValue() : outputHandler_.Uint64(u) && EndValue(); }
    bool Double(double d)   { return filterValueDepth_ > 0 ? EndValue() : outputHandler_.Double(d) && EndValue(); }
    bool RawNumber(const Ch* str, SizeType len, bool copy) { return filterValueDepth_ > 0 ? EndValue() : outputHandler_.RawNumber(str, len, copy) && EndValue(); }
    bool String   (const Ch* str, SizeType len, bool copy) { return filterValueDepth_ > 0 ? EndValue() : outputHandler_.String   (str, len, copy) && EndValue(); }
    
    bool StartObject() { 
        if (filterValueDepth_ > 0) {
            filterValueDepth_++;
            return true;
        }
        else {
            filteredKeyCount_.push(0);
            return outputHandler_.StartObject();
        }
    }
    
    bool Key(const Ch* str, SizeType len, bool copy) { 
        if (filterValueDepth_ > 0) 
            return true;
        else if (len == keyLength_ && std::memcmp(str, keyString_, len) == 0) {
            filterValueDepth_ = 1;
            return true;
        }
        else {
            ++filteredKeyCount_.top();
            return outputHandler_.Key(str, len, copy);
        }
    }

    bool EndObject(SizeType) {
        if (filterValueDepth_ > 0) {
            filterValueDepth_--;
            return EndValue();
        }
        else {
            // Use our own filtered memberCount
            SizeType memberCount = filteredKeyCount_.top();
            filteredKeyCount_.pop();
            return outputHandler_.EndObject(memberCount) && EndValue();
        }
    }

    bool StartArray() {
        if (filterValueDepth_ > 0) {
            filterValueDepth_++;
            return true;
        }
        else
            return outputHandler_.StartArray();
    }

    bool EndArray(SizeType elementCount) {
        if (filterValueDepth_ > 0) {
            filterValueDepth_--;
            return EndValue();
        }
        else
            return outputHandler_.EndArray(elementCount) && EndValue();
    }

private:
    FilterKeyHandler(const FilterKeyHandler&);
    FilterKeyHandler& operator=(const FilterKeyHandler&);

    bool EndValue() {
        if (filterValueDepth_ == 1) // Just at the end of value after filtered key
            filterValueDepth_ = 0;
        return true;
    }

    OutputHandler& outputHandler_;
    const char* keyString_;
    const SizeType keyLength_;
    unsigned filterValueDepth_;
    std::stack<SizeType> filteredKeyCount_;
};

// Implements a generator for Document::Populate()
template <typename InputStream>
class FilterKeyReader {
public:
    typedef char Ch;

    FilterKeyReader(InputStream& is, const Ch* keyString, SizeType keyLength) : 
        is_(is), keyString_(keyString), keyLength_(keyLength), parseResult_()
    {}

    // SAX event flow: reader -> filter -> handler
    template <typename Handler>
    bool operator()(Handler& handler) {
        FilterKeyHandler<Handler> filter(handler, keyString_, keyLength_);
        Reader reader;
        parseResult_ = reader.Parse(is_, filter);
        return parseResult_;
    }

    const ParseResult& GetParseResult() const { return parseResult_; }

private:
    FilterKeyReader(const FilterKeyReader&);
    FilterKeyReader& operator=(const FilterKeyReader&);

    InputStream& is_;
    const char* keyString_;
    const SizeType keyLength_;
    ParseResult parseResult_;
};

int main(int argc, char* argv[]) {
    if (argc != 2) {
        fprintf(stderr, "filterkeydom key < input.json > output.json\n");
        return 1;
    }

    // Prepare input stream.
    char readBuffer[65536];
    FileReadStream is(stdin, readBuffer, sizeof(readBuffer));

    // Prepare Filter
    FilterKeyReader<FileReadStream> reader(is, argv[1], static_cast<SizeType>(strlen(argv[1])));

    // Populates the filtered events from reader
    Document document;
    document.Populate(reader);
    ParseResult pr = reader.GetParseResult();
    if (!pr) {
        fprintf(stderr, "\nError(%u): %s\n", static_cast<unsigned>(pr.Offset()), GetParseError_En(pr.Code()));
        return 1;
    }

    // Prepare JSON writer and output stream.
    char writeBuffer[65536];
    FileWriteStream os(stdout, writeBuffer, sizeof(writeBuffer));
    Writer<FileWriteStream> writer(os);

    // Write the document to standard output
    document.Accept(writer);
    return 0;
}
