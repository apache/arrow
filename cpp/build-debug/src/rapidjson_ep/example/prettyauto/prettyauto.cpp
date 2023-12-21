// JSON pretty formatting example
// This example can handle UTF-8/UTF-16LE/UTF-16BE/UTF-32LE/UTF-32BE.
// The input firstly convert to UTF8, and then write to the original encoding with pretty formatting.

#include "rapidjson/reader.h"
#include "rapidjson/prettywriter.h"
#include "rapidjson/filereadstream.h"
#include "rapidjson/filewritestream.h"
#include "rapidjson/encodedstream.h"    // NEW
#include "rapidjson/error/en.h"
#ifdef _WIN32
#include <fcntl.h>
#include <io.h>
#endif

using namespace rapidjson;

int main(int, char*[]) {
#ifdef _WIN32
    // Prevent Windows converting between CR+LF and LF
    _setmode(_fileno(stdin), _O_BINARY);    // NEW
    _setmode(_fileno(stdout), _O_BINARY);   // NEW
#endif

    // Prepare reader and input stream.
    //Reader reader;
    GenericReader<AutoUTF<unsigned>, UTF8<> > reader;       // CHANGED
    char readBuffer[65536];
    FileReadStream is(stdin, readBuffer, sizeof(readBuffer));
    AutoUTFInputStream<unsigned, FileReadStream> eis(is);   // NEW

    // Prepare writer and output stream.
    char writeBuffer[65536];
    FileWriteStream os(stdout, writeBuffer, sizeof(writeBuffer));

#if 1
    // Use the same Encoding of the input. Also use BOM according to input.
    typedef AutoUTFOutputStream<unsigned, FileWriteStream> OutputStream;    // NEW
    OutputStream eos(os, eis.GetType(), eis.HasBOM());                      // NEW
    PrettyWriter<OutputStream, UTF8<>, AutoUTF<unsigned> > writer(eos);     // CHANGED
#else
    // You may also use static bound encoding type, such as output to UTF-16LE with BOM
    typedef EncodedOutputStream<UTF16LE<>,FileWriteStream> OutputStream;    // NEW
    OutputStream eos(os, true);                                             // NEW
    PrettyWriter<OutputStream, UTF8<>, UTF16LE<> > writer(eos);             // CHANGED
#endif

    // JSON reader parse from the input stream and let writer generate the output.
    //if (!reader.Parse<kParseValidateEncodingFlag>(is, writer)) {
    if (!reader.Parse<kParseValidateEncodingFlag>(eis, writer)) {   // CHANGED
        fprintf(stderr, "\nError(%u): %s\n", static_cast<unsigned>(reader.GetErrorOffset()), GetParseError_En(reader.GetParseErrorCode()));
        return 1;
    }

    return 0;
}
