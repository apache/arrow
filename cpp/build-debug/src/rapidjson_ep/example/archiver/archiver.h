#ifndef ARCHIVER_H_
#define ARCHIVER_H_

#include <cstddef>
#include <string>

/**
\class Archiver
\brief Archiver concept

Archiver can be a reader or writer for serialization or deserialization respectively.

class Archiver {
public:
    /// \returns true if the archiver is in normal state. false if it has errors.
    operator bool() const;

    /// Starts an object
    Archiver& StartObject();
    
    /// After calling StartObject(), assign a member with a name
    Archiver& Member(const char* name);

    /// After calling StartObject(), check if a member presents
    bool HasMember(const char* name) const;

    /// Ends an object
    Archiver& EndObject();

    /// Starts an array
    /// \param size If Archiver::IsReader is true, the size of array is written.
    Archiver& StartArray(size_t* size = 0);

    /// Ends an array
    Archiver& EndArray();

    /// Read/Write primitive types.
    Archiver& operator&(bool& b);
    Archiver& operator&(unsigned& u);
    Archiver& operator&(int& i);
    Archiver& operator&(double& d);
    Archiver& operator&(std::string& s);

    /// Write primitive types.
    Archiver& SetNull();

    //! Whether it is a reader.
    static const bool IsReader;

    //! Whether it is a writer.
    static const bool IsWriter;
};
*/

/// Represents a JSON reader which implements Archiver concept.
class JsonReader {
public:
    /// Constructor.
    /**
        \param json A non-const source json string for in-situ parsing.
        \note in-situ means the source JSON string will be modified after parsing.
    */
    JsonReader(const char* json);

    /// Destructor.
    ~JsonReader();

    // Archive concept

    operator bool() const { return !mError; }

    JsonReader& StartObject();
    JsonReader& Member(const char* name);
    bool HasMember(const char* name) const;
    JsonReader& EndObject();

    JsonReader& StartArray(size_t* size = 0);
    JsonReader& EndArray();

    JsonReader& operator&(bool& b);
    JsonReader& operator&(unsigned& u);
    JsonReader& operator&(int& i);
    JsonReader& operator&(double& d);
    JsonReader& operator&(std::string& s);

    JsonReader& SetNull();

    static const bool IsReader = true;
    static const bool IsWriter = !IsReader;

private:
    JsonReader(const JsonReader&);
    JsonReader& operator=(const JsonReader&);

    void Next();

    // PIMPL
    void* mDocument;              ///< DOM result of parsing.
    void* mStack;                 ///< Stack for iterating the DOM
    bool mError;                  ///< Whether an error has occurred.
};

class JsonWriter {
public:
    /// Constructor.
    JsonWriter();

    /// Destructor.
    ~JsonWriter();

    /// Obtains the serialized JSON string.
    const char* GetString() const;

    // Archive concept

    operator bool() const { return true; }

    JsonWriter& StartObject();
    JsonWriter& Member(const char* name);
    bool HasMember(const char* name) const;
    JsonWriter& EndObject();

    JsonWriter& StartArray(size_t* size = 0);
    JsonWriter& EndArray();

    JsonWriter& operator&(bool& b);
    JsonWriter& operator&(unsigned& u);
    JsonWriter& operator&(int& i);
    JsonWriter& operator&(double& d);
    JsonWriter& operator&(std::string& s);
    JsonWriter& SetNull();

    static const bool IsReader = false;
    static const bool IsWriter = !IsReader;

private:
    JsonWriter(const JsonWriter&);
    JsonWriter& operator=(const JsonWriter&);

    // PIMPL idiom
    void* mWriter;      ///< JSON writer.
    void* mStream;      ///< Stream buffer.
};

#endif // ARCHIVER_H__
