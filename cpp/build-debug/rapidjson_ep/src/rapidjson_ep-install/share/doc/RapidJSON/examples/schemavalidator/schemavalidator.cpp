// Schema Validator example

// The example validates JSON text from stdin with a JSON schema specified in the argument.

#define RAPIDJSON_HAS_STDSTRING 1

#include "rapidjson/error/en.h"
#include "rapidjson/filereadstream.h"
#include "rapidjson/schema.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/prettywriter.h"
#include <string>
#include <iostream>
#include <sstream>

using namespace rapidjson;

typedef GenericValue<UTF8<>, CrtAllocator > ValueType;

// Forward ref
static void CreateErrorMessages(const ValueType& errors, size_t depth, const char* context);

// Convert GenericValue to std::string
static std::string GetString(const ValueType& val) {
  std::ostringstream s;
  if (val.IsString())
    s << val.GetString();
  else if (val.IsDouble())
    s << val.GetDouble();
  else if (val.IsUint())
   s << val.GetUint();
  else if (val.IsInt())
    s << val.GetInt();
  else if (val.IsUint64())
    s << val.GetUint64();
  else if (val.IsInt64())
    s <<  val.GetInt64();
  else if (val.IsBool() && val.GetBool())
    s << "true";
  else if (val.IsBool())
    s << "false";
  else if (val.IsFloat())
    s << val.GetFloat();
  return s.str();}

// Create the error message for a named error
// The error object can either be empty or contain at least member properties:
// {"errorCode": <code>, "instanceRef": "<pointer>", "schemaRef": "<pointer>" }
// Additional properties may be present for use as inserts.
// An "errors" property may be present if there are child errors.
static void HandleError(const char* errorName, const ValueType& error, size_t depth, const char* context) {
  if (!error.ObjectEmpty()) {
    // Get error code and look up error message text (English)
    int code = error["errorCode"].GetInt();
    std::string message(GetValidateError_En(static_cast<ValidateErrorCode>(code)));
    // For each member property in the error, see if its name exists as an insert in the error message and if so replace with the stringified property value
    // So for example - "Number '%actual' is not a multiple of the 'multipleOf' value '%expected'." - we would expect "actual" and "expected" members.
    for (ValueType::ConstMemberIterator insertsItr = error.MemberBegin();
      insertsItr != error.MemberEnd(); ++insertsItr) {
      std::string insertName("%");
      insertName += insertsItr->name.GetString(); // eg "%actual"
      size_t insertPos = message.find(insertName);
      if (insertPos != std::string::npos) {
        std::string insertString("");
        const ValueType &insert = insertsItr->value;
        if (insert.IsArray()) {
          // Member is an array so create comma-separated list of items for the insert string
          for (ValueType::ConstValueIterator itemsItr = insert.Begin(); itemsItr != insert.End(); ++itemsItr) {
            if (itemsItr != insert.Begin()) insertString += ",";
            insertString += GetString(*itemsItr);
          }
        } else {
          insertString += GetString(insert);
        }
        message.replace(insertPos, insertName.length(), insertString);
      }
    }
    // Output error message, references, context
    std::string indent(depth * 2, ' ');
    std::cout << indent << "Error Name: " << errorName << std::endl;
    std::cout << indent << "Message: " << message.c_str() << std::endl;
    std::cout << indent << "Instance: " << error["instanceRef"].GetString() << std::endl;
    std::cout << indent << "Schema: " << error["schemaRef"].GetString() << std::endl;
    if (depth > 0) std::cout << indent << "Context: " << context << std::endl;
    std::cout << std::endl;

    // If child errors exist, apply the process recursively to each error structure.
    // This occurs for "oneOf", "allOf", "anyOf" and "dependencies" errors, so pass the error name as context.
    if (error.HasMember("errors")) {
      depth++;
      const ValueType &childErrors = error["errors"];
      if (childErrors.IsArray()) {
        // Array - each item is an error structure - example
        // "anyOf": {"errorCode": ..., "errors":[{"pattern": {"errorCode\": ...\"}}, {"pattern": {"errorCode\": ...}}]
        for (ValueType::ConstValueIterator errorsItr = childErrors.Begin();
             errorsItr != childErrors.End(); ++errorsItr) {
          CreateErrorMessages(*errorsItr, depth, errorName);
        }
      } else if (childErrors.IsObject()) {
        // Object - each member is an error structure - example
        // "dependencies": {"errorCode": ..., "errors": {"address": {"required": {"errorCode": ...}}, "name": {"required": {"errorCode": ...}}}
        for (ValueType::ConstMemberIterator propsItr = childErrors.MemberBegin();
             propsItr != childErrors.MemberEnd(); ++propsItr) {
          CreateErrorMessages(propsItr->value, depth, errorName);
        }
      }
    }
  }
}

// Create error message for all errors in an error structure
// Context is used to indicate whether the error structure has a parent 'dependencies', 'allOf', 'anyOf' or 'oneOf' error
static void CreateErrorMessages(const ValueType& errors, size_t depth = 0, const char* context = 0) {
    // Each member property contains one or more errors of a given type
    for (ValueType::ConstMemberIterator errorTypeItr = errors.MemberBegin(); errorTypeItr != errors.MemberEnd(); ++errorTypeItr) {
        const char* errorName = errorTypeItr->name.GetString();
        const ValueType& errorContent = errorTypeItr->value;
        if (errorContent.IsArray()) {
            // Member is an array where each item is an error - eg "type": [{"errorCode": ...}, {"errorCode": ...}]
            for (ValueType::ConstValueIterator contentItr = errorContent.Begin(); contentItr != errorContent.End(); ++contentItr) {
                HandleError(errorName, *contentItr, depth, context);
            }
        } else if (errorContent.IsObject()) {
            // Member is an object which is a single error - eg "type": {"errorCode": ... }
            HandleError(errorName, errorContent, depth, context);
        }
    }
}

int main(int argc, char *argv[]) {
    if (argc != 2) {
        fprintf(stderr, "Usage: schemavalidator schema.json < input.json\n");
        return EXIT_FAILURE;
    }

    // Read a JSON schema from file into Document
    Document d;
    char buffer[4096];

    {
        FILE *fp = fopen(argv[1], "r");
        if (!fp) {
            printf("Schema file '%s' not found\n", argv[1]);
            return -1;
        }
        FileReadStream fs(fp, buffer, sizeof(buffer));
        d.ParseStream(fs);
        if (d.HasParseError()) {
            fprintf(stderr, "Schema file '%s' is not a valid JSON\n", argv[1]);
            fprintf(stderr, "Error(offset %u): %s\n",
                static_cast<unsigned>(d.GetErrorOffset()),
                GetParseError_En(d.GetParseError()));
            fclose(fp);
            return EXIT_FAILURE;
        }
        fclose(fp);
    }
    
    // Then convert the Document into SchemaDocument
    SchemaDocument sd(d);

    // Use reader to parse the JSON in stdin, and forward SAX events to validator
    SchemaValidator validator(sd);
    Reader reader;
    FileReadStream is(stdin, buffer, sizeof(buffer));
    if (!reader.Parse(is, validator) && reader.GetParseErrorCode() != kParseErrorTermination) {
        // Schema validator error would cause kParseErrorTermination, which will handle it in next step.
        fprintf(stderr, "Input is not a valid JSON\n");
        fprintf(stderr, "Error(offset %u): %s\n",
            static_cast<unsigned>(reader.GetErrorOffset()),
            GetParseError_En(reader.GetParseErrorCode()));
    }

    // Check the validation result
    if (validator.IsValid()) {
        printf("Input JSON is valid.\n");
        return EXIT_SUCCESS;
    }
    else {
        printf("Input JSON is invalid.\n");
        StringBuffer sb;
        validator.GetInvalidSchemaPointer().StringifyUriFragment(sb);
        fprintf(stderr, "Invalid schema: %s\n", sb.GetString());
        fprintf(stderr, "Invalid keyword: %s\n", validator.GetInvalidSchemaKeyword());
        fprintf(stderr, "Invalid code: %d\n", validator.GetInvalidSchemaCode());
        fprintf(stderr, "Invalid message: %s\n", GetValidateError_En(validator.GetInvalidSchemaCode()));
        sb.Clear();
        validator.GetInvalidDocumentPointer().StringifyUriFragment(sb);
        fprintf(stderr, "Invalid document: %s\n", sb.GetString());
        // Detailed violation report is available as a JSON value
        sb.Clear();
        PrettyWriter<StringBuffer> w(sb);
        validator.GetError().Accept(w);
        fprintf(stderr, "Error report:\n%s\n", sb.GetString());
        CreateErrorMessages(validator.GetError());
        return EXIT_FAILURE;
    }
}
