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

#define RAPIDJSON_SCHEMA_VERBOSE 0
#define RAPIDJSON_HAS_STDSTRING 1

#include "unittest.h"
#include "rapidjson/schema.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"
#include "rapidjson/error/error.h"
#include "rapidjson/error/en.h"

#ifdef __clang__
RAPIDJSON_DIAG_PUSH
RAPIDJSON_DIAG_OFF(variadic-macros)
#elif defined(_MSC_VER)
RAPIDJSON_DIAG_PUSH
RAPIDJSON_DIAG_OFF(4822) // local class member function does not have a body
#endif

using namespace rapidjson;

#define TEST_HASHER(json1, json2, expected) \
{\
    Document d1, d2;\
    d1.Parse(json1);\
    ASSERT_FALSE(d1.HasParseError());\
    d2.Parse(json2);\
    ASSERT_FALSE(d2.HasParseError());\
    internal::Hasher<Value, CrtAllocator> h1, h2;\
    d1.Accept(h1);\
    d2.Accept(h2);\
    ASSERT_TRUE(h1.IsValid());\
    ASSERT_TRUE(h2.IsValid());\
    /*printf("%s: 0x%016llx\n%s: 0x%016llx\n\n", json1, h1.GetHashCode(), json2, h2.GetHashCode());*/\
    EXPECT_TRUE(expected == (h1.GetHashCode() == h2.GetHashCode()));\
}

TEST(SchemaValidator, Hasher) {
    TEST_HASHER("null", "null", true);

    TEST_HASHER("true", "true", true);
    TEST_HASHER("false", "false", true);
    TEST_HASHER("true", "false", false);
    TEST_HASHER("false", "true", false);
    TEST_HASHER("true", "null", false);
    TEST_HASHER("false", "null", false);

    TEST_HASHER("1", "1", true);
    TEST_HASHER("2147483648", "2147483648", true); // 2^31 can only be fit in unsigned
    TEST_HASHER("-2147483649", "-2147483649", true); // -2^31 - 1 can only be fit in int64_t
    TEST_HASHER("2147483648", "2147483648", true); // 2^31 can only be fit in unsigned
    TEST_HASHER("4294967296", "4294967296", true); // 2^32 can only be fit in int64_t
    TEST_HASHER("9223372036854775808", "9223372036854775808", true); // 2^63 can only be fit in uint64_t
    TEST_HASHER("1.5", "1.5", true);
    TEST_HASHER("1", "1.0", true);
    TEST_HASHER("1", "-1", false);
    TEST_HASHER("0.0", "-0.0", false);
    TEST_HASHER("1", "true", false);
    TEST_HASHER("0", "false", false);
    TEST_HASHER("0", "null", false);

    TEST_HASHER("\"\"", "\"\"", true);
    TEST_HASHER("\"\"", "\"\\u0000\"", false);
    TEST_HASHER("\"Hello\"", "\"Hello\"", true);
    TEST_HASHER("\"Hello\"", "\"World\"", false);
    TEST_HASHER("\"Hello\"", "null", false);
    TEST_HASHER("\"Hello\\u0000\"", "\"Hello\"", false);
    TEST_HASHER("\"\"", "null", false);
    TEST_HASHER("\"\"", "true", false);
    TEST_HASHER("\"\"", "false", false);

    TEST_HASHER("[]", "[ ]", true);
    TEST_HASHER("[1, true, false]", "[1, true, false]", true);
    TEST_HASHER("[1, true, false]", "[1, true]", false);
    TEST_HASHER("[1, 2]", "[2, 1]", false);
    TEST_HASHER("[[1], 2]", "[[1, 2]]", false);
    TEST_HASHER("[1, 2]", "[1, [2]]", false);
    TEST_HASHER("[]", "null", false);
    TEST_HASHER("[]", "true", false);
    TEST_HASHER("[]", "false", false);
    TEST_HASHER("[]", "0", false);
    TEST_HASHER("[]", "0.0", false);
    TEST_HASHER("[]", "\"\"", false);

    TEST_HASHER("{}", "{ }", true);
    TEST_HASHER("{\"a\":1}", "{\"a\":1}", true);
    TEST_HASHER("{\"a\":1}", "{\"b\":1}", false);
    TEST_HASHER("{\"a\":1}", "{\"a\":2}", false);
    TEST_HASHER("{\"a\":1, \"b\":2}", "{\"b\":2, \"a\":1}", true); // Member order insensitive
    TEST_HASHER("{}", "null", false);
    TEST_HASHER("{}", "false", false);
    TEST_HASHER("{}", "true", false);
    TEST_HASHER("{}", "0", false);
    TEST_HASHER("{}", "0.0", false);
    TEST_HASHER("{}", "\"\"", false);
}

// Test cases following http://spacetelescope.github.io/understanding-json-schema

#define VALIDATE(schema, json, expected) \
{\
    SchemaValidator validator(schema);\
    Document d;\
    /*printf("\n%s\n", json);*/\
    d.Parse(json);\
    EXPECT_FALSE(d.HasParseError());\
    EXPECT_TRUE(expected == d.Accept(validator));\
    EXPECT_TRUE(expected == validator.IsValid());\
    ValidateErrorCode code = validator.GetInvalidSchemaCode();\
    if (expected) {\
      EXPECT_TRUE(code == kValidateErrorNone);\
      EXPECT_TRUE(validator.GetInvalidSchemaKeyword() == 0);\
    }\
    if ((expected) && !validator.IsValid()) {\
        StringBuffer sb;\
        validator.GetInvalidSchemaPointer().StringifyUriFragment(sb);\
        printf("Invalid schema: %s\n", sb.GetString());\
        printf("Invalid keyword: %s\n", validator.GetInvalidSchemaKeyword());\
        printf("Invalid code: %d\n", code);\
        printf("Invalid message: %s\n", GetValidateError_En(code));\
        sb.Clear();\
        validator.GetInvalidDocumentPointer().StringifyUriFragment(sb);\
        printf("Invalid document: %s\n", sb.GetString());\
        sb.Clear();\
        Writer<StringBuffer> w(sb);\
        validator.GetError().Accept(w);\
        printf("Validation error: %s\n", sb.GetString());\
    }\
}

#define INVALIDATE(schema, json, invalidSchemaPointer, invalidSchemaKeyword, invalidDocumentPointer, error) \
{\
    INVALIDATE_(schema, json, invalidSchemaPointer, invalidSchemaKeyword, invalidDocumentPointer, error, kValidateDefaultFlags, SchemaValidator, Pointer) \
}

#define INVALIDATE_(schema, json, invalidSchemaPointer, invalidSchemaKeyword, invalidDocumentPointer, error, \
    flags, SchemaValidatorType, PointerType) \
{\
    SchemaValidatorType validator(schema);\
    validator.SetValidateFlags(flags);\
    Document d;\
    /*printf("\n%s\n", json);*/\
    d.Parse(json);\
    EXPECT_FALSE(d.HasParseError());\
    d.Accept(validator);\
    EXPECT_FALSE(validator.IsValid());\
    ValidateErrorCode code = validator.GetInvalidSchemaCode();\
    ASSERT_TRUE(code != kValidateErrorNone);\
    ASSERT_TRUE(strcmp(GetValidateError_En(code), "Unknown error.") != 0);\
    if (validator.GetInvalidSchemaPointer() != PointerType(invalidSchemaPointer)) {\
        StringBuffer sb;\
        validator.GetInvalidSchemaPointer().Stringify(sb);\
        printf("GetInvalidSchemaPointer() Expected: %s Actual: %s\n", invalidSchemaPointer, sb.GetString());\
        ADD_FAILURE();\
    }\
    ASSERT_TRUE(validator.GetInvalidSchemaKeyword() != 0);\
    if (strcmp(validator.GetInvalidSchemaKeyword(), invalidSchemaKeyword) != 0) {\
        printf("GetInvalidSchemaKeyword() Expected: %s Actual %s\n", invalidSchemaKeyword, validator.GetInvalidSchemaKeyword());\
        ADD_FAILURE();\
    }\
    if (validator.GetInvalidDocumentPointer() != PointerType(invalidDocumentPointer)) {\
        StringBuffer sb;\
        validator.GetInvalidDocumentPointer().Stringify(sb);\
        printf("GetInvalidDocumentPointer() Expected: %s Actual: %s\n", invalidDocumentPointer, sb.GetString());\
        ADD_FAILURE();\
    }\
    Document e;\
    e.Parse(error);\
    if (validator.GetError() != e) {\
        StringBuffer sb;\
        Writer<StringBuffer> w(sb);\
        validator.GetError().Accept(w);\
        printf("GetError() Expected: %s Actual: %s\n", error, sb.GetString());\
        ADD_FAILURE();\
    }\
}

TEST(SchemaValidator, Typeless) {
    Document sd;
    sd.Parse("{}");
    SchemaDocument s(sd);
    
    VALIDATE(s, "42", true);
    VALIDATE(s, "\"I'm a string\"", true);
    VALIDATE(s, "{ \"an\": [ \"arbitrarily\", \"nested\" ], \"data\": \"structure\" }", true);
}

TEST(SchemaValidator, MultiType) {
    Document sd;
    sd.Parse("{ \"type\": [\"number\", \"string\"] }");
    SchemaDocument s(sd);

    VALIDATE(s, "42", true);
    VALIDATE(s, "\"Life, the universe, and everything\"", true);
    INVALIDATE(s, "[\"Life\", \"the universe\", \"and everything\"]", "", "type", "",
        "{ \"type\": {"
        "    \"errorCode\": 20,"
        "    \"instanceRef\": \"#\", \"schemaRef\": \"#\","
        "    \"expected\": [\"string\", \"number\"], \"actual\": \"array\""
        "}}");
}

TEST(SchemaValidator, Enum_Typed) {
    Document sd;
    sd.Parse("{ \"type\": \"string\", \"enum\" : [\"red\", \"amber\", \"green\"] }");
    SchemaDocument s(sd);

    VALIDATE(s, "\"red\"", true);
    INVALIDATE(s, "\"blue\"", "", "enum", "",
        "{ \"enum\": { \"errorCode\": 19, \"instanceRef\": \"#\", \"schemaRef\": \"#\" }}");
}

TEST(SchemaValidator, Enum_Typless) {
    Document sd;
    sd.Parse("{  \"enum\": [\"red\", \"amber\", \"green\", null, 42] }");
    SchemaDocument s(sd);

    VALIDATE(s, "\"red\"", true);
    VALIDATE(s, "null", true);
    VALIDATE(s, "42", true);
    INVALIDATE(s, "0", "", "enum", "",
        "{ \"enum\": { \"errorCode\": 19, \"instanceRef\": \"#\", \"schemaRef\": \"#\" }}");
}

TEST(SchemaValidator, Enum_InvalidType) {
    Document sd;
    sd.Parse("{ \"type\": \"string\", \"enum\": [\"red\", \"amber\", \"green\", null] }");
    SchemaDocument s(sd);

    VALIDATE(s, "\"red\"", true);
    INVALIDATE(s, "null", "", "type", "",
        "{ \"type\": {"
        "    \"errorCode\": 20,"
        "    \"instanceRef\": \"#\", \"schemaRef\": \"#\","
        "    \"expected\": [\"string\"], \"actual\": \"null\""
        "}}");
}

TEST(SchemaValidator, AllOf) {
    {
        Document sd;
        sd.Parse("{\"allOf\": [{ \"type\": \"string\" }, { \"type\": \"string\", \"maxLength\": 5 }]}");
        SchemaDocument s(sd);

        VALIDATE(s, "\"ok\"", true);
        INVALIDATE(s, "\"too long\"", "", "allOf", "",
            "{ \"allOf\": {"
            "    \"errors\": ["
            "      {},"
            "      {\"maxLength\": {\"errorCode\": 6, \"instanceRef\": \"#\", \"schemaRef\": \"#/allOf/1\", \"expected\": 5, \"actual\": \"too long\"}}"
            "    ],"
            "    \"errorCode\": 23, \"instanceRef\": \"#\", \"schemaRef\": \"#\""
            "}}");
    }
    {
        Document sd;
        sd.Parse("{\"allOf\": [{ \"type\": \"string\" }, { \"type\": \"number\" } ] }");
        SchemaDocument s(sd);

        VALIDATE(s, "\"No way\"", false);
        INVALIDATE(s, "-1", "", "allOf", "",
            "{ \"allOf\": {"
            "    \"errors\": ["
            "      {\"type\": { \"instanceRef\": \"#\", \"schemaRef\": \"#/allOf/0\", \"errorCode\": 20, \"expected\": [\"string\"], \"actual\": \"integer\"}},"
            "      {}"
            "    ],"
            "    \"errorCode\": 23, \"instanceRef\": \"#\", \"schemaRef\": \"#\""
            "}}");
    }
}

TEST(SchemaValidator, AnyOf) {
    Document sd;
    sd.Parse("{\"anyOf\": [{ \"type\": \"string\" }, { \"type\": \"number\" } ] }");
    SchemaDocument s(sd);

    VALIDATE(s, "\"Yes\"", true);
    VALIDATE(s, "42", true);
    INVALIDATE(s, "{ \"Not a\": \"string or number\" }", "", "anyOf", "",
        "{ \"anyOf\": {"
        "    \"errorCode\": 24,"
        "    \"instanceRef\": \"#\", \"schemaRef\": \"#\", "
        "    \"errors\": ["
        "      { \"type\": {"
        "          \"errorCode\": 20,"
        "          \"instanceRef\": \"#\", \"schemaRef\": \"#/anyOf/0\","
        "          \"expected\": [\"string\"], \"actual\": \"object\""
        "      }},"
        "      { \"type\": {"
        "          \"errorCode\": 20,"
        "          \"instanceRef\": \"#\", \"schemaRef\": \"#/anyOf/1\","
        "          \"expected\": [\"number\"], \"actual\": \"object\""
        "      }}"
        "    ]"
        "}}");
}

TEST(SchemaValidator, OneOf) {
    Document sd;
    sd.Parse("{\"oneOf\": [{ \"type\": \"number\", \"multipleOf\": 5 }, { \"type\": \"number\", \"multipleOf\": 3 } ] }");
    SchemaDocument s(sd);

    VALIDATE(s, "10", true);
    VALIDATE(s, "9", true);
    INVALIDATE(s, "2", "", "oneOf", "",
        "{ \"oneOf\": {"
        "    \"errorCode\": 21,"
        "    \"instanceRef\": \"#\", \"schemaRef\": \"#\","
        "    \"errors\": ["
        "      { \"multipleOf\": {"
        "          \"errorCode\": 1,"
        "          \"instanceRef\": \"#\", \"schemaRef\": \"#/oneOf/0\","
        "          \"expected\": 5, \"actual\": 2"
        "      }},"
        "      { \"multipleOf\": {"
        "          \"errorCode\": 1,"
        "          \"instanceRef\": \"#\", \"schemaRef\": \"#/oneOf/1\","
        "          \"expected\": 3, \"actual\": 2"
        "      }}"
        "    ]"
        "}}");
    INVALIDATE(s, "15", "", "oneOf", "",
        "{ \"oneOf\": { \"errorCode\": 22, \"instanceRef\": \"#\", \"schemaRef\": \"#\", \"errors\": [{}, {}]}}");
}

TEST(SchemaValidator, Not) {
    Document sd;
    sd.Parse("{\"not\":{ \"type\": \"string\"}}");
    SchemaDocument s(sd);

    VALIDATE(s, "42", true);
    VALIDATE(s, "{ \"key\": \"value\" }", true);
    INVALIDATE(s, "\"I am a string\"", "", "not", "",
        "{ \"not\": { \"errorCode\": 25, \"instanceRef\": \"#\", \"schemaRef\": \"#\" }}");
}

TEST(SchemaValidator, Ref) {
    Document sd;
    sd.Parse(
        "{"
        "  \"$schema\": \"http://json-schema.org/draft-04/schema#\","
        ""
        "  \"definitions\": {"
        "    \"address\": {"
        "      \"type\": \"object\","
        "      \"properties\": {"
        "        \"street_address\": { \"type\": \"string\" },"
        "        \"city\":           { \"type\": \"string\" },"
        "        \"state\":          { \"type\": \"string\" }"
        "      },"
        "      \"required\": [\"street_address\", \"city\", \"state\"]"
        "    }"
        "  },"
        "  \"type\": \"object\","
        "  \"properties\": {"
        "    \"billing_address\": { \"$ref\": \"#/definitions/address\" },"
        "    \"shipping_address\": { \"$ref\": \"#/definitions/address\" }"
        "  }"
        "}");
    SchemaDocument s(sd);

    VALIDATE(s, "{\"shipping_address\": {\"street_address\": \"1600 Pennsylvania Avenue NW\", \"city\": \"Washington\", \"state\": \"DC\"}, \"billing_address\": {\"street_address\": \"1st Street SE\", \"city\": \"Washington\", \"state\": \"DC\"} }", true);
}

TEST(SchemaValidator, Ref_AllOf) {
    Document sd;
    sd.Parse(
        "{"
        "  \"$schema\": \"http://json-schema.org/draft-04/schema#\","
        ""
        "  \"definitions\": {"
        "    \"address\": {"
        "      \"type\": \"object\","
        "      \"properties\": {"
        "        \"street_address\": { \"type\": \"string\" },"
        "        \"city\":           { \"type\": \"string\" },"
        "        \"state\":          { \"type\": \"string\" }"
        "      },"
        "      \"required\": [\"street_address\", \"city\", \"state\"]"
        "    }"
        "  },"
        "  \"type\": \"object\","
        "  \"properties\": {"
        "    \"billing_address\": { \"$ref\": \"#/definitions/address\" },"
        "    \"shipping_address\": {"
        "      \"allOf\": ["
        "        { \"$ref\": \"#/definitions/address\" },"
        "        { \"properties\":"
        "          { \"type\": { \"enum\": [ \"residential\", \"business\" ] } },"
        "          \"required\": [\"type\"]"
        "        }"
        "      ]"
        "    }"
        "  }"
        "}");
    SchemaDocument s(sd);

    INVALIDATE(s, "{\"shipping_address\": {\"street_address\": \"1600 Pennsylvania Avenue NW\", \"city\": \"Washington\", \"state\": \"DC\"} }", "/properties/shipping_address", "allOf", "/shipping_address",
        "{ \"allOf\": {"
        "    \"errors\": ["
        "      {},"
        "      {\"required\": {\"errorCode\": 15, \"instanceRef\": \"#/shipping_address\", \"schemaRef\": \"#/properties/shipping_address/allOf/1\", \"missing\": [\"type\"]}}"
        "    ],"
        "    \"errorCode\":23,\"instanceRef\":\"#/shipping_address\",\"schemaRef\":\"#/properties/shipping_address\""
        "}}");
    VALIDATE(s, "{\"shipping_address\": {\"street_address\": \"1600 Pennsylvania Avenue NW\", \"city\": \"Washington\", \"state\": \"DC\", \"type\": \"business\"} }", true);
}

TEST(SchemaValidator, String) {
    Document sd;
    sd.Parse("{\"type\":\"string\"}");
    SchemaDocument s(sd);

    VALIDATE(s, "\"I'm a string\"", true);
    INVALIDATE(s, "42", "", "type", "",
        "{ \"type\": {"
        "    \"errorCode\": 20,"
        "    \"instanceRef\": \"#\", \"schemaRef\": \"#\","
        "    \"expected\": [\"string\"], \"actual\": \"integer\""
        "}}");
    INVALIDATE(s, "2147483648", "", "type", "",
        "{ \"type\": {"
        "    \"errorCode\": 20,"
        "    \"instanceRef\": \"#\", \"schemaRef\": \"#\","
        "    \"expected\": [\"string\"], \"actual\": \"integer\""
        "}}"); // 2^31 can only be fit in unsigned
    INVALIDATE(s, "-2147483649", "", "type", "",
        "{ \"type\": {"
        "    \"errorCode\": 20,"
        "    \"instanceRef\": \"#\", \"schemaRef\": \"#\","
        "    \"expected\": [\"string\"], \"actual\": \"integer\""
        "}}"); // -2^31 - 1 can only be fit in int64_t
    INVALIDATE(s, "4294967296", "", "type", "",
        "{ \"type\": {"
        "    \"errorCode\": 20,"
        "    \"instanceRef\": \"#\", \"schemaRef\": \"#\","
        "    \"expected\": [\"string\"], \"actual\": \"integer\""
        "}}"); // 2^32 can only be fit in int64_t
    INVALIDATE(s, "3.1415926", "", "type", "",
        "{ \"type\": {"
        "    \"errorCode\": 20,"
        "    \"instanceRef\": \"#\", \"schemaRef\": \"#\","
        "    \"expected\": [\"string\"], \"actual\": \"number\""
        "}}");
}

TEST(SchemaValidator, String_LengthRange) {
    Document sd;
    sd.Parse("{\"type\":\"string\",\"minLength\":2,\"maxLength\":3}");
    SchemaDocument s(sd);

    INVALIDATE(s, "\"A\"", "", "minLength", "",
        "{ \"minLength\": {"
        "    \"errorCode\": 7,"
        "    \"instanceRef\": \"#\", \"schemaRef\": \"#\","
        "    \"expected\": 2, \"actual\": \"A\""
        "}}");
    VALIDATE(s, "\"AB\"", true);
    VALIDATE(s, "\"ABC\"", true);
    INVALIDATE(s, "\"ABCD\"", "", "maxLength", "",
        "{ \"maxLength\": {"
        "    \"errorCode\": 6,"
        "    \"instanceRef\": \"#\", \"schemaRef\": \"#\","
        "    \"expected\": 3, \"actual\": \"ABCD\""
        "}}");
}

#if RAPIDJSON_SCHEMA_HAS_REGEX
TEST(SchemaValidator, String_Pattern) {
    Document sd;
    sd.Parse("{\"type\":\"string\",\"pattern\":\"^(\\\\([0-9]{3}\\\\))?[0-9]{3}-[0-9]{4}$\"}");
    SchemaDocument s(sd);

    VALIDATE(s, "\"555-1212\"", true);
    VALIDATE(s, "\"(888)555-1212\"", true);
    INVALIDATE(s, "\"(888)555-1212 ext. 532\"", "", "pattern", "",
        "{ \"pattern\": {"
        "    \"errorCode\": 8,"
        "    \"instanceRef\": \"#\", \"schemaRef\": \"#\","
        "    \"actual\": \"(888)555-1212 ext. 532\""
        "}}");
    INVALIDATE(s, "\"(800)FLOWERS\"", "", "pattern", "",
        "{ \"pattern\": {"
        "    \"errorCode\": 8,"
        "    \"instanceRef\": \"#\", \"schemaRef\": \"#\","
        "    \"actual\": \"(800)FLOWERS\""
        "}}");
}

TEST(SchemaValidator, String_Pattern_Invalid) {
    Document sd;
    sd.Parse("{\"type\":\"string\",\"pattern\":\"a{0}\"}"); // TODO: report regex is invalid somehow
    SchemaDocument s(sd);

    VALIDATE(s, "\"\"", true);
    VALIDATE(s, "\"a\"", true);
    VALIDATE(s, "\"aa\"", true);
}
#endif

TEST(SchemaValidator, Integer) {
    Document sd;
    sd.Parse("{\"type\":\"integer\"}");
    SchemaDocument s(sd);

    VALIDATE(s, "42", true);
    VALIDATE(s, "-1", true);
    VALIDATE(s, "2147483648", true); // 2^31 can only be fit in unsigned
    VALIDATE(s, "-2147483649", true); // -2^31 - 1 can only be fit in int64_t
    VALIDATE(s, "2147483648", true); // 2^31 can only be fit in unsigned
    VALIDATE(s, "4294967296", true); // 2^32 can only be fit in int64_t
    INVALIDATE(s, "3.1415926", "", "type", "",
        "{ \"type\": {"
        "    \"errorCode\": 20,"
        "    \"instanceRef\": \"#\", \"schemaRef\": \"#\","
        "    \"expected\": [\"integer\"], \"actual\": \"number\""
        "}}");
    INVALIDATE(s, "\"42\"", "", "type", "",
        "{ \"type\": {"
        "    \"errorCode\": 20,"
        "    \"instanceRef\": \"#\", \"schemaRef\": \"#\","
        "    \"expected\": [\"integer\"], \"actual\": \"string\""
        "}}");
}

TEST(SchemaValidator, Integer_Range) {
    Document sd;
    sd.Parse("{\"type\":\"integer\",\"minimum\":0,\"maximum\":100,\"exclusiveMaximum\":true}");
    SchemaDocument s(sd);

    INVALIDATE(s, "-1", "", "minimum", "",
        "{ \"minimum\": {"
        "    \"errorCode\": 4,"
        "    \"instanceRef\": \"#\", \"schemaRef\": \"#\","
        "    \"expected\": 0, \"actual\": -1"
        "}}");
    VALIDATE(s, "0", true);
    VALIDATE(s, "10", true);
    VALIDATE(s, "99", true);
    INVALIDATE(s, "100", "", "maximum", "",
        "{ \"maximum\": {"
        "    \"errorCode\": 3,"
        "    \"instanceRef\": \"#\", \"schemaRef\": \"#\","
        "    \"expected\": 100, \"exclusiveMaximum\": true, \"actual\": 100"
        "}}");
    INVALIDATE(s, "101", "", "maximum", "",
        "{ \"maximum\": {"
        "    \"errorCode\": 3,"
        "    \"instanceRef\": \"#\", \"schemaRef\": \"#\","
        "    \"expected\": 100, \"exclusiveMaximum\": true, \"actual\": 101"
        "}}");
}

TEST(SchemaValidator, Integer_Range64Boundary) {
    Document sd;
    sd.Parse("{\"type\":\"integer\",\"minimum\":-9223372036854775807,\"maximum\":9223372036854775806}");
    SchemaDocument s(sd);

    INVALIDATE(s, "-9223372036854775808", "", "minimum", "",
        "{ \"minimum\": {"
        "    \"errorCode\": 4,"
        "    \"instanceRef\": \"#\", \"schemaRef\": \"#\","
        "    \"expected\": -9223372036854775807, \"actual\": -9223372036854775808"
        "}}");
    VALIDATE(s, "-9223372036854775807", true);
    VALIDATE(s, "-2147483648", true); // int min
    VALIDATE(s, "0", true);
    VALIDATE(s, "2147483647", true);  // int max
    VALIDATE(s, "2147483648", true);  // unsigned first
    VALIDATE(s, "4294967295", true);  // unsigned max
    VALIDATE(s, "9223372036854775806", true);
    INVALIDATE(s, "9223372036854775807", "", "maximum", "",
        "{ \"maximum\": {"
        "    \"errorCode\": 2,"
        "    \"instanceRef\": \"#\", \"schemaRef\": \"#\","
        "    \"expected\": 9223372036854775806, \"actual\": 9223372036854775807"
        "}}");
    INVALIDATE(s, "18446744073709551615", "", "maximum", "",
        "{ \"maximum\": {"
        "    \"errorCode\": 2,"
        "    \"instanceRef\": \"#\", \"schemaRef\": \"#\","
        "    \"expected\": 9223372036854775806, \"actual\": 18446744073709551615"
        "}}");   // uint64_t max
}

TEST(SchemaValidator, Integer_RangeU64Boundary) {
    Document sd;
    sd.Parse("{\"type\":\"integer\",\"minimum\":9223372036854775808,\"maximum\":18446744073709551614}");
    SchemaDocument s(sd);

    INVALIDATE(s, "-9223372036854775808", "", "minimum", "",
        "{ \"minimum\": {"
        "    \"errorCode\": 4,"
        "    \"instanceRef\": \"#\", \"schemaRef\": \"#\","
        "    \"expected\": 9223372036854775808, \"actual\": -9223372036854775808"
        "}}");
    INVALIDATE(s, "9223372036854775807", "", "minimum", "",
        "{ \"minimum\": {"
        "    \"errorCode\": 4,"
        "    \"instanceRef\": \"#\", \"schemaRef\": \"#\","
        "    \"expected\": 9223372036854775808, \"actual\": 9223372036854775807"
        "}}");
    INVALIDATE(s, "-2147483648", "", "minimum", "",
        "{ \"minimum\": {"
        "    \"errorCode\": 4,"
        "    \"instanceRef\": \"#\", \"schemaRef\": \"#\","
        "    \"expected\": 9223372036854775808, \"actual\": -2147483648"
        "}}"); // int min
    INVALIDATE(s, "0", "", "minimum", "",
        "{ \"minimum\": {"
        "    \"errorCode\": 4,"
        "    \"instanceRef\": \"#\", \"schemaRef\": \"#\","
        "    \"expected\": 9223372036854775808, \"actual\": 0"
        "}}");
    INVALIDATE(s, "2147483647", "", "minimum", "",
        "{ \"minimum\": {"
        "    \"errorCode\": 4,"
        "    \"instanceRef\": \"#\", \"schemaRef\": \"#\","
        "    \"expected\": 9223372036854775808, \"actual\": 2147483647"
        "}}");  // int max
    INVALIDATE(s, "2147483648", "", "minimum", "",
        "{ \"minimum\": {"
        "    \"errorCode\": 4,"
        "    \"instanceRef\": \"#\", \"schemaRef\": \"#\","
        "    \"expected\": 9223372036854775808, \"actual\": 2147483648"
        "}}");  // unsigned first
    INVALIDATE(s, "4294967295", "", "minimum", "",
        "{ \"minimum\": {"
        "    \"errorCode\": 4,"
        "    \"instanceRef\": \"#\", \"schemaRef\": \"#\","
        "    \"expected\": 9223372036854775808, \"actual\": 4294967295"
        "}}");  // unsigned max
    VALIDATE(s, "9223372036854775808", true);
    VALIDATE(s, "18446744073709551614", true);
    INVALIDATE(s, "18446744073709551615", "", "maximum", "",
        "{ \"maximum\": {"
        "    \"errorCode\": 2,"
        "    \"instanceRef\": \"#\", \"schemaRef\": \"#\","
        "    \"expected\": 18446744073709551614, \"actual\": 18446744073709551615"
        "}}");
}

TEST(SchemaValidator, Integer_Range64BoundaryExclusive) {
    Document sd;
    sd.Parse("{\"type\":\"integer\",\"minimum\":-9223372036854775808,\"maximum\":18446744073709551615,\"exclusiveMinimum\":true,\"exclusiveMaximum\":true}");
    SchemaDocument s(sd);

    INVALIDATE(s, "-9223372036854775808", "", "minimum", "",
        "{ \"minimum\": {"
        "    \"errorCode\": 5,"
        "    \"instanceRef\": \"#\", \"schemaRef\": \"#\","
        "    \"expected\": -9223372036854775808, \"exclusiveMinimum\": true, "
        "    \"actual\": -9223372036854775808"
        "}}");
    VALIDATE(s, "-9223372036854775807", true);
    VALIDATE(s, "18446744073709551614", true);
    INVALIDATE(s, "18446744073709551615", "", "maximum", "",
        "{ \"maximum\": {"
        "    \"errorCode\": 3,"
        "    \"instanceRef\": \"#\", \"schemaRef\": \"#\","
        "    \"expected\": 18446744073709551615, \"exclusiveMaximum\": true, "
        "    \"actual\": 18446744073709551615"
        "}}");
}

TEST(SchemaValidator, Integer_MultipleOf) {
    Document sd;
    sd.Parse("{\"type\":\"integer\",\"multipleOf\":10}");
    SchemaDocument s(sd);

    VALIDATE(s, "0", true);
    VALIDATE(s, "10", true);
    VALIDATE(s, "-10", true);
    VALIDATE(s, "20", true);
    INVALIDATE(s, "23", "", "multipleOf", "",
        "{ \"multipleOf\": {"
        "    \"errorCode\": 1,"
        "    \"instanceRef\": \"#\", \"schemaRef\": \"#\","
        "    \"expected\": 10, \"actual\": 23"
        "}}");
    INVALIDATE(s, "-23", "", "multipleOf", "",
        "{ \"multipleOf\": {"
        "    \"errorCode\": 1,"
        "    \"instanceRef\": \"#\", \"schemaRef\": \"#\","
        "    \"expected\": 10, \"actual\": -23"
        "}}");
}

TEST(SchemaValidator, Integer_MultipleOf64Boundary) {
    Document sd;
    sd.Parse("{\"type\":\"integer\",\"multipleOf\":18446744073709551615}");
    SchemaDocument s(sd);

    VALIDATE(s, "0", true);
    VALIDATE(s, "18446744073709551615", true);
    INVALIDATE(s, "18446744073709551614", "", "multipleOf", "",
        "{ \"multipleOf\": {"
        "    \"errorCode\": 1,"
        "    \"instanceRef\": \"#\", \"schemaRef\": \"#\","
        "    \"expected\": 18446744073709551615, \"actual\": 18446744073709551614"
        "}}");
}

TEST(SchemaValidator, Number_Range) {
    Document sd;
    sd.Parse("{\"type\":\"number\",\"minimum\":0,\"maximum\":100,\"exclusiveMaximum\":true}");
    SchemaDocument s(sd);

    INVALIDATE(s, "-1", "", "minimum", "",
        "{ \"minimum\": {"
        "    \"errorCode\": 4,"
        "    \"instanceRef\": \"#\", \"schemaRef\": \"#\","
        "    \"expected\": 0, \"actual\": -1"
        "}}");
    VALIDATE(s, "0", true);
    VALIDATE(s, "0.1", true);
    VALIDATE(s, "10", true);
    VALIDATE(s, "99", true);
    VALIDATE(s, "99.9", true);
    INVALIDATE(s, "100", "", "maximum", "",
        "{ \"maximum\": {"
        "    \"errorCode\": 3,"
        "    \"instanceRef\": \"#\", \"schemaRef\": \"#\","
        "    \"expected\": 100, \"exclusiveMaximum\": true, \"actual\": 100"
        "}}");
    INVALIDATE(s, "100.0", "", "maximum", "",
        "{ \"maximum\": {"
        "    \"errorCode\": 3,"
        "    \"instanceRef\": \"#\", \"schemaRef\": \"#\","
        "    \"expected\": 100, \"exclusiveMaximum\": true, \"actual\": 100.0"
        "}}");
    INVALIDATE(s, "101.5", "", "maximum", "",
        "{ \"maximum\": {"
        "    \"errorCode\": 3,"
        "    \"instanceRef\": \"#\", \"schemaRef\": \"#\","
        "    \"expected\": 100, \"exclusiveMaximum\": true, \"actual\": 101.5"
        "}}");
}

TEST(SchemaValidator, Number_RangeInt) {
    Document sd;
    sd.Parse("{\"type\":\"number\",\"minimum\":-100,\"maximum\":-1,\"exclusiveMaximum\":true}");
    SchemaDocument s(sd);

    INVALIDATE(s, "-101", "", "minimum", "",
        "{ \"minimum\": {"
        "    \"errorCode\": 4,"
        "    \"instanceRef\": \"#\", \"schemaRef\": \"#\","
        "    \"expected\": -100, \"actual\": -101"
        "}}");
    INVALIDATE(s, "-100.1", "", "minimum", "",
        "{ \"minimum\": {"
        "    \"errorCode\": 4,"
        "    \"instanceRef\": \"#\", \"schemaRef\": \"#\","
        "    \"expected\": -100, \"actual\": -100.1"
        "}}");
    VALIDATE(s, "-100", true);
    VALIDATE(s, "-2", true);
    INVALIDATE(s, "-1", "", "maximum", "",
        "{ \"maximum\": {"
        "    \"errorCode\": 3,"
        "    \"instanceRef\": \"#\", \"schemaRef\": \"#\","
        "    \"expected\": -1, \"exclusiveMaximum\": true, \"actual\": -1"
        "}}");
    INVALIDATE(s, "-0.9", "", "maximum", "",
        "{ \"maximum\": {"
        "    \"errorCode\": 3,"
        "    \"instanceRef\": \"#\", \"schemaRef\": \"#\","
        "    \"expected\": -1, \"exclusiveMaximum\": true, \"actual\": -0.9"
        "}}");
    INVALIDATE(s, "0", "", "maximum", "",
        "{ \"maximum\": {"
        "    \"errorCode\": 3,"
        "    \"instanceRef\": \"#\", \"schemaRef\": \"#\","
        "    \"expected\": -1, \"exclusiveMaximum\": true, \"actual\": 0"
        "}}");
    INVALIDATE(s, "2147483647", "", "maximum", "",
        "{ \"maximum\": {"
        "    \"errorCode\": 3,"
        "    \"instanceRef\": \"#\", \"schemaRef\": \"#\","
        "    \"expected\": -1, \"exclusiveMaximum\": true, \"actual\": 2147483647"
        "}}");  // int max
    INVALIDATE(s, "2147483648", "", "maximum", "",
        "{ \"maximum\": {"
        "    \"errorCode\": 3,"
        "    \"instanceRef\": \"#\", \"schemaRef\": \"#\","
        "    \"expected\": -1, \"exclusiveMaximum\": true, \"actual\": 2147483648"
        "}}");  // unsigned first
    INVALIDATE(s, "4294967295", "", "maximum", "",
        "{ \"maximum\": {"
        "    \"errorCode\": 3,"
        "    \"instanceRef\": \"#\", \"schemaRef\": \"#\","
        "    \"expected\": -1, \"exclusiveMaximum\": true, \"actual\": 4294967295"
        "}}");  // unsigned max
    INVALIDATE(s, "9223372036854775808", "", "maximum", "",
        "{ \"maximum\": {"
        "    \"errorCode\": 3,"
        "    \"instanceRef\": \"#\", \"schemaRef\": \"#\","
        "    \"expected\": -1, \"exclusiveMaximum\": true, \"actual\": 9223372036854775808"
        "}}");
    INVALIDATE(s, "18446744073709551614", "", "maximum", "",
        "{ \"maximum\": {"
        "    \"errorCode\": 3,"
        "    \"instanceRef\": \"#\", \"schemaRef\": \"#\","
        "    \"expected\": -1, \"exclusiveMaximum\": true, \"actual\": 18446744073709551614"
        "}}");
    INVALIDATE(s, "18446744073709551615", "", "maximum", "",
        "{ \"maximum\": {"
        "    \"errorCode\": 3,"
        "    \"instanceRef\": \"#\", \"schemaRef\": \"#\","
        "    \"expected\": -1, \"exclusiveMaximum\": true, \"actual\": 18446744073709551615"
        "}}");
}

TEST(SchemaValidator, Number_RangeDouble) {
    Document sd;
    sd.Parse("{\"type\":\"number\",\"minimum\":0.1,\"maximum\":100.1,\"exclusiveMaximum\":true}");
    SchemaDocument s(sd);

    INVALIDATE(s, "-9223372036854775808", "", "minimum", "",
        "{ \"minimum\": {"
        "    \"errorCode\": 4,"
        "    \"instanceRef\": \"#\", \"schemaRef\": \"#\","
        "    \"expected\": 0.1, \"actual\": -9223372036854775808"
        "}}");
    INVALIDATE(s, "-2147483648", "", "minimum", "",
        "{ \"minimum\": {"
        "    \"errorCode\": 4,"
        "    \"instanceRef\": \"#\", \"schemaRef\": \"#\","
        "    \"expected\": 0.1, \"actual\": -2147483648"
        "}}"); // int min
    INVALIDATE(s, "-1", "", "minimum", "",
        "{ \"minimum\": {"
        "    \"errorCode\": 4,"
        "    \"instanceRef\": \"#\", \"schemaRef\": \"#\","
        "    \"expected\": 0.1, \"actual\": -1"
        "}}");
    VALIDATE(s, "0.1", true);
    VALIDATE(s, "10", true);
    VALIDATE(s, "99", true);
    VALIDATE(s, "100", true);
    INVALIDATE(s, "101", "", "maximum", "",
        "{ \"maximum\": {"
        "    \"errorCode\": 3,"
        "    \"instanceRef\": \"#\", \"schemaRef\": \"#\","
        "    \"expected\": 100.1, \"exclusiveMaximum\": true, \"actual\": 101"
        "}}");
    INVALIDATE(s, "101.5", "", "maximum", "",
        "{ \"maximum\": {"
        "    \"errorCode\": 3,"
        "    \"instanceRef\": \"#\", \"schemaRef\": \"#\","
        "    \"expected\": 100.1, \"exclusiveMaximum\": true, \"actual\": 101.5"
        "}}");
    INVALIDATE(s, "18446744073709551614", "", "maximum", "",
        "{ \"maximum\": {"
        "    \"errorCode\": 3,"
        "    \"instanceRef\": \"#\", \"schemaRef\": \"#\","
        "    \"expected\": 100.1, \"exclusiveMaximum\": true, \"actual\": 18446744073709551614"
        "}}");
    INVALIDATE(s, "18446744073709551615", "", "maximum", "",
        "{ \"maximum\": {"
        "    \"errorCode\": 3,"
        "    \"instanceRef\": \"#\", \"schemaRef\": \"#\","
        "    \"expected\": 100.1, \"exclusiveMaximum\": true, \"actual\": 18446744073709551615"
        "}}");
    INVALIDATE(s, "2147483647", "", "maximum", "",
        "{ \"maximum\": {"
        "    \"errorCode\": 3,"
        "    \"instanceRef\": \"#\", \"schemaRef\": \"#\","
        "    \"expected\": 100.1, \"exclusiveMaximum\": true, \"actual\": 2147483647"
        "}}");  // int max
    INVALIDATE(s, "2147483648", "", "maximum", "",
        "{ \"maximum\": {"
        "    \"errorCode\": 3,"
        "    \"instanceRef\": \"#\", \"schemaRef\": \"#\","
        "    \"expected\": 100.1, \"exclusiveMaximum\": true, \"actual\": 2147483648"
        "}}");  // unsigned first
    INVALIDATE(s, "4294967295", "", "maximum", "",
        "{ \"maximum\": {"
        "    \"errorCode\": 3,"
        "    \"instanceRef\": \"#\", \"schemaRef\": \"#\","
        "    \"expected\": 100.1, \"exclusiveMaximum\": true, \"actual\": 4294967295"
        "}}");  // unsigned max
    INVALIDATE(s, "9223372036854775808", "", "maximum", "",
        "{ \"maximum\": {"
        "    \"errorCode\": 3,"
        "    \"instanceRef\": \"#\", \"schemaRef\": \"#\","
        "    \"expected\": 100.1, \"exclusiveMaximum\": true, \"actual\": 9223372036854775808"
        "}}");
    INVALIDATE(s, "18446744073709551614", "", "maximum", "",
        "{ \"maximum\": {"
        "    \"errorCode\": 3,"
        "    \"instanceRef\": \"#\", \"schemaRef\": \"#\","
        "    \"expected\": 100.1, \"exclusiveMaximum\": true, \"actual\": 18446744073709551614"
        "}}");
    INVALIDATE(s, "18446744073709551615", "", "maximum", "",
        "{ \"maximum\": {"
        "    \"errorCode\": 3,"
        "    \"instanceRef\": \"#\", \"schemaRef\": \"#\","
        "    \"expected\": 100.1, \"exclusiveMaximum\": true, \"actual\": 18446744073709551615"
        "}}");
}

TEST(SchemaValidator, Number_RangeDoubleU64Boundary) {
    Document sd;
    sd.Parse("{\"type\":\"number\",\"minimum\":9223372036854775808.0,\"maximum\":18446744073709550000.0}");
    SchemaDocument s(sd);

    INVALIDATE(s, "-9223372036854775808", "", "minimum", "",
        "{ \"minimum\": {"
        "    \"errorCode\": 4,"
        "    \"instanceRef\": \"#\", \"schemaRef\": \"#\","
        "    \"expected\": 9223372036854775808.0, \"actual\": -9223372036854775808"
        "}}");
    INVALIDATE(s, "-2147483648", "", "minimum", "",
        "{ \"minimum\": {"
        "    \"errorCode\": 4,"
        "    \"instanceRef\": \"#\", \"schemaRef\": \"#\","
        "    \"expected\": 9223372036854775808.0, \"actual\": -2147483648"
        "}}"); // int min
    INVALIDATE(s, "0", "", "minimum", "",
        "{ \"minimum\": {"
        "    \"errorCode\": 4,"
        "    \"instanceRef\": \"#\", \"schemaRef\": \"#\","
        "    \"expected\": 9223372036854775808.0, \"actual\": 0"
        "}}");
    INVALIDATE(s, "2147483647", "", "minimum", "",
        "{ \"minimum\": {"
        "    \"errorCode\": 4,"
        "    \"instanceRef\": \"#\", \"schemaRef\": \"#\","
        "    \"expected\": 9223372036854775808.0, \"actual\": 2147483647"
        "}}");  // int max
    INVALIDATE(s, "2147483648", "", "minimum", "",
        "{ \"minimum\": {"
        "    \"errorCode\": 4,"
        "    \"instanceRef\": \"#\", \"schemaRef\": \"#\","
        "    \"expected\": 9223372036854775808.0, \"actual\": 2147483648"
        "}}");  // unsigned first
    INVALIDATE(s, "4294967295", "", "minimum", "",
        "{ \"minimum\": {"
        "    \"errorCode\": 4,"
        "    \"instanceRef\": \"#\", \"schemaRef\": \"#\","
        "    \"expected\": 9223372036854775808.0, \"actual\": 4294967295"
        "}}");  // unsigned max
    VALIDATE(s, "9223372036854775808", true);
    VALIDATE(s, "18446744073709540000", true);
    INVALIDATE(s, "18446744073709551615", "", "maximum", "",
        "{ \"maximum\": {"
        "    \"errorCode\": 2,"
        "    \"instanceRef\": \"#\", \"schemaRef\": \"#\","
        "    \"expected\": 18446744073709550000.0, \"actual\": 18446744073709551615"
        "}}");
}

TEST(SchemaValidator, Number_MultipleOf) {
    Document sd;
    sd.Parse("{\"type\":\"number\",\"multipleOf\":10.0}");
    SchemaDocument s(sd);

    VALIDATE(s, "0", true);
    VALIDATE(s, "10", true);
    VALIDATE(s, "-10", true);
    VALIDATE(s, "20", true);
    INVALIDATE(s, "23", "", "multipleOf", "",
        "{ \"multipleOf\": {"
        "    \"errorCode\": 1,"
        "    \"instanceRef\": \"#\", \"schemaRef\": \"#\","
        "    \"expected\": 10.0, \"actual\": 23"
        "}}");
    INVALIDATE(s, "-2147483648", "", "multipleOf", "",
        "{ \"multipleOf\": {"
        "    \"errorCode\": 1,"
        "    \"instanceRef\": \"#\", \"schemaRef\": \"#\","
        "    \"expected\": 10.0, \"actual\": -2147483648"
        "}}");  // int min
    VALIDATE(s, "-2147483640", true);
    INVALIDATE(s, "2147483647", "", "multipleOf", "",
        "{ \"multipleOf\": {"
        "    \"errorCode\": 1,"
        "    \"instanceRef\": \"#\", \"schemaRef\": \"#\","
        "    \"expected\": 10.0, \"actual\": 2147483647"
        "}}");  // int max
    INVALIDATE(s, "2147483648", "", "multipleOf", "",
        "{ \"multipleOf\": {"
        "    \"errorCode\": 1,"
        "    \"instanceRef\": \"#\", \"schemaRef\": \"#\","
        "    \"expected\": 10.0, \"actual\": 2147483648"
        "}}");  // unsigned first
    VALIDATE(s, "2147483650", true);
    INVALIDATE(s, "4294967295", "", "multipleOf", "",
        "{ \"multipleOf\": {"
        "    \"errorCode\": 1,"
        "    \"instanceRef\": \"#\", \"schemaRef\": \"#\","
        "    \"expected\": 10.0, \"actual\": 4294967295"
        "}}");  // unsigned max
    VALIDATE(s, "4294967300", true);
}

TEST(SchemaValidator, Number_MultipleOfOne) {
    Document sd;
    sd.Parse("{\"type\":\"number\",\"multipleOf\":1}");
    SchemaDocument s(sd);

    VALIDATE(s, "42", true);
    VALIDATE(s, "42.0", true);
    INVALIDATE(s, "3.1415926", "", "multipleOf", "",
        "{ \"multipleOf\": {"
        "    \"errorCode\": 1,"
        "    \"instanceRef\": \"#\", \"schemaRef\": \"#\","
        "    \"expected\": 1, \"actual\": 3.1415926"
        "}}");
}

TEST(SchemaValidator, Object) {
    Document sd;
    sd.Parse("{\"type\":\"object\"}");
    SchemaDocument s(sd);

    VALIDATE(s, "{\"key\":\"value\",\"another_key\":\"another_value\"}", true);
    VALIDATE(s, "{\"Sun\":1.9891e30,\"Jupiter\":1.8986e27,\"Saturn\":5.6846e26,\"Neptune\":10.243e25,\"Uranus\":8.6810e25,\"Earth\":5.9736e24,\"Venus\":4.8685e24,\"Mars\":6.4185e23,\"Mercury\":3.3022e23,\"Moon\":7.349e22,\"Pluto\":1.25e22}", true);    
    INVALIDATE(s, "[\"An\", \"array\", \"not\", \"an\", \"object\"]", "", "type", "",
        "{ \"type\": {"
        "    \"errorCode\": 20,"
        "    \"instanceRef\": \"#\", \"schemaRef\": \"#\","
        "    \"expected\": [\"object\"], \"actual\": \"array\""
        "}}");
    INVALIDATE(s, "\"Not an object\"", "", "type", "",
        "{ \"type\": {"
        "    \"errorCode\": 20,"
        "    \"instanceRef\": \"#\", \"schemaRef\": \"#\","
        "    \"expected\": [\"object\"], \"actual\": \"string\""
        "}}");
}

TEST(SchemaValidator, Object_Properties) {
    Document sd;
    sd.Parse(
        "{"
        "    \"type\": \"object\","
        "    \"properties\" : {"
        "        \"number\": { \"type\": \"number\" },"
        "        \"street_name\" : { \"type\": \"string\" },"
        "        \"street_type\" : { \"type\": \"string\", \"enum\" : [\"Street\", \"Avenue\", \"Boulevard\"] }"
        "    }"
        "}");

    SchemaDocument s(sd);

    VALIDATE(s, "{ \"number\": 1600, \"street_name\": \"Pennsylvania\", \"street_type\": \"Avenue\" }", true);
    INVALIDATE(s, "{ \"number\": \"1600\", \"street_name\": \"Pennsylvania\", \"street_type\": \"Avenue\" }", "/properties/number", "type", "/number",
        "{ \"type\": {"
        "    \"errorCode\": 20,"
        "    \"instanceRef\": \"#/number\", \"schemaRef\": \"#/properties/number\","
        "    \"expected\": [\"number\"], \"actual\": \"string\""
        "}}");
    INVALIDATE(s, "{ \"number\": \"One\", \"street_name\": \"Microsoft\", \"street_type\": \"Way\" }",
        "/properties/number", "type", "/number",
        "{ \"type\": {"
        "    \"errorCode\": 20,"
        "    \"instanceRef\": \"#/number\", \"schemaRef\": \"#/properties/number\","
        "    \"expected\": [\"number\"], \"actual\": \"string\""
        "}}"); // fail fast
    VALIDATE(s, "{ \"number\": 1600, \"street_name\": \"Pennsylvania\" }", true);
    VALIDATE(s, "{}", true);
    VALIDATE(s, "{ \"number\": 1600, \"street_name\": \"Pennsylvania\", \"street_type\": \"Avenue\", \"direction\": \"NW\" }", true);
}

TEST(SchemaValidator, Object_AdditionalPropertiesBoolean) {
    Document sd;
    sd.Parse(
        "{"
        "    \"type\": \"object\","
        "        \"properties\" : {"
        "        \"number\": { \"type\": \"number\" },"
        "            \"street_name\" : { \"type\": \"string\" },"
        "            \"street_type\" : { \"type\": \"string\","
        "            \"enum\" : [\"Street\", \"Avenue\", \"Boulevard\"]"
        "        }"
        "    },"
        "    \"additionalProperties\": false"
        "}");

    SchemaDocument s(sd);

    VALIDATE(s, "{ \"number\": 1600, \"street_name\": \"Pennsylvania\", \"street_type\": \"Avenue\" }", true);
    INVALIDATE(s, "{ \"number\": 1600, \"street_name\": \"Pennsylvania\", \"street_type\": \"Avenue\", \"direction\": \"NW\" }", "", "additionalProperties", "/direction",
        "{ \"additionalProperties\": {"
        "    \"errorCode\": 16,"
        "    \"instanceRef\": \"#\", \"schemaRef\": \"#\","
        "    \"disallowed\": \"direction\""
        "}}");
}

TEST(SchemaValidator, Object_AdditionalPropertiesObject) {
    Document sd;
    sd.Parse(
        "{"
        "    \"type\": \"object\","
        "    \"properties\" : {"
        "        \"number\": { \"type\": \"number\" },"
        "        \"street_name\" : { \"type\": \"string\" },"
        "        \"street_type\" : { \"type\": \"string\","
        "            \"enum\" : [\"Street\", \"Avenue\", \"Boulevard\"]"
        "        }"
        "    },"
        "    \"additionalProperties\": { \"type\": \"string\" }"
        "}");
    SchemaDocument s(sd);

    VALIDATE(s, "{ \"number\": 1600, \"street_name\": \"Pennsylvania\", \"street_type\": \"Avenue\" }", true);
    VALIDATE(s, "{ \"number\": 1600, \"street_name\": \"Pennsylvania\", \"street_type\": \"Avenue\", \"direction\": \"NW\" }", true);
    INVALIDATE(s, "{ \"number\": 1600, \"street_name\": \"Pennsylvania\", \"street_type\": \"Avenue\", \"office_number\": 201 }", "/additionalProperties", "type", "/office_number",
        "{ \"type\": {"
        "    \"errorCode\": 20,"
        "    \"instanceRef\": \"#/office_number\", \"schemaRef\": \"#/additionalProperties\","
        "    \"expected\": [\"string\"], \"actual\": \"integer\""
        "}}");
}

TEST(SchemaValidator, Object_Required) {
    Document sd;
    sd.Parse(
        "{"
        "    \"type\": \"object\","
        "    \"properties\" : {"
        "        \"name\":      { \"type\": \"string\" },"
        "        \"email\" : { \"type\": \"string\" },"
        "        \"address\" : { \"type\": \"string\" },"
        "        \"telephone\" : { \"type\": \"string\" }"
        "    },"
        "    \"required\":[\"name\", \"email\"]"
        "}");
    SchemaDocument s(sd);

    VALIDATE(s, "{ \"name\": \"William Shakespeare\", \"email\" : \"bill@stratford-upon-avon.co.uk\" }", true);
    VALIDATE(s, "{ \"name\": \"William Shakespeare\", \"email\" : \"bill@stratford-upon-avon.co.uk\", \"address\" : \"Henley Street, Stratford-upon-Avon, Warwickshire, England\", \"authorship\" : \"in question\"}", true);
    INVALIDATE(s, "{ \"name\": \"William Shakespeare\", \"address\" : \"Henley Street, Stratford-upon-Avon, Warwickshire, England\" }", "", "required", "",
        "{ \"required\": {"
        "    \"errorCode\": 15,"
        "    \"instanceRef\": \"#\", \"schemaRef\": \"#\","
        "    \"missing\": [\"email\"]"
        "}}");
    INVALIDATE(s, "{}", "", "required", "",
        "{ \"required\": {"
        "    \"errorCode\": 15,"
        "    \"instanceRef\": \"#\", \"schemaRef\": \"#\","
        "    \"missing\": [\"name\", \"email\"]"
        "}}");
}

TEST(SchemaValidator, Object_Required_PassWithDefault) {
    Document sd;
    sd.Parse(
        "{"
        "    \"type\": \"object\","
        "    \"properties\" : {"
        "        \"name\":      { \"type\": \"string\", \"default\": \"William Shakespeare\" },"
        "        \"email\" : { \"type\": \"string\", \"default\": \"\" },"
        "        \"address\" : { \"type\": \"string\" },"
        "        \"telephone\" : { \"type\": \"string\" }"
        "    },"
        "    \"required\":[\"name\", \"email\"]"
        "}");
    SchemaDocument s(sd);

    VALIDATE(s, "{ \"email\" : \"bill@stratford-upon-avon.co.uk\", \"address\" : \"Henley Street, Stratford-upon-Avon, Warwickshire, England\", \"authorship\" : \"in question\"}", true);
    INVALIDATE(s, "{ \"name\": \"William Shakespeare\", \"address\" : \"Henley Street, Stratford-upon-Avon, Warwickshire, England\" }", "", "required", "",
        "{ \"required\": {"
        "    \"errorCode\": 15,"
        "    \"instanceRef\": \"#\", \"schemaRef\": \"#\","
        "    \"missing\": [\"email\"]"
        "}}");
    INVALIDATE(s, "{}", "", "required", "",
        "{ \"required\": {"
        "    \"errorCode\": 15,"
        "    \"instanceRef\": \"#\", \"schemaRef\": \"#\","
        "    \"missing\": [\"email\"]"
        "}}");
}

TEST(SchemaValidator, Object_PropertiesRange) {
    Document sd;
    sd.Parse("{\"type\":\"object\", \"minProperties\":2, \"maxProperties\":3}");
    SchemaDocument s(sd);

    INVALIDATE(s, "{}", "", "minProperties", "",
        "{ \"minProperties\": {"
        "    \"errorCode\": 14,"
        "    \"instanceRef\": \"#\", \"schemaRef\": \"#\","
        "    \"expected\": 2, \"actual\": 0"
        "}}");
    INVALIDATE(s, "{\"a\":0}", "", "minProperties", "",
        "{ \"minProperties\": {"
        "    \"errorCode\": 14,"
        "    \"instanceRef\": \"#\", \"schemaRef\": \"#\","
        "    \"expected\": 2, \"actual\": 1"
        "}}");
    VALIDATE(s, "{\"a\":0,\"b\":1}", true);
    VALIDATE(s, "{\"a\":0,\"b\":1,\"c\":2}", true);
    INVALIDATE(s, "{\"a\":0,\"b\":1,\"c\":2,\"d\":3}", "", "maxProperties", "",
        "{ \"maxProperties\": {"
        "    \"errorCode\": 13,"
        "    \"instanceRef\": \"#\", \"schemaRef\": \"#\", "
        "    \"expected\": 3, \"actual\": 4"
        "}}");
}

TEST(SchemaValidator, Object_PropertyDependencies) {
    Document sd;
    sd.Parse(
        "{"
        "  \"type\": \"object\","
        "  \"properties\": {"
        "    \"name\": { \"type\": \"string\" },"
        "    \"credit_card\": { \"type\": \"number\" },"
        "    \"cvv_code\": { \"type\": \"number\" },"
        "    \"billing_address\": { \"type\": \"string\" }"
        "  },"
        "  \"required\": [\"name\"],"
        "  \"dependencies\": {"
        "    \"credit_card\": [\"cvv_code\", \"billing_address\"]"
        "  }"
        "}");
    SchemaDocument s(sd);

    VALIDATE(s, "{ \"name\": \"John Doe\", \"credit_card\": 5555555555555555, \"cvv_code\": 777, "
        "\"billing_address\": \"555 Debtor's Lane\" }", true);
    INVALIDATE(s, "{ \"name\": \"John Doe\", \"credit_card\": 5555555555555555 }", "", "dependencies", "",
        "{ \"dependencies\": {"
        "    \"errorCode\": 18,"
        "    \"instanceRef\": \"#\", \"schemaRef\": \"#\","
        "    \"errors\": {"
        "       \"credit_card\": {"
        "        \"required\": {"
        "          \"errorCode\": 15,"
        "          \"instanceRef\": \"#\", \"schemaRef\": \"#/dependencies/credit_card\","
        "          \"missing\": [\"cvv_code\", \"billing_address\"]"
        "    } } }"
        "}}");
    VALIDATE(s, "{ \"name\": \"John Doe\"}", true);
    VALIDATE(s, "{ \"name\": \"John Doe\", \"cvv_code\": 777, \"billing_address\": \"555 Debtor's Lane\" }", true);
}

TEST(SchemaValidator, Object_SchemaDependencies) {
    Document sd;
    sd.Parse(
        "{"
        "    \"type\": \"object\","
        "    \"properties\" : {"
        "        \"name\": { \"type\": \"string\" },"
        "        \"credit_card\" : { \"type\": \"number\" }"
        "    },"
        "    \"required\" : [\"name\"],"
        "    \"dependencies\" : {"
        "        \"credit_card\": {"
        "            \"properties\": {"
        "                \"billing_address\": { \"type\": \"string\" }"
        "            },"
        "            \"required\" : [\"billing_address\"]"
        "        }"
        "    }"
        "}");
    SchemaDocument s(sd);

    VALIDATE(s, "{\"name\": \"John Doe\", \"credit_card\" : 5555555555555555,\"billing_address\" : \"555 Debtor's Lane\"}", true);
    INVALIDATE(s, "{\"name\": \"John Doe\", \"credit_card\" : 5555555555555555 }", "", "dependencies", "",
        "{ \"dependencies\": {"
        "    \"errorCode\": 18,"
        "    \"instanceRef\": \"#\", \"schemaRef\": \"#\","
        "    \"errors\": {"
        "      \"credit_card\": {"
        "        \"required\": {"
        "          \"errorCode\": 15,"
        "          \"instanceRef\": \"#\", \"schemaRef\": \"#/dependencies/credit_card\","
        "          \"missing\": [\"billing_address\"]"
        "    } } }"
        "}}");
    VALIDATE(s, "{\"name\": \"John Doe\", \"billing_address\" : \"555 Debtor's Lane\"}", true);
}

#if RAPIDJSON_SCHEMA_HAS_REGEX
TEST(SchemaValidator, Object_PatternProperties) {
    Document sd;
    sd.Parse(
        "{"
        "  \"type\": \"object\","
        "  \"patternProperties\": {"
        "    \"^S_\": { \"type\": \"string\" },"
        "    \"^I_\": { \"type\": \"integer\" }"
        "  }"
        "}");
    SchemaDocument s(sd);

    VALIDATE(s, "{ \"S_25\": \"This is a string\" }", true);
    VALIDATE(s, "{ \"I_0\": 42 }", true);
    INVALIDATE(s, "{ \"S_0\": 42 }", "", "patternProperties", "/S_0",
        "{ \"type\": {"
        "    \"errorCode\": 20,"
        "    \"instanceRef\": \"#/S_0\", \"schemaRef\": \"#/patternProperties/%5ES_\","
        "    \"expected\": [\"string\"], \"actual\": \"integer\""
        "}}");
    INVALIDATE(s, "{ \"I_42\": \"This is a string\" }", "", "patternProperties", "/I_42",
        "{ \"type\": {"
        "    \"errorCode\": 20,"
        "    \"instanceRef\": \"#/I_42\", \"schemaRef\": \"#/patternProperties/%5EI_\","
        "    \"expected\": [\"integer\"], \"actual\": \"string\""
        "}}");
    VALIDATE(s, "{ \"keyword\": \"value\" }", true);
}

TEST(SchemaValidator, Object_PatternProperties_ErrorConflict) {
    Document sd;
    sd.Parse(
        "{"
        "  \"type\": \"object\","
        "  \"patternProperties\": {"
        "    \"^I_\": { \"multipleOf\": 5 },"
        "    \"30$\": { \"multipleOf\": 6 }"
        "  }"
        "}");
    SchemaDocument s(sd);

    VALIDATE(s, "{ \"I_30\": 30 }", true);
    INVALIDATE(s, "{ \"I_30\": 7 }", "", "patternProperties", "/I_30",
        "{ \"multipleOf\": ["
        "    {"
        "      \"errorCode\": 1,"
        "      \"instanceRef\": \"#/I_30\", \"schemaRef\": \"#/patternProperties/%5EI_\","
        "      \"expected\": 5, \"actual\": 7"
        "    }, {"
        "      \"errorCode\": 1,"
        "      \"instanceRef\": \"#/I_30\", \"schemaRef\": \"#/patternProperties/30%24\","
        "      \"expected\": 6, \"actual\": 7"
        "    }"
        "]}");
}

TEST(SchemaValidator, Object_Properties_PatternProperties) {
    Document sd;
    sd.Parse(
        "{"
        "  \"type\": \"object\","
        "  \"properties\": {"
        "    \"I_42\": { \"type\": \"integer\", \"minimum\": 73 }"
        "  },"
        "  \"patternProperties\": {"
        "    \"^I_\": { \"type\": \"integer\", \"multipleOf\": 6 }"
        "  }"
        "}");
    SchemaDocument s(sd);

    VALIDATE(s, "{ \"I_6\": 6 }", true);
    VALIDATE(s, "{ \"I_42\": 78 }", true);
    INVALIDATE(s, "{ \"I_42\": 42 }", "", "patternProperties", "/I_42",
        "{ \"minimum\": {"
        "    \"errorCode\": 4,"
        "    \"instanceRef\": \"#/I_42\", \"schemaRef\": \"#/properties/I_42\","
        "    \"expected\": 73, \"actual\": 42"
        "}}");
    INVALIDATE(s, "{ \"I_42\": 7 }", "", "patternProperties", "/I_42",
        "{ \"minimum\": {"
        "    \"errorCode\": 4,"
        "    \"instanceRef\": \"#/I_42\", \"schemaRef\": \"#/properties/I_42\","
        "    \"expected\": 73, \"actual\": 7"
        "  },"
        "  \"multipleOf\": {"
        "    \"errorCode\": 1,"
        "    \"instanceRef\": \"#/I_42\", \"schemaRef\": \"#/patternProperties/%5EI_\","
        "    \"expected\": 6, \"actual\": 7"
        "  }"
        "}");
}

TEST(SchemaValidator, Object_PatternProperties_AdditionalPropertiesObject) {
    Document sd;
    sd.Parse(
        "{"
        "  \"type\": \"object\","
        "  \"properties\": {"
        "    \"builtin\": { \"type\": \"number\" }"
        "  },"
        "  \"patternProperties\": {"
        "    \"^S_\": { \"type\": \"string\" },"
        "    \"^I_\": { \"type\": \"integer\" }"
        "  },"
        "  \"additionalProperties\": { \"type\": \"string\" }"
        "}");
    SchemaDocument s(sd);

    VALIDATE(s, "{ \"builtin\": 42 }", true);
    VALIDATE(s, "{ \"keyword\": \"value\" }", true);
    INVALIDATE(s, "{ \"keyword\": 42 }", "/additionalProperties", "type", "/keyword",
        "{ \"type\": {"
        "    \"errorCode\": 20,"
        "    \"instanceRef\": \"#/keyword\", \"schemaRef\": \"#/additionalProperties\","
        "    \"expected\": [\"string\"], \"actual\": \"integer\""
        "}}");
}

// Replaces test Issue285 and tests failure as well as success
TEST(SchemaValidator, Object_PatternProperties_AdditionalPropertiesBoolean) {
    Document sd;
    sd.Parse(
        "{"
        "  \"type\": \"object\","
        "  \"patternProperties\": {"
        "    \"^S_\": { \"type\": \"string\" },"
        "    \"^I_\": { \"type\": \"integer\" }"
        "  },"
        "  \"additionalProperties\": false"
        "}");
    SchemaDocument s(sd);

    VALIDATE(s, "{ \"S_25\": \"This is a string\" }", true);
    VALIDATE(s, "{ \"I_0\": 42 }", true);
    INVALIDATE(s, "{ \"keyword\": \"value\" }", "", "additionalProperties", "/keyword",
        "{ \"additionalProperties\": {"
        "    \"errorCode\": 16,"
        "    \"instanceRef\": \"#\", \"schemaRef\": \"#\","
        "    \"disallowed\": \"keyword\""
        "}}");
}
#endif

TEST(SchemaValidator, Array) {
    Document sd;
    sd.Parse("{\"type\":\"array\"}");
    SchemaDocument s(sd);

    VALIDATE(s, "[1, 2, 3, 4, 5]", true);
    VALIDATE(s, "[3, \"different\", { \"types\" : \"of values\" }]", true);
    INVALIDATE(s, "{\"Not\": \"an array\"}", "", "type", "",
        "{ \"type\": {"
        "    \"errorCode\": 20,"
        "    \"instanceRef\": \"#\", \"schemaRef\": \"#\","
        "    \"expected\": [\"array\"], \"actual\": \"object\""
        "}}");
}

TEST(SchemaValidator, Array_ItemsList) {
    Document sd;
    sd.Parse(
        "{"
        "    \"type\": \"array\","
        "    \"items\" : {"
        "        \"type\": \"number\""
        "    }"
        "}");
    SchemaDocument s(sd);

    VALIDATE(s, "[1, 2, 3, 4, 5]", true);
    INVALIDATE(s, "[1, 2, \"3\", 4, 5]", "/items", "type", "/2",
        "{ \"type\": {"
        "    \"errorCode\": 20,"
        "    \"instanceRef\": \"#/2\", \"schemaRef\": \"#/items\","
        "    \"expected\": [\"number\"], \"actual\": \"string\""
        "}}");
    VALIDATE(s, "[]", true);
}

TEST(SchemaValidator, Array_ItemsTuple) {
    Document sd;
    sd.Parse(
        "{"
        "  \"type\": \"array\","
        "  \"items\": ["
        "    {"
        "      \"type\": \"number\""
        "    },"
        "    {"
        "      \"type\": \"string\""
        "    },"
        "    {"
        "      \"type\": \"string\","
        "      \"enum\": [\"Street\", \"Avenue\", \"Boulevard\"]"
        "    },"
        "    {"
        "      \"type\": \"string\","
        "      \"enum\": [\"NW\", \"NE\", \"SW\", \"SE\"]"
        "    }"
        "  ]"
        "}");
    SchemaDocument s(sd);

    VALIDATE(s, "[1600, \"Pennsylvania\", \"Avenue\", \"NW\"]", true);
    INVALIDATE(s, "[24, \"Sussex\", \"Drive\"]", "/items/2", "enum", "/2",
        "{ \"enum\": { \"errorCode\": 19, \"instanceRef\": \"#/2\", \"schemaRef\": \"#/items/2\" }}");
    INVALIDATE(s, "[\"Palais de l'Elysee\"]", "/items/0", "type", "/0",
        "{ \"type\": {"
        "    \"errorCode\": 20,"
        "    \"instanceRef\": \"#/0\", \"schemaRef\": \"#/items/0\","
        "    \"expected\": [\"number\"], \"actual\": \"string\""
        "}}");
    INVALIDATE(s, "[\"Twenty-four\", \"Sussex\", \"Drive\"]", "/items/0", "type", "/0",
        "{ \"type\": {"
        "    \"errorCode\": 20,"
        "    \"instanceRef\": \"#/0\", \"schemaRef\": \"#/items/0\","
        "    \"expected\": [\"number\"], \"actual\": \"string\""
        "}}"); // fail fast
    VALIDATE(s, "[10, \"Downing\", \"Street\"]", true);
    VALIDATE(s, "[1600, \"Pennsylvania\", \"Avenue\", \"NW\", \"Washington\"]", true);
}

TEST(SchemaValidator, Array_AdditionalItems) {
    Document sd;
    sd.Parse(
        "{"
        "  \"type\": \"array\","
        "  \"items\": ["
        "    {"
        "      \"type\": \"number\""
        "    },"
        "    {"
        "      \"type\": \"string\""
        "    },"
        "    {"
        "      \"type\": \"string\","
        "      \"enum\": [\"Street\", \"Avenue\", \"Boulevard\"]"
        "    },"
        "    {"
        "      \"type\": \"string\","
        "      \"enum\": [\"NW\", \"NE\", \"SW\", \"SE\"]"
        "    }"
        "  ],"
        "  \"additionalItems\": false"
        "}");
    SchemaDocument s(sd);

    VALIDATE(s, "[1600, \"Pennsylvania\", \"Avenue\", \"NW\"]", true);
    VALIDATE(s, "[1600, \"Pennsylvania\", \"Avenue\"]", true);
    INVALIDATE(s, "[1600, \"Pennsylvania\", \"Avenue\", \"NW\", \"Washington\"]", "", "additionalItems", "/4",
        "{ \"additionalItems\": {"
        "    \"errorCode\": 12,"
        "    \"instanceRef\": \"#\", \"schemaRef\": \"#\","
        "    \"disallowed\": 4"
        "}}");
}

TEST(SchemaValidator, Array_ItemsRange) {
    Document sd;
    sd.Parse("{\"type\": \"array\",\"minItems\": 2,\"maxItems\" : 3}");
    SchemaDocument s(sd);

    INVALIDATE(s, "[]", "", "minItems", "",
        "{ \"minItems\": {"
        "    \"errorCode\": 10,"
        "    \"instanceRef\": \"#\", \"schemaRef\": \"#\","
        "    \"expected\": 2, \"actual\": 0"
        "}}");
    INVALIDATE(s, "[1]", "", "minItems", "",
        "{ \"minItems\": {"
        "    \"errorCode\": 10,"
        "    \"instanceRef\": \"#\", \"schemaRef\": \"#\","
        "    \"expected\": 2, \"actual\": 1"
        "}}");
    VALIDATE(s, "[1, 2]", true);
    VALIDATE(s, "[1, 2, 3]", true);
    INVALIDATE(s, "[1, 2, 3, 4]", "", "maxItems", "",
        "{ \"maxItems\": {"
        "    \"errorCode\": 9,"
        "    \"instanceRef\": \"#\", \"schemaRef\": \"#\","
        "    \"expected\": 3, \"actual\": 4"
        "}}");
}

TEST(SchemaValidator, Array_UniqueItems) {
    Document sd;
    sd.Parse("{\"type\": \"array\", \"uniqueItems\": true}");
    SchemaDocument s(sd);

    VALIDATE(s, "[1, 2, 3, 4, 5]", true);
    INVALIDATE(s, "[1, 2, 3, 3, 4]", "", "uniqueItems", "/3",
        "{ \"uniqueItems\": {"
        "    \"errorCode\": 11,"
        "    \"instanceRef\": \"#\", \"schemaRef\": \"#\","
        "    \"duplicates\": [2, 3]"
        "}}");
    INVALIDATE(s, "[1, 2, 3, 3, 3]", "", "uniqueItems", "/3",
        "{ \"uniqueItems\": {"
        "    \"errorCode\": 11,"
        "    \"instanceRef\": \"#\", \"schemaRef\": \"#\","
        "    \"duplicates\": [2, 3]"
        "}}"); // fail fast
    VALIDATE(s, "[]", true);
}

TEST(SchemaValidator, Boolean) {
    Document sd;
    sd.Parse("{\"type\":\"boolean\"}");
    SchemaDocument s(sd);

    VALIDATE(s, "true", true);
    VALIDATE(s, "false", true);
    INVALIDATE(s, "\"true\"", "", "type", "",
        "{ \"type\": {"
        "    \"errorCode\": 20,"
        "    \"instanceRef\": \"#\", \"schemaRef\": \"#\","
        "    \"expected\": [\"boolean\"], \"actual\": \"string\""
        "}}");
    INVALIDATE(s, "0", "", "type", "",
        "{ \"type\": {"
        "    \"errorCode\": 20,"
        "    \"instanceRef\": \"#\", \"schemaRef\": \"#\","
        "    \"expected\": [\"boolean\"], \"actual\": \"integer\""
        "}}");
}

TEST(SchemaValidator, Null) {
    Document sd;
    sd.Parse("{\"type\":\"null\"}");
    SchemaDocument s(sd);

    VALIDATE(s, "null", true);
    INVALIDATE(s, "false", "", "type", "",
        "{ \"type\": {"
        "    \"errorCode\": 20,"
        "    \"instanceRef\": \"#\", \"schemaRef\": \"#\","
        "    \"expected\": [\"null\"], \"actual\": \"boolean\""
        "}}");
    INVALIDATE(s, "0", "", "type", "",
        "{ \"type\": {"
        "    \"errorCode\": 20,"
        "    \"instanceRef\": \"#\", \"schemaRef\": \"#\","
        "    \"expected\": [\"null\"], \"actual\": \"integer\""
        "}}");
    INVALIDATE(s, "\"\"", "", "type", "",
        "{ \"type\": {"
        "    \"errorCode\": 20,"
        "    \"instanceRef\": \"#\", \"schemaRef\": \"#\","
        "    \"expected\": [\"null\"], \"actual\": \"string\""
        "}}");
}

// Additional tests

TEST(SchemaValidator, ObjectInArray) {
    Document sd;
    sd.Parse("{\"type\":\"array\", \"items\": { \"type\":\"string\" }}");
    SchemaDocument s(sd);

    VALIDATE(s, "[\"a\"]", true);
    INVALIDATE(s, "[1]", "/items", "type", "/0",
        "{ \"type\": {"
        "    \"errorCode\": 20,"
        "    \"instanceRef\": \"#/0\", \"schemaRef\": \"#/items\","
        "    \"expected\": [\"string\"], \"actual\": \"integer\""
        "}}");
    INVALIDATE(s, "[{}]", "/items", "type", "/0",
        "{ \"type\": {"
        "    \"errorCode\": 20,"
        "    \"instanceRef\": \"#/0\", \"schemaRef\": \"#/items\","
        "    \"expected\": [\"string\"], \"actual\": \"object\""
        "}}");
}

TEST(SchemaValidator, MultiTypeInObject) {
    Document sd;
    sd.Parse(
        "{"
        "    \"type\":\"object\","
        "    \"properties\": {"
        "        \"tel\" : {"
        "            \"type\":[\"integer\", \"string\"]"
        "        }"
        "    }"
        "}");
    SchemaDocument s(sd);

    VALIDATE(s, "{ \"tel\": 999 }", true);
    VALIDATE(s, "{ \"tel\": \"123-456\" }", true);
    INVALIDATE(s, "{ \"tel\": true }", "/properties/tel", "type", "/tel",
        "{ \"type\": {"
        "    \"errorCode\": 20,"
        "    \"instanceRef\": \"#/tel\", \"schemaRef\": \"#/properties/tel\","
        "    \"expected\": [\"string\", \"integer\"], \"actual\": \"boolean\""
        "}}");
}

TEST(SchemaValidator, MultiTypeWithObject) {
    Document sd;
    sd.Parse(
        "{"
        "    \"type\": [\"object\",\"string\"],"
        "    \"properties\": {"
        "        \"tel\" : {"
        "            \"type\": \"integer\""
        "        }"
        "    }"
        "}");
    SchemaDocument s(sd);

    VALIDATE(s, "\"Hello\"", true);
    VALIDATE(s, "{ \"tel\": 999 }", true);
    INVALIDATE(s, "{ \"tel\": \"fail\" }", "/properties/tel", "type", "/tel",
        "{ \"type\": {"
        "    \"errorCode\": 20,"
        "    \"instanceRef\": \"#/tel\", \"schemaRef\": \"#/properties/tel\","
        "    \"expected\": [\"integer\"], \"actual\": \"string\""
        "}}");
}

TEST(SchemaValidator, AllOf_Nested) {
    Document sd;
    sd.Parse(
    "{"
    "    \"allOf\": ["
    "        { \"type\": \"string\", \"minLength\": 2 },"
    "        { \"type\": \"string\", \"maxLength\": 5 },"
    "        { \"allOf\": [ { \"enum\" : [\"ok\", \"okay\", \"OK\", \"o\"] }, { \"enum\" : [\"ok\", \"OK\", \"o\"]} ] }"
    "    ]"
    "}");
    SchemaDocument s(sd);

    VALIDATE(s, "\"ok\"", true);
    VALIDATE(s, "\"OK\"", true);
    INVALIDATE(s, "\"okay\"", "", "allOf", "",
        "{ \"allOf\": {"
        "    \"errors\": ["
        "    {},{},"
        "    { \"allOf\": {"
        "      \"errors\": ["
        "        {},"
        "        { \"enum\": {\"errorCode\": 19, \"instanceRef\": \"#\", \"schemaRef\": \"#/allOf/2/allOf/1\" }}"
        "      ],"
        "      \"errorCode\": 23, \"instanceRef\": \"#\", \"schemaRef\": \"#/allOf/2\""
        "    }}],"
        "    \"errorCode\": 23, \"instanceRef\": \"#\", \"schemaRef\": \"#\""
        "}}");
    INVALIDATE(s, "\"o\"", "", "allOf", "",
        "{ \"allOf\": {"
        "  \"errors\": ["
        "    { \"minLength\": {\"actual\": \"o\", \"expected\": 2, \"errorCode\": 7, \"instanceRef\": \"#\", \"schemaRef\": \"#/allOf/0\" }},"
        "    {},{}"
        "  ],"
        "  \"errorCode\": 23, \"instanceRef\": \"#\", \"schemaRef\": \"#\""
        "}}");
    INVALIDATE(s, "\"n\"", "", "allOf", "",
        "{ \"allOf\": {"
        "    \"errors\": ["
        "      { \"minLength\": {\"actual\": \"n\", \"expected\": 2, \"errorCode\": 7, \"instanceRef\": \"#\", \"schemaRef\": \"#/allOf/0\" }},"
        "      {},"
        "      { \"allOf\": {"
        "          \"errors\": ["
        "            { \"enum\": {\"errorCode\": 19 ,\"instanceRef\": \"#\", \"schemaRef\": \"#/allOf/2/allOf/0\"}},"
        "            { \"enum\": {\"errorCode\": 19, \"instanceRef\": \"#\", \"schemaRef\": \"#/allOf/2/allOf/1\"}}"
        "          ],"
        "          \"errorCode\": 23, \"instanceRef\": \"#\", \"schemaRef\": \"#/allOf/2\""
        "      }}"
        "    ],"
        "    \"errorCode\":23,\"instanceRef\":\"#\",\"schemaRef\":\"#\""
        "}}");
    INVALIDATE(s, "\"too long\"", "", "allOf", "",
        "{ \"allOf\": {"
        "    \"errors\": ["
        "      {},"
        "      { \"maxLength\": {\"actual\": \"too long\", \"expected\": 5, \"errorCode\": 6, \"instanceRef\": \"#\", \"schemaRef\": \"#/allOf/1\" }},"
        "      { \"allOf\": {"
        "          \"errors\": ["
        "            { \"enum\": {\"errorCode\": 19 ,\"instanceRef\": \"#\", \"schemaRef\": \"#/allOf/2/allOf/0\"}},"
        "            { \"enum\": {\"errorCode\": 19, \"instanceRef\": \"#\", \"schemaRef\": \"#/allOf/2/allOf/1\"}}"
        "          ],"
        "          \"errorCode\": 23, \"instanceRef\": \"#\", \"schemaRef\": \"#/allOf/2\""
        "      }}"
        "    ],"
        "    \"errorCode\":23,\"instanceRef\":\"#\",\"schemaRef\":\"#\""
        "}}");
    INVALIDATE(s, "123", "", "allOf", "",
        "{ \"allOf\": {"
        "    \"errors\": ["
        "      {\"type\": {\"expected\": [\"string\"], \"actual\": \"integer\", \"errorCode\": 20, \"instanceRef\": \"#\", \"schemaRef\": \"#/allOf/0\"}},"
        "      {\"type\": {\"expected\": [\"string\"], \"actual\": \"integer\", \"errorCode\": 20, \"instanceRef\": \"#\", \"schemaRef\": \"#/allOf/1\"}},"
        "      { \"allOf\": {"
        "          \"errors\": ["
        "            { \"enum\": {\"errorCode\": 19 ,\"instanceRef\": \"#\", \"schemaRef\": \"#/allOf/2/allOf/0\"}},"
        "            { \"enum\": {\"errorCode\": 19, \"instanceRef\": \"#\", \"schemaRef\": \"#/allOf/2/allOf/1\"}}"
        "          ],"
        "          \"errorCode\": 23, \"instanceRef\": \"#\", \"schemaRef\": \"#/allOf/2\""
        "      }}"
        "    ],"
        "    \"errorCode\":23,\"instanceRef\":\"#\",\"schemaRef\":\"#\""
        "}}");
}

TEST(SchemaValidator, EscapedPointer) {
    Document sd;
    sd.Parse(
        "{"
        "  \"type\": \"object\","
        "  \"properties\": {"
        "    \"~/\": { \"type\": \"number\" }"
        "  }"
        "}");
    SchemaDocument s(sd);
    INVALIDATE(s, "{\"~/\":true}", "/properties/~0~1", "type", "/~0~1",
        "{ \"type\": {"
        "    \"errorCode\": 20,"
        "    \"instanceRef\": \"#/~0~1\", \"schemaRef\": \"#/properties/~0~1\","
        "    \"expected\": [\"number\"], \"actual\": \"boolean\""
        "}}");
}

TEST(SchemaValidator, SchemaPointer) {
    Document sd;
    sd.Parse(
        "{"
        "  \"swagger\": \"2.0\","
        "  \"paths\": {"
        "    \"/some/path\": {"
        "      \"post\": {"
        "        \"parameters\": ["
        "          {"
        "            \"in\": \"body\","
        "            \"name\": \"body\","
        "            \"schema\": {"
        "              \"properties\": {"
        "                \"a\": {"
        "                  \"$ref\": \"#/definitions/Prop_a\""
        "                },"
        "                \"b\": {"
        "                  \"type\": \"integer\""
        "                }"
        "              },"
        "              \"type\": \"object\""
        "            }"
        "          }"
        "        ],"
        "        \"responses\": {"
        "          \"200\": {"
        "            \"schema\": {"
        "              \"$ref\": \"#/definitions/Resp_200\""
        "            }"
        "          }"
        "        }"
        "      }"
        "    }"
        "  },"
        "  \"definitions\": {"
        "    \"Prop_a\": {"
        "      \"properties\": {"
        "        \"c\": {"
        "          \"enum\": ["
        "            \"C1\","
        "            \"C2\","
        "            \"C3\""
        "          ],"
        "          \"type\": \"string\""
        "        },"
        "        \"d\": {"
        "          \"$ref\": \"#/definitions/Prop_d\""
        "        },"
        "        \"s\": {"
        "          \"type\": \"string\""
        "        }"
        "      },"
        "      \"required\": [\"c\"],"
        "      \"type\": \"object\""
        "    },"
        "    \"Prop_d\": {"
        "      \"properties\": {"
        "        \"a\": {"
        "          \"$ref\": \"#/definitions/Prop_a\""
        "        },"
        "        \"c\": {"
        "          \"$ref\": \"#/definitions/Prop_a/properties/c\""
        "        }"
        "      },"
        "      \"type\": \"object\""
        "    },"
        "    \"Resp_200\": {"
        "      \"properties\": {"
        "        \"e\": {"
        "          \"type\": \"string\""
        "        },"
        "        \"f\": {"
        "          \"type\": \"boolean\""
        "        },"
        "        \"cyclic_source\": {"
        "          \"$ref\": \"#/definitions/Resp_200/properties/cyclic_target\""
        "        },"
        "        \"cyclic_target\": {"
        "          \"$ref\": \"#/definitions/Resp_200/properties/cyclic_source\""
        "        }"
        "      },"
        "      \"type\": \"object\""
        "    }"
        "  }"
        "}");
    SchemaDocument s1(sd, NULL, 0, NULL, NULL, Pointer("#/paths/~1some~1path/post/parameters/0/schema"));
    VALIDATE(s1,
        "{"
        "  \"a\": {"
        "    \"c\": \"C1\","
        "    \"d\": {"
        "      \"a\": {"
        "        \"c\": \"C2\""
        "      },"
        "      \"c\": \"C3\""
        "    }"
        "  },"
        "  \"b\": 123"
        "}",
         true);
    INVALIDATE(s1,
        "{"
        "  \"a\": {"
        "    \"c\": \"C1\","
        "    \"d\": {"
        "      \"a\": {"
        "        \"c\": \"C2\""
        "      },"
        "      \"c\": \"C3\""
        "    }"
        "  },"
        "  \"b\": \"should be an int\""
        "}",
        "#/paths/~1some~1path/post/parameters/0/schema/properties/b", "type", "#/b",
        "{ \"type\": {"
        "    \"errorCode\": 20,"
        "    \"instanceRef\":\"#/b\","
        "    \"schemaRef\":\"#/paths/~1some~1path/post/parameters/0/schema/properties/b\","
        "    \"expected\": [\"integer\"], \"actual\":\"string\""
        "}}");
    INVALIDATE(s1,
        "{"
        "  \"a\": {"
        "    \"c\": \"C1\","
        "    \"d\": {"
        "      \"a\": {"
        "        \"c\": \"should be within enum\""
        "      },"
        "      \"c\": \"C3\""
        "    }"
        "  },"
        "  \"b\": 123"
        "}",
        "#/definitions/Prop_a/properties/c", "enum", "#/a/d/a/c",
        "{ \"enum\": {"
        "    \"errorCode\": 19,"
        "    \"instanceRef\":\"#/a/d/a/c\","
        "    \"schemaRef\":\"#/definitions/Prop_a/properties/c\""
        "}}");
    INVALIDATE(s1,
        "{"
        "  \"a\": {"
        "    \"c\": \"C1\","
        "    \"d\": {"
        "      \"a\": {"
        "        \"s\": \"required 'c' is missing\""
        "      }"
        "    }"
        "  },"
        "  \"b\": 123"
        "}",
        "#/definitions/Prop_a", "required", "#/a/d/a",
        "{ \"required\": {"
        "    \"errorCode\": 15,"
        "    \"missing\":[\"c\"],"
        "    \"instanceRef\":\"#/a/d/a\","
        "    \"schemaRef\":\"#/definitions/Prop_a\""
        "}}");
    SchemaDocument s2(sd, NULL, 0, NULL, NULL, Pointer("#/paths/~1some~1path/post/responses/200/schema"));
    VALIDATE(s2,
        "{ \"e\": \"some string\", \"f\": false }",
        true);
    INVALIDATE(s2,
        "{ \"e\": true, \"f\": false }",
        "#/definitions/Resp_200/properties/e", "type", "#/e",
        "{ \"type\": {"
        "    \"errorCode\": 20,"
        "    \"instanceRef\":\"#/e\","
        "    \"schemaRef\":\"#/definitions/Resp_200/properties/e\","
        "    \"expected\": [\"string\"], \"actual\":\"boolean\""
        "}}");
    INVALIDATE(s2,
        "{ \"e\": \"some string\", \"f\": 123 }",
        "#/definitions/Resp_200/properties/f", "type", "#/f",
        "{ \"type\": {"
        "    \"errorCode\": 20,"
        "    \"instanceRef\":\"#/f\","
        "    \"schemaRef\":\"#/definitions/Resp_200/properties/f\","
        "    \"expected\": [\"boolean\"], \"actual\":\"integer\""
        "}}");
}

template <typename Allocator>
static char* ReadFile(const char* filename, Allocator& allocator) {
    const char *paths[] = {
        "",
        "bin/",
        "../bin/",
        "../../bin/",
        "../../../bin/"
    };
    char buffer[1024];
    FILE *fp = 0;
    for (size_t i = 0; i < sizeof(paths) / sizeof(paths[0]); i++) {
        sprintf(buffer, "%s%s", paths[i], filename);
        fp = fopen(buffer, "rb");
        if (fp)
            break;
    }

    if (!fp)
        return 0;

    fseek(fp, 0, SEEK_END);
    size_t length = static_cast<size_t>(ftell(fp));
    fseek(fp, 0, SEEK_SET);
    char* json = reinterpret_cast<char*>(allocator.Malloc(length + 1));
    size_t readLength = fread(json, 1, length, fp);
    json[readLength] = '\0';
    fclose(fp);
    return json;
}

TEST(SchemaValidator, ValidateMetaSchema) {
    CrtAllocator allocator;
    char* json = ReadFile("draft-04/schema", allocator);
    Document d;
    d.Parse(json);
    ASSERT_FALSE(d.HasParseError());
    SchemaDocument sd(d);
    SchemaValidator validator(sd);
    d.Accept(validator);
    if (!validator.IsValid()) {
        StringBuffer sb;
        validator.GetInvalidSchemaPointer().StringifyUriFragment(sb);
        printf("Invalid schema: %s\n", sb.GetString());
        printf("Invalid keyword: %s\n", validator.GetInvalidSchemaKeyword());
        printf("Invalid code: %d\n", validator.GetInvalidSchemaCode());
        printf("Invalid message: %s\n", GetValidateError_En(validator.GetInvalidSchemaCode()));
        sb.Clear();
        validator.GetInvalidDocumentPointer().StringifyUriFragment(sb);
        printf("Invalid document: %s\n", sb.GetString());
        sb.Clear();
        Writer<StringBuffer> w(sb);
        validator.GetError().Accept(w);
        printf("Validation error: %s\n", sb.GetString());
        ADD_FAILURE();
    }
    CrtAllocator::Free(json);
}

TEST(SchemaValidator, ValidateMetaSchema_UTF16) {
    typedef GenericDocument<UTF16<> > D;
    typedef GenericSchemaDocument<D::ValueType> SD;
    typedef GenericSchemaValidator<SD> SV;

    CrtAllocator allocator;
    char* json = ReadFile("draft-04/schema", allocator);

    D d;
    StringStream ss(json);
    d.ParseStream<0, UTF8<> >(ss);
    ASSERT_FALSE(d.HasParseError());
    SD sd(d);
    SV validator(sd);
    d.Accept(validator);
    if (!validator.IsValid()) {
        GenericStringBuffer<UTF16<> > sb;
        validator.GetInvalidSchemaPointer().StringifyUriFragment(sb);
        wprintf(L"Invalid schema: %ls\n", sb.GetString());
        wprintf(L"Invalid keyword: %ls\n", validator.GetInvalidSchemaKeyword());
        sb.Clear();
        validator.GetInvalidDocumentPointer().StringifyUriFragment(sb);
        wprintf(L"Invalid document: %ls\n", sb.GetString());
        sb.Clear();
        Writer<GenericStringBuffer<UTF16<> >, UTF16<> > w(sb);
        validator.GetError().Accept(w);
        printf("Validation error: %ls\n", sb.GetString());
        ADD_FAILURE();
    }
    CrtAllocator::Free(json);
}

template <typename SchemaDocumentType = SchemaDocument>
class RemoteSchemaDocumentProvider : public IGenericRemoteSchemaDocumentProvider<SchemaDocumentType> {
public:
    RemoteSchemaDocumentProvider() : 
        documentAllocator_(documentBuffer_, sizeof(documentBuffer_)), 
        schemaAllocator_(schemaBuffer_, sizeof(schemaBuffer_)) 
    {
        const char* filenames[kCount] = {
            "jsonschema/remotes/integer.json",
            "jsonschema/remotes/subSchemas.json",
            "jsonschema/remotes/folder/folderInteger.json",
            "draft-04/schema",
            "unittestschema/address.json"
        };
        const char* uris[kCount] = {
            "http://localhost:1234/integer.json",
            "http://localhost:1234/subSchemas.json",
            "http://localhost:1234/folder/folderInteger.json",
            "http://json-schema.org/draft-04/schema",
            "http://localhost:1234/address.json"
        };

        for (size_t i = 0; i < kCount; i++) {
            sd_[i] = 0;

            char jsonBuffer[8192];
            MemoryPoolAllocator<> jsonAllocator(jsonBuffer, sizeof(jsonBuffer));
            char* json = ReadFile(filenames[i], jsonAllocator);
            if (!json) {
                printf("json remote file %s not found", filenames[i]);
                ADD_FAILURE();
            }
            else {
                char stackBuffer[4096];
                MemoryPoolAllocator<> stackAllocator(stackBuffer, sizeof(stackBuffer));
                DocumentType d(&documentAllocator_, 1024, &stackAllocator);
                d.Parse(json);
                sd_[i] = new SchemaDocumentType(d, uris[i], static_cast<SizeType>(strlen(uris[i])), 0, &schemaAllocator_);
                MemoryPoolAllocator<>::Free(json);
            }
        };
    }

    ~RemoteSchemaDocumentProvider() {
        for (size_t i = 0; i < kCount; i++)
            delete sd_[i];
    }

    virtual const SchemaDocumentType* GetRemoteDocument(const char* uri, SizeType length) {
        for (size_t i = 0; i < kCount; i++)
            if (typename SchemaDocumentType::SValue(uri, length) == sd_[i]->GetURI())
                return sd_[i];
        return 0;
    }

private:
    typedef GenericDocument<typename SchemaDocumentType::EncodingType, MemoryPoolAllocator<>, MemoryPoolAllocator<> > DocumentType;

    RemoteSchemaDocumentProvider(const RemoteSchemaDocumentProvider&);
    RemoteSchemaDocumentProvider& operator=(const RemoteSchemaDocumentProvider&);

    static const size_t kCount = 5;
    SchemaDocumentType* sd_[kCount];
    typename DocumentType::AllocatorType documentAllocator_;
    typename SchemaDocumentType::AllocatorType schemaAllocator_;
    char documentBuffer_[16384];
    char schemaBuffer_[128u * 1024];
};

TEST(SchemaValidator, TestSuite) {
    const char* filenames[] = {
        "additionalItems.json",
        "additionalProperties.json",
        "allOf.json",
        "anyOf.json",
        "default.json",
        "definitions.json",
        "dependencies.json",
        "enum.json",
        "items.json",
        "maximum.json",
        "maxItems.json",
        "maxLength.json",
        "maxProperties.json",
        "minimum.json",
        "minItems.json",
        "minLength.json",
        "minProperties.json",
        "multipleOf.json",
        "not.json",
        "oneOf.json",
        "pattern.json",
        "patternProperties.json",
        "properties.json",
        "ref.json",
        "refRemote.json",
        "required.json",
        "type.json",
        "uniqueItems.json"
    };

    const char* onlyRunDescription = 0;
    //const char* onlyRunDescription = "a string is a string";

    unsigned testCount = 0;
    unsigned passCount = 0;

    typedef GenericSchemaDocument<Value, MemoryPoolAllocator<> > SchemaDocumentType;
    RemoteSchemaDocumentProvider<SchemaDocumentType> provider;

    char jsonBuffer[65536];
    char documentBuffer[65536];
    char documentStackBuffer[65536];
    char schemaBuffer[65536];
    char validatorBuffer[65536];
    MemoryPoolAllocator<> jsonAllocator(jsonBuffer, sizeof(jsonBuffer));
    MemoryPoolAllocator<> documentAllocator(documentBuffer, sizeof(documentBuffer));
    MemoryPoolAllocator<> documentStackAllocator(documentStackBuffer, sizeof(documentStackBuffer));
    MemoryPoolAllocator<> schemaAllocator(schemaBuffer, sizeof(schemaBuffer));
    MemoryPoolAllocator<> validatorAllocator(validatorBuffer, sizeof(validatorBuffer));

    for (size_t i = 0; i < sizeof(filenames) / sizeof(filenames[0]); i++) {
        char filename[FILENAME_MAX];
        sprintf(filename, "jsonschema/tests/draft4/%s", filenames[i]);
        char* json = ReadFile(filename, jsonAllocator);
        if (!json) {
            printf("json test suite file %s not found", filename);
            ADD_FAILURE();
        }
        else {
            //printf("\njson test suite file %s parsed ok\n", filename);
            GenericDocument<UTF8<>, MemoryPoolAllocator<>, MemoryPoolAllocator<> > d(&documentAllocator, 1024, &documentStackAllocator);
            d.Parse(json);
            if (d.HasParseError()) {
                printf("json test suite file %s has parse error", filename);
                ADD_FAILURE();
            }
            else {
                for (Value::ConstValueIterator schemaItr = d.Begin(); schemaItr != d.End(); ++schemaItr) {
                    {
                        const char* description1 = (*schemaItr)["description"].GetString();
                        //printf("\ncompiling schema for json test %s \n", description1);
                        SchemaDocumentType schema((*schemaItr)["schema"], filenames[i], static_cast<SizeType>(strlen(filenames[i])), &provider, &schemaAllocator);
                        GenericSchemaValidator<SchemaDocumentType, BaseReaderHandler<UTF8<> >, MemoryPoolAllocator<> > validator(schema, &validatorAllocator);
                        const Value& tests = (*schemaItr)["tests"];
                        for (Value::ConstValueIterator testItr = tests.Begin(); testItr != tests.End(); ++testItr) {
                            const char* description2 = (*testItr)["description"].GetString();
                            //printf("running json test %s \n", description2);
                            if (!onlyRunDescription || strcmp(description2, onlyRunDescription) == 0) {
                                const Value& data = (*testItr)["data"];
                                bool expected = (*testItr)["valid"].GetBool();
                                testCount++;
                                validator.Reset();
                                data.Accept(validator);
                                bool actual = validator.IsValid();
                                if (expected != actual)
                                    printf("Fail: %30s \"%s\" \"%s\"\n", filename, description1, description2);
                                else {
                                    //printf("Passed: %30s \"%s\" \"%s\"\n", filename, description1, description2);
                                    passCount++;
                                }
                            }
                        }
                        //printf("%zu %zu %zu\n", documentAllocator.Size(), schemaAllocator.Size(), validatorAllocator.Size());
                    }
                    schemaAllocator.Clear();
                    validatorAllocator.Clear();
                }
            }
        }
        documentAllocator.Clear();
        MemoryPoolAllocator<>::Free(json);
        jsonAllocator.Clear();
    }
    printf("%d / %d passed (%2d%%)\n", passCount, testCount, passCount * 100 / testCount);
    if (passCount != testCount)
        ADD_FAILURE();
}

TEST(SchemaValidatingReader, Simple) {
    Document sd;
    sd.Parse("{ \"type\": \"string\", \"enum\" : [\"red\", \"amber\", \"green\"] }");
    SchemaDocument s(sd);

    Document d;
    StringStream ss("\"red\"");
    SchemaValidatingReader<kParseDefaultFlags, StringStream, UTF8<> > reader(ss, s);
    d.Populate(reader);
    EXPECT_TRUE(reader.GetParseResult());
    EXPECT_TRUE(reader.IsValid());
    EXPECT_TRUE(d.IsString());
    EXPECT_STREQ("red", d.GetString());
}

TEST(SchemaValidatingReader, Invalid) {
    Document sd;
    sd.Parse("{\"type\":\"string\",\"minLength\":2,\"maxLength\":3}");
    SchemaDocument s(sd);

    Document d;
    StringStream ss("\"ABCD\"");
    SchemaValidatingReader<kParseDefaultFlags, StringStream, UTF8<> > reader(ss, s);
    d.Populate(reader);
    EXPECT_FALSE(reader.GetParseResult());
    EXPECT_FALSE(reader.IsValid());
    EXPECT_EQ(kParseErrorTermination, reader.GetParseResult().Code());
    EXPECT_STREQ("maxLength", reader.GetInvalidSchemaKeyword());
    EXPECT_TRUE(reader.GetInvalidSchemaCode() == kValidateErrorMaxLength);
    EXPECT_TRUE(reader.GetInvalidSchemaPointer() == SchemaDocument::PointerType(""));
    EXPECT_TRUE(reader.GetInvalidDocumentPointer() == SchemaDocument::PointerType(""));
    EXPECT_TRUE(d.IsNull());
    Document e;
    e.Parse(
        "{ \"maxLength\": {"
        "     \"errorCode\": 6,"
        "    \"instanceRef\": \"#\", \"schemaRef\": \"#\","
        "    \"expected\": 3, \"actual\": \"ABCD\""
        "}}");
    if (e != reader.GetError()) {
        ADD_FAILURE();
    }
}

TEST(SchemaValidatingWriter, Simple) {
    Document sd;
    sd.Parse("{\"type\":\"string\",\"minLength\":2,\"maxLength\":3}");
    SchemaDocument s(sd);

    Document d;
    StringBuffer sb;
    Writer<StringBuffer> writer(sb);
    GenericSchemaValidator<SchemaDocument, Writer<StringBuffer> > validator(s, writer);

    d.Parse("\"red\"");
    EXPECT_TRUE(d.Accept(validator));
    EXPECT_TRUE(validator.IsValid());
    EXPECT_STREQ("\"red\"", sb.GetString());

    sb.Clear();
    validator.Reset();
    d.Parse("\"ABCD\"");
    EXPECT_FALSE(d.Accept(validator));
    EXPECT_FALSE(validator.IsValid());
    EXPECT_TRUE(validator.GetInvalidSchemaPointer() == SchemaDocument::PointerType(""));
    EXPECT_TRUE(validator.GetInvalidDocumentPointer() == SchemaDocument::PointerType(""));
    EXPECT_TRUE(validator.GetInvalidSchemaCode() == kValidateErrorMaxLength);
    Document e;
    e.Parse(
        "{ \"maxLength\": {"
"            \"errorCode\": 6,"
        "    \"instanceRef\": \"#\", \"schemaRef\": \"#\","
        "    \"expected\": 3, \"actual\": \"ABCD\""
        "}}");
    EXPECT_EQ(e, validator.GetError());
}

TEST(Schema, Issue848) {
    rapidjson::Document d;
    rapidjson::SchemaDocument s(d);
    rapidjson::GenericSchemaValidator<rapidjson::SchemaDocument, rapidjson::Document> v(s);
}

#if RAPIDJSON_HAS_CXX11_RVALUE_REFS

static SchemaDocument ReturnSchemaDocument() {
    Document sd;
    sd.Parse("{ \"type\": [\"number\", \"string\"] }");
    SchemaDocument s(sd);
    return s;
}

TEST(Schema, Issue552) {
    SchemaDocument s = ReturnSchemaDocument();
    VALIDATE(s, "42", true);
    VALIDATE(s, "\"Life, the universe, and everything\"", true);
    INVALIDATE(s, "[\"Life\", \"the universe\", \"and everything\"]", "", "type", "",
        "{ \"type\": {"
        "    \"errorCode\": 20,"
        "    \"instanceRef\": \"#\", \"schemaRef\": \"#\","
        "    \"expected\": [\"string\", \"number\"], \"actual\": \"array\""
        "}}");
}

#endif // RAPIDJSON_HAS_CXX11_RVALUE_REFS

TEST(SchemaValidator, Issue608) {
    Document sd;
    sd.Parse("{\"required\": [\"a\", \"b\"] }");
    SchemaDocument s(sd);

    VALIDATE(s, "{\"a\" : null, \"b\": null}", true);
    INVALIDATE(s, "{\"a\" : null, \"a\" : null}", "", "required", "",
        "{ \"required\": {"
        "    \"errorCode\": 15,"
        "    \"instanceRef\": \"#\", \"schemaRef\": \"#\","
        "    \"missing\": [\"b\"]"
        "}}");
}

// Fail to resolve $ref in allOf causes crash in SchemaValidator::StartObject()
TEST(SchemaValidator, Issue728_AllOfRef) {
    Document sd;
    sd.Parse("{\"allOf\": [{\"$ref\": \"#/abc\"}]}");
    SchemaDocument s(sd);
    VALIDATE(s, "{\"key1\": \"abc\", \"key2\": \"def\"}", true);
}

TEST(SchemaValidator, Issue1017_allOfHandler) {
    Document sd;
    sd.Parse("{\"allOf\": [{\"type\": \"object\",\"properties\": {\"cyanArray2\": {\"type\": \"array\",\"items\": { \"type\": \"string\" }}}},{\"type\": \"object\",\"properties\": {\"blackArray\": {\"type\": \"array\",\"items\": { \"type\": \"string\" }}},\"required\": [ \"blackArray\" ]}]}");
    SchemaDocument s(sd);
    StringBuffer sb;
    Writer<StringBuffer> writer(sb);
    GenericSchemaValidator<SchemaDocument, Writer<StringBuffer> > validator(s, writer);
    EXPECT_TRUE(validator.StartObject());
    EXPECT_TRUE(validator.Key("cyanArray2", 10, false));
    EXPECT_TRUE(validator.StartArray());    
    EXPECT_TRUE(validator.EndArray(0));    
    EXPECT_TRUE(validator.Key("blackArray", 10, false));
    EXPECT_TRUE(validator.StartArray());    
    EXPECT_TRUE(validator.EndArray(0));    
    EXPECT_TRUE(validator.EndObject(0));
    EXPECT_TRUE(validator.IsValid());
    EXPECT_STREQ("{\"cyanArray2\":[],\"blackArray\":[]}", sb.GetString());
}

TEST(SchemaValidator, Ref_remote) {
    typedef GenericSchemaDocument<Value, MemoryPoolAllocator<> > SchemaDocumentType;
    RemoteSchemaDocumentProvider<SchemaDocumentType> provider;
    Document sd;
    sd.Parse("{\"$ref\": \"http://localhost:1234/subSchemas.json#/integer\"}");
    SchemaDocumentType s(sd, 0, 0, &provider);
    typedef GenericSchemaValidator<SchemaDocumentType, BaseReaderHandler<UTF8<> >, MemoryPoolAllocator<> > SchemaValidatorType;
    typedef GenericPointer<Value, MemoryPoolAllocator<> > PointerType;
    INVALIDATE_(s, "null", "/integer", "type", "",
        "{ \"type\": {"
        "    \"errorCode\": 20,"
        "    \"instanceRef\": \"#\","
        "    \"schemaRef\": \"http://localhost:1234/subSchemas.json#/integer\","
        "    \"expected\": [\"integer\"], \"actual\": \"null\""
        "}}",
        kValidateDefaultFlags, SchemaValidatorType, PointerType);
}

// Merge with id where $ref is full URI
TEST(SchemaValidator, Ref_remote_change_resolution_scope_uri) {
    typedef GenericSchemaDocument<Value, MemoryPoolAllocator<> > SchemaDocumentType;
    RemoteSchemaDocumentProvider<SchemaDocumentType> provider;
    Document sd;
    sd.Parse("{\"id\": \"http://ignore/blah#/ref\", \"type\": \"object\", \"properties\": {\"myInt\": {\"$ref\": \"http://localhost:1234/subSchemas.json#/integer\"}}}");
    SchemaDocumentType s(sd, 0, 0, &provider);
    typedef GenericSchemaValidator<SchemaDocumentType, BaseReaderHandler<UTF8<> >, MemoryPoolAllocator<> > SchemaValidatorType;
    typedef GenericPointer<Value, MemoryPoolAllocator<> > PointerType;
    INVALIDATE_(s, "{\"myInt\": null}", "/integer", "type", "/myInt",
        "{ \"type\": {"
        "    \"errorCode\": 20,"
        "    \"instanceRef\": \"#/myInt\","
        "    \"schemaRef\": \"http://localhost:1234/subSchemas.json#/integer\","
        "    \"expected\": [\"integer\"], \"actual\": \"null\""
        "}}",
        kValidateDefaultFlags, SchemaValidatorType, PointerType);
}

// Merge with id where $ref is a relative path
TEST(SchemaValidator, Ref_remote_change_resolution_scope_relative_path) {
    typedef GenericSchemaDocument<Value, MemoryPoolAllocator<> > SchemaDocumentType;
    RemoteSchemaDocumentProvider<SchemaDocumentType> provider;
    Document sd;
    sd.Parse("{\"id\": \"http://localhost:1234/\", \"type\": \"object\", \"properties\": {\"myInt\": {\"$ref\": \"subSchemas.json#/integer\"}}}");
    SchemaDocumentType s(sd, 0, 0, &provider);
    typedef GenericSchemaValidator<SchemaDocumentType, BaseReaderHandler<UTF8<> >, MemoryPoolAllocator<> > SchemaValidatorType;
    typedef GenericPointer<Value, MemoryPoolAllocator<> > PointerType;
    INVALIDATE_(s, "{\"myInt\": null}", "/integer", "type", "/myInt",
        "{ \"type\": {"
        "    \"errorCode\": 20,"
        "    \"instanceRef\": \"#/myInt\","
        "    \"schemaRef\": \"http://localhost:1234/subSchemas.json#/integer\","
        "    \"expected\": [\"integer\"], \"actual\": \"null\""
        "}}",
        kValidateDefaultFlags, SchemaValidatorType, PointerType);
}

// Merge with id where $ref is an absolute path
TEST(SchemaValidator, Ref_remote_change_resolution_scope_absolute_path) {
    typedef GenericSchemaDocument<Value, MemoryPoolAllocator<> > SchemaDocumentType;
    RemoteSchemaDocumentProvider<SchemaDocumentType> provider;
    Document sd;
    sd.Parse("{\"id\": \"http://localhost:1234/xxxx\", \"type\": \"object\", \"properties\": {\"myInt\": {\"$ref\": \"/subSchemas.json#/integer\"}}}");
    SchemaDocumentType s(sd, 0, 0, &provider);
    typedef GenericSchemaValidator<SchemaDocumentType, BaseReaderHandler<UTF8<> >, MemoryPoolAllocator<> > SchemaValidatorType;
    typedef GenericPointer<Value, MemoryPoolAllocator<> > PointerType;
    INVALIDATE_(s, "{\"myInt\": null}", "/integer", "type", "/myInt",
        "{ \"type\": {"
        "    \"errorCode\": 20,"
        "    \"instanceRef\": \"#/myInt\","
        "    \"schemaRef\": \"http://localhost:1234/subSchemas.json#/integer\","
        "    \"expected\": [\"integer\"], \"actual\": \"null\""
        "}}",
        kValidateDefaultFlags, SchemaValidatorType, PointerType);
}

// Merge with id where $ref is an absolute path, and the document has a base URI
TEST(SchemaValidator, Ref_remote_change_resolution_scope_absolute_path_document) {
    typedef GenericSchemaDocument<Value, MemoryPoolAllocator<> > SchemaDocumentType;
    RemoteSchemaDocumentProvider<SchemaDocumentType> provider;
    Document sd;
    sd.Parse("{\"type\": \"object\", \"properties\": {\"myInt\": {\"$ref\": \"/subSchemas.json#/integer\"}}}");
    SchemaDocumentType s(sd, "http://localhost:1234/xxxx", 26, &provider);
    typedef GenericSchemaValidator<SchemaDocumentType, BaseReaderHandler<UTF8<> >, MemoryPoolAllocator<> > SchemaValidatorType;
    typedef GenericPointer<Value, MemoryPoolAllocator<> > PointerType;
    INVALIDATE_(s, "{\"myInt\": null}", "/integer", "type", "/myInt",
        "{ \"type\": {"
        "    \"errorCode\": 20,"
        "    \"instanceRef\": \"#/myInt\","
        "    \"schemaRef\": \"http://localhost:1234/subSchemas.json#/integer\","
        "    \"expected\": [\"integer\"], \"actual\": \"null\""
        "}}",
        kValidateDefaultFlags, SchemaValidatorType, PointerType);
}

// $ref is a non-JSON pointer fragment and there a matching id
TEST(SchemaValidator, Ref_internal_id_1) {
    typedef GenericSchemaDocument<Value, MemoryPoolAllocator<> > SchemaDocumentType;
    Document sd;
    sd.Parse("{\"type\": \"object\", \"properties\": {\"myInt1\": {\"$ref\": \"#myId\"}, \"myStr\": {\"type\": \"string\", \"id\": \"#myStrId\"}, \"myInt2\": {\"type\": \"integer\", \"id\": \"#myId\"}}}");
    SchemaDocumentType s(sd);
    typedef GenericSchemaValidator<SchemaDocumentType, BaseReaderHandler<UTF8<> >, MemoryPoolAllocator<> > SchemaValidatorType;
    typedef GenericPointer<Value, MemoryPoolAllocator<> > PointerType;
    INVALIDATE_(s, "{\"myInt1\": null}", "/properties/myInt2", "type", "/myInt1",
        "{ \"type\": {"
        "    \"errorCode\": 20,"
        "    \"instanceRef\": \"#/myInt1\","
        "    \"schemaRef\": \"#/properties/myInt2\","
        "    \"expected\": [\"integer\"], \"actual\": \"null\""
        "}}",
        kValidateDefaultFlags, SchemaValidatorType, PointerType);
}

// $ref is a non-JSON pointer fragment and there are two matching ids so we take the first
TEST(SchemaValidator, Ref_internal_id_2) {
    typedef GenericSchemaDocument<Value, MemoryPoolAllocator<> > SchemaDocumentType;
    Document sd;
    sd.Parse("{\"type\": \"object\", \"properties\": {\"myInt1\": {\"$ref\": \"#myId\"}, \"myInt2\": {\"type\": \"integer\", \"id\": \"#myId\"}, \"myStr\": {\"type\": \"string\", \"id\": \"#myId\"}}}");
    SchemaDocumentType s(sd);
    typedef GenericSchemaValidator<SchemaDocumentType, BaseReaderHandler<UTF8<> >, MemoryPoolAllocator<> > SchemaValidatorType;
    typedef GenericPointer<Value, MemoryPoolAllocator<> > PointerType;
    INVALIDATE_(s, "{\"myInt1\": null}", "/properties/myInt2", "type", "/myInt1",
        "{ \"type\": {"
        "    \"errorCode\": 20,"
        "    \"instanceRef\": \"#/myInt1\","
        "    \"schemaRef\": \"#/properties/myInt2\","
        "    \"expected\": [\"integer\"], \"actual\": \"null\""
        "}}",
        kValidateDefaultFlags, SchemaValidatorType, PointerType);
}

// $ref is a non-JSON pointer fragment and there is a matching id within array
TEST(SchemaValidator, Ref_internal_id_in_array) {
    typedef GenericSchemaDocument<Value, MemoryPoolAllocator<> > SchemaDocumentType;
    Document sd;
    sd.Parse("{\"type\": \"object\", \"properties\": {\"myInt1\": {\"$ref\": \"#myId\"}, \"myInt2\": {\"anyOf\": [{\"type\": \"string\", \"id\": \"#myStrId\"}, {\"type\": \"integer\", \"id\": \"#myId\"}]}}}");
    SchemaDocumentType s(sd);
    typedef GenericSchemaValidator<SchemaDocumentType, BaseReaderHandler<UTF8<> >, MemoryPoolAllocator<> > SchemaValidatorType;
    typedef GenericPointer<Value, MemoryPoolAllocator<> > PointerType;
    INVALIDATE_(s, "{\"myInt1\": null}", "/properties/myInt2/anyOf/1", "type", "/myInt1",
        "{ \"type\": {"
        "    \"errorCode\": 20,"
        "    \"instanceRef\": \"#/myInt1\","
        "    \"schemaRef\": \"#/properties/myInt2/anyOf/1\","
        "    \"expected\": [\"integer\"], \"actual\": \"null\""
        "}}",
        kValidateDefaultFlags, SchemaValidatorType, PointerType);
}

// $ref is a non-JSON pointer fragment and there is a matching id, and the schema is embedded in the document
TEST(SchemaValidator, Ref_internal_id_and_schema_pointer) {
    typedef GenericSchemaDocument<Value, MemoryPoolAllocator<> > SchemaDocumentType;
    Document sd;
    sd.Parse("{ \"schema\": {\"type\": \"object\", \"properties\": {\"myInt1\": {\"$ref\": \"#myId\"}, \"myInt2\": {\"anyOf\": [{\"type\": \"integer\", \"id\": \"#myId\"}]}}}}");
    typedef GenericPointer<Value, MemoryPoolAllocator<> > PointerType;
    SchemaDocumentType s(sd, 0, 0, 0, 0, PointerType("/schema"));
    typedef GenericSchemaValidator<SchemaDocumentType, BaseReaderHandler<UTF8<> >, MemoryPoolAllocator<> > SchemaValidatorType;
    INVALIDATE_(s, "{\"myInt1\": null}", "/schema/properties/myInt2/anyOf/0", "type", "/myInt1",
        "{ \"type\": {"
        "    \"errorCode\": 20,"
        "    \"instanceRef\": \"#/myInt1\","
        "    \"schemaRef\": \"#/schema/properties/myInt2/anyOf/0\","
        "    \"expected\": [\"integer\"], \"actual\": \"null\""
        "}}",
        kValidateDefaultFlags, SchemaValidatorType, PointerType);
}

// Test that $refs are correctly resolved when intermediate multiple ids are present
// Includes $ref to a part of the document with a different in-scope id, which also contains $ref..
TEST(SchemaValidator, Ref_internal_multiple_ids) {
    typedef GenericSchemaDocument<Value, MemoryPoolAllocator<> > SchemaDocumentType;
    //RemoteSchemaDocumentProvider<SchemaDocumentType> provider;
    CrtAllocator allocator;
    char* schema = ReadFile("unittestschema/idandref.json", allocator);
    Document sd;
    sd.Parse(schema);
    ASSERT_FALSE(sd.HasParseError());
    SchemaDocumentType s(sd, "http://xyz", 10/*, &provider*/);
    typedef GenericSchemaValidator<SchemaDocumentType, BaseReaderHandler<UTF8<> >, MemoryPoolAllocator<> > SchemaValidatorType;
    typedef GenericPointer<Value, MemoryPoolAllocator<> > PointerType;
    INVALIDATE_(s, "{\"PA1\": \"s\", \"PA2\": \"t\", \"PA3\": \"r\", \"PX1\": 1, \"PX2Y\": 2, \"PX3Z\": 3, \"PX4\": 4, \"PX5\": 5, \"PX6\": 6, \"PX7W\": 7, \"PX8N\": { \"NX\": 8}}", "#", "errors", "#",
        "{ \"type\": ["
        "    {\"errorCode\": 20, \"instanceRef\": \"#/PA1\", \"schemaRef\": \"http://xyz#/definitions/A\", \"expected\": [\"integer\"], \"actual\": \"string\"},"
        "    {\"errorCode\": 20, \"instanceRef\": \"#/PA2\", \"schemaRef\": \"http://xyz#/definitions/A\", \"expected\": [\"integer\"], \"actual\": \"string\"},"
        "    {\"errorCode\": 20, \"instanceRef\": \"#/PA3\", \"schemaRef\": \"http://xyz#/definitions/A\", \"expected\": [\"integer\"], \"actual\": \"string\"},"
        "    {\"errorCode\": 20, \"instanceRef\": \"#/PX1\", \"schemaRef\": \"http://xyz#/definitions/B/definitions/X\", \"expected\": [\"boolean\"], \"actual\": \"integer\"},"
        "    {\"errorCode\": 20, \"instanceRef\": \"#/PX2Y\", \"schemaRef\": \"http://xyz#/definitions/B/definitions/X\", \"expected\": [\"boolean\"], \"actual\": \"integer\"},"
        "    {\"errorCode\": 20, \"instanceRef\": \"#/PX3Z\", \"schemaRef\": \"http://xyz#/definitions/B/definitions/X\", \"expected\": [\"boolean\"], \"actual\": \"integer\"},"
        "    {\"errorCode\": 20, \"instanceRef\": \"#/PX4\", \"schemaRef\": \"http://xyz#/definitions/B/definitions/X\", \"expected\": [\"boolean\"], \"actual\": \"integer\"},"
        "    {\"errorCode\": 20, \"instanceRef\": \"#/PX5\", \"schemaRef\": \"http://xyz#/definitions/B/definitions/X\", \"expected\": [\"boolean\"], \"actual\": \"integer\"},"
        "    {\"errorCode\": 20, \"instanceRef\": \"#/PX6\", \"schemaRef\": \"http://xyz#/definitions/B/definitions/X\", \"expected\": [\"boolean\"], \"actual\": \"integer\"},"
        "    {\"errorCode\": 20, \"instanceRef\": \"#/PX7W\", \"schemaRef\": \"http://xyz#/definitions/B/definitions/X\", \"expected\": [\"boolean\"], \"actual\": \"integer\"},"
        "    {\"errorCode\": 20, \"instanceRef\": \"#/PX8N/NX\", \"schemaRef\": \"http://xyz#/definitions/B/definitions/X\", \"expected\": [\"boolean\"], \"actual\": \"integer\"}"
        "]}",
        kValidateDefaultFlags | kValidateContinueOnErrorFlag, SchemaValidatorType, PointerType);
    CrtAllocator::Free(schema);
}

TEST(SchemaValidator, Ref_remote_issue1210) {
    class SchemaDocumentProvider : public IRemoteSchemaDocumentProvider {
        SchemaDocument** collection;

        // Dummy private copy constructor & assignment operator.
        // Function bodies added so that they compile in MSVC 2019.
        SchemaDocumentProvider(const SchemaDocumentProvider&) : collection(NULL) {
        }
        SchemaDocumentProvider& operator=(const SchemaDocumentProvider&) {
            return *this;
        }

        public:
          SchemaDocumentProvider(SchemaDocument** collection) : collection(collection) { }
          virtual const SchemaDocument* GetRemoteDocument(const char* uri, SizeType length) {
            int i = 0;
            while (collection[i] && SchemaDocument::SValue(uri, length) != collection[i]->GetURI()) ++i;
            return collection[i];
          }
    };
    SchemaDocument* collection[] = { 0, 0, 0 };
    SchemaDocumentProvider provider(collection);

    Document x, y, z;
    x.Parse("{\"properties\":{\"country\":{\"$ref\":\"y.json#/definitions/country_remote\"}},\"type\":\"object\"}");
    y.Parse("{\"definitions\":{\"country_remote\":{\"$ref\":\"z.json#/definitions/country_list\"}}}");
    z.Parse("{\"definitions\":{\"country_list\":{\"enum\":[\"US\"]}}}");

    SchemaDocument sz(z, "z.json", 6, &provider);
    collection[0] = &sz;
    SchemaDocument sy(y, "y.json", 6, &provider);
    collection[1] = &sy;
    SchemaDocument sx(x, "x.json", 6, &provider);

    VALIDATE(sx, "{\"country\":\"UK\"}", false);
    VALIDATE(sx, "{\"country\":\"US\"}", true);
}

// Test that when kValidateContinueOnErrorFlag is set, all errors are reported.
TEST(SchemaValidator, ContinueOnErrors) {
    CrtAllocator allocator;
    char* schema = ReadFile("unittestschema/address.json", allocator);
    Document sd;
    sd.Parse(schema);
    ASSERT_FALSE(sd.HasParseError());
    SchemaDocument s(sd);
    VALIDATE(s, "{\"version\": 1.0, \"address\": {\"number\": 24, \"street1\": \"The Woodlands\", \"street3\": \"Ham\", \"city\": \"Romsey\", \"area\": \"Kent\", \"country\": \"UK\", \"postcode\": \"SO51 0GP\"}, \"phones\": [\"0111-222333\", \"0777-666888\"], \"names\": [\"Fred\", \"Bloggs\"]}", true);
    INVALIDATE_(s, "{\"version\": 1.01, \"address\": {\"number\": 0, \"street2\": false,  \"street3\": \"Ham\", \"city\": \"RomseyTownFC\", \"area\": \"BC\", \"country\": \"USA\", \"postcode\": \"999ABC\"}, \"phones\": [], \"planet\": \"Earth\", \"extra\": {\"S_xxx\": 123}}", "#", "errors", "#",
        "{ \"multipleOf\": {"
        "    \"errorCode\": 1, \"instanceRef\": \"#/version\", \"schemaRef\": \"#/definitions/decimal_type\", \"expected\": 1.0, \"actual\": 1.01"
        "  },"
        "  \"minimum\": {"
        "    \"errorCode\": 5, \"instanceRef\": \"#/address/number\", \"schemaRef\": \"#/definitions/positiveInt_type\", \"expected\": 0, \"actual\": 0, \"exclusiveMinimum\": true"
        "  },"
        "  \"type\": ["
        "    {\"expected\": [\"null\", \"string\"], \"actual\": \"boolean\", \"errorCode\": 20, \"instanceRef\": \"#/address/street2\", \"schemaRef\": \"#/definitions/address_type/properties/street2\"},"
        "    {\"expected\": [\"string\"], \"actual\": \"integer\", \"errorCode\": 20, \"instanceRef\": \"#/extra/S_xxx\", \"schemaRef\": \"#/properties/extra/patternProperties/%5ES_\"}"
        "  ],"
        "  \"maxLength\": {"
        "    \"actual\": \"RomseyTownFC\", \"expected\": 10, \"errorCode\": 6, \"instanceRef\": \"#/address/city\", \"schemaRef\": \"#/definitions/address_type/properties/city\""
        "  },"
        "  \"anyOf\": {"
        "    \"errors\":["
        "      {\"pattern\": {\"actual\": \"999ABC\", \"errorCode\": 8, \"instanceRef\": \"#/address/postcode\", \"schemaRef\": \"#/definitions/address_type/properties/postcode/anyOf/0\"}},"
        "      {\"pattern\": {\"actual\": \"999ABC\", \"errorCode\": 8, \"instanceRef\": \"#/address/postcode\", \"schemaRef\": \"#/definitions/address_type/properties/postcode/anyOf/1\"}}"
        "    ],"
        "    \"errorCode\": 24, \"instanceRef\": \"#/address/postcode\", \"schemaRef\": \"#/definitions/address_type/properties/postcode\""
        "  },"
        "  \"allOf\": {"
        "    \"errors\":["
        "      {\"enum\":{\"errorCode\":19,\"instanceRef\":\"#/address/country\",\"schemaRef\":\"#/definitions/country_type\"}}"
        "    ],"
        "    \"errorCode\":23,\"instanceRef\":\"#/address/country\",\"schemaRef\":\"#/definitions/address_type/properties/country\""
        "  },"
        "  \"minItems\": {"
        "    \"actual\": 0, \"expected\": 1, \"errorCode\": 10, \"instanceRef\": \"#/phones\", \"schemaRef\": \"#/properties/phones\""
        "  },"
        "  \"additionalProperties\": {"
        "    \"disallowed\": \"planet\", \"errorCode\": 16, \"instanceRef\": \"#\", \"schemaRef\": \"#\""
        "  },"
        "  \"required\": {"
        "    \"missing\": [\"street1\"], \"errorCode\": 15, \"instanceRef\": \"#/address\", \"schemaRef\": \"#/definitions/address_type\""
        "  }"
        "}",
        kValidateDefaultFlags | kValidateContinueOnErrorFlag, SchemaValidator, Pointer);
    INVALIDATE_(s, "{\"address\": {\"number\": 200, \"street1\": {}, \"street3\": null, \"city\": \"Rom\", \"area\": \"Dorset\", \"postcode\": \"SO51 0GP\"}, \"phones\": [\"0111-222333\", \"0777-666888\", \"0777-666888\"], \"names\": [\"Fred\", \"S\", \"M\", \"Bloggs\"]}", "#", "errors", "#",
        "{ \"maximum\": {"
        "    \"errorCode\": 3, \"instanceRef\": \"#/address/number\", \"schemaRef\": \"#/definitions/positiveInt_type\", \"expected\": 100, \"actual\": 200, \"exclusiveMaximum\": true"
        "  },"
        "  \"type\": {"
        "    \"expected\": [\"string\"], \"actual\": \"object\", \"errorCode\": 20, \"instanceRef\": \"#/address/street1\", \"schemaRef\": \"#/definitions/address_type/properties/street1\""
        "  },"
        "  \"not\": {"
        "    \"errorCode\": 25, \"instanceRef\": \"#/address/street3\", \"schemaRef\": \"#/definitions/address_type/properties/street3\""
        "  },"
        "  \"minLength\": {"
        "    \"actual\": \"Rom\", \"expected\": 4, \"errorCode\": 7, \"instanceRef\": \"#/address/city\", \"schemaRef\": \"#/definitions/address_type/properties/city\""
        "  },"
        "  \"maxItems\": {"
        "    \"actual\": 3, \"expected\": 2, \"errorCode\": 9, \"instanceRef\": \"#/phones\", \"schemaRef\": \"#/properties/phones\""
        "  },"
        "  \"uniqueItems\": {"
        "    \"duplicates\": [1, 2], \"errorCode\": 11, \"instanceRef\": \"#/phones\", \"schemaRef\": \"#/properties/phones\""
        "  },"
        "  \"minProperties\": {\"actual\": 6, \"expected\": 7, \"errorCode\": 14, \"instanceRef\": \"#/address\", \"schemaRef\": \"#/definitions/address_type\""
        "  },"
        "  \"additionalItems\": ["
        "    {\"disallowed\": 2, \"errorCode\": 12, \"instanceRef\": \"#/names\", \"schemaRef\": \"#/properties/names\"},"
        "    {\"disallowed\": 3, \"errorCode\": 12, \"instanceRef\": \"#/names\", \"schemaRef\": \"#/properties/names\"}"
        "  ],"
        "  \"dependencies\": {"
        "    \"errors\": {"
        "      \"address\": {\"required\": {\"missing\": [\"version\"], \"errorCode\": 15, \"instanceRef\": \"#\", \"schemaRef\": \"#/dependencies/address\"}},"
        "      \"names\": {\"required\": {\"missing\": [\"version\"], \"errorCode\": 15, \"instanceRef\": \"#\", \"schemaRef\": \"#/dependencies/names\"}}"
        "    },"
        "    \"errorCode\": 18, \"instanceRef\": \"#\", \"schemaRef\": \"#\""
        "  },"
        "  \"oneOf\": {"
        "    \"errors\": ["
        "      {\"enum\": {\"errorCode\": 19, \"instanceRef\": \"#/address/area\", \"schemaRef\": \"#/definitions/county_type\"}},"
        "      {\"enum\": {\"errorCode\": 19, \"instanceRef\": \"#/address/area\", \"schemaRef\": \"#/definitions/province_type\"}}"
        "    ],"
        "    \"errorCode\": 21, \"instanceRef\": \"#/address/area\", \"schemaRef\": \"#/definitions/address_type/properties/area\""
        "  }"
        "}",
        kValidateDefaultFlags | kValidateContinueOnErrorFlag, SchemaValidator, Pointer);

        CrtAllocator::Free(schema);
}

// Test that when kValidateContinueOnErrorFlag is set, it is not propagated to oneOf sub-validator so we only get the first error.
TEST(SchemaValidator, ContinueOnErrors_OneOf) {
    typedef GenericSchemaDocument<Value, MemoryPoolAllocator<> > SchemaDocumentType;
    RemoteSchemaDocumentProvider<SchemaDocumentType> provider;
    CrtAllocator allocator;
    char* schema = ReadFile("unittestschema/oneOf_address.json", allocator);
    Document sd;
    sd.Parse(schema);
    ASSERT_FALSE(sd.HasParseError());
    SchemaDocumentType s(sd, 0, 0, &provider);
    typedef GenericSchemaValidator<SchemaDocumentType, BaseReaderHandler<UTF8<> >, MemoryPoolAllocator<> > SchemaValidatorType;
    typedef GenericPointer<Value, MemoryPoolAllocator<> > PointerType;
    INVALIDATE_(s, "{\"version\": 1.01, \"address\": {\"number\": 0, \"street2\": false,  \"street3\": \"Ham\", \"city\": \"RomseyTownFC\", \"area\": \"BC\", \"country\": \"USA\", \"postcode\": \"999ABC\"}, \"phones\": [], \"planet\": \"Earth\", \"extra\": {\"S_xxx\": 123}}", "#", "errors", "#",
        "{ \"oneOf\": {"
        "    \"errors\": [{"
        "      \"multipleOf\": {"
        "        \"errorCode\": 1, \"instanceRef\": \"#/version\", \"schemaRef\": \"http://localhost:1234/address.json#/definitions/decimal_type\", \"expected\": 1.0, \"actual\": 1.01"
        "      }"
        "    }],"
        "    \"errorCode\": 21, \"instanceRef\": \"#\", \"schemaRef\": \"#\""
        "  }"
        "}",
        kValidateDefaultFlags | kValidateContinueOnErrorFlag, SchemaValidatorType, PointerType);
    CrtAllocator::Free(schema);
}

// Test that when kValidateContinueOnErrorFlag is set, it is not propagated to allOf sub-validator so we only get the first error.
TEST(SchemaValidator, ContinueOnErrors_AllOf) {
    typedef GenericSchemaDocument<Value, MemoryPoolAllocator<> > SchemaDocumentType;
    RemoteSchemaDocumentProvider<SchemaDocumentType> provider;
    CrtAllocator allocator;
    char* schema = ReadFile("unittestschema/allOf_address.json", allocator);
    Document sd;
    sd.Parse(schema);
    ASSERT_FALSE(sd.HasParseError());
    SchemaDocumentType s(sd, 0, 0, &provider);
    typedef GenericSchemaValidator<SchemaDocumentType, BaseReaderHandler<UTF8<> >, MemoryPoolAllocator<> > SchemaValidatorType;
    typedef GenericPointer<Value, MemoryPoolAllocator<> > PointerType;
    INVALIDATE_(s, "{\"version\": 1.01, \"address\": {\"number\": 0, \"street2\": false,  \"street3\": \"Ham\", \"city\": \"RomseyTownFC\", \"area\": \"BC\", \"country\": \"USA\", \"postcode\": \"999ABC\"}, \"phones\": [], \"planet\": \"Earth\", \"extra\": {\"S_xxx\": 123}}", "#", "errors", "#",
        "{ \"allOf\": {"
        "    \"errors\": [{"
        "      \"multipleOf\": {"
        "        \"errorCode\": 1, \"instanceRef\": \"#/version\", \"schemaRef\": \"http://localhost:1234/address.json#/definitions/decimal_type\", \"expected\": 1.0, \"actual\": 1.01"
        "      }"
        "    }],"
        "    \"errorCode\": 23, \"instanceRef\": \"#\", \"schemaRef\": \"#\""
        "  }"
        "}",
        kValidateDefaultFlags | kValidateContinueOnErrorFlag, SchemaValidatorType, PointerType);
    CrtAllocator::Free(schema);
}

// Test that when kValidateContinueOnErrorFlag is set, it is not propagated to anyOf sub-validator so we only get the first error.
TEST(SchemaValidator, ContinueOnErrors_AnyOf) {
    typedef GenericSchemaDocument<Value, MemoryPoolAllocator<> > SchemaDocumentType;
    RemoteSchemaDocumentProvider<SchemaDocumentType> provider;
    CrtAllocator allocator;
    char* schema = ReadFile("unittestschema/anyOf_address.json", allocator);
    Document sd;
    sd.Parse(schema);
    ASSERT_FALSE(sd.HasParseError());
    SchemaDocumentType s(sd, 0, 0, &provider);
    typedef GenericSchemaValidator<SchemaDocumentType, BaseReaderHandler<UTF8<> >, MemoryPoolAllocator<> > SchemaValidatorType;
    typedef GenericPointer<Value, MemoryPoolAllocator<> > PointerType;
    INVALIDATE_(s, "{\"version\": 1.01, \"address\": {\"number\": 0, \"street2\": false,  \"street3\": \"Ham\", \"city\": \"RomseyTownFC\", \"area\": \"BC\", \"country\": \"USA\", \"postcode\": \"999ABC\"}, \"phones\": [], \"planet\": \"Earth\", \"extra\": {\"S_xxx\": 123}}", "#", "errors", "#",
        "{ \"anyOf\": {"
        "    \"errors\": [{"
        "      \"multipleOf\": {"
        "        \"errorCode\": 1, \"instanceRef\": \"#/version\", \"schemaRef\": \"http://localhost:1234/address.json#/definitions/decimal_type\", \"expected\": 1.0, \"actual\": 1.01"
        "      }"
        "    }],"
        "    \"errorCode\": 24, \"instanceRef\": \"#\", \"schemaRef\": \"#\""
        "  }"
        "}",
        kValidateDefaultFlags | kValidateContinueOnErrorFlag, SchemaValidatorType, PointerType);

    CrtAllocator::Free(schema);
}

// Test that when kValidateContinueOnErrorFlag is set, arrays with uniqueItems:true are correctly processed when an item is invalid.
// This tests that we don't blow up if a hasher does not get created.
TEST(SchemaValidator, ContinueOnErrors_UniqueItems) {
    CrtAllocator allocator;
    char* schema = ReadFile("unittestschema/address.json", allocator);
    Document sd;
    sd.Parse(schema);
    ASSERT_FALSE(sd.HasParseError());
    SchemaDocument s(sd);
    VALIDATE(s, "{\"phones\":[\"12-34\",\"56-78\"]}", true);
    INVALIDATE_(s, "{\"phones\":[\"12-34\",\"12-34\"]}", "#", "errors", "#",
        "{\"uniqueItems\": {\"duplicates\": [0,1], \"errorCode\": 11, \"instanceRef\": \"#/phones\", \"schemaRef\": \"#/properties/phones\"}}",
        kValidateDefaultFlags | kValidateContinueOnErrorFlag, SchemaValidator, Pointer);
    INVALIDATE_(s, "{\"phones\":[\"ab-34\",\"cd-78\"]}", "#", "errors", "#",
        "{\"pattern\": ["
        "  {\"actual\": \"ab-34\", \"errorCode\": 8, \"instanceRef\": \"#/phones/0\", \"schemaRef\": \"#/definitions/phone_type\"},"
        "  {\"actual\": \"cd-78\", \"errorCode\": 8, \"instanceRef\": \"#/phones/1\", \"schemaRef\": \"#/definitions/phone_type\"}"
        "]}",
        kValidateDefaultFlags | kValidateContinueOnErrorFlag, SchemaValidator, Pointer);
    CrtAllocator::Free(schema);
}

// Test that when kValidateContinueOnErrorFlag is set, an enum field is correctly processed when it has an invalid value.
// This tests that we don't blow up if a hasher does not get created.
TEST(SchemaValidator, ContinueOnErrors_Enum) {
    CrtAllocator allocator;
    char* schema = ReadFile("unittestschema/address.json", allocator);
    Document sd;
    sd.Parse(schema);
    ASSERT_FALSE(sd.HasParseError());
    SchemaDocument s(sd);
    VALIDATE(s, "{\"gender\":\"M\"}", true);
    INVALIDATE_(s, "{\"gender\":\"X\"}", "#", "errors", "#",
        "{\"enum\": {\"errorCode\": 19, \"instanceRef\": \"#/gender\", \"schemaRef\": \"#/properties/gender\"}}",
        kValidateDefaultFlags | kValidateContinueOnErrorFlag, SchemaValidator, Pointer);
    INVALIDATE_(s, "{\"gender\":1}", "#", "errors", "#",
        "{\"type\": {\"expected\":[\"string\"], \"actual\": \"integer\", \"errorCode\": 20, \"instanceRef\": \"#/gender\", \"schemaRef\": \"#/properties/gender\"}}",
        kValidateDefaultFlags | kValidateContinueOnErrorFlag, SchemaValidator, Pointer);
    CrtAllocator::Free(schema);
}

// Test that when kValidateContinueOnErrorFlag is set, an array appearing for an object property is handled
// This tests that we don't blow up when there is a type mismatch.
TEST(SchemaValidator, ContinueOnErrors_RogueArray) {
    CrtAllocator allocator;
    char* schema = ReadFile("unittestschema/address.json", allocator);
    Document sd;
    sd.Parse(schema);
    ASSERT_FALSE(sd.HasParseError());
    SchemaDocument s(sd);
    INVALIDATE_(s, "{\"address\":[{\"number\": 0}]}", "#", "errors", "#",
        "{\"type\": {\"expected\":[\"object\"], \"actual\": \"array\", \"errorCode\": 20, \"instanceRef\": \"#/address\", \"schemaRef\": \"#/definitions/address_type\"},"
        "  \"dependencies\": {"
        "    \"errors\": {"
        "      \"address\": {\"required\": {\"missing\": [\"version\"], \"errorCode\": 15, \"instanceRef\": \"#\", \"schemaRef\": \"#/dependencies/address\"}}"
        "    },\"errorCode\": 18, \"instanceRef\": \"#\", \"schemaRef\": \"#\"}}",
        kValidateDefaultFlags | kValidateContinueOnErrorFlag, SchemaValidator, Pointer);
    CrtAllocator::Free(schema);
}

// Test that when kValidateContinueOnErrorFlag is set, an object appearing for an array property is handled
// This tests that we don't blow up when there is a type mismatch.
TEST(SchemaValidator, ContinueOnErrors_RogueObject) {
    CrtAllocator allocator;
    char* schema = ReadFile("unittestschema/address.json", allocator);
    Document sd;
    sd.Parse(schema);
    ASSERT_FALSE(sd.HasParseError());
    SchemaDocument s(sd);
    INVALIDATE_(s, "{\"phones\":{\"number\": 0}}", "#", "errors", "#",
        "{\"type\": {\"expected\":[\"array\"], \"actual\": \"object\", \"errorCode\": 20, \"instanceRef\": \"#/phones\", \"schemaRef\": \"#/properties/phones\"}}",
        kValidateDefaultFlags | kValidateContinueOnErrorFlag, SchemaValidator, Pointer);
    CrtAllocator::Free(schema);
}

// Test that when kValidateContinueOnErrorFlag is set, a string appearing for an array or object property is handled
// This tests that we don't blow up when there is a type mismatch.
TEST(SchemaValidator, ContinueOnErrors_RogueString) {
    CrtAllocator allocator;
    char* schema = ReadFile("unittestschema/address.json", allocator);
    Document sd;
    sd.Parse(schema);
    ASSERT_FALSE(sd.HasParseError());
    SchemaDocument s(sd);
    INVALIDATE_(s, "{\"address\":\"number\"}", "#", "errors", "#",
        "{\"type\": {\"expected\":[\"object\"], \"actual\": \"string\", \"errorCode\": 20, \"instanceRef\": \"#/address\", \"schemaRef\": \"#/definitions/address_type\"},"
        "  \"dependencies\": {"
        "    \"errors\": {"
        "      \"address\": {\"required\": {\"missing\": [\"version\"], \"errorCode\": 15, \"instanceRef\": \"#\", \"schemaRef\": \"#/dependencies/address\"}}"
        "    },\"errorCode\": 18, \"instanceRef\": \"#\", \"schemaRef\": \"#\"}}",
        kValidateDefaultFlags | kValidateContinueOnErrorFlag, SchemaValidator, Pointer);
    INVALIDATE_(s, "{\"phones\":\"number\"}", "#", "errors", "#",
        "{\"type\": {\"expected\":[\"array\"], \"actual\": \"string\", \"errorCode\": 20, \"instanceRef\": \"#/phones\", \"schemaRef\": \"#/properties/phones\"}}",
        kValidateDefaultFlags | kValidateContinueOnErrorFlag, SchemaValidator, Pointer);
    CrtAllocator::Free(schema);
}

// Test that when kValidateContinueOnErrorFlag is set, an incorrect simple type with a sub-schema is handled correctly.
// This tests that we don't blow up when there is a type mismatch but there is a sub-schema present
TEST(SchemaValidator, ContinueOnErrors_Issue2) {
    Document sd;
    sd.Parse("{\"type\":\"string\", \"anyOf\":[{\"maxLength\":2}]}");
    ASSERT_FALSE(sd.HasParseError());
    SchemaDocument s(sd);
    VALIDATE(s, "\"AB\"", true);
    INVALIDATE_(s, "\"ABC\"", "#", "errors", "#",
        "{ \"anyOf\": {"
        "    \"errors\": [{"
        "      \"maxLength\": {"
        "        \"errorCode\": 6, \"instanceRef\": \"#\", \"schemaRef\": \"#/anyOf/0\", \"expected\": 2, \"actual\": \"ABC\""
        "      }"
        "    }],"
        "    \"errorCode\": 24, \"instanceRef\": \"#\", \"schemaRef\": \"#\""
        "  }"
        "}",
        kValidateDefaultFlags | kValidateContinueOnErrorFlag, SchemaValidator, Pointer);
    // Invalid type
    INVALIDATE_(s, "333", "#", "errors", "#",
        "{ \"type\": {"
        "    \"errorCode\": 20, \"instanceRef\": \"#\", \"schemaRef\": \"#\", \"expected\": [\"string\"], \"actual\": \"integer\""
        "  }"
        "}",
        kValidateDefaultFlags | kValidateContinueOnErrorFlag, SchemaValidator, Pointer);
}

TEST(SchemaValidator, Schema_UnknownError) {
    ASSERT_TRUE(SchemaValidator::SchemaType::GetValidateErrorKeyword(kValidateErrors).GetString() == std::string("null"));
}

#if defined(_MSC_VER) || defined(__clang__)
RAPIDJSON_DIAG_POP
#endif
