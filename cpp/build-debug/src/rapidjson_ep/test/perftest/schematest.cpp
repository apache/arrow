#include "perftest.h"

#if TEST_RAPIDJSON

#include "rapidjson/schema.h"
#include <ctime>
#include <string>
#include <vector>

#define ARRAY_SIZE(a) sizeof(a) / sizeof(a[0])

using namespace rapidjson;

RAPIDJSON_DIAG_PUSH
#if defined(__GNUC__) && __GNUC__ >= 7
RAPIDJSON_DIAG_OFF(format-overflow)
#endif

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

RAPIDJSON_DIAG_POP

class Schema : public PerfTest {
public:
    Schema() {}

    virtual void SetUp() {
        PerfTest::SetUp();

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

        char jsonBuffer[65536];
        MemoryPoolAllocator<> jsonAllocator(jsonBuffer, sizeof(jsonBuffer));

        for (size_t i = 0; i < ARRAY_SIZE(filenames); i++) {
            char filename[FILENAME_MAX];
            sprintf(filename, "jsonschema/tests/draft4/%s", filenames[i]);
            char* json = ReadFile(filename, jsonAllocator);
            if (!json) {
                printf("json test suite file %s not found", filename);
                return;
            }

            Document d;
            d.Parse(json);
            if (d.HasParseError()) {
                printf("json test suite file %s has parse error", filename);
                return;
            }

            for (Value::ConstValueIterator schemaItr = d.Begin(); schemaItr != d.End(); ++schemaItr) {
                std::string schemaDescription = (*schemaItr)["description"].GetString();
                if (IsExcludeTestSuite(schemaDescription))
                    continue;

                TestSuite* ts = new TestSuite;
                ts->schema = new SchemaDocument((*schemaItr)["schema"]);

                const Value& tests = (*schemaItr)["tests"];
                for (Value::ConstValueIterator testItr = tests.Begin(); testItr != tests.End(); ++testItr) {
                    if (IsExcludeTest(schemaDescription + ", " + (*testItr)["description"].GetString()))
                        continue;

                    Document* d2 = new Document;
                    d2->CopyFrom((*testItr)["data"], d2->GetAllocator());
                    ts->tests.push_back(d2);
                }
                testSuites.push_back(ts);
            }
        }
    }

    virtual void TearDown() {
        PerfTest::TearDown();
        for (TestSuiteList::const_iterator itr = testSuites.begin(); itr != testSuites.end(); ++itr)
            delete *itr;
        testSuites.clear();
    }

private:
    // Using the same exclusion in https://github.com/json-schema/JSON-Schema-Test-Suite
    static bool IsExcludeTestSuite(const std::string& description) {
        const char* excludeTestSuites[] = {
            //lost failing these tests
            "remote ref",
            "remote ref, containing refs itself",
            "fragment within remote ref",
            "ref within remote ref",
            "change resolution scope",
            // these below were added to get jsck in the benchmarks)
            "uniqueItems validation",
            "valid definition",
            "invalid definition"
        };

        for (size_t i = 0; i < ARRAY_SIZE(excludeTestSuites); i++)
            if (excludeTestSuites[i] == description)
                return true;
        return false;
    }

    // Using the same exclusion in https://github.com/json-schema/JSON-Schema-Test-Suite
    static bool IsExcludeTest(const std::string& description) {
        const char* excludeTests[] = {
            //lots of validators fail these
            "invalid definition, invalid definition schema",
            "maxLength validation, two supplementary Unicode code points is long enough",
            "minLength validation, one supplementary Unicode code point is not long enough",
            //this is to get tv4 in the benchmarks
            "heterogeneous enum validation, something else is invalid"
        };

        for (size_t i = 0; i < ARRAY_SIZE(excludeTests); i++)
            if (excludeTests[i] == description)
                return true;
        return false;
    }

    Schema(const Schema&);
    Schema& operator=(const Schema&);

protected:
    typedef std::vector<Document*> DocumentList;

    struct TestSuite {
        TestSuite() : schema() {}
        ~TestSuite() {
            delete schema;
            for (DocumentList::iterator itr = tests.begin(); itr != tests.end(); ++itr)
                delete *itr;
        }
        SchemaDocument* schema;
        DocumentList tests;
    };

    typedef std::vector<TestSuite* > TestSuiteList;
    TestSuiteList testSuites;
};

TEST_F(Schema, TestSuite) {
    char validatorBuffer[65536];
    MemoryPoolAllocator<> validatorAllocator(validatorBuffer, sizeof(validatorBuffer));

    const int trialCount = 100000;
    int testCount = 0;
    clock_t start = clock();
    for (int i = 0; i < trialCount; i++) {
        for (TestSuiteList::const_iterator itr = testSuites.begin(); itr != testSuites.end(); ++itr) {
            const TestSuite& ts = **itr;
            GenericSchemaValidator<SchemaDocument, BaseReaderHandler<UTF8<> >, MemoryPoolAllocator<> >  validator(*ts.schema, &validatorAllocator);
            for (DocumentList::const_iterator testItr = ts.tests.begin(); testItr != ts.tests.end(); ++testItr) {
                validator.Reset();
                (*testItr)->Accept(validator);
                testCount++;
            }
            validatorAllocator.Clear();
        }
    }
    clock_t end = clock();
    double duration = double(end - start) / CLOCKS_PER_SEC;
    printf("%d trials in %f s -> %f trials per sec\n", trialCount, duration, trialCount / duration);
    printf("%d tests per trial\n", testCount / trialCount);
}

#endif
