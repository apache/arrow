/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <limits>

#include <boost/test/included/unit_test_framework.hpp>
#include <boost/test/parameterized_test.hpp>
#include <boost/test/unit_test.hpp>

#include "../impl/json/JsonDom.hh"

#define S(x) #x

namespace avro {
namespace json {

template <typename T>
struct TestData {
  const char* input;
  EntityType type;
  T value;
  const char* output;
};

TestData<bool> boolData[] = {
    {"true", etBool, true, "true"},
    {"false", etBool, false, "false"},
};

TestData<int64_t> longData[] = {
    {"0", etLong, 0, "0"},
    {"-1", etLong, -1, "-1"},
    {"1", etLong, 1, "1"},
    {"9223372036854775807", etLong, 9223372036854775807LL, "9223372036854775807"},
    {"-9223372036854775807", etLong, -9223372036854775807LL, "-9223372036854775807"},
};

TestData<double> doubleData[] = {
    {"0.0", etDouble, 0.0, "0"},
    {"-1.0", etDouble, -1.0, "-1"},
    {"1.0", etDouble, 1.0, "1"},
    {"4.7e3", etDouble, 4700.0, "4700"},
    {"-7.2e-4", etDouble, -0.00072, NULL},
    {"1e4", etDouble, 10000, "10000"},
    {"-1e-4", etDouble, -0.0001, "-0.0001"},
    {"-0e0", etDouble, 0.0, "-0"},
};

TestData<const char*> stringData[] = {
    {"\"\"", etString, "", "\"\""},
    {"\"a\"", etString, "a", "\"a\""},
    {"\"\\U000a\"", etString, "\n", "\"\\n\""},
    {"\"\\u000a\"", etString, "\n", "\"\\n\""},
    {"\"\\\"\"", etString, "\"", "\"\\\"\""},
    {"\"\\/\"", etString, "/", "\"\\/\""},
    {"\"\\u20ac\"", etString, "\xe2\x82\xac", "\"\\u20ac\""},
    {"\"\\u03c0\"", etString, "\xcf\x80", "\"\\u03c0\""},
};

void testBool(const TestData<bool>& d) {
  Entity n = loadEntity(d.input);
  BOOST_CHECK_EQUAL(n.type(), d.type);
  BOOST_CHECK_EQUAL(n.boolValue(), d.value);
  BOOST_CHECK_EQUAL(n.toString(), d.output);
}

void testLong(const TestData<int64_t>& d) {
  Entity n = loadEntity(d.input);
  BOOST_CHECK_EQUAL(n.type(), d.type);
  BOOST_CHECK_EQUAL(n.longValue(), d.value);
  BOOST_CHECK_EQUAL(n.toString(), d.output);
}

void testDouble(const TestData<double>& d) {
  Entity n = loadEntity(d.input);
  BOOST_CHECK_EQUAL(n.type(), d.type);
  BOOST_CHECK_CLOSE(n.doubleValue(), d.value, 1e-10);
  if (d.output != NULL) {
    BOOST_CHECK_EQUAL(n.toString(), d.output);
  }
}

void testString(const TestData<const char*>& d) {
  Entity n = loadEntity(d.input);
  BOOST_CHECK_EQUAL(n.type(), d.type);
  BOOST_CHECK_EQUAL(n.stringValue(), d.value);
  BOOST_CHECK_EQUAL(n.toString(), d.output);
}

static void testNull() {
  Entity n = loadEntity("null");
  BOOST_CHECK_EQUAL(n.type(), etNull);
}

static void testArray0() {
  Entity n = loadEntity("[]");
  BOOST_CHECK_EQUAL(n.type(), etArray);
  const Array& a = n.arrayValue();
  BOOST_CHECK_EQUAL(a.size(), 0);
}

static void testArray1() {
  Entity n = loadEntity("[200]");
  BOOST_CHECK_EQUAL(n.type(), etArray);
  const Array& a = n.arrayValue();
  BOOST_CHECK_EQUAL(a.size(), 1);
  BOOST_CHECK_EQUAL(a[0].type(), etLong);
  BOOST_CHECK_EQUAL(a[0].longValue(), 200ll);
}

static void testArray2() {
  Entity n = loadEntity("[200, \"v100\"]");
  BOOST_CHECK_EQUAL(n.type(), etArray);
  const Array& a = n.arrayValue();
  BOOST_CHECK_EQUAL(a.size(), 2);
  BOOST_CHECK_EQUAL(a[0].type(), etLong);
  BOOST_CHECK_EQUAL(a[0].longValue(), 200ll);
  BOOST_CHECK_EQUAL(a[1].type(), etString);
  BOOST_CHECK_EQUAL(a[1].stringValue(), "v100");
}

static void testObject0() {
  Entity n = loadEntity("{}");
  BOOST_CHECK_EQUAL(n.type(), etObject);
  const Object& m = n.objectValue();
  BOOST_CHECK_EQUAL(m.size(), 0);
}

static void testObject1() {
  Entity n = loadEntity("{\"k1\": 100}");
  BOOST_CHECK_EQUAL(n.type(), etObject);
  const Object& m = n.objectValue();
  BOOST_CHECK_EQUAL(m.size(), 1);
  BOOST_CHECK_EQUAL(m.begin()->first, "k1");
  BOOST_CHECK_EQUAL(m.begin()->second.type(), etLong);
  BOOST_CHECK_EQUAL(m.begin()->second.longValue(), 100ll);
}

static void testObject2() {
  Entity n = loadEntity("{\"k1\": 100, \"k2\": [400, \"v0\"]}");
  BOOST_CHECK_EQUAL(n.type(), etObject);
  const Object& m = n.objectValue();
  BOOST_CHECK_EQUAL(m.size(), 2);

  Object::const_iterator it = m.find("k1");
  BOOST_CHECK(it != m.end());
  BOOST_CHECK_EQUAL(it->second.type(), etLong);
  BOOST_CHECK_EQUAL(m.begin()->second.longValue(), 100ll);

  it = m.find("k2");
  BOOST_CHECK(it != m.end());
  BOOST_CHECK_EQUAL(it->second.type(), etArray);
  const Array& a = it->second.arrayValue();
  BOOST_CHECK_EQUAL(a.size(), 2);
  BOOST_CHECK_EQUAL(a[0].type(), etLong);
  BOOST_CHECK_EQUAL(a[0].longValue(), 400ll);
  BOOST_CHECK_EQUAL(a[1].type(), etString);
  BOOST_CHECK_EQUAL(a[1].stringValue(), "v0");
}

}  // namespace json
}  // namespace avro

#define COUNTOF(x) (sizeof(x) / sizeof(x[0]))

boost::unit_test::test_suite* init_unit_test_suite(int argc, char* argv[]) {
  using namespace boost::unit_test;

  test_suite* ts = BOOST_TEST_SUITE("Avro C++ unit tests for json routines");

  ts->add(BOOST_TEST_CASE(&avro::json::testNull));
  ts->add(BOOST_PARAM_TEST_CASE(&avro::json::testBool, avro::json::boolData,
                                avro::json::boolData + COUNTOF(avro::json::boolData)));
  ts->add(BOOST_PARAM_TEST_CASE(&avro::json::testLong, avro::json::longData,
                                avro::json::longData + COUNTOF(avro::json::longData)));
  ts->add(
      BOOST_PARAM_TEST_CASE(&avro::json::testDouble, avro::json::doubleData,
                            avro::json::doubleData + COUNTOF(avro::json::doubleData)));
  ts->add(
      BOOST_PARAM_TEST_CASE(&avro::json::testString, avro::json::stringData,
                            avro::json::stringData + COUNTOF(avro::json::stringData)));

  ts->add(BOOST_TEST_CASE(&avro::json::testArray0));
  ts->add(BOOST_TEST_CASE(&avro::json::testArray1));
  ts->add(BOOST_TEST_CASE(&avro::json::testArray2));

  ts->add(BOOST_TEST_CASE(&avro::json::testObject0));
  ts->add(BOOST_TEST_CASE(&avro::json::testObject1));
  ts->add(BOOST_TEST_CASE(&avro::json::testObject2));

  return ts;
}
