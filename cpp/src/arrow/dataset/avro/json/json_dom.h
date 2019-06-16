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

#ifndef avro_json_JsonDom_hh__
#define avro_json_JsonDom_hh__

#include <iostream>
#include <stdint.h>
#include <map>
#include <string>
#include <vector>
#include <memory>

#include "boost/any.hpp"
#include "Config.hh"

namespace avro {

class AVRO_DECL InputStream;

namespace json {
class Entity;
    
typedef bool Bool;
typedef int64_t Long;
typedef double Double;
typedef std::string String;
typedef std::vector<Entity> Array;
typedef std::map<std::string, Entity> Object;
    
class AVRO_DECL JsonParser;
class JsonNullFormatter;

template<typename F = JsonNullFormatter>
class AVRO_DECL JsonGenerator;

enum EntityType {
    etNull,
    etBool,
    etLong,
    etDouble,
    etString,
    etArray,
    etObject
};

const char* typeToString(EntityType t);

class AVRO_DECL Entity {
    EntityType type_;
    boost::any value_;
    size_t line_; // can't be const else noncopyable...

    void ensureType(EntityType) const;
public:
    Entity(size_t line = 0) : type_(etNull), line_(line) { }
    Entity(Bool v, size_t line = 0) : type_(etBool), value_(v), line_(line) { }
    Entity(Long v, size_t line = 0) : type_(etLong), value_(v), line_(line) { }
    Entity(Double v, size_t line = 0) : type_(etDouble), value_(v), line_(line) { }
    Entity(const std::shared_ptr<String>& v, size_t line = 0) : type_(etString), value_(v), line_(line) { }
    Entity(const std::shared_ptr<Array>& v, size_t line = 0) : type_(etArray), value_(v), line_(line) { }
    Entity(const std::shared_ptr<Object>& v, size_t line = 0) : type_(etObject), value_(v), line_(line) { }
    
    EntityType type() const { return type_; }

    size_t line() const { return line_; }

    Bool boolValue() const {
        ensureType(etBool);
        return boost::any_cast<Bool>(value_);
    }

    Long longValue() const {
        ensureType(etLong);
        return boost::any_cast<Long>(value_);
    }
    
    Double doubleValue() const {
        ensureType(etDouble);
        return boost::any_cast<Double>(value_);
    }

    String stringValue() const;

    String bytesValue() const;
    
    const Array& arrayValue() const {
        ensureType(etArray);
        return **boost::any_cast<std::shared_ptr<Array> >(&value_);
    }

    const Object& objectValue() const {
        ensureType(etObject);
        return **boost::any_cast<std::shared_ptr<Object> >(&value_);
    }

    std::string toString() const;
};

template <typename T>
struct type_traits {
};

template <> struct type_traits<bool> {
    static EntityType type() { return etBool; }
    static const char* name() { return "bool"; }
};

template <> struct type_traits<int64_t> {
    static EntityType type() { return etLong; }
    static const char* name() { return "long"; }
};

template <> struct type_traits<double> {
    static EntityType type() { return etDouble; }
    static const char* name() { return "double"; }
};
    
template <> struct type_traits<std::string> {
    static EntityType type() { return etString; }
    static const char* name() { return "string"; }
};

template <> struct type_traits<std::vector<Entity> > {
    static EntityType type() { return etArray; }
    static const char* name() { return "array"; }
};

template <> struct type_traits<std::map<std::string, Entity> > {
    static EntityType type() { return etObject; }
    static const char* name() { return "object"; }
};

AVRO_DECL Entity readEntity(JsonParser& p);

AVRO_DECL Entity loadEntity(InputStream& in);
AVRO_DECL Entity loadEntity(const char* text);
AVRO_DECL Entity loadEntity(const uint8_t* text, size_t len);

void writeEntity(JsonGenerator<JsonNullFormatter>& g, const Entity& n);

}
}

#endif


