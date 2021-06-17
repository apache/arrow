// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <gtest/gtest.h>

#include "arrow/util/reflection_internal.h"

namespace arrow {
namespace internal {

// unmodified structure which we wish to reflect on:
struct Person {
  std::string name;
  int age;
};

// enumeration of properties:
template <>
struct ReflectionTraits<Person> {
  using Properties = std::tuple<DataMember<Person, int, &Person::age>,
                                DataMember<Person, std::string, &Person::name>>;
};

// generic property-based equality comparison
template <typename Class>
struct EqualsImpl {
  template <typename Property>
  void operator()(const Property& prop, size_t i) const {
    *out &= prop.get(l) == prop.get(r);
  }
  const Class& l;
  const Class& r;
  bool* out;
};

template <typename Class,
          typename Properties = typename ReflectionTraits<Class>::Properties>
bool operator==(const Class& l, const Class& r) {
  bool out = true;
  ForEachProperty<Properties>(EqualsImpl<Person>{l, r, &out});
  return out;
}

template <typename Class,
          typename Properties = typename ReflectionTraits<Class>::Properties>
bool operator!=(const Class& l, const Class& r) {
  return !(l == r);
}

const std::string& GenericToString(const std::string& str) { return str; }
std::string GenericToString(int i) { return std::to_string(i); }

template <typename Class, int NumProperties>
struct ToStringImpl {
  template <typename Property>
  void operator()(const Property& prop, size_t i) {
    members[i] = prop.name() + ": " + GenericToString(prop.get(obj));
  }

  std::string Finish() {
    std::string out = nameof<Person>(/*strip_namespace=*/true) + "{";
    for (auto&& member : members) {
      out += member + ", ";
    }
    out.resize(out.size() - 1);
    out.back() = '}';
    return out;
  }

  const Class& obj;
  std::array<std::string, NumProperties> members;
};

template <typename Class,
          typename Properties = typename ReflectionTraits<Class>::Properties>
std::string ToString(const Class& obj) {
  ToStringImpl<Class, std::tuple_size<Properties>::value> impl{obj, {}};
  ForEachProperty<Properties>(impl);
  return impl.Finish();
}

TEST(Reflection, Nameof) {
  ASSERT_EQ(nameof<Person>(), "arrow::internal::Person");
  ASSERT_EQ(nameof<Person>(/*strip_namespace=*/true), "Person");
}

TEST(Reflection, EqualityWithDataMembers) {
  Person genos{"Genos", 19};
  Person kuseno{"Kuseno", 45};

  ASSERT_EQ(genos, genos);
  ASSERT_EQ(kuseno, kuseno);

  ASSERT_NE(genos, kuseno);
  ASSERT_NE(kuseno, genos);
}

TEST(Reflection, ToStringFromDataMembers) {
  Person genos{"Genos", 19};
  Person kuseno{"Kuseno", 45};

  ASSERT_EQ(ToString(genos), "Person{age: 19, name: Genos}");
  ASSERT_EQ(ToString(kuseno), "Person{age: 45, name: Kuseno}");
}

}  // namespace internal
}  // namespace arrow
