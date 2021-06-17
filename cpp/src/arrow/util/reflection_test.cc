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

#include <sstream>

#include <gtest/gtest.h>

#include "arrow/util/reflection_internal.h"
#include "arrow/util/string.h"

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

template <typename Class,
          typename Properties = typename ReflectionTraits<Class>::Properties>
struct ToStringImpl {
  explicit ToStringImpl(const Class& obj)
      : obj_(obj), members_(std::tuple_size<Properties>::value) {}

  template <typename Property>
  void operator()(const Property& prop, size_t i) {
    std::stringstream ss;
    ss << prop.name() << ":" << prop.get(obj_);
    members_[i] = ss.str();
  }

  std::string Finish() {
    auto members = JoinStrings(members_, ",");
    return nameof<Class>(/*strip_namespace=*/true) + "{" + members + "}";
  }

  const Class& obj_;
  std::vector<std::string> members_;
};

template <typename Class,
          typename Properties = typename ReflectionTraits<Class>::Properties>
std::string ToString(const Class& obj) {
  ToStringImpl<Class> impl{obj};
  ForEachProperty<Properties>(impl);
  return impl.Finish();
}
template <typename Class,
          typename Properties = typename ReflectionTraits<Class>::Properties>
struct FromStringImpl {
  void Fail() { *obj_ = util::nullopt; }

  template <typename Property>
  void operator()(const Property& prop, size_t i) {
    if (!obj_->has_value()) return;

    auto first_colon = members_[i].find_first_of(':');
    if (first_colon == util::string_view::npos) return Fail();

    auto name = members_[i].substr(0, first_colon);
    if (name != prop.name()) return Fail();

    auto value_repr = members_[i].substr(first_colon + 1);
    typename Property::type value;
    try {
      std::stringstream ss(value_repr.to_string());
      ss >> value;
      if (!ss.eof()) return Fail();
    } catch (...) {
      return Fail();
    }
    prop.set(&obj_->value(), value);
  }

  util::optional<Class>* obj_;
  std::vector<util::string_view> members_;
};

template <typename Class,
          typename Properties = typename ReflectionTraits<Class>::Properties>
util::optional<Class> FromString(util::string_view repr) {
  auto first_brace = repr.find_first_of('{');
  if (first_brace == util::string_view::npos) return util::nullopt;

  auto name = repr.substr(0, first_brace);
  if (name != nameof<Class>(/*strip_namespace=*/true)) return util::nullopt;

  repr = repr.substr(first_brace + 1);
  if (repr.empty()) return util::nullopt;
  if (repr.back() != '}') return util::nullopt;
  repr = repr.substr(0, repr.size() - 1);

  auto members = SplitString(repr, ',');
  if (members.size() != std::tuple_size<Properties>::value) return util::nullopt;

  util::optional<Class> obj = Class{};
  FromStringImpl<Class> impl{&obj, members};
  ForEachProperty<Properties>(impl);
  return obj;
}

TEST(Reflection, Nameof) {
  EXPECT_EQ(nameof<Person>(), "arrow::internal::Person");
  EXPECT_EQ(nameof<Person>(/*strip_namespace=*/true), "Person");
}

TEST(Reflection, EqualityWithDataMembers) {
  Person genos{"Genos", 19};
  Person kuseno{"Kuseno", 45};

  EXPECT_EQ(genos, genos);
  EXPECT_EQ(kuseno, kuseno);

  EXPECT_NE(genos, kuseno);
  EXPECT_NE(kuseno, genos);
}

TEST(Reflection, ToStringFromDataMembers) {
  Person genos{"Genos", 19};
  Person kuseno{"Kuseno", 45};

  EXPECT_EQ(ToString(genos), "Person{age:19,name:Genos}");
  EXPECT_EQ(ToString(kuseno), "Person{age:45,name:Kuseno}");
}

TEST(Reflection, FromStringToDataMembers) {
  Person genos{"Genos", 19};

  EXPECT_EQ(FromString<Person>(ToString(genos)), genos);

  EXPECT_EQ(FromString<Person>(""), util::nullopt);
  EXPECT_EQ(FromString<Person>("Per"), util::nullopt);
  EXPECT_EQ(FromString<Person>("Person{"), util::nullopt);
  EXPECT_EQ(FromString<Person>("Person{age:19,name:Genos"), util::nullopt);

  EXPECT_EQ(FromString<Person>("Person{name:Genos"), util::nullopt);
  EXPECT_EQ(FromString<Person>("Person{age:19,name:Genos,extra:Cyborg}"), util::nullopt);
  EXPECT_EQ(FromString<Person>("Person{name:Genos,age:19"), util::nullopt);

  EXPECT_EQ(FromString<Person>("Fake{age:19,name:Genos}"), util::nullopt);

  EXPECT_EQ(FromString<Person>("Person{age,name:Genos}"), util::nullopt);
  EXPECT_EQ(FromString<Person>("Person{age:nineteen,name:Genos}"), util::nullopt);
  EXPECT_EQ(FromString<Person>("Person{age:19 ,name:Genos}"), util::nullopt);
  EXPECT_EQ(FromString<Person>("Person{age:19,moniker:Genos}"), util::nullopt);
}

}  // namespace internal
}  // namespace arrow
