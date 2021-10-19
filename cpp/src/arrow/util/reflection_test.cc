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

// generic property-based equality comparison
template <typename Class>
struct EqualsImpl {
  template <typename Properties>
  EqualsImpl(const Class& l, const Class& r, const Properties& props)
      : left_(l), right_(r) {
    props.ForEach(*this);
  }

  template <typename Property>
  void operator()(const Property& prop, size_t i) {
    equal_ &= prop.get(left_) == prop.get(right_);
  }

  const Class& left_;
  const Class& right_;
  bool equal_ = true;
};

// generic property-based serialization
template <typename Class>
struct ToStringImpl {
  template <typename Properties>
  ToStringImpl(util::string_view class_name, const Class& obj, const Properties& props)
      : class_name_(class_name), obj_(obj), members_(props.size()) {
    props.ForEach(*this);
  }

  template <typename Property>
  void operator()(const Property& prop, size_t i) {
    std::stringstream ss;
    ss << prop.name() << ":" << prop.get(obj_);
    members_[i] = ss.str();
  }

  std::string Finish() {
    return class_name_.to_string() + "{" + JoinStrings(members_, ",") + "}";
  }

  util::string_view class_name_;
  const Class& obj_;
  std::vector<std::string> members_;
};

// generic property-based deserialization
template <typename Class>
struct FromStringImpl {
  template <typename Properties>
  FromStringImpl(util::string_view class_name, util::string_view repr,
                 const Properties& props) {
    Init(class_name, repr, props.size());
    props.ForEach(*this);
  }

  void Fail() { obj_ = util::nullopt; }

  void Init(util::string_view class_name, util::string_view repr, size_t num_properties) {
    if (!repr.starts_with(class_name)) return Fail();

    repr = repr.substr(class_name.size());
    if (repr.empty()) return Fail();
    if (repr.front() != '{') return Fail();
    if (repr.back() != '}') return Fail();

    repr = repr.substr(1, repr.size() - 2);
    members_ = SplitString(repr, ',');
    if (members_.size() != num_properties) return Fail();
  }

  template <typename Property>
  void operator()(const Property& prop, size_t i) {
    if (!obj_) return;

    auto first_colon = members_[i].find_first_of(':');
    if (first_colon == util::string_view::npos) return Fail();

    auto name = members_[i].substr(0, first_colon);
    if (name != prop.name()) return Fail();

    auto value_repr = members_[i].substr(first_colon + 1);
    typename Property::Type value;
    try {
      std::stringstream ss(value_repr.to_string());
      ss >> value;
      if (!ss.eof()) return Fail();
    } catch (...) {
      return Fail();
    }
    prop.set(&*obj_, std::move(value));
  }

  util::optional<Class> obj_ = Class{};
  std::vector<util::string_view> members_;
};

// unmodified structure which we wish to reflect on:
struct Person {
  int age;
  std::string name;
};

// enumeration of properties:
// NB: no references to Person::age or Person::name after this
// NB: ordering of properties follows this enum, regardless of
//     order of declaration in `struct Person`
static auto kPersonProperties =
    MakeProperties(DataMember("age", &Person::age), DataMember("name", &Person::name));

// use generic facilities to define equality, serialization and deserialization
bool operator==(const Person& l, const Person& r) {
  return EqualsImpl<Person>{l, r, kPersonProperties}.equal_;
}

bool operator!=(const Person& l, const Person& r) { return !(l == r); }

std::string ToString(const Person& obj) {
  return ToStringImpl<Person>{"Person", obj, kPersonProperties}.Finish();
}

void PrintTo(const Person& obj, std::ostream* os) { *os << ToString(obj); }

util::optional<Person> PersonFromString(util::string_view repr) {
  return FromStringImpl<Person>("Person", repr, kPersonProperties).obj_;
}

TEST(Reflection, EqualityWithDataMembers) {
  Person genos{19, "Genos"};
  Person kuseno{45, "Kuseno"};

  EXPECT_EQ(genos, genos);
  EXPECT_EQ(kuseno, kuseno);

  EXPECT_NE(genos, kuseno);
  EXPECT_NE(kuseno, genos);
}

TEST(Reflection, ToStringFromDataMembers) {
  Person genos{19, "Genos"};
  Person kuseno{45, "Kuseno"};

  EXPECT_EQ(ToString(genos), "Person{age:19,name:Genos}");
  EXPECT_EQ(ToString(kuseno), "Person{age:45,name:Kuseno}");
}

TEST(Reflection, FromStringToDataMembers) {
  Person genos{19, "Genos"};

  EXPECT_EQ(PersonFromString(ToString(genos)), genos);

  EXPECT_EQ(PersonFromString(""), util::nullopt);
  EXPECT_EQ(PersonFromString("Per"), util::nullopt);
  EXPECT_EQ(PersonFromString("Person{"), util::nullopt);
  EXPECT_EQ(PersonFromString("Person{age:19,name:Genos"), util::nullopt);

  EXPECT_EQ(PersonFromString("Person{name:Genos"), util::nullopt);
  EXPECT_EQ(PersonFromString("Person{age:19,name:Genos,extra:Cyborg}"), util::nullopt);
  EXPECT_EQ(PersonFromString("Person{name:Genos,age:19"), util::nullopt);

  EXPECT_EQ(PersonFromString("Fake{age:19,name:Genos}"), util::nullopt);

  EXPECT_EQ(PersonFromString("Person{age,name:Genos}"), util::nullopt);
  EXPECT_EQ(PersonFromString("Person{age:nineteen,name:Genos}"), util::nullopt);
  EXPECT_EQ(PersonFromString("Person{age:19 ,name:Genos}"), util::nullopt);
  EXPECT_EQ(PersonFromString("Person{age:19,moniker:Genos}"), util::nullopt);

  EXPECT_EQ(PersonFromString("Person{age: 19, name: Genos}"), util::nullopt);
}

enum class PersonType : int8_t {
  EMPLOYEE,
  CONTRACTOR,
};

template <>
struct EnumTraits<PersonType>
    : BasicEnumTraits<PersonType, PersonType::EMPLOYEE, PersonType::CONTRACTOR> {
  static std::string name() { return "PersonType"; }
  static std::string value_name(PersonType value) {
    switch (value) {
      case PersonType::EMPLOYEE:
        return "EMPLOYEE";
      case PersonType::CONTRACTOR:
        return "CONTRACTOR";
    }
    return "<INVALID>";
  }
};

TEST(Reflection, EnumTraits) {
  static_assert(!has_enum_traits<Person>::value, "");
  static_assert(has_enum_traits<PersonType>::value, "");
  static_assert(std::is_same<EnumTraits<PersonType>::CType, int8_t>::value, "");
  static_assert(std::is_same<EnumTraits<PersonType>::Type, Int8Type>::value, "");
}

}  // namespace internal
}  // namespace arrow
