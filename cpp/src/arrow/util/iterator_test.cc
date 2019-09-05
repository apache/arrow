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

#include "arrow/util/iterator.h"

#include <memory>
#include <vector>

#include "arrow/testing/gtest_util.h"
#include "arrow/util/stl.h"

namespace arrow {

template <typename T>
std::vector<T> IteratorToVector(std::unique_ptr<Iterator<T>>& iterator) {
  std::vector<T> out;

  auto fn = [&out](T value) -> Status {
    out.emplace_back(std::move(value));
    return Status::OK();
  };

  ARROW_EXPECT_OK(iterator->Visit(fn));

  return out;
}

#define EmptyIt MakeEmptyIterator
#define VectorIt MakeVectorIterator
#define FlattenIt MakeFlattenIterator

// Iterators works on pointers
static const int one = 1;
static const int two = 2;
static const int three = 3;

template <typename T>
void AssertIteratorMatch(std::vector<T> expected, std::unique_ptr<Iterator<T>> actual) {
  EXPECT_EQ(expected, IteratorToVector(actual));
}

template <typename T>
void AssertIteratorNoMatch(std::vector<T> expected, std::unique_ptr<Iterator<T>> actual) {
  EXPECT_NE(expected, IteratorToVector(actual));
}

TEST(TestEmptyIterator, Basic) { AssertIteratorMatch({}, EmptyIt<int*>()); }

TEST(TestVectorIterator, Basic) {
  std::vector<const int*> input{&one, &two, &three};

  AssertIteratorMatch({}, VectorIt(std::vector<int*>{}));
  AssertIteratorMatch({&one, &two, &three}, VectorIt(input));

  AssertIteratorNoMatch({&one}, VectorIt(std::vector<const int*>{}));
  AssertIteratorNoMatch({}, VectorIt(input));
  AssertIteratorNoMatch({&one, &two, &two}, VectorIt(input));
  AssertIteratorNoMatch({&one, &two, &three, &one}, VectorIt(input));
}

TEST(FlattenVectorIterator, Basic) {
  std::vector<const int*> input{&one, &two, &three};

  // Flatten expects to consume Iterator<unique_ptr<Iterator<T>>>
  AssertIteratorMatch({}, FlattenIt(EmptyIt<std::unique_ptr<Iterator<int*>>>()));

  // unique_ptr and initializer lists is a nono
  std::vector<std::unique_ptr<Iterator<const int*>>> ok{3};
  ok[0] = VectorIt(std::vector<const int*>{&one});
  ok[1] = VectorIt(std::vector<const int*>{&two});
  ok[2] = VectorIt(std::vector<const int*>{&three});
  AssertIteratorMatch(input, FlattenIt(VectorIt(std::move(ok))));

  std::vector<std::unique_ptr<Iterator<const int*>>> not_enough{2};
  not_enough[0] = VectorIt(std::vector<const int*>{&one});
  not_enough[1] = VectorIt(std::vector<const int*>{&two});
  AssertIteratorNoMatch(input, FlattenIt(VectorIt(std::move(not_enough))));

  std::vector<std::unique_ptr<Iterator<const int*>>> too_much{4};
  too_much[0] = VectorIt(std::vector<const int*>{&one});
  too_much[1] = VectorIt(std::vector<const int*>{&two});
  too_much[2] = VectorIt(std::vector<const int*>{&three});
  too_much[3] = VectorIt(std::vector<const int*>{&two});
  AssertIteratorNoMatch(input, FlattenIt(VectorIt(std::move(too_much))));
}

template <typename T>
std::unique_ptr<Iterator<T>> Join(T a, T b) {
  std::vector<std::unique_ptr<Iterator<T>>> joined{2};
  joined[0] = VectorIt(std::vector<T>{a});
  joined[1] = VectorIt(std::vector<T>{b});

  return FlattenIt(VectorIt(std::move(joined)));
}

template <typename T>
std::unique_ptr<Iterator<T>> Join(T a, std::unique_ptr<Iterator<T>> b) {
  std::vector<std::unique_ptr<Iterator<T>>> joined{2};
  joined[0] = VectorIt(std::vector<T>{a});
  joined[1] = std::move(b);

  return FlattenIt(VectorIt(std::move(joined)));
}

TEST(FlattenVectorIterator, Pyramid) {
  auto it = Join(&one, Join(&two, Join(&two, Join(&three, Join(&three, &three)))));
  AssertIteratorMatch({&one, &two, &two, &three, &three, &three}, std::move(it));
}

}  // namespace arrow
