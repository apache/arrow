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

#pragma once

#include <atomic>
#include <utility>

#include "arrow/util/macros.h"

namespace arrow {
namespace util {

/// A lock free container with FILO storage order.
///
/// Two thread safe operations are supported:
/// - Push a value onto the stack
/// - move construct from another stack
///
/// range-for compatilble iteration is provided for
/// convenience, but it is *not* thread safe.
template <typename T>
class LockFreeStack {
 public:
  LockFreeStack() = default;

  ~LockFreeStack() {
    Node* next = nodes_.load();

    while (next != NULLPTR) {
      Node* node = next;
      next = next->next;
      delete node;
    }
  }

  LockFreeStack(const LockFreeStack&) = delete;
  LockFreeStack& operator=(const LockFreeStack&) = delete;
  LockFreeStack& operator=(LockFreeStack&& other) = delete;

  LockFreeStack(LockFreeStack&& other) {
    Node* other_nodes = other.nodes_.load();

    do {
    } while (!other.CompareExchange(&other_nodes, NULLPTR));

    nodes_.store(other_nodes);
  }

  void Push(T value) {
    Node* new_head = new Node{std::move(value), nodes_.load()};

    do {
    } while (!CompareExchange(&new_head->next, new_head));
  }

  struct iterator;
  iterator begin();
  iterator end();

 private:
  struct Node {
    T value;
    Node* next;
  };

  bool CompareExchange(Node** expected, Node* desired) {
    return nodes_.compare_exchange_strong(*expected, desired);
  }

  std::atomic<Node*> nodes_{NULLPTR};
};

template <typename T>
struct LockFreeStack<T>::iterator {
  bool operator!=(iterator other) const { return node != other.node; }

  T& operator*() const { return node->value; }

  iterator& operator++() {
    node = node->next;
    return *this;
  }

  Node* node;
};

template <typename T>
typename LockFreeStack<T>::iterator LockFreeStack<T>::begin() {
  return {nodes_.load()};
}

template <typename T>
typename LockFreeStack<T>::iterator LockFreeStack<T>::end() {
  return {NULLPTR};
}

}  // namespace util
}  // namespace arrow

