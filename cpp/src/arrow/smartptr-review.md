---
name: smartptr-review
description: >-
  Review C++ code (.cpp/.cc/.cxx/.h/.hpp) for four specific smart pointer
  defects: useless shared_ptr copies, shared_ptr passed by value without a
  move, unique_ptr passed by const&, and same-thread reentrancy hazards when
  dereferencing a global, static, or member shared_ptr. Use when asked to
  review or audit smart pointer usage, refcount overhead, unnecessary
  shared_ptr copies, pass-by-value versus const&, or reentrancy and
  use-after-free risk around shared_ptr — including for in-house pointer
  types such as intrusive_ptr or ComPtr. Not for general C++ review, memory
  leaks, raw new/delete, make_shared versus new, ownership design, or
  non-C++ code.
---

You are performing a focused code review for C++ smart pointer usage patterns.

Before beginning, read `cpp-review-procedure.md` in full. Its procedure, definitions, and output contract are mandatory and are not repeated here.


## Definitions

### Pointer type categories

  - **OwningPtr type (heuristic)**: A type that is dereferenceable
    AND whose name ends in `ptr` (case-insensitively)
    AND has a user-defined destructor
    AND has a user-defined dereferencing operator that does not spell out
      `const` for the returned reference.

> Note: This heuristic is intended to correctly detect owning smart pointer types including custom in-house types, but exclude non-owning indirection-like types (e.g., `std::observer_ptr`, STL iterators, `std::optional`). It may still misfire on non-owning handles named `*ptr` that happen to have a user-defined destructor and a non-`const`ified return for deference.

  - **UniquePtr type**: A type that satisfies any of the following sub-bullets:
    - **(heuristic)** is an OwningPtr type AND is movable AND is not copyable
    - **(allow-list)** is specified as such by the user (e.g., in the prompt or in a config file) OR is any of { `unique_ptr` }

  - **SharedPtr type**: A type that satisfies any of the following sub-bullets:
    - **(heuristic)** is an OwningPtr type AND is copyable
    - **(allow-list)** is specified as such by the user (e.g., in the prompt or in a config file) OR is any of { `shared_ptr`, `intrusive_ptr`, `ComPtr`, `CountedPtr`, `Ptr` }

### Dereferences

  - **Dereferencing operator**: Unary `*` or `->` or `[]`.
  - **Dereference**: When a dereferencing operator is applied to an object.
  - **Dereferenceable**: A type or object that provides a dereferencing operator.


## Rules to check

### Rule SP1: Useless SharedPtr local copies

**Pattern**: A local non-parameter object `ptr` of SharedPtr type is copied to another local object `ptr_copy` AND `ptr` is not potentially modified during the lifetime of `ptr_copy` AND `ptr_copy` is not potentially modified during the lifetime of `ptr_copy` AND `ptr_copy` is not moved from during its lifetime.

**Issue**: Unnecessary reference count increment/decrement overhead.

**Severity**: Count a violation's severity as:
  - "high (performance)" if it is very likely that the copy is happening on a hot path;
  - otherwise, as "medium (performance)" if the copy is happening in a loop or the function is a commonly used library utility;
  - otherwise, as "low (performance)".

**Fix**: Use the original object instead of making a copy.

**Example violation**:
```cpp
void foo() {
    std::shared_ptr<Widget> ptr = getWidget();
    for (/*...*/) {
        auto copy = ptr; // ❌ Useless copy - just use ptr
        copy->doSomething();
    }
}
// Should be:
//  void foo() {
//      std::shared_ptr<Widget> ptr = getWidget();
//      for (/*...*/) {
//          ptr->doSomething();
//      }
//  }
```

**Example violation**:
```cpp
void foo() {
    std::shared_ptr<Widget> ptr = getWidget();
    std::shared_ptr<Widget> copy = ptr; // ❌ Useless copy - just use ptr
    copy->doSomethingElse();
}
// Should be:
//  void foo() {
//      std::shared_ptr<Widget> ptr = getWidget();
//      ptr->doSomethingElse();
//  }
```

**Example non-violation**:
```cpp
void f() {
    std::shared_ptr<Widget> const local = global_pointer; // ✅ OK - not a violation, global_pointer is not a local object
    local->doWork();
}
```


### Rule SP2: Useless SharedPtr pass by value

**Pattern**: A parameter `ptr` of SharedPtr type is passed by value, but `ptr` is never moved from in the function body.

**Issue**: Unnecessary reference count increment/decrement overhead at call site.

**Severity**: Count a violation's severity as:
  - "high (performance)" if it is very likely that the function is called on a hot path;
  - otherwise, as "medium (performance)" if the function is called in a loop or the function is a commonly used library utility;
  - otherwise, as "low (performance)".

**Fix**:
  - If any visible call site passes a non-local SharedPtr (namespace-scope, static, or a nonstatic data member) and the callee's call tree could reset it, ignore it as a violation of this rule.
  - Otherwise, if the function would not compile if `ptr` was passed by `const&` instead, report that the parameter could not be changed to `const&` and report the specific use(s) in the body that would fail to compile. (Example: if `ptr` or `&ptr` is passed to a function that takes a reference or pointer to the non-`const` SharedPtr type.) For each definite last use of `ptr` that is a copy from `ptr` and for which writing `std::move(ptr)` instead of `ptr` would compile, write `std::move(ptr)` instead.
  - Otherwise, pass `ptr` by `const&` instead.

**Example violation**:
```cpp
void process( std::shared_ptr<Data> ptr ) { // ❌ Passed by value and never moved from
    ptr->process();
}
// Should be:
//  void process( std::shared_ptr<Data> const& ptr ) {
//      ptr->process();
//  }
```

**Example not-locally-fixable violation**:
```cpp
void takes_ptr_to_nonconst( std::shared_ptr<Data>* );
void takes_copy( std::shared_ptr<Data> );

void process( std::shared_ptr<Data> ptr ) { // ❌ Passed by value and never moved from, but cannot be changed to const& - report this instead, and suggest changing takes_ptr_to_nonconst to take by const*
    takes_ptr_to_nonconst(&ptr);
    takes_copy(ptr);
}
// Should be:
//  void process( std::shared_ptr<Data> ptr ) {
//      takes_ptr_to_nonconst(&ptr);  // should be reported as preventing a const& parameter
//      takes_copy(std::move(ptr));   // change definite last use from copy to move
//  }
```

**Example non-violation**:
```cpp
void process( std::shared_ptr<Data> ptr ) {  // ✅ OK - not a violation, ptr is moved from via .swap
    ptr.swap(some->other->storage);
}
```


### Rule SP3: UniquePtr passed by const&

**Pattern**: A parameter `ptr` of UniquePtr type is passed by `const&`.

**Issue**: Conceptual error -- there is no valid use for passing a UniquePtr by `const&`.

**Severity**: Count a violation as "low (design)" severity.

**Fix**: If the function body tests `ptr`'s nullness, pass a raw pointer to the owned object instead. Otherwise, pass a reference to the owned object instead.

**Example violation**:
```cpp
void inspect1( std::unique_ptr<Widget> const& ptr ) { // ❌ Nonsensical pattern
    ptr->process();
}
// Should be:
//  void inspect1(Widget& w) {
//      w.process();
//  }
```

**Example violation**:
```cpp
void inspect2( std::unique_ptr<Widget> const& ptr ) { // ❌ Nonsensical pattern
    if (ptr) {
        ptr->process();
    }
}
// Should be:
//  void inspect2(Widget* ptr) {
//      if (ptr) {
//          ptr->process();
//      }
//  }
```


### Rule SP4: Same-thread-reentrancy-unsafe SharedPtr dereference

**Pattern**: In a function body, a non-local object `ptr` of SharedPtr type is dereferenced AND the resulting pointer or reference is used as a function argument (including possibly to the implicit `this` parameter) AND ( either `ptr` is not a nonstatic data member OR the function does not assert that `ptr` still has the same pointer value at function exit that it had at function entry ).

**Issue**: Same-thread reentrancy hazard -- if the function or its callees can trigger code that releases the last reference to the shared object, dereferencing becomes undefined behavior (use-after-free).

**Severity**: Count a violation's severity as:
  - "high (correctness)" if there is a code path in the called function's call tree that could modify `ptr`;
  - otherwise, as "medium (correctness)" with a list of warnings that identify where the call tree could not be examined (e.g., "call tree was opaque at `Foo::onEvent` (virtual call)"), if any part of the called function's call tree is opaque and could not be inspected for modifications of `ptr`;
  - otherwise, as "medium (correctness)" only.

**Fix**:
  - If `ptr` is a nonstatic data member AND there is no evidence that its value is likely to change during the function call, assert that `ptr` still has the same pointer value at function exit that it had on function entry. The assertion should use a scope guard object (e.g., using `gsl::finally` or `std::experimental::scope_exit`). (Note: This is a concession to avoid false positives; the ideal fix is still the second bullet which is correct by construction.)
  - Otherwise, first take a local `const` copy of the SharedPtr to increment the reference count, and dereference the local copy instead; this single reference count protects the entire function body and its transitive call tree.

**Example violation**:
```cpp
void f() {
    global_state->doWork(); // ❌ Dereferencing global/static/heap SharedPtr
    // What if doWork triggers code that resets global_state?
}
// Should be:
//  void f() {
//      auto const local = global_state; // Take local copy first
//      local->doWork();                 // Now safe
//  }
```

**Example violation**:
```cpp
class Handler {
    std::shared_ptr<State> state_;

    void process() {
        state_->doWork(); // ❌ Dereferencing member SharedPtr without asserting value does not change
        // What if doWork triggers code that resets state_?
    }
};
// Should be this (if there is no evidence in the call tree that state_ is likely to change):
//  void process() {
//      auto guard = gsl::finally(
//          [this, old = state_.get()]             // Remember original value
//          { assert(old == this->state_.get()); } // assert it hasn't changed
//          );
//      state_->doWork(); // Now safe, if value did not change
//  }
//
// or else this if the value really may change:
//  void process() {
//      auto const local = state_; // Take local copy first
//      local->doWork();           // Now safe
//  }
```

**Example non-violation**:
```cpp
class Handler {
    std::shared_ptr<State> state_;

    void process() { // ✅ OK - not a violation, state_ is asserted not to change
        auto guard = std::experimental::scope_exit(
            [this, old = state_.get()]
            { assert(old == this->state_.get()); }
            );
        state_->doWork();
    }
};
```
