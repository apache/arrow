
You are performing a review of C++ code.

By default, operate in read-only mode and only generate a report of violations and recommendations. Do not make changes to existing files or create a PR unless the user explicitly requests that action.


## Scope

Review C++ source files (`.cpp`, `.cc`, `.cxx`, `.h`, `.hpp`) only. Skip `.c` files (plain C).

If the user provides file paths or folder names, look for violations in those. Otherwise, report violations in the current git diff for the user's local working copy. To determine violations in this code usually requires reading more context in surrounding code (e.g., the enclosing function, the class definition).

First probe for `compile_commands.json`, `clangd`, or `clang-query` to try to find an accurate language service that can reliably verify characteristics about an object's type (e.g., whether the object's type has a user-defined `operator*`). If you cannot use an accurate language service, then ignore (heuristic) bullets and use only (allow-list) bullets.

Report a result from a header file only once even if it is included in multiple TUs. For a given definition in a header file: if the definition parses cleanly using an accurate language service in one TU and badly in another, or resolves overloads more cleanly in one TU than another, keep the results from the cleanest version and discard the others.

Do not report violations or fixes not mentioned in the skill.

Do not report suppressed violations: On a line or statement that has a violation of two different rules labeled X and Y, and X (but not Y) was explicitly suppressed such as "// suppress rule X" or "[[suppress(X)]]" or similar, do not report the violation of X but do report the violation of Y. So that suppressions remain stable, rule labels in each skill are intended to not be reused; if a rule is deleted, its label subheading should be left with a body of "(Rule deleted)."


## Procedure

Do the work in two passes under different roles. Finish pass 1 completely and write its findings to a file. Then begin pass 2 as a reviewer whose assignment is to find defects in that file, working from the file and the repository source rather than from pass 1's reasoning. No report is printed and no change is suggested until pass 2 is complete.

### Pass 1 — analysis

Emit interim findings as a machine-readable file with one record per candidate violation, each carrying the rule ID, path, line, the fix chosen, the evidence for that fix, and details that are referred to in the rule (e.g., if the rule is about a parameter and its use in the body, include the parameter name, declared type, canonical type, every use of the parameter with its line and classification). Write no prose about the findings during this pass.

### Pass 2 — review

Run each of the following checks and state, for each, what was checked, what came back, and what changed as a result. A check whose result is reported as clean must name the evidence that makes it clean; "no issues found" on its own is not a result.

1. **Sample audit.** Draw a random sample of the greater of ten findings or five percent of them, spread across subsystems and across every fix category the sweep produced. For each, re-derive the verdict from the source rather than from the interim record: read the enclosing function in full, the declarations of any callees involved, and the definition of any type the rule turns on. Then record three separate judgments: whether the violation is real, whether the prescribed fix is the one the skill prescribes, and whether the stated justification matches what the code does. Report a defect rate for each judgment across the sample.

2. **Escalation.** If the sample audit finds real-violation defects above one in ten, or fix defects above one in five, stop; the sweep is not reportable. Diagnose the cause, fix it, rerun, and audit a fresh sample.

3. **Named failure hypotheses.** Test each of these against the interim file rather than reasoning about whether it applies: failures hidden by a degraded parse of one translation unit while another parsed the same header cleanly; a violation reported at a macro expansion site rather than at the macro body; a violation reported at a point of instantiation rather than at the template definition; the same violation reported twice through different paths to the same header; a parameter reported as unused because its only use sits in an inactive preprocessor branch; a use classified as a read only because the callee could not be resolved.

4. **Fix interactions.** Group findings by function and by caller-callee pairs within the finding set. Where fixing one changes the evidence for another, say which to apply first and what the second becomes afterward.

5. **Severity re-derivation.** For every finding whose severity is based on being on a hot path, name the caller or user annotation that puts it on a hot path.

6. **Compilation check.** Where the environment permits, apply each reported fix and compile the affected translation unit. Where it does not, say so rather than implying the fixes were tested.

## Generated, vendored, macro-expanded, templated, and conditionally compiled code

More than one of the following may apply to a given code location.

  - **Generated code**: A file produced by a code generator, identified by a banner comment such as `DO NOT EDIT`, `@generated`, or `Generated by ...` within the first 40 lines, by a conventional name (`*.pb.h`, `*.pb.cc`, `moc_*.cpp`, `ui_*.h`, `*_generated.h`, `*.pas.h`), or by residence in a build output directory. Do not report violations in generated code; instead, report a single aggregate note giving the count and naming the generator, so that the user can decide whether to take the fix upstream into the generator or its input schema.
  
  - **Vendored third-party code**: A file under a dependency tree such as `third_party/`, `thirdparty/`, `vendor/`, `external/`, `extern/`, `deps/`, or `contrib/`, or outside the repository root (including system headers). Do not report violations in vendored code by default; report an aggregate count instead. If the user names a vendored path explicitly, review it as normal and classify every finding in it as non-locally fixable.
  
  - **Macro-expanded code**: A violation whose offending construct originates in a macro body rather than at the expansion site. Report it once, at the location of the macro definition, and state that the fix applies to the macro body; do not report it separately at each expansion site. If the macro body is not visible, do not report the violation at all.
  
  - **Template-instantiated code**: A violation whose offending construct originates in a template body rather than at the point of instantiation. Report it once, at the location of the template definition, and state that the fix applies to the template body; do not report it separately at each point of instantiation. If the template body is not visible, do not report the violation at all.
  
  - **Conditionally compiled code**: Consider only the code that the active configuration compiles; where no configuration is known, consider the code that survives with no macros defined beyond the compiler's own, and say so in the report.
  

## Definitions

### Severity order

Within the same severity level (e.g., "high"), consider the annotations in the following order from highest to lowest sub-severity:
  - "(correctness)"
  - "(performance)"
  - "(design)"
  - no annotation

### Function body

Consider the member initializer list as part of the function body.

### Locally fixable violations

  - **Non-locally fixable violation**: A violation whose fix would require
    changing a parameter's declared type on a function whose signature cannot
    be changed locally, including: a function marked `virtual`, `override`,
    and/or `final`; a function whose address is taken; a callback bound to a
    fixed signature; a published API boundary; a template instantiation
    constrained by a concept.

  - **Locally fixable violation**: A violation that is not non-locally fixable.

### Local object

  - **Local object**:
    - A function parameter
    - OR a non-`static` non-`thread_local` variable declared in the function body
    - OR a lambda capture by value, including an init-capture, whatever its
      initializer, since the closure holds its own copy and its own reference count
    - OR a lambda capture by reference whose referent is a local object

A variable that is a reference or a raw pointer bound to a non-local object is itself non-local, since it aliases rather than copies. A SharedPtr that is a _copy_ of a non-local SharedPtr is a local object; the copy is what makes it safe.

### Function's call tree

  - **Function's call tree**: A function `f`'s call tree is the set of code that is transitively called by `f`. Consider the known call tree of called functions whose definitions are available; ignore unknowable parts of the call tree that are hidden by opaque calls, including but not limited to: calls to forward-declared functions; calls to virtual functions; calls through pointers to function; calls through `std::function`.

### Hot path

  - **Hot path**: A code sequence that is likely to be executed frequently in production (not tests). Examples include but are not limited to: an override of a known per-request interface; a caller inside a loop in the same translation unit; an explicit hot-path annotation or profile file supplied by the user.

### Definite last use

  - **Definite last use**: A definite last use of local object `x` means a use of `x` in an expression E where `x` appears only once in E AND `x` is not referred to again after E on any local control flow path that includes E. (Reminder: In a constructor, all control flow paths begin with the member-initializers in declaration order.)

### Moved from

  - **Moved from**: An object `x` is moved from if any of the following operations are performed: `std::move(x)`, `std::exchange(x, /*...*/)`, `x.swap(y)`, `static_cast<T&&>(x)`, `return x;` if returned by value, or code that causes `x` to be passed as an argument to the parameter of a move constructor or move assignment operator.

### Potentially modified

  - **Potentially modified**: An object `x` is potentially modified if it is assigned to or used as an argument to a non-`const` paramter (including the implicit `this` parameter by calling a non-`const` member function `x.may_modify()`).

### Namespaces and type names

"Under namespaces `X` and `Y`" includes named or anonymous subnamespaces such as `X::X2::X3` and inline namespaces. For example, "Under namespaces `std` or `boost`" includes for example under namespaces `std::tr1` and `boost::interprocess`.

Any specific _unqualified_ named type, such as `shared_ptr`, means any type having that name as a suffix in the global namespace or under namespaces `std` and `boost` OR any typedef/aliases of such a type. For example, `shared_ptr` includes (but is not limited to) `std::tr1::shared_ptr`, `boost::local_shared_ptr`,`boost::interprocess::shared_ptr`.


## Output contract

State up front:
  - Whether an accurate language service was used
  - A count of suppressed violations
  - The verification results: the outcome of each Pass 2 check, plus the sample size (absolute) and the defect rate (percentage) for each of the three judgments, and every adjustment the review pass caused.

Where the evidence for a fix rests on a callee the language service could not resolve, say so at that finding and use an alternative fix that does not rely on knowing the unresolved callee.

The main findings report should be divided into two sections:
  - Locally fixable violations
  - Non-locally fixable violations

In each section:
  - Report one finding per violation with `path:line`, a stable rule ID,
    the severity string, a quoted snippet that shows the violation, the
    concrete fix(es), and a copy of the same quoted snippet with the
    fix(es) applied
  - Order findings by severity descending, then path.
  - If there are no findings in the section, print "None found."
  - By default show only the 30 most severe violations in the section,
    if the user does not specify a different number. If the limit is
    reached, add a line stating how many items were withheld.

Report a count of violations skipped in generated or vendored code, with the paths or trees they came from.

Report conditionally compiled code that was skipped because it was not in the active configuration.

Report the same violation only once, even if it is in a header file.
