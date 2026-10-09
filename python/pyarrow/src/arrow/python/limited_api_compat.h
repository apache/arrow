// Cython codegen calls Py_REFCNT() from its internal macros. With 3.14+ headers
// and a Py_LIMITED_API floor of 3.11-3.13, Py_REFCNT resolves to an exported
// function that only libpython >= 3.14 provides, so the modules fail to import
// on 3.11-3.13 with "undefined symbol: Py_REFCNT". Redefine it as a macro over
// the stable 16-byte header layout (ob_refcnt is first).
//
// Included (via common.pxd) after Python.h, so the macro overrides the
// header's function declaration for every use in generated code.
#if defined(Py_LIMITED_API) && Py_LIMITED_API+0 < 0x030e0000
#  undef Py_REFCNT
#  define Py_REFCNT(o) ((PyObject *)(o))->ob_refcnt
#endif
