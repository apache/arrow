// Copyright Stefan Seefeld 2005.
// Distributed under the Boost Software License, Version 1.0. (See
// accompanying file LICENSE_1_0.txt or copy at
// http://www.boost.org/LICENSE_1_0.txt)

#include <boost/python/exec.hpp>
#include <boost/python/borrowed.hpp>
#include <boost/python/dict.hpp>
#include <boost/python/extract.hpp>
#include <boost/python/handle.hpp>

namespace boost 
{ 
namespace python 
{

object BOOST_PYTHON_DECL eval(str string, object global, object local)
{
    return eval(python::extract<char const *>(string), global, local);
}

object BOOST_PYTHON_DECL eval(char const *string, object global, object local)
{
  // Set suitable default values for global and local dicts.
  if (global.is_none())
  {
    if (PyObject *g = PyEval_GetGlobals())
      global = object(detail::borrowed_reference(g));
    else
      global = dict();
  }
  if (local.is_none()) local = global;
  // should be 'char const *' but older python versions don't use 'const' yet.
  char *s = const_cast<char *>(string);
  PyObject* result = PyRun_String(s, Py_eval_input, global.ptr(), local.ptr());
  if (!result) throw_error_already_set();
  return object(detail::new_reference(result));
}

object BOOST_PYTHON_DECL exec(str string, object global, object local)
{
    return exec(python::extract<char const *>(string), global, local);
}

object BOOST_PYTHON_DECL exec(char const *string, object global, object local)
{
  // Set suitable default values for global and local dicts.
  if (global.is_none())
  {
    if (PyObject *g = PyEval_GetGlobals())
      global = object(detail::borrowed_reference(g));
    else
      global = dict();
  }
  if (local.is_none()) local = global;
  // should be 'char const *' but older python versions don't use 'const' yet.
  char *s = const_cast<char *>(string);
  PyObject* result = PyRun_String(s, Py_file_input, global.ptr(), local.ptr());
  if (!result) throw_error_already_set();
  return object(detail::new_reference(result));
}

object BOOST_PYTHON_DECL exec_statement(str string, object global, object local)
{
    return exec_statement(python::extract<char const *>(string), global, local);
}

object BOOST_PYTHON_DECL exec_statement(char const *string, object global, object local)
{
  // Set suitable default values for global and local dicts.
  if (global.is_none())
  {
    if (PyObject *g = PyEval_GetGlobals())
      global = object(detail::borrowed_reference(g));
    else
      global = dict();
  }
  if (local.is_none()) local = global;
  // should be 'char const *' but older python versions don't use 'const' yet.
  char *s = const_cast<char *>(string);
  PyObject* result = PyRun_String(s, Py_single_input, global.ptr(), local.ptr());
  if (!result) throw_error_already_set();
  return object(detail::new_reference(result));
}

// Execute python source code from file filename.
// global and local are the global and local scopes respectively,
// used during execution.
object BOOST_PYTHON_DECL exec_file(str filename, object global, object local)
{
    return exec_file(python::extract<char const *>(filename), global, local);
}

object BOOST_PYTHON_DECL exec_file(char const *filename, object global, object local)
{
  // Set suitable default values for global and local dicts.
  if (global.is_none())
  {
    if (PyObject *g = PyEval_GetGlobals())
      global = object(detail::borrowed_reference(g));
    else
      global = dict();
  }
  if (local.is_none()) local = global;
  // should be 'char const *' but older python versions don't use 'const' yet.
  char *f = const_cast<char *>(filename);
#if PY_VERSION_HEX >= 0x03010000
  // Let python manage any UTF bits to avoid potential incompatibilities.
  PyObject *fo = Py_BuildValue("s", f);
  PyObject *fb = Py_None;
  PyUnicode_FSConverter(fo, &fb);
  char *f_as_uft = PyBytes_AsString(fb);
  FILE *fs = fopen(f_as_uft, "r");
  Py_DECREF(fo);
  Py_DECREF(fb);
#elif PY_VERSION_HEX >= 0x03000000
  // Let python open the file to avoid potential binary incompatibilities.
  PyObject *fo = Py_BuildValue("s", f);
  FILE *fs = fopen(fo, "r");
  Py_DECREF(fo);
#else
  // Let python open the file to avoid potential binary incompatibilities.
  PyObject *pyfile = PyFile_FromString(f, const_cast<char*>("r"));
  if (!pyfile) throw std::invalid_argument(std::string(f) + " : no such file");
  python::handle<> file(pyfile);
  FILE *fs = PyFile_AsFile(file.get());
#endif
  PyObject* result = PyRun_File(fs,
                f,
                Py_file_input,
		global.ptr(), local.ptr());
  fclose(fs);
  if (!result) throw_error_already_set();
  return object(detail::new_reference(result));
}

}  // namespace boost::python
}  // namespace boost
