// (‑●‑●)> dual licensed under the WTFPL v2 and MIT licenses
//   without any warranty.
//   by Gregory Pakosz (@gpakosz)
// https://github.com/gpakosz/whereami

// in case you want to #include "whereami.c" in a larger compilation unit
#if !defined(WHEREAMI_H)
#include "whereami.h"
#endif

#ifdef __cplusplus
extern "C" {
#endif

#if defined(__linux__) || defined(__CYGWIN__)
#undef _DEFAULT_SOURCE
#define _DEFAULT_SOURCE
#elif defined(__APPLE__)
#undef _DARWIN_C_SOURCE
#define _DARWIN_C_SOURCE
#define _DARWIN_BETTER_REALPATH
#endif

#if !defined(WAI_MALLOC) || !defined(WAI_FREE) || !defined(WAI_REALLOC)
#include <stdlib.h>
#endif

#if !defined(WAI_MALLOC)
#define WAI_MALLOC(size) malloc(size)
#endif

#if !defined(WAI_FREE)
#define WAI_FREE(p) free(p)
#endif

#if !defined(WAI_REALLOC)
#define WAI_REALLOC(p, size) realloc(p, size)
#endif

#ifndef WAI_NOINLINE
#if defined(_MSC_VER)
#define WAI_NOINLINE __declspec(noinline)
#elif defined(__GNUC__)
#define WAI_NOINLINE __attribute__((noinline))
#else
#error unsupported compiler
#endif
#endif

#if defined(_MSC_VER)
#define WAI_RETURN_ADDRESS() _ReturnAddress()
#elif defined(__GNUC__)
#define WAI_RETURN_ADDRESS() __builtin_extract_return_addr(__builtin_return_address(0))
#else
#error unsupported compiler
#endif

#if defined(_WIN32)

#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#if defined(_MSC_VER)
#pragma warning(push, 3)
#endif
#include <windows.h>
#include <intrin.h>
#if defined(_MSC_VER)
#pragma warning(pop)
#endif
#include <stdbool.h>

static int WAI_PREFIX(getModulePath_)(HMODULE module, char* out, int capacity, int* dirname_length)
{
  wchar_t buffer1[MAX_PATH];
  wchar_t buffer2[MAX_PATH];
  wchar_t* path = NULL;
  int length = -1;
  bool ok;

  for (ok = false; !ok; ok = true)
  {
    DWORD size;
    int length_, length__;

    size = GetModuleFileNameW(module, buffer1, sizeof(buffer1) / sizeof(buffer1[0]));

    if (size == 0)
      break;
    else if (size == (DWORD)(sizeof(buffer1) / sizeof(buffer1[0])))
    {
      DWORD size_ = size;
      do
      {
        wchar_t* path_;

        path_ = (wchar_t*)WAI_REALLOC(path, sizeof(wchar_t) * size_ * 2);
        if (!path_)
          break;
        size_ *= 2;
        path = path_;
        size = GetModuleFileNameW(module, path, size_);
      }
      while (size == size_);

      if (size == size_)
        break;
    }
    else
      path = buffer1;

    if (!_wfullpath(buffer2, path, MAX_PATH))
      break;
    length_ = (int)wcslen(buffer2);
    length__ = WideCharToMultiByte(CP_UTF8, 0, buffer2, length_ , out, capacity, NULL, NULL);

    if (length__ == 0)
      length__ = WideCharToMultiByte(CP_UTF8, 0, buffer2, length_, NULL, 0, NULL, NULL);
    if (length__ == 0)
      break;

    if (length__ <= capacity && dirname_length)
    {
      int i;

      for (i = length__ - 1; i >= 0; --i)
      {
        if (out[i] == '\\')
        {
          *dirname_length = i;
          break;
        }
      }
    }

    length = length__;
  }

  if (path != buffer1)
    WAI_FREE(path);

  return ok ? length : -1;
}

WAI_NOINLINE WAI_FUNCSPEC
int WAI_PREFIX(getExecutablePath)(char* out, int capacity, int* dirname_length)
{
  return WAI_PREFIX(getModulePath_)(NULL, out, capacity, dirname_length);
}

WAI_NOINLINE WAI_FUNCSPEC
int WAI_PREFIX(getModulePath)(char* out, int capacity, int* dirname_length)
{
  HMODULE module;
  int length = -1;

#if defined(_MSC_VER)
#pragma warning(push)
#pragma warning(disable: 4054)
#endif
  if (GetModuleHandleEx(GET_MODULE_HANDLE_EX_FLAG_FROM_ADDRESS | GET_MODULE_HANDLE_EX_FLAG_UNCHANGED_REFCOUNT, (LPCTSTR)WAI_RETURN_ADDRESS(), &module))
#if defined(_MSC_VER)
#pragma warning(pop)
#endif
  {
    length = WAI_PREFIX(getModulePath_)(module, out, capacity, dirname_length);
  }

  return length;
}

#elif defined(__linux__) || defined(__CYGWIN__) || defined(__sun) || defined(WAI_USE_PROC_SELF_EXE)

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#if defined(__linux__)
#include <linux/limits.h>
#else
#include <limits.h>
#endif
#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif
#include <inttypes.h>
#include <stdbool.h>

#if !defined(WAI_PROC_SELF_EXE)
#if defined(__sun)
#define WAI_PROC_SELF_EXE "/proc/self/path/a.out"
#else
#define WAI_PROC_SELF_EXE "/proc/self/exe"
#endif
#endif

WAI_FUNCSPEC
int WAI_PREFIX(getExecutablePath)(char* out, int capacity, int* dirname_length)
{
  char buffer[PATH_MAX];
  char* resolved = NULL;
  int length = -1;
  bool ok;

  for (ok = false; !ok; ok = true)
  {
    resolved = realpath(WAI_PROC_SELF_EXE, buffer);
    if (!resolved)
      break;

    length = (int)strlen(resolved);
    if (length <= capacity)
    {
      memcpy(out, resolved, length);

      if (dirname_length)
      {
        int i;

        for (i = length - 1; i >= 0; --i)
        {
          if (out[i] == '/')
          {
            *dirname_length = i;
            break;
          }
        }
      }
    }
  }

  return ok ? length : -1;
}

#if !defined(WAI_PROC_SELF_MAPS_RETRY)
#define WAI_PROC_SELF_MAPS_RETRY 5
#endif

#if !defined(WAI_PROC_SELF_MAPS)
#if defined(__sun)
#define WAI_PROC_SELF_MAPS "/proc/self/map"
#else
#define WAI_PROC_SELF_MAPS "/proc/self/maps"
#endif
#endif

#if defined(__ANDROID__) || defined(ANDROID)
#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>
#endif
#include <stdbool.h>

WAI_NOINLINE WAI_FUNCSPEC
int WAI_PREFIX(getModulePath)(char* out, int capacity, int* dirname_length)
{
  int length = -1;
  FILE* maps = NULL;

  for (int r = 0; r < WAI_PROC_SELF_MAPS_RETRY; ++r)
  {
    maps = fopen(WAI_PROC_SELF_MAPS, "r");
    if (!maps)
      break;

    for (;;)
    {
      char buffer[PATH_MAX < 1024 ? 1024 : PATH_MAX];
      uint64_t low, high;
      char perms[5];
      uint64_t offset;
      uint32_t major, minor;
      char path[PATH_MAX];
      uint32_t inode;

      if (!fgets(buffer, sizeof(buffer), maps))
        break;

      if (sscanf(buffer, "%" PRIx64 "-%" PRIx64 " %s %" PRIx64 " %x:%x %u %s\n", &low, &high, perms, &offset, &major, &minor, &inode, path) == 8)
      {
        uint64_t addr = (uintptr_t)WAI_RETURN_ADDRESS();
        if (low <= addr && addr <= high)
        {
          char* resolved;

          resolved = realpath(path, buffer);
          if (!resolved)
            break;

          length = (int)strlen(resolved);
#if defined(__ANDROID__) || defined(ANDROID)
          if (length > 4
              &&buffer[length - 1] == 'k'
              &&buffer[length - 2] == 'p'
              &&buffer[length - 3] == 'a'
              &&buffer[length - 4] == '.')
          {
            int fd = open(path, O_RDONLY);
            if (fd == -1)
            {
              length = -1; // retry
              break;
            }

            char* begin = (char*)mmap(0, offset, PROT_READ, MAP_SHARED, fd, 0);
            if (begin == MAP_FAILED)
            {
              close(fd);
              length = -1; // retry
              break;
            }

            char* p = begin + offset - 30; // minimum size of local file header
            while (p >= begin) // scan backwards
            {
              if (*((uint32_t*)p) == 0x04034b50UL) // local file header signature found
              {
                uint16_t length_ = *((uint16_t*)(p + 26));

                if (length + 2 + length_ < (int)sizeof(buffer))
                {
                  memcpy(&buffer[length], "!/", 2);
                  memcpy(&buffer[length + 2], p + 30, length_);
                  length += 2 + length_;
                }

                break;
              }

              --p;
            }

            munmap(begin, offset);
            close(fd);
          }
#endif
          if (length <= capacity)
          {
            memcpy(out, resolved, length);

            if (dirname_length)
            {
              int i;

              for (i = length - 1; i >= 0; --i)
              {
                if (out[i] == '/')
                {
                  *dirname_length = i;
                  break;
                }
              }
            }
          }

          break;
        }
      }
    }

    fclose(maps);
    maps = NULL;

    if (length != -1)
      break;
  }

  return length;
}

#elif defined(__APPLE__)

#include <mach-o/dyld.h>
#include <limits.h>
#include <stdlib.h>
#include <string.h>
#include <dlfcn.h>
#include <stdbool.h>

WAI_FUNCSPEC
int WAI_PREFIX(getExecutablePath)(char* out, int capacity, int* dirname_length)
{
  char buffer1[PATH_MAX];
  char buffer2[PATH_MAX];
  char* path = buffer1;
  char* resolved = NULL;
  int length = -1;
  bool ok;

  for (ok = false; !ok; ok = true)
  {
    uint32_t size = (uint32_t)sizeof(buffer1);
    if (_NSGetExecutablePath(path, &size) == -1)
    {
      path = (char*)WAI_MALLOC(size);
      if (!_NSGetExecutablePath(path, &size))
        break;
    }

    resolved = realpath(path, buffer2);
    if (!resolved)
      break;

    length = (int)strlen(resolved);
    if (length <= capacity)
    {
      memcpy(out, resolved, length);

      if (dirname_length)
      {
        int i;

        for (i = length - 1; i >= 0; --i)
        {
          if (out[i] == '/')
          {
            *dirname_length = i;
            break;
          }
        }
      }
    }
  }

  if (path != buffer1)
    WAI_FREE(path);

  return ok ? length : -1;
}

WAI_NOINLINE WAI_FUNCSPEC
int WAI_PREFIX(getModulePath)(char* out, int capacity, int* dirname_length)
{
  char buffer[PATH_MAX];
  char* resolved = NULL;
  int length = -1;

  for(;;)
  {
    Dl_info info;

    if (dladdr(WAI_RETURN_ADDRESS(), &info))
    {
      resolved = realpath(info.dli_fname, buffer);
      if (!resolved)
        break;

      length = (int)strlen(resolved);
      if (length <= capacity)
      {
        memcpy(out, resolved, length);

        if (dirname_length)
        {
          int i;

          for (i = length - 1; i >= 0; --i)
          {
            if (out[i] == '/')
            {
              *dirname_length = i;
              break;
            }
          }
        }
      }
    }

    break;
  }

  return length;
}

#elif defined(__QNXNTO__)

#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <dlfcn.h>
#include <stdbool.h>

#if !defined(WAI_PROC_SELF_EXE)
#define WAI_PROC_SELF_EXE "/proc/self/exefile"
#endif

WAI_FUNCSPEC
int WAI_PREFIX(getExecutablePath)(char* out, int capacity, int* dirname_length)
{
  char buffer1[PATH_MAX];
  char buffer2[PATH_MAX];
  char* resolved = NULL;
  FILE* self_exe = NULL;
  int length = -1;
  bool ok;

  for (ok = false; !ok; ok = true)
  {
    self_exe = fopen(WAI_PROC_SELF_EXE, "r");
    if (!self_exe)
      break;

    if (!fgets(buffer1, sizeof(buffer1), self_exe))
      break;

    resolved = realpath(buffer1, buffer2);
    if (!resolved)
      break;

    length = (int)strlen(resolved);
    if (length <= capacity)
    {
      memcpy(out, resolved, length);

      if (dirname_length)
      {
        int i;

        for (i = length - 1; i >= 0; --i)
        {
          if (out[i] == '/')
          {
            *dirname_length = i;
            break;
          }
        }
      }
    }
  }

  fclose(self_exe);

  return ok ? length : -1;
}

WAI_FUNCSPEC
int WAI_PREFIX(getModulePath)(char* out, int capacity, int* dirname_length)
{
  char buffer[PATH_MAX];
  char* resolved = NULL;
  int length = -1;

  for(;;)
  {
    Dl_info info;

    if (dladdr(WAI_RETURN_ADDRESS(), &info))
    {
      resolved = realpath(info.dli_fname, buffer);
      if (!resolved)
        break;

      length = (int)strlen(resolved);
      if (length <= capacity)
      {
        memcpy(out, resolved, length);

        if (dirname_length)
        {
          int i;

          for (i = length - 1; i >= 0; --i)
          {
            if (out[i] == '/')
            {
              *dirname_length = i;
              break;
            }
          }
        }
      }
    }

    break;
  }

  return length;
}

#elif defined(__DragonFly__) || defined(__FreeBSD__) || \
      defined(__FreeBSD_kernel__) || defined(__NetBSD__) || defined(__OpenBSD__)

#include <limits.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/sysctl.h>
#include <dlfcn.h>
#include <stdbool.h>

#if defined(__OpenBSD__)

#include <unistd.h>

WAI_FUNCSPEC
int WAI_PREFIX(getExecutablePath)(char* out, int capacity, int* dirname_length)
{
  char buffer1[4096];
  char buffer2[PATH_MAX];
  char buffer3[PATH_MAX];
  char** argv = (char**)buffer1;
  char* resolved = NULL;
  int length = -1;
  bool ok;

  for (ok = false; !ok; ok = true)
  {
    int mib[4] = { CTL_KERN, KERN_PROC_ARGS, getpid(), KERN_PROC_ARGV };
    size_t size;

    if (sysctl(mib, 4, NULL, &size, NULL, 0) != 0)
        break;

    if (size > sizeof(buffer1))
    {
      argv = (char**)WAI_MALLOC(size);
      if (!argv)
        break;
    }

    if (sysctl(mib, 4, argv, &size, NULL, 0) != 0)
        break;

    if (strchr(argv[0], '/'))
    {
      resolved = realpath(argv[0], buffer2);
      if (!resolved)
        break;
    }
    else
    {
      const char* PATH = getenv("PATH");
      if (!PATH)
        break;

      size_t argv0_length = strlen(argv[0]);

      const char* begin = PATH;
      while (1)
      {
        const char* separator = strchr(begin, ':');
        const char* end = separator ? separator : begin + strlen(begin);

        if (end - begin > 0)
        {
          if (*(end -1) == '/')
            --end;

          if (((end - begin) + 1 + argv0_length + 1) <= sizeof(buffer2))
          {
            memcpy(buffer2, begin, end - begin);
            buffer2[end - begin] = '/';
            memcpy(buffer2 + (end - begin) + 1, argv[0], argv0_length + 1);

            resolved = realpath(buffer2, buffer3);
            if (resolved)
              break;
          }
        }

        if (!separator)
          break;

        begin = ++separator;
      }

      if (!resolved)
        break;
    }

    length = (int)strlen(resolved);
    if (length <= capacity)
    {
      memcpy(out, resolved, length);

      if (dirname_length)
      {
        int i;

        for (i = length - 1; i >= 0; --i)
        {
          if (out[i] == '/')
          {
            *dirname_length = i;
            break;
          }
        }
      }
    }
  }

  if (argv != (char**)buffer1)
    WAI_FREE(argv);

  return ok ? length : -1;
}

#else

WAI_FUNCSPEC
int WAI_PREFIX(getExecutablePath)(char* out, int capacity, int* dirname_length)
{
  char buffer1[PATH_MAX];
  char buffer2[PATH_MAX];
  char* path = buffer1;
  char* resolved = NULL;
  int length = -1;
  bool ok;

  for (ok = false; !ok; ok = true)
  {
#if defined(__NetBSD__)
    int mib[4] = { CTL_KERN, KERN_PROC_ARGS, -1, KERN_PROC_PATHNAME };
#else
    int mib[4] = { CTL_KERN, KERN_PROC, KERN_PROC_PATHNAME, -1 };
#endif
    size_t size = sizeof(buffer1);

    if (sysctl(mib, 4, path, &size, NULL, 0) != 0)
        break;

    resolved = realpath(path, buffer2);
    if (!resolved)
      break;

    length = (int)strlen(resolved);
    if (length <= capacity)
    {
      memcpy(out, resolved, length);

      if (dirname_length)
      {
        int i;

        for (i = length - 1; i >= 0; --i)
        {
          if (out[i] == '/')
          {
            *dirname_length = i;
            break;
          }
        }
      }
    }
  }

  return ok ? length : -1;
}

#endif

WAI_NOINLINE WAI_FUNCSPEC
int WAI_PREFIX(getModulePath)(char* out, int capacity, int* dirname_length)
{
  char buffer[PATH_MAX];
  char* resolved = NULL;
  int length = -1;

  for(;;)
  {
    Dl_info info;

    if (dladdr(WAI_RETURN_ADDRESS(), &info))
    {
      resolved = realpath(info.dli_fname, buffer);
      if (!resolved)
        break;

      length = (int)strlen(resolved);
      if (length <= capacity)
      {
        memcpy(out, resolved, length);

        if (dirname_length)
        {
          int i;

          for (i = length - 1; i >= 0; --i)
          {
            if (out[i] == '/')
            {
              *dirname_length = i;
              break;
            }
          }
        }
      }
    }

    break;
  }

  return length;
}

#else

#error unsupported platform

#endif

#ifdef __cplusplus
}
#endif
