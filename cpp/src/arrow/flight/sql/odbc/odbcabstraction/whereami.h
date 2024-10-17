// (‑●‑●)> dual licensed under the WTFPL v2 and MIT licenses
//   without any warranty.
//   by Gregory Pakosz (@gpakosz)
// https://github.com/gpakosz/whereami

#ifndef WHEREAMI_H
#define WHEREAMI_H

#ifdef __cplusplus
extern "C" {
#endif

#ifndef WAI_FUNCSPEC
#define WAI_FUNCSPEC
#endif
#ifndef WAI_PREFIX
#define WAI_PREFIX(function) wai_##function
#endif

/**
 * Returns the path to the current executable.
 *
 * Usage:
 *  - first call `int length = wai_getExecutablePath(NULL, 0, NULL);` to
 *    retrieve the length of the path
 *  - allocate the destination buffer with `path = (char*)malloc(length + 1);`
 *  - call `wai_getExecutablePath(path, length, NULL)` again to retrieve the
 *    path
 *  - add a terminal NUL character with `path[length] = '\0';`
 *
 * @param out destination buffer, optional
 * @param capacity destination buffer capacity
 * @param dirname_length optional recipient for the length of the dirname part
 *   of the path.
 *
 * @return the length of the executable path on success (without a terminal NUL
 * character), otherwise `-1`
 */
WAI_FUNCSPEC
int WAI_PREFIX(getExecutablePath)(char* out, int capacity, int* dirname_length);

/**
 * Returns the path to the current module
 *
 * Usage:
 *  - first call `int length = wai_getModulePath(NULL, 0, NULL);` to retrieve
 *    the length  of the path
 *  - allocate the destination buffer with `path = (char*)malloc(length + 1);`
 *  - call `wai_getModulePath(path, length, NULL)` again to retrieve the path
 *  - add a terminal NUL character with `path[length] = '\0';`
 *
 * @param out destination buffer, optional
 * @param capacity destination buffer capacity
 * @param dirname_length optional recipient for the length of the dirname part
 *   of the path.
 *
 * @return the length of the module path on success (without a terminal NUL
 * character), otherwise `-1`
 */
WAI_FUNCSPEC
int WAI_PREFIX(getModulePath)(char* out, int capacity, int* dirname_length);

#ifdef __cplusplus
}
#endif

#endif // #ifndef WHEREAMI_H
