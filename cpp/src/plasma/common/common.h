#ifndef COMMON_H
#define COMMON_H

#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <inttypes.h>
#ifndef _WIN32
#include <execinfo.h>
#endif

#include "utarray.h"
#ifdef __cplusplus
extern "C" {
#endif
#include "sha256.h"
#ifdef __cplusplus
}
#endif

#include "arrow/util/logging.h"

/** Assertion definitions, with optional logging. */

#define CHECK_MSG(COND, M, ...)                                \
  if (!(COND)) {                                               \
    ARROW_LOG(FATAL) << "Check failure: " << M << " " << #COND " " ##__VA_ARGS__; \
  }

#define CHECK(COND) CHECK_MSG(COND, "")

/* Arrow defines the same macro, only define it if it has not already been
 * defined. */
#ifndef UNUSED
#define UNUSED(x) ((void) (x))
#endif

/* These are exit codes for common errors that can occur in Ray components. */
#define EXIT_COULD_NOT_BIND_PORT -2

/** This macro indicates that this pointer owns the data it is pointing to
 *  and is responsible for freeing it. */
#define OWNER

/** Definitions for unique ID types. */
#define UNIQUE_ID_SIZE 20

#define UNIQUE_ID_EQ(id1, id2) (memcmp((id1).id, (id2).id, UNIQUE_ID_SIZE) == 0)

#define IS_NIL_ID(id) UNIQUE_ID_EQ(id, NIL_ID)

typedef struct { unsigned char id[UNIQUE_ID_SIZE]; } UniqueID;

extern const UT_icd object_id_icd;

extern const UniqueID NIL_ID;

/* Generate a globally unique ID. */
UniqueID globally_unique_id(void);

#define NIL_OBJECT_ID NIL_ID

typedef UniqueID ObjectID;

#define ID_STRING_SIZE (2 * UNIQUE_ID_SIZE + 1)

/**
 * Convert an object ID to a hexdecimal string. This function assumes that
 * buffer points to an already allocated char array of size ID_STRING_SIZE. And
 * it writes a null-terminated hex-formatted string to id_string.
 *
 * @param obj_id The object ID to convert to a string.
 * @param id_string A buffer to write the string to. It is assumed that this is
 *        managed by the caller and is sufficiently long to store the object ID
 *        string.
 * @param id_length The length of the id_string buffer.
 */
char *ObjectID_to_string(ObjectID obj_id, char *id_string, int id_length);

/**
 * Compare two object IDs.
 *
 * @param first_id The first object ID to compare.
 * @param second_id The first object ID to compare.
 * @return True if the object IDs are the same and false otherwise.
 */
bool ObjectID_equal(ObjectID first_id, ObjectID second_id);

/**
 * Compare a object ID to the nil ID.
 *
 * @param id The object ID to compare to nil.
 * @return True if the object ID is equal to nil.
 */
bool ObjectID_is_nil(ObjectID id);

/** Definitions for computing hash digests. */
#define DIGEST_SIZE SHA256_BLOCK_SIZE

extern const unsigned char NIL_DIGEST[DIGEST_SIZE];

#endif
