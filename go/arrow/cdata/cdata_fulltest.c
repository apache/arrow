// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// +build cgo
// +build test

#include <assert.h>
#include <string.h>
#include <stdlib.h>
#include <stdbool.h>
#include <stdint.h>
#include "arrow/c/abi.h"
#include "arrow/c/helpers.h"
#include "utils.h"

static const int64_t kDefaultFlags = ARROW_FLAG_NULLABLE;

static void release_int32_type(struct ArrowSchema* schema) {
    // mark released
    schema->release = NULL;
}

void export_int32_type(struct ArrowSchema* schema) {
    const char* encoded_metadata;
    if (is_little_endian() == 1) {
        encoded_metadata = kEncodedMeta1LE;
    } else {
        encoded_metadata = kEncodedMeta1BE;
    }
    *schema = (struct ArrowSchema) {
        // Type description
        .format = "i",
        .name = "",
        .metadata = encoded_metadata,
        .flags = 0,
        .n_children = 0,
        .children = NULL,
        .dictionary = NULL,
        // bookkeeping
        .release = &release_int32_type,
    };
}

static bool test1_released = false;

int test1_is_released() { return test1_released; }

static void release_int32_array(struct ArrowArray* array) {
    assert(array->n_buffers == 2);
    // free the buffers and buffers array
    free((void *) array->buffers[1]);
    free(array->buffers);
    // mark released
    array->release = NULL;
    test1_released = true;
}

void export_int32_array(const int32_t* data, int64_t nitems, struct ArrowArray* array) {
    // initialize primitive fields
    *array = (struct ArrowArray) {
        .length = nitems,
        .offset = 0,
        .null_count = 0,
        .n_buffers = 2,
        .n_children = 0,
        .children = NULL,
        .dictionary = NULL,
        // bookkeeping
        .release = &release_int32_array
    };

    // allocate list of buffers
    array->buffers = (const void**)malloc(sizeof(void*) * array->n_buffers);
    assert(array->buffers != NULL);
    array->buffers[0] = NULL; // no nulls, null bitmap can be omitted
    array->buffers[1] = data;
}


static void release_primitive(struct ArrowSchema* schema) {
    free((void *)schema->format);
    schema->release = NULL;
}

static void release_nested_internal(struct ArrowSchema* schema,
                                    int is_dynamic) {
    assert(!ArrowSchemaIsReleased(schema));
    for (int i = 0; i < schema->n_children; ++i) {
        ArrowSchemaRelease(schema->children[i]);
        free(schema->children[i]);
    }
    if (is_dynamic) {
        free((void*)schema->format);
        free((void*)schema->name);
    }
    ArrowSchemaMarkReleased(schema);
}

static void release_nested_static(struct ArrowSchema* schema) {
    release_nested_internal(schema, /*is_dynamic=*/0);
}

static void release_nested_dynamic(struct ArrowSchema* schema) {
    release_nested_internal(schema, /*is_dynamic=*/1);
}

static void release_nested_dynamic_toplevel(struct ArrowSchema* schema) {
    assert(!ArrowSchemaIsReleased(schema));
    for (int i = 0; i < schema->n_children; ++i) {
        ArrowSchemaRelease(schema->children[i]);
        free(schema->children[i]);
    }
    free((void*)schema->format);
    if (strlen(schema->name) > 0) {
        free((void*)schema->name);
    }
    ArrowSchemaMarkReleased(schema);
}

void test_primitive(struct ArrowSchema* schema, const char* fmt) {
    *schema = (struct ArrowSchema) {
        // Type description
        .format = fmt,
        .name = "",
        .metadata = NULL,
        .flags = 0,
        .n_children = 0,
        .children = NULL,
        .dictionary = NULL,
        // bookkeeping
        .release = &release_primitive,
    };
}

// Since test_lists et al. allocate an entirely array of ArrowSchema pointers,
// need to expose a function to free it.
void free_malloced_schemas(struct ArrowSchema** schemas) {
    free(schemas);
}

struct ArrowSchema** test_lists(const char** fmts, const char** names, const int* nullflags, const int n) {
    struct ArrowSchema** schemas = malloc(sizeof(struct ArrowSchema*)*n);
    for (int i = 0; i < n; ++i) {
        schemas[i] = malloc(sizeof(struct ArrowSchema));
        *schemas[i] = (struct ArrowSchema) {
            .format = fmts[i],
            .name = names[i],
            .metadata = NULL,
            .flags = 0,
            .children = NULL,
            .n_children = 0,
            .dictionary = NULL,
            .release = &release_nested_dynamic,
        };
        if (i != 0) {
            schemas[i-1]->n_children = 1;
            schemas[i-1]->children = &schemas[i];
            schemas[i]->flags = nullflags[i-1];
        }
    }
    return schemas;
}

struct ArrowSchema** fill_structs(const char** fmts, const char** names, int64_t* flags, const int n) {
    struct ArrowSchema** schemas = malloc(sizeof(struct ArrowSchema*)*n);
    for (int i = 0; i < n; ++i) {
        schemas[i] = malloc(sizeof(struct ArrowSchema));
        *schemas[i] = (struct ArrowSchema) {
            .format = fmts[i],
            .name = names[i],
            .metadata = NULL,
            .flags = flags[i],
            .children = NULL,
            .n_children = 0,
            .dictionary = NULL,
            .release = &release_nested_dynamic,
        };
    }

    schemas[0]->children = &schemas[1];
    schemas[0]->n_children = n-1;
    return schemas;
}

struct ArrowSchema** test_struct(const char** fmts, const char** names, int64_t* flags, const int n) {
    struct ArrowSchema** schemas = fill_structs(fmts, names, flags, n);

    if (is_little_endian() == 1) {
        schemas[n-1]->metadata = kEncodedMeta2LE;
    } else {
        schemas[n-1]->metadata = kEncodedMeta2BE;
    }

    return schemas;
}

struct ArrowSchema** test_schema(const char** fmts, const char** names, int64_t* flags, const int n) {
    struct ArrowSchema** schemas = fill_structs(fmts, names, flags, n);

    if (is_little_endian() == 1) {
        schemas[0]->metadata = kEncodedMeta2LE;
        schemas[n-1]->metadata = kEncodedMeta1LE;
    } else {
        schemas[0]->metadata = kEncodedMeta2BE;
        schemas[n-1]->metadata = kEncodedMeta1BE;
    }
    return schemas;
}

struct ArrowSchema** test_map(const char** fmts, const char** names, int64_t* flags, const int n) {
    struct ArrowSchema** schemas = malloc(sizeof(struct ArrowSchema*)*n);
    for (int i = 0; i < n; ++i) {
        schemas[i] = malloc(sizeof(struct ArrowSchema));
        *schemas[i] = (struct ArrowSchema) {
            .format = fmts[i],
            .name = names[i],
            .metadata = NULL,
            .flags = flags[i],
            .children = NULL,
            .n_children = 0,
            .dictionary = NULL,
            .release = &release_nested_dynamic,
        };
    }

    schemas[0]->n_children = 1;
    schemas[0]->children = &schemas[1];
    schemas[1]->n_children = n-2;
    schemas[1]->children = &schemas[2];

    return schemas;
}

struct ArrowSchema** test_union(const char** fmts, const char** names, int64_t* flags, const int n) {
    struct ArrowSchema** schemas = malloc(sizeof(struct ArrowSchema*)*n);
     for (int i = 0; i < n; ++i) {
        schemas[i] = malloc(sizeof(struct ArrowSchema));
        *schemas[i] = (struct ArrowSchema) {
            .format = fmts[i],
            .name = names[i],
            .metadata = NULL,
            .flags = flags[i],
            .children = NULL,
            .n_children = 0,
            .dictionary = NULL,
            .release = &release_nested_dynamic,
        };
    }

    schemas[0]->n_children = n-1;
    schemas[0]->children = &schemas[1];
    return schemas;
}

struct streamcounter {
    int n;
    int max;
};

static int stream_schema(struct ArrowArrayStream* st, struct ArrowSchema* out) {
    out->children = malloc(sizeof(struct ArrowSchema*)*2);
    out->n_children = 2;

    out->children[0] = malloc(sizeof(struct ArrowSchema));
    *out->children[0] = (struct ArrowSchema) {
        .format = "i",
        .name = "a",
        .metadata = NULL,
        .flags = ARROW_FLAG_NULLABLE,
        .children = NULL,
        .n_children = 0,
        .dictionary = NULL,
        .release = &release_nested_static,
    };

    out->children[1] = malloc(sizeof(struct ArrowSchema));
    *out->children[1] = (struct ArrowSchema) {
        .format = "u",
        .name = "b",
        .metadata = NULL,
        .flags = ARROW_FLAG_NULLABLE,
        .children = NULL,
        .n_children = 0,
        .dictionary = NULL,
        .release = &release_nested_static,
    };

    out->format = "+s";
    out->release = &release_nested_static;

    return 0;
}

static void release_stream(struct ArrowArrayStream* st) {
    free(st->private_data);
    ArrowArrayStreamMarkReleased(st);
}

static void release_the_array(struct ArrowArray* out) {
    for (int i = 0; i < out->n_children; ++i) {
        ArrowArrayRelease(out->children[i]);
    }
    free((void*)out->children);
    free(out->buffers);
    out->release = NULL;
}

void export_int32_array(const int32_t*, int64_t, struct ArrowArray*);

static void release_str_array(struct ArrowArray* array) {
    assert(array->n_buffers == 3);
    free((void*) array->buffers[1]);
    free((void*) array->buffers[2]);
    free(array->buffers);
    array->release = NULL;
}

void export_str_array(const char* data, const int32_t* offsets, int64_t nitems, struct ArrowArray* out) {
    *out = (struct ArrowArray) {
        .length = nitems,
        .offset = 0,
        .null_count = 0,
        .n_buffers = 3,
        .n_children = 0,
        .children = NULL,
        .dictionary = NULL,
        // bookkeeping
        .release = &release_str_array
    };

    out->buffers = (const void**)malloc(sizeof(void*) * out->n_buffers);
    assert(out->buffers != NULL);
    out->buffers[0] = NULL;
    out->buffers[1] = offsets;
    out->buffers[2] = data;
}

static int next_record(struct ArrowArrayStream* st, struct ArrowArray* out) {
    struct streamcounter* cnter = (struct streamcounter*)(st->private_data);
    if (cnter->n == cnter->max) {
        ArrowArrayMarkReleased(out);
        return 0;
    }

    cnter->n++;

    *out = (struct ArrowArray) {
        .offset = 0,
        .dictionary = NULL,
        .length = 3,
        .null_count = 0,
        .buffers = (const void**)malloc(sizeof(void*)),
        .n_children = 2,
        .n_buffers = 1,
        .release = &release_the_array
    };

    out->buffers[0] = NULL;
    out->children = (struct ArrowArray**)malloc(sizeof(struct ArrowArray*)*2);
    int32_t* intdata = malloc(sizeof(int32_t)*3);
    for (int i = 0; i < 3; ++i) {
        intdata[i] = cnter->n * (i+1);
    }

    out->children[0] = malloc(sizeof(struct ArrowArray));
    export_int32_array(intdata, 3, out->children[0]);
    out->children[1] = malloc(sizeof(struct ArrowArray));
    char* strdata = strdup("foobarbaz");
    int32_t* offsets = malloc(sizeof(int32_t)*4);
    offsets[0] = 0;
    offsets[1] = 3;
    offsets[2] = 6;
    offsets[3] = 9;
    export_str_array(strdata, offsets, 3, out->children[1]);

    return 0;
}

void setup_array_stream_test(const int n_batches, struct ArrowArrayStream* out) {
    struct streamcounter* cnt = malloc(sizeof(struct streamcounter));
    cnt->max = n_batches;
    cnt->n = 0;

    out->get_next = &next_record;
    out->get_schema = &stream_schema;
    out->release = &release_stream;
    out->private_data = cnt;
}

int test_exported_stream(struct ArrowArrayStream* stream) {
  while (1) {
    struct ArrowArray array;
    // Garbage - implementation should not try to call it, though!
    array.release = (void*)0xDEADBEEF;
    int rc = stream->get_next(stream, &array);
    if (rc != 0) return rc;

    if (array.release == NULL) {
      stream->release(stream);
      break;
    }
  }
  return 0;
}
