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
#include "arrow/c/abi.h"
#include "arrow/c/helpers.h"
#include "utils.h"

static void release_primitive(struct ArrowSchema* schema) {
    free((void *)schema->format);    
    schema->release = NULL;
}

static void release_nested(struct ArrowSchema* schema) {
    for (int i = 0; i < schema->n_children; ++i) {
        assert(!ArrowSchemaIsReleased(schema->children[i]));
        ArrowSchemaRelease(schema->children[i]);
        ArrowSchemaMarkReleased(schema->children[i]);
    }
    assert(!ArrowSchemaIsReleased(schema));
    free((void *)schema->format);
    if (strlen(schema->name) > 0) {
        free((void *)schema->name);
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

void free_nested_schemas(struct ArrowSchema* top) {    
    for (int i = 0; i < top->n_children; ++i) {
        free_nested_schemas(top->children[i]);
    }
    ArrowSchemaMarkReleased(top);
    // free(top);
}

void free_top_schema(struct ArrowSchema** top) {
    free_nested_schemas(top[0]);
    free(top);
}

struct ArrowSchema** test_lists(const char** fmts, const char** names, const int n) {
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
            .release = &release_nested,
        };
        if (i != 0) {
            schemas[i-1]->n_children = 1;
            schemas[i-1]->children = &schemas[i];
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
            .release = &release_nested,
        };
    }

    schemas[0]->children = &schemas[1];
    schemas[0]->n_children = n-1;
    return schemas;
}

struct ArrowSchema** test_struct(const char** fmts, const char** names, int64_t* flags, const int n) {
    struct ArrowSchema** schemas = fill_structs(fmts, names, flags, n);

    if (check_for_endianness() == 1) {
        schemas[n-1]->metadata = kEncodedMeta2LE;
    } else {
        schemas[n-1]->metadata = kEncodedMeta2BE;
    }

    return schemas;
}

struct ArrowSchema** test_schema(const char** fmts, const char** names, int64_t* flags, const int n) {
    struct ArrowSchema** schemas = fill_structs(fmts, names, flags, n);

    if (check_for_endianness() == 1) {
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
            .release = &release_nested,
        };
    }

    schemas[0]->n_children = 1;
    schemas[0]->children = &schemas[1];
    schemas[1]->n_children = n-2;
    schemas[1]->children = &schemas[2];

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
        .release = &release_nested,
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
        .release = &release_nested,
    };

    out->format = "+s";
    out->release = &free_nested_schemas;

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
