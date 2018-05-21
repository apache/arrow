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
#include <string>
#include <cstring>
#include <iostream>
#include <stdlib.h>
#include <pthread.h>
#include <unistd.h>
#include "plasma/client.h"
#include "org_apache_arrow_plasma_PlasmaClientJNI.h"

using namespace std;
using namespace plasma;
using namespace arrow;

const jsize LEN_OF_OBJECTID = sizeof(ObjectID) / sizeof(jbyte);

inline void jbyteArray_to_object_id(JNIEnv *env, jbyteArray a, ObjectID *oid)
{
    env->GetByteArrayRegion(a, 0, LEN_OF_OBJECTID, (jbyte *) oid);
}

inline void object_id_to_jbyteArray(JNIEnv *env, jbyteArray a, ObjectID *oid)
{
    env->SetByteArrayRegion(a, 0, LEN_OF_OBJECTID, (jbyte *) oid);
}

class JByteArrayGetter {
private:
    JNIEnv *_env;
    jbyteArray _a;
    jbyte *bp;

public:

    JByteArrayGetter(JNIEnv *env, jbyteArray a, jbyte **out) {
        _env = env;
        _a = a;

        bp = _env->GetByteArrayElements(_a, NULL);
        *out = bp;
    }

    ~JByteArrayGetter() {
        _env->ReleaseByteArrayElements(_a, bp, 0);
    }
};

JNIEXPORT jlong JNICALL Java_org_apache_arrow_plasma_PlasmaClientJNI_connect
  (JNIEnv *env, jclass cls, jstring store_socket_name, jstring manager_socket_name, jint release_delay)
{
    const char *s_name = env->GetStringUTFChars(store_socket_name, NULL);
    const char *m_name = env->GetStringUTFChars(manager_socket_name, NULL);

    PlasmaClient *client = new PlasmaClient();
    ARROW_CHECK_OK(
        client->Connect(s_name, m_name, release_delay));

    //fprintf (stdout, "JNI plasma client init, fd = %d\n", client->get_store_fd());
    //fflush (stdout);

    env->ReleaseStringUTFChars(store_socket_name, s_name);
    env->ReleaseStringUTFChars(manager_socket_name, m_name);
    return (long) client;
}

JNIEXPORT void JNICALL Java_org_apache_arrow_plasma_PlasmaClientJNI_disconnect
  (JNIEnv *env, jclass cls, jlong conn)
{
    PlasmaClient *client = (PlasmaClient *) conn;

    //fprintf (stdout, "JNI plasma client disconnect, fd = %d\n", client->get_store_fd());
    //fflush (stdout);

    ARROW_CHECK_OK(client->Disconnect());
    delete client;
    return;
}

JNIEXPORT jobject JNICALL Java_org_apache_arrow_plasma_PlasmaClientJNI_create
  (JNIEnv *env, jclass cls, jlong conn, jbyteArray object_id, jint size, jbyteArray metadata)
{
    PlasmaClient *client = (PlasmaClient *) conn;
    ObjectID oid;
    jbyteArray_to_object_id(env, object_id, &oid);

    // prepare metadata buffer
    uint8_t *md = NULL;
    jsize md_size = 0;
    std::unique_ptr<JByteArrayGetter> md_getter;
    if (metadata != NULL) {
        md_size = env->GetArrayLength(metadata);
    }
    if (md_size > 0) {
        md_getter.reset(new JByteArrayGetter(env, metadata, (jbyte **)&md));
    }

    std::shared_ptr<Buffer> data;
    Status s = client->Create(oid, size, md, md_size, &data);
    if (s.IsPlasmaObjectExists()) {
        jclass Exception = env->FindClass("org/ray/spi/impl/PlasmaObjectExistsException");
        env->ThrowNew(Exception, "An object with this ID already exists in the plasma store.");
        return NULL;
    }
    if (s.IsPlasmaStoreFull()) {
        jclass Exception = env->FindClass("org/ray/spi/impl/PlasmaOutOfMemoryException");
        env->ThrowNew(Exception, "The plasma store ran out of memory and could not create this object.");
        return NULL;
    }
    ARROW_CHECK(s.ok());

    return env->NewDirectByteBuffer(data->mutable_data(), size);
}

JNIEXPORT jbyteArray JNICALL Java_org_apache_arrow_plasma_PlasmaClientJNI_hash
  (JNIEnv *env, jclass cls, jlong conn, jbyteArray object_id)
{
    PlasmaClient *client = (PlasmaClient *) conn;
    ObjectID oid;
    jbyteArray_to_object_id(env, object_id, &oid);

    unsigned char digest[kDigestSize];
    bool success = client->Hash(oid, digest).ok();

    if (success) {
        jbyteArray ret = env->NewByteArray(kDigestSize);
        env->SetByteArrayRegion(ret, 0, kDigestSize, (jbyte *) digest);
        return ret;
    }
    else {
        return NULL;
    }
}

JNIEXPORT void JNICALL Java_org_apache_arrow_plasma_PlasmaClientJNI_seal
  (JNIEnv *env, jclass cls, jlong conn, jbyteArray object_id)
{
    PlasmaClient *client = (PlasmaClient *) conn;
    ObjectID oid;
    jbyteArray_to_object_id(env, object_id, &oid);

    ARROW_CHECK_OK(client->Seal(oid));
}

JNIEXPORT void JNICALL Java_org_apache_arrow_plasma_PlasmaClientJNI_release
  (JNIEnv *env, jclass cls, jlong conn, jbyteArray object_id)
{
    PlasmaClient *client = (PlasmaClient *) conn;
    ObjectID oid;
    jbyteArray_to_object_id(env, object_id, &oid);

    //fprintf (stdout, "JNI plasma client release, fd = %d\n", client->get_store_fd());
    //fflush (stdout);

    ARROW_CHECK_OK(client->Release(oid));
}

JNIEXPORT jobjectArray JNICALL Java_org_apache_arrow_plasma_PlasmaClientJNI_get
  (JNIEnv *env, jclass cls, jlong conn, jobjectArray object_ids, jint timeout_ms)
{
    PlasmaClient *client = (PlasmaClient *) conn;
    //fprintf (stdout, "JNI plasma client get, fd = %d\n", client->get_store_fd());
    //fflush (stdout);

    jsize num_oids = env->GetArrayLength(object_ids);
    std::vector<ObjectID> oids(num_oids);
    std::vector<ObjectBuffer> obufs(num_oids);
    for (int i = 0; i < num_oids; ++i) {
        jbyteArray_to_object_id(env,
                (jbyteArray) env->GetObjectArrayElement(object_ids, i), &oids[i]);
    }
    // TODO: may be blocked. consider to add the thread support
    ARROW_CHECK_OK(
            client->Get(oids.data(), num_oids, timeout_ms, obufs.data()));

    jclass clsByteBuffer = env->FindClass("java/nio/ByteBuffer");
    jclass clsByteBufferArray = env->FindClass("[Ljava/nio/ByteBuffer;");

    jobjectArray ret = env->NewObjectArray(num_oids, clsByteBufferArray, NULL);
    jobjectArray o = NULL;
    jobject dataBuf, metadataBuf;
    for (int i = 0; i < num_oids; ++i) {
        o = env->NewObjectArray(2, clsByteBuffer, NULL);
        if (obufs[i].data && obufs[i].data->size() != -1 ) {
            dataBuf = env->NewDirectByteBuffer(const_cast<uint8_t *>(obufs[i].data->data()), obufs[i].data->size());
            if (obufs[i].metadata && obufs[i].metadata->size() > 0) {
                metadataBuf = env->NewDirectByteBuffer(const_cast<uint8_t *>(obufs[i].metadata->data()), obufs[i].metadata->size());
            }
            else {
                metadataBuf = NULL;
            }
        }
        else {
            dataBuf = NULL;
            metadataBuf = NULL;
        }
        
        env->SetObjectArrayElement(o, 0, dataBuf);
        env->SetObjectArrayElement(o, 1, metadataBuf);
        env->SetObjectArrayElement(ret, i, o);
    }
    return ret;
}

JNIEXPORT jboolean JNICALL Java_org_apache_arrow_plasma_PlasmaClientJNI_contains
  (JNIEnv *env, jclass cls, jlong conn, jbyteArray object_id)
{
    PlasmaClient *client = (PlasmaClient *) conn;
    ObjectID oid;
    jbyteArray_to_object_id(env, object_id, &oid);

    bool has_object;
    ARROW_CHECK_OK(client->Contains(oid, &has_object));

    if (has_object) {
        return true;
    }
    else {
        return false;
    }
}

JNIEXPORT void JNICALL Java_org_apache_arrow_plasma_PlasmaClientJNI_fetch
  (JNIEnv *env, jclass cls, jlong conn, jobjectArray object_ids)
{
    PlasmaClient *client = (PlasmaClient *) conn;
    jsize num_oids = env->GetArrayLength(object_ids);

    /*
    if (!plasma_manager_is_connected(client)) {
        jclass Exception = env->FindClass("java/lang/RuntimeException");
        env->ThrowNew(Exception, "Not connected to the plasma manager.");
        return;
    }
    */

    std::vector<ObjectID> oids(num_oids);
    for (int i = 0; i < num_oids; ++i) {
        jbyteArray_to_object_id(env,
                (jbyteArray) env->GetObjectArrayElement(object_ids, i), &oids[i]);
    }

    ARROW_CHECK_OK(client->Fetch((int) num_oids, oids.data()));

    return;
}

JNIEXPORT jobjectArray JNICALL Java_org_apache_arrow_plasma_PlasmaClientJNI_wait
  (JNIEnv *env, jclass cls, jlong conn, jobjectArray object_ids, jint timeout_ms, jint num_returns)
{
    PlasmaClient *client = (PlasmaClient *) conn;
    jsize num_oids = env->GetArrayLength(object_ids);

    /*
    if (!plasma_manager_is_connected(client)) {
        jclass Exception = env->FindClass("java/lang/RuntimeException");
        env->ThrowNew(Exception, "Not connected to the plasma manager.");
        return NULL;
    }
    */

    if (num_returns < 0) {
        jclass Exception = env->FindClass("java/lang/RuntimeException");
        env->ThrowNew(Exception, "The argument num_returns cannot be less than zero.");
        return NULL;
    }
    if (num_returns > num_oids) {
        jclass Exception = env->FindClass("java/lang/RuntimeException");
        env->ThrowNew(Exception,
                "The argument num_returns cannot be greater than len(object_ids).");
        return NULL;
    }

    std::vector<ObjectRequest> oreqs(num_oids);

    for (int i = 0; i < num_oids; ++i) {
        jbyteArray_to_object_id(env,
                (jbyteArray) env->GetObjectArrayElement(object_ids, i), &oreqs[i].object_id);
        oreqs[i].type = PLASMA_QUERY_ANYWHERE;
    }

    int num_return_objects;
    // TODO: may be blocked. consider to add the thread support
    ARROW_CHECK_OK(client->Wait((int) num_oids, oreqs.data(), num_returns,
                (uint64_t) timeout_ms, &num_return_objects));

    int num_to_return = min(num_return_objects, num_returns);
    jclass clsByteArray = env->FindClass("[B");
    jobjectArray ret = env->NewObjectArray(num_to_return, clsByteArray, NULL);

    int num_returned = 0;
    jbyteArray oid = NULL;
    for (int i = 0; i < num_oids; ++i) {
        if (num_returned >= num_to_return) {
            break;
        }

        if (oreqs[i].status == ObjectStatusLocal
                || oreqs[i].status == ObjectStatusRemote) {
            oid = env->NewByteArray(LEN_OF_OBJECTID);
            object_id_to_jbyteArray(env, oid, &oreqs[i].object_id);
            env->SetObjectArrayElement(ret, num_returned, oid);
            num_returned++;
        }
        else {
            //ARROW_CHECK(oreqs[i].status == ObjectStatus_Nonexistent);
        }
    }
    ARROW_CHECK(num_returned == num_to_return);    

    return ret;
}

JNIEXPORT jlong JNICALL Java_org_apache_arrow_plasma_PlasmaClientJNI_evict
  (JNIEnv *env, jclass cls, jlong conn, jlong num_bytes)
{
    PlasmaClient *client = (PlasmaClient *) conn;

    int64_t evicted_bytes;
    ARROW_CHECK_OK(client->Evict((int64_t) num_bytes, evicted_bytes));

    return (jlong) evicted_bytes;
}

