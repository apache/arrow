.. Licensed to the Apache Software Foundation (ASF) under one
.. or more contributor license agreements.  See the NOTICE file
.. distributed with this work for additional information
.. regarding copyright ownership.  The ASF licenses this file
.. to you under the Apache License, Version 2.0 (the
.. "License"); you may not use this file except in compliance
.. with the License.  You may obtain a copy of the License at

..   http://www.apache.org/licenses/LICENSE-2.0

.. Unless required by applicable law or agreed to in writing,
.. software distributed under the License is distributed on an
.. "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
.. KIND, either express or implied.  See the License for the
.. specific language governing permissions and limitations
.. under the License.

.. default-domain:: cpp
.. highlight:: cpp

.. _cpp-flight-async-internals:

=========================================
Developing the Asynchronous Flight Server
=========================================

This page explains how the asynchronous gRPC Flight server
(:class:`arrow::flight::AsyncGenericFlightServerBase`) is built, for
developers changing it. The user-facing guide is :ref:`cpp-flight-async-server`.

Building and layout
===================

The server uses gRPC's generic callback API, which exists from gRPC 1.65.
``transport/grpc/async_grpc_compat.h`` defines
``ARROW_FLIGHT_HAS_ASYNC_SERVER``. CMake and Meson compile the
``async_grpc_*.cc`` sources and the ``arrow-flight-async-grpc-test`` binary
only for gRPC 1.65 or later. With an older gRPC, the transport still registers
an async factory, and that factory answers ``NotImplemented`` so ``Init()``
reports the reason.

All sources live in ``cpp/src/arrow/flight/transport/grpc/``:

==================================  ===========================================
File                                Contents
==================================  ===========================================
``async_grpc_service.cc``           ``AsyncGenericFlightService``: maps a method
                                    name to a reactor
``async_grpc_service_internal.h``   ``AsyncCallContext``, ``AsyncReactorBase``,
                                    the unary reactor
``async_grpc_streaming.cc``         ListActions, DoAction, ListFlights
``async_grpc_do_get.cc``            DoGet
``async_grpc_do_put.cc``            DoPut
``async_grpc_exchange*.cc``         DoExchange: reactor, reader, writer
``async_grpc_handshake.cc``         Handshake
==================================  ===========================================

``arrow/flight/server_async.h`` declares the public handler interfaces, and
``arrow/flight/flight_data_decoder.h`` declares the DoPut listener, its
metadata writer and the push decoder that DoPut and DoExchange share.

Dispatch
========

``AsyncGenericFlightService::CreateReactor()`` runs once per RPC on a gRPC
thread. It builds the call's ``AsyncCallContext`` (running authentication and
server middleware through ``GrpcServerCallContextHelper``), then creates the
reactor for the method. An unknown method and a DoPut whose
``CreateDoPutListener()`` returns ``nullptr`` get a reactor that only finishes
the call as ``UNIMPLEMENTED``. Every other reactor derives from
``AsyncReactorBase``; a handler the server class does not override answers
``NotImplemented`` through that reactor.

Reactor lifetime
================

gRPC forbids deleting a reactor before its ``OnDone()``, but continuations on
application futures may still run after ``OnDone()``. So ``AsyncReactorBase``
counts references (``refs_``):

* The RPC holds the first reference, and ``OnDone()`` releases it.
* Every continuation that may run after ``OnDone()`` captures a
  ``HoldToken`` (``token = hold()``). This is a ``std::unique_ptr`` whose
  deleter releases one reference.
* The last release deletes the reactor.

Never call ``release()`` on a ``HoldToken``: that gives up the pointer without
dropping the reference, and the reactor leaks. ``hold()`` is ``[[nodiscard]]``.

A ``HoldToken`` keeps the reactor alive, but not the gRPC call. That is why the
reactor never starts I/O on a bare ``finished()`` check.

Finishing and writing
=====================

* ``FinishOnce(status)`` finishes the call once: the first status wins, and
  later calls are no-ops. It runs the middleware's ``CallCompleted()`` first,
  outside ``write_mutex_``, because that is application code that may call
  back into the RPC. Then it calls ``Finish()`` under ``write_mutex_``.
* ``WriteIfLive(fn)`` runs ``fn`` (a ``StartWrite()`` or ``StartRead()``)
  under the same mutex, only while the call is unfinished. Either the
  operation is issued before ``Finish()``, or it is not issued at all.

Every continuation that starts I/O goes through ``WriteIfLive()``.

Cancellation
============

``AsyncReactorBase::OnCancel()`` is ``final`` and does two things in order:

1. ``TearDownOnCancel()``: the reactor's own teardown, which settles the
   status with ``FinishOnce()`` and then unwinds (DoGet closes its stream,
   DoExchange wakes its reader and writer, DoPut reports ``OnFinish()``).
2. ``AsyncCallContext::RunCancelCallbacks()``: runs the callbacks handlers
   registered through
   :func:`arrow::flight::AsyncGenericFlightServerBase::AddCancelCallback`.

The callback registry lives behind a ``std::unique_ptr`` inside
``AsyncCallContext``, so the context stays movable from ``CreateReactor()``
into the reactor, and registering never touches gRPC state.
``AddCancelCallback()`` finds the registry with a ``dynamic_cast`` from
``ServerCallContext``, and refuses (``Invalid``) contexts that the async
server did not create.

Per-method notes
================

DoGet
   ``stream_mutex_`` guards the stream, ``closed_`` and ``wrote_schema_``.
   Each ``GetSchemaPayloadAsync()`` / ``NextAsync()`` demand is made under it,
   and ``CloseStreamOnce()`` claims the stream under it. So ``Close()`` never
   overlaps a demand, and runs exactly once. A stream that arrives after
   cancellation is closed by whichever path sees it last.

DoPut
   The reactor is the listener's
   ``internal::FlightDataListenerTransport``. It is published with
   ``SetTransport(listener, this)`` and retired with
   ``SetTransport(listener, nullptr)`` in ``OnDone()``, under the listener's
   mutexes. ``AsyncFlightDataListener::Cancel()`` pins it with ``Hold()``
   under that mutex before calling in. One PutResult write slot is shared by
   ``WriteMetadataAsync()`` and the final acknowledgement, and
   ``OnWriteDone()`` completes the pending metadata future. ``upload_ending_``
   makes the first ending (client, server cancel, failure) decide the
   upload's status.

DoExchange
   ``ExchangeReader`` and ``ExchangeWriter`` reach the reactor through plain
   back-pointers guarded by their own mutex. ``OnDone()`` detaches both
   before releasing the RPC's reference. The reader and writer take a
   ``HoldToken`` under that mutex around each call into the reactor.

Testing
=======

``arrow-flight-async-grpc-test`` holds the transport tests, with shared
fixtures in ``async_grpc_test_fixtures.h``:

* ``StopClientAndServer()`` closes the client and stops the server.
* ``HandBuiltServer`` / ``StartHandBuilt()`` serve a base on a hand-built
  ``grpc::ServerBuilder``.
* ``CallDoPutRaw()`` sends FlightData the client would refuse to build.
* ``WaitFor()`` polls a condition with a bound.

Tests that end an RPC early (cancellation, deadlines, parked handlers) record
``AsyncReactorBase::live_reactors()`` at the start and finish with
``ExpectNoReactorLeak(before)``: a reactor still alive once the server has
stopped is a leaked reference. Run lifetime-sensitive changes repeatedly, for
example:

.. code-block:: shell

   arrow-flight-async-grpc-test --gtest_repeat=20 \
     --gtest_filter='*DoExchange*:*Cancel*:*PendingHandlerFinishes*'
