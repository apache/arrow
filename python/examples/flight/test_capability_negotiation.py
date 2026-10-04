# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Protocol and localhost integration tests for the experimental example."""

import copy
import json
import time

import pyarrow as pa
import pyarrow.flight as flight
import pytest

import capability_negotiation as cn


def offer(versions=(1, 2), limit=1024, required=False):
    return {
        "protocol": 1,
        "features": {"example.feature": {
            "versions": list(versions), "limits": {"max_bytes": limit}}},
        "required": ["example.feature"] if required else [],
    }


def encoded(value):
    return json.dumps(value).encode("utf-8")


def test_select_highest_common_version_and_minimum_limits():
    client = offer((1, 3, 5), 1024, required=True)
    server = offer((3, 1, 6), 512)
    original = copy.deepcopy((client, server))
    selection = cn.select_capabilities(client, server)
    assert selection == {
        "protocol": 1,
        "features": {"example.feature": {
            "version": 3, "limits": {"max_bytes": 512}}},
    }
    assert (client, server) == original
    assert cn.decode_selection(encoded(selection), client) == selection


def test_unknown_optional_features_are_ignored():
    client = offer()
    server = offer()
    client["features"]["future.client"] = {"versions": [1], "limits": {}}
    server["features"]["future.server"] = {"versions": [1], "limits": {}}
    assert set(cn.select_capabilities(client, server)["features"]) == {
        "example.feature"}


@pytest.mark.parametrize("incompatible", ["unknown", "version", "limits"])
@pytest.mark.parametrize("required_side", [None, "client", "server"])
def test_incompatible_features(incompatible, required_side):
    client, server = offer(), offer()
    if incompatible == "unknown":
        server["features"]["other.feature"] = server["features"].pop(
            "example.feature")
    elif incompatible == "version":
        server["features"]["example.feature"]["versions"] = [3]
    else:
        server["features"]["example.feature"]["limits"] = {"other_limit": 10}
    if required_side == "client":
        client["required"] = list(client["features"])
    elif required_side == "server":
        server["required"] = list(server["features"])
    if required_side:
        with pytest.raises(cn.NegotiationError, match="required features"):
            cn.select_capabilities(client, server)
    else:
        assert cn.select_capabilities(client, server)["features"] == {}


@pytest.mark.parametrize("body", [
    b"", b"[]", b"null", b"\xff", b'{"protocol":1,"protocol":1}',
    b'{"protocol":NaN}', b'{"protocol":Infinity}', b"{" * 1200,
    b"[" * 1200 + b"]" * 1200,
    b" " * (cn.MAX_BODY_BYTES + 1),
])
def test_malformed_documents(body):
    with pytest.raises(cn.NegotiationError):
        cn.decode_offer(body)


@pytest.mark.parametrize("versions", [[], [True], [0], [-1], [1.0], ["1"],
                                      [1, 1], [cn.MAX_INTEGER + 1],
                                      list(range(1, cn.MAX_VERSIONS + 2))])
def test_invalid_versions(versions):
    value = offer()
    value["features"]["example.feature"]["versions"] = versions
    with pytest.raises(cn.NegotiationError):
        cn.decode_offer(encoded(value))


@pytest.mark.parametrize("limit", [True, 0, -1, 1.0, "1", None,
                                   cn.MAX_INTEGER + 1])
def test_invalid_limits(limit):
    with pytest.raises(cn.NegotiationError):
        cn.encode_offer(offer(limit=limit))


@pytest.mark.parametrize("change", [
    lambda v: v.update(protocol=True),
    lambda v: v.update(protocol=2),
    lambda v: v.update(extra="field"),
    lambda v: v.update(required=["absent"]),
    lambda v: v.update(required=["example.feature", "example.feature"]),
    lambda v: v.update(required=[{}]),
    lambda v: v.update(features={"BAD NAME": {}}),
    lambda v: v.update(
        features={f"f{i}": {} for i in range(cn.MAX_FEATURES + 1)}),
    lambda v: v["features"]["example.feature"].update(extra=True),
    lambda v: v["features"]["example.feature"].update(
        limits={f"l{i}": 1 for i in range(cn.MAX_LIMITS + 1)}),
])
def test_strict_offer_shape(change):
    value = offer()
    change(value)
    with pytest.raises(cn.NegotiationError):
        cn.decode_offer(encoded(value))


@pytest.mark.parametrize("change", [
    lambda v: v.update(protocol=True),
    lambda v: v.update(protocol=2),
    lambda v: v.update(extra=1),
    lambda v: v["features"].update(unoffered={"version": 1, "limits": {}}),
    lambda v: v["features"]["example.feature"].update(version=3),
    lambda v: v["features"]["example.feature"].update(version=True),
    lambda v: v["features"]["example.feature"].update(limits={}),
    lambda v: v["features"]["example.feature"].update(
        limits={"max_bytes": 1025}),
    lambda v: v["features"].clear(),
])
def test_reply_must_fit_the_offer(change):
    client = offer(required=True)
    value = cn.select_capabilities(client, offer())
    change(value)
    with pytest.raises(cn.NegotiationError):
        cn.decode_selection(encoded(value), client)


def test_encoded_offer_is_also_bounded():
    value = {"protocol": 1, "features": {}, "required": []}
    for i in range(cn.MAX_FEATURES):
        value["features"][f"feature{i}"] = {
            "versions": [1],
            "limits": {f"limit{j}" + "x" * 70: cn.MAX_INTEGER
                       for j in range(cn.MAX_LIMITS)},
        }
    cn.validate_offer(value)
    with pytest.raises(cn.NegotiationError, match="byte limit"):
        cn.encode_offer(value)


def test_localhost_action_and_repeated_negotiation():
    with cn.CapabilityServer(offer((2, 3), 512),
                             location="grpc://127.0.0.1:0") as server:
        with flight.connect(("127.0.0.1", server.port)) as client:
            actions = [action.type for action in client.list_actions()]
            assert cn.ACTION in actions
            first = cn.negotiate(client, offer(required=True))
            assert first["features"]["example.feature"]["limits"] == {
                "max_bytes": 512}
            server.supported = offer((1,), 256)
            second = cn.negotiate(client, offer(required=True))
            assert second["features"]["example.feature"] == {
                "version": 1, "limits": {"max_bytes": 256}}


def test_localhost_endpoint_agreements_are_independent():
    for bound in (512, 256):
        with cn.CapabilityServer(offer(limit=bound),
                                 location="grpc://127.0.0.1:0") as server:
            with flight.connect(("127.0.0.1", server.port)) as client:
                result = cn.negotiate(client, offer(required=True))
                assert result["features"]["example.feature"]["limits"] == {
                    "max_bytes": bound}


def test_localhost_invalid_request_and_required_rejection():
    with cn.CapabilityServer(offer((3,)),
                             location="grpc://127.0.0.1:0") as server:
        with flight.connect(("127.0.0.1", server.port)) as client:
            with pytest.raises(pa.ArrowInvalid, match="required features"):
                cn.negotiate(client, offer(required=True))
            for body in (b"{}", b"x" * (cn.MAX_BODY_BYTES + 1)):
                with pytest.raises(pa.ArrowInvalid):
                    list(client.do_action(flight.Action(cn.ACTION, body)))


class ReplyServer(flight.FlightServerBase):
    def __init__(self, bodies=(), error=None, delay=0):
        super().__init__(location="grpc://127.0.0.1:0")
        self.bodies = bodies
        self.error = error
        self.delay = delay

    def do_action(self, context, action):
        if self.delay:
            time.sleep(self.delay)
        for body in self.bodies:
            yield flight.Result(body)
        if self.error:
            raise self.error


def test_localhost_legacy_fallback_only_for_optional_offer():
    with ReplyServer(error=pa.ArrowNotImplementedError("legacy")) as server:
        with flight.connect(("127.0.0.1", server.port)) as client:
            assert cn.negotiate(client, offer()) is None
            with pytest.raises(cn.NegotiationError, match="required"):
                cn.negotiate(client, offer(required=True))


@pytest.mark.parametrize("error", [
    flight.FlightUnauthenticatedError("authenticate"),
    flight.FlightUnauthorizedError("permission denied"),
    flight.FlightUnavailableError("unavailable"),
    flight.FlightInternalError("internal"),
    pa.ArrowInvalid("invalid request"),
])
def test_localhost_errors_do_not_trigger_fallback(error):
    with ReplyServer(error=error) as server:
        with flight.connect(("127.0.0.1", server.port)) as client:
            with pytest.raises(type(error)):
                cn.negotiate(client, offer())


def test_localhost_deadline_does_not_trigger_fallback():
    with ReplyServer(delay=0.1) as server:
        with flight.connect(("127.0.0.1", server.port)) as client:
            with pytest.raises(flight.FlightTimedOutError):
                cn.negotiate(client, offer(),
                             flight.FlightCallOptions(timeout=0.01))


@pytest.mark.parametrize("bodies", [
    (), (b"{}",), (b"x" * (cn.MAX_BODY_BYTES + 1),),
    (b'{"protocol":1,"protocol":1,"features":{}}',),
    (b'{"protocol":1,"features":{}}', b'{"protocol":1,"features":{}}'),
])
def test_localhost_malformed_replies_do_not_trigger_fallback(bodies):
    with ReplyServer(bodies) as server:
        with flight.connect(("127.0.0.1", server.port)) as client:
            with pytest.raises(cn.NegotiationError):
                cn.negotiate(client, offer())


def test_localhost_partial_reply_then_unimplemented_is_not_legacy():
    body = encoded(cn.select_capabilities(offer(), offer()))
    with ReplyServer([body], pa.ArrowNotImplementedError("partial")) as server:
        with flight.connect(("127.0.0.1", server.port)) as client:
            with pytest.raises(pa.ArrowNotImplementedError):
                cn.negotiate(client, offer())


class Authentication(flight.ServerMiddlewareFactory):
    def start_call(self, info, headers):
        if headers.get("authorization") != ["Bearer example-token"]:
            raise flight.FlightUnauthenticatedError("authentication required")


def test_localhost_authenticated_options_are_forwarded():
    with cn.CapabilityServer(offer(), location="grpc://127.0.0.1:0",
                             middleware={"auth": Authentication()}) as server:
        with flight.connect(("127.0.0.1", server.port)) as client:
            with pytest.raises(flight.FlightUnauthenticatedError):
                cn.negotiate(client, offer())
            options = flight.FlightCallOptions(
                timeout=5,
                headers=[(b"authorization", b"Bearer example-token")])
            result = cn.negotiate(client, offer(required=True), options)
            assert result["features"]["example.feature"]["version"] == 2
