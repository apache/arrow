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

"""Opt-in, experimental capability negotiation using Flight DoAction.

This is an application example, not a standardized Flight or Flight SQL
protocol. Both peers must explicitly implement the action and any negotiated
feature. A successful exchange alone does not change Flight data transfers.
Authenticate first. Negotiate again for each endpoint, new client connection,
or authentication change; do not cache agreements globally.

Run ``python capability_negotiation.py`` for a localhost demonstration.
Run ``pytest python/examples/flight/test_capability_negotiation.py`` from the
repository root for the example's tests (requires an installed PyArrow).
"""

import json
import re

import pyarrow as pa
import pyarrow.flight as flight


ACTION = "org.apache.arrow.flight.experimental.example.negotiate.v1"
MAX_BODY_BYTES = 16 * 1024
MAX_FEATURES = 32
MAX_VERSIONS = 16
MAX_LIMITS = 16
MAX_INTEGER = (1 << 63) - 1
_NAME = re.compile(r"[a-z][a-z0-9_.-]{0,95}\Z")


class NegotiationError(ValueError):
    """Invalid or incompatible experimental capability documents."""


def _require(condition, message):
    if not condition:
        raise NegotiationError(message)


def _keys(value, expected):
    _require(type(value) is dict and value.keys() == expected,
             f"expected object fields: {sorted(expected)}")


def _name(value):
    _require(type(value) is str and _NAME.fullmatch(value) is not None,
             "invalid feature or limit name")


def _integer(value):
    _require(type(value) is int and 0 < value <= MAX_INTEGER,
             "expected a positive signed 64-bit integer")


def _limits(value):
    _require(type(value) is dict and len(value) <= MAX_LIMITS,
             "invalid limit map")
    for name, limit in value.items():
        _name(name)
        _integer(limit)


def _envelope(value, fields):
    _keys(value, fields)
    _require(type(value["protocol"]) is int and value["protocol"] == 1,
             "unsupported negotiation protocol version")
    features = value["features"]
    _require(type(features) is dict and len(features) <= MAX_FEATURES,
             "invalid feature map")
    for name in features:
        _name(name)


def validate_offer(offer):
    """Validate an offer without altering it; unknown feature names are valid.

Each version list is a set of supported versions. Limits are positive upper
bounds whose names and units belong to the feature contract. Both peers must
provide exactly the same limit names for a feature to be selected.
"""
    _envelope(offer, {"protocol", "features", "required"})
    required = offer["required"]
    _require(type(required) is list and len(required) <= MAX_FEATURES,
             "invalid required feature list")
    for name in required:
        _name(name)
        _require(name in offer["features"], "required feature is not offered")
    _require(len(set(required)) == len(required), "duplicate required feature")
    for feature in offer["features"].values():
        _keys(feature, {"versions", "limits"})
        versions = feature["versions"]
        _require(type(versions) is list and 0 < len(versions) <= MAX_VERSIONS,
                 "invalid feature version list")
        for version in versions:
            _integer(version)
        _require(len(set(versions)) == len(versions), "duplicate version")
        _limits(feature["limits"])


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        _require(key not in result, "duplicate JSON field")
        result[key] = value
    return result


def _reject_constant(value):
    raise NegotiationError(f"invalid JSON constant: {value}")


def _decode(body):
    # Check a Flight Buffer before copying it into Python. gRPC has already
    # received the message: this bounds our parser, not transport allocations.
    _require(type(body) is bytes or isinstance(body, pa.Buffer),
             "expected UTF-8 JSON bytes")
    _require(len(body) <= MAX_BODY_BYTES, "capability body exceeds byte limit")
    if isinstance(body, pa.Buffer):
        body = body.to_pybytes()
    try:
        return json.loads(body.decode("utf-8"),
                          object_pairs_hook=_unique_object,
                          parse_constant=_reject_constant)
    except (ValueError, UnicodeError, RecursionError) as exc:
        raise NegotiationError(f"invalid capability JSON: {exc}") from exc


def _encode(value):
    body = json.dumps(value, separators=(",", ":"), sort_keys=True,
                      allow_nan=False).encode("utf-8")
    _require(len(body) <= MAX_BODY_BYTES, "capability body exceeds byte limit")
    return body


def encode_offer(offer):
    """Encode a validated offer, bounded by MAX_BODY_BYTES."""
    validate_offer(offer)
    return _encode(offer)


def decode_offer(body):
    """Parse a bounded offer, rejecting duplicate and unexpected fields."""
    offer = _decode(body)
    validate_offer(offer)
    return offer


def select_capabilities(offer, supported):
    """Choose common feature versions and minimum upper bounds.

Unknown optional features, disjoint version sets, and mismatched limit names
are omitted. Required features on either side must be selected.
"""
    validate_offer(offer)
    validate_offer(supported)
    selected = {}
    for name, feature in offer["features"].items():
        peer = supported["features"].get(name)
        if peer is None or feature["limits"].keys() != peer["limits"].keys():
            continue
        versions = set(feature["versions"]).intersection(peer["versions"])
        if not versions:
            continue
        selected[name] = {
            "version": max(versions),
            "limits": {key: min(value, peer["limits"][key])
                       for key, value in feature["limits"].items()},
        }
    required = set(offer["required"]).union(supported["required"])
    missing = required - selected.keys()
    _require(required.issubset(selected),
             f"required features unavailable: {sorted(missing)}")
    return {"protocol": 1, "features": selected}


def decode_selection(body, offer):
    """Validate a reply against exactly what this client offered."""
    validate_offer(offer)
    selection = _decode(body)
    _envelope(selection, {"protocol", "features"})
    for name, feature in selection["features"].items():
        _require(name in offer["features"],
                 "server selected an unoffered feature")
        _keys(feature, {"version", "limits"})
        _integer(feature["version"])
        _limits(feature["limits"])
        requested = offer["features"][name]
        _require(feature["version"] in requested["versions"],
                 "server selected an unoffered version")
        _require(feature["limits"].keys() == requested["limits"].keys(),
                 "server changed the limit names")
        _require(all(value <= requested["limits"][key]
                     for key, value in feature["limits"].items()),
                 "server increased a requested upper bound")
    _require(set(offer["required"]).issubset(selection["features"]),
             "server omitted a required feature")
    return selection


def negotiate(client, offer, options=None):
    """Negotiate once on the caller's authenticated Flight client.

Return None only if an optional-only offer meets UNIMPLEMENTED before any
reply. Authentication, permission, timeout, malformed reply, and other errors
propagate. The caller must explicitly choose its legacy transfer path when
None is returned. No result is cached and no transfer is enabled here.

Pass authenticated headers and a finite deadline via FlightCallOptions when
needed. The default deadline is five seconds.
"""
    # Snapshot the offer so caller mutations cannot change reply validation.
    body = encode_offer(offer)
    offer = decode_offer(body)
    if options is None:
        options = flight.FlightCallOptions(timeout=5)
    try:
        replies = iter(client.do_action(flight.Action(ACTION, body), options))
        first = next(replies)
    except pa.ArrowNotImplementedError:
        _require(not offer["required"],
                 "server does not implement required capability negotiation")
        return None
    except StopIteration as exc:
        raise NegotiationError("server returned no capability reply") from exc
    selection = decode_selection(first.body, offer)
    # Read the terminal status too. Never downgrade a partial reply followed
    # by UNIMPLEMENTED or another RPC error into a successful legacy fallback.
    try:
        next(replies)
    except StopIteration:
        return selection
    raise NegotiationError("server returned more than one capability reply")


class CapabilityServer(flight.FlightServerBase):
    """Example server; advertise only features the application implements."""

    def __init__(self, supported, **kwargs):
        self.supported = decode_offer(encode_offer(supported))
        super().__init__(**kwargs)

    def list_actions(self, context):
        return [(ACTION, "Experimental example capability negotiation")]

    def do_action(self, context, action):
        if action.type != ACTION:
            raise pa.ArrowNotImplementedError("unknown action")
        try:
            selected = select_capabilities(decode_offer(action.body),
                                           self.supported)
            body = _encode(selected)
        except NegotiationError as exc:
            raise pa.ArrowInvalid(str(exc)) from exc
        yield flight.Result(body)


def main():
    # This demonstrates a negotiation exchange only. The example feature
    # represents no implemented data transfer behavior.
    offer = {
        "protocol": 1,
        "features": {"example.feature": {
            "versions": [1, 2], "limits": {"max_bytes": 1024}}},
        "required": ["example.feature"],
    }
    supported = {
        "protocol": 1,
        "features": {"example.feature": {
            "versions": [2, 3], "limits": {"max_bytes": 512}}},
        "required": [],
    }
    with CapabilityServer(supported, location="grpc://127.0.0.1:0") as server:
        with flight.connect(("127.0.0.1", server.port)) as client:
            print(json.dumps(negotiate(client, offer), indent=2))


if __name__ == "__main__":
    main()
