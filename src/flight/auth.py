from __future__ import annotations

import pyarrow.flight as flight


def _decode_maybe(value):
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return value


def _first_value(value):
    if isinstance(value, list):
        value = value[0] if value else None
    return _decode_maybe(value)


def _get_header(headers, name: str):
    if headers is None:
        return None

    for key in (name, name.encode()):
        try:
            if key in headers:
                return _first_value(headers[key])
        except TypeError:
            pass

    try:
        items = headers.items()
    except AttributeError:
        items = []

    for k, v in items:
        ks = _decode_maybe(k)
        if isinstance(ks, str) and ks.lower() == name.lower():
            return _first_value(v)

    return None


def _extract_bearer(raw):
    raw = _decode_maybe(raw)
    if not raw:
        return None
    parts = raw.split()
    if len(parts) == 2 and parts[0].lower() == "bearer":
        return parts[1]
    return raw


class BearerAuthFactory(flight.ServerMiddlewareFactory):
    def __init__(self, expected_token: str):
        self.expected_token = expected_token

    def start_call(self, info, headers=None):
        hdrs = headers if headers is not None else getattr(info, "headers", None)
        raw = _get_header(hdrs, "authorization")
        token = _extract_bearer(raw)

        if token != self.expected_token:
            raise flight.FlightUnauthenticatedError("invalid or missing bearer token")

        return ServerAuthMiddleware(token)


class ServerAuthMiddleware(flight.ServerMiddleware):
    def __init__(self, token: str):
        self.token = token


class TokenClientMiddlewareFactory(flight.ClientMiddlewareFactory):
    def __init__(self, token: str):
        self.token = token

    def start_call(self, info):
        return TokenClientMiddleware(self.token)


class TokenClientMiddleware(flight.ClientMiddleware):
    def __init__(self, token: str):
        self.token = token

    def sending_headers(self):
        return {"authorization": f"Bearer {self.token}"}