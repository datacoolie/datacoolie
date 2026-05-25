"""
Minimal HTTP server for API introspection tests.

Endpoints:
  GET  /openapi.json          — OpenAPI spec (no auth)
  GET  /openapi-apikey.json   — OpenAPI spec (requires X-API-Key: testkey123)
  GET  /openapi-bearer.json   — OpenAPI spec (requires Authorization: Bearer testtoken456)
  POST /graphql               — GraphQL introspection mock
  GET  /$metadata             — OData v4 metadata XML mock
  GET  /health                — healthcheck
"""
import http.server
import json
import os

SPEC_PATH = os.path.join(os.path.dirname(__file__), "openapi-petstore.json")
PAGINATED_SPEC_PATH = os.path.join(os.path.dirname(__file__), "openapi-paginated.json")
SAMPLE_SPEC_PATH = os.path.join(os.path.dirname(__file__), "openapi-sample.json")
PORT = 8092

_GRAPHQL_SCHEMA = json.dumps({
    "data": {
        "__schema": {
            "queryType": {"name": "Query"},
            "types": [
                {
                    "name": "Pet",
                    "kind": "OBJECT",
                    "fields": [
                        {"name": "id",     "type": {"name": "Int",    "kind": "SCALAR", "ofType": None}, "args": []},
                        {"name": "name",   "type": {"name": "String", "kind": "SCALAR", "ofType": None}, "args": []},
                        {"name": "status", "type": {"name": "String", "kind": "SCALAR", "ofType": None}, "args": []},
                    ],
                },
                {
                    "name": "Order",
                    "kind": "OBJECT",
                    "fields": [
                        {"name": "id",       "type": {"name": "Int",    "kind": "SCALAR", "ofType": None}, "args": []},
                        {"name": "petId",    "type": {"name": "Int",    "kind": "SCALAR", "ofType": None}, "args": []},
                        {"name": "quantity", "type": {"name": "Int",    "kind": "SCALAR", "ofType": None}, "args": []},
                        {"name": "status",   "type": {"name": "String", "kind": "SCALAR", "ofType": None}, "args": []},
                    ],
                },
                # built-in scalars — introspect.py skips these (starts with __ or not OBJECT)
                {"name": "__Schema",  "kind": "OBJECT",  "fields": []},
                {"name": "String",    "kind": "SCALAR",  "fields": None},
                {"name": "Int",       "kind": "SCALAR",  "fields": None},
            ],
        }
    }
}).encode()

_ODATA_METADATA = b"""<?xml version="1.0" encoding="UTF-8"?>
<edmx:Edmx xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx" Version="4.0">
  <edmx:DataServices>
    <Schema xmlns="http://docs.oasis-open.org/odata/ns/edm" Namespace="Petstore">
      <EntityType Name="Pet">
        <Key><PropertyRef Name="id"/></Key>
        <Property Name="id"     Type="Edm.Int64"  Nullable="false"/>
        <Property Name="name"   Type="Edm.String" Nullable="false"/>
        <Property Name="status" Type="Edm.String"/>
      </EntityType>
      <EntityType Name="Order">
        <Key><PropertyRef Name="id"/></Key>
        <Property Name="id"       Type="Edm.Int64" Nullable="false"/>
        <Property Name="petId"    Type="Edm.Int64"/>
        <Property Name="quantity" Type="Edm.Int32"/>
        <Property Name="status"   Type="Edm.String"/>
      </EntityType>
    </Schema>
  </edmx:DataServices>
</edmx:Edmx>"""


def _check_apikey(handler) -> bool:
    return handler.headers.get("X-API-Key") == "testkey123"


def _check_bearer(handler) -> bool:
    auth = handler.headers.get("Authorization", "")
    return auth == "Bearer testtoken456"


def _check_basic(handler) -> bool:
    import base64
    auth = handler.headers.get("Authorization", "")
    if not auth.startswith("Basic "):
        return False
    expected = base64.b64encode(b"testuser:testpass").decode()
    return auth == f"Basic {expected}"


# Sample data for sample-call testing
_PETS_DATA = json.dumps([
    {"id": 1, "name": "Buddy", "status": "available", "tag": "dog"},
    {"id": 2, "name": "Whiskers", "status": "pending", "tag": "cat"},
    {"id": 3, "name": "Goldie", "status": "sold", "tag": "fish"},
]).encode()


class SpecHandler(http.server.BaseHTTPRequestHandler):
    def log_message(self, format, *args):  # noqa: A002
        pass  # suppress access log noise

    def _send_json(self, data: bytes) -> None:
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _send_xml(self, data: bytes) -> None:
        self.send_response(200)
        self.send_header("Content-Type", "application/xml")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _send_401(self, reason: str) -> None:
        body = json.dumps({"error": "unauthorized", "detail": reason}).encode()
        self.send_response(401)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):  # noqa: N802
        with open(SPEC_PATH, "rb") as f:
            spec_data = f.read()

        if self.path in ("/openapi.json", "/"):
            self._send_json(spec_data)

        elif self.path == "/openapi-paginated.json":
            with open(PAGINATED_SPEC_PATH, "rb") as f:
                self._send_json(f.read())

        elif self.path == "/openapi-sample.json":
            with open(SAMPLE_SPEC_PATH, "rb") as f:
                self._send_json(f.read())

        elif self.path == "/openapi-apikey.json":
            if not _check_apikey(self):
                self._send_401("X-API-Key header required (value: testkey123)")
            else:
                self._send_json(spec_data)

        elif self.path == "/openapi-bearer.json":
            if not _check_bearer(self):
                self._send_401("Authorization: Bearer testtoken456 required")
            else:
                self._send_json(spec_data)

        elif self.path == "/$metadata":
            self._send_xml(_ODATA_METADATA)

        elif self.path == "/openapi-basic.json":
            if not _check_basic(self):
                self._send_401("Authorization: Basic (testuser:testpass) required")
            else:
                self._send_json(spec_data)

        elif self.path == "/pets":
            # Data endpoint for sample-call testing
            self._send_json(_PETS_DATA)

        elif self.path == "/health":
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.end_headers()
            self.wfile.write(b"ok")

        else:
            self.send_response(404)
            self.end_headers()

    def do_POST(self):  # noqa: N802
        if self.path == "/graphql":
            # Return mock introspection response regardless of query body
            self._send_json(_GRAPHQL_SCHEMA)
        else:
            self.send_response(404)
            self.end_headers()


if __name__ == "__main__":
    server = http.server.HTTPServer(("0.0.0.0", PORT), SpecHandler)
    print(f"Serving OpenAPI spec at http://0.0.0.0:{PORT}/openapi.json")
    server.serve_forever()

