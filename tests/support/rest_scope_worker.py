"""Read native scope sentinels through separately verified Docker routes."""
import json
import sys


def main():
    from pyiceberg.catalog.rest import RestCatalog

    receipt = json.loads(sys.argv[1])
    uuids = []
    for uri in ("http://iceberg-rest:8181", receipt["container_uri"]):
        catalog = RestCatalog("scope-probe", uri=uri, **{
            "s3.endpoint": "http://minio:9000", "s3.access-key-id": "minioadmin",
            "s3.secret-access-key": "minioadmin", "s3.region": "us-east-1",
        })
        table = catalog.load_table((receipt["namespace"], "scope"))
        uuids.append(str(table.metadata.table_uuid))
    assert uuids == receipt["uuids"]
    print("REST_SCOPES=" + json.dumps({"uuids": uuids, "container_aliases_verified": True}))


if __name__ == "__main__":
    main()
