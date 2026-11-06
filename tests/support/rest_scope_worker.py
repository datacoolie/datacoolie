"""Read native scope sentinels through separately verified Docker routes."""
import json
import os
import sys


def main():
    from pyiceberg.catalog.rest import RestCatalog

    receipt = json.loads(sys.argv[1])
    uuids = []
    endpoint = os.getenv("DATACOOLIE_SIM_S3_ENDPOINT", "http://rustfs:9000")
    access_key = os.getenv("DATACOOLIE_SIM_S3_ACCESS_KEY", "minioadmin")
    secret_key = os.getenv("DATACOOLIE_SIM_S3_SECRET_KEY", "minioadmin")
    region = os.getenv("DATACOOLIE_SIM_S3_REGION", "us-east-1")
    for uri in ("http://iceberg-rest:8181", receipt["container_uri"]):
        catalog = RestCatalog("scope-probe", uri=uri, **{
            "s3.endpoint": endpoint, "s3.access-key-id": access_key,
            "s3.secret-access-key": secret_key, "s3.region": region,
        })
        table = catalog.load_table((receipt["namespace"], "scope"))
        uuids.append(str(table.metadata.table_uuid))
    assert uuids == receipt["uuids"]
    print("REST_SCOPES=" + json.dumps({"uuids": uuids, "container_aliases_verified": True}))


if __name__ == "__main__":
    main()
