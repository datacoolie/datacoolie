"""Start and seed only the sources exercised by Discover integration tests."""
from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

HERE = Path(__file__).resolve().parent
COMPOSE = ["docker", "compose", "-f", str(HERE / "compose.yaml")]
DATABASE_URLS = {
    "postgres": "postgresql+psycopg2://datacoolie:datacoolie@localhost:5442/pagila",
    "mysql": "mysql+pymysql://datacoolie:datacoolie@localhost:3316/sakila",
    "mssql": ("mssql+pyodbc://sa:Testing%40123@localhost:1444/AdventureWorksLT"
              "?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes"),
}
sys.path.insert(0, str(HERE.parents[1] / "datacoolie-discover/scripts"))


def pytest_addoption(parser):
    parser.addoption("--keep-services", action="store_true", help="Keep Discover Docker fixtures")


@pytest.fixture(scope="session")
def source_environment(request):
    def run(command):
        subprocess.run(command, cwd=HERE, check=True)

    primary_error = None
    try:
        run([*COMPOSE, "up", "-d", "--wait"])
        run(["docker", "exec", "skills-test-mssql", "bash", "-lc",
             '/opt/mssql-tools18/bin/sqlcmd -S localhost -U sa '
             '-P "$MSSQL_SA_PASSWORD" -No -i /opt/mssql-scripts/adventureworks-lt.sql'])
        run([sys.executable, str(HERE / "fixtures/iceberg/seed_iceberg.py")])
        environment = dict(os.environ)
        environment.update({f"DATACOOLIE_TEST_{name.upper()}_URL": url
                            for name, url in DATABASE_URLS.items()})
        yield environment
    except BaseException as error:
        primary_error = error
        raise
    finally:
        if not request.config.getoption("--keep-services"):
            try:
                run([*COMPOSE, "down", "--volumes", "--remove-orphans"])
            except (OSError, subprocess.SubprocessError) as cleanup_error:
                if primary_error is None:
                    raise
                primary_error.add_note(f"Docker fixture cleanup also failed: {cleanup_error}")
