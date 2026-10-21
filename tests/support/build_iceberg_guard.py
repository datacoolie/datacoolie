"""Explicit opt-in experimental Java guard build inside Spark 3.5 container."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
import subprocess


def main() -> None:
    import pyspark

    assert pyspark.__version__ == "3.5.9", pyspark.__version__
    product = Path(__file__).resolve().parents[2]
    sources = sorted((product / "tests/support/native_iceberg").glob("*.java"))
    jars = product / "usecase-sim/.runtime/dependencies/spark/spark35/jars"
    if not jars.is_dir():
        jars = Path("/tmp/datacoolie-usecase-sim/dependencies/spark35/jars")
    runtime = jars / "org.apache.iceberg_iceberg-spark-runtime-3.5_2.12-1.10.1.jar"
    assert hashlib.sha256(runtime.read_bytes()).hexdigest() == "39ea09e6c03550a300b9d9ab498949836d2d434e441da4c12a943a086e396940"
    fingerprint = hashlib.sha256(b"".join(p.read_bytes() for p in sources)).hexdigest()
    output = product / "usecase-sim/.runtime/dependencies/native-iceberg-qualification" / fingerprint
    classes = output / "classes"
    classes.mkdir(parents=True, exist_ok=True)
    classpath = f"{runtime}:{Path(pyspark.__file__).parent}/jars/*"
    subprocess.run(["javac", "--release", "11", "-proc:none", "-cp", classpath, "-d", str(classes),
                    *map(str, sources)], check=True)
    jar = output / "guard.jar"
    subprocess.run(["jar", "--create", "--file", str(jar), "-C", str(classes), "."], check=True)
    print(json.dumps({"jar": str(jar), "source_fingerprint": fingerprint,
                      "jar_sha256": hashlib.sha256(jar.read_bytes()).hexdigest()}))


if __name__ == "__main__":
    main()
