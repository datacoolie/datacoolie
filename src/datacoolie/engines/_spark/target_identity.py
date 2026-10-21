"""Bounded, memory-only physical-name projection for loaded Spark 3.5 catalogs.

Private JVM fields are deliberate: public catalog/session getters can lazily
create resources. Unsupported layouts or missing state remain unqualified.
"""

from __future__ import annotations

import hashlib
import posixpath
import re
from typing import Any


def _field(obj: Any, name: str) -> Any:
    cls = obj.getClass()
    while cls is not None:
        # Enumerate fields rather than provoke noisy Py4J exceptions on each
        # superclass (native Analyzer may be an anonymous subclass).
        for field in cls.getDeclaredFields():
            if field.getName() == name:
                field.setAccessible(True)
                return field.get(obj)
        cls = cls.getSuperclass()
    return None


def _class_name(obj: Any) -> str | None:
    return obj.getClass().getName() if obj is not None else None


def target_name_identity(spark: Any, *, fmt: str, table_name: str) -> tuple[str, str] | None:
    """Return identity only from the qualified, already-initialized profiles."""
    if not spark.version.startswith("3.5.") or fmt not in ("delta", "iceberg"):
        return None
    raw_parts = tuple(part.strip() for part in table_name.split("."))
    simple = r"[A-Za-z_][A-Za-z0-9_]*"
    if len(raw_parts) < 3 or any(not re.fullmatch(rf"(?:{simple}|`{simple}`)", part) for part in raw_parts):
        return None
    parts = tuple(part.removeprefix("`").removesuffix("`") for part in raw_parts)
    catalog_name = parts[0]
    if catalog_name.lower() == "spark_catalog":
        catalog_name = "spark_catalog"
    try:
        session = spark._jsparkSession
        state = _field(session, "sessionState")
        if state is None:
            return None
        analyzer = _field(state, "analyzer")
        if analyzer is None:
            return None
        manager = _field(analyzer, "catalogManager")
        if manager is None:
            return None
        catalogs = _field(manager, "catalogs")
        if catalogs is None:
            return None
        cached = catalogs.get(catalog_name)
        if cached.isEmpty():
            return None
        plugin = cached.get()
        if fmt == "delta":
            if (catalog_name != "spark_catalog" or len(parts) != 3
                or _class_name(plugin) != "org.apache.spark.sql.delta.catalog.DeltaCatalog"):
                return None
            catalog = _field(state, "catalog")
            if catalog is None or catalog.conf().caseSensitiveAnalysis():
                return None
            external = _field(catalog, "externalCatalog")
            if _class_name(external) == "org.apache.spark.sql.catalyst.catalog.ExternalCatalogWithListener":
                external = _field(external, "delegate")
            if _class_name(external) != "org.apache.spark.sql.catalyst.catalog.InMemoryCatalog":
                return None
            return f"spark-session:{session.sessionUUID()}", ".".join((catalog_name, *(p.lower() for p in parts[1:])))

        if _class_name(plugin) != "org.apache.iceberg.spark.SparkCatalog":
            return None
        native = plugin.icebergCatalog()
        if _class_name(native) == "org.apache.iceberg.CachingCatalog":
            if not _field(native, "caseSensitive"):
                return None
            native = _field(native, "catalog")
        if _class_name(native) != "org.apache.iceberg.hadoop.HadoopCatalog":
            return None
        filesystem = _field(native, "fs")
        if _class_name(filesystem) in (
            "org.apache.hadoop.fs.LocalFileSystem", "org.apache.hadoop.hive.ql.io.ProxyLocalFileSystem"
        ):
            filesystem = _field(filesystem, "fs")
        if (_class_name(filesystem) != "org.apache.hadoop.fs.RawLocalFileSystem"
            or _class_name(_field(native, "fileIO")) != "org.apache.iceberg.hadoop.HadoopFileIO"):
            return None
        warehouse = _field(native, "warehouseLocation")
        if not isinstance(warehouse, str) or not warehouse.startswith("/") or ".." in warehouse.split("/"):
            return None
        warehouse = posixpath.normpath(warehouse)
        scope = hashlib.sha256(warehouse.encode("utf-8")).hexdigest()
        return f"spark-iceberg-local:{scope}", ".".join(parts[1:])
    except Exception:
        # Unsupported JVM layouts/security managers cannot establish equality.
        # Do not fall back to config or trigger catalog loading to fill a gap.
        return None
