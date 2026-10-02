"""Execution preparation for declarative metadata.

Preparation is an orchestration concern: it creates the mutable execution
copy that readers consume while leaving the metadata snapshot available for
logging and audit records.
"""

from datacoolie.orchestration.preparation.dataflow import (
    ConnectionSecretResolver,
    PreparedDataFlow,
    prepare_execution_dataflow,
    validate_preparation,
)
from datacoolie.orchestration.preparation.query import (
    QueryReference,
    classify_query,
    resolve_query,
)

__all__ = [
    "ConnectionSecretResolver",
    "PreparedDataFlow",
    "QueryReference",
    "classify_query",
    "prepare_execution_dataflow",
    "resolve_query",
    "validate_preparation",
]
