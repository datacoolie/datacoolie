from __future__ import annotations

from botocore.exceptions import ClientError

from datacoolie.platforms._aws.errors import (
    error_code,
    http_status,
    is_entity_too_large,
    is_not_found,
    is_precondition_failed,
    is_throttled,
)


def _error(code: str, status: int) -> ClientError:
    return ClientError(
        {
            "Error": {"Code": code, "Message": code},
            "ResponseMetadata": {"HTTPStatusCode": status},
        },
        "operation",
    )


def test_structured_not_found_and_status_are_classified() -> None:
    exc = _error("NoSuchKey", 404)
    assert error_code(exc) == "NoSuchKey"
    assert http_status(exc) == 404
    assert is_not_found(exc)


def test_access_denied_is_not_missing() -> None:
    assert not is_not_found(_error("AccessDenied", 403))
    assert not is_not_found(RuntimeError("not found"))


def test_conditional_conflict_is_classified() -> None:
    assert is_precondition_failed(_error("PreconditionFailed", 412))


def test_throttle_and_large_copy_are_classified() -> None:
    assert is_throttled(_error("SlowDown", 503))
    assert is_entity_too_large(_error("EntityTooLarge", 400))
