from unittest.mock import MagicMock, patch

from datacoolie.engines._spark import runtime


def test_safe_cache_returns_cached_dataframe() -> None:
    df = MagicMock()
    cached = MagicMock()
    df.cache.return_value = cached

    assert runtime.safe_cache(df) is cached


def test_safe_cache_falls_back_to_original_dataframe() -> None:
    df = MagicMock()
    df.cache.side_effect = RuntimeError("unsupported")

    assert runtime.safe_cache(df) is df


def test_safe_unpersist_ignores_runtime_failure() -> None:
    df = MagicMock()
    df.unpersist.side_effect = RuntimeError("unsupported")

    runtime.safe_unpersist(df)


def test_supports_merge_into_uses_capability_detection() -> None:
    with patch.object(runtime.DataFrame, "mergeInto", create=True):
        assert runtime.supports_merge_into() is True
