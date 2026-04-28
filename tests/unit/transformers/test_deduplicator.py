"""Tests for Deduplicator transformer."""

from __future__ import annotations

import pytest

from datacoolie.core.constants import LoadType
from datacoolie.transformers.deduplicator import Deduplicator
from tests.unit.transformers.support import MockEngine, make_dataflow


@pytest.fixture()
def engine() -> MockEngine:
    return MockEngine()


_make_dataflow = make_dataflow


class TestDeduplicator:
    def test_order_is_20(self, engine: MockEngine) -> None:
        d = Deduplicator(engine)
        assert d.order == 20

    def test_noop_no_partition_cols(self, engine: MockEngine) -> None:
        df = _make_dataflow(dedup_cols=None, latest_cols=["modified_at"])
        d = Deduplicator(engine)
        d.transform({"x": 1}, df)
        assert not engine._deduplicated
        assert not engine._dedup_by_rank

    def test_noop_no_latest_cols_and_no_watermark(self, engine: MockEngine) -> None:
        """No latest_data_columns and no source watermark_columns → no-op."""
        df = _make_dataflow(dedup_cols=["id"], latest_cols=[], watermark_cols=[])
        d = Deduplicator(engine)
        d.transform({"x": 1}, df)
        assert not engine._deduplicated
        assert not engine._dedup_by_rank

    def test_row_number_dedup_for_merge_upsert(self, engine: MockEngine) -> None:
        df = _make_dataflow(
            load_type=LoadType.MERGE_UPSERT.value,
            dedup_cols=["id"],
            latest_cols=["modified_at"],
        )
        d = Deduplicator(engine)
        d.transform({"x": 1}, df)
        assert engine._deduplicated is True
        assert engine._dedup_by_rank is False

    def test_rank_dedup_for_merge_overwrite(self, engine: MockEngine) -> None:
        """MERGE_OVERWRITE + merge_keys + no explicit dedup_cols → rank."""
        df = _make_dataflow(
            load_type=LoadType.MERGE_OVERWRITE.value,
            merge_keys=["id"],
            dedup_cols=None,
            latest_cols=["modified_at"],
        )
        d = Deduplicator(engine)
        d.transform({"x": 1}, df)
        assert engine._dedup_by_rank is True
        assert engine._deduplicated is False

    def test_merge_overwrite_explicit_dedup_uses_row_number(self, engine: MockEngine) -> None:
        """MERGE_OVERWRITE with explicit dedup_cols → ROW_NUMBER, not rank."""
        df = _make_dataflow(
            load_type=LoadType.MERGE_OVERWRITE.value,
            merge_keys=["id"],
            dedup_cols=["id"],
            latest_cols=["modified_at"],
        )
        d = Deduplicator(engine)
        d.transform({"x": 1}, df)
        assert engine._deduplicated is True
        assert engine._dedup_by_rank is False

    def test_merge_overwrite_no_merge_keys_uses_row_number(self, engine: MockEngine) -> None:
        """MERGE_OVERWRITE but merge_keys is empty → ROW_NUMBER."""
        df = _make_dataflow(
            load_type=LoadType.MERGE_OVERWRITE.value,
            merge_keys=[],
            dedup_cols=["id"],
            latest_cols=["modified_at"],
        )
        d = Deduplicator(engine)
        d.transform({"x": 1}, df)
        assert engine._deduplicated is True
        assert engine._dedup_by_rank is False

    def test_rank_dedup_via_configure_flag(self, engine: MockEngine) -> None:
        """transform.configure[deduplicate_by_rank]=True → rank regardless of load type."""
        df = _make_dataflow(
            load_type=LoadType.APPEND.value,
            dedup_cols=["id"],
            latest_cols=["modified_at"],
            transform_configure={"deduplicate_by_rank": True},
        )
        d = Deduplicator(engine)
        d.transform({"x": 1}, df)
        assert engine._dedup_by_rank is True
        assert engine._deduplicated is False

    def test_watermark_fallback_for_order_cols(self, engine: MockEngine) -> None:
        """latest_data_columns is empty → falls back to source.watermark_columns."""
        df = _make_dataflow(
            load_type=LoadType.MERGE_UPSERT.value,
            dedup_cols=["id"],
            latest_cols=[],
            watermark_cols=["updated_at"],
        )
        d = Deduplicator(engine)
        d.transform({"x": 1}, df)
        assert engine._deduplicated is True

    def test_noop_no_order_cols_and_no_watermark(self, engine: MockEngine) -> None:
        """latest_data_columns and watermark_columns both empty → no-op."""
        df = _make_dataflow(
            dedup_cols=["id"],
            latest_cols=[],
            watermark_cols=[],
        )
        d = Deduplicator(engine)
        d.transform({"x": 1}, df)
        assert not engine._deduplicated
        assert not engine._dedup_by_rank

    def test_row_number_dedup_for_overwrite(self, engine: MockEngine) -> None:
        df = _make_dataflow(
            load_type=LoadType.OVERWRITE.value,
            dedup_cols=["id"],
            latest_cols=["modified_at"],
        )
        d = Deduplicator(engine)
        d.transform({"x": 1}, df)
        assert engine._deduplicated is True

    def test_row_number_dedup_for_append(self, engine: MockEngine) -> None:
        df = _make_dataflow(
            load_type=LoadType.APPEND.value,
            dedup_cols=["id"],
            latest_cols=["modified_at"],
        )
        d = Deduplicator(engine)
        d.transform({"x": 1}, df)
        assert engine._deduplicated is True

    def test_fallback_to_merge_keys(self, engine: MockEngine) -> None:
        """When deduplicate_columns is None, falls back to merge_keys."""
        df = _make_dataflow(
            load_type=LoadType.MERGE_UPSERT.value,
            merge_keys=["order_id"],
            dedup_cols=None,
            latest_cols=["modified_at"],
        )
        # deduplicate_columns on DataFlow falls back to merge_keys
        assert df.deduplicate_columns == ["order_id"]
        d = Deduplicator(engine)
        d.transform({"x": 1}, df)
        assert engine._deduplicated is True
