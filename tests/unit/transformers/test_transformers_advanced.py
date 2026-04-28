"""Advanced behavior-driven tests for transformer components.

Focus areas:
- pipeline error semantics and runtime metadata
- dedup strategy matrix (table-driven)
- partition/no-expression warning behavior
"""

from __future__ import annotations

from typing import List, Optional

import pytest

from datacoolie.core.constants import LoadType
from datacoolie.core.exceptions import TransformError
from datacoolie.core.models import DataFlow, PartitionColumn
from datacoolie.transformers.base import BaseTransformer, TransformerPipeline
from datacoolie.transformers.deduplicator import Deduplicator
from datacoolie.transformers.partition_handler import PartitionHandler
from tests.unit.transformers.support import MockEngine, make_dataflow


_make_dataflow = make_dataflow


class PassThroughTransformer(BaseTransformer[dict]):
    def __init__(self, order_value: int = 10):
        self._order = order_value

    @property
    def order(self) -> int:
        return self._order

    def transform(self, df: dict, dataflow: DataFlow) -> dict:
        result = dict(df)
        result["passed"] = True
        return result


class HardFailTransformer(BaseTransformer[dict]):
    @property
    def order(self) -> int:
        return 20

    def transform(self, df: dict, dataflow: DataFlow) -> dict:
        raise ValueError("unexpected failure")


class TestTransformerPipelineAdvanced:
    def test_runtime_info_keeps_applied_transformers_on_wrapped_failure(self) -> None:
        engine = MockEngine()
        pipeline = TransformerPipeline(engine)
        pipeline.add_transformer(PassThroughTransformer(order_value=10))
        pipeline.add_transformer(HardFailTransformer())

        with pytest.raises(TransformError, match="Transformer pipeline failed") as exc:
            pipeline.transform({"id": 1}, _make_dataflow())

        info = pipeline.get_runtime_info()
        assert info.status == "failed"
        assert info.transformers_applied == ["PassThroughTransformer"]
        assert "unexpected failure" in (info.error_message or "")
        assert exc.value.details.get("applied") == ["PassThroughTransformer"]

    def test_remove_transformer_removes_multiple_instances(self) -> None:
        engine = MockEngine()
        pipeline = TransformerPipeline(engine)
        pipeline.add_transformer(PassThroughTransformer(order_value=10))
        pipeline.add_transformer(PassThroughTransformer(order_value=30))

        removed = pipeline.remove_transformer(PassThroughTransformer)
        assert removed is True
        assert pipeline.transformers == []


class TestDeduplicatorAdvanced:
    @pytest.mark.parametrize(
        "load_type,merge_keys,dedup_cols,dedup_by_rank,expected_kind",
        [
            (LoadType.MERGE_OVERWRITE.value, ["id"], None, False, "rank"),
            (LoadType.MERGE_OVERWRITE.value, ["id"], ["id"], False, "row_number"),
            (LoadType.APPEND.value, ["id"], ["id"], True, "rank"),
            (LoadType.MERGE_UPSERT.value, ["id"], ["id"], False, "row_number"),
        ],
    )
    def test_dedup_strategy_matrix(
        self,
        load_type: str,
        merge_keys: List[str],
        dedup_cols: Optional[List[str]],
        dedup_by_rank: bool,
        expected_kind: str,
    ) -> None:
        engine = MockEngine()
        d = Deduplicator(engine)
        dataflow = _make_dataflow(
            load_type=load_type,
            merge_keys=merge_keys,
            dedup_cols=dedup_cols,
            latest_cols=["updated_at"],
            deduplicate_by_rank=dedup_by_rank,
        )

        d.transform({"id": 1, "updated_at": "2026-03-28"}, dataflow)

        if expected_kind == "rank":
            assert engine._dedup_by_rank is True
            assert engine._deduplicated is False
        else:
            assert engine._deduplicated is True
            assert engine._dedup_by_rank is False


class TestPartitionHandlerAdvanced:
    def test_missing_partition_column_without_expression_logs_warning(self, caplog: pytest.LogCaptureFixture) -> None:
        engine = MockEngine()
        engine.set_columns(["id"])
        handler = PartitionHandler(engine)
        df = {"id": 1}
        dataflow = _make_dataflow(
            partition_cols=[PartitionColumn(column="event_month")],
        )

        result = handler.transform(df, dataflow)

        assert result == df
        assert "has no expression and is not in DataFrame" in caplog.text
