"""Pipeline and base-transformer focused tests."""

from __future__ import annotations

import pytest

from datacoolie.core.constants import DataFlowStatus
from datacoolie.core.exceptions import TransformError
from datacoolie.transformers.base import BaseTransformer, TransformerPipeline
from tests.unit.transformers.support import MockEngine, make_dataflow


@pytest.fixture()
def engine() -> MockEngine:
    return MockEngine()


_make_dataflow = make_dataflow


class DummyTransformer(BaseTransformer[dict]):
    def __init__(self, order_val: int = 100):
        self._order = order_val

    @property
    def order(self) -> int:
        return self._order

    def transform(self, df, dataflow):
        result = dict(df)
        result[f"dummy_{self._order}"] = True
        return result


class FailingTransformer(BaseTransformer[dict]):
    @property
    def order(self) -> int:
        return 99

    def transform(self, df, dataflow):
        raise RuntimeError("Transform failed!")


class TransformErrorTransformer(BaseTransformer[dict]):
    @property
    def order(self) -> int:
        return 98

    def transform(self, df, dataflow):
        raise TransformError("explicit transform error")


class SkippingTransformer(BaseTransformer[dict]):
    """Calls _mark_skipped — should not appear in applied list."""

    def __init__(self, order_val: int = 50):
        self._order = order_val

    @property
    def order(self) -> int:
        return self._order

    def transform(self, df, dataflow):
        self._mark_skipped()
        return df


class CustomLabelTransformer(BaseTransformer[dict]):
    """Calls _mark_applied with a detail string."""

    def __init__(self, order_val: int = 60, detail: str = "lower"):
        self._order = order_val
        self._detail = detail

    @property
    def order(self) -> int:
        return self._order

    def transform(self, df, dataflow):
        self._mark_applied(self._detail)
        result = dict(df)
        result["custom"] = True
        return result


class TestBaseTransformer:
    def test_cannot_instantiate_abc(self) -> None:
        with pytest.raises(TypeError):
            BaseTransformer()  # type: ignore[abstract]

    def test_name_property(self) -> None:
        t = DummyTransformer(10)
        assert t.name == "DummyTransformer"

    def test_order_property(self) -> None:
        t = DummyTransformer(42)
        assert t.order == 42


class TestTransformerPipeline:
    def test_empty_pipeline(self, engine: MockEngine) -> None:
        pipeline = TransformerPipeline(engine)
        df = _make_dataflow()
        result = pipeline.transform({"data": 1}, df)
        assert result == {"data": 1}

    def test_single_transformer(self, engine: MockEngine) -> None:
        pipeline = TransformerPipeline(engine)
        pipeline.add_transformer(DummyTransformer(10))
        df = _make_dataflow()
        result = pipeline.transform({"data": 1}, df)
        assert result["dummy_10"] is True

    def test_order_is_respected(self, engine: MockEngine) -> None:
        pipeline = TransformerPipeline(engine)
        pipeline.add_transformer(DummyTransformer(30))
        pipeline.add_transformer(DummyTransformer(10))
        pipeline.add_transformer(DummyTransformer(20))
        assert [t.order for t in pipeline.transformers] == [10, 20, 30]

    def test_runtime_info_success(self, engine: MockEngine) -> None:
        pipeline = TransformerPipeline(engine)
        pipeline.add_transformer(DummyTransformer(10))
        df = _make_dataflow()
        pipeline.transform({"x": 1}, df)
        info = pipeline.get_runtime_info()
        assert info.status == DataFlowStatus.SUCCEEDED.value
        assert "DummyTransformer" in info.transformers_applied

    def test_runtime_info_failure(self, engine: MockEngine) -> None:
        pipeline = TransformerPipeline(engine)
        pipeline.add_transformer(FailingTransformer())
        df = _make_dataflow()
        with pytest.raises(TransformError):
            pipeline.transform({"x": 1}, df)
        info = pipeline.get_runtime_info()
        assert info.status == DataFlowStatus.FAILED.value

    def test_runtime_info_transform_error_passthrough(self, engine: MockEngine) -> None:
        pipeline = TransformerPipeline(engine)
        pipeline.add_transformer(TransformErrorTransformer())
        df = _make_dataflow()
        with pytest.raises(TransformError, match="explicit transform error"):
            pipeline.transform({"x": 1}, df)
        info = pipeline.get_runtime_info()
        assert info.status == DataFlowStatus.FAILED.value

    def test_remove_transformer(self, engine: MockEngine) -> None:
        pipeline = TransformerPipeline(engine)
        pipeline.add_transformer(DummyTransformer(10))
        assert pipeline.remove_transformer(DummyTransformer) is True
        assert len(pipeline.transformers) == 0

    def test_remove_nonexistent_transformer(self, engine: MockEngine) -> None:
        pipeline = TransformerPipeline(engine)
        assert pipeline.remove_transformer(DummyTransformer) is False

    def test_clear(self, engine: MockEngine) -> None:
        pipeline = TransformerPipeline(engine)
        pipeline.add_transformer(DummyTransformer(10))
        pipeline.add_transformer(DummyTransformer(20))
        pipeline.clear()
        assert len(pipeline.transformers) == 0

    def test_transformers_applied_tracks_names(self, engine: MockEngine) -> None:
        pipeline = TransformerPipeline(engine)
        pipeline.add_transformer(DummyTransformer(10))
        pipeline.add_transformer(DummyTransformer(20))
        pipeline.transform({"x": 1}, _make_dataflow())
        info = pipeline.get_runtime_info()
        assert len(info.transformers_applied) == 2

    # -- tracking helpers ----------------------------------------------

    def test_mark_skipped_excludes_from_applied(self, engine: MockEngine) -> None:
        pipeline = TransformerPipeline(engine)
        pipeline.add_transformer(DummyTransformer(10))
        pipeline.add_transformer(SkippingTransformer(20))
        pipeline.add_transformer(DummyTransformer(30))
        pipeline.transform({"x": 1}, _make_dataflow())
        info = pipeline.get_runtime_info()
        assert info.transformers_applied == ["DummyTransformer", "DummyTransformer"]

    def test_mark_applied_with_detail(self, engine: MockEngine) -> None:
        pipeline = TransformerPipeline(engine)
        pipeline.add_transformer(CustomLabelTransformer(10, detail="lower"))
        pipeline.transform({"x": 1}, _make_dataflow())
        info = pipeline.get_runtime_info()
        assert info.transformers_applied == ["CustomLabelTransformer(lower)"]

    def test_no_tracking_call_defaults_to_name(self, engine: MockEngine) -> None:
        """Backward compat: no _mark_* call → record class name."""
        pipeline = TransformerPipeline(engine)
        pipeline.add_transformer(DummyTransformer(10))
        pipeline.transform({"x": 1}, _make_dataflow())
        info = pipeline.get_runtime_info()
        assert info.transformers_applied == ["DummyTransformer"]

    def test_mixed_applied_skipped_default(self, engine: MockEngine) -> None:
        pipeline = TransformerPipeline(engine)
        pipeline.add_transformer(DummyTransformer(10))          # default → name
        pipeline.add_transformer(SkippingTransformer(20))       # skipped → excluded
        pipeline.add_transformer(CustomLabelTransformer(30))    # detail → "...(lower)"
        pipeline.transform({"x": 1}, _make_dataflow())
        info = pipeline.get_runtime_info()
        assert info.transformers_applied == [
            "DummyTransformer",
            "CustomLabelTransformer(lower)",
        ]
