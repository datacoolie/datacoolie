"""Tests for :class:`PluginRegistry`."""

from __future__ import annotations

import threading
from abc import ABC, abstractmethod
from unittest.mock import MagicMock, patch

import pytest

from datacoolie.core.exceptions import DataCoolieError
from datacoolie.core.registry import PluginRegistry


# ---------------------------------------------------------------------------
# Fixtures — simple base class + concrete classes for testing
# ---------------------------------------------------------------------------


class _DummyBase(ABC):
    @abstractmethod
    def run(self) -> str: ...


class _ConcreteA(_DummyBase):
    def __init__(self, **kwargs):
        self.kwargs = kwargs

    def run(self) -> str:
        return "A"


class _ConcreteB(_DummyBase):
    def __init__(self, **kwargs):
        self.kwargs = kwargs

    def run(self) -> str:
        return "B"


class _NotASubclass:
    pass


@pytest.fixture
def registry() -> PluginRegistry[_DummyBase]:
    return PluginRegistry("test.plugins", _DummyBase)


# ---------------------------------------------------------------------------
# Registration
# ---------------------------------------------------------------------------


class TestRegister:
    def test_register_valid_class(self, registry: PluginRegistry) -> None:
        registry.register("alpha", _ConcreteA)
        assert registry.is_available("alpha")

    def test_register_rejects_non_subclass(self, registry: PluginRegistry) -> None:
        with pytest.raises(DataCoolieError, match="must be a subclass"):
            registry.register("bad", _NotASubclass)  # type: ignore[arg-type]

    def test_register_rejects_non_type(self, registry: PluginRegistry) -> None:
        with pytest.raises(DataCoolieError, match="must be a subclass"):
            registry.register("bad", "not-a-class")  # type: ignore[arg-type]

    def test_register_overwrites_existing(self, registry: PluginRegistry) -> None:
        registry.register("x", _ConcreteA)
        registry.register("x", _ConcreteB)
        instance = registry.get("x")
        assert instance.run() == "B"


# ---------------------------------------------------------------------------
# Get
# ---------------------------------------------------------------------------


class TestGet:
    def test_get_returns_instance(self, registry: PluginRegistry) -> None:
        registry.register("alpha", _ConcreteA)
        result = registry.get("alpha")
        assert isinstance(result, _ConcreteA)
        assert result.run() == "A"

    def test_get_passes_kwargs(self, registry: PluginRegistry) -> None:
        registry.register("alpha", _ConcreteA)
        result = registry.get("alpha", x=1, y="hello")
        assert result.kwargs == {"x": 1, "y": "hello"}

    def test_get_missing_plugin_raises(self, registry: PluginRegistry) -> None:
        registry.register("alpha", _ConcreteA)
        with pytest.raises(DataCoolieError, match="No plugin registered for 'missing'"):
            registry.get("missing")

    def test_get_missing_lists_available(self, registry: PluginRegistry) -> None:
        registry.register("alpha", _ConcreteA)
        registry.register("beta", _ConcreteB)
        with pytest.raises(DataCoolieError, match="Available: alpha, beta"):
            registry.get("missing")

    def test_get_empty_registry_shows_none(self, registry: PluginRegistry) -> None:
        with pytest.raises(DataCoolieError, match=r"Available: \(none\)"):
            registry.get("anything")

    def test_get_creates_new_instance_each_call(self, registry: PluginRegistry) -> None:
        registry.register("alpha", _ConcreteA)
        a1 = registry.get("alpha")
        a2 = registry.get("alpha")
        assert a1 is not a2


# ---------------------------------------------------------------------------
# List & availability
# ---------------------------------------------------------------------------


class TestListAndAvailability:
    def test_list_plugins_sorted(self, registry: PluginRegistry) -> None:
        registry.register("beta", _ConcreteB)
        registry.register("alpha", _ConcreteA)
        assert registry.list_plugins() == ["alpha", "beta"]

    def test_list_plugins_empty(self, registry: PluginRegistry) -> None:
        assert registry.list_plugins() == []

    def test_is_available_true(self, registry: PluginRegistry) -> None:
        registry.register("alpha", _ConcreteA)
        assert registry.is_available("alpha") is True

    def test_is_available_false(self, registry: PluginRegistry) -> None:
        assert registry.is_available("nope") is False


# ---------------------------------------------------------------------------
# Entry-point discovery
# ---------------------------------------------------------------------------


class TestDiscovery:
    def test_discover_loads_entry_points(self, registry: PluginRegistry) -> None:
        mock_ep = MagicMock()
        mock_ep.name = "discovered"
        mock_ep.load.return_value = _ConcreteA

        with patch("datacoolie.core.registry.entry_points", return_value=[mock_ep]):
            result = registry.get("discovered")

        assert isinstance(result, _ConcreteA)
        mock_ep.load.assert_called_once()

    def test_discover_skips_invalid_class(self, registry: PluginRegistry) -> None:
        mock_ep = MagicMock()
        mock_ep.name = "bad_plugin"
        mock_ep.load.return_value = _NotASubclass

        with patch("datacoolie.core.registry.entry_points", return_value=[mock_ep]):
            assert registry.is_available("bad_plugin") is False

    def test_discover_skips_broken_entry_point(self, registry: PluginRegistry) -> None:
        mock_ep = MagicMock()
        mock_ep.name = "broken"
        mock_ep.load.side_effect = ImportError("missing dependency")

        with patch("datacoolie.core.registry.entry_points", return_value=[mock_ep]):
            assert registry.is_available("broken") is False

    def test_discover_runs_only_once(self, registry: PluginRegistry) -> None:
        with patch("datacoolie.core.registry.entry_points", return_value=[]) as mock_ep_fn:
            registry.list_plugins()
            registry.list_plugins()
            registry.is_available("x")
            mock_ep_fn.assert_called_once()

    def test_manual_registration_takes_priority(self, registry: PluginRegistry) -> None:
        registry.register("alpha", _ConcreteA)

        mock_ep = MagicMock()
        mock_ep.name = "alpha"
        mock_ep.load.return_value = _ConcreteB

        with patch("datacoolie.core.registry.entry_points", return_value=[mock_ep]):
            result = registry.get("alpha")

        assert isinstance(result, _ConcreteA)
        mock_ep.load.assert_not_called()

    def test_discover_handles_entry_points_exception(self, registry: PluginRegistry) -> None:
        with patch("datacoolie.core.registry.entry_points", side_effect=Exception("boom")):
            assert registry.list_plugins() == []


class TestDiscoveryAdvanced:
    def test_discover_accepts_only_subclasses(self) -> None:
        reg: PluginRegistry[_DummyBase] = PluginRegistry("test.group", _DummyBase)

        good = MagicMock()
        good.name = "good"
        good.load.return_value = _ConcreteA

        bad = MagicMock()
        bad.name = "bad"
        bad.load.return_value = _NotASubclass

        with patch("datacoolie.core.registry.entry_points", return_value=[good, bad]):
            assert reg.is_available("good") is True
            assert reg.is_available("bad") is False

    def test_discover_skips_exception_during_load(self) -> None:
        reg: PluginRegistry[_DummyBase] = PluginRegistry("test.group", _DummyBase)

        broken = MagicMock()
        broken.name = "broken"
        broken.load.side_effect = RuntimeError("load failed")

        with patch("datacoolie.core.registry.entry_points", return_value=[broken]):
            assert reg.list_plugins() == []

    def test_entry_points_outer_exception_still_marks_discovered(self) -> None:
        reg: PluginRegistry[_DummyBase] = PluginRegistry("test.group", _DummyBase)

        with patch("datacoolie.core.registry.entry_points", side_effect=RuntimeError("boom")):
            assert reg.list_plugins() == []

        # no second discovery attempt after first discovery cycle
        with patch("datacoolie.core.registry.entry_points") as ep_fn:
            assert reg.is_available("x") is False
            ep_fn.assert_not_called()

    def test_get_instantiates_with_kwargs(self) -> None:
        reg: PluginRegistry[_DummyBase] = PluginRegistry("test.group", _DummyBase)
        reg.register("impl", _ConcreteA)
        inst = reg.get("impl", a=1, b="two")
        assert isinstance(inst, _ConcreteA)
        assert inst.kwargs == {"a": 1, "b": "two"}


# ---------------------------------------------------------------------------
# Thread safety
# ---------------------------------------------------------------------------


class TestThreadSafety:
    def test_concurrent_registration(self) -> None:
        reg = PluginRegistry("test.concurrent", _DummyBase)
        errors: list[Exception] = []

        def register_many(prefix: str) -> None:
            try:
                for i in range(50):
                    # Alternate between A and B to stress thread safety
                    cls = _ConcreteA if i % 2 == 0 else _ConcreteB
                    reg.register(f"{prefix}_{i}", cls)
            except Exception as exc:
                errors.append(exc)

        threads = [threading.Thread(target=register_many, args=(f"t{t}",)) for t in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert errors == []
        plugins = reg.list_plugins()
        assert len(plugins) == 200  # 4 threads × 50 each

    def test_concurrent_get_with_discovery(self) -> None:
        results: list[_DummyBase] = []
        errors: list[Exception] = []

        mock_ep = MagicMock()
        mock_ep.name = "shared"
        mock_ep.load.return_value = _ConcreteA

        reg = PluginRegistry("test.concurrent2", _DummyBase)

        def get_plugin() -> None:
            try:
                with patch("datacoolie.core.registry.entry_points", return_value=[mock_ep]):
                    results.append(reg.get("shared"))
            except Exception as exc:
                errors.append(exc)

        threads = [threading.Thread(target=get_plugin) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert errors == []
        assert len(results) == 10
        assert all(isinstance(r, _ConcreteA) for r in results)


# ---------------------------------------------------------------------------
# Global registries smoke test
# ---------------------------------------------------------------------------


class TestGlobalRegistries:
    def test_engine_registry_exists(self) -> None:
        from datacoolie import engine_registry
        assert isinstance(engine_registry, PluginRegistry)

    def test_platform_registry_exists(self) -> None:
        from datacoolie import platform_registry
        assert isinstance(platform_registry, PluginRegistry)

    def test_source_registry_exists(self) -> None:
        from datacoolie import source_registry
        assert isinstance(source_registry, PluginRegistry)

    def test_destination_registry_exists(self) -> None:
        from datacoolie import destination_registry
        assert isinstance(destination_registry, PluginRegistry)

    def test_transformer_registry_exists(self) -> None:
        from datacoolie import transformer_registry
        assert isinstance(transformer_registry, PluginRegistry)

    def test_create_engine_factory(self) -> None:
        from datacoolie import create_engine
        assert callable(create_engine)

    def test_create_platform_factory(self) -> None:
        from datacoolie import create_platform
        assert callable(create_platform)

    def test_create_platform_delegates_to_registry_get(self) -> None:
        from datacoolie import create_platform

        sentinel = object()
        with patch("datacoolie.platform_registry.get", return_value=sentinel) as mock_get:
            result = create_platform("local", config={"region": "us"})

        assert result is sentinel
        mock_get.assert_called_once_with("local", config={"region": "us"})

    def test_register_builtins_swallows_registration_errors(self, monkeypatch: pytest.MonkeyPatch) -> None:
        import datacoolie

        monkeypatch.setattr(datacoolie.engine_registry, "register", MagicMock(side_effect=RuntimeError("boom")))
        monkeypatch.setattr(datacoolie.platform_registry, "register", MagicMock(side_effect=RuntimeError("boom")))
        monkeypatch.setattr(datacoolie.source_registry, "register", MagicMock(side_effect=RuntimeError("boom")))
        monkeypatch.setattr(datacoolie.destination_registry, "register", MagicMock(side_effect=RuntimeError("boom")))
        monkeypatch.setattr(datacoolie.transformer_registry, "register", MagicMock(side_effect=RuntimeError("boom")))

        # Defensive behavior: optional dependency or registration failures should not fail import.
        datacoolie._register_builtins()


# ---------------------------------------------------------------------------
# get_or_create (singleton caching)
# ---------------------------------------------------------------------------


class TestGetOrCreate:
    def test_returns_instance(self, registry: PluginRegistry) -> None:
        registry.register("alpha", _ConcreteA)
        result = registry.get_or_create("alpha")
        assert isinstance(result, _ConcreteA)

    def test_returns_same_instance(self, registry: PluginRegistry) -> None:
        registry.register("alpha", _ConcreteA)
        a1 = registry.get_or_create("alpha")
        a2 = registry.get_or_create("alpha")
        assert a1 is a2

    def test_different_names_get_different_instances(self, registry: PluginRegistry) -> None:
        registry.register("alpha", _ConcreteA)
        registry.register("beta", _ConcreteB)
        a = registry.get_or_create("alpha")
        b = registry.get_or_create("beta")
        assert a is not b

    def test_missing_plugin_raises(self, registry: PluginRegistry) -> None:
        with pytest.raises(DataCoolieError, match="No plugin registered"):
            registry.get_or_create("missing")

    def test_passes_kwargs_on_first_creation(self, registry: PluginRegistry) -> None:
        registry.register("alpha", _ConcreteA)
        a = registry.get_or_create("alpha", x=42)
        assert a.kwargs == {"x": 42}

    def test_ignores_kwargs_on_subsequent_calls(self, registry: PluginRegistry) -> None:
        registry.register("alpha", _ConcreteA)
        a1 = registry.get_or_create("alpha", x=1)
        a2 = registry.get_or_create("alpha", x=999)
        assert a1 is a2
        assert a1.kwargs == {"x": 1}

    def test_clear_singletons(self, registry: PluginRegistry) -> None:
        registry.register("alpha", _ConcreteA)
        a1 = registry.get_or_create("alpha")
        registry.clear_singletons()
        a2 = registry.get_or_create("alpha")
        assert a1 is not a2

    def test_thread_safe(self) -> None:
        reg = PluginRegistry("test.singleton", _DummyBase)
        reg.register("shared", _ConcreteA)
        results: list[_DummyBase] = []
        errors: list[Exception] = []

        def get_singleton() -> None:
            try:
                results.append(reg.get_or_create("shared"))
            except Exception as exc:
                errors.append(exc)

        threads = [threading.Thread(target=get_singleton) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert errors == []
        assert len(results) == 10
        # All threads must have received the exact same instance
        assert all(r is results[0] for r in results)

    def test_triggers_discovery(self, registry: PluginRegistry) -> None:
        mock_ep = MagicMock()
        mock_ep.name = "discovered"
        mock_ep.load.return_value = _ConcreteA

        with patch("datacoolie.core.registry.entry_points", return_value=[mock_ep]):
            result = registry.get_or_create("discovered")

        assert isinstance(result, _ConcreteA)
        mock_ep.load.assert_called_once()


# ---------------------------------------------------------------------------
# SCD2ColumnAdder registration + factory functions
# ---------------------------------------------------------------------------


class TestBuiltinRegistrations:
    def test_scd2_column_adder_registered(self) -> None:
        from datacoolie import transformer_registry
        assert transformer_registry.is_available("scd2_column_adder")

    def test_resolver_registry_has_env(self) -> None:
        from datacoolie import resolver_registry
        assert resolver_registry.is_available("env")


class TestFactoryFunctions:
    def test_create_source_exists(self) -> None:
        from datacoolie import create_source
        assert callable(create_source)

    def test_create_destination_exists(self) -> None:
        from datacoolie import create_destination
        assert callable(create_destination)

    def test_create_transformer_exists(self) -> None:
        from datacoolie import create_transformer
        assert callable(create_transformer)

    def test_create_resolver_exists(self) -> None:
        from datacoolie import create_resolver
        assert callable(create_resolver)

    def test_create_resolver_delegates_to_registry(self) -> None:
        from datacoolie import create_resolver

        sentinel = object()
        with patch("datacoolie.resolver_registry.get", return_value=sentinel) as mock_get:
            result = create_resolver("env", foo="bar")

        assert result is sentinel
        mock_get.assert_called_once_with("env", foo="bar")
