"""
MkDocs hook: suppress known mkdocstrings ↔ mkdocs-autorefs deprecation warnings.

mkdocstrings 1.0.x still imports from mkdocs_autorefs.plugin / .references /
_internal.plugin, which mkdocs-autorefs 1.0.0+ deprecated in favour of
top-level imports.  These are harmless shim warnings that will disappear when
mkdocstrings releases an update; until then, filter them out so the build log
stays clean.
"""
import warnings


def on_startup(**_kwargs) -> None:  # noqa: ANN003
    """Register filters once before the build starts."""
    _register()


def on_config(config, **_kwargs):  # noqa: ANN001, ANN003
    """Belt-and-suspenders: also register during config phase."""
    _register()
    return config


def _register() -> None:
    for msg in (
        "Importing from 'mkdocs_autorefs.plugin' is deprecated.",
        "Importing from 'mkdocs_autorefs.references' is deprecated.",
        "Setting a fallback anchor function is deprecated",
    ):
        warnings.filterwarnings("ignore", message=msg, category=DeprecationWarning)
