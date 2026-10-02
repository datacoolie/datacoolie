"""Argument parser for the framework/project CLI."""

from __future__ import annotations

import argparse

from datacoolie import __version__


class CLIUsageError(Exception):
    """A command-line syntax or option-scope error."""

    def __init__(self, message: str, *, usage: str | None = None) -> None:
        super().__init__(message)
        self.usage = usage


class DataCoolieArgumentParser(argparse.ArgumentParser):
    """Argument parser that lets the public entry point render failures."""

    def error(self, message: str) -> None:
        raise CLIUsageError(message, usage=self.format_usage())


def _common(parser: argparse.ArgumentParser, *, project: bool = True) -> None:
    parser.add_argument("--format", choices=("text", "json"), default=argparse.SUPPRESS, help="Result format (default: text on TTY, JSON otherwise)")
    if project:
        parser.add_argument("--project-dir", type=str, default=argparse.SUPPRESS, help="Project directory or datacoolie.yml path")


def create_parser() -> argparse.ArgumentParser:
    parser = DataCoolieArgumentParser(
        prog="dc",
        description="Prepare, validate, inspect, and build DataCoolie projects.",
    )
    parser.add_argument("--version", action="version", version=str(__version__))
    parser.add_argument("--format", choices=("text", "json"), default=None, help="Result format (default: text on TTY, JSON otherwise)")
    parser.add_argument("--project-dir", type=str, default=None, help="Project directory or datacoolie.yml path")
    commands = parser.add_subparsers(dest="command", required=True)

    init = commands.add_parser("init", help="Create an empty DataCoolie project")
    _common(init, project=False)
    init.add_argument("path", nargs="?", default=".", help="Directory to create")
    init.add_argument("--name", help="Project name")
    init.add_argument("--env", action="append", dest="environments", help="Environment name (repeatable)")
    init.add_argument("--config", type=str, help="YAML configuration seed")

    validate = commands.add_parser("validate", help="Validate a project, metadata target, or artifact")
    _common(validate)
    targets = validate.add_mutually_exclusive_group()
    targets.add_argument("--metadata-path", type=str)
    targets.add_argument("--artifact-path", type=str)
    validate.add_argument("--env", action="append", dest="environments")
    validate.add_argument("--only", action="append", choices=("config", "metadata", "resources"))
    validate.add_argument("--sql-base-path", action="append", type=str, help="SQL root (repeatable for multiple roots)")
    validate.add_argument("--artifact-base-path", type=str)

    inspect = commands.add_parser("inspect", help="Inspect project, metadata, capabilities, or artifacts")
    _common(inspect)
    inspect_sub = inspect.add_subparsers(dest="inspect_command")
    config = inspect_sub.add_parser("config", help="Inspect effective project configuration")
    _common(config)
    config.add_argument("--env")
    metadata = inspect_sub.add_parser("metadata", help="Inspect metadata inventory")
    _common(metadata)
    metadata.add_argument("--metadata-path", type=str)
    metadata.add_argument("--env")
    metadata.add_argument("--section", choices=("connections", "dataflows", "schema_hints"))
    metadata.add_argument("--name")
    metadata.add_argument("--stage")
    metadata.add_argument("--full", action="store_true")
    capabilities = inspect_sub.add_parser("capabilities", help="List installed registrations")
    _common(capabilities, project=False)
    artifact = inspect_sub.add_parser("artifact", help="Inspect a build/current artifact")
    _common(artifact)
    artifact.add_argument("--artifact-path", type=str)

    build = commands.add_parser("build", help="Build all project environments")
    _common(build)
    build.add_argument("--metadata-layout", choices=("single", "split", "preserve"))
    build.add_argument("--metadata-format", choices=("json", "yaml", "excel", "preserve"))
    build.add_argument("--dry-run", action="store_true")

    metadata_group = commands.add_parser("metadata", help="Metadata helper operations")
    _common(metadata_group, project=False)
    metadata_sub = metadata_group.add_subparsers(dest="metadata_command", required=True)
    convert = metadata_sub.add_parser("convert", help="Convert one metadata document")
    _common(convert, project=False)
    convert.add_argument("--input", required=True, type=str)
    convert.add_argument("--output", required=True, type=str)
    convert.add_argument("--to", choices=("json", "yaml", "excel"))
    convert.add_argument("--overwrite", action="store_true")

    agents = commands.add_parser("agents", help="Manage project AGENTS.md")
    _common(agents)
    agents_sub = agents.add_subparsers(dest="agents_command", required=True)
    update = agents_sub.add_parser("update", help="Download the latest canonical AGENTS.md")
    _common(update)

    return parser


__all__ = ["CLIUsageError", "DataCoolieArgumentParser", "create_parser"]
