# Changelog

All notable changes to this project will be documented in this file.
The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/)
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

No unreleased changes yet.

## [0.1.0] - 2026-04-28

Initial public release. See earlier commits for the pre-release history —
the project was developed in a closed-loop repository prior to tagging.

### Added

- Full MkDocs Material documentation site with Diataxis structure
  (getting-started, concepts, how-to, reference, extending, operations, ADR).
- API reference auto-generated from Google-style docstrings via
  `mkdocstrings`.
- `mike`-backed docs deployment on pushes to `main`, publishing the site
  under the `latest` alias.
- Build-time generators for plugin entry-point tables and metadata schema
  (`docs/scripts/gen_plugin_table.py`, `gen_metadata_schema.py`).
- Four initial ADRs capturing key architectural decisions.

### Changed

- Documentation style switched from ad-hoc READMEs to Diataxis tiers.
