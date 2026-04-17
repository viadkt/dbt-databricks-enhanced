# Design: Fork Versioning Scheme

## Context

This repository is a fork of `dbt-databricks` that tracks upstream releases. It will be published to PyPI under a different package name. A versioning scheme is needed to handle fork-specific bugfixes between upstream releases.

## Scheme

Match upstream version by default. Add a fourth segment (PEP 440 micro release) only for fork-specific bugfixes released between upstream versions.

### Version Lifecycle

| Scenario | Version | Action |
|---|---|---|
| Upstream releases `1.11.7` | `1.11.7` | Rebase onto upstream, adjust, release with same version |
| Fork-specific bugfix needed | `1.11.7.1` | Bump fourth segment in `__version__.py` |
| Another fork bugfix | `1.11.7.2` | Increment fourth segment |
| Upstream releases `1.11.8` | `1.11.8` | Rebase onto upstream, fourth segment resets (disappears) |

### PEP 440 Compliance

All versions are valid PEP 440. Ordering is correct:

```
1.11.7 < 1.11.7.1 < 1.11.7.2 < 1.11.8
```

### Mechanics

- Version string lives in `dbt/adapters/databricks/__version__.py`
- Hatch reads it via `tool.hatch.version.path` in `pyproject.toml`
- No tooling changes required — manual edit of one file when bumping
- No automation, no version scripts, no build system modifications

### Current State

Version `1.11.6` — no change needed for the first PyPI release.
