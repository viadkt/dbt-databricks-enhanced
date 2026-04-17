# Design: Session Mode Seed Parameter Binding Rendering

## Problem

`dbt seed` fails in session mode because `SessionCursorWrapper.execute()` rejects SQL parameter bindings. The seed macro (`helpers.sql`) builds parameterized INSERT statements with `%s` placeholders and a bindings list. The DBSQL path handles this via the native connector's parameterized query support, but `SparkSession.sql()` only accepts fully-formed SQL strings.

## Constraint

This repository must stay as close to the original dbt-databricks as possible. Only session mode code should change. No modifications to the DBSQL path, seed macros, or connection manager.

## Solution

Render bindings into the SQL string inside `SessionCursorWrapper.execute()` instead of raising an error. This is the minimal change that unblocks seeds in session mode while keeping all other code paths untouched.

## Rendering Rules

Match DBSQL connector behavior for seed data types:

| Python type | SQL rendering | Example |
|---|---|---|
| `None` | `NULL` | `NULL` |
| `str` | Single-quoted, with `'` escaped as `''` | `'O''Brien'` |
| `bool` | `TRUE` / `FALSE` (must check before `int`) | `TRUE` |
| `int` | Bare numeric literal via `str()` | `42` |
| `float` | Bare numeric literal via `str()` | `3.14` |
| `Decimal` | Convert to `float` first (matches `SqlUtils.translate_bindings`), then `str()` | `0.73` |
| other | `str()` fallback | — |

**Important:** `bool` must be checked before `int` because `bool` is a subclass of `int` in Python.

## Implementation

### session.py — `SessionCursorWrapper`

Add a `_render_binding` staticmethod:

```python
@staticmethod
def _render_binding(value: Any) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, str):
        return "'" + value.replace("'", "''") + "'"
    if isinstance(value, decimal.Decimal):
        return str(float(value))
    return str(value)
```

Replace the error-raising block in `execute()`:

```python
def execute(self, sql: str, bindings: Optional[Sequence[Any]] = None) -> "SessionCursorWrapper":
    if bindings:
        sql = sql % tuple(self._render_binding(b) for b in bindings)
    cleaned_sql = SqlUtils.clean_sql(sql)
    ...
```

### test_session.py

- Replace `test_execute_with_bindings_raises` with `test_execute_with_bindings_renders`
- Verify SQL string interpolation with mixed types: `str`, `str` with quotes, `None`, `bool`, `int`, `float`, `Decimal`
- Verify the rendered SQL is passed to `spark.sql()` after `clean_sql`

## Files Changed

| File | Change |
|---|---|
| `dbt/adapters/databricks/session.py` | Replace error block with `_render_binding` + interpolation (~15 lines) |
| `tests/unit/test_session.py` | Replace rejection test with rendering tests (~20 lines) |

## Files NOT Changed

- `helpers.sql` — seed macro unchanged
- `connections.py` — connection manager unchanged
- `handle.py` — DBSQL path unchanged

## Edge Cases

- **Single quotes in strings:** Handled by `'` → `''` escaping, standard SQL supported by Spark
- **NULL with CAST:** `cast(NULL as string)` is valid Spark SQL, no special handling needed
- **Decimal precision:** Matches existing `translate_bindings` behavior (Decimal → float)
- **Empty bindings list:** Falsy, skipped by `if bindings:` — same as current behavior
