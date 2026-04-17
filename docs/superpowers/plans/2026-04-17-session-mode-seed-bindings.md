# Session Mode Seed Parameter Binding Rendering — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Enable `dbt seed` in session mode by rendering SQL parameter bindings into the query string in `SessionCursorWrapper.execute()`.

**Architecture:** Replace the error-raising block in `SessionCursorWrapper.execute()` with a `_render_binding` staticmethod that converts Python values to SQL literals, then interpolates them into the SQL string using `%` formatting. This matches the DBSQL path's type handling (particularly `Decimal` → `float`) while keeping all non-session code untouched.

**Tech Stack:** Python, pytest, pyspark (SparkSession)

**Spec:** `docs/superpowers/specs/2026-04-17-session-mode-seed-bindings-design.md`

---

### Task 1: Write failing tests for binding rendering

**Files:**
- Modify: `tests/unit/test_session.py:37-44`

- [ ] **Step 1: Replace the rejection test with rendering tests**

Replace the existing `test_execute_with_bindings_raises` test (lines 37-44) with these tests:

```python
def test_execute_with_bindings_renders_into_sql(self, cursor, mock_spark):
    """Test that execute renders bindings into the SQL string."""
    mock_spark.sql.return_value = MagicMock()

    cursor.execute("INSERT INTO t VALUES (%s, %s, %s)", (42, "hello", None))

    mock_spark.sql.assert_called_once_with("INSERT INTO t VALUES (42, 'hello', NULL)")

def test_execute_with_bindings_escapes_quotes(self, cursor, mock_spark):
    """Test that string bindings with single quotes are properly escaped."""
    mock_spark.sql.return_value = MagicMock()

    cursor.execute("INSERT INTO t VALUES (%s)", ("O'Brien",))

    mock_spark.sql.assert_called_once_with("INSERT INTO t VALUES ('O''Brien')")

def test_execute_with_bindings_handles_bool_before_int(self, cursor, mock_spark):
    """Test that booleans render as TRUE/FALSE, not 1/0."""
    mock_spark.sql.return_value = MagicMock()

    cursor.execute("INSERT INTO t VALUES (%s, %s)", (True, False))

    mock_spark.sql.assert_called_once_with("INSERT INTO t VALUES (TRUE, FALSE)")

def test_execute_with_bindings_handles_decimal(self, cursor, mock_spark):
    """Test that Decimal values are converted to float, matching translate_bindings."""
    from decimal import Decimal

    mock_spark.sql.return_value = MagicMock()

    cursor.execute("INSERT INTO t VALUES (%s)", (Decimal("0.73"),))

    mock_spark.sql.assert_called_once_with("INSERT INTO t VALUES (0.73)")

def test_execute_with_bindings_handles_float(self, cursor, mock_spark):
    """Test that float values render as bare numeric literals."""
    mock_spark.sql.return_value = MagicMock()

    cursor.execute("INSERT INTO t VALUES (%s)", (3.14,))

    mock_spark.sql.assert_called_once_with("INSERT INTO t VALUES (3.14)")
```

Also add the `MagicMock` import if not already present at the top of the file (it is already imported on line 3).

- [ ] **Step 2: Run the tests to verify they fail**

Run: `pytest tests/unit/test_session.py::TestSessionCursorWrapper::test_execute_with_bindings_renders_into_sql tests/unit/test_session.py::TestSessionCursorWrapper::test_execute_with_bindings_escapes_quotes tests/unit/test_session.py::TestSessionCursorWrapper::test_execute_with_bindings_handles_bool_before_int tests/unit/test_session.py::TestSessionCursorWrapper::test_execute_with_bindings_handles_decimal tests/unit/test_session.py::TestSessionCursorWrapper::test_execute_with_bindings_handles_float -v`

Expected: All 5 tests FAIL — `DbtRuntimeError` is raised because bindings are currently rejected.

---

### Task 2: Implement binding rendering in SessionCursorWrapper

**Files:**
- Modify: `dbt/adapters/databricks/session.py:13` (add import)
- Modify: `dbt/adapters/databricks/session.py:46-52` (replace execute logic)

- [ ] **Step 1: Add the `decimal` import**

Add `import decimal` after line 13 (`import sys`), so the imports section becomes:

```python
import decimal
import sys
from collections.abc import Sequence
```

- [ ] **Step 2: Add `_render_binding` staticmethod to `SessionCursorWrapper`**

Add this method to the `SessionCursorWrapper` class, right before the `execute` method (before line 46):

```python
@staticmethod
def _render_binding(value: Any) -> str:
    """Render a Python value as a SQL literal string.

    SparkSession.sql() does not support parameterized queries, so bindings
    must be rendered into the SQL string. Type handling matches
    SqlUtils.translate_bindings() behavior for DBSQL parity.
    """
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

- [ ] **Step 3: Replace the error block in `execute`**

Replace the current `execute` method body (the `if bindings:` block that raises `DbtRuntimeError`) with:

```python
def execute(self, sql: str, bindings: Optional[Sequence[Any]] = None) -> "SessionCursorWrapper":
    """Execute a SQL statement and store the resulting DataFrame."""
    if bindings:
        # SparkSession.sql() doesn't support parameterized queries.
        # Render bindings into the SQL string to match DBSQL connector behavior.
        sql = sql % tuple(self._render_binding(b) for b in bindings)
    cleaned_sql = SqlUtils.clean_sql(sql)
    log_sql = redact_credentials(cleaned_sql)
    logger.debug(f"Session mode executing SQL: {log_sql[:200]}...")
    self._df = self._spark.sql(cleaned_sql)
    self._rows = None  # Reset cached rows
    return self
```

- [ ] **Step 4: Run the new tests to verify they pass**

Run: `pytest tests/unit/test_session.py::TestSessionCursorWrapper -v`

Expected: All tests in `TestSessionCursorWrapper` PASS, including the 5 new binding tests.

- [ ] **Step 5: Run the full test suite to verify no regressions**

Run: `pytest tests/unit/ -q`

Expected: All tests pass (was 799 before this change, should be 803 now — 5 new tests added, 1 removed).

- [ ] **Step 6: Commit**

```bash
git add dbt/adapters/databricks/session.py tests/unit/test_session.py
git commit -m "feat(session): render SQL parameter bindings for seed support

SessionCursorWrapper.execute() now renders bindings into the SQL string
instead of rejecting them. This enables dbt seed in session mode, where
SparkSession.sql() only accepts fully-formed SQL strings.

Type rendering matches DBSQL connector behavior:
- None → NULL
- bool → TRUE/FALSE (checked before int)
- str → single-quoted with quote escaping
- Decimal → float (matches SqlUtils.translate_bindings)
- int/float → bare numeric literals"
```
