# Session Mode Test Coverage Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Increase unit test coverage for session mode code from 74% (PR coverage) to ≥79% (project baseline) by covering the most important untested paths.

**Architecture:** Tests are added to three existing test files and one new file. All tests use `unittest.mock` — no real Spark or Databricks connections needed. Each task covers one cohesive area of the codebase.

**Tech Stack:** pytest, unittest.mock, dbt-core, dbt-databricks

---

## Files

- Modify: `tests/unit/test_session_credentials.py` — add `_validate_session_mode` and `unique_field` tests
- Modify: `tests/unit/test_session.py` — add `cancel`, `dbr_version` fallback, `execute` closed-handle, handle lifecycle tests
- Modify: `tests/unit/test_session_python.py` — add `submit_python_job` auto-select session tests
- Create: `tests/unit/test_session_connections.py` — new file for `DatabricksConnectionManager` session-mode tests

---

### Task 1: credentials.py — `_validate_session_mode` error paths

**Files:**
- Modify: `tests/unit/test_session_credentials.py`

Lines covered: `credentials.py:179-188`

- [ ] **Step 1: Add tests to `TestSessionModeValidation` class (or create it) in `test_session_credentials.py`**

Append at end of file:

```python
class TestValidateSessionMode:
    """Tests for _validate_session_mode error paths."""

    def test_validate_session_mode_raises_when_pyspark_missing(self):
        """_validate_session_mode raises DbtRuntimeError when pyspark is not installed."""
        import sys
        from dbt_common.exceptions import DbtRuntimeError

        with patch.dict(os.environ, {DBT_DATABRICKS_SESSION_MODE_ENV: "true"}):
            creds = DatabricksCredentials(method="session", schema="my_schema")

        # Simulate pyspark not being importable
        with patch.dict(sys.modules, {"pyspark": None, "pyspark.sql": None}):
            with pytest.raises(DbtRuntimeError, match="Session mode requires PySpark"):
                creds._validate_session_mode()

    def test_validate_session_mode_raises_when_schema_missing(self):
        """_validate_session_mode raises DbtValidationError when schema is None."""
        from dbt_common.exceptions import DbtValidationError

        with patch.dict(os.environ, {DBT_DATABRICKS_SESSION_MODE_ENV: "true"}):
            creds = DatabricksCredentials(method="session", schema="placeholder")

        creds.schema = None  # force None after construction

        with patch.dict("sys.modules", {"pyspark": MagicMock(), "pyspark.sql": MagicMock()}):
            with pytest.raises(DbtValidationError, match="Schema is required"):
                creds._validate_session_mode()
```

Also add `from unittest.mock import MagicMock` to imports if not present.

- [ ] **Step 2: Run tests to verify they pass**

```bash
cd /Users/alexey/Documents/git/dbt-databricks-enhanced
uvx --with hatch hatch run -v +py=3.12 test:unit -- tests/unit/test_session_credentials.py::TestValidateSessionMode -v
```

Expected: 2 passed

- [ ] **Step 3: Commit**

```bash
git add tests/unit/test_session_credentials.py
git commit -m "test: cover _validate_session_mode error paths"
```

---

### Task 2: credentials.py — `unique_field` and `_connection_keys_session`

**Files:**
- Modify: `tests/unit/test_session_credentials.py`

Lines covered: `credentials.py:266, 282, 286`

- [ ] **Step 1: Add tests**

Append after `TestValidateSessionMode`:

```python
class TestSessionModeUniqueField:
    """Tests for unique_field and connection key methods in session mode."""

    def test_unique_field_returns_session_uri(self):
        """unique_field returns session://catalog/schema in session mode."""
        with patch.dict(os.environ, {DBT_DATABRICKS_SESSION_MODE_ENV: "true"}):
            creds = DatabricksCredentials(
                method="session",
                database="my_catalog",
                schema="my_schema",
            )
        assert creds.unique_field == "session://my_catalog/my_schema"

    def test_connection_keys_session_includes_catalog(self):
        """_connection_keys_session includes catalog when database is set."""
        with patch.dict(os.environ, {DBT_DATABRICKS_SESSION_MODE_ENV: "true"}):
            creds = DatabricksCredentials(
                method="session",
                database="my_catalog",
                schema="my_schema",
            )
        keys = creds._connection_keys_session()
        assert "catalog" in keys
        assert "method" in keys
        assert "schema" in keys

    def test_connection_keys_session_with_aliases(self):
        """_connection_keys_session with aliases includes database instead of catalog."""
        with patch.dict(os.environ, {DBT_DATABRICKS_SESSION_MODE_ENV: "true"}):
            creds = DatabricksCredentials(
                method="session",
                database="my_catalog",
                schema="my_schema",
            )
        keys = creds._connection_keys_session(with_aliases=True)
        assert "database" in keys

    def test_connection_keys_session_with_session_properties(self):
        """_connection_keys_session includes session_properties when set."""
        with patch.dict(os.environ, {DBT_DATABRICKS_SESSION_MODE_ENV: "true"}):
            creds = DatabricksCredentials(
                method="session",
                schema="my_schema",
                session_properties={"spark.sql.shuffle.partitions": "8"},
            )
        keys = creds._connection_keys_session()
        assert "session_properties" in keys
```

- [ ] **Step 2: Run tests**

```bash
uvx --with hatch hatch run -v +py=3.12 test:unit -- tests/unit/test_session_credentials.py::TestSessionModeUniqueField -v
```

Expected: 4 passed

- [ ] **Step 3: Commit**

```bash
git add tests/unit/test_session_credentials.py
git commit -m "test: cover unique_field and _connection_keys_session in session mode"
```

---

### Task 3: session.py — `cancel`, `dbr_version` fallbacks, `execute` closed-handle

**Files:**
- Modify: `tests/unit/test_session.py`

Lines covered: `session.py:79, 112-113, 123, 179-182, 211-215, 224-225, 230`

- [ ] **Step 1: Add tests to `TestSessionCursorWrapper` class**

Find the existing `TestSessionCursorWrapper` class and append these test methods:

```python
    def test_fetchmany_returns_empty_when_no_rows(self, cursor, mock_spark):
        """fetchmany returns empty list when no rows exist."""
        mock_df = MagicMock()
        mock_df.collect.return_value = []
        mock_spark.sql.return_value = mock_df

        cursor.execute("SELECT 1")
        result = cursor.fetchmany(5)

        assert result == []

    def test_cancel_sets_open_to_false(self, cursor):
        """cancel() sets open to False."""
        assert cursor.open is True
        cursor.cancel()
        assert cursor.open is False
```

- [ ] **Step 2: Add tests to `TestDatabricksSessionHandle` class**

Find the existing `TestDatabricksSessionHandle` class and append:

```python
    def test_dbr_version_fallback_on_empty_string(self, handle, mock_spark):
        """dbr_version returns (maxsize, maxsize) when version string is empty."""
        import sys
        mock_spark.conf.get.return_value = ""

        version = handle.dbr_version

        assert version == (sys.maxsize, sys.maxsize)

    def test_dbr_version_fallback_on_exception(self, handle, mock_spark):
        """dbr_version returns (maxsize, maxsize) when conf.get raises."""
        import sys
        mock_spark.conf.get.side_effect = Exception("conf unavailable")

        # Reset cached value
        handle._dbr_version = None
        version = handle.dbr_version

        assert version == (sys.maxsize, sys.maxsize)

    def test_session_id_fallback_on_exception(self, handle, mock_spark):
        """session_id returns 'session-unknown' when applicationId raises."""
        mock_spark.sparkContext.applicationId = None
        # force property to re-evaluate by making applicationId raise
        type(mock_spark.sparkContext).applicationId = property(
            lambda self: (_ for _ in ()).throw(Exception("no app id"))
        )

        result = handle.session_id

        assert result == "session-unknown"

    def test_execute_raises_on_closed_handle(self, handle):
        """execute raises DbtRuntimeError when handle is closed."""
        from dbt_common.exceptions import DbtRuntimeError

        handle.close()
        with pytest.raises(DbtRuntimeError, match="closed session handle"):
            handle.execute("SELECT 1")
```

- [ ] **Step 3: Run tests**

```bash
uvx --with hatch hatch run -v +py=3.12 test:unit -- tests/unit/test_session.py::TestSessionCursorWrapper::test_fetchmany_returns_empty_when_no_rows tests/unit/test_session.py::TestSessionCursorWrapper::test_cancel_sets_open_to_false tests/unit/test_session.py::TestDatabricksSessionHandle::test_dbr_version_fallback_on_empty_string tests/unit/test_session.py::TestDatabricksSessionHandle::test_dbr_version_fallback_on_exception tests/unit/test_session.py::TestDatabricksSessionHandle::test_session_id_fallback_on_exception tests/unit/test_session.py::TestDatabricksSessionHandle::test_execute_raises_on_closed_handle -v
```

Expected: 6 passed

- [ ] **Step 4: Commit**

```bash
git add tests/unit/test_session.py
git commit -m "test: cover session cursor cancel, dbr_version fallbacks, closed handle guard"
```

---

### Task 4: session.py — `DatabricksSessionHandle` lifecycle methods

**Files:**
- Modify: `tests/unit/test_session.py`

Lines covered: `session.py:283-298`

- [ ] **Step 1: Append to `TestDatabricksSessionHandle`**

```python
    def test_cancel_closes_cursor_and_sets_open_false(self, handle, mock_spark):
        """cancel() closes active cursor and sets open=False."""
        mock_df = MagicMock()
        mock_spark.sql.return_value = mock_df
        cursor = handle.execute("SELECT 1")
        assert cursor.open is True

        handle.cancel()

        assert handle.open is False
        assert cursor.open is False

    def test_close_closes_cursor_and_sets_open_false(self, handle, mock_spark):
        """close() closes active cursor and sets open=False."""
        mock_df = MagicMock()
        mock_spark.sql.return_value = mock_df
        cursor = handle.execute("SELECT 1")

        handle.close()

        assert handle.open is False
        assert cursor.open is False

    def test_rollback_is_noop(self, handle):
        """rollback() does not raise."""
        handle.rollback()  # should not raise

    def test_del_does_not_raise(self, handle):
        """__del__ does not raise even with open cursor."""
        mock_df = MagicMock()
        handle._spark.sql.return_value = mock_df
        handle.execute("SELECT 1")
        handle.__del__()  # should not raise
```

- [ ] **Step 2: Run tests**

```bash
uvx --with hatch hatch run -v +py=3.12 test:unit -- tests/unit/test_session.py::TestDatabricksSessionHandle::test_cancel_closes_cursor_and_sets_open_false tests/unit/test_session.py::TestDatabricksSessionHandle::test_close_closes_cursor_and_sets_open_false tests/unit/test_session.py::TestDatabricksSessionHandle::test_rollback_is_noop tests/unit/test_session.py::TestDatabricksSessionHandle::test_del_does_not_raise -v
```

Expected: 4 passed

- [ ] **Step 3: Commit**

```bash
git add tests/unit/test_session.py
git commit -m "test: cover DatabricksSessionHandle cancel, close, rollback, __del__"
```

---

### Task 5: impl.py — `submit_python_job` auto-selects session method

**Files:**
- Modify: `tests/unit/test_session_python.py`

Lines covered: `impl.py:836-842`

- [ ] **Step 1: Append to existing test class in `test_session_python.py`**

Read the existing class name first, then append:

```python
class TestSubmitPythonJobSessionMode:
    """Tests for submit_python_job session mode auto-selection."""

    def test_auto_selects_session_submission_when_session_mode(self):
        """submit_python_job sets submission_method='session' when is_session_mode and none set."""
        from unittest.mock import MagicMock, patch

        from dbt.adapters.databricks.impl import DatabricksAdapter

        mock_config = MagicMock()
        mock_config.credentials.is_session_mode = True

        adapter = DatabricksAdapter.__new__(DatabricksAdapter)
        adapter.config = mock_config
        adapter.behavior = MagicMock()
        adapter.behavior.use_user_folder_for_python.setting = False

        parsed_model = {"config": {}}

        with patch.object(
            DatabricksAdapter.__bases__[0],
            "submit_python_job",
            return_value=MagicMock(),
        ) as mock_super:
            adapter.submit_python_job(parsed_model, "result = 1")

        assert parsed_model["config"]["submission_method"] == "session"

    def test_does_not_override_explicit_submission_method(self):
        """submit_python_job does not overwrite submission_method when already set."""
        from unittest.mock import MagicMock, patch

        from dbt.adapters.databricks.impl import DatabricksAdapter

        mock_config = MagicMock()
        mock_config.credentials.is_session_mode = True

        adapter = DatabricksAdapter.__new__(DatabricksAdapter)
        adapter.config = mock_config
        adapter.behavior = MagicMock()
        adapter.behavior.use_user_folder_for_python.setting = False

        parsed_model = {"config": {"submission_method": "notebook"}}

        with patch.object(
            DatabricksAdapter.__bases__[0],
            "submit_python_job",
            return_value=MagicMock(),
        ):
            adapter.submit_python_job(parsed_model, "result = 1")

        assert parsed_model["config"]["submission_method"] == "notebook"

    def test_skips_auto_select_when_not_session_mode(self):
        """submit_python_job does not set submission_method when not in session mode."""
        from unittest.mock import MagicMock, patch

        from dbt.adapters.databricks.impl import DatabricksAdapter

        mock_config = MagicMock()
        mock_config.credentials.is_session_mode = False

        adapter = DatabricksAdapter.__new__(DatabricksAdapter)
        adapter.config = mock_config
        adapter.behavior = MagicMock()
        adapter.behavior.use_user_folder_for_python.setting = False

        parsed_model = {"config": {}}

        with patch.object(
            DatabricksAdapter.__bases__[0],
            "submit_python_job",
            return_value=MagicMock(),
        ):
            adapter.submit_python_job(parsed_model, "result = 1")

        assert "submission_method" not in parsed_model["config"]
```

- [ ] **Step 2: Run tests**

```bash
uvx --with hatch hatch run -v +py=3.12 test:unit -- tests/unit/test_session_python.py::TestSubmitPythonJobSessionMode -v
```

Expected: 3 passed

- [ ] **Step 3: Commit**

```bash
git add tests/unit/test_session_python.py
git commit -m "test: cover submit_python_job session mode auto-selection"
```

---

### Task 6: connections.py — new `test_session_connections.py`

**Files:**
- Create: `tests/unit/test_session_connections.py`

Lines covered: `connections.py:163-176, 180, 226-232, 506-531, 534-542, 615-618`

- [ ] **Step 1: Create the file**

```python
"""Unit tests for DatabricksConnectionManager session mode methods."""

from multiprocessing import get_context
from unittest.mock import MagicMock, patch

import pytest
from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.databricks.connections import DatabricksConnectionManager
from dbt.adapters.databricks.credentials import DatabricksCredentials
from dbt.adapters.databricks.relation import DatabricksRelation
from dbt.adapters.databricks.session import DatabricksSessionHandle, SessionCursorWrapper


def _make_manager(is_session: bool) -> DatabricksConnectionManager:
    """Create a DatabricksConnectionManager with mocked profile credentials."""
    mock_config = MagicMock()
    mock_config.credentials.is_session_mode = is_session
    return DatabricksConnectionManager(mock_config, get_context("spawn"))


class TestApiClientSessionMode:
    """api_client raises in session mode."""

    def test_api_client_raises_in_session_mode(self):
        manager = _make_manager(is_session=True)
        with pytest.raises(DbtRuntimeError, match="not available in session mode"):
            _ = manager.api_client


class TestIsClusterSessionMode:
    """is_cluster returns True in session mode without checking http_path."""

    def test_is_cluster_returns_true_in_session_mode(self):
        manager = _make_manager(is_session=True)
        assert manager.is_cluster() is True


class TestCancelOpenSessionMode:
    """cancel_open skips PythonRunTracker in session mode."""

    def test_cancel_open_skips_python_tracker_in_session_mode(self):
        manager = _make_manager(is_session=True)

        with patch.object(
            DatabricksConnectionManager,
            "cancel_open",
            wraps=manager.cancel_open,
        ):
            with patch(
                "dbt.adapters.databricks.connections.PythonRunTracker"
            ) as mock_tracker:
                with patch.object(
                    type(manager).__bases__[0], "cancel_open", return_value=[]
                ):
                    result = manager.cancel_open()

        mock_tracker.cancel_runs.assert_not_called()
        assert result == []


class TestCacheSessionCapabilities:
    """_cache_session_capabilities caches on first call, skips on second."""

    def setup_method(self):
        # Reset class-level cache between tests
        DatabricksConnectionManager._session_capabilities = None

    def teardown_method(self):
        DatabricksConnectionManager._session_capabilities = None

    def test_caches_capabilities_on_first_call(self):
        mock_handle = MagicMock(spec=DatabricksSessionHandle)
        mock_handle.dbr_version = (14, 3)

        DatabricksConnectionManager._cache_session_capabilities(mock_handle)

        caps = DatabricksConnectionManager._session_capabilities
        assert caps is not None
        assert caps.dbr_version == (14, 3)
        assert caps.is_sql_warehouse is False

    def test_skips_cache_on_second_call(self):
        mock_handle = MagicMock(spec=DatabricksSessionHandle)
        mock_handle.dbr_version = (14, 3)

        DatabricksConnectionManager._cache_session_capabilities(mock_handle)
        mock_handle.dbr_version = (15, 0)  # change version
        DatabricksConnectionManager._cache_session_capabilities(mock_handle)

        # Should still be (14, 3) — second call was skipped
        assert DatabricksConnectionManager._session_capabilities.dbr_version == (14, 3)


class TestOpenSession:
    """_open_session sets handle and state on the connection."""

    def setup_method(self):
        DatabricksConnectionManager._session_capabilities = None

    def teardown_method(self):
        DatabricksConnectionManager._session_capabilities = None

    def test_open_session_sets_handle_and_state(self):
        from dbt.adapters.contracts.connection import ConnectionState

        mock_creds = MagicMock(spec=DatabricksCredentials)
        mock_creds.database = "my_catalog"
        mock_creds.schema = "my_schema"
        mock_creds.session_properties = {}
        mock_creds.is_session_mode = True

        mock_connection = MagicMock()
        mock_connection.state = ConnectionState.INIT

        mock_handle = MagicMock(spec=DatabricksSessionHandle)
        mock_handle.session_id = "app-123"
        mock_handle.dbr_version = (14, 3)

        with patch(
            "dbt.adapters.databricks.connections.DatabricksSessionHandle.create",
            return_value=mock_handle,
        ):
            result = DatabricksConnectionManager._open_session(mock_connection, mock_creds)

        assert result.state == ConnectionState.OPEN
        assert result.handle is mock_handle

    def test_open_session_wraps_exception_in_dbt_database_error(self):
        from dbt_common.exceptions import DbtDatabaseError

        mock_creds = MagicMock(spec=DatabricksCredentials)
        mock_creds.database = "my_catalog"
        mock_creds.schema = "my_schema"
        mock_creds.session_properties = {}

        mock_connection = MagicMock()

        with patch(
            "dbt.adapters.databricks.connections.DatabricksSessionHandle.create",
            side_effect=RuntimeError("spark unavailable"),
        ):
            with pytest.raises(DbtDatabaseError, match="Failed to create session connection"):
                DatabricksConnectionManager._open_session(mock_connection, mock_creds)


class TestGetResponseSessionCursor:
    """get_response handles SessionCursorWrapper correctly."""

    def test_get_response_with_session_cursor(self):
        mock_spark = MagicMock()
        cursor = SessionCursorWrapper(mock_spark)

        response = DatabricksConnectionManager.get_response(cursor)

        assert response._message == "OK"
        assert response.query_id == "session-query"
```

- [ ] **Step 2: Run tests**

```bash
uvx --with hatch hatch run -v +py=3.12 test:unit -- tests/unit/test_session_connections.py -v
```

Expected: all passed

- [ ] **Step 3: Lint check**

```bash
uvx ruff check tests/unit/test_session_connections.py && uvx ruff format --check tests/unit/test_session_connections.py
```

Expected: All checks passed

- [ ] **Step 4: Commit**

```bash
git add tests/unit/test_session_connections.py
git commit -m "test: add connection manager session mode tests"
```

---

### Task 7: Push and verify coverage

- [ ] **Step 1: Push branch**

```bash
git push origin feat/session-mode
```

- [ ] **Step 2: Wait for CI coverage comment on PR #1**

```bash
gh pr view 1 --repo viadkt/dbt-databricks-enhanced --json url --jq .url
```

Monitor until the coverage comment appears on the PR.

- [ ] **Step 3: Verify coverage ≥79%**

Check the coverage comment on PR #1. The overall project coverage badge should read ≥79% and PR coverage should be ≥79%.

If still below, re-read the coverage report for remaining uncovered lines and add targeted tests following the same pattern.
