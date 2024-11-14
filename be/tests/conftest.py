import pytest
import asyncio
from fastapi.testclient import TestClient
import duckdb
from pathlib import Path
import os
from contextlib import asynccontextmanager
from main import app, tasks

@pytest.fixture
async def test_client():
    """Create a test client with its own event loop."""
    client = TestClient(app)
    yield client

class AsyncDuckDBConnection:
    """Wrapper for DuckDB connection to support async context manager for transactions."""
    def __init__(self, conn):
        self.conn = conn

    def execute(self, *args, **kwargs):
        return self.conn.execute(*args, **kwargs)

    @asynccontextmanager
    async def transaction(self):
        """Async context manager for transactions."""
        self.conn.execute('BEGIN TRANSACTION')
        try:
            yield
            self.conn.execute('COMMIT')
        except Exception:
            self.conn.execute('ROLLBACK')
            raise

    def close(self):
        self.conn.close()

@pytest.fixture
async def test_db():
    """Create a fresh test database for each test."""
    # Store the original database connection
    original_db = app.state.db if hasattr(app.state, 'db') else None

    # Create a new test database
    test_db_path = "test_db.duckdb"
    if os.path.exists(test_db_path):
        os.remove(test_db_path)

    # Create and set up the test database
    db_conn = duckdb.connect(test_db_path)
    db_conn.execute('''
        CREATE TABLE IF NOT EXISTS data (
            batch_id VARCHAR,
            id VARCHAR,
            data VARCHAR
        )
    ''')

    # Create async wrapper
    test_conn = AsyncDuckDBConnection(db_conn)

    # Replace the app's database connection
    app.state.db = test_conn

    yield test_conn

    # Clean up
    test_conn.close()
    if os.path.exists(test_db_path):
        os.remove(test_db_path)

    # Restore original connection
    if original_db is not None:
        app.state.db = original_db
    else:
        delattr(app.state, 'db')

@pytest.fixture(autouse=True)
async def clean_tasks():
    """Clean up any existing tasks before and after each test."""
    # Cancel any existing tasks
    for task_id, task in tasks.items():
        if not task.done():
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass
    tasks.clear()

    yield

    # Clean up after test
    for task_id, task in tasks.items():
        if not task.done():
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass
    tasks.clear()

# Let pytest-asyncio handle the event loop
@pytest.fixture(scope="session")
def event_loop():
    """Create and provide a new event loop for each test session."""
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()
