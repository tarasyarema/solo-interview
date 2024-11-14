import pytest
import asyncio
import duckdb
from fastapi.testclient import TestClient
from main import app, tasks

@pytest.fixture(scope="function")
def event_loop():
    """Create and provide a new event loop for each test."""
    policy = asyncio.get_event_loop_policy()
    loop = policy.new_event_loop()
    asyncio.set_event_loop(loop)
    yield loop
    loop.close()

@pytest.fixture
def test_client():
    """Create a test client."""
    return TestClient(app, base_url="http://test")

class AsyncDuckDBConnection:
    def __init__(self, connection):
        self.connection = connection
        self._closed = False

    def execute(self, query, parameters=None):
        if self._closed:
            raise RuntimeError("Connection is closed")
        return self.connection.execute(query, parameters)

    def close(self):
        if not self._closed:
            try:
                self.connection.close()
            finally:
                self._closed = True

@pytest.fixture
def test_db():
    """Create a fresh test database for each test."""
    # Store the original database connection
    original_db = app.state.db if hasattr(app.state, 'db') else None

    # Create a new test database
    test_db_path = "test_db.duckdb"
    db_conn = duckdb.connect(test_db_path)
    db_conn.execute("""
        CREATE TABLE IF NOT EXISTS data (
            id BIGINT,
            batch_id VARCHAR,
            data VARCHAR,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Clean up any existing data
    db_conn.execute('DELETE FROM data')

    # Create async wrapper and set it as app state
    test_conn = AsyncDuckDBConnection(db_conn)
    app.state.db = test_conn

    yield test_conn

    # Clean up
    test_conn.close()
    if original_db is not None:
        app.state.db = original_db
    else:
        delattr(app.state, 'db')

@pytest.fixture(autouse=True)
async def clean_tasks():
    """Clean up any existing tasks before and after each test."""
    # Clear tasks dict before each test
    for task_id, task in list(tasks.items()):
        if not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
    tasks.clear()

    yield

    # Clean up tasks after test
    for task_id, task in list(tasks.items()):
        if not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        elif task.done() and task.exception():
            print(f"Task {task_id} failed with: {task.exception()}")
    tasks.clear()
