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
    return TestClient(app)

class AsyncDuckDBConnection:
    def __init__(self, connection):
        self.connection = connection

    def execute(self, query, parameters=None):
        return self.connection.execute(query, parameters)

    def close(self):
        if not self.connection.is_closed():
            self.connection.close()

@pytest.fixture(scope="function")
async def test_db():
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

    # Create async wrapper and set it as app state
    test_conn = AsyncDuckDBConnection(db_conn)
    app.state.db = test_conn

    yield test_conn

    # Clean up
    test_conn.close()
    if original_db is not None:
        app.state.db = original_db

@pytest.fixture(autouse=True)
async def clean_tasks(event_loop):
    """Clean up any existing tasks before and after each test."""
    async def cleanup_tasks():
        for task_id, task in list(tasks.items()):
            if not task.done():
                task.cancel()
                try:
                    await asyncio.wait_for(task, timeout=0.5)
                except (asyncio.CancelledError, asyncio.TimeoutError):
                    pass
        tasks.clear()

    await cleanup_tasks()
    yield
    await cleanup_tasks()
