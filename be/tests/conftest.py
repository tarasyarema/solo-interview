import pytest
import asyncio
import duckdb
from fastapi.testclient import TestClient
from main import app
import pytest_asyncio

@pytest_asyncio.fixture(scope="function")
async def event_loop():
    """Create and provide a new event loop for each test."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    yield loop
    # Clean up pending tasks
    pending = asyncio.all_tasks(loop)
    for task in pending:
        if not task.done():
            task.cancel()
    await asyncio.gather(*pending, return_exceptions=True)
    loop.close()

@pytest.fixture(autouse=True)
async def setup_app():
    """Setup app state before each test."""
    # Initialize database
    db = duckdb.connect(':memory:')
    db.execute('''
        CREATE TABLE IF NOT EXISTS data (
            id INTEGER,
            batch_id VARCHAR,
            timestamp TIMESTAMP,
            value INTEGER
        )
    ''')
    app.state.db = db
    app.state.tasks = {}

    yield

    # Clean up
    if hasattr(app.state, 'tasks'):
        tasks = list(app.state.tasks.values())
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        app.state.tasks.clear()

    if hasattr(app.state, 'db'):
        app.state.db.close()

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

    # Create a new in-memory test database
    db_conn = duckdb.connect(':memory:')
    db_conn.execute("""
        CREATE TABLE IF NOT EXISTS data (
            id INTEGER,
            batch_id VARCHAR,
            timestamp TIMESTAMP,
            value INTEGER
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
    """Clean up tasks after each test."""
    yield

    # Clean up any remaining tasks
    if hasattr(app.state, 'tasks'):
        tasks = list(app.state.tasks.values())
        for task in tasks:
            if isinstance(task, asyncio.Task) and not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
                except Exception as e:
                    print(f"Error cleaning up task: {str(e)}")
        app.state.tasks.clear()
