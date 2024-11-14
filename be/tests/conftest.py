import pytest
import asyncio
import duckdb
from fastapi.testclient import TestClient
from main import app
import pytest_asyncio

@pytest.fixture(scope="function")
async def event_loop():
    """Create an instance of the default event loop for each test case."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    yield loop
    # Clean up pending tasks
    pending = asyncio.all_tasks(loop)
    for task in pending:
        task.cancel()

    # Wait for tasks to complete with a timeout
    if pending:
        try:
            loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
        except (asyncio.CancelledError, Exception):
            pass

    loop.run_until_complete(loop.shutdown_asyncgens())
    loop.close()

@pytest.fixture(scope="function")
async def setup_app():
    """Set up the FastAPI application with a clean state for each test."""
    # Initialize app state
    if not hasattr(app.state, 'tasks'):
        app.state.tasks = {}
    else:
        app.state.tasks.clear()

    # Create a new in-memory DuckDB database for testing
    conn = duckdb.connect(':memory:')
    conn.execute('''
        CREATE TABLE IF NOT EXISTS data (
            id INTEGER,
            batch_id VARCHAR,
            timestamp TIMESTAMP,
            value INTEGER
        )
    ''')
    app.state.db = AsyncDuckDBConnection(conn)

    yield app

    # Clean up database
    await app.state.db.close()

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
    # Setup - ensure tasks dictionary exists
    if not hasattr(app.state, 'tasks'):
        app.state.tasks = {}
    else:
        app.state.tasks.clear()

    yield

    # Clean up any remaining tasks
    if hasattr(app.state, 'tasks'):
        tasks = list(app.state.tasks.values())
        for task in tasks:
            if isinstance(task, asyncio.Task) and not task.done():
                task.cancel()
                try:
                    # Wait for task to be cancelled with timeout
                    await asyncio.wait_for(
                        asyncio.shield(task),  # Shield to prevent cancellation propagation
                        timeout=0.5
                    )
                except (asyncio.CancelledError, asyncio.TimeoutError):
                    pass
                except Exception as e:
                    print(f"Error cleaning up task: {str(e)}")

        # Clear all tasks after cleanup
        app.state.tasks.clear()

        # Wait a bit to ensure all tasks are properly cleaned up
        await asyncio.sleep(0.1)

        # Ensure event loop is clean
        loop = asyncio.get_event_loop()
        for task in asyncio.all_tasks(loop):
            if not task.done() and task != asyncio.current_task():
                task.cancel()
                try:
                    await asyncio.wait_for(task, timeout=0.1)
                except (asyncio.CancelledError, asyncio.TimeoutError):
                    pass
