import asyncio
import pytest
import pytest_asyncio
import duckdb
from fastapi.testclient import TestClient
from main import app

# Use pytest-asyncio's built-in event loop fixture
pytestmark = pytest.mark.asyncio

class AsyncDuckDBConnection:
    """Async wrapper for DuckDB connection."""
    def __init__(self, conn):
        self.conn = conn
        self._closed = False

    async def execute(self, query, params=None):
        """Execute a query asynchronously."""
        if self._closed:
            raise RuntimeError("Connection is closed")
        try:
            if params is None:
                return await asyncio.to_thread(lambda: self.conn.execute(query))
            return await asyncio.to_thread(lambda: self.conn.execute(query, params))
        except Exception as e:
            print(f"Database error: {str(e)}")
            raise

    async def close(self):
        """Close the database connection."""
        if not self._closed:
            try:
                await asyncio.to_thread(self.conn.close)
                self._closed = True
            except Exception as e:
                print(f"Error closing connection: {str(e)}")
                raise

    async def __aenter__(self):
        """Async context manager entry."""
        if self._closed:
            raise RuntimeError("Connection is closed")
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.close()

@pytest_asyncio.fixture(scope="function")
async def setup_app():
    """Set up the FastAPI test application."""
    app.state.tasks = {}  # Initialize tasks dictionary
    try:
        yield app
    finally:
        # Clean up tasks
        if hasattr(app.state, 'tasks'):
            for task in app.state.tasks.values():
                if not task.done():
                    task.cancel()
            app.state.tasks = {}

@pytest.fixture
def test_client():
    """Create a test client."""
    return TestClient(app)

@pytest_asyncio.fixture(scope="function")
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
                    await asyncio.wait_for(task, timeout=0.5)
                except (asyncio.CancelledError, asyncio.TimeoutError):
                    pass
                except Exception as e:
                    print(f"Error cleaning up task: {str(e)}")

        app.state.tasks.clear()

@pytest_asyncio.fixture(scope="function")
async def test_db(setup_app):
    """Create a test database connection."""
    print("Setting up test database...")  # Debug logging
    # Create a new in-memory database for each test
    conn = duckdb.connect(':memory:')
    conn.execute('''
        CREATE TABLE IF NOT EXISTS data (
            id INTEGER,
            batch_id VARCHAR,
            timestamp TIMESTAMP,
            value INTEGER
        )
    ''')
    db = AsyncDuckDBConnection(conn)
    print("Database initialized, setting up app state...")  # Debug logging
    app.state.db = db
    await db.__aenter__()  # Ensure proper async context initialization
    try:
        print("Yielding database connection...")  # Debug logging
        yield db
    finally:
        print("Cleaning up database connection...")  # Debug logging
        await db.__aexit__(None, None, None)
        if hasattr(app.state, 'db'):
            delattr(app.state, 'db')
