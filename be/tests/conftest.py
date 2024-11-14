import asyncio
import pytest
import duckdb
from fastapi.testclient import TestClient
from main import app

# Use pytest-asyncio's built-in event loop fixture
pytestmark = pytest.mark.asyncio

class AsyncDuckDBConnection:
    """Async wrapper for DuckDB connection."""
    def __init__(self, conn):
        self.conn = conn

    async def execute(self, query, params=None):
        """Execute a query asynchronously."""
        try:
            if params is None:
                return await asyncio.to_thread(self.conn.execute, query)
            return await asyncio.to_thread(self.conn.execute, query, params)
        except Exception as e:
            print(f"Database error: {str(e)}")
            raise

    async def close(self):
        """Close the database connection."""
        try:
            if not self.conn.closed():
                await asyncio.to_thread(self.conn.close)
        except Exception as e:
            print(f"Error closing connection: {str(e)}")
            raise

    async def __aenter__(self):
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.close()

@pytest.fixture(scope="function")
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
                    await asyncio.wait_for(task, timeout=0.5)
                except (asyncio.CancelledError, asyncio.TimeoutError):
                    pass
                except Exception as e:
                    print(f"Error cleaning up task: {str(e)}")

        app.state.tasks.clear()

@pytest.fixture
async def test_db():
    """Create a test database connection."""
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
    app.state.db = db  # Set the database in app state before yielding
    try:
        yield db
    finally:
        await db.close()
        if hasattr(app.state, 'db'):
            delattr(app.state, 'db')
