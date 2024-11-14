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
        self._closed = False

    async def execute(self, query, params=None):
        """Execute a query asynchronously."""
        if self._closed:
            raise RuntimeError("Connection is closed")

        loop = asyncio.get_event_loop()
        try:
            if params:
                result = await loop.run_in_executor(
                    None, self.conn.execute, query, params
                )
            else:
                result = await loop.run_in_executor(
                    None, self.conn.execute, query
                )
            return result
        except Exception as e:
            print(f"Database error: {str(e)}")
            raise

    async def close(self):
        """Close the connection."""
        if not self._closed:
            try:
                self.conn.close()
            finally:
                self._closed = True

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
    yield db
    await db.close()
