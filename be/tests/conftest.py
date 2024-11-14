import asyncio
import pytest
import pytest_asyncio
import aiosqlite
from fastapi.testclient import TestClient
from main import app

# Use pytest-asyncio's built-in event loop fixture
pytestmark = pytest.mark.asyncio

@pytest_asyncio.fixture(scope="function")
async def setup_app():
    """Set up the FastAPI test application."""
    app.state.tasks = {}  # Initialize tasks dictionary
    try:
        yield app
    finally:
        # Clean up tasks - but don't cancel them, let clean_tasks handle that
        if hasattr(app.state, 'tasks'):
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

    # Clean up any remaining tasks with a longer timeout
    if hasattr(app.state, 'tasks'):
        tasks = list(app.state.tasks.values())
        for task in tasks:
            if isinstance(task, asyncio.Task) and not task.done():
                print(f"Waiting for task to complete...")
                try:
                    # Wait longer than the 2-second sleep in insert_task
                    await asyncio.wait_for(task, timeout=3.0)
                except asyncio.TimeoutError:
                    print(f"Task timed out, cancelling...")
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
    db = None
    try:
        # Create a new in-memory database for each test
        db = await aiosqlite.connect(':memory:')

        # Create the data table with an index on batch_id
        async with db.execute('''
            CREATE TABLE IF NOT EXISTS data (
                id INTEGER PRIMARY KEY,
                batch_id TEXT,
                timestamp DATETIME,
                value INTEGER
            )
        '''):
            pass

        async with db.execute('CREATE INDEX IF NOT EXISTS idx_batch_id ON data(batch_id)'):
            pass

        await db.commit()

        # Set the database connection in app state
        app.state.db = db

        yield db

    except Exception as e:
        print(f"Error setting up database: {str(e)}")
        if db:
            try:
                await db.close()
            except Exception:
                pass
        raise

    finally:
        # Wait for any pending tasks before closing the database
        if hasattr(app.state, 'tasks'):
            tasks = list(app.state.tasks.values())
            for task in tasks:
                if isinstance(task, asyncio.Task) and not task.done():
                    try:
                        await asyncio.wait_for(task, timeout=3.0)
                    except (asyncio.TimeoutError, asyncio.CancelledError):
                        pass

        # Clean up database connection
        if db:
            try:
                await db.close()
            except Exception as e:
                print(f"Error closing database: {str(e)}")

        if hasattr(app.state, 'db'):
            delattr(app.state, 'db')
