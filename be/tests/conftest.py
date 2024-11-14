import pytest
import asyncio
from fastapi.testclient import TestClient
import duckdb
from pathlib import Path
import os
from main import app, tasks

@pytest.fixture(scope="function")
def test_client():
    return TestClient(app)

@pytest.fixture(scope="function")
async def test_db():
    """Create a fresh test database for each test."""
    # Store the original database connection
    original_db = app.state.db

    # Create a new test database
    test_db_path = "test_db.duckdb"
    if os.path.exists(test_db_path):
        os.remove(test_db_path)

    # Create and set up the test database
    test_conn = duckdb.connect(test_db_path)
    test_conn.execute('''
        CREATE TABLE IF NOT EXISTS data (
            batch_id VARCHAR,
            id VARCHAR,
            data VARCHAR
        )
    ''')

    # Replace the app's database connection
    app.state.db = test_conn

    try:
        yield test_conn
    finally:
        # Clean up
        test_conn.close()
        if os.path.exists(test_db_path):
            os.remove(test_db_path)
        # Restore original connection
        app.state.db = original_db

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
