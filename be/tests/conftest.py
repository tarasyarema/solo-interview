import os
import tempfile
from typing import AsyncGenerator, Generator

import pytest
from fastapi.testclient import TestClient
from httpx import AsyncClient
import duckdb

from main import app, tasks

@pytest.fixture
def test_client() -> Generator[TestClient, None, None]:
    with TestClient(app) as client:
        yield client

@pytest.fixture
async def async_client() -> AsyncGenerator[AsyncClient, None]:
    async with AsyncClient(app=app, base_url="http://test") as ac:
        yield ac

@pytest.fixture(autouse=True)
async def test_db():
    """Fixture that creates a fresh database for each test"""
    # Create a temporary file for the test database
    db_file = tempfile.mktemp()

    # Store the original database connection
    original_db = app.db if hasattr(app, 'db') else None

    # Create a new database connection for testing
    test_db_conn = duckdb.connect(db_file)
    test_db_conn.execute('''
        CREATE TABLE IF NOT EXISTS data (
            batch_id TEXT,
            id TEXT,
            data TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_data_id ON data (batch_id);
    ''')

    # Replace the app's database connection with our test database
    app.db = test_db_conn

    yield test_db_conn

    # Cleanup
    test_db_conn.close()
    if os.path.exists(db_file):
        os.unlink(db_file)

    # Restore original database connection
    if original_db is not None:
        app.db = original_db

@pytest.fixture(autouse=True)
async def clean_tasks():
    """Fixture to ensure background tasks are cleaned up after tests"""
    # Clear any existing tasks before the test
    tasks.clear()

    yield

    # Cancel any running tasks
    for task_id, task in tasks.items():
        if not task.done():
            task.cancel()
            try:
                await task
            except:
                pass
    tasks.clear()
