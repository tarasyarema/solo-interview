import pytest
import asyncio
from fastapi.testclient import TestClient
import duckdb
from pathlib import Path
import os
from main import app, tasks

@pytest.fixture
def test_client():
    """Create a test client with its own event loop."""
    with TestClient(app) as client:
        yield client

class AsyncDuckDBConnection:
    def __init__(self, connection):
        self.connection = connection

    def execute(self, query, parameters=None):
        return self.connection.execute(query, parameters)

    def close(self):
        self.connection.close()

@pytest.fixture
def test_db():
    """Create a fresh test database for each test."""
    test_db_path = "test_db.duckdb"
    db_conn = duckdb.connect(test_db_path)
    db_conn.execute("""
        CREATE TABLE IF NOT EXISTS data (
            id INTEGER,
            batch_id VARCHAR,
            data VARCHAR,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    yield AsyncDuckDBConnection(db_conn)
    db_conn.close()

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

@pytest.fixture(scope="function")
def event_loop():
    """Create and provide a new event loop for each test."""
    policy = asyncio.get_event_loop_policy()
    loop = policy.new_event_loop()
    asyncio.set_event_loop(loop)
    yield loop
    loop.close()
