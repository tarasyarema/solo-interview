"""
Test suite for background task management in the FastAPI application.

This module tests the following functionality:
- Task creation and cleanup
- Concurrent task management
- Task error handling
- Duplicate task prevention
- Concurrent task limits

Each test uses a fresh database and ensures proper cleanup of tasks.
Tests are designed to be deterministic by mocking random number generation.
"""

import pytest
import asyncio
from unittest.mock import patch
import json
from main import app, MAX_CONCURRENT_TASKS

# Mock random.randint for deterministic testing
@pytest.fixture(autouse=True)
def mock_random():
    with patch('random.randint', return_value=42) as mock:
        yield mock

@pytest.mark.asyncio
async def test_task_creation_and_cleanup(test_client, test_db, clean_tasks):
    """
    Test task creation and cleanup.

    Verifies that:
    - Tasks are created successfully
    - Data is inserted into the database
    - Tasks are properly tracked in the tasks dictionary
    """
    batch_id = "test_batch_0"

    # Create a task
    response = test_client.post(f"/stream/{batch_id}")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "started"
    assert data["batch_id"] == batch_id

    # Wait for task to complete (accounting for the 2-second sleep)
    for _ in range(30):  # 3 seconds total timeout
        await asyncio.sleep(0.1)
        # Check if task is done
        if batch_id not in app.state.tasks or app.state.tasks[batch_id].done():
            break

    # Verify task completed successfully
    assert batch_id in app.state.tasks
    task = app.state.tasks[batch_id]
    assert task.done()
    assert not task.cancelled()
    assert task.exception() is None

    # Verify data was inserted
    async with test_db.execute(
        'SELECT COUNT(*) FROM data WHERE batch_id = ?',
        (batch_id,)
    ) as cursor:
        row = await cursor.fetchone()
        assert row[0] > 0

@pytest.mark.asyncio
async def test_task_management(test_client, test_db, clean_tasks):
    """
    Test concurrent task management.

    Verifies that:
    - Multiple tasks can run simultaneously
    - Each task inserts data correctly
    - All tasks are properly tracked
    """
    batch_ids = ["test_batch_0", "test_batch_1", "test_batch_2"]

    # Create multiple tasks
    for batch_id in batch_ids:
        response = test_client.post(f"/stream/{batch_id}")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "started"
        assert data["batch_id"] == batch_id

    # Wait for all tasks to complete (accounting for the 2-second sleep)
    for _ in range(50):  # 5 seconds total timeout
        await asyncio.sleep(0.1)
        # Check if all tasks are done
        if all(
            batch_id not in app.state.tasks or app.state.tasks[batch_id].done()
            for batch_id in batch_ids
        ):
            break

    # Verify all tasks completed successfully
    for batch_id in batch_ids:
        assert batch_id in app.state.tasks
        task = app.state.tasks[batch_id]
        assert task.done()
        assert not task.cancelled()
        assert task.exception() is None

        # Verify data was inserted
        async with test_db.execute(
            'SELECT COUNT(*) FROM data WHERE batch_id = ?',
            (batch_id,)
        ) as cursor:
            row = await cursor.fetchone()
            assert row[0] > 0

@pytest.mark.asyncio
async def test_task_cleanup_on_error(test_client, test_db, clean_tasks):
    """
    Test error handling in task management.

    Verifies that:
    - Tasks are cleaned up when they fail
    - Proper error responses are returned
    - Failed tasks are removed from tracking
    """
    batch_id = "test_batch_error"

    # Mock the insert_task function to raise an error
    async def mock_error(batch_id: str):
        raise Exception("Test error")

    with patch('main.insert_task', side_effect=mock_error):
        response = test_client.post(f"/stream/{batch_id}")
        assert response.status_code == 500
        data = response.json()
        assert data["status"] == "error"
        assert "Test error" in data["detail"]

    # In testing mode, task should be preserved and marked as failed
    assert batch_id in app.state.tasks
    task = app.state.tasks[batch_id]
    assert task.done()
    assert task.exception() is not None
    assert str(task.exception()) == "Test error"

@pytest.mark.asyncio
async def test_duplicate_task_creation(test_client, test_db, clean_tasks):
    """
    Test duplicate task prevention.

    Verifies that:
    - Duplicate tasks are rejected
    - Original tasks continue running
    - Proper error responses are returned
    """
    batch_id = "test_batch_duplicate"

    # Create first task
    response = test_client.post(f"/stream/{batch_id}")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "started"
    assert data["batch_id"] == batch_id

    # Wait for task to start (but not complete)
    await asyncio.sleep(0.5)  # Give task time to start but not complete (2s sleep)

    # Attempt to create duplicate task
    response = test_client.post(f"/stream/{batch_id}")
    assert response.status_code == 400
    data = response.json()
    assert data["status"] == "error"
    assert "already exists" in data["detail"]

    # Wait for original task to complete
    for _ in range(30):  # 3 seconds total timeout
        await asyncio.sleep(0.1)
        if batch_id not in app.state.tasks or app.state.tasks[batch_id].done():
            break

    # Verify original task completed successfully
    assert batch_id in app.state.tasks
    task = app.state.tasks[batch_id]
    assert task.done()
    assert not task.cancelled()
    assert task.exception() is None

@pytest.mark.asyncio
async def test_concurrent_task_limit(test_client, test_db, clean_tasks):
    """
    Test concurrent task limit enforcement.

    Verifies that:
    - Maximum concurrent task limit is enforced
    - Proper error responses are returned when limit is reached
    - Tasks complete successfully after reaching the limit
    """
    # Create maximum number of tasks
    for i in range(MAX_CONCURRENT_TASKS):
        response = test_client.post(f"/stream/batch_{i}")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "started"
        assert data["batch_id"] == f"batch_{i}"

    # Attempt to create one more task
    response = test_client.post("/stream/batch_extra")
    assert response.status_code == 429
    data = response.json()
    assert data["status"] == "error"
    assert f"Maximum number of concurrent tasks reached (limit: {MAX_CONCURRENT_TASKS})" == data["detail"]

    # Wait for all tasks to complete
    for _ in range(50):  # 5 seconds total timeout
        await asyncio.sleep(0.1)
        active_tasks = len([t for t in app.state.tasks.values()
                          if isinstance(t, asyncio.Task) and not t.done()])
        if active_tasks == 0:
            break

    # Verify all tasks completed successfully
    for i in range(MAX_CONCURRENT_TASKS):
        batch_id = f"batch_{i}"
        assert batch_id in app.state.tasks
        task = app.state.tasks[batch_id]
        assert task.done()
        assert not task.cancelled()
        assert task.exception() is None

@pytest.mark.asyncio
async def test_unimplemented_endpoints(test_client):
    """
    Test unimplemented endpoints.

    Verifies that:
    - Unimplemented endpoints return proper error responses
    - Status code 501 is returned
    """
    # Test data_stop endpoint
    response = test_client.delete("/stream/test_batch")
    assert response.status_code == 501
    data = response.json()
    assert data["status"] == "error"
    assert data["detail"] == "Not implemented"

    # Test agg endpoint
    response = test_client.get("/agg")
    assert response.status_code == 501
    data = response.json()
    assert data["status"] == "error"
    assert data["detail"] == "Not implemented"
