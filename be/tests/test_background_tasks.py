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
from main import tasks, insert_task, app, MAX_CONCURRENT_TASKS

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
    assert "message" in data
    assert f"Started task for batch {batch_id}" in data["message"]

    # Wait for task to start and insert data
    await asyncio.sleep(0.5)  # Reduced sleep time to prevent timeouts

    # Verify task is running and has inserted data
    result = test_db.execute(
        'SELECT COUNT(*) FROM data WHERE batch_id = ?',
        [batch_id]
    ).fetchone()
    assert result[0] > 0

    # Verify task is in the tasks dictionary
    assert batch_id in tasks
    task = tasks[batch_id]
    assert not task.done()

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
        assert "message" in data
        assert f"Started task for batch {batch_id}" in data["message"]
        await asyncio.sleep(0.5)  # Reduced sleep time to prevent timeouts

    # Verify all tasks are running and have data
    for batch_id in batch_ids:
        # Check database
        result = test_db.execute(
            'SELECT COUNT(*) FROM data WHERE batch_id = ?',
            [batch_id]
        ).fetchone()
        assert result[0] > 0

        # Check task status
        assert batch_id in tasks
        task = tasks[batch_id]
        assert not task.done()

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
        assert "Test error" in data["error"]

    # Verify task was cleaned up
    assert batch_id not in tasks

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
    await asyncio.sleep(0.5)  # Reduced sleep time to prevent timeouts

    # Attempt to create duplicate task
    response = test_client.post(f"/stream/{batch_id}")
    assert response.status_code == 400
    data = response.json()
    assert f"Task for batch {batch_id} already exists" in data["error"]

    # Verify original task is still running
    assert batch_id in tasks
    task = tasks[batch_id]
    assert not task.done()

@pytest.mark.asyncio
async def test_concurrent_task_limit(test_client, test_db, clean_tasks):
    """
    Test concurrent task limit enforcement.

    Verifies that:
    - Maximum concurrent task limit is enforced
    - Proper error responses are returned when limit is reached
    """
    # Create maximum number of tasks
    for i in range(MAX_CONCURRENT_TASKS):
        response = test_client.post(f"/stream/batch_{i}")
        assert response.status_code == 200
        await asyncio.sleep(0.5)  # Reduced sleep time to prevent timeouts

    # Attempt to create one more task
    response = test_client.post("/stream/batch_extra")
    assert response.status_code == 429
    data = response.json()
    assert "Maximum number of concurrent tasks reached" in data["error"]

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
    assert "Not implemented" in data["detail"]

    # Test agg endpoint
    response = test_client.get("/agg")
    assert response.status_code == 501
    data = response.json()
    assert "Not implemented" in data["detail"]
