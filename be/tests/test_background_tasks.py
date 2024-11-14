import pytest
import asyncio
from unittest.mock import patch
import json
from main import tasks, insert_task, app

@pytest.mark.asyncio
async def test_task_creation_and_cleanup(test_client, test_db, clean_tasks):
    """Test that tasks are created and cleaned up properly."""
    batch_id = "test_batch_0"

    # Create a task
    response = test_client.post(f"/stream/{batch_id}")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "started"
    assert data["batch_id"] == batch_id

    # Wait for task to start and insert data
    await asyncio.sleep(1.5)  # Increased sleep time to ensure task is running

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
    """Test that multiple tasks can be managed simultaneously."""
    batch_ids = ["test_batch_0", "test_batch_1", "test_batch_2"]

    # Create multiple tasks
    for batch_id in batch_ids:
        response = test_client.post(f"/stream/{batch_id}")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "started"
        assert data["batch_id"] == batch_id
        await asyncio.sleep(1.5)  # Increased sleep time to ensure task is running

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
    """Test that tasks are cleaned up properly when they fail."""
    batch_id = "test_batch_error"

    # Mock the insert_task function to raise an error
    async def mock_error(batch_id: str):
        raise Exception("Test error")

    with patch('main.insert_task', side_effect=mock_error):
        response = test_client.post(f"/stream/{batch_id}")
        assert response.status_code == 500
        data = response.json()
        assert "Test error" in data["detail"]

    # Verify task was cleaned up
    assert batch_id not in tasks

@pytest.mark.asyncio
async def test_duplicate_task_creation(test_client, test_db, clean_tasks):
    """Test that creating a duplicate task fails properly."""
    batch_id = "test_batch_duplicate"

    # Create first task
    response = test_client.post(f"/stream/{batch_id}")
    assert response.status_code == 200
    await asyncio.sleep(1.5)  # Increased sleep time to ensure task is running

    # Attempt to create duplicate task
    response = test_client.post(f"/stream/{batch_id}")
    assert response.status_code == 400
    data = response.json()
    assert f"Task {batch_id} already exists" in data["detail"]

    # Verify original task is still running
    assert batch_id in tasks
    task = tasks[batch_id]
    assert not task.done()

@pytest.mark.asyncio
async def test_concurrent_task_limit(test_client, test_db, clean_tasks):
    """Test that concurrent task limit is enforced."""
    from main import MAX_CONCURRENT_TASKS

    # Create maximum number of tasks
    for i in range(MAX_CONCURRENT_TASKS):
        response = test_client.post(f"/stream/batch_{i}")
        assert response.status_code == 200
        await asyncio.sleep(1.5)  # Increased sleep time to ensure task is running

    # Attempt to create one more task
    response = test_client.post("/stream/batch_extra")
    assert response.status_code == 429
    data = response.json()
    assert "Too many concurrent tasks" in data["detail"]
