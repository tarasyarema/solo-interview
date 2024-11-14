import asyncio
from unittest.mock import patch
import pytest
from fastapi import HTTPException

from main import tasks, insert_task

@pytest.mark.asyncio
async def test_task_creation_and_cleanup(test_client, test_db, clean_tasks):
    """Test that tasks are properly created and cleaned up"""
    batch_id = "test_batch_creation"

    # Create a task
    response = test_client.post(f"/stream/{batch_id}")
    assert response.status_code == 200

    # Verify task exists
    assert batch_id in tasks
    assert not tasks[batch_id].done()

    # Clean up
    tasks[batch_id].cancel()
    await asyncio.sleep(0.1)  # Allow task to cancel
    assert tasks[batch_id].cancelled()

@pytest.mark.asyncio
async def test_task_management(test_client, test_db, clean_tasks):
    """Test task management functionality"""
    batch_ids = [f"test_batch_{i}" for i in range(3)]

    # Create multiple tasks
    for batch_id in batch_ids:
        response = test_client.post(f"/stream/{batch_id}")
        assert response.status_code == 200
        assert batch_id in tasks

    # Verify all tasks are running
    assert len(tasks) == 3
    for batch_id in batch_ids:
        assert not tasks[batch_id].done()

@pytest.mark.asyncio
@patch('random.randint')
async def test_insert_task_data_generation(mock_randint, test_db, clean_tasks):
    """Test that insert_task generates data correctly with mocked random values"""
    mock_randint.return_value = 42
    batch_id = "test_batch_insert"

    # Start the task
    task = asyncio.create_task(insert_task(batch_id))
    tasks[batch_id] = task

    # Allow some data to be generated
    await asyncio.sleep(1)

    # Cancel the task
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    # Verify data was inserted with our mocked value
    result = test_db.execute(
        'SELECT data FROM data WHERE batch_id = ? LIMIT 1',
        [batch_id]
    ).fetchone()

    assert result is not None
    assert '"key": 42' in result[0]

@pytest.mark.asyncio
async def test_task_cleanup_on_error(test_client, test_db, clean_tasks):
    """Test that tasks are properly cleaned up when errors occur"""
    batch_id = "test_batch_error"

    # Create a task that will fail
    with patch('main.insert_task', side_effect=Exception("Test error")):
        response = test_client.post(f"/stream/{batch_id}")
        assert response.status_code == 200

        # Allow task to fail
        await asyncio.sleep(0.1)

        # Verify task is removed or marked as done
        assert batch_id not in tasks or tasks[batch_id].done()

@pytest.mark.asyncio
async def test_duplicate_task_creation(test_client, test_db, clean_tasks):
    """Test that creating a duplicate task is handled properly"""
    batch_id = "test_batch_duplicate"

    # Create first task
    response = test_client.post(f"/stream/{batch_id}")
    assert response.status_code == 200

    # Attempt to create duplicate task
    response = test_client.post(f"/stream/{batch_id}")
    assert response.status_code == 400
    assert "Task already exists" in response.json()["detail"]
