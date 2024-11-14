import pytest
import asyncio
from unittest.mock import patch
import json
from main import tasks, insert_task, app

@pytest.mark.asyncio
async def test_task_creation_and_cleanup(test_client, test_db, clean_tasks):
    """Test that tasks are properly created and cleaned up"""
    batch_id = "test_batch_creation"

    # Create a task
    response = test_client.post(f"/stream/{batch_id}")
    assert response.status_code == 200
    assert response.json()["status"] == "started"

    # Wait for task to start and insert some data
    await asyncio.sleep(0.2)  # Wait for task to fully initialize

    # Verify task exists and is running
    assert batch_id in tasks
    task = tasks[batch_id]
    assert not task.done()

    # Verify data was inserted
    result = test_db.execute(
        'SELECT COUNT(*) FROM data WHERE batch_id = ?',
        [batch_id]
    ).fetchone()
    assert result[0] > 0

    # Clean up
    task = tasks[batch_id]
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    # Task should still be in tasks dict after cancellation if it inserted data
    assert batch_id in tasks

@pytest.mark.asyncio
async def test_task_management(test_client, test_db, clean_tasks):
    """Test task management functionality"""
    # Clear any existing tasks
    tasks.clear()

    batch_ids = [f"test_batch_{i}" for i in range(3)]

    # Create multiple tasks
    for batch_id in batch_ids:
        response = test_client.post(f"/stream/{batch_id}")
        assert response.status_code == 200
        assert response.json()["status"] == "started"
        await asyncio.sleep(0.2)  # Wait for each task to fully initialize

    # Verify all tasks are running
    assert len(tasks) == 3
    for batch_id in batch_ids:
        assert batch_id in tasks
        task = tasks[batch_id]
        assert not task.done()
        # Verify data was inserted
        result = test_db.execute(
            'SELECT COUNT(*) FROM data WHERE batch_id = ?',
            [batch_id]
        ).fetchone()
        assert result[0] > 0

    # Clean up tasks
    for batch_id in batch_ids:
        task = tasks[batch_id]
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    # Tasks should still be in tasks dict after cancellation if they inserted data
    assert len(tasks) == 3

@pytest.mark.asyncio
async def test_insert_task_data_generation(test_db, clean_tasks):
    """Test that insert_task generates data correctly with mocked random values"""
    batch_id = "test_batch_insert"

    # Mock random.randint to return predictable values
    with patch('random.randint', return_value=42):
        # Start the task
        task = asyncio.create_task(insert_task(batch_id))
        tasks[batch_id] = task

        # Allow some data to be generated
        await asyncio.sleep(1.0)  # Increased wait time

        # Cancel the task
        task.cancel()
        try:
            await asyncio.sleep(0.5)  # Increased cleanup time
            await task
        except asyncio.CancelledError:
            pass

        # Verify data was inserted with our mocked value
        result = test_db.execute(
            'SELECT data FROM data WHERE batch_id = ? LIMIT 1',
            [batch_id]
        ).fetchone()

        assert result is not None
        data = json.loads(result[0])
        assert data["value"] == 42  # Changed from "key" to "value"

@pytest.mark.asyncio
async def test_task_cleanup_on_error(test_client, test_db, clean_tasks):
    """Test that tasks are properly cleaned up when errors occur"""
    batch_id = "test_batch_error"

    # Create a task that will fail
    with patch('main.insert_task', side_effect=Exception("Test error")):
        response = test_client.post(f"/stream/{batch_id}")
        assert response.status_code == 500
        assert "Test error" in response.json()["detail"]

        # Allow task to fail
        await asyncio.sleep(0.5)

        # Verify task is removed
        assert batch_id not in tasks

@pytest.mark.asyncio
async def test_duplicate_task_creation(test_client, test_db, clean_tasks):
    """Test that creating a duplicate task is handled properly"""
    batch_id = "test_batch_duplicate"

    # Create first task
    response = test_client.post(f"/stream/{batch_id}")
    assert response.status_code == 200
    assert response.json()["status"] == "started"
    await asyncio.sleep(0.2)  # Wait for task to fully initialize

    # Verify first task is running
    assert batch_id in tasks
    task = tasks[batch_id]
    assert not task.done()

    # Attempt to create duplicate task
    response = test_client.post(f"/stream/{batch_id}")
    assert response.status_code == 400
    assert "Task already exists" in response.json()["detail"]

    # Clean up
    task = tasks[batch_id]
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    # Task should still be in tasks dict after cancellation if it inserted data
    assert batch_id in tasks
