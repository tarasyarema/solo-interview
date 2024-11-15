import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
import asyncio
import json
from main import app, MAX_CONCURRENT_TASKS

@pytest.mark.asyncio
async def test_root_endpoint(test_client, test_db):
    """Test the root endpoint returns correct row and task counts"""
    # Insert some test data
    await test_db.execute(
        'INSERT INTO data (id, batch_id, timestamp, value) VALUES (?, ?, CURRENT_TIMESTAMP, ?)',
        (1, "test_batch", 42)
    )

    response = test_client.get("/")
    assert response.status_code == 200
    data = response.json()
    assert "row_count" in data
    assert "task_count" in data
    assert isinstance(data["row_count"], int)
    assert isinstance(data["task_count"], int)
    assert data["row_count"] > 0  # Should have our test data

@pytest.mark.asyncio
async def test_create_stream_task(test_client, test_db, clean_tasks):
    """Test creating a new stream task"""
    batch_id = "test_batch_1"
    response = test_client.post(f"/stream/{batch_id}")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "started"
    assert data["batch_id"] == batch_id

    # Wait for task to start and insert data
    await asyncio.sleep(0.2)  # Reduced wait time

    # Verify data was inserted
    result = await test_db.execute(
        'SELECT COUNT(*) FROM data WHERE batch_id = ?',
        [batch_id]
    )
    row = await result.fetchone()
    assert row[0] > 0

@pytest.mark.asyncio
async def test_duplicate_task_creation(test_client, test_db, clean_tasks):
    """Test that creating a duplicate task returns an error."""
    # Create first task
    response = test_client.post("/stream/test_batch_duplicate")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "started"
    assert data["batch_id"] == "test_batch_duplicate"

    # Wait for task to properly start and insert some data
    await asyncio.sleep(0.5)  # Increased wait time

    # Verify task is running and has data
    result = await test_db.execute(
        'SELECT COUNT(*) FROM data WHERE batch_id = ?',
        ["test_batch_duplicate"]
    )
    row = await result.fetchone()
    assert row[0] > 0

    # Try to create duplicate task
    response = test_client.post("/stream/test_batch_duplicate")
    assert response.status_code == 400
    data = response.json()
    assert data["status"] == "error"
    assert "already exists" in data["detail"]

@pytest.mark.asyncio
async def test_tasks_endpoint(test_client, test_db, clean_tasks):
    """Test that /tasks endpoint returns list of active tasks"""
    # Create a test task first
    batch_id = "test_batch_tasks"
    response = test_client.post(f"/stream/{batch_id}")
    assert response.status_code == 200

    # Wait for task to properly start and insert some data
    await asyncio.sleep(0.5)  # Increased wait time

    # Verify task is running and has data
    result = await test_db.execute(
        'SELECT COUNT(*) FROM data WHERE batch_id = ?',
        [batch_id]
    )
    row = await result.fetchone()
    assert row[0] > 0

    # Check tasks endpoint
    response = test_client.get("/tasks")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "success"
    assert isinstance(data["tasks"], dict)
    assert batch_id in data["tasks"]
    assert data["tasks"][batch_id] is True  # Task should be active

@pytest.mark.asyncio
async def test_unimplemented_stop_endpoint(test_client):
    """Test that DELETE /stream/{batch_id} returns 501 Not Implemented"""
    response = test_client.delete("/stream/test_batch")
    assert response.status_code == 501
    data = response.json()
    assert data["status"] == "error"
    assert data["detail"] == "Not implemented"

@pytest.mark.asyncio
async def test_unimplemented_agg_endpoint(test_client):
    """Test that GET /agg returns 501 Not Implemented"""
    response = test_client.get("/agg")
    assert response.status_code == 501
    data = response.json()
    assert data["status"] == "error"
    assert data["detail"] == "Not implemented"

@pytest.mark.asyncio
async def test_concurrent_task_limit(test_client, test_db, clean_tasks):
    """Test that we can't create more than MAX_CONCURRENT_TASKS tasks"""
    tasks_created = []
    try:
        # Successfully create MAX_CONCURRENT_TASKS tasks
        for i in range(MAX_CONCURRENT_TASKS):
            batch_id = f"batch_{i}"
            response = test_client.post(f"/stream/{batch_id}")
            assert response.status_code == 200
            data = response.json()
            assert data["status"] == "started"
            assert data["batch_id"] == batch_id
            tasks_created.append(batch_id)
            await asyncio.sleep(0.5)  # Increased wait time

        # Verify all tasks are running and have data
        for batch_id in tasks_created:
            result = await test_db.execute(
                'SELECT COUNT(*) FROM data WHERE batch_id = ?',
                [batch_id]
            )
            row = await result.fetchone()
            assert row[0] > 0

        # Attempt to create another task should fail with 429 Too Many Requests
        response = test_client.post("/stream/batch_extra")
        assert response.status_code == 429
        data = response.json()
        assert data["status"] == "error"
        assert f"Maximum number of concurrent tasks reached (limit: {MAX_CONCURRENT_TASKS})" == data["detail"]

    finally:
        # Clean up all tasks
        if hasattr(app.state, 'tasks'):
            for batch_id in tasks_created:
                if batch_id in app.state.tasks:
                    task = app.state.tasks[batch_id]
                    if isinstance(task, asyncio.Task) and not task.done():
                        task.cancel()
                        try:
                            await asyncio.wait_for(task, timeout=0.5)
                        except (asyncio.CancelledError, asyncio.TimeoutError):
                            pass
                        except Exception as e:
                            print(f"Error cleaning up task {batch_id}: {str(e)}")
