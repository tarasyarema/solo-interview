import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
import asyncio
import json

@pytest.mark.asyncio
async def test_root_endpoint(test_client, test_db):
    """Test the root endpoint returns correct row and task counts"""
    # Insert some test data
    test_db.execute(
        'INSERT INTO data (batch_id, data, timestamp) VALUES (?, ?, CURRENT_TIMESTAMP)',
        ("test_batch", json.dumps({"value": 42}))
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
    result = test_db.execute(
        'SELECT COUNT(*) FROM data WHERE batch_id = ?',
        [batch_id]
    ).fetchone()
    assert result[0] > 0

@pytest.mark.asyncio
async def test_duplicate_task_creation(test_client, test_db, clean_tasks):
    """Test that creating a duplicate task fails properly"""
    batch_id = "test_batch_duplicate"

    # Create first task
    response = test_client.post(f"/stream/{batch_id}")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "started"
    assert data["batch_id"] == batch_id

    await asyncio.sleep(0.2)  # Wait for task to start

    # Attempt to create duplicate task
    response = test_client.post(f"/stream/{batch_id}")
    assert response.status_code == 400
    data = response.json()
    assert data["status"] == "error"
    assert "Task for batch test_batch_duplicate already exists" in data["detail"]

@pytest.mark.asyncio
async def test_unimplemented_tasks_endpoint(test_client):
    """Test that /tasks endpoint returns 501 Not Implemented"""
    response = test_client.get("/tasks")
    assert response.status_code == 501
    data = response.json()
    assert data["status"] == "error"
    assert data["detail"] == "Not implemented"

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
    from main import MAX_CONCURRENT_TASKS

    # Successfully create MAX_CONCURRENT_TASKS tasks
    for i in range(MAX_CONCURRENT_TASKS):
        response = test_client.post(f"/stream/batch_{i}")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "started"
        assert data["batch_id"] == f"batch_{i}"
        await asyncio.sleep(0.2)  # Reduced wait time

    # Verify all tasks are running and have data
    for i in range(MAX_CONCURRENT_TASKS):
        result = test_db.execute(
            'SELECT COUNT(*) FROM data WHERE batch_id = ?',
            [f"batch_{i}"]
        ).fetchone()
        assert result[0] > 0

    # Attempt to create another task should fail with 429 Too Many Requests
    response = test_client.post("/stream/batch_extra")
    assert response.status_code == 429
    data = response.json()
    assert data["status"] == "error"
    assert "Maximum number of concurrent tasks reached" in data["detail"]
