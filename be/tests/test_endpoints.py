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
    assert data["status"] == "started"  # Updated to match new response format
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
async def test_stream_endpoint(test_client, test_db, clean_tasks):
    """Test the streaming endpoint returns SSE data"""
    batch_id = "test_batch_2"

    # First create the task
    response = test_client.post(f"/stream/{batch_id}")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "started"  # Updated to match new response format

    # Wait for task to start and insert data
    await asyncio.sleep(0.2)  # Reduced wait time

    # Verify data was inserted
    result = test_db.execute(
        'SELECT COUNT(*) FROM data WHERE batch_id = ?',
        [batch_id]
    ).fetchone()
    assert result[0] > 0

    # Now test the stream
    response = test_client.get(f"/stream/{batch_id}")
    assert response.status_code == 200
    assert response.headers["content-type"] == "text/event-stream"

    # Read the response content with timeout
    content = b""
    async def read_stream():
        nonlocal content
        for chunk in response.iter_bytes():
            content += chunk
            if b"data: " in content:
                return

    try:
        await asyncio.wait_for(read_stream(), timeout=1.0)  # Reduced timeout
        data_line = content.decode().split("\n")[0].removeprefix("data: ")
        assert "value" in data_line
    except asyncio.TimeoutError:
        pytest.fail("Timeout waiting for stream data")

@pytest.mark.asyncio
async def test_unimplemented_tasks_endpoint(test_client):
    """Test that /tasks endpoint returns 501 Not Implemented"""
    response = test_client.get("/tasks")
    assert response.status_code == 501
    data = response.json()
    assert "detail" in data
    assert data["detail"] == "Not implemented"

@pytest.mark.asyncio
async def test_unimplemented_stop_endpoint(test_client):
    """Test that DELETE /stream/{batch_id} returns 501 Not Implemented"""
    response = test_client.delete("/stream/test_batch")
    assert response.status_code == 501
    data = response.json()
    assert "detail" in data
    assert data["detail"] == "Not implemented"

@pytest.mark.asyncio
async def test_unimplemented_agg_endpoint(test_client):
    """Test that GET /agg/{batch_id} returns 501 Not Implemented"""
    response = test_client.get("/agg/test_batch")
    assert response.status_code == 501
    data = response.json()
    assert "detail" in data
    assert data["detail"] == "Not implemented"

@pytest.mark.asyncio
async def test_concurrent_task_limit(test_client, test_db, clean_tasks):
    """Test that we can't create more than MAX_CONCURRENT_TASKS tasks"""
    # Successfully create MAX_CONCURRENT_TASKS tasks
    for i in range(10):  # Using our new limit of 10
        response = test_client.post(f"/stream/batch_{i}")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "started"  # Updated to match new response format
        await asyncio.sleep(0.2)  # Reduced wait time

    # Verify all tasks are running and have data
    for i in range(10):
        result = test_db.execute(
            'SELECT COUNT(*) FROM data WHERE batch_id = ?',
            [f"batch_{i}"]
        ).fetchone()
        assert result[0] > 0

    # Attempt to create another task should fail with 429 Too Many Requests
    response = test_client.post("/stream/batch_extra")
    assert response.status_code == 429  # Updated to match new error code
    data = response.json()
    assert "detail" in data
    assert "Too many concurrent tasks" in data["detail"]  # Updated error message
