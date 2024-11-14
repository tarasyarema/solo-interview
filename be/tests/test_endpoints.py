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
    assert data["message"] == "Batch started"
    assert data["batch_id"] == batch_id

    # Wait for task to start and insert data
    await asyncio.sleep(0.5)

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

    # Wait for task to start and insert data
    await asyncio.sleep(0.5)

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
        await asyncio.wait_for(read_stream(), timeout=2.0)
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
    """Test that we can't create more than 5 concurrent tasks"""
    # Successfully create 5 tasks
    for i in range(5):
        response = test_client.post(f"/stream/batch_{i}")
        assert response.status_code == 200
        data = response.json()
        assert data["message"] == "Batch started"
        await asyncio.sleep(0.5)  # Give each task time to start and insert data

    # Verify all tasks are running and have data
    for i in range(5):
        result = test_db.execute(
            'SELECT COUNT(*) FROM data WHERE batch_id = ?',
            [f"batch_{i}"]
        ).fetchone()
        assert result[0] > 0

    # Attempt to create 6th task should fail
    response = test_client.post("/stream/batch_6")
    assert response.status_code == 400
    data = response.json()
    assert "detail" in data
    assert "Maximum number of concurrent tasks reached" in data["detail"]
