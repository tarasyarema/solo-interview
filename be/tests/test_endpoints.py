import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

@pytest.mark.asyncio
async def test_root_endpoint(test_client, test_db):
    """Test the root endpoint returns correct row and task counts"""
    response = test_client.get("/")
    assert response.status_code == 200
    data = response.json()
    assert "row_count" in data
    assert "task_count" in data
    assert isinstance(data["row_count"], int)
    assert isinstance(data["task_count"], int)

@pytest.mark.asyncio
async def test_create_stream_task(test_client, test_db, clean_tasks):
    """Test creating a new stream task"""
    batch_id = "test_batch_1"
    response = test_client.post(f"/stream/{batch_id}")
    assert response.status_code == 200
    data = response.json()
    assert data["message"] == "Batch started"
    assert data["batch_id"] == batch_id

@pytest.mark.asyncio
async def test_stream_endpoint(async_client, test_db, clean_tasks):
    """Test the streaming endpoint returns SSE data"""
    batch_id = "test_batch_2"
    async with async_client.stream("GET", f"/stream/{batch_id}") as response:
        assert response.status_code == 200
        assert response.headers["content-type"] == "text/event-stream"
        # Read first few events to verify format
        async for line in response.aiter_lines():
            if line.startswith("data: "):
                data_line = line.removeprefix("data: ")
                assert "value" in data_line
                break

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
async def test_concurrent_task_limit(test_client, clean_tasks):
    """Test that we can't create more than 5 concurrent tasks"""
    # Successfully create 5 tasks
    for i in range(5):
        response = test_client.post(f"/stream/batch_{i}")
        assert response.status_code == 200
        data = response.json()
        assert data["message"] == "Batch started"

    # Attempt to create 6th task should fail
    response = test_client.post("/stream/batch_6")
    assert response.status_code == 400
    data = response.json()
    assert "detail" in data
    assert "Maximum number of concurrent tasks reached" in data["detail"]
