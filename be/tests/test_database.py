import pytest
import asyncio
import json
from datetime import datetime, timedelta

@pytest.mark.asyncio
async def test_data_insertion(test_db, clean_tasks):
    """Test basic data insertion functionality"""
    batch_id = "test_batch_db"
    test_data = json.dumps({"value": 42})
    current_time = datetime.now()

    # Insert test data
    test_db.execute(
        'INSERT INTO data (batch_id, data, timestamp) VALUES (?, ?, ?)',
        (batch_id, test_data, current_time)
    )

    # Verify data was inserted
    result = test_db.execute('SELECT COUNT(*) FROM data WHERE batch_id = ?', [batch_id]).fetchone()
    assert result[0] == 1

@pytest.mark.asyncio
async def test_data_retrieval(test_db, clean_tasks):
    """Test data retrieval functionality"""
    batch_id = "test_batch_retrieve"
    test_data = json.dumps({"value": 100})
    current_time = datetime.now()

    # Insert test data
    test_db.execute(
        'INSERT INTO data (batch_id, data, timestamp) VALUES (?, ?, ?)',
        (batch_id, test_data, current_time)
    )

    # Retrieve and verify data
    result = test_db.execute(
        'SELECT data FROM data WHERE batch_id = ?',
        [batch_id]
    ).fetchone()

    assert result is not None
    assert result[0] == test_data

@pytest.mark.asyncio
async def test_data_aggregation(test_db, clean_tasks):
    """Test data aggregation for the last minute"""
    batch_id = "test_batch_agg"
    now = datetime.now()

    # Insert test data with timestamps in the last minute
    for i in range(5):
        test_data = json.dumps({"value": i * 10})
        timestamp = now - timedelta(seconds=i * 10)
        test_db.execute(
            'INSERT INTO data (batch_id, data, timestamp) VALUES (?, ?, ?)',
            (batch_id, test_data, timestamp)
        )

    # Verify data count
    result = test_db.execute(
        'SELECT COUNT(*) FROM data WHERE batch_id = ?',
        [batch_id]
    ).fetchone()
    assert result[0] == 5

    # Verify data ordering
    results = test_db.execute(
        'SELECT data FROM data WHERE batch_id = ? ORDER BY timestamp DESC',
        [batch_id]
    ).fetchall()
    assert len(results) == 5
    for i, row in enumerate(results):
        data = json.loads(row[0])
        assert "value" in data
        assert data["value"] == i * 10  # Values should match the order we inserted them

@pytest.mark.asyncio
async def test_data_cleanup(test_db, clean_tasks):
    """Test data cleanup functionality"""
    batch_id = "test_batch_cleanup"
    current_time = datetime.now()

    # Insert some test data
    test_db.execute(
        'INSERT INTO data (batch_id, data, timestamp) VALUES (?, ?, ?)',
        (batch_id, json.dumps({"value": 1}), current_time)
    )

    # Verify data exists
    result = test_db.execute(
        'SELECT COUNT(*) FROM data WHERE batch_id = ?',
        [batch_id]
    ).fetchone()
    assert result[0] == 1

    # Clean up data
    test_db.execute('DELETE FROM data WHERE batch_id = ?', [batch_id])

    # Verify data was cleaned up
    result = test_db.execute(
        'SELECT COUNT(*) FROM data WHERE batch_id = ?',
        [batch_id]
    ).fetchone()
    assert result[0] == 0

@pytest.mark.asyncio
async def test_concurrent_batch_isolation(test_db, clean_tasks):
    """Test that data from different batches is properly isolated"""
    batch_ids = ["batch_1", "batch_2"]
    current_time = datetime.now()

    # Insert data for different batches
    for batch_id in batch_ids:
        test_db.execute(
            'INSERT INTO data (batch_id, data, timestamp) VALUES (?, ?, ?)',
            (batch_id, json.dumps({"value": 42}), current_time)
        )

    # Verify each batch has correct data
    for batch_id in batch_ids:
        result = test_db.execute(
            'SELECT COUNT(*) FROM data WHERE batch_id = ?',
            [batch_id]
        ).fetchone()
        assert result[0] == 1

        # Verify data content
        data = test_db.execute(
            'SELECT data FROM data WHERE batch_id = ?',
            [batch_id]
        ).fetchone()
        assert json.loads(data[0])["value"] == 42
