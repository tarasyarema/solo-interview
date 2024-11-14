import json
import pytest
from datetime import datetime, timedelta

@pytest.mark.asyncio
async def test_data_insertion(test_db, clean_tasks):
    """Test basic data insertion functionality"""
    batch_id = "test_batch_db"
    test_data = json.dumps({"key": 42})

    # Insert test data
    test_db.execute(
        'INSERT INTO data (batch_id, id, data) VALUES (?, ?, ?)',
        [batch_id, datetime.now().isoformat(), test_data]
    )

    # Verify insertion
    result = test_db.execute(
        'SELECT COUNT(*) FROM data WHERE batch_id = ?',
        [batch_id]
    ).fetchone()

    assert result[0] == 1

@pytest.mark.asyncio
async def test_data_retrieval(test_db, clean_tasks):
    """Test data retrieval functionality"""
    batch_id = "test_batch_retrieve"
    test_data = json.dumps({"key": 100})
    test_id = datetime.now().isoformat()

    # Insert test data
    test_db.execute(
        'INSERT INTO data (batch_id, id, data) VALUES (?, ?, ?)',
        [batch_id, test_id, test_data]
    )

    # Retrieve and verify data
    result = test_db.execute(
        'SELECT data FROM data WHERE batch_id = ?',
        [batch_id]
    ).fetchone()

    assert result is not None
    data = json.loads(result[0])
    assert data["key"] == 100

@pytest.mark.asyncio
async def test_data_aggregation(test_db, clean_tasks):
    """Test data aggregation for the last minute"""
    batch_id = "test_batch_agg"
    now = datetime.now()

    # Insert test data with timestamps in the last minute
    for i, seconds in enumerate([10, 20, 30, 40, 50]):
        timestamp = (now - timedelta(seconds=seconds)).isoformat()
        test_data = json.dumps({"key": i * 10})
        test_db.execute(
            'INSERT INTO data (batch_id, id, data) VALUES (?, ?, ?)',
            [batch_id, timestamp, test_data]
        )

    # Insert old data that shouldn't be included
    old_timestamp = (now - timedelta(minutes=2)).isoformat()
    test_db.execute(
        'INSERT INTO data (batch_id, id, data) VALUES (?, ?, ?)',
        [batch_id, old_timestamp, json.dumps({"key": 999})]
    )

    # Calculate aggregations for the last minute
    result = test_db.execute('''
        WITH parsed_data AS (
            SELECT
                batch_id,
                id,
                CAST(JSON_EXTRACT(data, '$.key') AS INTEGER) as key_value,
                CAST(id AS TIMESTAMP) as timestamp
            FROM data
            WHERE batch_id = ?
            AND CAST(id AS TIMESTAMP) >= ?
            AND CAST(id AS TIMESTAMP) < ?
        )
        SELECT
            COUNT(*) as count,
            AVG(key_value) as avg,
            SUM(key_value) as sum
        FROM parsed_data
    ''', [
        batch_id,
        (now - timedelta(minutes=1)).isoformat(),
        now.isoformat()
    ]).fetchone()

    count, avg, sum_val = result
    assert count == 5  # Only data from last minute
    assert avg == 20.0  # Average of 0, 10, 20, 30, 40
    assert sum_val == 100  # Sum of 0, 10, 20, 30, 40

@pytest.mark.asyncio
async def test_data_cleanup(test_db, clean_tasks):
    """Test data cleanup functionality"""
    batch_id = "test_batch_cleanup"

    # Insert some test data
    test_db.execute(
        'INSERT INTO data (batch_id, id, data) VALUES (?, ?, ?)',
        [batch_id, datetime.now().isoformat(), json.dumps({"key": 1})]
    )

    # Verify data exists
    count_before = test_db.execute(
        'SELECT COUNT(*) FROM data WHERE batch_id = ?',
        [batch_id]
    ).fetchone()[0]
    assert count_before == 1

    # Clean up data
    test_db.execute('DELETE FROM data WHERE batch_id = ?', [batch_id])

    # Verify data is cleaned up
    count_after = test_db.execute(
        'SELECT COUNT(*) FROM data WHERE batch_id = ?',
        [batch_id]
    ).fetchone()[0]
    assert count_after == 0

@pytest.mark.asyncio
async def test_concurrent_batch_isolation(test_db, clean_tasks):
    """Test that data from different batches is properly isolated"""
    batch_ids = ["batch_1", "batch_2"]

    # Insert data for different batches
    for batch_id in batch_ids:
        test_db.execute(
            'INSERT INTO data (batch_id, id, data) VALUES (?, ?, ?)',
            [batch_id, datetime.now().isoformat(), json.dumps({"key": 42})]
        )

    # Verify each batch has correct data
    for batch_id in batch_ids:
        result = test_db.execute(
            'SELECT COUNT(*) FROM data WHERE batch_id = ?',
            [batch_id]
        ).fetchone()
        assert result[0] == 1
