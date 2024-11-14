import pytest
import asyncio
import json
from datetime import datetime, timedelta

@pytest.mark.asyncio
async def test_data_insertion(test_db, clean_tasks):
    """Test basic data insertion functionality"""
    batch_id = "test_batch_db"
    test_data = json.dumps({"key": 42})

    # Insert test data
    await asyncio.sleep(0.1)  # Allow any pending transactions to complete
    test_db.execute(
        'INSERT INTO data (batch_id, id, data) VALUES (?, ?, ?)',
        (batch_id, datetime.now().isoformat(), test_data)
    )

    # Verify data was inserted
    result = test_db.execute('SELECT COUNT(*) FROM data WHERE batch_id = ?', [batch_id]).fetchone()
    assert result[0] == 1


@pytest.mark.asyncio
async def test_data_retrieval(test_db, clean_tasks):
    """Test data retrieval functionality"""
    batch_id = "test_batch_retrieve"
    test_data = json.dumps({"key": 100})
    test_id = datetime.now().isoformat()

    # Insert test data
    await asyncio.sleep(0.1)  # Allow any pending transactions to complete
    test_db.execute(
        'INSERT INTO data (batch_id, id, data) VALUES (?, ?, ?)',
        (batch_id, test_id, test_data)
    )

    # Retrieve and verify data
    result = test_db.execute(
        'SELECT data FROM data WHERE batch_id = ? AND id = ?',
        [batch_id, test_id]
    ).fetchone()

    assert result is not None
    assert result[0] == test_data


@pytest.mark.asyncio
async def test_data_aggregation(test_db, clean_tasks):
    """Test data aggregation for the last minute"""
    batch_id = "test_batch_agg"
    now = datetime.now()

    # Insert test data with timestamps in the last minute
    await asyncio.sleep(0.1)  # Allow any pending transactions to complete
    for i, seconds in enumerate([10, 20, 30, 40, 50]):
        timestamp = (now - timedelta(seconds=seconds)).isoformat()
        test_data = json.dumps({"key": i * 10})
        test_db.execute(
            'INSERT INTO data (batch_id, id, data) VALUES (?, ?, ?)',
            (batch_id, timestamp, test_data)
        )

    # Verify data count
    result = test_db.execute(
        'SELECT COUNT(*) FROM data WHERE batch_id = ?',
        [batch_id]
    ).fetchone()
    assert result[0] == 5


@pytest.mark.asyncio
async def test_data_cleanup(test_db, clean_tasks):
    """Test data cleanup functionality"""
    batch_id = "test_batch_cleanup"

    # Insert some test data
    await asyncio.sleep(0.1)  # Allow any pending transactions to complete
    test_db.execute(
        'INSERT INTO data (batch_id, id, data) VALUES (?, ?, ?)',
        (batch_id, datetime.now().isoformat(), json.dumps({"key": 1}))
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

    # Insert data for different batches
    await asyncio.sleep(0.1)  # Allow any pending transactions to complete
    for batch_id in batch_ids:
        test_db.execute(
            'INSERT INTO data (batch_id, id, data) VALUES (?, ?, ?)',
            (batch_id, datetime.now().isoformat(), json.dumps({"key": 42}))
        )

    # Verify each batch has correct data
    for batch_id in batch_ids:
        result = test_db.execute(
            'SELECT COUNT(*) FROM data WHERE batch_id = ?',
            [batch_id]
        ).fetchone()
        assert result[0] == 1
