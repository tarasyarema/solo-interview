import pytest
import asyncio
from datetime import datetime, timedelta

@pytest.mark.asyncio
async def test_data_insertion(test_db, clean_tasks):
    """Test basic data insertion functionality"""
    batch_id = "test_batch_db"
    value = 42
    current_time = datetime.now()

    # Insert test data
    test_db.execute(
        'INSERT INTO data (id, batch_id, timestamp, value) VALUES (?, ?, ?, ?)',
        (1, batch_id, current_time, value)
    )

    # Verify data was inserted
    result = test_db.execute('SELECT COUNT(*) FROM data WHERE batch_id = ?', [batch_id]).fetchone()
    assert result[0] == 1

@pytest.mark.asyncio
async def test_data_retrieval(test_db, clean_tasks):
    """Test data retrieval functionality"""
    batch_id = "test_batch_retrieve"
    value = 100
    current_time = datetime.now()

    # Insert test data
    test_db.execute(
        'INSERT INTO data (id, batch_id, timestamp, value) VALUES (?, ?, ?, ?)',
        (1, batch_id, current_time, value)
    )

    # Retrieve and verify data
    result = test_db.execute(
        'SELECT value FROM data WHERE batch_id = ?',
        [batch_id]
    ).fetchone()

    assert result is not None
    assert result[0] == value

@pytest.mark.asyncio
async def test_data_aggregation(test_db, clean_tasks):
    """Test data aggregation for the last minute"""
    batch_id = "test_batch_agg"
    now = datetime.now()

    # Insert test data with timestamps in the last minute
    for i in range(5):
        value = i * 10
        timestamp = now - timedelta(seconds=i * 10)
        test_db.execute(
            'INSERT INTO data (id, batch_id, timestamp, value) VALUES (?, ?, ?, ?)',
            (i + 1, batch_id, timestamp, value)
        )

    # Verify data count
    result = test_db.execute(
        'SELECT COUNT(*) FROM data WHERE batch_id = ?',
        [batch_id]
    ).fetchone()
    assert result[0] == 5

    # Verify data ordering
    results = test_db.execute(
        'SELECT value FROM data WHERE batch_id = ? ORDER BY timestamp DESC',
        [batch_id]
    ).fetchall()
    assert len(results) == 5
    for i, row in enumerate(results):
        assert row[0] == (4 - i) * 10  # Values should be in reverse order due to DESC

@pytest.mark.asyncio
async def test_data_cleanup(test_db, clean_tasks):
    """Test data cleanup functionality"""
    batch_id = "test_batch_cleanup"
    current_time = datetime.now()

    # Insert some test data
    test_db.execute(
        'INSERT INTO data (id, batch_id, timestamp, value) VALUES (?, ?, ?, ?)',
        (1, batch_id, current_time, 42)
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
    for i, batch_id in enumerate(batch_ids):
        test_db.execute(
            'INSERT INTO data (id, batch_id, timestamp, value) VALUES (?, ?, ?, ?)',
            (i + 1, batch_id, current_time, 42)
        )

    # Verify each batch has correct data
    for batch_id in batch_ids:
        result = test_db.execute(
            'SELECT COUNT(*) FROM data WHERE batch_id = ?',
            [batch_id]
        ).fetchone()
        assert result[0] == 1

        # Verify data content
        value = test_db.execute(
            'SELECT value FROM data WHERE batch_id = ?',
            [batch_id]
        ).fetchone()
        assert value[0] == 42
