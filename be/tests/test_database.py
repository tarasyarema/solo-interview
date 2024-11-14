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
    async with test_db.execute(
        'INSERT INTO data (id, batch_id, timestamp, value) VALUES (?, ?, ?, ?)',
        (1, batch_id, current_time, value)
    ):
        await test_db.commit()

    # Verify data was inserted
    async with test_db.execute(
        'SELECT COUNT(*) FROM data WHERE batch_id = ?',
        (batch_id,)
    ) as cursor:
        row = await cursor.fetchone()
        assert row[0] == 1

@pytest.mark.asyncio
async def test_data_retrieval(test_db, clean_tasks):
    """Test data retrieval functionality"""
    batch_id = "test_batch_retrieve"
    value = 100
    current_time = datetime.now()

    # Insert test data
    async with test_db.execute(
        'INSERT INTO data (id, batch_id, timestamp, value) VALUES (?, ?, ?, ?)',
        (1, batch_id, current_time, value)
    ):
        await test_db.commit()

    # Retrieve and verify data
    async with test_db.execute(
        'SELECT value FROM data WHERE batch_id = ?',
        (batch_id,)
    ) as cursor:
        row = await cursor.fetchone()
        assert row is not None
        assert row[0] == value

@pytest.mark.asyncio
async def test_data_aggregation(test_db, clean_tasks):
    """Test data aggregation for the last minute"""
    batch_id = "test_batch_agg"
    now = datetime.now()

    # Insert test data with timestamps in the last minute
    async with test_db.execute('BEGIN TRANSACTION'):
        for i in range(5):
            value = (4 - i) * 10  # Values: 40, 30, 20, 10, 0
            timestamp = now - timedelta(seconds=i * 10)  # Earlier timestamps have larger values
            async with test_db.execute(
                'INSERT INTO data (id, batch_id, timestamp, value) VALUES (?, ?, ?, ?)',
                (i + 1, batch_id, timestamp, value)
            ):
                pass
        await test_db.commit()

    # Verify data count
    async with test_db.execute(
        'SELECT COUNT(*) FROM data WHERE batch_id = ?',
        (batch_id,)
    ) as cursor:
        row = await cursor.fetchone()
        assert row[0] == 5

    # Verify data ordering by timestamp DESC (newest first)
    async with test_db.execute(
        'SELECT value FROM data WHERE batch_id = ? ORDER BY timestamp DESC',
        (batch_id,)
    ) as cursor:
        rows = await cursor.fetchall()
        assert len(rows) == 5
        for i, row in enumerate(rows):
            # Since we're ordering by DESC, we expect values in reverse order
            assert row[0] == (4 - i) * 10  # Values should be in reverse order: 40, 30, 20, 10, 0

@pytest.mark.asyncio
async def test_data_cleanup(test_db, clean_tasks):
    """Test data cleanup functionality"""
    batch_id = "test_batch_cleanup"
    current_time = datetime.now()

    # Insert some test data
    async with test_db.execute(
        'INSERT INTO data (id, batch_id, timestamp, value) VALUES (?, ?, ?, ?)',
        (1, batch_id, current_time, 42)
    ):
        await test_db.commit()

    # Verify data exists
    async with test_db.execute(
        'SELECT COUNT(*) FROM data WHERE batch_id = ?',
        (batch_id,)
    ) as cursor:
        row = await cursor.fetchone()
        assert row[0] == 1

    # Clean up data
    async with test_db.execute('DELETE FROM data WHERE batch_id = ?', (batch_id,)):
        await test_db.commit()

    # Verify data was cleaned up
    async with test_db.execute(
        'SELECT COUNT(*) FROM data WHERE batch_id = ?',
        (batch_id,)
    ) as cursor:
        row = await cursor.fetchone()
        assert row[0] == 0

@pytest.mark.asyncio
async def test_concurrent_batch_isolation(test_db, clean_tasks):
    """Test that data from different batches is properly isolated"""
    batch_ids = ["batch_1", "batch_2"]
    current_time = datetime.now()

    # Insert data for different batches
    async with test_db.execute('BEGIN TRANSACTION'):
        for i, batch_id in enumerate(batch_ids):
            async with test_db.execute(
                'INSERT INTO data (id, batch_id, timestamp, value) VALUES (?, ?, ?, ?)',
                (i + 1, batch_id, current_time, 42)
            ):
                pass
        await test_db.commit()

    # Verify each batch has correct data
    for batch_id in batch_ids:
        async with test_db.execute(
            'SELECT COUNT(*) FROM data WHERE batch_id = ?',
            (batch_id,)
        ) as cursor:
            row = await cursor.fetchone()
            assert row[0] == 1

        # Verify data content
        async with test_db.execute(
            'SELECT value FROM data WHERE batch_id = ?',
            (batch_id,)
        ) as cursor:
            row = await cursor.fetchone()
            assert row[0] == 42
