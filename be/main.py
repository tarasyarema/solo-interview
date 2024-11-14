from contextlib import asynccontextmanager
from json import dumps
from datetime import datetime
import random

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse

import duckdb
import asyncio


MAX_CONCURRENT_TASKS = 5
tasks: dict[str, asyncio.Task] = {}


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Create and configure database
    db = duckdb.connect('data.db')
    db.execute('''
        CREATE TABLE IF NOT EXISTS data (
            id BIGINT,
            batch_id VARCHAR,
            data VARCHAR,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE INDEX IF NOT EXISTS idx_data_batch_id ON data (batch_id);
    ''')

    # Store database connection in app state
    app.state.db = db

    yield

    # Cleanup
    app.state.db.close()


app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
async def root():
    count = app.state.db.execute('SELECT COUNT(*) FROM data').fetchone()

    if count:
        count = count[0]
    else:
        count = 0

    return {
        "row_count": count,
        "task_count": len(tasks),
    }


@app.get("/tasks")
async def get_tasks():
    raise HTTPException(status_code=501, detail="Not implemented")


async def insert_task(batch_id: str):
    print(f"Starting task for batch {batch_id}")
    try:
        while True:
            # Check if task is cancelled before sleeping
            current_task = asyncio.current_task()
            if current_task and current_task.cancelled():
                break

            # Insert at least one value before potentially being cancelled
            value = random.randint(0, 100)

            # Ensure we have access to the database connection
            if not hasattr(app.state, 'db'):
                raise RuntimeError("Database connection not available")

            print(f"Inserting data for batch {batch_id}")
            app.state.db.execute(
                'INSERT INTO data (batch_id, data, timestamp) VALUES (?, ?, ?)',
                (batch_id, dumps({"value": value}), datetime.now())
            )

            # Only sleep after first insert
            await asyncio.sleep(0.25)

    except asyncio.CancelledError:
        print(f"Task {batch_id} was cancelled")
        raise
    except Exception as e:
        print(f"Error in task {batch_id}: {str(e)}")
        if batch_id in tasks:
            del tasks[batch_id]
        raise
    finally:
        # Only remove from tasks dict if this is the current task for this batch_id
        if batch_id in tasks and tasks[batch_id] == asyncio.current_task():
            del tasks[batch_id]


@app.post("/stream/{batch_id}")
async def start(batch_id: str):
    # Check for duplicate task first
    if batch_id in tasks:
        raise HTTPException(status_code=400, detail="Task already exists")

    # Then check concurrent task limit
    if len(tasks) >= MAX_CONCURRENT_TASKS:
        raise HTTPException(status_code=400, detail="Maximum number of concurrent tasks reached")

    try:
        # Create new task and store it before any potential errors
        task = asyncio.create_task(insert_task(batch_id))
        tasks[batch_id] = task

        # Wait a short time to ensure task starts and inserts at least one value
        await asyncio.sleep(0.25)

        # Check if task failed immediately
        if task.done() and task.exception():
            raise task.exception()

        return {
            "message": "Batch started",
            "batch_id": batch_id,
        }
    except Exception as e:
        # Clean up task if it exists
        if batch_id in tasks:
            tasks[batch_id].cancel()
            del tasks[batch_id]
        # Re-raise as HTTP exception
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/stream/{batch_id}")
async def data_stop(batch_id: str):
    raise HTTPException(status_code=501, detail="Not implemented")


@app.get("/stream/{batch_id}")
async def stream(batch_id: str):
    if batch_id not in tasks:
        raise HTTPException(status_code=404, detail="Task not found")

    async def _gen():
        while True:
            # Get the latest data for this batch
            result = app.state.db.execute(
                'SELECT data FROM data WHERE batch_id = ? ORDER BY timestamp DESC LIMIT 1',
                [batch_id]
            ).fetchone()

            if result:
                yield f"data: {result[0]}\n\n"
            await asyncio.sleep(0.1)

    return StreamingResponse(_gen(), media_type="text/event-stream")


@app.get("/agg/{batch_id}")
async def agg(batch_id: str):
    raise HTTPException(status_code=501, detail="Not implemented")
