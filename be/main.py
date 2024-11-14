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
    try:
        while not asyncio.current_task().cancelled():
            await asyncio.sleep(0.25)
            value = random.randint(0, 100)

            # Ensure we have access to the database connection
            if not hasattr(app.state, 'db'):
                raise RuntimeError("Database connection not available")

            print(f"Inserting data for batch {batch_id}")
            app.state.db.execute(
                'INSERT INTO data (batch_id, data, timestamp) VALUES (?, ?, ?)',
                (batch_id, dumps({"value": value}), datetime.now())
            )
    except asyncio.CancelledError:
        print(f"Task {batch_id} was cancelled")
        # Remove the task from the tasks dict
        if batch_id in tasks:
            del tasks[batch_id]
        raise
    except Exception as e:
        print(f"Error in task {batch_id}: {str(e)}")
        # Remove the task from the tasks dict if it fails
        if batch_id in tasks:
            del tasks[batch_id]
        raise


@app.post("/stream/{batch_id}")
async def start(batch_id: str):
    # Check for duplicate task
    if batch_id in tasks:
        raise HTTPException(status_code=400, detail="Task already exists")

    # Check concurrent task limit
    if len(tasks) >= MAX_CONCURRENT_TASKS:
        raise HTTPException(status_code=400, detail="Maximum number of concurrent tasks reached")

    # Create new task
    task = asyncio.create_task(insert_task(batch_id))
    tasks[batch_id] = task

    return {
        "message": "Batch started",
        "batch_id": batch_id,
    }


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
