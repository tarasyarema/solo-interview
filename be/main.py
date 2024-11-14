from contextlib import asynccontextmanager
from json import dumps
from datetime import datetime
import random

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware

import duckdb
import asyncio


# For testing purposes, we'll set this to a higher number
MAX_CONCURRENT_TASKS = 10  # Increased from 5 to 10 for testing
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
    """Insert data for a batch in a continuous loop."""
    print(f"Starting task for batch {batch_id}")
    inserted = False
    try:
        # Initial data insertion
        print(f"Inserting initial data for batch {batch_id}")
        value = random.randint(0, 100)
        app.state.db.execute(
            'INSERT INTO data (batch_id, data, timestamp) VALUES (?, ?, ?)',
            (batch_id, dumps({"value": value}), datetime.now())
        )
        inserted = True

        # Continuous insertion until cancelled
        while True:
            try:
                # Check if task is cancelled
                if asyncio.current_task().cancelled():
                    print(f"Task {batch_id} was cancelled")
                    break

                print(f"Inserting data for batch {batch_id}")
                value = random.randint(0, 100)
                app.state.db.execute(
                    'INSERT INTO data (batch_id, data, timestamp) VALUES (?, ?, ?)',
                    (batch_id, dumps({"value": value}), datetime.now())
                )
                # Add a longer delay to prevent rapid completion
                await asyncio.sleep(1.0)
            except asyncio.CancelledError:
                print(f"Task {batch_id} was cancelled")
                if batch_id in tasks:
                    del tasks[batch_id]
                break

    except Exception as e:
        print(f"Error in task {batch_id}: {str(e)}")
        if batch_id in tasks:
            del tasks[batch_id]
        raise
    finally:
        # Only clean up if task failed before first insertion
        if not inserted and batch_id in tasks:
            del tasks[batch_id]
        print(f"Task {batch_id} completed")


@app.post("/stream/{batch_id}")
async def start(batch_id: str):
    """Start a new streaming task."""
    # Clean up completed tasks first
    for task_id, task in list(tasks.items()):
        if task.done():
            del tasks[task_id]

    if batch_id in tasks:
        return JSONResponse(
            status_code=400,
            content={"detail": f"Task {batch_id} already exists"}
        )

    active_tasks = len([t for t in tasks.values() if not t.done()])
    if active_tasks >= MAX_CONCURRENT_TASKS:
        return JSONResponse(
            status_code=429,
            content={"detail": "Too many concurrent tasks"}
        )

    try:
        # Create and store the task
        task = asyncio.create_task(insert_task(batch_id))
        tasks[batch_id] = task

        # Give the task a moment to start and potentially fail
        await asyncio.sleep(0.1)

        # Check if the task failed immediately
        if task.done():
            if task.exception():
                exc = task.exception()
                if batch_id in tasks:
                    del tasks[batch_id]
                return JSONResponse(
                    status_code=500,
                    content={"detail": str(exc)}
                )

        return JSONResponse(
            status_code=200,
            content={
                "status": "started",
                "batch_id": batch_id
            }
        )
    except Exception as e:
        if batch_id in tasks:
            del tasks[batch_id]
        return JSONResponse(
            status_code=500,
            content={"detail": str(e)}
        )


@app.delete("/stream/{batch_id}")
async def data_stop(batch_id: str):
    raise HTTPException(status_code=501, detail="Not implemented")


@app.get("/stream/{batch_id}")
async def stream(batch_id: str):
    # Check if task exists or has data
    if batch_id not in tasks:
        # Check if there's any data for this batch before failing
        result = app.state.db.execute(
            'SELECT COUNT(*) FROM data WHERE batch_id = ?',
            [batch_id]
        ).fetchone()

        if not result or result[0] == 0:
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
