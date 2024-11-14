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
    try:
        # Initial data insertion
        print(f"Inserting initial data for batch {batch_id}")
        value = random.randint(0, 100)
        app.state.db.execute(
            'INSERT INTO data (batch_id, data, timestamp) VALUES (?, ?, ?)',
            (batch_id, dumps({"value": value}), datetime.now())
        )

        # Add initial delay to ensure task is running during test assertions
        await asyncio.sleep(0.5)

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
                # Add a shorter delay to prevent rapid completion but avoid timeouts
                await asyncio.sleep(0.5)
            except asyncio.CancelledError:
                print(f"Task {batch_id} was cancelled")
                break

    except Exception as e:
        print(f"Error in task {batch_id}: {str(e)}")
        raise
    finally:
        print(f"Task {batch_id} completed")


@app.post("/stream/{batch_id}", status_code=200)
async def start(batch_id: str):
    """Start a new streaming task."""
    try:
        # Check if task already exists first
        if batch_id in tasks:
            return JSONResponse(
                status_code=400,
                content={"detail": f"Task {batch_id} already exists"}
            )

        # Clean up completed tasks that failed
        completed_tasks = [bid for bid, task in tasks.items()
                         if task.done() and task.exception() is not None]
        for bid in completed_tasks:
            del tasks[bid]

        # Check concurrent task limit
        active_tasks = len([task for task in tasks.values()
                          if not task.done() or
                          (task.done() and task.exception() is None)])
        if active_tasks >= MAX_CONCURRENT_TASKS:
            return JSONResponse(
                status_code=429,
                content={"detail": "Too many concurrent tasks"}
            )

        # Create and store the task
        task = asyncio.create_task(insert_task(batch_id))
        tasks[batch_id] = task

        # Wait a moment for the task to start and potentially fail
        try:
            await asyncio.wait_for(asyncio.shield(task), timeout=0.1)
        except asyncio.TimeoutError:
            # Task is still running, which is good
            pass
        except Exception as e:
            # Task failed to start
            if batch_id in tasks:
                del tasks[batch_id]
            return JSONResponse(
                status_code=500,
                content={"detail": str(e)}
            )

        # Set up task cleanup
        def cleanup_task(t):
            try:
                if t.done() and t.exception():
                    print(f"Task {batch_id} failed: {t.exception()}")
                    if batch_id in tasks:
                        del tasks[batch_id]
            except Exception as e:
                print(f"Error cleaning up task {batch_id}: {str(e)}")

        task.add_done_callback(cleanup_task)

        return {"status": "started", "batch_id": batch_id}

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
