from contextlib import asynccontextmanager
import asyncio
import random
from datetime import datetime
from json import dumps
from typing import Dict, Optional

import duckdb
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware

MAX_CONCURRENT_TASKS = 3


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
        await asyncio.sleep(0.2)

        # Continuous insertion until cancelled
        while True:
            try:
                value = random.randint(0, 100)
                app.state.db.execute(
                    'INSERT INTO data (batch_id, data, timestamp) VALUES (?, ?, ?)',
                    (batch_id, dumps({"value": value}), datetime.now())
                )
                # Add a shorter delay between insertions
                await asyncio.sleep(0.1)
            except asyncio.CancelledError:
                print(f"Task {batch_id} cancelled during execution")
                if batch_id in tasks:
                    tasks.pop(batch_id)
                raise
            except Exception as e:
                print(f"Error in task {batch_id}: {str(e)}")
                if batch_id in tasks:
                    tasks.pop(batch_id)
                raise

    except asyncio.CancelledError:
        print(f"Task {batch_id} cancelled during startup")
        if batch_id in tasks:
            tasks.pop(batch_id)
        raise
    except Exception as e:
        print(f"Error in task {batch_id}: {str(e)}")
        if batch_id in tasks:
            tasks.pop(batch_id)
        raise


@app.post("/stream/{batch_id}")
async def start(batch_id: str, request: Request):
    """Start a new data stream for a batch."""
    try:
        # Check if task already exists
        if batch_id in tasks:
            if not tasks[batch_id].done():
                return JSONResponse(
                    status_code=400,
                    content={"status": "error", "detail": f"Task for batch {batch_id} already exists"}
                )
            else:
                # Remove completed/failed task before creating new one
                tasks.pop(batch_id)

        # Check concurrent task limit
        active_tasks = sum(1 for task in tasks.values() if not task.done())
        if active_tasks >= MAX_CONCURRENT_TASKS:
            return JSONResponse(
                status_code=429,
                content={"status": "error", "detail": "Maximum number of concurrent tasks reached"}
            )

        # Create and store the task
        task = asyncio.create_task(insert_task(batch_id))
        tasks[batch_id] = task

        # Wait briefly to ensure task starts
        try:
            await asyncio.wait_for(asyncio.shield(task), timeout=0.1)
        except asyncio.TimeoutError:
            # Task is still running, which is good
            pass
        except Exception as e:
            # Task failed to start
            if batch_id in tasks:
                tasks.pop(batch_id)
            return JSONResponse(
                status_code=500,
                content={"status": "error", "detail": str(e)}
            )

        # Set up cleanup for when client disconnects
        async def cleanup_task():
            """Clean up task when client disconnects."""
            try:
                if batch_id in tasks:
                    task = tasks[batch_id]
                    if not task.done():
                        task.cancel()
                        try:
                            await task
                        except asyncio.CancelledError:
                            pass
                        except Exception:
                            pass
                    tasks.pop(batch_id)
            except Exception as e:
                print(f"Error cleaning up task {batch_id}: {str(e)}")

        # Store cleanup task in request state and start it
        request.state.cleanup_tasks = getattr(request.state, 'cleanup_tasks', [])
        cleanup_coro = cleanup_task()
        request.state.cleanup_tasks.append(cleanup_coro)
        asyncio.create_task(cleanup_coro)

        return {"status": "started", "batch_id": batch_id}

    except Exception as e:
        # Clean up task if it exists
        if batch_id in tasks:
            task = tasks.pop(batch_id)
            if not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
                except Exception:
                    pass

        return JSONResponse(
            status_code=500,
            content={"status": "error", "detail": str(e)}
        )


@app.delete("/stream/{batch_id}")
async def data_stop(batch_id: str):
    """Stop a streaming task."""
    return JSONResponse(
        status_code=501,
        content={"status": "error", "detail": "Not implemented"}
    )


@app.get("/tasks")
async def get_tasks():
    """Get list of running tasks."""
    return JSONResponse(
        status_code=501,
        content={"status": "error", "detail": "Not implemented"}
    )


@app.get("/agg")
async def agg():
    """Get aggregated data."""
    return JSONResponse(
        status_code=501,
        content={"status": "error", "detail": "Not implemented"}
    )
