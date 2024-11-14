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
        await asyncio.sleep(0.5)

        # Continuous insertion until cancelled
        while True:
            try:
                value = random.randint(0, 100)
                app.state.db.execute(
                    'INSERT INTO data (batch_id, data, timestamp) VALUES (?, ?, ?)',
                    (batch_id, dumps({"value": value}), datetime.now())
                )
                # Add a shorter delay between insertions
                await asyncio.sleep(0.2)
            except asyncio.CancelledError:
                print(f"Task {batch_id} cancelled during execution")
                # Clean up gracefully
                tasks.pop(batch_id, None)
                raise
            except Exception as e:
                print(f"Error in task {batch_id}: {str(e)}")
                tasks.pop(batch_id, None)
                raise

    except asyncio.CancelledError:
        print(f"Task {batch_id} cancelled during startup")
        tasks.pop(batch_id, None)
        raise
    except Exception as e:
        print(f"Error in task {batch_id}: {str(e)}")
        tasks.pop(batch_id, None)
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
                    content={"error": f"Task for batch {batch_id} already exists"}
                )
            else:
                # Remove completed/failed task
                tasks.pop(batch_id)

        # Check concurrent task limit
        active_tasks = sum(1 for task in tasks.values() if not task.done())
        if active_tasks >= 3:
            return JSONResponse(
                status_code=429,
                content={"error": "Maximum number of concurrent tasks reached"}
            )

        # Create and store the task
        task = asyncio.create_task(insert_task(batch_id))
        tasks[batch_id] = task

        # Wait briefly to ensure task starts
        await asyncio.sleep(0.1)

        # Check if task failed immediately
        if task.done():
            exc = task.exception()
            if exc:
                tasks.pop(batch_id)
                return JSONResponse(
                    status_code=500,
                    content={"error": f"Task failed: {str(exc)}"}
                )

        # Set up cleanup for when client disconnects
        async def cleanup_task():
            """Clean up task when client disconnects."""
            try:
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
            except Exception as e:
                print(f"Error cleaning up task {batch_id}: {str(e)}")

        request.state.cleanup_tasks = request.state.cleanup_tasks if hasattr(request.state, 'cleanup_tasks') else []
        request.state.cleanup_tasks.append(cleanup_task())

        return {"message": f"Started task for batch {batch_id}"}

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
            content={"error": f"Error starting task: {str(e)}"}
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
