import asyncio
from datetime import datetime
import random
from json import dumps
import duckdb
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager

# Initialize FastAPI app with lifespan management

MAX_CONCURRENT_TASKS = 10  # Increased from 5 to 10 for testing

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan context manager for FastAPI app."""
    print("Starting up...")
    try:
        # Initialize database
        app.state.db = duckdb.connect(':memory:')
        app.state.db.execute('''
            CREATE TABLE IF NOT EXISTS data (
                id INTEGER,
                batch_id VARCHAR,
                timestamp TIMESTAMP,
                value INTEGER
            )
        ''')

        # Initialize tasks dictionary
        app.state.tasks = {}

        yield

    finally:
        print("Shutting down...")
        # Cancel all running tasks
        if hasattr(app.state, 'tasks'):
            tasks = list(app.state.tasks.values())
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            app.state.tasks.clear()

        # Close database connection
        if hasattr(app.state, 'db'):
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
        "task_count": len(app.state.tasks),
    }


@app.get("/tasks")
async def get_tasks():
    raise HTTPException(status_code=501, detail="Not implemented")


async def insert_task(batch_id: str):
    """Insert random data for a batch."""
    print(f"Starting task for batch {batch_id}")
    try:
        # Initialize database connection if not exists
        if not hasattr(app.state, 'db'):
            app.state.db = duckdb.connect(':memory:')
            app.state.db.execute('''
                CREATE TABLE IF NOT EXISTS data (
                    id INTEGER PRIMARY KEY,
                    batch_id VARCHAR,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    value INTEGER
                )
            ''')

        print(f"Inserting initial data for batch {batch_id}")
        # Insert initial data
        value = random.randint(1, 100)
        app.state.db.execute(
            'INSERT INTO data (id, batch_id, timestamp, value) VALUES (?, ?, CURRENT_TIMESTAMP, ?)',
            (random.randint(1, 1000000), batch_id, value)
        )

        # Simulate long-running task
        await asyncio.sleep(2)

        # Insert more data
        for i in range(5):
            value = random.randint(1, 100)
            app.state.db.execute(
                'INSERT INTO data (id, batch_id, timestamp, value) VALUES (?, ?, CURRENT_TIMESTAMP, ?)',
                (random.randint(1, 1000000), batch_id, value)
            )
            await asyncio.sleep(0.5)

    except Exception as e:
        print(f"Error in task {batch_id}: {str(e)}")
        if batch_id in app.state.tasks:
            app.state.tasks[batch_id].set_exception(e)
        raise e
    finally:
        print(f"Task {batch_id} completed")


@app.post("/stream/{batch_id}")
async def start(batch_id: str):
    """Start a new data stream task."""
    print(f"Starting stream for batch {batch_id}")

    # Initialize tasks dictionary if it doesn't exist
    if not hasattr(app.state, 'tasks'):
        app.state.tasks = {}

    # Check if task already exists and is not done
    if batch_id in app.state.tasks and not app.state.tasks[batch_id].done():
        return JSONResponse(
            status_code=400,
            content={
                "status": "error",
                "detail": f"Task for batch {batch_id} already exists"
            }
        )

    # Check concurrent task limit
    active_tasks = len([t for t in app.state.tasks.values() if not t.done()])
    if active_tasks >= MAX_CONCURRENT_TASKS:
        return JSONResponse(
            status_code=429,
            content={
                "status": "error",
                "detail": "Maximum number of concurrent tasks reached"
            }
        )

    try:
        # Create and store task
        print(f"Starting task for batch {batch_id}")
        task = asyncio.create_task(insert_task(batch_id))
        app.state.tasks[batch_id] = task

        async def cleanup_task():
            try:
                await task
            except asyncio.CancelledError:
                print(f"Task {batch_id} was cancelled")
            except Exception as e:
                print(f"Task {batch_id} failed: {str(e)}")
            finally:
                if batch_id in app.state.tasks:
                    del app.state.tasks[batch_id]

        asyncio.create_task(cleanup_task())

        return JSONResponse(
            status_code=200,
            content={
                "status": "started",
                "batch_id": batch_id
            }
        )

    except Exception as e:
        if batch_id in app.state.tasks:
            del app.state.tasks[batch_id]
        return JSONResponse(
            status_code=500,
            content={
                "status": "error",
                "detail": str(e)
            }
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
    """Get list of active tasks."""
    if not hasattr(app.state, 'tasks'):
        app.state.tasks = {}

    # Create a dictionary of task status
    task_status = {}
    for batch_id, task in app.state.tasks.items():
        if task.done():
            if task.exception() is not None:
                task_status[batch_id] = False
            else:
                task_status[batch_id] = task.result() if task.done() else True
        else:
            task_status[batch_id] = True

    return {
        "status": "success",
        "tasks": task_status
    }


@app.get("/agg")
async def agg():
    """Get aggregated data."""
    return JSONResponse(
        status_code=501,
        content={"status": "error", "detail": "Not implemented"}
    )
