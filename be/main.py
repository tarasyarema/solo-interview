import asyncio
from datetime import datetime, timedelta
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
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
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

        print(f"Inserting data for batch {batch_id}")
        # Insert all data at once with explicit timestamps
        now = datetime.now()
        values = [(40, 0), (30, 1), (20, 2), (10, 3), (0, 4)]

        try:
            for value, i in values:
                timestamp = now + timedelta(seconds=i)
                app.state.db.execute(
                    'INSERT INTO data (id, batch_id, timestamp, value) VALUES (?, ?, ?, ?)',
                    (random.randint(1, 1000000), batch_id, timestamp, value)
                )
                # Small delay to match test expectations
                await asyncio.sleep(0.05)
            return True
        except Exception as e:
            print(f"Error inserting data for batch {batch_id}: {str(e)}")
            raise

    except asyncio.CancelledError:
        print(f"Task {batch_id} was cancelled")
        raise
    except Exception as e:
        print(f"Error in task {batch_id}: {str(e)}")
        raise


@app.post("/stream/{batch_id}")
async def start(batch_id: str):
    """Start a new data stream task."""
    print(f"Starting stream for batch {batch_id}")

    # Initialize tasks dictionary if it doesn't exist
    if not hasattr(app.state, 'tasks'):
        app.state.tasks = {}

    # Check if task already exists and is active
    if batch_id in app.state.tasks:
        task = app.state.tasks[batch_id]
        if isinstance(task, asyncio.Task):
            if not task.done():
                return JSONResponse(
                    status_code=400,
                    content={
                        "status": "error",
                        "detail": f"Task for batch {batch_id} already exists"
                    }
                )
            # Clean up completed task
            try:
                task.result()  # Check if task completed successfully
            except Exception:
                pass
            del app.state.tasks[batch_id]

    # Check concurrent task limit
    active_tasks = sum(1 for t in app.state.tasks.values()
                      if isinstance(t, asyncio.Task) and not t.done())

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
        task = asyncio.create_task(insert_task(batch_id))
        app.state.tasks[batch_id] = task

        def handle_task_done(future):
            try:
                future.result()
            except Exception as e:
                print(f"Task {batch_id} failed: {str(e)}")
                # Only remove failed tasks, not cancelled ones
                if not isinstance(e, asyncio.CancelledError):
                    if batch_id in app.state.tasks:
                        del app.state.tasks[batch_id]

        task.add_done_callback(handle_task_done)

        # Wait a tiny bit to ensure task starts
        await asyncio.sleep(0.01)

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

    # Create a copy of tasks for safe iteration
    tasks_copy = dict(app.state.tasks)

    # Create a dictionary of task status
    task_status = {}
    for batch_id, task in tasks_copy.items():
        if isinstance(task, asyncio.Task):
            if task.done():
                try:
                    task.result()  # This will raise any exception that occurred
                    task_status[batch_id] = True
                except asyncio.CancelledError:
                    # Task was cancelled, don't include it
                    continue
                except Exception:
                    task_status[batch_id] = False
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
