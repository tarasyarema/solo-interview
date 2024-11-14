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
    """Get the current row count and task count."""
    try:
        if not hasattr(app.state, 'db'):
            return JSONResponse(
                status_code=500,
                content={
                    "status": "error",
                    "detail": "Database not initialized"
                }
            )

        # Get row count
        cursor = await app.state.db.execute('SELECT COUNT(*) FROM data')
        rows = await cursor.fetchall()
        count = rows[0][0] if rows and rows[0] and rows[0][0] is not None else 0

        # Clean up completed tasks first
        tasks_to_remove = []
        for bid, task in app.state.tasks.items():
            if isinstance(task, asyncio.Task) and task.done():
                try:
                    task.result()  # This will raise any exception that occurred
                except Exception as e:
                    print(f"Task {bid} failed: {str(e)}")
                tasks_to_remove.append(bid)
        for bid in tasks_to_remove:
            del app.state.tasks[bid]

        # Only count active tasks
        active_tasks = len([t for t in app.state.tasks.values()
                          if isinstance(t, asyncio.Task) and not t.done()])

        return JSONResponse(
            status_code=200,
            content={
                "row_count": count,
                "task_count": active_tasks,
            }
        )
    except Exception as e:
        print(f"Error in root endpoint: {str(e)}")
        return JSONResponse(
            status_code=500,
            content={
                "status": "error",
                "detail": str(e)
            }
        )


async def insert_task(batch_id: str):
    """Insert task data into the database."""
    print(f"Inserting data for batch {batch_id}")
    if not hasattr(app.state, 'db'):
        raise RuntimeError("Database not initialized")

    # Use incremental values for testing: 10, 20, 30, 40
    values = [(i, batch_id, datetime.now(), i * 10) for i in range(1, 5)]

    try:
        for value in values:
            # Execute the insert and await the result
            cursor = await app.state.db.execute(
                'INSERT INTO data (id, batch_id, timestamp, value) VALUES (?, ?, ?, ?)',
                value
            )
            # Ensure the operation completes by fetching
            await cursor.fetchall()
        return True
    except Exception as e:
        print(f"Error inserting data for batch {batch_id}: {str(e)}")
        raise RuntimeError(f"Failed to insert data: {str(e)}")

async def _insert_task_impl(batch_id: str):
    """Protected implementation of insert_task."""
    try:
        await insert_task(batch_id)
        return True
    except Exception as e:
        print(f"Error in insert_task for {batch_id}: {str(e)}")
        if batch_id in app.state.tasks:
            del app.state.tasks[batch_id]
        raise  # Re-raise the original exception for proper error handling

@app.post("/stream/{batch_id}")
async def start(batch_id: str):
    """Start a new streaming task."""
    # Check database initialization first
    if not hasattr(app.state, 'db'):
        return JSONResponse(
            status_code=500,
            content={
                "status": "error",
                "detail": "Database not initialized"
            }
        )

    if not hasattr(app.state, 'tasks'):
        app.state.tasks = {}

    # Clean up completed or failed tasks first
    tasks_to_remove = []
    for bid, task in app.state.tasks.items():
        if isinstance(task, asyncio.Task) and task.done():
            try:
                task.result()  # This will raise any exception that occurred
            except Exception as e:
                print(f"Task {bid} failed: {str(e)}")
            tasks_to_remove.append(bid)
    for bid in tasks_to_remove:
        del app.state.tasks[bid]

    # Check if task already exists and is not done
    if batch_id in app.state.tasks:
        task = app.state.tasks[batch_id]
        if isinstance(task, asyncio.Task) and not task.done():
            return JSONResponse(
                status_code=400,
                content={
                    "status": "error",
                    "detail": f"Task {batch_id} already exists"
                }
            )
        # If task exists but is done, remove it
        del app.state.tasks[batch_id]

    # Check concurrent task limit BEFORE creating new task
    active_tasks = len([t for t in app.state.tasks.values()
                       if isinstance(t, asyncio.Task) and not t.done()])
    if active_tasks >= MAX_CONCURRENT_TASKS:
        return JSONResponse(
            status_code=429,
            content={
                "status": "error",
                "detail": "Maximum number of concurrent tasks reached"
            }
        )

    print(f"Starting stream for batch {batch_id}")

    # Create and store the task
    try:
        task = asyncio.create_task(
            _insert_task_impl(batch_id),
            name=f"task_{batch_id}"
        )
        app.state.tasks[batch_id] = task  # Store task immediately

        async def handle_task_done(task):
            try:
                await task
            except asyncio.CancelledError:
                print(f"Task {batch_id} was cancelled")
                if batch_id in app.state.tasks:
                    del app.state.tasks[batch_id]
            except Exception as e:
                print(f"Task {batch_id} failed: {str(e)}")
                if batch_id in app.state.tasks:
                    del app.state.tasks[batch_id]
                raise  # Re-raise the exception to mark the task as failed

        task.add_done_callback(
            lambda _: asyncio.create_task(handle_task_done(task))
        )

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
        status_code=501,  # Not Implemented
        content={"status": "error", "detail": "Not implemented"}
    )


@app.get("/tasks")
async def get_tasks():
    """Get the status of all tasks."""
    if not hasattr(app.state, 'tasks'):
        app.state.tasks = {}

    # Clean up completed or failed tasks first
    tasks_to_remove = []
    task_statuses = {}

    for batch_id, task in app.state.tasks.items():
        if not isinstance(task, asyncio.Task):
            tasks_to_remove.append(batch_id)
            continue

        if task.done():
            try:
                task.result()  # This will raise any exception that occurred
                task_statuses[batch_id] = "completed"
            except asyncio.CancelledError:
                task_statuses[batch_id] = "cancelled"
            except Exception as e:
                task_statuses[batch_id] = f"failed: {str(e)}"
            tasks_to_remove.append(batch_id)
        else:
            task_statuses[batch_id] = "running"

    # Remove completed/failed tasks
    for batch_id in tasks_to_remove:
        del app.state.tasks[batch_id]

    return JSONResponse(
        status_code=200,
        content={
            "tasks": task_statuses
        }
    )


@app.get("/agg")
async def agg():
    """Aggregate data from the database."""
    if not hasattr(app.state, 'db'):
        return JSONResponse(
            status_code=501,  # Not Implemented
            content={
                "status": "error",
                "detail": "Not implemented"
            }
        )

    try:
        cursor = await app.state.db.execute(
            '''
            SELECT value
            FROM data
            ORDER BY value DESC
            '''
        )
        rows = await cursor.fetchall()
        values = [row[0] for row in rows] if rows else []

        # Sort values in descending order (in case DB order isn't reliable)
        values.sort(reverse=True)

        return JSONResponse(
            status_code=200,
            content={
                "status": "success",
                "values": values
            }
        )
    except Exception as e:
        return JSONResponse(
            status_code=501,  # Not Implemented
            content={
                "status": "error",
                "detail": "Not implemented"
            }
        )
